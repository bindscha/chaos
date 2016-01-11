/*
 * Chaos 
 *
 * Copyright 2015 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _BFS_FOREST_
#define _BFS_FOREST_

#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>

namespace algorithm {
  namespace sg_simple {
    class bfs_forest_per_processor_data : public per_processor_data {
    public:
        static unsigned long merges;
        unsigned long local_merges;
        unsigned long *mc_merges;
        unsigned long machines;

        bfs_forest_per_processor_data(unsigned long machines_in)
            : local_merges(0),
              machines(machines_in) {
          mc_merges = new unsigned long[machines];
          for (unsigned long i = 0; i < machines; i++) {
            mc_merges[i] = 0;
          }
        }

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          for (unsigned long i = 0; i < processors; i++) {
            bfs_forest_per_processor_data *data =
                static_cast<bfs_forest_per_processor_data *>(per_cpu_array[i]);
            merges += data->local_merges;
            data->local_merges = 0;
          }
          return false;
        }
    }  __attribute__((__aligned__(64)));

    template<typename F>
    class bfs_forest {
    public:

        static unsigned long checkpoint_size() {
          return sizeof(unsigned long);
        }

        static void take_checkpoint(unsigned char *buffer,
                                    per_processor_data **per_cpu_array,
                                    unsigned long processors) {
          bfs_forest_per_processor_data *data =
              static_cast<bfs_forest_per_processor_data *>(per_cpu_array[0]);
          (void) data->reduce(per_cpu_array, processors);
          memcpy(buffer, &bfs_forest_per_processor_data::merges, sizeof(unsigned long));
        }

        static void restore_checkpoint(unsigned char *buffer,
                                       per_processor_data **per_cpu_array,
                                       unsigned long processors) {
          memcpy(&bfs_forest_per_processor_data::merges, buffer, sizeof(unsigned long));
        }


        struct __attribute__((__packed__)) bfs_update {
            vertex_t component;
            vertex_t parent;
            vertex_t child;
        };
        struct __attribute__((__packed__)) bfs_vertex {
            vertex_t component;
            vertex_t bfs_parent;
            vertex_t bsp_phase;
        };

        static unsigned long split_size_bytes() {
          return sizeof(struct bfs_update);
        }

        static unsigned long vertex_state_bytes() {
          return sizeof(struct bfs_vertex);
        }

        static unsigned long split_key(unsigned char *buffer,
                                       unsigned long jump) {
          struct bfs_update *update = (struct bfs_update *) buffer;
          vertex_t key = update->child;
          key = key >> jump;
          return key;
        }

        static
        bool need_data_barrier() {
          return false;
        }

        class db_sync {
        public:
            void prep_db_data(per_processor_data **pcpu_array,
                              unsigned long me,
                              unsigned long processors) { }

            void finalize_db_data(per_processor_data **pcpu_array,
                                  unsigned long me,
                                  unsigned long processors) { }

            unsigned char *db_buffer() { return NULL; }

            unsigned long db_size() { return 0; }

            void db_generate() { }

            void db_merge() { }

            void db_absorb() { }
        };

        static
        db_sync *get_db_sync() { return new db_sync(); }

        static bool need_scatter_merge(unsigned long bsp_phase) {
          return false;
        }

        static void vertex_apply(unsigned char *v,
                                 unsigned char *copy,
                                 unsigned long copy_machine,
                                 per_processor_data *per_cpu_data,
                                 unsigned long bsp_phase) {
          struct bfs_vertex *vtx = (struct bfs_vertex *) v;
          struct bfs_vertex *vtx_cpy = (struct bfs_vertex *) copy;
          bfs_forest_per_processor_data *cpu_data =
              static_cast<bfs_forest_per_processor_data *>(per_cpu_data);
          if (vtx->component > vtx_cpy->component) {
            vtx->component = vtx_cpy->component;
            if (vtx->bfs_parent == (vertex_t) - 1) {
              cpu_data->local_merges++;
            }
            vtx->bfs_parent = vtx_cpy->bfs_parent;
            vtx->bsp_phase = bsp_phase;
          }
        }


        static bool apply_one_update(unsigned char *vertex_state,
                                     unsigned char *update_stream,
                                     per_processor_data *per_cpu_data,
                                     bool local_tile,
                                     unsigned long bsp_phase) {
          struct bfs_update *update = (struct bfs_update *) update_stream;
          unsigned long vindex = x_lib::configuration::map_offset(update->child);
          struct bfs_vertex *vertices = (struct bfs_vertex *) vertex_state;
          if (vertices[vindex].component > update->component) {
            if (local_tile && vertices[vindex].bfs_parent == (vertex_t) - 1) {
              static_cast
                  <bfs_forest_per_processor_data *>(per_cpu_data)->local_merges++;
            }
            vertices[vindex].bfs_parent = update->parent;
            vertices[vindex].component = update->component;
            vertices[vindex].bsp_phase = bsp_phase;
            return true;
          }
          else {
            return false;
          }
        }

        static bool generate_update(unsigned char *vertex_state,
                                    unsigned char *edge_format,
                                    unsigned char *update_stream,
                                    per_processor_data *per_cpu_data,
                                    bool local_tile,
                                    unsigned long bsp_phase) {
          vertex_t src, dst;
          F::read_edge(edge_format, src, dst);
          unsigned long vindex = x_lib::configuration::map_offset(src);
          struct bfs_vertex *vertices = (struct bfs_vertex *) vertex_state;
          if (vertices[vindex].bsp_phase == bsp_phase) {
            struct bfs_update *update = (struct bfs_update *) update_stream;
            update->component = vertices[vindex].component;
            update->parent = src;
            update->child = dst;
            return true;
          }
          else {
            return false;
          }
        }

        static bool init(unsigned char *vertex_state,
                         unsigned long vertex_index,
                         unsigned long bsp_phase,
                         per_processor_data *cpu_state) {
          struct bfs_vertex *vstate = (struct bfs_vertex *) vertex_state;
          vstate->bfs_parent = (vertex_t) - 1;
          vstate->component = vertex_index;
          vstate->bsp_phase = 0;
          return true;
        }

        static bool need_init(unsigned long bsp_phase) {
          return (bsp_phase == 0);
        }

        static per_processor_data *
        create_per_processor_data(unsigned long processor_id,
                                  unsigned long machines) {
          return new bfs_forest_per_processor_data(machines);
        }

        // Unused
        static void preprocessing() {
        }

        static void postprocessing() {
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_FOREST::MERGES "
          << bfs_forest_per_processor_data::merges;
        }

        static unsigned long min_super_phases() {
          return 1;
        }

    };

    unsigned long bfs_forest_per_processor_data::merges = 0;
  }
}
#endif
