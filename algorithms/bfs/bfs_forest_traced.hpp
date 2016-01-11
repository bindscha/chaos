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

#ifndef _BFS_FOREST_TRACED_
#define _BFS_FOREST_TRACED_

#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>

namespace algorithm {
  namespace sg_simple {
    class bfs_forest_traced_per_processor_data : public per_processor_data {
    public:
        static unsigned long min_labels_propagated;
        unsigned long local_min_label;
        unsigned long local_min_labels_propagated;
        unsigned long samples;
        unsigned long print_interval;

        bfs_forest_traced_per_processor_data()
            : local_min_label(-1UL),
              local_min_labels_propagated(0),
              samples(0) {
          print_interval = vm["bfs_forest_traced::print_interval"].as < unsigned
          long > ();
        }

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          unsigned long min_label = -1UL;
          unsigned long propagated = 0;
          for (unsigned long i = 0; i < processors; i++) {
            bfs_forest_traced_per_processor_data *data =
                static_cast<bfs_forest_traced_per_processor_data *>(per_cpu_array[i]);
            if (data->local_min_label < min_label) {
              propagated = data->local_min_labels_propagated;
              min_label = data->local_min_label;
            }
            else if (data->local_min_label == min_label) {
              propagated += data->local_min_labels_propagated;
            }
            data->local_min_label = -1UL;
            data->local_min_labels_propagated = 0;
          }
          min_labels_propagated += propagated;
          if ((++samples) == print_interval) {
            BOOST_LOG_TRIVIAL(info) <<
            "ALGORITHM::BFS_FOREST_TRACED::min_labels_propagated " <<
            min_labels_propagated;
            samples = 0;
          }
          return false;
        }
    }  __attribute__((__aligned__(64)));

    template<typename F>
    class bfs_forest_traced {
    public:
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

        static bool apply_one_update(unsigned char *vertex_state,
                                     unsigned char *update_stream,
                                     per_processor_data *per_cpu_data,
                                     unsigned long bsp_phase) {
          struct bfs_update *update = (struct bfs_update *) update_stream;
          unsigned long vindex = x_lib::configuration::map_offset(update->child);
          struct bfs_vertex *vertices = (struct bfs_vertex *) vertex_state;
          if (vertices[vindex].component > update->component) {
            vertices[vindex].bfs_parent = update->parent;
            vertices[vindex].component = update->component;
            vertices[vindex].bsp_phase = bsp_phase;
            bfs_forest_traced_per_processor_data *data =
                static_cast<bfs_forest_traced_per_processor_data *>(per_cpu_data);
            if (update->component < data->local_min_label) {
              data->local_min_label = update->component;
              data->local_min_labels_propagated = 1;
            }
            else if (update->component == data->local_min_label) {
              data->local_min_labels_propagated++;
            }
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
          vstate->bfs_parent = vertex_index;
          vstate->component = vstate->bfs_parent;
          vstate->bsp_phase = 0;
          return true;
        }

        static bool need_init(unsigned long bsp_phase) {
          return (bsp_phase == 0);
        }

        static per_processor_data *
        create_per_processor_data(unsigned long processor_id) {
          return new bfs_forest_traced_per_processor_data();
        }

        // Unused
        static void preprocessing() {
        }

        static void postprocessing() {
        }

        static unsigned long min_super_phases() {
          return 1;
        }

    };

    unsigned long bfs_forest_traced_per_processor_data::min_labels_propagated = 0;
  }
}
#endif
