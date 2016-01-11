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

#ifndef _CONDUCTANCE_
#define _CONDUCTANCE_

#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>

namespace algorithm {
  namespace sg_simple {
    // Assumes a partition between even numbered vertices (black) and 
    // odd numbered vertices (red)
    struct mc_cond {
        unsigned long ce;
        unsigned long re;
        unsigned long be;
    };

    class conductance_per_processor_data : public per_processor_data {
    public:
        static unsigned long crossover_edges;
        unsigned long crossover_edges_local;
        static unsigned long edges_from_red;
        unsigned long edges_from_red_local;
        static unsigned long edges_from_black;
        unsigned long edges_from_black_local;
        mc_cond *mc_cond_array;
        unsigned long machines;
        static bool do_reduce;

        conductance_per_processor_data(unsigned long machines_in)
            : crossover_edges_local(0),
              edges_from_red_local(0),
              edges_from_black_local(0),
              machines(machines_in) {
          mc_cond_array = new mc_cond[machines];
          memset(mc_cond_array, 0, machines * sizeof(mc_cond));
        }

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          if (do_reduce) {
            for (unsigned long i = 0; i < processors; i++) {
              conductance_per_processor_data *data =
                  static_cast<conductance_per_processor_data *>(per_cpu_array[i]);
              crossover_edges += data->crossover_edges_local;
              data->crossover_edges_local = 0;
              edges_from_red += data->edges_from_red_local;
              data->edges_from_red_local = 0;
              edges_from_black += data->edges_from_black_local;
              data->edges_from_black_local = 0;
            }
          }
          else { // reduce on second pass
            do_reduce = true;
          }
          return false;
        }
    }  __attribute__((__aligned__(64)));


    template<typename F>
    class conductance {
        static bool is_red(vertex_t v) {
          return ((v & 1) != 0);
        }

        static bool different(vertex_t a, vertex_t b) {
          return (((a ^ b) & 1) != 0);
        }

    public:

        static unsigned long checkpoint_size() {
          return 3 * sizeof(unsigned long);
        }

        static void take_checkpoint(unsigned char *buffer,
                                    per_processor_data **per_cpu_array,
                                    unsigned long processors) {
          conductance_per_processor_data *data =
              static_cast<conductance_per_processor_data *>(per_cpu_array[0]);
          memcpy(buffer, &data->edges_from_red_local, sizeof(unsigned long));
          buffer += sizeof(unsigned long);
          memcpy(buffer, &data->edges_from_black_local, sizeof(unsigned long));
          buffer += sizeof(unsigned long);
          memcpy(buffer, &data->crossover_edges_local, sizeof(unsigned long));
        }

        static void restore_checkpoint(unsigned char *buffer,
                                       per_processor_data **per_cpu_array,
                                       unsigned long processors) {
          memcpy(&conductance_per_processor_data::edges_from_red,
                 buffer,
                 sizeof(unsigned long));
          buffer += sizeof(unsigned long);
          memcpy(&conductance_per_processor_data::edges_from_black,
                 buffer,
                 sizeof(unsigned long));
          buffer += sizeof(unsigned long);
          memcpy(&conductance_per_processor_data::crossover_edges,
                 buffer,
                 sizeof(unsigned long));
        }

        static
        bool need_data_barrier() {
          return true;
        }

        class db_sync {
            mc_cond *mc_cond_array;
            mc_cond *io;
            unsigned long machines;
        public:
            void prep_db_data(per_processor_data **pcpu_array,
                              unsigned long me,
                              unsigned long processors) {
              for (unsigned long i = 0; i < processors; i++) {
                conductance_per_processor_data *cpu_data =
                    static_cast <conductance_per_processor_data *>(pcpu_array[i]);
                if (i == 0) {
                  mc_cond_array = new mc_cond[cpu_data->machines];
                  io = new mc_cond[cpu_data->machines];
                  machines = cpu_data->machines;
                  for (unsigned long j = 0; j < cpu_data->machines; j++) {
                    memset(&mc_cond_array[j], 0, sizeof(mc_cond));
                  }
                }
                for (unsigned long j = 0; j < cpu_data->machines; j++) {
                  mc_cond_array[j].ce += cpu_data->crossover_edges_local;
                  mc_cond_array[j].re += cpu_data->edges_from_red_local;
                  mc_cond_array[j].be += cpu_data->edges_from_black_local;
                }
                cpu_data->crossover_edges_local = 0;
                cpu_data->edges_from_red_local = 0;
                cpu_data->edges_from_black_local = 0;
              }
            }

            void finalize_db_data(per_processor_data **pcpu_array,
                                  unsigned long me,
                                  unsigned long processors) {
              conductance_per_processor_data *cpu_data =
                  static_cast <conductance_per_processor_data *>(pcpu_array[0]);
              cpu_data->crossover_edges_local = mc_cond_array[me].ce;
              cpu_data->edges_from_red_local = mc_cond_array[me].re;
              cpu_data->edges_from_black_local = mc_cond_array[me].be;
            }

            unsigned char *db_buffer() { return (unsigned char *) io; }

            unsigned long db_size() { return machines * sizeof(mc_cond); }

            void db_generate() { memcpy(io, mc_cond_array, db_size()); }

            void db_merge() {
              for (unsigned long i = 0; i < machines; i++) {
                io[i].ce += mc_cond_array[i].ce;
                io[i].re += mc_cond_array[i].re;
                io[i].be += mc_cond_array[i].be;
              }
            }

            void db_absorb() {
              for (unsigned long i = 0; i < machines; i++) {
                mc_cond_array[i].ce = io[i].ce;
                mc_cond_array[i].re = io[i].re;
                mc_cond_array[i].be = io[i].be;
              }
            }

            ~db_sync() {
              delete io;
              delete mc_cond_array;
            }

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
          // Nothing
        }

        static unsigned long split_size_bytes() {
          return 1;
        }

        static unsigned long split_key(unsigned char *buffer,
                                       unsigned long jump) {
          return 0;
        }

        static unsigned long vertex_state_bytes() {
          return 0;
        }

        static bool apply_one_update(unsigned char *vertex_state,
                                     unsigned char *update_stream,
                                     per_processor_data *per_cpu_data,
                                     bool local_tile,
                                     unsigned long bsp_phase) {
          BOOST_ASSERT_MSG(false, "Should not be called !");
          return false;
        }

        static bool generate_update(unsigned char *vertex_state,
                                    unsigned char *edge_format,
                                    unsigned char *update_stream,
                                    per_processor_data *per_cpu_data,
                                    bool local_tile,
                                    unsigned long bsp_phase) {
          vertex_t src, dst;
          if (bsp_phase == 0) {
            F::read_edge(edge_format, src, dst);
            conductance_per_processor_data *data =
                static_cast<conductance_per_processor_data *>(per_cpu_data);
            if (is_red(src)) {
              data->edges_from_red_local++;
            }
            else {
              data->edges_from_black_local++;
            }
            if (different(src, dst)) {
              data->crossover_edges_local++;
            }
          }
          return false;
        }

        static bool init(unsigned char *vertex_state,
                         unsigned long vertex_index,
                         unsigned long bsp_phase,
                         per_processor_data *cpu_state) {
          return true;
        }

        static bool need_init(unsigned long bsp_phase) {
          return (bsp_phase == 0);
        }

        static void postprocessing() {
          unsigned long base_edges;
          if (conductance_per_processor_data::edges_from_red >
              conductance_per_processor_data::edges_from_black) {
            base_edges = conductance_per_processor_data::edges_from_red;
          }
          else {
            base_edges = conductance_per_processor_data::edges_from_black;
          }
          weight_t conductance_value =
              ((weight_t) conductance_per_processor_data::crossover_edges) /
              base_edges;
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::CONDUCTANCE::VALUE "
          << conductance_value;
        }

        static void preprocessing() { }

        static per_processor_data *
        create_per_processor_data(unsigned long processor_id,
                                  unsigned long machines) {
          return new conductance_per_processor_data(machines);
        }

        static unsigned long min_super_phases() {
          return 1;
        }
    };

    bool conductance_per_processor_data::do_reduce = false;
    unsigned long conductance_per_processor_data::crossover_edges = 0;
    unsigned long conductance_per_processor_data::edges_from_red = 0;
    unsigned long conductance_per_processor_data::edges_from_black = 0;
  }
}

#endif
