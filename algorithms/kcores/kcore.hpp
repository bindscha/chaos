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

#ifndef __KCORE__
#define __KCORE__

#include "../../core/sg_driver.hpp"

namespace algorithm {
  namespace sg_simple {
    class kcore_per_processor_data : public per_processor_data {
    public:
        static vertex_t max_kcore;
        vertex_t local_max_kcore;
        bool converged;

        kcore_per_processor_data()
            : local_max_kcore(0),
              converged(true) { }

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          max_kcore = 0;
          bool global_convergence = true;
          for (unsigned long i = 0; i < processors; i++) {
            kcore_per_processor_data *data =
                static_cast<kcore_per_processor_data *>(per_cpu_array[i]);
            if (data->local_max_kcore > max_kcore) {
              max_kcore = data->local_max_kcore;
            }
            global_convergence = global_convergence && data->converged;
            data->local_max_kcore = 0;
            data->converged = true; // redundant.
          }
          return global_convergence;
        }
    }  __attribute__((__aligned__(64)));

    vertex_t kcore_per_processor_data::max_kcore = 0;

    template<typename F>
    class kcore {
    private:

        struct vertex {
            bool uptodate;
            vertex_t estimated_kcore;
            vertex_t supporting_kcore;
            vertex_t supporting_count;
        } __attribute__((__packed__));

        struct update {
            vertex_t target;
            vertex_t kcore;
        } __attribute__((__packed__));

    public:

        static unsigned long vertex_state_bytes() {
          return sizeof(struct vertex);
        }

        static unsigned long split_size_bytes() {
          return sizeof(struct update);
        }

        static unsigned long split_key(unsigned char *buffer, unsigned long jump) {
          struct update *u = (struct update *) buffer;
          vertex_t key = u->target;
          key = key >> jump;
          return key;
        }

        static bool init(unsigned char *vertex_state,
                         unsigned long vertex_index,
                         unsigned long bsp_phase,
                         per_processor_data *cpu_state) {
          struct vertex *vertex = (struct vertex *) vertex_state;
          if (bsp_phase == 0) {
            vertex->estimated_kcore = 0;
          }
          else {
            vertex->uptodate = false;
            vertex->supporting_kcore = 0;
            vertex->supporting_count = 0;
          }
          return true;
        }

        static bool need_init(unsigned long bsp_phase) {
          return true;
        }

        static void preprocessing() {
        }

        static bool apply_one_update(unsigned char *vertex_state,
                                     unsigned char *update_stream,
                                     per_processor_data *per_cpu_data,
                                     unsigned long bsp_phase) {
          struct update *u = (struct update *) update_stream;
          struct vertex *vertices = (struct vertex *) vertex_state;
          struct vertex *v = &vertices[x_lib::configuration::map_offset(u->target)];

          vertex_t support = u->kcore;
          // Get maximum support, capped by count
          if (support > v->estimated_kcore) {
            support = v->estimated_kcore;
          }
          if (support > v->supporting_kcore) {
            v->supporting_kcore = support;
            v->supporting_count = 1;
          }
          else if (support == v->supporting_kcore) {
            v->supporting_count++;
          }
          return true; // Always scatter
        }

        static bool generate_update(unsigned char *vertex_state,
                                    unsigned char *edge_format,
                                    unsigned char *update_stream,
                                    per_processor_data *per_cpu_data,
                                    unsigned long bsp_phase) {
          vertex_t src, dst;
          F::read_edge(edge_format, src, dst);
          kcore_per_processor_data *cpu =
              static_cast<kcore_per_processor_data *>(per_cpu_data);
          struct vertex *vertices = (struct vertex *) vertex_state;
          struct vertex *v = &vertices[x_lib::configuration::map_offset(src)];

          if (bsp_phase == 0) {
            v->estimated_kcore++;
            return false;
          }
          else {
            // Update kcore estimate on first edge
            if (!v->uptodate && bsp_phase > 1) {
              bool connected_is_supported =
                  (v->supporting_kcore <= v->supporting_count);
              if (v->estimated_kcore == v->supporting_kcore
                  && connected_is_supported) {
                // all is well.
              }
              else {
                cpu->converged = false;
                if (connected_is_supported) {
                  v->estimated_kcore = v->supporting_kcore;
                }
                else {
                  v->estimated_kcore = v->supporting_kcore - 1;
                }
              }
              v->uptodate = true;
            }
            if (v->estimated_kcore > cpu->local_max_kcore) {
              cpu->local_max_kcore = v->estimated_kcore;
            }
            update *u = (update *) update_stream;
            u->target = dst;
            u->kcore = v->estimated_kcore;
            return true;
          }
        }

        static per_processor_data *create_per_processor_data(unsigned long processor_id) {
          return new kcore_per_processor_data();
        }

        static unsigned long min_super_phases() {
          return 2;
        }

        static void postprocessing() {
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::MAX_KCORE "
          << kcore_per_processor_data::max_kcore;
        }

    };
  }
}
#endif
