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

#ifndef _NOOP_
#define _NOOP_
#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"

namespace algorithm {
namespace sg_simple {

class noop_pcpu : public per_processor_data {
public:
  noop_pcpu() {}
  bool reduce(per_processor_data** per_cpu_array, unsigned long processors)
  {
    return false;
  }
} __attribute__((__aligned__(64)));

template<typename F>
class noop {
private:
  struct vertex {
  } __attribute__((__packed__));

  struct update {
    vertex_t target;
  } __attribute__((__packed__));

public:
  static unsigned long vertex_state_bytes() {
    return sizeof(struct vertex);
  }
  static unsigned long split_size_bytes() {
    return sizeof(struct update);
  }

  static unsigned long split_key(unsigned char* buffer, unsigned long jump) {
    struct update* u = (struct update*)buffer;
    vertex_t key = u->target;
    return key >> jump;
  }

  static unsigned long min_super_phases() {
    return 1;
  }
  static bool need_init(unsigned long bsp_phase) {
    return bsp_phase == 0;
  }

  static bool init(unsigned char* vertex_state,
                   unsigned long vertex_index,
		   unsigned long bsp_phase,
		   per_processor_data* cpu_state)
  {
    struct vertex* vertices = (struct vertex*)vertex_state;
    (void)vertices;
    return true;
  }

  static bool apply_one_update(unsigned char* vertex_state,
                               unsigned char* update_stream,
			       per_processor_data* per_cpu_data,
			       unsigned long bsp_phase)
  {
    struct update* u = (struct update*)update_stream;
    struct vertex* vertices = (struct vertex*)vertex_state;
    struct vertex* v = &vertices[x_lib::configuration::map_offset(u->target)];

    (void)u;
    (void)vertices;
    (void)v;

    return false;
  }

  static bool generate_update(unsigned char* vertex_state,
                              unsigned char* edge_stream,
			      unsigned char* update_stream,
			      per_processor_data* per_cpu_data,
			      unsigned long bsp_phase)
  {
    vertex_t src, dst;
    F::read_edge(edge_stream, src, dst);

    struct vertex* vertices = (struct vertex*)vertex_state;
    struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];
    (void)vertices;
    (void)v;

    struct update* u = (struct update*)update_stream;
    u->target = dst;

    return false;
  }

  static void preprocessing() {}
  static void postprocessing() {}

  static per_processor_data* create_per_processor_data(unsigned long proc_id) {
    return NULL;
  } 
};
    
} // namespace sg_simple
} // namespace algorithm

#endif // _NOOP_
