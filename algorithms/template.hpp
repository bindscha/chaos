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

#ifndef _XXX_
#define _XXX_

namespace algorithm {
  
  template <typename F>
  class XXX {
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

    static unsigned long split_key(unsigned char* buffer, unsigned long jump)
    {
      struct update* u = (struct update*)buffer;
      vertex_t key = u->target;
      key = key >> jump;
      return key;
    }
    
    static bool need_init(unsigned long bsp_phase)
    {
      return (bsp_phase == 0);
    }

    // Return true if the partition partition_id should be active in the first scatter phase
    static bool init(unsigned char* vertex_state,
                     unsigned long global_partition_id,
                     unsigned long vertex_cnt,
		     unsigned long bsp_phase)
    {
      struct vertex* vertices = (struct vertex*)vertex_state;
      for (unsigned long i = 0; i < vertex_cnt; ++i) {
        vertices[i]
      }

      return true;
    }

    // Return true if the vertex should generate an update in the next phase 
    // true here means that the vertex's partition will be active in the next scatter phase
    static bool apply_one_update(unsigned char* vertex_state,
                                 unsigned char* update_stream,
				 per_processor_data* per_cpu_data,
                                 unsigned long bsp_phase)
    {
      struct update* u = (struct update*)update_stream;
      struct vertex* vertices = (struct vertex*)vertex_state;
      struct vertex* v = &vertices[ce_bsp::map_vertex_to_pindex(u->target)];

      return true;
    }

    // Return true if update generated
    static bool generate_update(unsigned char* vertex_state,
                                unsigned char* edge_format,
                                unsigned char* update_stream,
				per_processor_data* per_cpu_data,
                                unsigned long bsp_phase)
    {
      unsigned long src, dst;
      weight_t value;
      F::read_edge(edge_format, src, dst, value);

      struct vertex* vertices = (struct vertex*)vertex_state;
      struct vertex* v = &vertices[ce_bsp::map_vertex_to_pindex(src)];

      struct update* u = (struct update*)update_stream;
      u->target = dst;

      return true;
    }

    static per_processor_data* create_per_processor_data(unsigned long processor_id)
    {
      return NULL;
    }

    static unsigned long min_super_phases()
    {
      return 1;
    }

    static void postprocessing() {}

  };
}

#endif
