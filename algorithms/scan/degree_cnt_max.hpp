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

#ifndef _DEGREE_CNT_MAX_
#define _DEGREE_CNT_MAX_
#include "../../core/x-lib.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>
namespace algorithm {
  namespace sg_simple {
    class degree_cnt_pcpu : public per_processor_data {
    public:
      static unsigned long max_degree;
      static vertex_t max_degree_vertex_id;
      unsigned long local_max_degree;
      vertex_t local_max_degree_vertex_id;
      degree_cnt_pcpu() 
	:local_max_degree(0),
	 local_max_degree_vertex_id((vertex_t)-1)
      {}
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	degree_cnt_pcpu* data = static_cast<degree_cnt_pcpu*>(per_cpu_array[0]);
	max_degree = data->local_max_degree;
	max_degree_vertex_id = data->local_max_degree_vertex_id;
	for (unsigned long i=1; i<processors; i++) {
	  data = static_cast<degree_cnt_pcpu*>(per_cpu_array[i]);
	  if (data->local_max_degree > max_degree) {
	    max_degree = data->local_max_degree;
	    max_degree_vertex_id = data->local_max_degree_vertex_id;
	  }
	}
	return false;
      }
    } __attribute__((__aligned__(64)))  ;

    template<typename F>
    class degree_cnt_max {
    public:
      struct __attribute__((__packed__)) degree_cnts {
	unsigned long degree;
      };
    
      static unsigned long split_size_bytes()
      {
	return 0;
      }
    
      static unsigned long split_key(unsigned char *buffer,
				     unsigned long jump)
      {
	return 0;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct degree_cnts);
      }

      static bool apply_one_update(unsigned char *vertex_state,
				   unsigned char *update_stream,
				   per_processor_data *per_cpu_data,
				   unsigned long bsp_phase)
      {
	BOOST_ASSERT_MSG(false, "Should not be called !");
	return false;
      }

      static bool generate_update(unsigned char *vertex_state,
				  unsigned char *edge_format,
				  unsigned char *update_stream,
				  per_processor_data *per_cpu_data,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);
	unsigned long vindex = x_lib::configuration::map_offset(src);
	struct degree_cnts *v = (struct degree_cnts *)vertex_state;
	if (bsp_phase == 0) {
	  v[vindex].degree++;
	}
	return false;
      }

      static bool init(unsigned char * vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct degree_cnts * dc = (struct degree_cnts *)vertex_state;
	if (bsp_phase == 0) {
	  dc->degree = 0;
	  return true;
	} else if (bsp_phase == 1) {
	  degree_cnt_pcpu* pcpu = static_cast<degree_cnt_pcpu*>(cpu_state);
	  if (dc->degree > pcpu->local_max_degree) {
	    pcpu->local_max_degree = dc->degree;
	    pcpu->local_max_degree_vertex_id = vertex_index;
	  }
	}
	return false;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase < 2);
      }

      static void preprocessing() {}
      static void postprocessing() {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::DEGREE_CNT::MAX_DEGREE " 
				<< degree_cnt_pcpu::max_degree;
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::DEGREE_CNT::VERTEX_WITH_MAX_DEGREE " 
				<< degree_cnt_pcpu::max_degree_vertex_id;

      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return new degree_cnt_pcpu();
      }

      static unsigned long min_super_phases()
      {
	return 2;
      }
    };

    unsigned long degree_cnt_pcpu::max_degree = 0;
    vertex_t degree_cnt_pcpu::max_degree_vertex_id = (vertex_t)-1;
  }
}

#endif
