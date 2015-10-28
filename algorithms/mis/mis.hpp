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

#ifndef _MIS_
#define _MIS_
#include "../../core/x-lib.hpp"

namespace algorithm {
  namespace sg_simple {
    class mis_per_processor_data:public per_processor_data {
    public:
      static unsigned long not_in_mis;
      unsigned long voted_out;
      unsigned long machines;
      mis_per_processor_data(unsigned long machines_in) 
	:voted_out(0),
	 machines(machines_in)
      {
      }
      
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	unsigned long total_not_in_mis = 0;
	for(unsigned long i=0;i<processors;i++) {
	  mis_per_processor_data * data = 
	    static_cast<mis_per_processor_data *>(per_cpu_array[i]);
	  total_not_in_mis += data->voted_out;
	  data->voted_out = 0;
	}
	bool stop = (total_not_in_mis == not_in_mis);
	not_in_mis = total_not_in_mis;
	return stop;
      }
    }  __attribute__((__aligned__(64)))  ;

  
    template <typename F>
    class mis {
    private:
      struct vertex {
	bool in_mis;
      } __attribute__((__packed__));
    
      struct update {
	vertex_t target;
      } __attribute__((__packed__));

    public:

      static unsigned long checkpoint_size()
      {
	return 2*sizeof(unsigned long);
      }

      static void take_checkpoint(unsigned char* buffer,
				  per_processor_data **per_cpu_array,
				  unsigned long processors)
      {
	mis_per_processor_data * data = 
	    static_cast<mis_per_processor_data *>(per_cpu_array[0]);
	memcpy(buffer, &data->voted_out, sizeof(unsigned long));
	buffer += sizeof(unsigned long);
	memcpy(buffer, &mis_per_processor_data::not_in_mis, sizeof(unsigned long));
      }

      static void restore_checkpoint(unsigned char* buffer,
				     per_processor_data **per_cpu_array,
				     unsigned long processors)
      {
	mis_per_processor_data * data = 
	  static_cast<mis_per_processor_data *>(per_cpu_array[0]);
	memcpy(&data->voted_out, buffer, sizeof(unsigned long));
	buffer += sizeof(unsigned long);
	memcpy(&mis_per_processor_data::not_in_mis, buffer, sizeof(unsigned long));
      }
    
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

       static
      bool need_data_barrier()
      {
	return true;
      }

      class db_sync 
      {
	unsigned long *mc_voted_out ;
	unsigned long *io;
	unsigned long machines;
      public:
	void prep_db_data(per_processor_data **pcpu_array,
			  unsigned long me,
			  unsigned long processors)
	{
	  for(unsigned long i=0;i<processors;i++) {
	    mis_per_processor_data *cpu_data = 
	      static_cast <mis_per_processor_data *>(pcpu_array[i]);
	    if(i == 0) {
	      mc_voted_out = new unsigned long[cpu_data->machines];
	      io = new unsigned long[cpu_data->machines];
	      machines = cpu_data->machines;
	      for(unsigned long j=0;j<cpu_data->machines;j++) {
		mc_voted_out[j] = 0;
	      }
	    }
	    for(unsigned long j=0;j<cpu_data->machines;j++) {
	      mc_voted_out[j]     += cpu_data->voted_out;
	    }
	    cpu_data->voted_out  = 0;
	  }
	}
	
	void finalize_db_data(per_processor_data **pcpu_array,
			      unsigned long me,
			      unsigned long processors)
	{
	  mis_per_processor_data *cpu_data = 
	    static_cast <mis_per_processor_data *>(pcpu_array[0]);
	  cpu_data->voted_out = mc_voted_out[me];
	}

	unsigned char *db_buffer() 
	{return (unsigned char *)io;}

	unsigned long db_size()
	{ return machines*sizeof(unsigned long);}

	void db_generate()
	{ memcpy(io, mc_voted_out, db_size());}

	void db_merge()
	{
	  for(unsigned long i=0;i<machines;i++) {
	    io[i] += mc_voted_out[i];
	  }
	}

	void db_absorb()
	{
	  for(unsigned long i=0;i<machines;i++) {
	    mc_voted_out[i] = io[i];
	  }
	}

	~db_sync()
	{
	  delete io;
	  delete mc_voted_out;
	}
      };
      
      static
      db_sync * get_db_sync() {return new db_sync();}

      static bool need_scatter_merge(unsigned long bsp_phase)
      {
	return false;
      }

      static void vertex_apply(unsigned char * v,
			       unsigned char * copy,
			       unsigned long copy_machine,
			       per_processor_data *per_cpu_data,
			       unsigned long bsp_phase)
      {
	struct vertex *vtx     = (struct vertex *)v;
	struct vertex *vtx_cpy = (struct vertex *)copy;
	mis_per_processor_data * cpu_data = 
	  static_cast<mis_per_processor_data *>(per_cpu_data);
	if((vtx->in_mis) && (!vtx_cpy->in_mis)) {
	  vtx->in_mis     = false;
	  cpu_data->voted_out++;
	}
      }

      static bool init(unsigned char* vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	struct vertex* vertices = (struct vertex*)vertex_state;
	vertices->in_mis = true;
	return true;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return true;
      }

      static bool apply_one_update(unsigned char* vertex_state,
				   unsigned char* update_stream,
				   per_processor_data* per_cpu_data,
				   bool local_tile,
				   unsigned long bsp_phase)
      {
	struct update * updt = (struct update *)update_stream;
	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(updt->target)];
	if(v->in_mis) {
	  if(local_tile) {
	    static_cast
	      <mis_per_processor_data *>(per_cpu_data)->voted_out++;
	  }
	  v->in_mis = false;
	}
	return true;
      }

      static bool generate_update(unsigned char* vertex_state,
				  unsigned char* edge_format,
				  unsigned char* update_stream,
				  per_processor_data* per_cpu_data,
				  bool local_tile,
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);

	struct vertex* vertices = (struct vertex*)vertex_state;
	struct vertex* v = &vertices[x_lib::configuration::map_offset(src)];

	if ((v->in_mis) && (src < dst)) {
	  struct update * updt = (struct update *)update_stream;
	  updt->target = dst;
	  return true;
	}
	else {
	  return false;
	}
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id,
				unsigned long machines)
      {
	return new mis_per_processor_data(machines);
      }

      static unsigned long min_super_phases()
      {
	return 1;
      }

      static void preprocessing()
      {
      }

      static void postprocessing() 
      {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::MIS::NOT_IN_MIS " 
				<< mis_per_processor_data::not_in_mis;
      }

    };
    unsigned long mis_per_processor_data::not_in_mis = 0;
  }
}
#endif
