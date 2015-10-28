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

#ifndef _BFS_
#define _BFS_
#include "../../utils/per_cpu_data.hpp"
#include "../../core/autotuner.hpp"
#include "../../utils/options_utils.h"
#include "../../utils/boost_log_wrapper.h"
#include<errno.h>
#include<string>
namespace algorithm {
  namespace sg_simple {  
    class bfs_per_processor_data:public per_processor_data {
    public:
      static unsigned long vertices_discovered;
      static unsigned long edges_explored;
      unsigned long local_vertices_discovered;
      unsigned long local_edges_explored;
      bfs_per_processor_data(unsigned long machines_in) 
	:local_vertices_discovered(0),
	 local_edges_explored(0)
      {
      }
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	for(unsigned long i=0;i<processors;i++) {
	  bfs_per_processor_data * data = 
	    static_cast<bfs_per_processor_data *>(per_cpu_array[i]);
	  vertices_discovered += data->local_vertices_discovered;
	  data->local_vertices_discovered = 0;
	  edges_explored += data->local_edges_explored;
	  data->local_edges_explored = 0;
	}
	return false;
      }
    }  __attribute__((__aligned__(64)))  ;

    template<typename F>
    class bfs {
      static unsigned long bfs_root;
    public:
      static
      bool need_data_barrier()
      {
	return false;
      }

      static unsigned long checkpoint_size()
      {
	return 2*sizeof(unsigned long);
      }

      static void take_checkpoint(unsigned char* buffer,
				  per_processor_data **per_cpu_array,
				  unsigned long processors)
      {
	bfs_per_processor_data * data = 
	  static_cast<bfs_per_processor_data *>(per_cpu_array[0]);
	(void)data->reduce(per_cpu_array, processors);
	memcpy(buffer, &bfs_per_processor_data::vertices_discovered, sizeof(unsigned long));
	buffer += sizeof(unsigned long);
	memcpy(buffer, &bfs_per_processor_data::edges_explored, sizeof(unsigned long));
      }

      static void restore_checkpoint(unsigned char* buffer,
				     per_processor_data **per_cpu_array,
				     unsigned long processors)
      {
	memcpy(&bfs_per_processor_data::vertices_discovered, buffer, sizeof(unsigned long));
	buffer += sizeof(unsigned long);
	memcpy(&bfs_per_processor_data::edges_explored, buffer, sizeof(unsigned long));
      }
      
      class db_sync {
      public:
	void prep_db_data(per_processor_data **pcpu_array,
			  unsigned long me,
			  unsigned long processors)
	{}

	void finalize_db_data(per_processor_data **pcpu_array,
			      unsigned long me,
			      unsigned long processors)
	{}
	
	unsigned char *db_buffer() 
	{return NULL;}

	unsigned long db_size()
	{ return 0;}

	void db_generate()
	{}

	void db_merge()
	{}

	void db_absorb()
	{}
      };
      
      static
      db_sync * get_db_sync()
      {
	return NULL;
      }

      struct __attribute__((__packed__)) bfs_update {
	vertex_t parent;
	vertex_t child;
      };
      struct __attribute__((__packed__)) bfs_vertex {
	vertex_t bfs_parent;
	vertex_t bsp_phase;
      };
    
      static unsigned long split_size_bytes()
      {
	return sizeof(struct bfs_update);
      }
      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct bfs_vertex);
      }
      
      
      static unsigned long split_key(unsigned char *buffer,
				     unsigned long jump)
      {
	struct bfs_update *update = (struct bfs_update *)buffer;
	vertex_t key = update->child;
	key = key >> jump;
	return key;
      }
      static bool apply_one_update(unsigned char *vertex_state,
				   unsigned char *update_stream,
				   per_processor_data *per_cpu_data,
				   bool local_tile,
				   unsigned long bsp_phase)
      {
	struct bfs_update *update = (struct bfs_update *)update_stream;
	unsigned long vindex = 
	  x_lib::configuration::map_offset(update->child);
	struct bfs_vertex *vertices = (struct bfs_vertex *)vertex_state;
	if(vertices[vindex].bfs_parent == (vertex_t)-1) {
	  if(local_tile) {
	    static_cast<bfs_per_processor_data*>
	      (per_cpu_data)->local_vertices_discovered++;
	  }
	  vertices[vindex].bfs_parent = update->parent;
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
				  unsigned long bsp_phase)
      {
	vertex_t src, dst;
	F::read_edge(edge_format, src, dst);
	unsigned long vindex = x_lib::configuration::map_offset(src);
	struct bfs_vertex *vertices = (struct bfs_vertex *)vertex_state;
	if(vertices[vindex].bfs_parent != (vertex_t)-1 && 
	   vertices[vindex].bsp_phase == bsp_phase) {
	  struct bfs_update * update = (struct bfs_update *)update_stream;
	  update->parent = src;
	  update->child = dst;
	  static_cast
	    <bfs_per_processor_data *>(per_cpu_data)->local_edges_explored++;
	  return true;
	}
	else {
	  return false;
	}
      }

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
	struct bfs_vertex *vtx     = (struct bfs_vertex *)v;
	struct bfs_vertex *vtx_cpy = (struct bfs_vertex *)copy;
	bfs_per_processor_data * cpu_data = 
	  static_cast<bfs_per_processor_data *>(per_cpu_data);
	if(vtx->bfs_parent == (vertex_t)-1 && 
	   vtx_cpy->bfs_parent != (vertex_t)-1) {
	  vtx->bfs_parent = vtx_cpy->bfs_parent;
	  vtx->bsp_phase  = bsp_phase;
	  cpu_data->local_vertices_discovered++;
	}
      }

      static bool init(unsigned char * vertex_state,
		       unsigned long vertex_index,
		       unsigned long bsp_phase,
		       per_processor_data *cpu_state)
      {
	bool will_start = false;
	struct bfs_vertex *vstate = (struct bfs_vertex *)vertex_state;
	vstate->bfs_parent = (vertex_t)-1;
	if(bfs_root == vertex_index) {
	  will_start = true;
	  vstate->bfs_parent = bfs_root;
	  vstate->bsp_phase = 0;
	}
	return will_start;
      }

      static bool need_init(unsigned long bsp_phase)
      {
	return (bsp_phase == 0);
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id,
				unsigned long machines)
      {
	return new bfs_per_processor_data(machines);
      }

      static void preprocessing()
      {
	bfs_root = vm["bfs::root"].as<unsigned long>();
      }

      static void postprocessing() 
      {
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS::VERTICES_DISCOVERED " 
				<< bfs_per_processor_data::vertices_discovered;
	BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS::EDGES_EXPLORED " 
				<< bfs_per_processor_data::edges_explored;
      }
    
      static unsigned long min_super_phases()
      {
	return 1;
      }
    };
  
    template<typename F>
    unsigned long bfs<F>::bfs_root = ULONG_MAX;
    unsigned long bfs_per_processor_data::vertices_discovered = 0;
    unsigned long bfs_per_processor_data::edges_explored = 0;
  
  }
}
#endif
