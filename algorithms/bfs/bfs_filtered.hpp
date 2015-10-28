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

#ifndef _BFS_FILTERED_
#define _BFS_FILTERED_
#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// BFS with edge filtering and direction reversal

namespace algorithm {
  namespace bfs {
    const static unsigned long step_split_edges         = 0;
    const static unsigned long step_updates_in          = 1;
    const static unsigned long step_updates_out         = 2;
    const static unsigned long step_filter_edges        = 3;
    const static unsigned long step_bounce_fwd          = 4;
    const static unsigned long step_bounce_bck          = 5; 

    const static vertex_t SENTINEL = (vertex_t)ULONG_MAX;

    struct bfs_filt_pcpu:public per_processor_data {
      unsigned long processor_id;
      unsigned long wasted_local;
      unsigned long wasted_updates_internal_local;
      unsigned long wasted_updates_sibling_local;
      unsigned long total_updates_local;
      unsigned long vertices_discovered_local;
      unsigned long edges_explored_local;
      const unsigned long num_processors;
      bool activate_partition_for_scatter;
      unsigned long horizon_size_local;
      /* begin work specs. */
      static x_lib::filter *scatter_filter;
      static unsigned long current_step;
      static unsigned long bfs_iteration;
      static unsigned long wasted_edges;
      static unsigned long total_edges;
      static unsigned long vertices_discovered;
      static unsigned long edges_explored;
      static unsigned long total_updates;
      static unsigned long wasted_updates_internal;
      static unsigned long wasted_updates_sibling;
      static unsigned long horizon_size;

      /* end work specs. */
      bool reduce(per_processor_data **per_cpu_array,
		  unsigned long processors)
      {
	if(current_step == step_updates_out) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    bfs_filt_pcpu *cpu    = static_cast<bfs_filt_pcpu *>(per_cpu_array[i]);
	    wasted_edges         += cpu->wasted_local;
	    cpu->wasted_local     = 0;
	    vertices_discovered  += cpu->vertices_discovered_local;
	    edges_explored       += cpu->edges_explored_local;
	    cpu->vertices_discovered_local = 0;
	    cpu->edges_explored_local = 0;
	  }
	}
	else if (current_step == step_updates_in) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    bfs_filt_pcpu *cpu    = 
	      static_cast<bfs_filt_pcpu *>(per_cpu_array[i]);
	    wasted_updates_sibling += cpu->wasted_updates_sibling_local;
	    cpu->wasted_updates_sibling_local = 0;
	    wasted_updates_internal += cpu->wasted_updates_internal_local;
	    cpu->wasted_updates_internal_local = 0;
	    total_updates += cpu->total_updates_local;
	    cpu->total_updates_local = 0;
	    horizon_size += cpu->horizon_size_local;
	    cpu->horizon_size_local = 0;
	  }
	}
	else if(current_step == step_filter_edges) {
	  for(unsigned long i=0;i<num_processors;i++) {
	    bfs_filt_pcpu *cpu = static_cast<bfs_filt_pcpu *>(per_cpu_array[i]);
	    total_edges       -= cpu->wasted_local; // eliminated in this pass
	    cpu->wasted_local  = 0;
	  }
	}
	return false; 
      }
      bfs_filt_pcpu(unsigned long processor_id_in,
		    unsigned long num_processors_in)
	:processor_id(processor_id_in),
	 wasted_local(0),
	 wasted_updates_internal_local(0),
	 wasted_updates_sibling_local(0),
	 vertices_discovered_local(0),
	 edges_explored_local(0),
	 num_processors(num_processors_in),
	 activate_partition_for_scatter(false),
	 horizon_size_local(0)
      {
      }
    } __attribute__((__aligned__(64)));

    struct __attribute__((__packed__)) bfs_filt_vertex {
      vertex_t bfs_parent;
      unsigned long update_iteration;
    };

    struct __attribute__((__packed__)) bfs_filt_update {
      vertex_t src;
      vertex_t dst;
    };

    template<typename F>
    class bfs_filt {
      static bfs_filt_pcpu ** pcpu_array;
      static unsigned long bfs_root;
      bool heartbeat;
      x_lib::streamIO<bfs_filt> *graph_storage;
      unsigned long vertex_stream;
      unsigned long init_stream;
      unsigned long edges_stream0;
      unsigned long edges_stream1;
      unsigned long messages_stream0;
      unsigned long messages_stream1;
      rtc_clock wall_clock;
      rtc_clock setup_time;
      float bfs_filt_threshold;
      bool mode_rev_bfs;
    public:
      bfs_filt();
      static void partition_pre_callback(unsigned long super_partition,
					 unsigned long partition,
					 per_processor_data* cpu_state);
      static void partition_callback(x_lib::stream_callback_state *state);
      static void partition_post_callback(unsigned long super_partition,
					  unsigned long partition,
					  per_processor_data *cpu_state);
      
      void operator() ();
      
      static unsigned long max_streams()
      {
	return 6; 
      }
      
      static unsigned long max_buffers()
      {
	return 5;
      }

      static unsigned long vertex_state_bytes()
      {
	return sizeof(struct bfs_filt_vertex);
      }

      static unsigned long vertex_stream_buffer_bytes()
      {
	return sizeof(bfs_filt_vertex) + 
	  MAX(F::split_size_bytes(), sizeof(bfs_filt_update));
      }
    
      static void state_iter_callback(unsigned long superp, 
				      unsigned long partition,
				      unsigned long index,
				      unsigned char *vertex,
				      per_processor_data *cpu_state)
      {
	bfs_filt_vertex *v = (bfs_filt_vertex *)vertex;	
	v->bfs_parent = SENTINEL;
	v->update_iteration = 0; // Don't care
	unsigned long global_index = 
	  x_lib::configuration::map_inverse(superp, partition, index);
	if(global_index == bfs_root) {
	  v->bfs_parent = global_index;
	  bfs_filt_pcpu::scatter_filter->q(partition);
	}
      }

      static per_processor_data * 
      create_per_processor_data(unsigned long processor_id)
      {
	return pcpu_array[processor_id];
      }
  
      static void do_cpu_callback(per_processor_data *cpu_state)
      {
	bfs_filt_pcpu *cpu = static_cast<bfs_filt_pcpu *>(cpu_state);
	bfs_filt_pcpu::scatter_filter->done(cpu->processor_id);
      }

      void state_store(unsigned long superp)
      {
	if(graph_storage->get_config()->super_partitions > 1) {
	  graph_storage->state_store(vertex_stream, superp);
	}
      }

      void state_load(bool first_touch, unsigned long superp)
      {
	if(graph_storage->get_config()->super_partitions > 1) {
	  if(!first_touch) {
	    graph_storage->state_load(vertex_stream, superp);
	  }
	  graph_storage->state_prepare(superp);
	}
	else {
	  if(first_touch) {
	    graph_storage->state_prepare(superp);
	  }
	}
      }
    };
  
    template<typename F>
    bfs_filt<F>::bfs_filt()
    {
      wall_clock.start();
      setup_time.start();
      bfs_root = vm["bfs::root"].as<unsigned long>();
      bfs_filt_threshold = vm["bfs_filt::filter_threshold"].as<float>();
      heartbeat = (vm.count("heartbeat") > 0);
      unsigned long num_processors = vm["processors"].as<unsigned long>();
      pcpu_array = new bfs_filt_pcpu *[num_processors];
      for(unsigned long i=0;i<num_processors;i++) {
	pcpu_array[i] = new bfs_filt_pcpu(i, num_processors);
      }
      graph_storage = new x_lib::streamIO<bfs_filt>();
      vertex_stream = 
	graph_storage->open_stream("vertices", true, 
				   vm["vertices_disk"].as<unsigned long>(),
				   graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = 
	graph_storage->open_stream((const char *)efile.c_str(), false,
				   vm["input_disk"].as<unsigned long>(),
				   F::split_size_bytes(), 1);
      edges_stream0 = 
	graph_storage->open_stream("edges0", true,
				   vm["edges_disk"].as<unsigned long>(),
				   F::split_size_bytes());
      edges_stream1 = 
	graph_storage->open_stream("edges1", true,
				   vm["edges_disk"].as<unsigned long>(),
				   F::split_size_bytes());
      
      messages_stream0 = 
	graph_storage->open_stream("messages0", true, 
				   vm["updates0_disk"].as<unsigned long>(),
				   sizeof(bfs_filt_update));
      messages_stream1 = 
	graph_storage->open_stream("messages1", true,
				   vm["updates1_disk"].as<unsigned long>(),
				   sizeof(bfs_filt_update));
      bfs_filt_pcpu::scatter_filter = new
	x_lib::filter(MAX(graph_storage->get_config()->cached_partitions,
			  graph_storage->get_config()->super_partitions),
		      num_processors);
      mode_rev_bfs = false;
      setup_time.stop();
    }
  
    template<typename F> 
    struct bfs_filt_edge_wrapper
    {
      static unsigned long item_size()
      {
	return F::split_size_bytes();
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return F::split_key(buffer, 0);
      }
    };

    struct bfs_filt_update_wrapper
    {
      static unsigned long item_size()
      {
	return sizeof(bfs_filt_update);
      }
    
      static unsigned long key(unsigned char *buffer)
      {
	return ((bfs_filt_update *)buffer)->dst;
      }
    };

    template<typename F>
    void bfs_filt<F>::operator() ()
    {
      const x_lib::configuration *config = graph_storage->get_config();
      bfs_filt_pcpu::total_edges = pt.get<unsigned long>("graph.edges");
      bfs_filt_pcpu::current_step = step_split_edges;
      x_lib::do_stream< bfs_filt<F>, 
      			bfs_filt_edge_wrapper<F>, 
			bfs_filt_edge_wrapper<F> >
	(graph_storage, 0, init_stream, edges_stream0, NULL); 
      graph_storage->close_stream(init_stream);
      // Main loop
      bool do_init = true;
      unsigned long current_estream = 0;
      unsigned long PHASE = 0;
      unsigned long messages_in_stream;
      unsigned long messages_out_stream;
      unsigned long edges_in_stream;
      do {
	if(PHASE == 0) {
	  messages_in_stream  = messages_stream0;
	  messages_out_stream = messages_stream1;
	}
	else {
	  messages_in_stream  = messages_stream1;
	  messages_out_stream = messages_stream0;
	}
	if(current_estream == 0) {
	  edges_in_stream = edges_stream0;
	}
	else {
	  edges_in_stream = edges_stream1;
	}
	graph_storage->rewind_stream(messages_in_stream);
	graph_storage->rewind_stream(edges_in_stream);
	bfs_filt_pcpu::wasted_edges            = 0;
	bfs_filt_pcpu::wasted_updates_internal = 0;
	bfs_filt_pcpu::wasted_updates_sibling  = 0;
	bfs_filt_pcpu::total_updates           = 0;
	bfs_filt_pcpu::horizon_size            = 0;
	for(unsigned long i=0;i<config->super_partitions;i++) {
	  state_load(do_init, i);
	  do_init = false;
	  bfs_filt_pcpu::current_step = step_updates_in;
	  if(bfs_filt_pcpu::bfs_iteration == 0) {
	    x_lib::do_state_iter<bfs_filt<F> > (graph_storage, i);
	  }
	  x_lib::do_stream< bfs_filt<F>, 
			    bfs_filt_update_wrapper, 
			    bfs_filt_update_wrapper >
	    (graph_storage, i, messages_in_stream, ULONG_MAX, NULL);
	  graph_storage->reset_stream(messages_in_stream, i);
	  bfs_filt_pcpu::current_step = step_updates_out;
	  x_lib::do_stream< bfs_filt<F>, 
			    bfs_filt_edge_wrapper<F>, 
			    bfs_filt_update_wrapper >
	    (graph_storage, i, edges_in_stream, messages_out_stream,
	     bfs_filt_pcpu::scatter_filter);
	  (void)x_lib::do_cpu<bfs_filt<F> >(graph_storage, i);
	  state_store(i);
	}
	float wasted_frac;
	float wasted_frac_sib_updt;
	float wasted_frac_int_updt;
	if(bfs_filt_pcpu::total_edges > 0) {
	  wasted_frac =
	    ((float)bfs_filt_pcpu::wasted_edges)/bfs_filt_pcpu::total_edges;
	}
	else {
	  wasted_frac =	0.0f;
	}
	if(bfs_filt_pcpu::total_updates > 0) {
	  wasted_frac_sib_updt =
	    ((float)bfs_filt_pcpu::wasted_updates_sibling)/bfs_filt_pcpu::total_updates;
	  wasted_frac_int_updt =
	    ((float)bfs_filt_pcpu::wasted_updates_internal)/bfs_filt_pcpu::total_updates;
	}
	else {
	  wasted_frac_sib_updt = 0.0f;
	  wasted_frac_int_updt = 0.0f;
	}
	if(wasted_frac > bfs_filt_threshold) {
	  unsigned long edges_out_stream;
	  bfs_filt_pcpu::current_step = step_filter_edges;
	  if(current_estream == 0) {
	    edges_out_stream = edges_stream1;
	  }
	  else {
	    edges_out_stream = edges_stream0;
	  }
	  graph_storage->rewind_stream(edges_in_stream);
	  for(unsigned long i=0;i<config->super_partitions;i++) {
	    state_load(do_init, i);
	    x_lib::do_stream< bfs_filt<F>, 
			      bfs_filt_edge_wrapper<F>, 
			      bfs_filt_edge_wrapper<F> >
	      (graph_storage, i, edges_in_stream, edges_out_stream, NULL);
	    graph_storage->reset_stream(edges_in_stream, i);
	    state_store(i);
	  }
	  current_estream =  1 - current_estream;
	}
	PHASE = 1 - PHASE;
	if(heartbeat) {
	  BOOST_LOG_TRIVIAL(info) << clock::timestamp() 
				  << " Heartbeat: " << bfs_filt_pcpu::bfs_iteration
				  << " VERTICES: " << bfs_filt_pcpu::vertices_discovered
				  << " EDGES: "    
				  << bfs_filt_pcpu::edges_explored
				  << " UNFILTERED_EDGES: "
				  << bfs_filt_pcpu::total_edges
				  << " WASTED_FRAC_EDGES: " 
				  << wasted_frac
				  << " TOTAL_UPDATES: "
				  << bfs_filt_pcpu::total_updates
				  << " WASTED_FRAC_UPDATES_INTERNAL: "
				  << wasted_frac_int_updt
				  << " WASTED_FRAC_UPDATES_SIBLING: "
				  << wasted_frac_sib_updt
				  << " HORIZON_SIZE: "
				  << bfs_filt_pcpu::horizon_size;
	}
	bfs_filt_pcpu::bfs_iteration++;
      } while(!graph_storage->stream_empty(messages_out_stream));
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS::VERTICES_SEEN " <<
	bfs_filt_pcpu::vertices_discovered;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS::EDGES_EXPLORED " <<
	bfs_filt_pcpu::edges_explored;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS::PHASES " <<
	bfs_filt_pcpu::bfs_iteration;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void bfs_filt<F>::partition_pre_callback(unsigned long superp, 
					     unsigned long partition,
					     per_processor_data *pcpu)
    {
      bfs_filt_pcpu *my_pcpu = static_cast<bfs_filt_pcpu *>(pcpu);
      my_pcpu->activate_partition_for_scatter = false;
    }

    template<typename F>
    void bfs_filt<F>::partition_callback (x_lib::stream_callback_state *callback)
    {
      bfs_filt_pcpu *pcpu = static_cast<bfs_filt_pcpu *>(callback->cpu_state);
      switch(bfs_filt_pcpu::current_step) {
      
      case step_split_edges: {
	while(callback->bytes_in) {
	  if(callback->bytes_out + F::split_size_bytes() > 
	     callback->bytes_out_max) {
	    break;
	  }
	  memcpy(callback->bufout, callback->bufin, F::split_size_bytes());
	  callback->bufout    += F::split_size_bytes();
	  callback->bytes_out += F::split_size_bytes();
	  callback->bytes_in  -= F::split_size_bytes();
	  callback->bufin     += F::split_size_bytes(); 
	}
	break;
      }
      
      case step_updates_in: {
	while(callback->bytes_in) {
	  bfs_filt_update *m = (bfs_filt_update *)callback->bufin;
	  bfs_filt_vertex *v = ((bfs_filt_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(m->dst);
	  pcpu->total_updates_local++;
	  if(v->bfs_parent == SENTINEL) {
	    v->bfs_parent       = m->src;
	    v->update_iteration = bfs_filt_pcpu::bfs_iteration;
	    pcpu->activate_partition_for_scatter = true;
	    pcpu->vertices_discovered_local++;
	    pcpu->horizon_size_local++;
	  }
	  else if(v->update_iteration == bfs_filt_pcpu::bfs_iteration){
	    pcpu->wasted_updates_sibling_local++;
	  }
	  else {
	    pcpu->wasted_updates_internal_local++;
	  }
	  callback->bufin     += sizeof(bfs_filt_update);
	  callback->bytes_in  -= sizeof(bfs_filt_update);
	}
	break;
      }

      case step_updates_out: {
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  F::read_edge(callback->bufin, src, dst);
	  bfs_filt_vertex *v = ((bfs_filt_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(src);
	  if(v->bfs_parent != SENTINEL) {
	    if(v->update_iteration == bfs_filt_pcpu::bfs_iteration) {
	      if((callback->bytes_out + sizeof(bfs_filt_update)) > 
		 callback->bytes_out_max) {
		break;
	      }
	      bfs_filt_update *mout = (bfs_filt_update *)(callback->bufout);
	      mout->src       = src;
	      mout->dst       = dst;
	      pcpu->edges_explored_local++;
	      callback->bufout    += sizeof(bfs_filt_update);
	      callback->bytes_out += sizeof(bfs_filt_update);
	    }
	    else {
	      pcpu->wasted_local++;
	    }
	  }
	  callback->bufin     += F::split_size_bytes();
	  callback->bytes_in  -= F::split_size_bytes();
	}
	break;
      }
      
      case step_filter_edges: {
	while(callback->bytes_in) {
	  vertex_t src, dst;
	  F::read_edge(callback->bufin, src, dst);
	  bfs_filt_vertex *v = ((bfs_filt_vertex *)(callback->state)) +
	    x_lib::configuration::map_offset(src);
	  if(v->bfs_parent == SENTINEL) {
	    if((callback->bytes_out + F::split_size_bytes()) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    memcpy(callback->bufout, callback->bufin, F::split_size_bytes());
	    callback->bufout    += F::split_size_bytes();
	    callback->bytes_out += F::split_size_bytes();
	  }
	  else {
	    pcpu->wasted_local++;
	  }
	  callback->bufin     += F::split_size_bytes();
	  callback->bytes_in  -= F::split_size_bytes();
	}
	break;
      }
      default:
	BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	exit(-1);
      }
    }
  
    template<typename F>
    void bfs_filt<F>::partition_post_callback(unsigned long superp, 
					      unsigned long partition,
					      per_processor_data *pcpu)
    {
      bfs_filt_pcpu *my_pcpu = static_cast<bfs_filt_pcpu *>(pcpu);
      if(my_pcpu->current_step == step_updates_in) {
	if(my_pcpu->activate_partition_for_scatter) {
	  bfs_filt_pcpu::scatter_filter->q(partition);
	}
      }
    }

    template<typename F> bfs_filt_pcpu ** bfs_filt<F>::pcpu_array = NULL;
    template<typename F> unsigned long bfs_filt<F>::bfs_root = 0;
    x_lib::filter * bfs_filt_pcpu::scatter_filter = NULL;
    unsigned long bfs_filt_pcpu::current_step;
    unsigned long bfs_filt_pcpu::bfs_iteration = 0;
    unsigned long bfs_filt_pcpu::wasted_edges = 0;
    unsigned long bfs_filt_pcpu::total_edges = 0;
    unsigned long bfs_filt_pcpu::vertices_discovered = 0;
    unsigned long bfs_filt_pcpu::edges_explored = 0;
    unsigned long bfs_filt_pcpu::wasted_updates_internal = 0;
    unsigned long bfs_filt_pcpu::wasted_updates_sibling = 0;
    unsigned long bfs_filt_pcpu::total_updates  = 0;
    unsigned long bfs_filt_pcpu::horizon_size  = 0;
  }
}
#endif
