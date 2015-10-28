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

#ifndef _SG_DRIVER_
#define _SG_DRIVER_
#include<sys/time.h>
#include<sys/resource.h>
#include "x-lib.hpp"


// Implement a wrapper for simpler graph algorithms that alternate between
// synchronously gathering updates and synchronously scattering them along edges

namespace algorithm {
  const unsigned long phase_edge_split    = 0;
  const unsigned long superphase_begin    = 1;
  const unsigned long phase_gather        = 2;
  const unsigned long phase_scatter       = 3;
  const unsigned long phase_post_scatter  = 4;
  const unsigned long phase_terminate     = 5;

  struct sg_pcpu:public per_processor_data {
    unsigned long processor_id;
    bool i_vote_to_stop;
    static sg_pcpu ** per_cpu_array;
    static x_barrier *sync;
    // Stats
    unsigned long update_bytes_out;
    unsigned long update_bytes_in;
    unsigned long edge_bytes_streamed;
    unsigned long partitions_processed;
    // 
    static x_lib::filter *scatter_filter;
    bool activate_partition_for_scatter;
    
    /* begin work specs. */
    static unsigned long bsp_phase;
    static unsigned long current_step;
    static bool do_algo_reduce;
    /* end work specs. */

    static per_processor_data **algo_pcpu_array;
    per_processor_data *algo_pcpu;

    bool reduce(per_processor_data **per_cpu_array,
		unsigned long processors)
    {
      if(algo_pcpu_array[0] != NULL && do_algo_reduce) {
	return algo_pcpu_array[0]->reduce(algo_pcpu_array, processors);
      }
      else {
	return false; // Should be don't care
      }
    }
  } __attribute__((__aligned__(64)));

  class sg_sync_stop {
    bool state;
    bool io;
  public:
    sg_sync_stop(bool state_in)
      :state(state_in)
    {}
    unsigned char *db_buffer() 
    {
      return (unsigned char *)&io;
    }

    unsigned long db_size()
    { return sizeof(bool);}

    void db_generate()
    {
      io = state;
    }

    void db_merge()
    {
      io= io && state;
    }

    void db_absorb()
    {
      state = io;
    }

    bool get_state()
    {
      return state;
    }

  };

  template<typename A, typename F>
  class scatter_gather {
    sg_pcpu ** pcpu_array;
    bool heartbeat;
    bool measure_scatter_gather;
    x_lib::streamIO<scatter_gather> *graph_storage;
    unsigned long vertex_stream;
    unsigned long edge_stream;
    unsigned long updates_stream;
    unsigned long init_stream;
    rtc_clock wall_clock;
    rtc_clock setup_time;
    rtc_clock state_iter_cost;
    rtc_clock scatter_cost;
    rtc_clock gather_cost;

  public:
    static unsigned long checkpoint_size()
    {
      return sizeof(unsigned long) + sizeof(bool) + A::checkpoint_size();
    }

    void take_checkpoint(unsigned char *buffer)
    {
      bool vote_to_stop = true;
      memcpy(buffer, &sg_pcpu::bsp_phase, sizeof(unsigned long));
      buffer += sizeof(unsigned long);
      for(unsigned long i=0;i<graph_storage->get_config()->processors;i++) {
	vote_to_stop = vote_to_stop && pcpu_array[i]->i_vote_to_stop;
      }
      memcpy(buffer, &vote_to_stop, sizeof(bool));
      buffer += sizeof(bool);
      A::take_checkpoint(buffer, 
			 sg_pcpu::algo_pcpu_array, 
			 graph_storage->get_config()->processors);
    }

    void restore_checkpoint(unsigned char *buffer)
    {
      bool vote_to_stop;
      memcpy(&sg_pcpu::bsp_phase, buffer, sizeof(unsigned long));
      buffer += sizeof(unsigned long);
      memcpy(&vote_to_stop, buffer, sizeof(bool));
      buffer += sizeof(bool);
      for(unsigned long i=0;i<graph_storage->get_config()->processors;i++) {
	pcpu_array[i]->i_vote_to_stop = vote_to_stop;
      }
      A::restore_checkpoint(buffer,
			    sg_pcpu::algo_pcpu_array, 
			    graph_storage->get_config()->processors);
    }

    scatter_gather();
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
      return 4; // vertices, edges, init_edges, updates
    }
    static unsigned long max_buffers()
    {
      return 4;
    }

    static unsigned long vertex_state_bytes()
    {
      return A::vertex_state_bytes();
    }

    static unsigned long vertex_stream_buffer_bytes()
    {
      return A::split_size_bytes() + F::split_size_bytes();
    }

    static void state_iter_callback(unsigned long superp, 
				    unsigned long partition,
				    unsigned long index,
				    unsigned char *vertex,
				    per_processor_data *cpu_state)
    {
      unsigned long global_index = 
	x_lib::configuration::map_inverse(superp, partition, index);
      sg_pcpu *pcpu = static_cast<sg_pcpu *>(cpu_state);
      (void)A::init(vertex, global_index,
		    sg_pcpu::bsp_phase,
		    sg_pcpu::algo_pcpu_array[pcpu->processor_id]);
      /*
      if(will_scatter) {
	sg_pcpu::scatter_filter->q(partition);
      }
      */
    }

    static void vertex_apply(unsigned char *v,
                             unsigned char *copy,
                             unsigned long copy_machine,
                             per_processor_data *cpu_state)
    {
      sg_pcpu *pcpu = static_cast<sg_pcpu *>(cpu_state);
      A::vertex_apply(v,
		      copy,
		      copy_machine, 
		      sg_pcpu::algo_pcpu_array[pcpu->processor_id],
		      sg_pcpu::bsp_phase);
    }

    static per_processor_data * 
    create_per_processor_data(unsigned long processor_id)
    {
      return sg_pcpu::per_cpu_array[processor_id];
    }
  
    static void do_cpu_callback(per_processor_data *cpu_state)
    {
      sg_pcpu *cpu = static_cast<sg_pcpu *>(cpu_state);
      if(sg_pcpu::current_step == superphase_begin) {
	cpu->i_vote_to_stop = true;
      }
      else if(sg_pcpu::current_step == phase_post_scatter) {
	//sg_pcpu::scatter_filter->done(cpu->processor_id);
      }
      else if(sg_pcpu::current_step == phase_terminate) {
	BOOST_LOG_TRIVIAL(info)<< "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
	BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
      }
    }
  };
  
  template<typename A, typename F>
  scatter_gather<A, F>::scatter_gather()
  {
    BOOST_LOG_TRIVIAL(info)<<"SG-DRIVER-ORIGINAL";
    wall_clock.start();
    setup_time.start();
    heartbeat = (vm.count("heartbeat") > 0);
    measure_scatter_gather = (vm.count("measure_scatter_gather") > 0);
    unsigned long num_processors = vm["processors"].as<unsigned long>();
    unsigned long machines       = pt_slipstore.get<unsigned long>("machines.count");
    per_processor_data **algo_pcpu_array = new per_processor_data *[num_processors];
    sg_pcpu::per_cpu_array = pcpu_array = new sg_pcpu *[num_processors];
    sg_pcpu::sync = new x_barrier(num_processors);
    sg_pcpu::do_algo_reduce = false;
    for(unsigned long i=0;i<num_processors;i++) {
      pcpu_array[i] = new sg_pcpu();
      pcpu_array[i]->processor_id = i;
      pcpu_array[i]->update_bytes_in = 0;
      pcpu_array[i]->update_bytes_out = 0;
      pcpu_array[i]->edge_bytes_streamed = 0;
      pcpu_array[i]->partitions_processed = 0;
      algo_pcpu_array[i] = A::create_per_processor_data(i, machines);
    }
    sg_pcpu::algo_pcpu_array = algo_pcpu_array;
    A::preprocessing(); // Note: ordering critical with the next statement
    graph_storage = new x_lib::streamIO<scatter_gather>();
    /*
    sg_pcpu::scatter_filter = new
      x_lib::filter(MAX(graph_storage->get_config()->cached_partitions,
			graph_storage->get_config()->super_partitions),
		    num_processors);
    */
    sg_pcpu::bsp_phase = 0;
    vertex_stream = slipstore::STREAM_VERTEX_STATE;
    edge_stream = graph_storage->open_stream();
    init_stream = slipstore::STREAM_INPUT;
    updates_stream = graph_storage->open_stream();
    setup_time.stop();
  }
  
  template<typename F> 
  struct edge_type_wrapper
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

  template<typename A> 
  struct update_type_wrapper
  {
    static unsigned long item_size()
    {
      return A::split_size_bytes();
    }
    static unsigned long key(unsigned char *buffer)
    {
      return A::split_key(buffer, 0);
    }
  };

  template<typename A, typename F>
  void scatter_gather<A, F>::operator() ()
  {
    bool restored = x_lib::load_checkpoint<scatter_gather<A, F> >
      (graph_storage, this);
    if(!restored) {
      // Edge split
      sg_pcpu::current_step = phase_edge_split;
      x_lib::do_init_stream< scatter_gather<A, F>, 
			     edge_type_wrapper<F>, 
			     edge_type_wrapper<F> >
	(graph_storage, init_stream, edge_stream);
      if(vm.count("destroy_init") > 0) {
	graph_storage->reset_stream(init_stream, 0);
      }
    }
    BOOST_LOG_TRIVIAL(info) 
      << clock::timestamp() 
      << " Completed init "; 
    // Supersteps
    bool global_stop;
    while(true) {
      graph_storage->rewind_stream(edge_stream);
      if(!restored) {
	sg_pcpu::current_step = superphase_begin;
	x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, ULONG_MAX);
	if(A::need_init(sg_pcpu::bsp_phase)) {
	  if(measure_scatter_gather) {
	    state_iter_cost.start();
	  }
	  x_lib::do_state_iter<scatter_gather<A, F> > (graph_storage);
	  if(measure_scatter_gather) {
	    state_iter_cost.stop();
	  }
	}
	sg_pcpu::current_step = phase_gather;
	if(measure_scatter_gather) {
	  gather_cost.start();
	}
	// Old interface
	//for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	//  graph_storage->state_load(i);
	//  x_lib::do_stream<scatter_gather<A, F>, 
	//		   update_type_wrapper<A>,
	//		   update_type_wrapper<A> >
	//    (graph_storage, i, updates_stream, ULONG_MAX, NULL, true);
	//  graph_storage->reset_stream(updates_stream, i);
	//  graph_storage->state_store(i);
	//}
	x_lib::do_stream_skip<scatter_gather<A, F>,
			      update_type_wrapper<A>,
			      update_type_wrapper<A> >
	  (graph_storage, updates_stream, ULONG_MAX, NULL, true, true);
	if(measure_scatter_gather) {
	  gather_cost.stop();
	}
	
	if(A::need_data_barrier()) {
	  typename A::db_sync * alg_sync_object = A::get_db_sync();
	  alg_sync_object->prep_db_data(sg_pcpu::algo_pcpu_array,
					slipstore::slipstore_client_barrier->get_me(),
					graph_storage->get_config()->processors);
	  slipstore::sync_barrier<typename A::db_sync>(slipstore::slipstore_client_barrier,
						       alg_sync_object);
	  alg_sync_object->finalize_db_data(sg_pcpu::algo_pcpu_array,
					    slipstore::slipstore_client_barrier->get_me(),
					    graph_storage->get_config()->processors);
	  delete alg_sync_object;
	}
	// Take a checkpoint here before proceeding to the next iteration
	// This ensures the checkpoints are smaller as update stream is empty
	x_lib::take_checkpoint<scatter_gather<A, F> >(graph_storage, this);
      }
      if(restored) {
	restored = false;
      }
      sg_pcpu::current_step = phase_scatter;
      if(measure_scatter_gather) {
	scatter_cost.start();
      }
      // Old interface
      //for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
      //	graph_storage->state_load(i);
      // x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, i);
      // x_lib::do_stream<scatter_gather<A, F>, 
      // edge_type_wrapper<F>,
      // update_type_wrapper<A> >
      // (graph_storage, i, edge_stream, updates_stream, NULL,
      // A::need_scatter_merge(sg_pcpu::bsp_phase));
      // sg_pcpu::current_step = phase_post_scatter;
      // if(i == (graph_storage->get_config()->super_partitions - 1)) {
      //	  sg_pcpu::do_algo_reduce = true;
      //}
      //	bool stop_result =  x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, i);
      //	if(i == (graph_storage->get_config()->super_partitions - 1)) {
      //	  global_stop = global_stop && stop_result;
      //	}
      //	sg_pcpu::do_algo_reduce = false;
      //	graph_storage->state_store(i);
      //}

      x_lib::do_stream_skip<scatter_gather<A, F>, 
			    edge_type_wrapper<F>,
			    update_type_wrapper<A> >
	(graph_storage, edge_stream, updates_stream, NULL,
	 A::need_scatter_merge(sg_pcpu::bsp_phase), false);
      sg_pcpu::do_algo_reduce = true;
      global_stop =  x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, 0);
      sg_pcpu::do_algo_reduce = false;
      if(measure_scatter_gather) {
	scatter_cost.stop();
      }
      graph_storage->rewind_stream(updates_stream);
      unsigned long no_voter;
      sg_pcpu::bsp_phase++;
      if(heartbeat) {
	BOOST_LOG_TRIVIAL(info) 
	  << clock::timestamp() 
	  << " Completed phase " 
	  << sg_pcpu::bsp_phase
	  << " Bytes read "
	  << stat_bytes_read
	  << " Bytes written "
	  << stat_bytes_written;
      }
      if(sg_pcpu::bsp_phase > A::min_super_phases()) {
	for(no_voter=0;no_voter<graph_storage->get_config()->processors;no_voter++) {
	  if(!pcpu_array[no_voter]->i_vote_to_stop) {
	    break;
	  }
	}
	sg_sync_stop sync_obj
	  ((no_voter == graph_storage->get_config()->processors)|| global_stop);
	// Synchronize sync_stop across machines
	slipstore::sync_barrier<sg_sync_stop>
	  (slipstore::slipstore_client_barrier, &sync_obj);
	if(sync_obj.get_state()) {
	  break;
	}
      }
    }
    A::postprocessing();
    sg_pcpu::current_step = phase_terminate;
    x_lib::do_cpu<scatter_gather<A, F> >(graph_storage, ULONG_MAX);
    setup_time.start();
    graph_storage->terminate();
    setup_time.stop();
    wall_clock.stop();
    BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << sg_pcpu::bsp_phase;
    setup_time.print("CORE::TIME::SETUP");
    if(measure_scatter_gather) {
      state_iter_cost.print("CORE::TIME::STATE_ITER");
      gather_cost.print("CORE::TIME::GATHER");
      scatter_cost.print("CORE::TIME::SCATTER");
    }
    wall_clock.print("CORE::TIME::WALL");
  }

  template<typename A, typename F>
  void scatter_gather<A, F>::partition_pre_callback(unsigned long superp, 
						    unsigned long partition,
						    per_processor_data *pcpu)
  {
    sg_pcpu *pcpu_actual = static_cast<sg_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
      pcpu_actual->activate_partition_for_scatter = false;
    }
  }

  
  template<typename A, typename F>
  void scatter_gather<A, F>::partition_callback
  (x_lib::stream_callback_state *callback)
  {
    sg_pcpu *pcpu = static_cast<sg_pcpu *>(callback->cpu_state);
    switch(sg_pcpu::current_step) {
    case phase_edge_split: {
      unsigned long bytes_to_copy = 
	(callback->bytes_in < callback->bytes_out_max) ?
	callback->bytes_in:callback->bytes_out_max;
      bytes_to_copy = 
	(bytes_to_copy/F::split_size_bytes())*F::split_size_bytes();
      callback->bytes_in -= bytes_to_copy;
      memcpy(callback->bufout, callback->bufin, bytes_to_copy);
      callback->bufin += bytes_to_copy;
      callback->bytes_out = bytes_to_copy;
      break;
    }
    case phase_gather: {
      pcpu->update_bytes_in += callback->bytes_in;
      while(callback->bytes_in) {
	bool activate = 
	  A::apply_one_update(callback->state,
			      callback->bufin,
			      sg_pcpu::algo_pcpu_array[pcpu->processor_id],
			      callback->local_tile,
			      pcpu->bsp_phase);
	callback->bufin += A::split_size_bytes();
	callback->bytes_in -= A::split_size_bytes();
	pcpu->activate_partition_for_scatter |= activate;
	pcpu->i_vote_to_stop = pcpu->i_vote_to_stop && !activate;
      }
      break;
    }
    case phase_scatter: {
      unsigned long tmp = callback->bytes_in;
      unsigned char *bufout = callback->bufout;
      while(callback->bytes_in) {
	if((callback->bytes_out + A::split_size_bytes()) >
	   callback->bytes_out_max) {
	  break;
	}
	bool up = A::generate_update(callback->state,
				     callback->bufin, 
				     bufout,
				     sg_pcpu::algo_pcpu_array[pcpu->processor_id],
				     callback->local_tile,
				     sg_pcpu::bsp_phase);
	callback->bufin    += F::split_size_bytes();
	callback->bytes_in -= F::split_size_bytes();
	if(up) {
	  callback->bytes_out += A::split_size_bytes();
	  bufout += A::split_size_bytes();
	}
      }
      pcpu->update_bytes_out    += callback->bytes_out;
      pcpu->edge_bytes_streamed += (tmp - callback->bytes_in); 
      break;
    }
    default:
      BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
      exit(-1);
    }
  }

  template<typename A, typename F>
  void scatter_gather<A, F>::partition_post_callback(unsigned long superp, 
						     unsigned long partition,
						     per_processor_data *pcpu)
  {
    sg_pcpu *pcpu_actual = static_cast<sg_pcpu *>(pcpu);
    if(pcpu_actual->current_step == phase_gather) {
      if(pcpu_actual->activate_partition_for_scatter) {
	//sg_pcpu::scatter_filter->q(partition);
      }
      pcpu_actual->partitions_processed++;
    }
  }
}
#endif
