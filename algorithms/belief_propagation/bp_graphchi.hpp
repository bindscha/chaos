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

#ifndef _BP_GRAPHCHI_
#define _BP_GRAPHCHI_
#include<sys/time.h>
#include<sys/resource.h>
#include<math.h>
#include "../../core/x-lib.hpp"

// Belief propagation using the technique of 
// Kung et. al. 
// Same implementation as that used by Kyrola et al. in their OSDI 2012 paper


namespace algorithm {
  namespace belief_prop {
    namespace graphchi {
      struct bpchi_pcpu:public per_processor_data {
	unsigned long processor_id;
	// Stats
	unsigned long update_bytes_out;
	unsigned long update_bytes_in;
	unsigned long edge_bytes_streamed;
	unsigned long partitions_processed;
	// 

	/* begin work specs. */
	static unsigned long bsp_phase;
	static unsigned long current_step;
	/* end work specs. */
	bool reduce(per_processor_data **per_cpu_array,
		    unsigned long processors)
	{
	  return false; 
	}
      } __attribute__((__aligned__(64)));
      
      struct __attribute__((__packed__)) belief_propagation_graphchi_vertex {
	double logProdM0;
	double logProdM1;
	double belief0; // Avoid recomputing for every edge
	double belief1; // Avoid recomputing for every edge 
      };
      
      struct __attribute__((__packed__)) belief_propagation_graphchi_edge {
	vertex_t src;
	vertex_t dst;
	float msg;
      };

      template<typename F>
      class belief_propagation_graphchi {
	const static unsigned long step_gen_edge_potential  = 0;
	const static unsigned long step_init                = 1;
	const static unsigned long step_absorb              = 2;
	const static unsigned long step_rescale             = 3;
	const static unsigned long step_emit                = 4;
	const static unsigned long step_terminate           = 5;
	unsigned long niters;
	static bpchi_pcpu ** pcpu_array;
	bool heartbeat;
	x_lib::streamIO<belief_propagation_graphchi> *graph_storage;
	unsigned long vertex_stream;
	unsigned long updates0_stream;
	unsigned long updates1_stream;
	unsigned long init_stream;
	rtc_clock wall_clock;
	rtc_clock setup_time;
	rtc_clock edge_potential_generation_time;
	unsigned long PHASE;
	unsigned long iters;

	static bool is_seed(unsigned long vid) 
	{
	  return (((17+vid)*3428971)%100 <= 1);
	}
	
	static bool is_good(unsigned long vid) 
	{
	  return (((17+vid)*3428971)%100  == 0);
	}
    
    
	static bool is_bad(unsigned long vid) 
	{
	  return (((17+vid)*3428971)%100  == 1);
	}
    
	static bool isnan(float x) 
	{
	  return !(x<0 || x>=0);
	}


	static double computePriorP0(unsigned long vid)
	{
	  double priorP0 = 0.5;
	  // Hack before we do actual seed selection
	  if (is_bad(vid)) priorP0 = 0.95; // BAD
	  else if (is_good(vid)) priorP0 = 0.05; // GOOD
	  return priorP0;
	}
    
      public:
	
	belief_propagation_graphchi();
	static void partition_pre_callback(unsigned long super_partition,
					   unsigned long partition,
					   per_processor_data* cpu_state);
	static void generate_initial_belief
	(unsigned char *edge, 
	 struct belief_propagation_graphchi_edge* be_fwd,
	 struct belief_propagation_graphchi_edge* be_rev);
	static void partition_callback(x_lib::stream_callback_state *state);
	static void partition_post_callback(unsigned long super_partition,
					    unsigned long partition,
					    per_processor_data *cpu_state);

	static
	bool need_data_barrier()
	{
	  return false;
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
	db_sync * get_db_sync() {return NULL;}
	
	static void vertex_apply(unsigned char *v,
				 unsigned char *copy,
				 unsigned long copy_machine,
				 per_processor_data *per_cpu_data)
	{
	  belief_propagation_graphchi_vertex *vtx     =
	    (belief_propagation_graphchi_vertex *)v;
	  belief_propagation_graphchi_vertex *vtx_cpy =
	    (belief_propagation_graphchi_vertex *)copy;

	    vtx->logProdM0 += vtx_cpy->logProdM0;
	    vtx->logProdM1 += vtx_cpy->logProdM1;
	}

	void operator() ();
	static unsigned long max_streams()
	{
	  return 5; // vertices, edges, init_edges, updates0, updates1
	}
	static unsigned long max_buffers()
	{
	  return 4;
	}
	
	static unsigned long vertex_state_bytes()
	{
	  return sizeof(struct belief_propagation_graphchi_vertex);
	}
	
	static unsigned long vertex_stream_buffer_bytes()
	{
	  return sizeof(belief_propagation_graphchi_vertex) + 
	    sizeof(belief_propagation_graphchi_edge);
	}
    
	static void state_iter_callback(unsigned long superp, 
					unsigned long partition,
					unsigned long index,
					unsigned char *vertex,
					per_processor_data *cpu_state)
	{
	  belief_propagation_graphchi_vertex *v = 
	    (struct belief_propagation_graphchi_vertex *)vertex;
	  if(bpchi_pcpu::current_step == step_init) {
	    v->logProdM0  = 0.0;
	    v->logProdM1  = 0.0; 
	    if(bpchi_pcpu::bsp_phase == 0) {
	      v->belief0  = computePriorP0
		(x_lib::configuration::map_inverse(superp, partition, index));
	      v->belief1  = 1.0 - v->belief0;
	    }
	  }
	  else if(bpchi_pcpu::current_step == step_rescale) {
	    // Rescale for numerical reasons (the larger one becomes zero)
	    double maxM = std::max(v->logProdM0, v->logProdM1);
	    v->logProdM0 -= maxM;
	    v->logProdM1 -= maxM;
	    // Compute beliefs
	    double priorP0, priorP1;
	    unsigned long vid = 
	      x_lib::configuration::map_inverse(superp, partition, index);
	    if (!is_seed(vid)) {
	      priorP0 = computePriorP0(vid);
	      priorP1 = 1.0 - priorP0;
	      v->belief0 = priorP0 * exp(v->logProdM0);
	      v->belief1 = priorP1 * exp(v->logProdM1);
	      if (v->belief0<1e-4) v->belief0 = 1e-4; // Do not let factors go to zero (for numerical reasons)
	      if (v->belief1<1e-4) v->belief1 = 1e-4; // Do not let factors go to zero (for numerical reasons)
	      // Normalize
	      double norm = 1.0/(v->belief0+v->belief1);
	      v->belief0 *= norm;
	      v->belief1 *= norm;
	    }
	    BOOST_ASSERT_MSG(!isnan(v->belief0), "Belief0 went to NAN!");
	  }
	  else {
	    BOOST_LOG_TRIVIAL(fatal)<<  "Unkown step in state iteration !";
	    exit(-1);
	  }
	}

	static per_processor_data * 
	create_per_processor_data(unsigned long processor_id)
	{
	  return pcpu_array[processor_id];
	}

	static void do_cpu_callback(per_processor_data *cpu_state)
	{
	  bpchi_pcpu *cpu = static_cast<bpchi_pcpu *>(cpu_state);
	  if(bpchi_pcpu::current_step == step_terminate) {
	    BOOST_LOG_TRIVIAL(info)<< "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
	    BOOST_LOG_TRIVIAL(info)<< "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
	  }
	}

	static unsigned long checkpoint_size()
	{
	  return 3*sizeof(unsigned long);
	}
	
	void take_checkpoint(unsigned char *buffer)
	{
	  memcpy(buffer, &PHASE, sizeof(unsigned long));
	  buffer += sizeof(unsigned long);
	  memcpy(buffer, &iters, sizeof(unsigned long));
	  buffer += sizeof(unsigned long);
	  memcpy(buffer, &bpchi_pcpu::bsp_phase, sizeof(unsigned long));
	}

	void restore_checkpoint(unsigned char *buffer)
	{
	  memcpy(&PHASE, buffer, sizeof(unsigned long));
	  buffer += sizeof(unsigned long);
	  memcpy(&iters, buffer, sizeof(unsigned long));
	  buffer += sizeof(unsigned long);
	  memcpy(&bpchi_pcpu::bsp_phase, buffer, sizeof(unsigned long));
	}
      };
  
      template<typename F>
      belief_propagation_graphchi<F>::belief_propagation_graphchi()
      {
	wall_clock.start();
	setup_time.start();
	heartbeat = (vm.count("heartbeat") > 0);
	niters    = vm["belief_propagation::niters"].as<unsigned long>();
	unsigned long num_processors = vm["processors"].as<unsigned long>();
	pcpu_array = new bpchi_pcpu *[num_processors];
	for(unsigned long i=0;i<num_processors;i++) {
	  pcpu_array[i] = new bpchi_pcpu();
	  pcpu_array[i]->processor_id = i;
	  pcpu_array[i]->update_bytes_in = 0;
	  pcpu_array[i]->update_bytes_out = 0;
	  pcpu_array[i]->edge_bytes_streamed = 0;
	}
	graph_storage = new x_lib::streamIO<belief_propagation_graphchi>();
	bpchi_pcpu::bsp_phase = 0;
	vertex_stream = slipstore::STREAM_VERTEX_STATE;
	std::string efile = pt.get<std::string>("graph.name");
	init_stream = slipstore::STREAM_INPUT;
	updates0_stream = graph_storage->open_stream();
	updates1_stream = graph_storage->open_stream();
	setup_time.stop();
      }
  
      template<typename F> 
      struct init_edge_wrapper
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

      struct belief_edge_wrapper
      {
	static unsigned long item_size()
	{
	  return sizeof(struct belief_propagation_graphchi_edge);
	}
	static unsigned long key(unsigned char *buffer)
	{
	  return ((struct belief_propagation_graphchi_edge *)buffer)->dst;
	}
      };

      template<typename F>
      void belief_propagation_graphchi<F>::operator() ()
      {
	bool restored = x_lib::load_checkpoint<belief_propagation_graphchi<F> >
	  (graph_storage, this);
	// Generate edge potentials
	if(!restored) {
	  edge_potential_generation_time.start();
	  bpchi_pcpu::current_step = step_gen_edge_potential;
	  x_lib::do_stream< belief_propagation_graphchi<F>, 
			    init_edge_wrapper<F>, 
			    belief_edge_wrapper >
	    (graph_storage, 0, init_stream, updates0_stream, NULL, false);
	  edge_potential_generation_time.stop();
	  // Supersteps
	  PHASE = 0;
	  iters = 0;
	  graph_storage->rewind_stream(updates0_stream);
	  if(vm.count("destroy_init") > 0) {
	    graph_storage->reset_stream(init_stream, 0);
	  }
	}
	while(restored || (iters < niters)) {
	  if(!restored) {
	    iters++;
	  }
	  unsigned long updates_in_stream;
	  unsigned long updates_out_stream; 
	  if(!restored) {
	    x_lib::take_checkpoint<belief_propagation_graphchi<F> >
	      (graph_storage, this);
	  }
	  else {
	    restored = false;
	  }
	  updates_in_stream  = (PHASE == 0 ? updates1_stream:updates0_stream);
	  updates_out_stream = (PHASE == 0 ? updates0_stream:updates1_stream);
	  for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	    graph_storage->state_load(i);
	    bpchi_pcpu::current_step = step_init;
	    x_lib::do_state_iter<belief_propagation_graphchi<F> > (graph_storage, i);
	    graph_storage->state_store(i);
	  }
	  for(unsigned long i=0;i<graph_storage->get_config()->super_partitions;i++) {
	    graph_storage->state_load(i);
	    bpchi_pcpu::current_step = step_absorb;
	    x_lib::do_stream<belief_propagation_graphchi<F>, 
			     belief_edge_wrapper,
			     belief_edge_wrapper >
	      (graph_storage, i, updates_in_stream, ULONG_MAX, NULL, true);
	    bpchi_pcpu::current_step = step_rescale;
	    x_lib::do_state_iter<belief_propagation_graphchi<F> > (graph_storage, i);
	    // need to replay the stream to determine updates
	    graph_storage->rewind_stream(updates_in_stream);
	    bpchi_pcpu::current_step = step_emit;
	    x_lib::do_stream<belief_propagation_graphchi<F>, 
			     belief_edge_wrapper,
			     belief_edge_wrapper >
	      (graph_storage, i, updates_in_stream, updates_out_stream, NULL, false);
	    graph_storage->reset_stream(updates_in_stream, i);
	    graph_storage->state_store(i);
	  }
	  graph_storage->rewind_stream(updates_out_stream);
	  PHASE = 1 - PHASE;
	  bpchi_pcpu::bsp_phase++;
	  if(heartbeat) {
	    BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " <<
	      bpchi_pcpu::bsp_phase;
	  }
	}
	bpchi_pcpu::current_step = step_terminate;
	x_lib::do_cpu<belief_propagation_graphchi<F> >(graph_storage, ULONG_MAX);
	setup_time.start();
	graph_storage->terminate();
	setup_time.stop();
	wall_clock.stop();
	BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << bpchi_pcpu::bsp_phase;
	setup_time.print("CORE::TIME::SETUP");
	edge_potential_generation_time.print("CORE::TIME::EDGE_POT_GEN");
	wall_clock.print("CORE::TIME::WALL");
      }
      
      template<typename F>
      void belief_propagation_graphchi<F>::partition_pre_callback(unsigned long superp, 
							 unsigned long partition,
							 per_processor_data *pcpu)
      {
	// Nothing
      }
      
      template<typename F>
      void belief_propagation_graphchi<F>::generate_initial_belief
      (unsigned char *edge, 
       struct belief_propagation_graphchi_edge* be_fwd,
       struct belief_propagation_graphchi_edge *be_rev)
      {
	vertex_t src, dst;
	weight_t weight;
	F::read_edge(edge, src, dst, weight);
	be_fwd->src = src;
	be_fwd->dst = dst;
	be_fwd->msg = computePriorP0(src);
	be_rev->src = dst;
	be_rev->dst = src;
	be_rev->msg = computePriorP0(dst);
      }
  
      template<typename F>
      void belief_propagation_graphchi<F>::partition_callback
      (x_lib::stream_callback_state *callback)
      {
	const double EPSILON = 0.05;
	double PHI[2][2] = { {1-EPSILON, EPSILON}, {0.5, 0.5} };
	bpchi_pcpu *pcpu = static_cast<bpchi_pcpu *>(callback->cpu_state);
	switch(bpchi_pcpu::current_step) {
	case step_gen_edge_potential: {
	  unsigned long tmp = callback->bytes_in;
	  while(callback->bytes_in) {
	    if((callback->bytes_out + 2*sizeof(struct belief_propagation_graphchi_edge)) >
	       callback->bytes_out_max) {
	      break;
	    }
	    belief_propagation_graphchi_edge *e_fwd = 
	      (belief_propagation_graphchi_edge *)
	      (callback->bufout + callback->bytes_out);
	    belief_propagation_graphchi_edge *e_rev = 
	      (belief_propagation_graphchi_edge *)
	      (callback->bufout + callback->bytes_out + 
	       sizeof(struct belief_propagation_graphchi_edge));
	    generate_initial_belief(callback->bufin, e_fwd, e_rev);
	    callback->bytes_out += 2*sizeof(struct belief_propagation_graphchi_edge);
	    callback->bufin += F::split_size_bytes();
	    callback->bytes_in -= F::split_size_bytes();
	  }
	  pcpu->edge_bytes_streamed += (tmp - callback->bytes_in); 
	  break;
	}
	case step_absorb: {
	  pcpu->update_bytes_in += callback->bytes_in;
	  while(callback->bytes_in) {
	    belief_propagation_graphchi_edge *u = 
	      (belief_propagation_graphchi_edge *)(callback->bufin);
	    belief_propagation_graphchi_vertex *v = 
	      ((belief_propagation_graphchi_vertex *)(callback->state))
	      + x_lib::configuration::map_offset(u->dst);
	    v->logProdM0 += log(u->msg);
	    v->logProdM1 += log(1.0 - u->msg);
	    callback->bufin += sizeof(struct belief_propagation_graphchi_edge);
	    callback->bytes_in -= sizeof(struct belief_propagation_graphchi_edge);
	  }
	  break;
	}
	case step_emit: {
	  while(callback->bytes_in) {
	    if((callback->bytes_out + sizeof(belief_propagation_graphchi_edge)) > 
	       callback->bytes_out_max) {
	      break;
	    }
	    BOOST_ASSERT_MSG(callback->bytes_out < callback->bytes_out_max,
			     "Update buffer overflow !!!");
	    belief_propagation_graphchi_edge *ein = 
	      (belief_propagation_graphchi_edge *)(callback->bufin);
	    belief_propagation_graphchi_edge *eout = 
	      (belief_propagation_graphchi_edge *)(callback->bufout);
	    belief_propagation_graphchi_vertex *v = 
	      ((belief_propagation_graphchi_vertex *)(callback->state)) +
	      x_lib::configuration::map_offset(ein->dst);
	    eout->src = ein->dst;
	    eout->dst = ein->src;
	    double messageFrom0 = ein->msg;
	    double message0 = v->belief0 * PHI[0][0] / messageFrom0 + v->belief1 * PHI[1][0]/(1-messageFrom0);
	    double message1 = v->belief0 * PHI[0][1] / messageFrom0 + v->belief1 * PHI[1][1]/(1-messageFrom0);
	    // Rescale 
	    message0 /= (message0+message1); // Normalization
	    if (message0 < 1e-4) message0 = 1e-4;
	    else if (message0 > 0.9999) message0 = 0.9999;
	    eout->msg = message0;
	    callback->bufin     += sizeof(struct belief_propagation_graphchi_edge);
	    callback->bufout    += sizeof(struct belief_propagation_graphchi_edge);
	    callback->bytes_in  -= sizeof(struct belief_propagation_graphchi_edge);
	    callback->bytes_out += sizeof(struct belief_propagation_graphchi_edge);
	  }
	  pcpu->update_bytes_out    += callback->bytes_out;
	  break;
	}
	default:
	  BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
	  exit(-1);
	}
      }

      template<typename F>
      void belief_propagation_graphchi<F>::partition_post_callback(unsigned long superp, 
							  unsigned long partition,
							  per_processor_data *pcpu)
      {
	bpchi_pcpu *pcpu_actual = static_cast<bpchi_pcpu *>(pcpu);
	pcpu_actual->partitions_processed++;
      }

      template<typename F>
      bpchi_pcpu ** belief_propagation_graphchi<F>::pcpu_array = NULL;
      unsigned long bpchi_pcpu::bsp_phase = 0;
      unsigned long bpchi_pcpu::current_step;
    }
  }
}
#endif
