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

#include "../slipstore/interface.hpp"

class null_barrier_work:public slipstore::barrier_work {
public:
  virtual void operator() ()
  {}
};

void control_barrier()
{
  null_barrier_work null_obj;
  slipstore::slipstore_client_barrier->in_progress = true;
  slipstore::ioService.post
    (boost::bind(&slipstore::client_barrier::work_barrier,
		 slipstore::slipstore_client_barrier,
		 &null_obj));
  while(slipstore::slipstore_client_barrier->in_progress);
}

volatile unsigned long bthread_running = 0;

class interference {
  unsigned char *buffer;
public:
  interference()
  {
    buffer = (unsigned char *)map_anon_memory(512*1024*1024, true, "Dummy buf");
  }

  void operator() ()
  {
    (void)__sync_fetch_and_add(&bthread_running, 1UL);
    while(true) {
      for(unsigned long i=0;i<512*1024*1024;i++) {
	buffer[i]++;
      }
    }
  }
};


int main(int argc, const char **argv)
{
  slipstore::io *streams;
  rtc_clock fill_clock;
  rtc_clock drain_clock;
  slipstore::slipstore_req_t req;
  bool silent;
  /* Get options */
  setup_options(argc, argv);
  unsigned long slipchunk = vm["slipchunk"].as<unsigned long>();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPCHUNK "
			  << slipchunk;
  unsigned long slipfile_size = vm["slipbench_fsize"].as<unsigned long>();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPBENCH_FIZE "
			  << slipfile_size;
  unsigned long background_threads =
    vm["slipbench_background_threads"].as<unsigned long>();
  unsigned long align = vm["slipbench_align"].as<unsigned long>();
  slipchunk = (slipchunk/align)*align;
  unsigned long blockmem = vm["blocked_memory"].as<unsigned long>();
  /* Read in slipstore information */
  init_slipstore_desc();
  bool run_drain_test = (vm.count("drain_test") > 0);
  bool run_fill_test  = (vm.count("fill_test") > 0);
  streams = new slipstore::io(1, 1, 1, NULL);
  slipstore::init(streams, 100, 1); // tilesize does not matter
  unsigned char *buffer = new unsigned char[slipchunk];
  unsigned long fill_bytes = 0;
  unsigned char * big_buffer;
  unsigned long big_bufsize = 512*1024*1024;
  if(blockmem > 0) {
    BOOST_LOG_TRIVIAL(info) << "Blocking " << blockmem;
  }
  else {
    blockmem = big_bufsize;
    BOOST_LOG_TRIVIAL(info) << "Allocating " << blockmem;
  }
  if(blockmem < big_bufsize) {
    blockmem = big_bufsize;
  }
  big_buffer = (unsigned char *)map_anon_memory(blockmem, true, "Blockmem");
  BOOST_LOG_TRIVIAL(info) << "Done ";

  bool centralized = (vm.count("centralized") > 0);
  bool batching    = (vm.count("request_batching") > 0);
  
  unsigned long silenced_clients = vm["slipbench_silence"].as<unsigned long>();
  if(silenced_clients != ULONG_MAX &&
     slipstore::slipstore_client_drain->get_me() < silenced_clients) {
    silent = true;
  }
  else {
    silent = false;
  }

  if(background_threads > 0) {
    BOOST_LOG_TRIVIAL(info) << "Starting background threads";
    for(unsigned long i=0;i<background_threads;i++) {
      (void) new boost::thread(boost::ref(* new interference()));
    }
    while(bthread_running != background_threads);
    BOOST_LOG_TRIVIAL(info) << "done";
  }
  
  control_barrier(); // everyone started
  if(run_drain_test) {
    if(!silent) {
      BOOST_LOG_TRIVIAL(info) << "Starting drain test";
      drain_clock.start();
      unsigned long drain_bytes = slipfile_size;
      if(batching) {
	while(drain_bytes) {
	  req.cmd = slipstore::CMD_DRAIN;
	  req.stream = slipstore::STREAM_VERTEX_STATE;
	  req.partition = 0;
	  req.tile      = 0;
	  unsigned long bytes_to_drain= drain_bytes;
	  if(bytes_to_drain > big_bufsize) {
	    bytes_to_drain = big_bufsize;
	  }
	  if(slipstore::slipstore_client_drain->batch_access_store(&req,
								   big_buffer,
								   slipchunk,
								   bytes_to_drain)) {
	    BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
	    exit(-1);
	  }
	  drain_bytes -= bytes_to_drain;
	}
      }
      else {
	while(drain_bytes) {
	  unsigned long bytes;
	  bytes = (drain_bytes > slipchunk) ?
	    slipchunk:drain_bytes;
	  if(centralized) {
	    req.cmd = slipstore::CMD_NOP;
	    req.stream = slipstore::STREAM_VERTEX_STATE;
	    req.partition = 0;
	    req.tile      = 0;
	    req.size  = 0;
	    if(!slipstore::slipstore_client_drain->access_store(&req, buffer, 0)) {
	      BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
	      exit(-1);
	    }
	  }
	  req.cmd = slipstore::CMD_DRAIN;
	  req.stream = slipstore::STREAM_VERTEX_STATE;
	  req.partition = 0;
	  req.tile      = 0;
	  req.size  = bytes;
	  if(!slipstore::slipstore_client_drain->access_store(&req, buffer)) {
	    BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
	    exit(-1);
	  }
	  drain_bytes -= bytes;
	}
      }
      drain_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "Completed drain test";
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SILENCED !";
    }
    if(run_fill_test) {
      control_barrier();
      streams->rewind(0, 0, 0);
      control_barrier();
    }
  }
  if(run_fill_test) {
    streams->fill_test_prep(slipstore::STREAM_VERTEX_STATE);
    control_barrier();
    if(!silent) {
      BOOST_LOG_TRIVIAL(info) << "Starting fill test";
      fill_clock.start();
      if(batching) {
	unsigned long filled_bytes;
	do {
	  req.cmd = slipstore::CMD_FILL;
	  req.stream = slipstore::STREAM_VERTEX_STATE;
	  req.partition = 0;
	  req.tile      = 0;
	  filled_bytes = 
	    slipstore::slipstore_client_fill->batch_access_store(&req,
								  big_buffer,
								  slipchunk,
								  big_bufsize);
	  fill_bytes += filled_bytes;
	} while(filled_bytes > 0);
      }
      else {
	do {
	  if(centralized) {
	    req.cmd = slipstore::CMD_NOP;
	    req.stream = slipstore::STREAM_VERTEX_STATE;
	    req.partition = 0;
	    req.tile      = 0;
	    req.size  = 0;
	    if(!slipstore::slipstore_client_drain->access_store(&req, buffer, 0)) {
	      BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
	      exit(-1);
	    }
	  }
	  req.cmd = slipstore::CMD_FILL;
	  req.stream = slipstore::STREAM_VERTEX_STATE;
	  req.partition = 0;
	  req.tile      = 0;
	  req.size  = slipchunk;
	  if(!slipstore::slipstore_client_fill->access_store(&req, buffer)) {
	    break;
	  }
	  fill_bytes += req.size;
	}while(true);
      }
      fill_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "Completed fill test";
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SILENCED !";
    }
  }
  control_barrier(); // everyone done
  slipstore::shutdown();
  delete buffer;
  delete streams;
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::DRAIN_BYTES "
			  << slipfile_size;
  drain_clock.print("SLIPBENCH::DRAIN_TIME ");
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::FILL_BYTES "
			  << fill_bytes;
  fill_clock.print("SLIPBENCH::FILL_TIME ");
}
