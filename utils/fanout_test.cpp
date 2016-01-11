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

class null_barrier_work : public slipstore::barrier_work {
public:
    virtual void operator()() { }
};

slipstore::cyclic *choice;


void control_barrier() {
  null_barrier_work null_obj;
  slipstore::slipstore_client_barrier->in_progress = true;
  slipstore::ioService.post
      (boost::bind(&slipstore::client_barrier::work_barrier,
                   slipstore::slipstore_client_barrier,
                   &null_obj));
  while (slipstore::slipstore_client_barrier->in_progress);
}

unsigned long slipchunk;

void drain_test(unsigned char *buffer, unsigned long amount) {
  slipstore::slipstore_req_t req;
  while (amount) {
    unsigned long bytes;
    bytes = (amount > slipchunk) ?
            slipchunk : amount;
    req.cmd = slipstore::CMD_DRAIN;
    req.stream = slipstore::STREAM_VERTEX_STATE;
    req.partition = 0;
    req.tile = choice->cyclic_next();
    req.size = bytes;
    if (!slipstore::slipstore_client_drain->access_store(&req, buffer)) {
      BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
      exit(-1);
    }
    amount -= bytes;
  }
}

int main(int argc, const char **argv) {
  slipstore::io *streams;
  rtc_clock prefill_clock;
  rtc_clock drain_clock;
  /* Get options */
  setup_options(argc, argv);
  slipchunk = vm["slipchunk"].as < unsigned
  long > ();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::SLIPCHUNK "
  << slipchunk;
  unsigned long slipfile_size = vm["slipbench_fsize"].as < unsigned
  long > ();
  unsigned long blockmem = vm["blocked_memory"].as < unsigned
  long > ();
  unsigned long prefill = vm["test_prefill"].as < unsigned
  long > ();
  /* Read in slipstore information */
  init_slipstore_desc();
  unsigned long fanout = vm["ext_fanout"].as < unsigned
  long > ();
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::FANOUT_UNDER_TEST "
  << fanout;
  streams = new slipstore::io(1, 1, fanout, NULL);
  slipstore::init(streams, 100, 1); // tilesize does not matter
  unsigned char *buffer = new unsigned char[slipchunk];
  choice = new slipstore::cyclic(fanout,
                                 slipstore::slipstore_client_drain->get_me());
  if (blockmem > 0) {
    BOOST_LOG_TRIVIAL(info) << "Blocking " << blockmem;
    (void) map_anon_memory(blockmem, true, "Blockmem");
    BOOST_LOG_TRIVIAL(info) << "Done";
  }
  unsigned long drain_bytes = slipfile_size;
  if (drain_bytes < 2 * fanout * slipchunk) {
    drain_bytes = 2 * fanout * slipchunk;
  }
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::PREFILL "
  << prefill;
  BOOST_LOG_TRIVIAL(info) << "SLIPBENCH::DRAIN_AMOUNT "
  << drain_bytes;
  /////////// Prefill
  control_barrier(); // everyone started
  prefill_clock.start();
  drain_test(buffer, prefill);
  prefill_clock.stop();
  control_barrier(); // everyone done
  prefill_clock.print("SLIPBENCH::PREFILL_TIME ");
  ////////// Drain test
  control_barrier(); // everyone started
  drain_clock.start();
  drain_test(buffer, drain_bytes);
  control_barrier(); // everyone done
  // Flush the remaining blocks
  for (unsigned long i = 0; i < fanout; i++) {
    streams->rewind(slipstore::STREAM_VERTEX_STATE, 0, i);
  }
  drain_clock.stop();
  drain_clock.print("SLIPBENCH::DRAIN_TIME ");
  ///////////////////////
  slipstore::shutdown();
  delete buffer;
  delete streams;

}
