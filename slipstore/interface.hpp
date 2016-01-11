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

#ifndef _STORE_INTERFACE_
#define _STORE_INTERFACE_

#include "../utils/desc_utils.h"
#include "../utils/options_utils.h"
#include "io.hpp"
#include<boost/random.hpp>
#include<boost/thread.hpp>
#include<zmq.h>
#include<netinet/ip.h>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>

// Interface for the slipstorage service

namespace slipstore {
  extern void *zmq_context;
  extern void *zmq_context_data_out;
  extern void *zmq_context_data_in;
  extern unsigned long baseport;
  extern boost::asio::io_service ioService;
  extern boost::thread_group threadpool;
  extern boost::asio::io_service dioService;
  extern boost::thread_group diopool;

  const std::string protocol = "tcp";
  const std::string separator = ":";

  // Number of endpoints
  static unsigned long endpoint_count(unsigned long machines) {
    return machines + //server data in
           machines + //client data in
           1 + // server control
           1;  // barrier
  }

  // Utility functions to generate endpoints for connections
  static
  std::string conn_server_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_server_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://server";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_client_data_endpoint(unsigned long mc_target,
                                        unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_client_data_bindpoint(unsigned long mc_target,
                                         unsigned long mc_source) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    if (mc_target == mc_source) {
      addr << "ipc://client";
    } else {
      addr << protocol << "://";
      addr << pt_slipstore.get<std::string>(name.str().c_str());
      unsigned long machines = pt_slipstore.get < unsigned
      long > ("machines.count");
      unsigned long port = baseport + endpoint_count(machines) * mc_target +
                           machines + mc_source;
      addr << separator << port;
    }
    return addr.str();
  }

  static
  std::string conn_server_control_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_server_control_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_client_barrier_endpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.name" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }

  static
  std::string conn_client_barrier_bindpoint(unsigned long mc_target) {
    std::stringstream name;
    std::stringstream addr;
    name << "machines.iface" << mc_target;
    addr << protocol << "://";
    addr << pt_slipstore.get<std::string>(name.str().c_str());
    unsigned long machines = pt_slipstore.get < unsigned
    long > ("machines.count");
    unsigned long port = baseport + endpoint_count(machines) * mc_target +
                         2 * machines + 1;
    addr << separator << port;
    return addr.str();
  }

  static
  void slipstore_bind_endpoint(void *socket, const char *endpoint) {
    int rc = zmq_bind(socket, endpoint);
    if (rc != 0) {
      BOOST_LOG_TRIVIAL(fatal) <<
      "SLIPSTORE::COMM Unable to setup listening socket at "
      << endpoint;
      perror("zmq_bind:");
      exit(-1);
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Listening at "
      << endpoint;
    }
  }

  static
  void slipstore_connect_endpoint(void *socket, const char *endpoint) {
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::COMM Connecting to " <<
    endpoint;
    zmq_connect(socket, endpoint);
  }

  static
  void slipstore_mark_low_latency(void *socket) {
#ifdef LOW_LATENCY_CONTROL
    int tos_value = IPTOS_LOWDELAY;
    int ret = zmq_setsockopt(socket, ZMQ_TOS, &tos_value, sizeof(tos_value));
    if(ret == -1) {
      BOOST_LOG_TRIVIAL(warn) 
  << "SLIPSTORE: Unable to make socket low latency: "
  << strerror(errno);
    }
#endif
  }

  static
  unsigned long do_zmq_send(void *socket,
                            unsigned char *data,
                            unsigned long size,
                            const char *context) {
    while (true) {
      int rc = zmq_send(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to transmit";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return size;
  }

  static
  unsigned long do_zmq_recv(void *socket,
                            unsigned char *data,
                            unsigned long size,
                            const char *context) {
    int rc;
    while (true) {
      rc = zmq_recv(socket, data, size, 0);
      if (rc == -1) {
        if (errno != EAGAIN) {
          BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
          perror(context);
          exit(-1);
        }
        // Retry
      }
      else {
        break;
      }
    }
    return (unsigned long) rc;
  }

  static
  unsigned long do_zmq_recv_noblock(void *socket,
                                    unsigned char *data,
                                    unsigned long size,
                                    const char *context) {
    int rc;
    rc = zmq_recv(socket, data, size, ZMQ_NOBLOCK);

    if (rc == -1 && errno != EAGAIN) {
      BOOST_LOG_TRIVIAL(fatal) << "SLIPSTORE: Unable to receive";
      perror(context);
      exit(-1);
    }
    return (unsigned long) rc;
  }

  class random_selector {
      unsigned long *permutation;
      unsigned long machines;
      unsigned long active_machines;
      struct drand48_data rand_buffer;
  public:
      random_selector(unsigned long machines_in, unsigned long me)
          : machines(machines_in), active_machines(machines_in) {
        srand48_r(me, &rand_buffer);
        permutation = new unsigned long[machines];
        for (unsigned long i = 0; i < machines; i++) {
          permutation[i] = i;
        }
      }

      unsigned long select() {
        if (active_machines == 0) {
          return ULONG_MAX;
        }
        double r;
        drand48_r(&rand_buffer, &r);
        unsigned long index = (unsigned long) (r * active_machines);
        unsigned long mc = permutation[index];
        permutation[index] = permutation[--active_machines];
        return mc;
      }

      bool have_options() {
        return (active_machines != 0);
      }

      void deselect(unsigned long mc) {
        permutation[active_machines++] = mc;
      }

      void deselect_all() {
        for (unsigned long i = 0; i < machines; i++) {
          permutation[i] = i;
        }
        active_machines = machines;
      }

      ~random_selector() {
        delete permutation;
      }
  };

  class cyclic {
      unsigned long *permutation;
      unsigned long cyclic_index;
      unsigned long machines;
      struct drand48_data rand_buffer;
  public:
      cyclic(unsigned long machines_in, unsigned long me)
          : machines(machines_in) {
        srand48_r(me, &rand_buffer);
        permutation = new unsigned long[machines];
        permutation[0] = 0;
        for (unsigned long i = 1; i < machines; i++) {
          permutation[i] = i;
          double r;
          drand48_r(&rand_buffer, &r);
          unsigned long interchange = (unsigned long) (r * (i + 1));
          unsigned long tmp = permutation[interchange];
          permutation[interchange] = permutation[i];
          permutation[i] = tmp;
        }
        cyclic_index = 0;
      }

      unsigned long cyclic_next() {
        unsigned long mc = permutation[cyclic_index];
        cyclic_index = (cyclic_index + 1) % machines;
        return mc;
      }

      unsigned long cycle_size() {
        return machines;
      }

      ~cyclic() {
        delete permutation;
      }
  };

  class help_heuristics {
      bool policy_help_all;
      bool policy_help_none;
      bool policy_heuristic_new;
      double alpha;
      bool sorted_order;
      unsigned long tile_size;
      unsigned long machines;
      io *disks;
      unsigned long partitions;
      unsigned long *partition_order;
  public:
      unsigned long help_offers;
      unsigned long help_offers_accepted;

      help_heuristics(unsigned long tile_size_in,
                      unsigned long machines_in,
                      io *disks_in,
                      unsigned long partitions_in)
          : tile_size(tile_size_in),
            machines(machines_in),
            disks(disks_in),
            partitions(partitions_in) {
        policy_help_all = (vm.count("policy_help_all") > 0);
        policy_help_none = (vm.count("policy_help_none") > 0);
        policy_heuristic_new = (vm.count("policy_heuristic_new") > 0);
        alpha = atof(vm["alpha"].as<std::string>().c_str());
        sorted_order = (vm.count("sorted_order") > 0);
        if (policy_help_all) {
          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY ALL";
        }
        else if (policy_help_none) {
          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY NONE";
        }
        else if (policy_heuristic_new) {
          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY HEURISTIC_NEW";
        }
        else {
          BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_POLICY HEURISTIC";
        }
        partition_order = new unsigned long[partitions_in];
        help_offers = 0;
        help_offers_accepted = 0;
      }

      unsigned long think(slipstore_req_t *req,
                          unsigned long current_workers) {
        bool offer_help_sync = (req->size == 1);
        help_offers++;
        unsigned long resp;
        if (policy_help_all) {
          resp = RESP_ACK;
        }
        else if (policy_help_none) {
          resp = RESP_NACK;
        }
        else if (policy_heuristic_new) {
          unsigned long tile_sync_cost =
              offer_help_sync ? tile_size : 0;
          unsigned long bytes_left = machines *
                                     (double) disks->get_bytes_left(req->stream,
                                                                    req->partition,
                                                                    req->tile);
          double load_estimate = ((double) bytes_left) / current_workers;
          // New load estimate
          double new_load_estimate =
              // Copy and help
              (bytes_left + tile_size) /
              (1.0 + (double) current_workers) +
              // Sync serially
              ((double) tile_sync_cost);
          if (new_load_estimate < alpha * load_estimate) {
            resp = RESP_ACK;
          }
          else {
            resp = RESP_NACK;
          }
        }
        else {
          unsigned long tile_sync_cost =
              offer_help_sync ? tile_size : 0;
          double load_estimate = machines *
                                 (double) disks->get_bytes_left(req->stream,
                                                                req->partition,
                                                                req->tile);
          // New load estimate
          double new_load_estimate =
              (load_estimate + tile_size + tile_sync_cost) /
              (1.0 + 1.0 / (double) current_workers);
          if (new_load_estimate < load_estimate) {
            resp = RESP_ACK;
          }
          else {
            resp = RESP_NACK;
          }
        }
        if (resp == RESP_ACK) {
          help_offers_accepted++;
        }
        return resp;
      }

      void init_order(unsigned long stream) {
        if (stream == ULONG_MAX || !sorted_order) {
          for (unsigned long i = 0; i < partitions; i++) {
            partition_order[i] = i;
          }
          return;
        }
        // Simple insertion sort
        partition_order[0] = 0;
        for (unsigned long i = 1; i < partitions; i++) {
          unsigned long key = disks->get_bytes_left(stream, i, 0);
          unsigned long j;
          for (j = i; j != 0; j--) {
            if (disks->get_bytes_left(stream, partition_order[j - 1], 0) < key) {
              partition_order[j] = partition_order[j - 1];
            }
            else {
              break;
            }
          }
          partition_order[j] = i;
        }
      }

      unsigned long order_next(unsigned long index) {
        return partition_order[index];
      }

      void dump_stats() {
        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_OFFERS_RECVD "
        << help_offers;
        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_OFFERS_ACCEPTED "
        << help_offers_accepted;
        double ratio;
        if (help_offers > 0) {
          ratio = ((double) help_offers_accepted) / help_offers;
        }
        else {
          ratio = ((double) 0);
        }
        BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::HELP_ACCEPTANCE_RATIO "
        << ratio;
      }
  };

  class help_base {
  public:
      virtual void setup(unsigned long superps) = 0;

      virtual unsigned long next_helper(unsigned long superp) = 0;

      virtual unsigned long make_decision(slipstore_req_t *req) = 0;

      virtual void dump_stats() = 0;

      virtual void reset() = 0;

      virtual void close(unsigned long superp) = 0;

      virtual void sem_down(unsigned long mc) = 0;

      virtual void sem_up(unsigned long mc) = 0;

      virtual void init_order(unsigned long stream) = 0;

      virtual unsigned long order_next(unsigned long index) = 0;

      virtual ~help_base() { }
  };

  class help_original : public help_base {
      // Track helping
      bool *helped_me;
      cyclic *helper_cycle;
      unsigned long machines;
      unsigned long me;
      help_heuristics heuristics;

      unsigned long count_helpers() {
        unsigned long cnt = 0;
        for (unsigned long i = 0; i < machines; i++) {
          cnt += (helped_me[i] ? 1 : 0);
        }
        return cnt;
      }

  public:
      help_original(unsigned long tile_size_in,
                    unsigned long machines_in,
                    unsigned long me_in,
                    io *disks_in,
                    unsigned long partitions_in)
          : machines(machines_in),
            me(me_in),
            heuristics(tile_size_in, machines_in, disks_in, partitions_in) {
        helped_me = new bool[machines];
        helper_cycle = new cyclic(machines, me);
      }

      // Help tracking stuff
      void reset() {
        for (unsigned long i = 0; i < machines; i++) {
          helped_me[i] = false;
        }
      }

      virtual unsigned long next_helper(unsigned long next_superp) {
        for (unsigned long i = 0; i < machines; i++) {
          unsigned long mc = helper_cycle->cyclic_next();
          if (mc == me) { continue; }
          if (helped_me[mc]) {
            helped_me[mc] = false;
            return mc;
          }
        }
        return ULONG_MAX;
      }

      virtual unsigned long make_decision(slipstore_req_t *req) {
        unsigned long resp;
        resp = heuristics.think(req, count_helpers() + 1); // workers includes me
        if (resp == RESP_ACK) {
          helped_me[req->source_mc] = true;
        }
        return resp;
      }

      virtual void dump_stats() {
        heuristics.dump_stats();
      }

      virtual void close(unsigned long superp) {
        BOOST_LOG_TRIVIAL(fatal) << "Close not allowed in original helper";
      }

      virtual void setup(unsigned long superps) {
        BOOST_LOG_TRIVIAL(fatal) << "Setup not allowed in original helper";
      }

      virtual void sem_down(unsigned long mc) {
        BOOST_LOG_TRIVIAL(fatal) << "await_release not allowed in original helper";
      }

      virtual void sem_up(unsigned long mc) {
        BOOST_LOG_TRIVIAL(fatal) << "release not allowed in original helper";
      }

      virtual void init_order(unsigned long stream) {
        heuristics.init_order(stream);
      }

      virtual unsigned long order_next(unsigned long index) {
        return heuristics.order_next(index);
      }

      virtual ~help_original() {
      }
  };

  class help_skip : public help_base {
      // Track helping
      bool **helped_me;
      bool *closed;
      cyclic *helper_cycle;
      unsigned long machines;
      unsigned long super_partitions;
      unsigned long me;
      help_heuristics heuristics;
      volatile unsigned long lock;
      volatile int *semaphores;

      unsigned long count_helpers(unsigned long superp) {
        unsigned long cnt = 0;
        for (unsigned long i = 0; i < machines; i++) {
          cnt += (helped_me[superp][i] ? 1 : 0);
        }
        return cnt;
      }

      void lock_state() {
        do {
          while (lock == 1);
        } while (!__sync_bool_compare_and_swap(&lock, 0, 1));
      }

      void unlock_state() {
        __sync_synchronize();
        lock = 0;
      }

  public:
      help_skip(unsigned long tile_size_in,
                unsigned long machines_in,
                unsigned long me_in,
                io *disks_in,
                unsigned long partitions_in)
          : machines(machines_in),
            super_partitions(0),
            me(me_in),
            heuristics(tile_size_in, machines_in, disks_in, partitions_in) {
        lock = 0;
        semaphores = new int[machines];
        for (unsigned long i = 0; i < machines; i++) {
          semaphores[i] = 0;
        }
        helped_me = NULL;
        closed = NULL;
        helper_cycle = new cyclic(machines, me);
      }

      virtual void setup(unsigned long super_partitions_in) {
        super_partitions = super_partitions_in;
        helped_me = new bool *[super_partitions];
        closed = new bool[super_partitions];
        for (unsigned long i = 0; i < super_partitions; i++) {
          helped_me[i] = new bool[machines];
          closed[i] = false;
          for (unsigned long j = 0; j < machines; j++) {
            helped_me[i][j] = false;
          }
        }
      }

      // Help tracking stuff
      virtual void reset() {
        for (unsigned long i = 0; i < super_partitions; i++) {
          closed[i] = false;
          for (unsigned long j = 0; j < machines; j++) {
            helped_me[i][j] = false;
          }
        }
        __sync_synchronize();
      }

      virtual unsigned long next_helper(unsigned long superp) {
        for (unsigned long i = 0; i < machines; i++) {
          unsigned long mc = helper_cycle->cyclic_next();
          if (mc == me) { continue; }
          if (helped_me[superp][mc]) {
            helped_me[superp][mc] = false;
            return mc;
          }
        }
        return ULONG_MAX;
      }

      virtual unsigned long make_decision(slipstore_req_t *req) {
        unsigned long resp;
        lock_state();
        if (!closed[req->partition]) {
          resp = heuristics.think(req, count_helpers(req->partition) + 1); // workers includes me
        }
        else {
          heuristics.help_offers++;
          resp = RESP_NACK;
        }
        if (resp == RESP_ACK) {
          helped_me[req->partition][req->source_mc] = true;
        }
        unlock_state();
        return resp;
      }

      virtual void dump_stats() {
        heuristics.dump_stats();
      }

      virtual void close(unsigned long superp) {
        lock_state();
        closed[superp] = true;
        unlock_state();
      }

      virtual void sem_down(unsigned long mc) {
        (void) __sync_fetch_and_sub(&semaphores[mc], 1);
        while (semaphores[mc] < 0);
      }

      virtual void sem_up(unsigned long mc) {
        (void) __sync_fetch_and_add(&semaphores[mc], 1);
      }

      virtual void init_order(unsigned long stream) {
        heuristics.init_order(stream);
      }

      virtual unsigned long order_next(unsigned long index) {
        return heuristics.order_next(index);
      }

      virtual ~help_skip() {
        delete (closed);
        delete (semaphores);
        for (unsigned long i = 0; i < super_partitions; i++) {
          delete (helped_me[i]);
        }
        delete (helped_me);
      }
  };


  class server {
  protected:
      unsigned long machines;
      unsigned long me;
      // Command sockets
      void *socket_cmd_in;
      // Data sockets
      void **sockets_data_out;
      void **sockets_data_in;
      // Slipstore chunk size
      unsigned long slipchunk;
      // Handle for IO code
      io *disks;
      // Handle for help heuristics
      help_base *help_stuff;

      void setup_conn() {
        /////////////////////////////////// Command sockets
        socket_cmd_in = zmq_socket(zmq_context, ZMQ_REP);
        slipstore_mark_low_latency(socket_cmd_in);
        slipstore_bind_endpoint(socket_cmd_in, conn_server_control_bindpoint(me).c_str());
        ////////////////////////////////// Data sockets
        sockets_data_in = new void *[machines];
        sockets_data_out = new void *[machines];
        for (unsigned long i = 0; i < machines; i++) {
          sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_PULL);
          slipstore_bind_endpoint(sockets_data_in[i],
                                  conn_server_data_bindpoint(me, i).c_str());
          //sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_REP);
          sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_PUSH);
          slipstore_connect_endpoint(sockets_data_out[i],
                                     conn_client_data_endpoint(i, me).c_str());
        }
      }

      void teardown_conn() {
        zmq_close(socket_cmd_in);
        for (unsigned long i = 0; i < machines; i++) {
          zmq_close(sockets_data_in[i]);
          zmq_close(sockets_data_out[i]);
        }
      }

  public:
      server(io *disks_in, unsigned long tile_size_in, unsigned long partitions) {
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        disks = disks_in;
        //help_stuff = new help_original(tile_size_in, machines, me, disks, partitions);
        help_stuff = new help_skip(tile_size_in, machines, me, disks, partitions);
      }

      // Trapdoor to the help
      help_base *help_handle() {
        return help_stuff;
      }

      // Server
      virtual void operator()() {
        delete (help_stuff);
        BOOST_LOG_TRIVIAL(fatal) << "Called operator () on base server class!";
        exit(-1);
      };
  };

  class server_sync : public server {
  public:
      server_sync(io *disks_in,
                  unsigned long tile_size_in,
                  unsigned long partitions_in)
          : server(disks_in, tile_size_in, partitions_in) { };

      virtual void operator()() {
        setup_conn(); // Sockets should be set up in the same thread
        unsigned char *buffer = new unsigned char[slipchunk];
        rtc_clock total_time;
        rtc_clock busy_time_disk;
        rtc_clock busy_time_mem;
        rtc_clock *current_busy_time;
        bool polling_server = (vm.count("polling_server") > 0);
        total_time.start();
        busy_time_disk.start();
        current_busy_time = &busy_time_disk;
        while (true) {
          // Get cmd
          slipstore_req_t req;
          unsigned long requestor;
          unsigned long resp;
          current_busy_time->stop();
          if (polling_server) {
            long rc;
            do {
              rc = do_zmq_recv_noblock(socket_cmd_in,
                                       (unsigned char *) &req,
                                       sizeof(slipstore_req_t),
                                       "server recv cmd:");
            } while (rc < 0);
          }
          else {
            (void) do_zmq_recv(socket_cmd_in,
                               (unsigned char *) &req,
                               sizeof(slipstore_req_t),
                               "server recv cmd:");
          }
          if (req.cmd == CMD_FILL && req.stream == STREAM_MEMBUFFER) {
            busy_time_mem.start();
            current_busy_time = &busy_time_mem;
          }
          else {
            busy_time_disk.start();
            current_busy_time = &busy_time_disk;
          }
          requestor = req.source_mc;
          if (req.cmd == CMD_TERMINATE) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            BOOST_LOG_TRIVIAL(info) << "SLIPSTORE service is exiting";
            break;
          }
          else if (req.cmd == CMD_NOP) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_ORDER_INIT) {
            help_stuff->init_order(req.stream);
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_SEMUP) {
            resp = RESP_ACK;
            help_stuff->sem_up(req.source_mc);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_OFFER_HELP) {
            resp = help_stuff->make_decision(&req);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_EOF) {
            // Check whether stream is empty
            if (disks->eof_stream(&req)) {
              resp = RESP_ACK;
            }
            else {
              resp = RESP_NACK;
            }
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_RESET) {
            // Reset stream
            resp = RESP_ACK;
            disks->trunc(req.stream, req.partition, req.tile);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          // Check size of request
          if (req.size > slipchunk && req.cmd != CMD_ORDER_NEXT) {
            BOOST_LOG_TRIVIAL(warn)
            << "SLIPSTORE: Rejecting request sized = "
            << req.size
            << " larger than slipchunk = "
            << slipchunk
            << " from: " << req.source_mc;
            resp = RESP_NACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          switch (req.cmd) {
            case CMD_ORDER_NEXT:
              resp = RESP_ACK;
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              *(unsigned long *) buffer = help_stuff->order_next(req.partition);
              (void) do_zmq_send(sockets_data_out[requestor],
                                 buffer,
                                 sizeof(unsigned long),
                                 "server data out:");
              break;
            case CMD_FILL:
              if (req.stream == STREAM_MEMBUFFER || !disks->eof(&req)) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->fill(buffer, &req);
                /*unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_SEEK_AND_FILL:
              resp = RESP_ACK;
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->seek_and_fill(buffer, &req);
                /*
                unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_DRAIN:
              // Check whether we have space left
              if (disks->have_drain_space(&req)) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then receive the data
              if (resp == RESP_ACK) {
                (void) do_zmq_recv(sockets_data_in[requestor],
                                   buffer,
                                   req.size,
                                   "server data in:");
                disks->drain(buffer, &req);
              }
              break;
            default:
              BOOST_LOG_TRIVIAL(fatal) << "Unknown command to slipstore service:"
              << req.cmd;
              exit(-1);
          }
        }
        current_busy_time->stop();
        total_time.stop();
        delete buffer;
        teardown_conn();
        help_stuff->dump_stats();
        busy_time_disk.print("SLIPSTORE::SERVER_BUSY_DISK ");
        busy_time_mem.print("SLIPSTORE::SERVER_BUSY_MEM ");
        total_time.print("SLIPSTORE::SERVER_TOTAL ");
      }
  };

  class server_async : public server {

  public:
      server_async(io *disks_in,
                   unsigned long tile_size_in,
                   unsigned long partitions_in)
          : server(disks_in, tile_size_in, partitions_in) { };

      // Server
      virtual void operator()() {
        rtc_clock total_time;
        rtc_clock busy_time_disk;
        rtc_clock busy_time_mem;
        rtc_clock *current_busy_time;
        setup_conn(); // Sockets should be set up in the same thread
        unsigned char *buffer = new unsigned char[slipchunk];
        bool polling_server = (vm.count("polling_server") > 0);
        // Store previous request (used to overlap IO and network)
        slipstore_req_t prev_req = {0};
        bool use_prev_req = false, skip_ack = false;
        total_time.start();
        busy_time_disk.start();
        current_busy_time = &busy_time_disk;
        while (true) {
          // Get cmd
          slipstore_req_t req;
          unsigned long requestor;
          unsigned long resp;

          // Is there a previous req?
          if (use_prev_req) { // If yes, then req = prev_req
            req = prev_req;
            use_prev_req = false;
          } else { // If no, receive new req
            current_busy_time->stop();
            if (polling_server) {
              long rc;
              do {
                rc = do_zmq_recv_noblock(socket_cmd_in,
                                         (unsigned char *) &req,
                                         sizeof(slipstore_req_t),
                                         "server recv cmd:");
              } while (rc < 0);
            }
            else {
              (void) do_zmq_recv(socket_cmd_in,
                                 (unsigned char *) &req,
                                 sizeof(slipstore_req_t),
                                 "server recv cmd:");
            }
            if (req.cmd == CMD_FILL && req.stream == STREAM_MEMBUFFER) {
              busy_time_mem.start();
              current_busy_time = &busy_time_mem;
            }
            else {
              busy_time_disk.start();
              current_busy_time = &busy_time_disk;
            }
          }

          requestor = req.source_mc;
          if (req.cmd == CMD_TERMINATE) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            BOOST_LOG_TRIVIAL(info) << "SLIPSTORE service is exiting";
            break;
          }
          else if (req.cmd == CMD_NOP) {
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_ORDER_INIT) {
            help_stuff->init_order(req.stream);
            resp = RESP_ACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_SEMUP) {
            resp = RESP_ACK;
            help_stuff->sem_up(req.source_mc);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_OFFER_HELP) {
            resp = help_stuff->make_decision(&req);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_EOF) {
            // Check whether stream is empty
            if (disks->eof_stream(&req)) {
              resp = RESP_ACK;
            }
            else {
              resp = RESP_NACK;
            }
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          else if (req.cmd == CMD_RESET) {
            // Reset stream
            resp = RESP_ACK;
            disks->trunc(req.stream, req.partition, req.tile);
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          // Check size of request
          if (req.size > slipchunk && req.cmd != CMD_ORDER_NEXT) {
            BOOST_LOG_TRIVIAL(warn)
            << "SLIPSTORE: Rejecting request sized = "
            << req.size
            << " larger than slipchunk = "
            << slipchunk
            << " from: " << req.source_mc;
            resp = RESP_NACK;
            (void) do_zmq_send(socket_cmd_in,
                               (unsigned char *) &resp,
                               sizeof(unsigned long),
                               "server resp out:");
            continue;
          }
          switch (req.cmd) {
            case CMD_ORDER_NEXT:
              resp = RESP_ACK;
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              *(unsigned long *) buffer = help_stuff->order_next(req.partition);
              (void) do_zmq_send(sockets_data_out[requestor],
                                 buffer,
                                 sizeof(unsigned long),
                                 "server data out:");
              break;
            case CMD_FILL:
              if (req.stream == STREAM_MEMBUFFER || !disks->eof(&req)) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->fill(buffer, &req);
                /*
                unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_SEEK_AND_FILL:
              resp = RESP_ACK;
              (void) do_zmq_send(socket_cmd_in,
                                 (unsigned char *) &resp,
                                 sizeof(unsigned long),
                                 "server resp out:");
              // If we responded positively then send the data
              if (resp == RESP_ACK) {
                unsigned long size = disks->seek_and_fill(buffer, &req);
                /*
                unsigned int dummy;
                (void) do_zmq_recv(sockets_data_out[requestor],
                 (unsigned char *)&dummy,
                 sizeof(int),
                 "dummy");
                */
                (void) do_zmq_send(sockets_data_out[requestor],
                                   buffer,
                                   size,
                                   "server data out:");
              }
              break;
            case CMD_DRAIN:
              // Check whether we have space left
              if (disks->have_drain_space(&req)) {
                resp = RESP_ACK;
              }
              else {
                resp = RESP_NACK;
              }

              if (!skip_ack) {
                (void) do_zmq_send(socket_cmd_in,
                                   (unsigned char *) &resp,
                                   sizeof(unsigned long),
                                   "server resp out:");
              } else {
                skip_ack = false;
              }
              // If we responded positively then receive the data
              if (resp == RESP_ACK) {
                // Read data
                (void) do_zmq_recv(sockets_data_in[requestor],
                                   buffer,
                                   req.size,
                                   "server data in:");

                // Peek
                long rc = do_zmq_recv_noblock(socket_cmd_in,
                                              (unsigned char *) &prev_req,
                                              sizeof(slipstore_req_t),
                                              "server recv cmd:");

                if (rc >= 0) {
                  // We have another request waiting
                  // Check if it's also a drain and we have enough space...
                  if (prev_req.cmd == CMD_DRAIN && disks->have_drain_space(prev_req.size + req.size)) {
                    resp = RESP_ACK;
                    (void) do_zmq_send(socket_cmd_in,
                                       (unsigned char *) &resp,
                                       sizeof(unsigned long),
                                       "server resp out:");
                    skip_ack = true;
                  } else {
                    skip_ack = false;
                  }
                  use_prev_req = true;
                } else {
                  skip_ack = false;
                  use_prev_req = false;
                }

                // Drain
                disks->drain(buffer, &req);
              }
              break;
            default:
              BOOST_LOG_TRIVIAL(fatal) << "Unknown command to slipstore service:"
              << req.cmd;
              exit(-1);
          }
        }
        current_busy_time->stop();
        total_time.stop();
        delete buffer;
        teardown_conn();
        help_stuff->dump_stats();
        busy_time_disk.print("SLIPSTORE::SERVER_BUSY_DISK ");
        busy_time_mem.print("SLIPSTORE::SERVER_BUSY_MEM ");
        total_time.print("SLIPSTORE::SERVER_TOTAL ");
      }
  };

  /* Fix some compiler oddities */
  class client_fill;

  class client_drain;

  class client_barrier;

  struct busy_counter {
      rtc_clock counter;
      unsigned long users;
      volatile unsigned long lock;

      busy_counter()
          : users(0),
            lock(0) { }

      void start() {
        while (!__sync_bool_compare_and_swap(&lock, 0, 1));
        users++;
        if (users == 1) {
          counter.start();
        }
        __sync_synchronize();
        __sync_bool_compare_and_swap(&lock, 1, 0);
      }

      void stop() {
        while (!__sync_bool_compare_and_swap(&lock, 0, 1));
        users--;
        if (users == 0) {
          counter.stop();
        }
        __sync_synchronize();
        __sync_bool_compare_and_swap(&lock, 1, 0);
      }
  };

  extern busy_counter *have_outstanding_request;

  class client_fill {
      // Command sockets
      void **sockets_cmd_out;
      // Data sockets
      void **sockets_data_in;

      unsigned long slipchunk;
      unsigned long machines;
      unsigned long me;
      cyclic *request_cycle;
      cyclic *reset_cycle;
      random_selector *server_selector;
      unsigned long batch_size;
      unsigned long *window_mc;
      unsigned long *size_mc;
      bool *ready;
      unsigned long disk_bytes_read;
      unsigned long mem_bytes_read;
      rtc_clock disk_read_time;
      rtc_clock mem_read_time;

      void setup_conn() {
        /////////////////////////////////// Command sockets
        sockets_cmd_out = new void *[machines];
        for (unsigned long i = 0; i < machines; i++) {
          sockets_cmd_out[i] = zmq_socket(zmq_context, ZMQ_REQ);
          slipstore_mark_low_latency(sockets_cmd_out);
          slipstore_connect_endpoint(sockets_cmd_out[i],
                                     conn_server_control_endpoint(i).c_str());
        }
        ////////////////////////////////// Data sockets
        sockets_data_in = new void *[machines];
        for (unsigned long i = 0; i < machines; i++) {
          //sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_REQ);
          sockets_data_in[i] = zmq_socket(zmq_context_data_in, ZMQ_PULL);
          slipstore_bind_endpoint(sockets_data_in[i],
                                  conn_client_data_bindpoint(me, i).c_str());
        }
      }

  public:

      void teardown_conn() {
        for (unsigned long i = 0; i < machines; i++) {
          zmq_close(sockets_cmd_out[i]);
          zmq_close(sockets_data_in[i]);
        }
      }

      client_fill() {
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        setup_conn();
        if (vm["slipstore_servers"].as < unsigned
        long > () == ULONG_MAX) {
          request_cycle = new cyclic(machines, me);
          reset_cycle = new cyclic(machines, me);
        }
        else {
          request_cycle =
              new cyclic(vm["slipstore_servers"].as < unsigned
          long > (), me);
          reset_cycle =
              new cyclic(vm["slipstore_servers"].as < unsigned
          long > (), me);
        }
        server_selector = new random_selector(machines, me + 128);
        batch_size = vm["batch_size"].as < unsigned
        long > ();
        window_mc = new unsigned long[batch_size];
        size_mc = new unsigned long[batch_size];
        ready = new bool[batch_size];
        disk_bytes_read = 0;
        mem_bytes_read = 0;
        for (unsigned long i = 0; i < batch_size; i++) {
          window_mc[i] = ULONG_MAX;
        }
      }

      unsigned long get_machines() {
        return machines;
      }

      unsigned long get_me() {
        return me;
      }

      unsigned long get_slipchunk() {
        return slipchunk;
      }

      bool check_empty(unsigned long stream) {
        bool empty = true;
        slipstore_req_t req;
        req.stream = stream;
        req.cmd = CMD_EOF;
        have_outstanding_request->start();
        disk_read_time.start();
        for (unsigned long i = 0; i < machines; i++) {
          // Issue request
          (void) do_zmq_send(sockets_cmd_out[i],
                             (unsigned char *) &req,
                             sizeof(slipstore_req_t),
                             "client cmd out");
          // Get reply
          unsigned long rep;
          (void) do_zmq_recv(sockets_cmd_out[i],
                             (unsigned char *) &rep,
                             sizeof(unsigned long),
                             "client rep in");
          empty = empty && (rep == RESP_ACK);
        }
        disk_read_time.stop();
        have_outstanding_request->stop();
        return empty;
      }

      void reset(unsigned long stream,
                 unsigned long partition,
                 unsigned long tile) {
        slipstore_req_t req;
        req.stream = stream;
        req.partition = partition;
        req.tile = tile;
        req.cmd = CMD_RESET;
        have_outstanding_request->start();
        disk_read_time.start();
        for (unsigned long i = 0; i < machines; i++) {
          unsigned long mc = reset_cycle->cyclic_next();
          // Issue request
          (void) do_zmq_send(sockets_cmd_out[mc],
                             (unsigned char *) &req,
                             sizeof(slipstore_req_t),
                             "client cmd out");
          // Get reply
          unsigned long rep;
          (void) do_zmq_recv(sockets_cmd_out[mc],
                             (unsigned char *) &rep,
                             sizeof(unsigned long),
                             "client rep in");
        }
        disk_read_time.stop();
        have_outstanding_request->stop();
      }

      void init_order(unsigned long stream) {
        slipstore_req_t req;
        req.stream = stream;
        req.cmd = CMD_ORDER_INIT;
        req.source_mc = me;
        // Issue request
        (void) do_zmq_send(sockets_cmd_out[me],
                           (unsigned char *) &req,
                           sizeof(slipstore_req_t),
                           "client cmd out");
        // Get reply
        unsigned long rep;
        (void) do_zmq_recv(sockets_cmd_out[me],
                           (unsigned char *) &rep,
                           sizeof(unsigned long),
                           "client rep in");
      }

      unsigned long order_next(unsigned long master, unsigned long index) {
        slipstore_req_t req;
        unsigned long nextp;
        req.cmd = CMD_ORDER_NEXT;
        req.source_mc = me;
        req.partition = index;
        // Issue request
        (void) do_zmq_send(sockets_cmd_out[master],
                           (unsigned char *) &req,
                           sizeof(slipstore_req_t),
                           "client cmd out");
        // Get reply
        unsigned long rep;
        (void) do_zmq_recv(sockets_cmd_out[master],
                           (unsigned char *) &rep,
                           sizeof(unsigned long),
                           "client rep in");

        // Get next order on data socket
        (void) do_zmq_recv(sockets_data_in[master],
                           (unsigned char *) &nextp,
                           sizeof(unsigned long),
                           "client data in");
        return nextp;
      }

      // Send a release message
      void send_semup(unsigned long mc) {
        slipstore_req_t req;
        req.cmd = CMD_SEMUP;
        req.source_mc = me;
        (void) do_zmq_send(sockets_cmd_out[mc],
                           (unsigned char *) &req,
                           sizeof(slipstore_req_t),
                           "client cmd out");
        // Get reply
        unsigned long rep;
        (void) do_zmq_recv(sockets_cmd_out[mc],
                           (unsigned char *) &rep,
                           sizeof(unsigned long),
                           "client rep in");
      }

      // Client: execute a data transfer command
      bool access_store(slipstore_req_t *req,
                        unsigned char *buffer,
                        unsigned long mc = ULONG_MAX) {
        unsigned long trials;
        unsigned long *bytes_read;
        rtc_clock *read_time;
        if (req->cmd == CMD_FILL && req->stream == STREAM_MEMBUFFER) {
          bytes_read = &mem_bytes_read;
          read_time = &mem_read_time;
        }
        else {
          bytes_read = &disk_bytes_read;
          read_time = &disk_read_time;
        }
        if (mc != ULONG_MAX) {
          trials = machines - 1;
        }
        else {
          trials = 0; // try all machines
          mc = request_cycle->cyclic_next();
        }
        req->source_mc = me;
        have_outstanding_request->start();
        read_time->start();
        do {
          // Issue request
          (void) do_zmq_send(sockets_cmd_out[mc],
                             (unsigned char *) req,
                             sizeof(slipstore_req_t),
                             "client cmd out");
          // Get reply
          unsigned long rep;
          (void) do_zmq_recv(sockets_cmd_out[mc],
                             (unsigned char *) &rep,
                             sizeof(unsigned long),
                             "client rep in");
          // If this was a terminate or nop command we are done
          if (req->cmd == CMD_TERMINATE || req->cmd == CMD_NOP) {
            have_outstanding_request->stop();
            read_time->stop();
            return true;
          }
          if (req->cmd == CMD_OFFER_HELP) {
            have_outstanding_request->stop();
            read_time->stop();
            return (rep == RESP_ACK);
          }
          BOOST_ASSERT_MSG(req->cmd == CMD_FILL || req->cmd == CMD_SEEK_AND_FILL,
                           "Non fill cmd to fill client");
          if (rep == RESP_ACK) {
            // Great, this machines agrees to serve
            /*
            unsigned int dummy = 0xdeadbeef;
            (void) do_zmq_send(sockets_data_in[mc],
                   (unsigned char *)&dummy,
                   sizeof(int),
                   "dummy");
            */
            req->size = do_zmq_recv(sockets_data_in[mc],
                                    buffer,
                                    req->size,
                                    "client data in");
            (*bytes_read) += req->size;
            have_outstanding_request->stop();
            read_time->stop();
            return true;
          }
          mc = request_cycle->cyclic_next();
        } while ((++trials) < request_cycle->cycle_size());
        read_time->stop();
        have_outstanding_request->stop();
        return false;
      }

      // Client: execute a data transfer command
      unsigned long batch_access_store(slipstore_req_t *req,
                                       unsigned char *buffer,
                                       unsigned long chunksize,
                                       unsigned long bufsize) {
        unsigned long offset = 0;
        unsigned long used_bytes = 0;
        unsigned long outstanding = 0;
        slipstore_req_t saved_req;
        req->source_mc = me;
        memcpy(&saved_req, req, sizeof(slipstore_req_t));
        disk_read_time.start();
        have_outstanding_request->start();
        bool do_polling = (vm.count("polling_client") > 0);
        do {
          if (outstanding == batch_size) {
            // Poll here
            if (!do_polling) {
              zmq_pollitem_t items[batch_size];
              for (unsigned long i = 0; i < batch_size; i++) {
                items[i].socket = sockets_cmd_out[window_mc[i]];
                items[i].events = ZMQ_POLLIN;
              }
              (void) zmq_poll(items, batch_size, -1);
            }
          }
          // Initiate as many transfers as possible
          for (unsigned long i = 0; i < batch_size; i++) {
            //Complete request
            if (window_mc[i] != ULONG_MAX && !ready[i]) {
              unsigned long rep;
              long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
                                            (unsigned char *) &rep,
                                            sizeof(unsigned long),
                                            "client rep in");
              if (rc >= 0) {
                if (rep == RESP_ACK) {
                  /*
                  unsigned int dummy = 0xdeadbeef;
                  (void) do_zmq_send(sockets_data_in[window_mc[i]],
                         (unsigned char *)&dummy,
                         sizeof(int),
                         "dummy");
                  */
                  ready[i] = true;
                }
                else {
                  used_bytes -= size_mc[i];
                  outstanding--;
                  window_mc[i] = ULONG_MAX;
                }
              }
            }
          }
          // Complete transfers and issue fresh requests
          for (unsigned long i = 0; i < batch_size; i++) {
            if (window_mc[i] != ULONG_MAX && ready[i]) {
              // Complete fill
              unsigned long recvd_bytes;
              recvd_bytes = do_zmq_recv(sockets_data_in[window_mc[i]],
                                        buffer + offset,
                                        size_mc[i],
                                        "client data in");
              server_selector->deselect(window_mc[i]);
              offset += recvd_bytes;
              used_bytes -= (size_mc[i] - recvd_bytes);
              outstanding--;
              window_mc[i] = ULONG_MAX;
            }
            //Make request
            if (window_mc[i] == ULONG_MAX && used_bytes <= (bufsize - chunksize)) {
              // Issue request
              window_mc[i] = server_selector->select();
              ready[i] = false;
              if (window_mc[i] != ULONG_MAX) {
                memcpy(req, &saved_req, sizeof(slipstore_req_t));
                req->size = chunksize;
                size_mc[i] = req->size;
                used_bytes += size_mc[i];
                (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
                                   (unsigned char *) req,
                                   sizeof(slipstore_req_t),
                                   "client cmd out");
                outstanding++;
              }
            }
          }
        } while (outstanding > 0);
        server_selector->deselect_all();
        disk_read_time.stop();
        have_outstanding_request->stop();
        disk_bytes_read += used_bytes;
        return used_bytes;
      }

      void print_times() {
        disk_read_time.print("SLIPSTORE::CLIENT::DISK_READ_TIME ");
        mem_read_time.print("SLIPSTORE::CLIENT::MEM_READ_TIME ");
      }

      unsigned long get_disk_bytes_read() {
        return disk_bytes_read;
      }

      unsigned long get_mem_bytes_read() {
        return mem_bytes_read;
      }
  };


  struct substream_tracker {
      // Buffer and index
      unsigned char *buffer;
      unsigned long index_entries;
      unsigned long skip_index;
      unsigned long cpus;
      unsigned long **index;
      unsigned long *per_cpu_end;
      unsigned long tiles;
      // Iterator state
      unsigned long next_index;
      unsigned long next_cpu;

      void init() {
        next_index = 0;
        next_cpu = 0;
      }

      void next_io(unsigned long stream,
                   slipstore_req_t *req,
                   unsigned char *iobuf,
                   unsigned long bufsize) {
        req->size = 0;
        while (next_index < index_entries) {
          if (next_index == skip_index) {
            next_index++;
            continue;
          }
          while (next_cpu < cpus) {
            unsigned long bytes_available;
            if (bufsize == 0) {
              return;
            }
            if (next_index < (index_entries - 1)) {
              bytes_available = index[next_cpu][next_index + 1] -
                                index[next_cpu][next_index];
            }
            else {
              bytes_available = per_cpu_end[next_cpu] -
                                index[next_cpu][next_index];
            }
            if (bytes_available == 0) {
              next_cpu++;
            }
            else {
              if (bytes_available > bufsize) {
                bytes_available = bufsize;
              }
              /* have some data for i/o */
              req->size += bytes_available;
              memcpy(iobuf,
                     buffer + index[next_cpu][next_index],
                     bytes_available);
              iobuf += bytes_available;
              bufsize -= bytes_available;
              req->cmd = slipstore::CMD_DRAIN;
              req->stream = stream;
              req->partition = next_index / tiles;
              req->tile = next_index % tiles;
              index[next_cpu][next_index] += bytes_available;
            }
          }
          next_cpu = 0;
          next_index++;
          if (req->size > 0) {
            return; // Limit req to one sp
          }
        }
      }
  };


  class client_drain {
      // Command sockets
      void **sockets_cmd_out;
      // Data sockets
      void **sockets_data_out;

      unsigned long slipchunk;
      unsigned long machines;
      unsigned long me;
      cyclic *request_cycle;
      random_selector *server_selector;
      unsigned long batch_size;
      unsigned long *window_mc;
      unsigned long *size_mc;
      unsigned char **buf_mc;
      slipstore_req_t *req_mc;
      bool *retry_mc;
      rtc_clock write_time;
      unsigned long bytes_written;

      void setup_conn() {
        /////////////////////////////////// Command sockets
        sockets_cmd_out = new void *[machines];
        for (unsigned long i = 0; i < machines; i++) {
          sockets_cmd_out[i] = zmq_socket(zmq_context, ZMQ_REQ);
          slipstore_mark_low_latency(sockets_cmd_out[i]);
          slipstore_connect_endpoint(sockets_cmd_out[i],
                                     conn_server_control_endpoint(i).c_str());
        }
        ////////////////////////////////// Data sockets
        sockets_data_out = new void *[machines];
        for (unsigned long i = 0; i < machines; i++) {
          sockets_data_out[i] = zmq_socket(zmq_context_data_out, ZMQ_PUSH);
          slipstore_connect_endpoint(sockets_data_out[i],
                                     conn_server_data_endpoint(i, me).c_str());
        }
      }

  public:

      void teardown_conn() {
        for (unsigned long i = 0; i < machines; i++) {
          zmq_close(sockets_cmd_out[i]);
          zmq_close(sockets_data_out[i]);
        }
      }

      client_drain() {
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        setup_conn();
        if (vm["slipstore_servers"].as < unsigned
        long > () == ULONG_MAX) {
          request_cycle = new cyclic(machines, me);
        }
        else {
          request_cycle =
              new cyclic(vm["slipstore_servers"].as < unsigned
          long > (), me);
        }
        server_selector = new random_selector(machines, me + 256);
        batch_size = vm["batch_size"].as < unsigned
        long > ();
        window_mc = new unsigned long[batch_size];
        if (vm.count("gather_io_drain") > 0) {
          buf_mc = new unsigned char *[batch_size];
          for (unsigned long i = 0; i < batch_size; i++) {
            buf_mc[i] = new unsigned char[slipchunk];
          }
          req_mc = new slipstore_req_t[batch_size];
        }
        size_mc = new unsigned long[batch_size];
        retry_mc = new bool[batch_size];
        bytes_written = 0;
        for (unsigned long i = 0; i < batch_size; i++) {
          window_mc[i] = ULONG_MAX;
          retry_mc[i] = false;
        }
      }

      unsigned long get_machines() {
        return machines;
      }

      unsigned long get_me() {
        return me;
      }

      unsigned long get_slipchunk() {
        return slipchunk;
      }

      // Client: execute a data transfer command
      bool access_store(slipstore_req_t *req,
                        unsigned char *buffer,
                        unsigned long mc = ULONG_MAX) {
        unsigned long trials;
        if (mc != ULONG_MAX) {
          trials = machines - 1;
        }
        else {
          trials = 0; // try all machines
          mc = request_cycle->cyclic_next();
        }
        req->source_mc = me;
        write_time.start();
        have_outstanding_request->start();
        do {
          // Issue request
          (void) do_zmq_send(sockets_cmd_out[mc],
                             (unsigned char *) req,
                             sizeof(slipstore_req_t),
                             "client cmd out");
          // Get reply
          unsigned long rep;
          (void) do_zmq_recv(sockets_cmd_out[mc],
                             (unsigned char *) &rep,
                             sizeof(unsigned long),
                             "client rep in");
          // If this was a terminate or nop command we are done
          if (req->cmd == CMD_TERMINATE ||
              req->cmd == CMD_NOP ||
              req->cmd == CMD_MEMREAD) {
            write_time.stop();
            have_outstanding_request->stop();
            return true;
          }
          BOOST_ASSERT_MSG(req->cmd == CMD_DRAIN,
                           "Non drain cmd to drain client");
          if (rep == RESP_ACK) {
            // Great, this machines agrees to serve
            bytes_written += req->size;
            (void) do_zmq_send(sockets_data_out[mc],
                               buffer,
                               req->size,
                               "client data out");
            write_time.stop();
            have_outstanding_request->stop();
            return true;
          }
          mc = request_cycle->cyclic_next();
        } while ((++trials) < request_cycle->cycle_size());
        write_time.stop();
        have_outstanding_request->stop();
        return false;
      }

      // Client: execute a data transfer command
      unsigned long batch_access_store(slipstore_req_t *req,
                                       unsigned char *buffer,
                                       unsigned long chunksize,
                                       unsigned long bufsize) {
        unsigned long offset = 0;
        unsigned long outstanding = 0;
        slipstore_req_t saved_req;
        req->source_mc = me;
        memcpy(&saved_req, req, sizeof(slipstore_req_t));
        bytes_written += bufsize;
        write_time.start();
        have_outstanding_request->start();
        bool do_polling = (vm.count("polling_client") > 0);
        do {
          if (outstanding == batch_size) {
            if (!do_polling) {
              // Poll here
              zmq_pollitem_t items[batch_size];
              for (unsigned long i = 0; i < batch_size; i++) {
                items[i].socket = sockets_cmd_out[window_mc[i]];
                items[i].events = ZMQ_POLLIN;
              }
              (void) zmq_poll(items, batch_size, -1);
            }
          }
          // Can do some work
          for (unsigned long i = 0; i < batch_size; i++) {
            //Complete request
            if (window_mc[i] != ULONG_MAX) {
              unsigned long rep;
              long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
                                            (unsigned char *) &rep,
                                            sizeof(unsigned long),
                                            "client rep in");
              if (rc >= 0) {
                if (rep == RESP_ACK) {
                  // Complete drain
                  (void) do_zmq_send(sockets_data_out[window_mc[i]],
                                     buffer + offset,
                                     size_mc[i],
                                     "client data out");
                  server_selector->deselect(window_mc[i]);
                  offset += size_mc[i];
                }
                else {
                  bufsize += size_mc[i];
                }
                outstanding--;
                window_mc[i] = ULONG_MAX;
              }
            }
            //Make request
            if (window_mc[i] == ULONG_MAX && bufsize > 0) {
              // Issue request
              window_mc[i] = server_selector->select();
              if (window_mc[i] != ULONG_MAX) {
                memcpy(req, &saved_req, sizeof(slipstore_req_t));
                if (chunksize > bufsize) {
                  req->size = bufsize;
                }
                else {
                  req->size = chunksize;
                }
                size_mc[i] = req->size;
                bufsize -= size_mc[i];
                (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
                                   (unsigned char *) req,
                                   sizeof(slipstore_req_t),
                                   "client cmd out");
                outstanding++;
              }
            }
          }
        } while (outstanding > 0);
        server_selector->deselect_all();
        write_time.stop();
        have_outstanding_request->stop();
        bytes_written -= bufsize;
        return bufsize;
      }

      // Client: execute a data transfer command
      bool batch_access_store_gather_io(substream_tracker *subtracker,
                                        unsigned long stream,
                                        unsigned long chunksize) {
        unsigned long outstanding = 0;
        unsigned long retry_outstanding = 0;
        write_time.start();
        have_outstanding_request->start();
        bool do_polling = (vm.count("polling_client") > 0);
        do {
          if (outstanding == batch_size) {
            if (!do_polling) {
              // Poll here
              zmq_pollitem_t items[batch_size];
              for (unsigned long i = 0; i < batch_size; i++) {
                items[i].socket = sockets_cmd_out[window_mc[i]];
                items[i].events = ZMQ_POLLIN;
              }
              (void) zmq_poll(items, batch_size, -1);
            }
          }
          // Can do some work
          for (unsigned long i = 0; i < batch_size; i++) {
            //Complete request
            if (window_mc[i] != ULONG_MAX) {
              unsigned long rep;
              long rc = do_zmq_recv_noblock(sockets_cmd_out[window_mc[i]],
                                            (unsigned char *) &rep,
                                            sizeof(unsigned long),
                                            "client rep in");
              if (rc >= 0) {
                if (rep == RESP_ACK) {
                  // Complete drain
                  (void) do_zmq_send(sockets_data_out[window_mc[i]],
                                     buf_mc[i],
                                     req_mc[i].size,
                                     "client data out");
                  server_selector->deselect(window_mc[i]);
                  window_mc[i] = ULONG_MAX;
                  outstanding--;
                }
                else {
                  window_mc[i] = ULONG_MAX;
                  retry_mc[i] = true;
                  retry_outstanding++;
                }
              }
            }
            //Make request
            if (window_mc[i] == ULONG_MAX && server_selector->have_options()) {
              // Issue request
              if (!retry_mc[i]) {
                subtracker->next_io(stream, &req_mc[i], buf_mc[i], chunksize);
              }
              if (req_mc[i].size != 0) {
                window_mc[i] = server_selector->select();
                req_mc[i].source_mc = me;
                (void) do_zmq_send(sockets_cmd_out[window_mc[i]],
                                   (unsigned char *) &req_mc[i],
                                   sizeof(slipstore_req_t),
                                   "client cmd out");
                if (!retry_mc[i]) {
                  outstanding++;
                }
                else {
                  retry_mc[i] = false;
                  retry_outstanding--;
                }
              }
            }
            if (retry_outstanding == outstanding &&
                retry_outstanding != 0 &&
                !server_selector->have_options()) {
              // Out of space !!!
              return false;
            }
          }
        } while (outstanding > 0);
        server_selector->deselect_all();
        write_time.stop();
        have_outstanding_request->stop();
        return true;
      }


      unsigned long get_bytes_written() {
        write_time.print("SLIPSTORE::CLIENT::WRITE_TIME ");
        return bytes_written;
      }

  };

  extern client_fill *slipstore_client_fill;
  extern client_drain *slipstore_client_drain;

  class barrier_work {
  public:
      virtual void operator()() = 0;
  };

  class client_barrier {
      // Barrier sockets
      void *socket_barrier_in;
      void *socket_barrier_out;
      void *socket_local_store;

      unsigned long slipchunk;
      unsigned long machines;
      unsigned long me;

      void setup_conn() {
        ////////////////////////////////// Barrier sockets
        socket_barrier_out = zmq_socket(zmq_context, ZMQ_PUSH);
        unsigned long barrier_ring_next = (me + 1) % machines;
        slipstore_connect_endpoint(socket_barrier_out,
                                   conn_client_barrier_endpoint(barrier_ring_next).c_str());
        socket_barrier_in = zmq_socket(zmq_context, ZMQ_PULL);
        slipstore_bind_endpoint(socket_barrier_in,
                                conn_client_barrier_bindpoint(me).c_str());
        ///////////////////////////////// Connection to local store
        socket_local_store = zmq_socket(zmq_context, ZMQ_REQ);
        slipstore_mark_low_latency(socket_local_store);
        slipstore_connect_endpoint(socket_local_store,
                                   conn_server_control_endpoint(me).c_str());
      }

      void flush_local_store() {
        slipstore_req_t req;
        req.cmd = CMD_NOP;
        // Issue request
        (void) do_zmq_send(socket_local_store,
                           (unsigned char *) &req,
                           sizeof(slipstore_req_t),
                           "client barrier cmd out");
        // Get reply
        unsigned long rep;
        (void) do_zmq_recv(socket_local_store,
                           (unsigned char *) &rep,
                           sizeof(unsigned long),
                           "client barrier rep in");
      }

  public:

      // In progress
      volatile bool in_progress;

      void teardown_conn() {
        zmq_close(socket_barrier_out);
        zmq_close(socket_barrier_in);
        zmq_close(socket_local_store);
      }

      client_barrier() {
        slipchunk = vm["slipchunk"].as < unsigned
        long > ();
        machines = pt_slipstore.get < unsigned
        long > ("machines.count");
        me = pt_slipstore.get < unsigned
        long > ("machines.me");
        setup_conn();
        in_progress = false;
        __sync_synchronize();
      }

      unsigned long get_machines() {
        return machines;
      }

      unsigned long get_me() {
        return me;
      }

      unsigned long get_slipchunk() {
        return slipchunk;
      }

      template<typename A>
      friend
      void sync_barrier_internal(client_barrier *sl_client, A *sync_obj);

      template<typename A>
      friend
      void sync_barrier(client_barrier *sl_client, A *sync_obj);

      void work_barrier(barrier_work *work_object) {
        unsigned long token = 0xf141516182235UL;
        // Twice around the ring
        if (get_me() == 0) {
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
          // Everyone is here
          // Flush the local store
          flush_local_store();
          // Do the work
          (*work_object)();
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
          // Extra round to ensure that everyone has done the work
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
        }
        else {
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
          // Everyone is here
          // Flush the local store
          flush_local_store();
          // Do the work
          (*work_object)();
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
          // Extra round to ensure that everyone has done the work
          (void) do_zmq_recv(socket_barrier_in,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier in");
          (void) do_zmq_send(socket_barrier_out,
                             (unsigned char *) &token,
                             sizeof(unsigned long),
                             "barrier out");
        }
        // Everyone has done the work
        __sync_synchronize();
        in_progress = false;
      }
  };

  template<typename A>
  static void sync_barrier_internal(client_barrier *sl_client,
                                    A *sync_object) {
    if (sl_client->me == 0) {
      sync_object->db_generate();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_absorb();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
    }
    else {
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_merge();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
      (void) do_zmq_recv(sl_client->socket_barrier_in,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data in");
      sync_object->db_absorb();
      (void) do_zmq_send(sl_client->socket_barrier_out,
                         sync_object->db_buffer(),
                         sync_object->db_size(),
                         "barrier data out");
    }
    __sync_synchronize();
    sl_client->in_progress = false;
  }

  template<typename A>
  static void sync_barrier(client_barrier *sl_client,
                           A *sync_object) {
    sl_client->in_progress = true;
    ioService.post
        (boost::bind(&sync_barrier_internal<A>,
                     sl_client,
                     sync_object));
    while (sl_client->in_progress);
  }

  extern client_barrier *slipstore_client_barrier;

  extern server *slipstore_server;
  extern boost::thread *server_thread;


  static
  void init(io *disks, unsigned long tile_size, unsigned long partitions) {
    unsigned long zmq_threads = vm["zmq_threads"].as < unsigned
    long > ();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::ZMQ_THREADS " <<
    zmq_threads;
    zmq_context = zmq_init(zmq_threads);
    zmq_context_data_out = zmq_init(zmq_threads);
    zmq_context_data_in = zmq_init(zmq_threads);
    baseport = vm["baseport"].as < unsigned
    long > ();
    slipstore_client_fill = new client_fill();
    slipstore_client_drain = new client_drain();
    slipstore_client_barrier = new client_barrier();
    have_outstanding_request = new busy_counter();

    if (vm.count("use_async_server") > 0) {
      slipstore_server = new server_async(disks, tile_size, partitions);
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SERVER ASYNC";
    } else {
      slipstore_server = new server_sync(disks, tile_size, partitions);
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SERVER SYNC";
    }

    // Launch server thread
    server_thread = new boost::thread(boost::ref(*slipstore_server));
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::MACHINES " <<
    slipstore_client_fill->get_machines();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::ME " <<
    slipstore_client_fill->get_me();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::SLIPCHUNK " <<
    slipstore_client_barrier->get_slipchunk();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::QUOTA " <<
    quota;
    // Launch async client thread
    threadpool.create_thread(boost::bind(&boost::asio::io_service::run,
                                         &ioService));
    //Launch async dio thread if needed
    if (vm.count("use_direct_io") > 0) {
      diopool.create_thread(boost::bind(&boost::asio::io_service::run,
                                        &dioService));
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO DIRECT";
    }
    else if (vm.count("use_dummy_io") > 0) {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO DUMMY";
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::IO PAGECACHE";
    }
  }

  static
  void shutdown() {
    // Shut down async client service
    ioService.stop();
    threadpool.join_all();
    // Shut down dio service
    if (vm.count("use_direct_io") > 0) {
      dioService.stop();
      diopool.join_all();
    }
    // Terminate server
    slipstore_req_t req;
    req.cmd = CMD_TERMINATE;
    slipstore_client_drain->access_store(&req,
                                         NULL,
                                         slipstore_client_drain->get_me());
    server_thread->join();
    // Teardown sockets
    slipstore_client_drain->teardown_conn();
    slipstore_client_fill->teardown_conn();
    slipstore_client_barrier->teardown_conn();
    // Terminate context
    zmq_term(zmq_context);
    zmq_term(zmq_context_data_out);
    zmq_term(zmq_context_data_in);
    slipstore_client_fill->print_times();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::DISK_BYTES_READ " <<
    slipstore_client_fill->get_disk_bytes_read();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::MEM_BYTES_READ " <<
    slipstore_client_fill->get_mem_bytes_read();
    BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CLIENT::BYTES_WRITTEN " <<
    slipstore_client_drain->get_bytes_written();
    have_outstanding_request->counter.print("SLIPSTORE::CLIENT_BUSY ");
  }
}

#endif

