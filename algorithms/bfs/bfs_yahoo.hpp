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

#ifndef _BFS_YAHOO_
#define _BFS_YAHOO_

#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// We use a directed graph as input. If the algorithm should be run on an undirected
// graph, we first generate the undirected version by creating a new edge file with
// both forward and reverse edges. Then we run bfs for a fixed number of iterations.
// Finally, we output edges from the directed graph whose source vertex has been
// discovered by bfs. The undirected graph is also created from the directed one.

// Note: The output edges stream is in files that are a multiple of 4K, so it
// may have garbage at the end. Use an external command to truncate it.
// Number of output edges can be found in the log. I am not sure how to do this
// if the output is partitioned into multiple files.

namespace algorithm {
  namespace bfs {
    const unsigned long phase_make_undirected = 0;
    const unsigned long phase_edge_split = 1;
    const unsigned long phase_bfs_gather = 2;
    const unsigned long phase_bfs_scatter = 3;
    const unsigned long phase_bfs_post_scatter = 4;
    const unsigned long phase_eliminate = 5;
    const unsigned long phase_terminate = 6;

    struct bfs_pcpu : public per_processor_data {
        unsigned long processor_id;
        // Stats
        unsigned long update_bytes_out;
        unsigned long update_bytes_in;
        unsigned long edge_bytes_streamed;
        unsigned long partitions_processed;
        unsigned long output_edges;
        static unsigned long output_edges_total;
        unsigned long vertices_discovered;
        static unsigned long vertices_discovered_total;
        unsigned long edges_explored;
        static unsigned long edges_explored_total;
        // work specs
        static unsigned long bsp_phase;
        static unsigned long current_step;
        // filter
        static x_lib::filter *scatter_filter;
        bool activate_partition_for_scatter;

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          if (current_step == phase_terminate) {
            for (unsigned long i = 0; i < processors; i++) {
              bfs_pcpu *pcpu = static_cast<bfs_pcpu *>(per_cpu_array[i]);
              output_edges_total += pcpu->output_edges;
            }
          }
          vertices_discovered_total = 0;
          edges_explored_total = 0;
          for (unsigned long i = 0; i < processors; i++) {
            bfs_pcpu *pcpu = static_cast<bfs_pcpu *>(per_cpu_array[i]);
            vertices_discovered_total += pcpu->vertices_discovered;
            edges_explored_total += pcpu->edges_explored;
          }
          return false;
        }
    } __attribute__((__aligned__(64)));

    struct bfs_vertex_t {
        vertex_t bfs_parent;
        vertex_t bsp_phase;
    } __attribute__((__packed__));

    struct bfs_update_t {
        vertex_t parent;
        vertex_t child;
        weight_t value;
    } __attribute__((__packed__));

    template<typename F>
    class bfs_yahoo {
        static bfs_pcpu **pcpu_array;
        x_lib::streamIO<bfs_yahoo> *graph_storage;
        bool heartbeat;
        unsigned long niters;
        bool undirected;
        static unsigned long bfs_root;

        unsigned long vertex_stream;
        unsigned long edges_d_stream;
        unsigned long edges_u_stream;
        unsigned long updates0_stream;
        unsigned long updates1_stream;
        unsigned long init_stream;
        unsigned long out_edges_d_stream;
        unsigned long out_edges_u_stream;

        rtc_clock wall_clock;
        rtc_clock setup_time;

    public:
        bfs_yahoo();

        static void partition_pre_callback(unsigned long super_partition,
                                           unsigned long partition,
                                           per_processor_data *cpu_state);

        static void partition_callback(x_lib::stream_callback_state *state);

        static void partition_post_callback(unsigned long super_partition,
                                            unsigned long partition,
                                            per_processor_data *cpu_state);

        static void do_cpu_callback(per_processor_data *cpu_state);

        static void state_iter_callback(unsigned long superp,
                                        unsigned long partition,
                                        unsigned long index,
                                        unsigned char *vertex,
                                        per_processor_data *cpu_state);

        void operator()();

        static unsigned long max_streams() {
          return 8; // vertices, edges_d, edges_u, init_edges, updates0, updates1, out_edges_d, out_edges_u
        }

        static unsigned long max_buffers() {
          return 5;
        }

        static unsigned long vertex_state_bytes() {
          return sizeof(bfs_vertex_t);
        }

        static unsigned long vertex_stream_buffer_bytes() {
          return sizeof(bfs_vertex_t) + sizeof(bfs_update_t);
        }

        static per_processor_data *create_per_processor_data(unsigned long processor_id) {
          return pcpu_array[processor_id];
        }
    };

    template<typename F>
    bfs_yahoo<F>::bfs_yahoo() {
      wall_clock.start();
      setup_time.start();
      heartbeat = (vm.count("heartbeat") > 0);
      niters = vm["bfs_yahoo::niters"].as < unsigned
      long > ();
      undirected = (vm.count("bfs_yahoo::undirected") > 0);
      bfs_root = vm["bfs::root"].as < unsigned
      long > ();
      unsigned long num_processors = vm["processors"].as < unsigned
      long > ();
      pcpu_array = new bfs_pcpu *[num_processors];
      for (unsigned long i = 0; i < num_processors; i++) {
        pcpu_array[i] = new bfs_pcpu();
        pcpu_array[i]->processor_id = i;
        pcpu_array[i]->update_bytes_in = 0;
        pcpu_array[i]->update_bytes_out = 0;
        pcpu_array[i]->edge_bytes_streamed = 0;
        pcpu_array[i]->partitions_processed = 0;
        pcpu_array[i]->output_edges = 0;
        pcpu_array[i]->vertices_discovered = 0;
        pcpu_array[i]->edges_explored = 0;
      }
      graph_storage = new x_lib::streamIO<bfs_yahoo>();
      bfs_pcpu::scatter_filter = new x_lib::filter(MAX(graph_storage->get_config()->cached_partitions,
                                                       graph_storage->get_config()->super_partitions),
                                                   num_processors);
      bfs_pcpu::bsp_phase = 0;
      vertex_stream = graph_storage->open_stream("vertices", true,
                                                 vm["vertices_disk"].as < unsigned
      long > (),
          graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = graph_storage->open_stream((const char *) efile.c_str(), false,
                                               vm["input_disk"].as < unsigned
      long > (),
          F::split_size_bytes(), 1);
      if (graph_storage->get_config()->super_partitions > 1) {
        edges_d_stream = graph_storage->open_stream("edges_d", true,
                                                    vm["edges_disk"].as < unsigned
        long > (),
            F::split_size_bytes());
      }
      if (undirected) {
        edges_u_stream = graph_storage->open_stream("edges_u", true,
                                                    vm["edges_disk"].as < unsigned
        long > (),
            F::split_size_bytes());
      }
      updates0_stream = graph_storage->open_stream("updates0", true,
                                                   vm["updates0_disk"].as < unsigned
      long > (),
          sizeof(bfs_update_t));
      updates1_stream = graph_storage->open_stream("updates1", true,
                                                   vm["updates1_disk"].as < unsigned
      long > (),
          sizeof(bfs_update_t));
      out_edges_d_stream = graph_storage->open_stream("truncated", true,
                                                      vm["output_disk"].as < unsigned
      long > (),
          F::split_size_bytes());
      out_edges_u_stream = graph_storage->open_stream("truncated-und", "true",
                                                      vm["output_disk"].as < unsigned
      long > (),
          F::split_size_bytes());
      setup_time.stop();
    }

    template<typename F>
    struct edge_type_wrapper {
        static unsigned long item_size() {
          return F::split_size_bytes();
        }

        static unsigned long key(unsigned char *buffer) {
          return F::split_key(buffer, 0);
        }
    };

    struct update_type_wrapper {
        static unsigned long item_size() {
          return sizeof(bfs_update_t);
        }

        static unsigned long key(unsigned char *buffer) {
          return ((bfs_update_t *) buffer)->child;
        }
    };

    template<typename F>
    void bfs_yahoo<F>::operator()() {
      if (undirected) {
        // Emit a new edge file with both forward and reverse edges
        bfs_pcpu::current_step = phase_make_undirected;
        x_lib::do_stream<bfs_yahoo<F>,
            edge_type_wrapper<F>,
            edge_type_wrapper<F> >
            (graph_storage, 0, init_stream, edges_u_stream, NULL);
        graph_storage->rewind_stream(init_stream);
        graph_storage->rewind_stream(edges_u_stream);
      }

      if (graph_storage->get_config()->super_partitions > 1) {
        // Split edges
        bfs_pcpu::current_step = phase_edge_split;
        x_lib::do_stream<bfs_yahoo<F>,
            edge_type_wrapper<F>,
            edge_type_wrapper<F> >
            (graph_storage, 0, init_stream, edges_d_stream, NULL);
        graph_storage->close_stream(init_stream);
        graph_storage->rewind_stream(edges_d_stream);
        init_stream = edges_d_stream;
      }

      unsigned long edge_stream;
      if (undirected) {
        edge_stream = edges_u_stream;
      } else {
        edge_stream = init_stream;
      }

      // bfs_forest
      unsigned long PHASE = 0;
      unsigned long iters = 0;
      unsigned long updates_in_stream;
      unsigned long updates_out_stream;
      while (iters++ < niters) {
        updates_in_stream = (PHASE == 0 ? updates1_stream : updates0_stream);
        updates_out_stream = (PHASE == 0 ? updates0_stream : updates1_stream);

        for (unsigned long i = 0; i < graph_storage->get_config()->super_partitions; i++) {
          if (graph_storage->get_config()->super_partitions > 1) {
            if (bfs_pcpu::bsp_phase > 0) {
              graph_storage->state_load(vertex_stream, i);
            }
            graph_storage->state_prepare(i);
          }
          else if (bfs_pcpu::bsp_phase == 0) {
            graph_storage->state_prepare(0);
          }

          // Init
          if (bfs_pcpu::bsp_phase == 0) {
            x_lib::do_state_iter<bfs_yahoo<F> >(graph_storage, i);
          }

          bfs_pcpu::current_step = phase_bfs_gather;
          x_lib::do_stream<bfs_yahoo<F>,
              update_type_wrapper,
              update_type_wrapper>
              (graph_storage, i, updates_in_stream, ULONG_MAX, NULL);
          graph_storage->reset_stream(updates_in_stream, i);

          bfs_pcpu::current_step = phase_bfs_scatter;
          x_lib::do_stream<bfs_yahoo<F>,
              edge_type_wrapper<F>,
              update_type_wrapper>
              (graph_storage, i, edge_stream, updates_out_stream, bfs_pcpu::scatter_filter);

          bfs_pcpu::current_step = phase_bfs_post_scatter;
          x_lib::do_cpu<bfs_yahoo<F> >(graph_storage, i);

          if (graph_storage->get_config()->super_partitions > 1) {
            graph_storage->state_store(vertex_stream, i);
          }
        }

        graph_storage->rewind_stream(edge_stream);
        graph_storage->rewind_stream(updates_out_stream);
        if (graph_storage->get_config()->super_partitions > 1) {
          graph_storage->rewind_stream(vertex_stream);
        }

        PHASE = 1 - PHASE;
        bfs_pcpu::bsp_phase++;

        if (heartbeat) {
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::VERTICES_DISCOVERED " <<
          bfs_pcpu::vertices_discovered_total;
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::EDGES_EXPLORED " << bfs_pcpu::edges_explored_total;
          BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " << bfs_pcpu::bsp_phase;
        }
      }

      // Edge elimination
      bfs_pcpu::current_step = phase_eliminate;
      for (unsigned long i = 0; i < graph_storage->get_config()->super_partitions; i++) {
        if (graph_storage->get_config()->super_partitions > 1) {
          graph_storage->state_load(vertex_stream, i);
          graph_storage->state_prepare(i);
        }

        x_lib::do_stream<bfs_yahoo<F>,
            edge_type_wrapper<F>,
            edge_type_wrapper<F> >
            (graph_storage, i, init_stream, out_edges_d_stream, NULL);
        graph_storage->reset_stream(updates_out_stream, i);

        if (graph_storage->get_config()->super_partitions > 1) {
          graph_storage->state_store(vertex_stream, i);
        }
      }
      graph_storage->rewind_stream(out_edges_d_stream);

      // Emit the undirected graph as well
      bfs_pcpu::current_step = phase_make_undirected;
      for (unsigned long i = 0; i < graph_storage->get_config()->super_partitions; i++) {
        x_lib::do_stream<bfs_yahoo<F>,
            edge_type_wrapper<F>,
            edge_type_wrapper<F> >
            (graph_storage, i, out_edges_d_stream, out_edges_u_stream, NULL);
      }
      graph_storage->rewind_stream(out_edges_u_stream);

      if (graph_storage->get_config()->super_partitions == 1) {
        graph_storage->state_store(vertex_stream, 0);
      }

      bfs_pcpu::current_step = phase_terminate;
      x_lib::do_cpu<bfs_yahoo<F> >(graph_storage, ULONG_MAX);
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();

      // Note: should truncate output edges file here to size (bfs_pcpu::output_edges_total * F::split_size_bytes()),
      // but if the stream is partitioned into multiple files not sure how to do it.

      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::BFS_ROOT " << bfs_root;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::NITERS " << niters;
      BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << bfs_pcpu::bsp_phase;
      BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::OUTPUT_EDGES " << bfs_pcpu::output_edges_total;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void bfs_yahoo<F>::partition_pre_callback(unsigned long superp,
                                              unsigned long partition,
                                              per_processor_data *pcpu) {
      bfs_pcpu *pcpu_actual = static_cast<bfs_pcpu *>(pcpu);
      if (pcpu_actual->current_step == phase_bfs_gather) {
        pcpu_actual->activate_partition_for_scatter = false;
      }
    }

    template<typename F>
    void bfs_yahoo<F>::partition_callback(x_lib::stream_callback_state *callback) {
      bfs_pcpu *pcpu = static_cast<bfs_pcpu *>(callback->cpu_state);
      switch (bfs_pcpu::current_step) {
        case phase_make_undirected: {
          while (callback->bytes_in) {
            if (callback->bytes_out + 2 * F::split_size_bytes() > callback->bytes_out_max) {
              break;
            }
            vertex_t src, dst;
            weight_t value;
            F::read_edge(callback->bufin, src, dst, value);
            callback->bufin += F::split_size_bytes();
            callback->bytes_in -= F::split_size_bytes();
            F::write_edge(callback->bufout, src, dst, value);
            callback->bufout += F::split_size_bytes();
            callback->bytes_out += F::split_size_bytes();
            F::write_edge(callback->bufout, dst, src, value);
            callback->bufout += F::split_size_bytes();
            callback->bytes_out += F::split_size_bytes();
          }
          break;
        }
        case phase_bfs_gather: {
          pcpu->update_bytes_in += callback->bytes_in;
          while (callback->bytes_in) {
            bfs_update_t *u = (bfs_update_t *) (callback->bufin);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(u->child);
            if (v->bfs_parent == (vertex_t) - 1) {
              v->bfs_parent = u->parent;
              v->bsp_phase = bfs_pcpu::bsp_phase;
              pcpu->vertices_discovered++;
              pcpu->activate_partition_for_scatter = true;
            }
            callback->bufin += sizeof(bfs_update_t);
            callback->bytes_in -= sizeof(bfs_update_t);
          }
          break;
        }
        case phase_bfs_scatter: {
          unsigned long tmp = callback->bytes_in;
          while (callback->bytes_in) {
            if (callback->bytes_out + sizeof(bfs_update_t) > callback->bytes_out_max) {
              break;
            }
            vertex_t src, dst;
            F::read_edge(callback->bufin, src, dst);
            bfs_update_t *u = (bfs_update_t *) (callback->bufout);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(src);
            if (v->bfs_parent != (vertex_t) - 1 && v->bsp_phase == bfs_pcpu::bsp_phase) {
              u->parent = src;
              u->child = dst;
              pcpu->edges_explored++;
              callback->bytes_out += sizeof(bfs_update_t);
              callback->bufout += sizeof(bfs_update_t);
            }
            callback->bufin += F::split_size_bytes();
            callback->bytes_in -= F::split_size_bytes();
          }
          pcpu->update_bytes_out += callback->bytes_out;
          pcpu->edge_bytes_streamed += (tmp - callback->bytes_in);
          break;
        }
        case phase_eliminate: {
          while (callback->bytes_in) {
            if (callback->bytes_out + F::split_size_bytes() > callback->bytes_out_max) {
              break;
            }
            vertex_t src, dst;
            weight_t val;
            F::read_edge(callback->bufin, src, dst, val);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(src);
            if (v->bfs_parent != (vertex_t) - 1) {
              F::write_edge(callback->bufout, src, dst, val);
              callback->bufout += F::split_size_bytes();
              callback->bytes_out += F::split_size_bytes();
              pcpu->output_edges++;
            }
            callback->bufin += F::split_size_bytes();
            callback->bytes_in -= F::split_size_bytes();
          }
          break;
        }
        default:
          BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
          exit(-1);
      }
    }

    template<typename F>
    void bfs_yahoo<F>::partition_post_callback(unsigned long superp,
                                               unsigned long partition,
                                               per_processor_data *pcpu) {
      bfs_pcpu *pcpu_actual = static_cast<bfs_pcpu *>(pcpu);
      if (pcpu_actual->current_step == phase_bfs_gather) {
        if (pcpu_actual->activate_partition_for_scatter) {
          bfs_pcpu::scatter_filter->q(partition);
        }
        pcpu_actual->partitions_processed++;
      }
    }

    template<typename F>
    void bfs_yahoo<F>::do_cpu_callback(per_processor_data *cpu_state) {
      bfs_pcpu *cpu = static_cast<bfs_pcpu *>(cpu_state);
      if (bfs_pcpu::current_step == phase_bfs_post_scatter) {
        bfs_pcpu::scatter_filter->done(cpu->processor_id);
      }
      else if (bfs_pcpu::current_step == phase_terminate) {
        BOOST_LOG_TRIVIAL(info) << "CORE::PARTITIONS_PROCESSED " << cpu->partitions_processed;
        BOOST_LOG_TRIVIAL(info) << "CORE::BYTES::EDGES_STREAMED " << cpu->edge_bytes_streamed;
        BOOST_LOG_TRIVIAL(info) << "CORE::BYTES::UPDATES_OUT " << cpu->update_bytes_out;
        BOOST_LOG_TRIVIAL(info) << "CORE::BYTES::UPDATES_IN " << cpu->update_bytes_in;
      }
    }

    template<typename F>
    void bfs_yahoo<F>::state_iter_callback(unsigned long superp,
                                           unsigned long partition,
                                           unsigned long index,
                                           unsigned char *vertex,
                                           per_processor_data *cpu_state) {
      unsigned long global_index = x_lib::configuration::map_inverse(superp, partition, index);
      bfs_vertex_t *v = (bfs_vertex_t *) vertex;
      v->bsp_phase = 0;
      if (global_index == bfs_root) {
        v->bfs_parent = bfs_root;
        bfs_pcpu::scatter_filter->q(partition);
      } else {
        v->bfs_parent = (vertex_t) - 1;
      }
    }

    template<typename F>
    bfs_pcpu **bfs_yahoo<F>::pcpu_array = NULL;
    template<typename F>
    unsigned long bfs_yahoo<F>::bfs_root;
    unsigned long bfs_pcpu::bsp_phase = 0;
    unsigned long bfs_pcpu::current_step;
    unsigned long bfs_pcpu::output_edges_total = 0;
    unsigned long bfs_pcpu::vertices_discovered_total;
    unsigned long bfs_pcpu::edges_explored_total;
    x_lib::filter *bfs_pcpu::scatter_filter;
  }
}
#endif
