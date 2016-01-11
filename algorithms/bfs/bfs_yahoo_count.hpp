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

#ifndef _BFS_YAHOO_COUNT_
#define _BFS_YAHOO_COUNT_

#include<sys/time.h>
#include<sys/resource.h>
#include "../../core/x-lib.hpp"

// We use a directed graph as input. First, we generate the undirected version
// by creating a new edge file with both forward and reverse edges. Then we run
// bfs_forest on the undirected graph for a fixed number of iterations. Finally,
// we output edges from the directed graph whose source and destination belong
// to the same connected component, as computed by bfs_forest. The undirected
// truncated graph can then be created by a script from the directed one.

// Note: The output edges stream is in files that are a multiple of 4K, so it
// may have garbage at the end. Use an external command to truncate it.
// Number of output edges can be found in the log. I am not sure how to do this
// if the output is partitioned into multiple files.

namespace algorithm {
  namespace bfs_cnt {
    const unsigned long phase_emit_reverse = 0;
    const unsigned long phase_gather = 1;
    const unsigned long phase_scatter = 2;
    const unsigned long phase_post_scatter = 3;
    const unsigned long phase_count = 4;
    const unsigned long phase_terminate = 5;

    struct bfs_cnt_pcpu : public per_processor_data {
        unsigned long processor_id;
        // Stats
        unsigned long covered_edges;
        static unsigned long covered_edges_total;
        static bool sum_edge_counts;
        // work specs
        static unsigned long bsp_phase;
        static unsigned long current_step;
        static bool scatter_all;
        // filter
        static x_lib::filter *scatter_filter;
        bool activate_partition_for_scatter;

        bool reduce(per_processor_data **per_cpu_array,
                    unsigned long processors) {
          if (sum_edge_counts) {
            covered_edges_total = 0;
            for (unsigned long i = 0; i < processors; i++) {
              bfs_cnt_pcpu *pcpu = static_cast<bfs_cnt_pcpu *>(per_cpu_array[i]);
              covered_edges_total += pcpu->covered_edges;
              pcpu->covered_edges = 0;
            }
          }
          return false;
        }
    } __attribute__((__aligned__(64)));

    struct bfs_vertex_t {
        vertex_t component;
        vertex_t bfs_parent;
        vertex_t bsp_phase;
    } __attribute__((__packed__));

    struct bfs_update_t {
        vertex_t component;
        vertex_t parent;
        vertex_t child;
    } __attribute__((__packed__));

    template<typename F>
    class bfs_yahoo_count {
        static bfs_cnt_pcpu **pcpu_array;
        x_lib::streamIO<bfs_yahoo_count> *graph_storage;
        bool heartbeat;
        unsigned long niters;

        unsigned long vertex_stream;
        unsigned long edge_stream;
        unsigned long updates0_stream;
        unsigned long updates1_stream;
        unsigned long updates2_stream;
        unsigned long init_stream;

        rtc_clock wall_clock;
        rtc_clock setup_time;

    public:
        bfs_yahoo_count();

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
          return 7; // vertices, edges, init_edges, updates0, updates1, updates2
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
    bfs_yahoo_count<F>::bfs_yahoo_count() {
      wall_clock.start();
      setup_time.start();
      heartbeat = true;
      niters = vm["bfs_yahoo::niters"].as < unsigned
      long > ();
      unsigned long num_processors = vm["processors"].as < unsigned
      long > ();
      pcpu_array = new bfs_cnt_pcpu *[num_processors];
      for (unsigned long i = 0; i < num_processors; i++) {
        pcpu_array[i] = new bfs_cnt_pcpu();
        pcpu_array[i]->processor_id = i;
        pcpu_array[i]->covered_edges = 0;
      }
      graph_storage = new x_lib::streamIO<bfs_yahoo_count>();
      bfs_cnt_pcpu::scatter_filter = new x_lib::filter(MAX(graph_storage->get_config()->cached_partitions,
                                                           graph_storage->get_config()->super_partitions),
                                                       num_processors);
      bfs_cnt_pcpu::bsp_phase = 0;
      vertex_stream = graph_storage->open_stream("vertices", true,
                                                 vm["vertices_disk"].as < unsigned
      long > (),
          graph_storage->get_config()->vertex_size);
      std::string efile = pt.get<std::string>("graph.name");
      init_stream = graph_storage->open_stream((const char *) efile.c_str(), false,
                                               vm["input_disk"].as < unsigned
      long > (),
          F::split_size_bytes(), 1);
      edge_stream = graph_storage->open_stream("edges", true,
                                               vm["edges_disk"].as < unsigned
      long > (),
          F::split_size_bytes());
      updates0_stream = graph_storage->open_stream("updates0", true,
                                                   vm["updates0_disk"].as < unsigned
      long > (),
          sizeof(bfs_update_t));
      updates1_stream = graph_storage->open_stream("updates1", true,
                                                   vm["updates1_disk"].as < unsigned
      long > (),
          sizeof(bfs_update_t));
      updates2_stream = graph_storage->open_stream("updates2", true,
                                                   vm["updates0_disk"].as < unsigned
      long > (),
          sizeof(bfs_update_t));
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
    void bfs_yahoo_count<F>::operator()() {
      // Emit a new edge file with both forward and reverse edges
      bfs_cnt_pcpu::current_step = phase_emit_reverse;
      x_lib::do_stream<bfs_yahoo_count<F>,
          edge_type_wrapper<F>,
          edge_type_wrapper<F> >
          (graph_storage, 0, init_stream, edge_stream, NULL);
      graph_storage->rewind_stream(init_stream);
      graph_storage->rewind_stream(edge_stream);

      unsigned long PHASE = 0;
      unsigned long iters = 0;
      while (iters++ < niters) {
        unsigned long bfs_updates_in_stream, bfs_updates_out_stream, count_updates_in_stream, count_updates_out_stream;
        switch (PHASE) {
          case 0:
            bfs_updates_in_stream = updates1_stream;
            bfs_updates_out_stream = updates0_stream;
            count_updates_in_stream = updates2_stream;
            count_updates_out_stream = updates1_stream;
            break;
          case 1:
            bfs_updates_in_stream = updates0_stream;
            bfs_updates_out_stream = updates2_stream;
            count_updates_in_stream = updates1_stream;
            count_updates_out_stream = updates0_stream;
            break;
          case 2:
            bfs_updates_in_stream = updates2_stream;
            bfs_updates_out_stream = updates1_stream;
            count_updates_in_stream = updates0_stream;
            count_updates_out_stream = updates2_stream;
            break;
        }

        // BFS
        for (unsigned long i = 0; i < graph_storage->get_config()->super_partitions; i++) {
          if (graph_storage->get_config()->super_partitions > 1) {
            if (bfs_cnt_pcpu::bsp_phase > 0) {
              graph_storage->state_load(vertex_stream, i);
            }
            graph_storage->state_prepare(i);
          }
          else if (bfs_cnt_pcpu::bsp_phase == 0) {
            graph_storage->state_prepare(0);
          }

          // Init
          if (bfs_cnt_pcpu::bsp_phase == 0) {
            x_lib::do_state_iter<bfs_yahoo_count<F> >(graph_storage, i);
          }

          bfs_cnt_pcpu::current_step = phase_gather;
          x_lib::do_stream<bfs_yahoo_count<F>,
              update_type_wrapper,
              update_type_wrapper>
              (graph_storage, i, bfs_updates_in_stream, ULONG_MAX, NULL);
          graph_storage->reset_stream(bfs_updates_in_stream, i);

          bfs_cnt_pcpu::current_step = phase_scatter;
          x_lib::do_stream<bfs_yahoo_count<F>,
              edge_type_wrapper<F>,
              update_type_wrapper>
              (graph_storage, i, edge_stream, bfs_updates_out_stream, bfs_cnt_pcpu::scatter_filter);

          bfs_cnt_pcpu::current_step = phase_post_scatter;
          x_lib::do_cpu<bfs_yahoo_count<F> >(graph_storage, i);

          if (graph_storage->get_config()->super_partitions > 1) {
            graph_storage->state_store(vertex_stream, i);
          }
        }

        graph_storage->rewind_stream(edge_stream);
        graph_storage->rewind_stream(bfs_updates_out_stream);
        if (graph_storage->get_config()->super_partitions > 1) {
          graph_storage->rewind_stream(vertex_stream);
        }

        // edge count
        for (unsigned long i = 0; i < graph_storage->get_config()->super_partitions; i++) {
          if (graph_storage->get_config()->super_partitions > 1) {
            graph_storage->state_load(vertex_stream, i);
            graph_storage->state_prepare(i);
          }

          bfs_cnt_pcpu::current_step = phase_count;
          x_lib::do_stream<bfs_yahoo_count<F>,
              update_type_wrapper,
              update_type_wrapper>
              (graph_storage, i, count_updates_in_stream, ULONG_MAX, NULL);
          if (i == graph_storage->get_config()->super_partitions - 1) {
            bfs_cnt_pcpu::sum_edge_counts = true;
            x_lib::do_cpu<bfs_yahoo_count<F> >(graph_storage, i);
            bfs_cnt_pcpu::sum_edge_counts = false;
          }
          graph_storage->reset_stream(count_updates_in_stream, i);

          bfs_cnt_pcpu::current_step = phase_scatter;
          bfs_cnt_pcpu::scatter_all = true;
          x_lib::do_stream<bfs_yahoo_count<F>,
              edge_type_wrapper<F>,
              update_type_wrapper>
              (graph_storage, i, init_stream, count_updates_out_stream, NULL);
          bfs_cnt_pcpu::scatter_all = false;

          if (graph_storage->get_config()->super_partitions > 1) {
            graph_storage->state_store(vertex_stream, i);
          }
        }

        graph_storage->rewind_stream(init_stream);
        graph_storage->rewind_stream(count_updates_out_stream);
        if (graph_storage->get_config()->super_partitions > 1) {
          graph_storage->rewind_stream(vertex_stream);
        }

        PHASE = (PHASE + 1) % 3;
        bfs_cnt_pcpu::bsp_phase++;

        if (heartbeat) {
          BOOST_LOG_TRIVIAL(info) << "ALGORITHM::BFS_YAHOO::COVERED_EDGES_COUNT " << bfs_cnt_pcpu::covered_edges_total;
          BOOST_LOG_TRIVIAL(info) << clock::timestamp() << " Completed phase " << bfs_cnt_pcpu::bsp_phase;
        }
      }

      if (graph_storage->get_config()->super_partitions == 1) {
        graph_storage->state_store(vertex_stream, 0);
      }

      bfs_cnt_pcpu::current_step = phase_terminate;
      x_lib::do_cpu<bfs_yahoo_count<F> >(graph_storage, ULONG_MAX);
      setup_time.start();
      graph_storage->terminate();
      setup_time.stop();
      wall_clock.stop();
      BOOST_LOG_TRIVIAL(info) << "CORE::PHASES " << bfs_cnt_pcpu::bsp_phase;
      setup_time.print("CORE::TIME::SETUP");
      wall_clock.print("CORE::TIME::WALL");
    }

    template<typename F>
    void bfs_yahoo_count<F>::partition_pre_callback(unsigned long superp,
                                                    unsigned long partition,
                                                    per_processor_data *pcpu) {
      bfs_cnt_pcpu *pcpu_actual = static_cast<bfs_cnt_pcpu *>(pcpu);
      if (pcpu_actual->current_step == phase_gather) {
        pcpu_actual->activate_partition_for_scatter = false;
      }
    }

    template<typename F>
    void bfs_yahoo_count<F>::partition_callback(x_lib::stream_callback_state *callback) {
      bfs_cnt_pcpu *pcpu = static_cast<bfs_cnt_pcpu *>(callback->cpu_state);
      switch (bfs_cnt_pcpu::current_step) {
        case phase_emit_reverse: {
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
        case phase_gather: {
          while (callback->bytes_in) {
            bfs_update_t *u = (bfs_update_t *) (callback->bufin);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(u->child);
            if (v->component > u->component) {
              v->bfs_parent = u->parent;
              v->component = u->component;
              v->bsp_phase = bfs_cnt_pcpu::bsp_phase;
              pcpu->activate_partition_for_scatter = true;
            }
            callback->bufin += sizeof(bfs_update_t);
            callback->bytes_in -= sizeof(bfs_update_t);
          }
          break;
        }
        case phase_scatter: {
          while (callback->bytes_in) {
            if (callback->bytes_out + sizeof(bfs_update_t) > callback->bytes_out_max) {
              break;
            }
            vertex_t src, dst;
            F::read_edge(callback->bufin, src, dst);
            bfs_update_t *u = (bfs_update_t *) (callback->bufout);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(src);
            if (bfs_cnt_pcpu::scatter_all || (v->bsp_phase == bfs_cnt_pcpu::bsp_phase)) {
              u->component = v->component;
              u->parent = src;
              u->child = dst;
              callback->bufout += sizeof(bfs_update_t);
              callback->bytes_out += sizeof(bfs_update_t);
            }
            callback->bufin += F::split_size_bytes();
            callback->bytes_in -= F::split_size_bytes();
          }
          break;
        }
        case phase_count: {
          while (callback->bytes_in) {
            bfs_update_t *u = (bfs_update_t *) (callback->bufin);
            bfs_vertex_t *v = (bfs_vertex_t *) (callback->state) + x_lib::configuration::map_offset(u->child);
            if (u->component == v->component) {
              pcpu->covered_edges++;
            }
            callback->bufin += sizeof(bfs_update_t);
            callback->bytes_in -= sizeof(bfs_update_t);
          }
          break;
        }
        default:
          BOOST_LOG_TRIVIAL(fatal) << "Unknown operation in stream callback !";
          exit(-1);
      }
    }

    template<typename F>
    void bfs_yahoo_count<F>::partition_post_callback(unsigned long superp,
                                                     unsigned long partition,
                                                     per_processor_data *pcpu) {
      bfs_cnt_pcpu *pcpu_actual = static_cast<bfs_cnt_pcpu *>(pcpu);
      if (pcpu_actual->current_step == phase_gather) {
        if (pcpu_actual->activate_partition_for_scatter) {
          bfs_cnt_pcpu::scatter_filter->q(partition);
        }
      }
    }

    template<typename F>
    void bfs_yahoo_count<F>::do_cpu_callback(per_processor_data *cpu_state) {
      bfs_cnt_pcpu *cpu = static_cast<bfs_cnt_pcpu *>(cpu_state);
      if (bfs_cnt_pcpu::current_step == phase_post_scatter) {
        bfs_cnt_pcpu::scatter_filter->done(cpu->processor_id);
      }
      else if (bfs_cnt_pcpu::current_step == phase_terminate) {
        // Log things here
      }
    }

    template<typename F>
    void bfs_yahoo_count<F>::state_iter_callback(unsigned long superp,
                                                 unsigned long partition,
                                                 unsigned long index,
                                                 unsigned char *vertex,
                                                 per_processor_data *cpu_state) {
      unsigned long global_index = x_lib::configuration::map_inverse(superp, partition, index);
      bfs_vertex_t *v = (bfs_vertex_t *) vertex;
      v->bfs_parent = global_index;
      v->component = v->bfs_parent;
      v->bsp_phase = 0;
      bfs_cnt_pcpu::scatter_filter->q(partition);
    }

    template<typename F>
    bfs_cnt_pcpu **bfs_yahoo_count<F>::pcpu_array = NULL;
    unsigned long bfs_cnt_pcpu::bsp_phase = 0;
    unsigned long bfs_cnt_pcpu::current_step;
    bool bfs_cnt_pcpu::scatter_all = false;
    unsigned long bfs_cnt_pcpu::covered_edges_total = 0;
    bool bfs_cnt_pcpu::sum_edge_counts = false;
    x_lib::filter *bfs_cnt_pcpu::scatter_filter;
  }
}
#endif
