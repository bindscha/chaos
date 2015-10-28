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

#ifndef _X_LIB_
#define _X_LIB_

#include "../slipstore/memory_buffer.hpp"
#include "autotuner.hpp"
#include "../utils/clock_utils.h"
#include "../utils/memory_utils.h"
#include "../utils/barrier.h"
#include<boost/thread.hpp>
#include "../utils/barrier.h"
#include "../utils/per_cpu_data.hpp"
#include "x-ingest.hpp"
#include<sys/resource.h>

#define OUTBUF_SIZE 8192
namespace x_lib {
  struct stream_callback_state {
    bool ingest;
    bool local_tile;
    unsigned char *state;
    unsigned long superp;
    unsigned long partition_id;
    unsigned char *bufin;    // callee can change
    unsigned long bytes_in;  // callee must set
    unsigned char *bufout;   // callee can change
    unsigned long bytes_out; // callee must set
    unsigned long bytes_out_max;
#ifdef PYTHON_SUPPORT
    PyObject *pbufin;
    unsigned long pbufin_offset;
    PyObject *pbufout;
    PyObject *pstate;
    unsigned long pstate_offset;
#endif
    algorithm::per_processor_data *cpu_state;
  };

  struct work_base {
    virtual void operator()
        (unsigned long processor_id,
         configuration *config,
         x_barrier *sync,
         algorithm::per_processor_data *cpu_state,
         unsigned char *outbuf) = 0;
  };

  template<typename A>
  struct state_iter_work : public work_base {
    memory_buffer *state_buffer;
    unsigned long superp;

    void operator()(unsigned long processor_id,
                    configuration *config,
                    x_barrier *sync,
                    algorithm::per_processor_data *cpu_state,
                    unsigned char *outbuf) {
      unsigned long tile_size =
          config->cached_partitions / config->tiles;
      unsigned long partitions_per_cpu = tile_size / config->processors;
      unsigned long remainder = tile_size -
                                partitions_per_cpu * config->processors;
      unsigned long start = partitions_per_cpu * processor_id;
      unsigned long count = partitions_per_cpu;
      if (processor_id == (config->processors - 1)) {
        count += remainder;
      }
      unsigned long i, j, k;
      for (i = start, j = 0; j < count; i++, j++) {
        unsigned long vertices = config->state_count(superp, i);
        for (k = 0; k < vertices; k++) {
          unsigned char *vertex =
              &state_buffer->buffer[state_buffer->index[0][i] + k * config->vertex_size];
          A::state_iter_callback(superp, i, k, vertex, cpu_state);
        }
      }
    }
  } __attribute__((__aligned__(64)));


  template<typename A>
  struct vertex_merge_work : public work_base {
    unsigned char *main_tile;
    unsigned char *copy_tile;
    unsigned long copy_machine;
    unsigned long tile_vertices;

    void operator()(unsigned long processor_id,
                    configuration *config,
                    x_barrier *sync,
                    algorithm::per_processor_data *cpu_state,
                    unsigned char *outbuf) {
      unsigned long vertices_per_cpu =
          tile_vertices / config->processors;
      unsigned long remainder = tile_vertices -
                                vertices_per_cpu * config->processors;
      unsigned long start = vertices_per_cpu * processor_id;
      unsigned long count = vertices_per_cpu;
      if (processor_id == (config->processors - 1)) {
        count += remainder;
      }
      unsigned char *vertex = main_tile + start * config->vertex_size;
      unsigned char *copy = copy_tile + start * config->vertex_size;
      for (unsigned long i = 0; i < count; i++) {
        A::vertex_apply(vertex, copy, copy_machine, cpu_state);
        vertex += config->vertex_size;
        copy += config->vertex_size;
      }
      sync->wait();
    }
  } __attribute__((__aligned__(64)));


  template<typename A, typename IN, typename OUT>
  struct work : public work_base {
    unsigned long superp;
    memory_buffer *state;
    memory_buffer *stream_in;
    memory_buffer *ingest;
    memory_buffer *stream_out;
    filter *input_filter;
    unsigned long disk_stream_out;
    rtc_clock *io_clock;
    bool local_tile;

    bool flush_buffer(unsigned long processor_id,
                      configuration *config,
                      x_barrier *sync,
                      algorithm::per_processor_data *cpu_state) {
      bool empty = false;
      sync->wait();
      if (stream_out->bufsize > 0) {
        make_index<OUT, map_spshift_wrap>(stream_out, processor_id,
                                          config->super_partitions * config->tiles,
                                          sync);
        if (processor_id == 0) {
          slipstore::ioService.post(boost::bind(&memory_buffer::drain,
                                                stream_out,
                                                slipstore::slipstore_client_drain,
                                                disk_stream_out,
                                                OUT::item_size(),
                                                config->processors));
          stream_out = memory_buffer::get_free_buffer(*io_clock);
        }
      }
      else {
        empty = true;
      }
      sync->wait();
      return empty;
    }

    void final_flush(unsigned long processor_id,
                     configuration *config,
                     x_barrier *sync,
                     algorithm::per_processor_data *cpu_state) {
      bool empty;
      do {
        empty = flush_buffer(processor_id, config, sync, cpu_state);
      } while (!empty);
      sync->wait();
      if (processor_id == 0) {
        stream_out->cleanup();
        memory_buffer::release_buffer(stream_out);
      }
    }

    void append_buffer(unsigned char *buffer, unsigned long bytes,
                       unsigned long processor_id, configuration *config,
                       x_barrier *sync, algorithm::per_processor_data *cpu_state) {
      while (bytes) {
        unsigned char *base = stream_out->buffer;
        unsigned long offset_start = stream_out->bufsize;
        unsigned long space =
            (MIN(stream_out->bufbytes - offset_start, bytes)
             / OUT::item_size()) * OUT::item_size();
        unsigned long offset_stop = offset_start + space;
        if (__sync_bool_compare_and_swap(&stream_out->bufsize,
                                         offset_start,
                                         offset_stop)) {
          memcpy(base + offset_start, buffer, space);
          if (space != bytes) { // Need flush
            (void) flush_buffer(processor_id, config, sync, cpu_state);
          }
          bytes -= space;
          buffer += space;
        }
      }
    }

    void execute_callback(stream_callback_state *callback,
                          unsigned long processor_id,
                          configuration *config,
                          x_barrier *sync,
                          algorithm::per_processor_data *cpu_state,
                          unsigned char *outbuf) {
      callback->bufout = outbuf;
      callback->bytes_out = 0;
#ifdef PYTHON_SUPPORT
      callback->pbufin        = stream_in->pBuffer;
      callback->pbufin_offset = callback->bufin - stream_in->buffer;
      npy_intp dim[1]={OUTBUF_SIZE};
      callback->pbufout       = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)outbuf);
      callback->pstate        = state->pBuffer;
      callback->pstate_offset = callback->state - state->buffer;
#endif
      while (callback->bytes_in) {
        A::partition_callback(callback);
        if (stream_out != NULL && callback->bytes_out > 0) {
          append_buffer(outbuf, callback->bytes_out,
                        processor_id, config, sync,
                        cpu_state);
          callback->bufout = outbuf;
          callback->bytes_out = 0;
        }
      }
#ifdef PYTHON_SUPPORT
      Py_DECREF(callback->pbufout);
#endif
    }

    void operator()(unsigned long processor_id,
                    configuration *config,
                    x_barrier *sync,
                    algorithm::per_processor_data *cpu_state,
                    unsigned char *outbuf) {
      if (stream_in == NULL && stream_out == NULL) {
        A::do_cpu_callback(cpu_state);
        return;
      }
      stream_callback_state callback_state;
      callback_state.superp = superp;
      callback_state.local_tile = local_tile;
      callback_state.bytes_out_max =
          (OUTBUF_SIZE / OUT::item_size()) * OUT::item_size();
      callback_state.cpu_state = cpu_state;
      // Must play ingest first
      if (ingest != NULL) {
        sync->wait();
        callback_state.ingest = true;
        make_index<IN, map_spshift_wrap>(ingest, processor_id,
                                         config->super_partitions,
                                         sync);
        unsigned long ingest_bytes;
        unsigned char *ingest_src =
            ingest->get_substream(processor_id, superp, &ingest_bytes);
        unsigned long offset_start;
        unsigned long offset_stop;
        do {
          offset_start = stream_in->bufsize;
          offset_stop = offset_start + ingest_bytes;
        } while (!__sync_bool_compare_and_swap(&stream_in->bufsize,
                                               offset_start,
                                               offset_stop));
        memcpy(stream_in->buffer + offset_start,
               ingest_src,
               ingest_bytes);
        sync->wait();
        if (processor_id == 0) {
          BOOST_ASSERT_MSG(stream_in->bufsize == ingest->bufsize,
                           "Error in partitioning ingest !");
          ingest = NULL;
        }
      }
      else {
        callback_state.ingest = false;
      }
      if (stream_in != NULL) {
        make_index<IN, map_cached_partition_wrap>
            (stream_in, processor_id, config->cached_partitions, sync);
      }
      sync->wait();
      input_filter->prep_dq(processor_id);
      sync->wait();
      unsigned long partition_id;
      while ((partition_id = input_filter->dq(processor_id)) != ULONG_MAX) {
        callback_state.state = &state->buffer[state->index[0][partition_id]];
        callback_state.partition_id = partition_id;
        A::partition_pre_callback(superp, partition_id, cpu_state);
        if (stream_in != NULL) {
          for (unsigned long i = 0; i < config->processors; i++) {
            callback_state.bufin =
                stream_in->get_substream(i, partition_id,
                                         &callback_state.bytes_in);
            execute_callback(&callback_state,
                             processor_id,
                             config,
                             sync,
                             cpu_state,
                             outbuf);
          }
        }
        else {
          callback_state.bufin = callback_state.state;
          callback_state.bytes_in =
              config->vertex_size * config->state_count(superp, partition_id);
          execute_callback(&callback_state, processor_id, config, sync,
                           cpu_state, outbuf);
        }
        A::partition_post_callback(superp, partition_id, cpu_state);
      }
      if (stream_out != NULL) {
        final_flush(processor_id, config, sync, cpu_state);
      }
    }
  } __attribute__((aligned(64)));

  template<typename OBJ>
  struct sp_copy : public work_base {
    memory_buffer *stream_out;
    unsigned long disk_stream_out;
    rtc_clock *io_clock;
    unsigned long fanout;
    slipstore::io *handle;

    void operator()(unsigned long processor_id,
                    configuration *config,
                    x_barrier *sync,
                    algorithm::per_processor_data *cpu_state,
                    unsigned char *outbuf) {
      sync->wait();
      make_index<OBJ, map_spshift_wrap>(stream_out,
                                        processor_id,
                                        config->super_partitions * config->tiles,
                                        sync);
      if (processor_id == 0) {
        slipstore::ioService.post(boost::bind(&memory_buffer::drain_local,
                                              stream_out,
                                              slipstore::slipstore_client_drain,
                                              handle,
                                              disk_stream_out,
                                              OBJ::item_size(),
                                              config->processors));
      }
      sync->wait();
    }
  };

  class x_thread {
  public:
    static x_barrier *sync;
    struct configuration *config;
    algorithm::per_processor_data *const cpu_state;
    const unsigned long processor_id;
    static volatile bool terminate;
    static struct work_base *volatile work_to_do;
    unsigned char *outbuf;

    x_thread(struct configuration *config_in,
             unsigned long processor_id_in,
             algorithm::per_processor_data *cpu_state_in)
        : config(config_in),
          cpu_state(cpu_state_in),
          processor_id(processor_id_in) {
      if (sync == NULL) { // First object
        sync = new x_barrier(config->processors);
      }
      outbuf = (unsigned char *) map_anon_memory(OUTBUF_SIZE, true, "thread outbuf");
    }

    void operator()() {
      do {
        sync->wait();
        if (terminate) {
          break;
        }
        else {
          (*work_to_do)(processor_id, config, sync, cpu_state, outbuf);
          sync->wait(); // Must synchronize before p0 exits (object is on stack)
        }
      } while (processor_id != 0);
    }

    friend class stream_IO;
  } __attribute__((__aligned__(64)));

  class null_barrier_work : public slipstore::barrier_work {
  public:
    virtual void operator()() {
    }
  };

  class rewind_barrier_work : public slipstore::barrier_work {
    unsigned long stream;
    unsigned long super_partition;
    slipstore::io *streams;
  public:
    rewind_barrier_work(unsigned long stream_in,
                        unsigned long super_partition_in,
                        slipstore::io *streams_in)
        : stream(stream_in),
          super_partition(super_partition_in),
          streams(streams_in) {
    }

    virtual void operator()() {
      for (unsigned long i = 0; i < configuration::tiles; i++) {
        streams->rewind(stream, super_partition, i);
      }
    }
  };

  class check_empty_work {
    unsigned long stream;
    slipstore::io *streams;
  public:
    volatile bool empty;
    volatile bool in_progress;

    check_empty_work(unsigned long stream_in,
                     slipstore::io *streams_in)
        : stream(stream_in),
          streams(streams_in),
          empty(false),
          in_progress(false) {
    }

    void do_it() {
      empty = slipstore::slipstore_client_fill->check_empty(stream);
      __sync_synchronize();
      in_progress = false;
    }
  };

  class tile_reset_work {
    unsigned long stream;
    unsigned long partition;
  public:
    volatile bool in_progress;

    tile_reset_work(unsigned long stream_in, unsigned long partition_in)
        : stream(stream_in),
	  partition(partition_in),
          in_progress(false)
    {
    }

    void do_it() {
      slipstore::slipstore_client_fill->reset(stream, partition, 0);
      __sync_synchronize();
      in_progress = false;
    }
  };

  class semup_work {
    unsigned long mc;
  public:
    volatile bool in_progress;

    semup_work(unsigned long mc_in)
      : mc(mc_in),
	in_progress(false)
    {
    }

    void do_it() {
      slipstore::slipstore_client_fill->send_semup(mc);
      __sync_synchronize();
      in_progress = false;
    }
  };

  class order_work {
  public:
    unsigned long cmd;
    unsigned long stream;
    unsigned long master;
    unsigned long index;
    volatile unsigned long order_next;
    volatile bool in_progress;

    order_work()
    {
    }

    void do_it() {
      if(cmd == slipstore::CMD_ORDER_INIT) {
	slipstore::slipstore_client_fill->init_order(stream);
      }
      else if(cmd == slipstore::CMD_ORDER_NEXT) {
	order_next = slipstore::slipstore_client_fill->order_next(master,
								  index);
      }
      __sync_synchronize();
      in_progress = false;
    }
  };

  class reset_barrier_work : public slipstore::barrier_work {
    unsigned long stream;
    unsigned long super_partition;
    slipstore::io *streams;
  public:
    reset_barrier_work(unsigned long stream_in,
                       unsigned long super_partition_in,
                       slipstore::io *streams_in)
        : stream(stream_in),
          super_partition(super_partition_in),
          streams(streams_in) {
    }

    virtual void operator()() {
      for (unsigned long i = 0; i < configuration::tiles; i++) {
        streams->trunc(stream, super_partition, i);
      }
    }
  };

  class help_offer {
    unsigned long stream;
    unsigned long super_partition;
    unsigned long beneficiary;
    bool tile_sync;
  public:
    volatile bool done;
    volatile bool accepted;

    help_offer(unsigned long stream_in,
               unsigned long super_partition_in,
               unsigned long beneficiary_in,
               bool tile_sync_in)
        : stream(stream_in),
          super_partition(super_partition_in),
          beneficiary(beneficiary_in),
          tile_sync(tile_sync_in) {
      done = false;
      accepted = false;
    }

    void make_offer() {
      slipstore::slipstore_req_t req;
      req.cmd = slipstore::CMD_OFFER_HELP;
      req.stream = stream;
      req.partition = super_partition;
      req.tile = 0;
      if (tile_sync) {
        req.size = 1;
      }
      else {
        req.size = 0;
      }
      if (slipstore::slipstore_client_fill->access_store(&req, NULL, beneficiary)) {
        accepted = true;
      }
      else {
        accepted = false;
      }
      __sync_synchronize();
      done = true;
    }
  };


  class sync_start_checkpoint {
  public:
    unsigned long checkpoint;
    unsigned long io;

    sync_start_checkpoint(unsigned long checkpoint_in)
        : checkpoint(checkpoint_in) {
    }

    unsigned char *db_buffer() {
      return (unsigned char *) &io;
    }

    unsigned long db_size() {
      return sizeof(unsigned long);
    }

    void db_generate() {
      io = checkpoint;
    }

    void db_merge() {
      if (io > checkpoint) {
        io = checkpoint;
      }
    }

    void db_absorb() {
      checkpoint = io;
    }
  };

  template<typename A>
  class streamIO {
    struct configuration *config;
    slipstore::io *streams;
    unsigned long open_streams;
    memory_buffer **ingest_buffers;
    memory_buffer *state_buffer;
    unsigned char *tile_buffer;
    x_thread **workers;
    boost::thread **thread_array;
    algorithm::per_processor_data **cpu_state_array;
    bool state_loaded;
    bool take_checkpoints;
    bool *first_touch;
    bool ext_mem_shuffle;
    bool sorted_order;
    bool centralized_order;
    unsigned long ext_fanout;
    unsigned long ext_tmps[2];

    /* Tile buffers to copy across data for merging */
    /* Two to overlap merge work with remote copies */
    unsigned char *tile_buffers[2];

    /* Clocks */
    rtc_clock io_wait_time;
    rtc_clock tile_copy_time;
    rtc_clock tile_merge_time;
    rtc_clock im_barrier_time;
    rtc_clock processing_stolen_time;
    rtc_clock merge_wait_time;


#ifdef PYTHON_SUPPORT
     //M: has to be called before creating python arrays! It is a macro, it has to be inside an int-returning function
    int import_numpy_array(){
      import_array();
      return 0;
    }
#endif

    void make_config() {
#ifdef PYTHON_SUPPORT
      Py_Initialize();
      import_numpy_array();
#endif
      config->memory_buffer_object_size = sizeof(memory_buffer);
      config->vertex_size = A::vertex_state_bytes();
      config->vertex_footprint = config->vertex_size +
                                 A::vertex_stream_buffer_bytes();
      config->max_streams = A::max_streams();
      if (ext_mem_shuffle) {
        config->max_streams += 2;
      }
      config->max_buffers = A::max_buffers();
      config->init();
      if (vm.count("autotune") > 0) {
        bool success = config->autotune();
        if (!success) {
          BOOST_LOG_TRIVIAL(fatal) << "Auto-tuning failed !";
          config->dump_config();
          exit(-1);
        }
      }
      else {
        config->manual();
      }
      config->dump_config();
      /* Sanity checks */
      //unsigned long machines = pt_slipstore.get < unsigned
      //long > ("machines.count");
      check_pow_2(config->super_partitions, "Super partitions must be power of two");
      check_pow_2(config->cached_partitions, "Cached partitions must be power of two");
      check_pow_2(config->tiles, "Tiles must be power of two");
      //check_pow_2(config->processors, "Processors must be power of two");
      //check_pow_2(machines, "Machines must be power of two");
      check_pow_2(ext_fanout, "ext_fanout must be power of two");
    }

  public:
    volatile bool tile_copy_done;
    unsigned long tile_copy_total_bytes;

    void tile_copy(unsigned long partition,
                   unsigned long tile) {
      tile = 0;

      // Are we striping vertices?
      bool striping = vm.count("use_vertex_striping") > 0;

      // Permutation that defines the order in which to access the stripes
      slipstore::cyclic permutation(slipstore::slipstore_client_fill->get_machines(),
                                    slipstore::slipstore_client_fill->get_me());

      // Tile size
      const unsigned long tile_bytes = tile_size(partition, tile);
      unsigned long fill_bytes = tile_bytes;
      tile_copy_total_bytes += fill_bytes; // Stats counter?

      // Number of machines
      const unsigned long m = slipstore::slipstore_client_fill->get_machines();

      // Master for this partition
      const unsigned long start_mc = partition % m;

      // Offset in local buffer for the tile
      unsigned long fill_offset = tile_offset(partition, tile);

      // Chunk size
      unsigned long slipchunk = slipstore::slipstore_client_fill->get_slipchunk();

      // Request object to fill vertex state for a helpee tile
      slipstore::slipstore_req_t req;
      req.cmd = slipstore::CMD_SEEK_AND_FILL;
      req.stream = slipstore::STREAM_VERTEX_STATE;
      req.partition = partition;
      req.tile = tile;

      // Remember the first element in the permutation so we know when we've done a complete round
      unsigned long start_m = permutation.cyclic_next();

      // Offset in the remote file
      unsigned long seek_offset = 0;

      unsigned long relative_id = 0;

      // Fill loop
      for (unsigned long i = permutation.cyclic_next(); // i = machine id for the next stripe (used only if vertex striping enabled)
           fill_bytes > 0; // until buffer is filled
           i = permutation.cyclic_next() // i goes through the permutation
          ) {

        req.offset = seek_offset;
        req.size = (fill_bytes > slipchunk) ? slipchunk : fill_bytes;

        // The relative id of this chunk so we know what the fill offset should be
        relative_id = (i + m - start_mc) % m;

        // Assign offset for the fill in the local buffer as follows:
        // - if striping: base + rounds * slipchunk * m + relative id * slipchunk
        // - if not striping: base + seek_offset
        fill_offset =
            tile_offset(partition, tile) + (striping ? (seek_offset * m + relative_id * slipchunk) : seek_offset);

        // Access server i's store if striping, else server tile's store
        // This can fail if we're in the last round! We check this as follows:
        // - The round id is seek_offset / slipchunk.
        // - The last round has id tile_size / (m * slipchunk)
        // => If round id != last_round and the server access failed, die
        if (!slipstore::slipstore_client_fill->access_store(&req, state_buffer->get_buffer() + fill_offset,
                                                            striping ? i : tile)
            && seek_offset / slipchunk != tile_bytes / (m * slipchunk)) {
          BOOST_LOG_TRIVIAL(fatal) << "Unable to copy tile";
          exit(-1);
        }

        if ((striping && i == start_m) ||
            !striping) { seek_offset += req.size; } // increment offset in remote file only after each round (if striping) or for each request (if not striping)
        fill_bytes -= req.size; // decrement target number of bytes
      }

      __sync_synchronize();
      tile_copy_done = true;
    }

    void tile_copy_mem(unsigned long superp,
                       unsigned long me,
                       unsigned long mc) {
      unsigned long tile_bytes = this->tile_size(superp, mc);
      unsigned long slipchunk =
          slipstore::slipstore_client_fill->get_slipchunk();
      unsigned long tile_offset = this->tile_offset(superp, mc);
      unsigned char *main_tile =
          state_buffer->get_buffer() + tile_offset;
      unsigned long io_bytes;
      io_bytes = (tile_bytes > slipchunk) ? slipchunk : tile_bytes;
      io_bytes = (io_bytes / config->vertex_size) * config->vertex_size;
      // Issue first bit of I/O
      if (io_bytes > 0) {
        tile_copy_done = false;
        tile_copy_partial(main_tile, mc, tile_offset, io_bytes);
      }
      while (tile_bytes) {
        io_wait_time.start();
        while (!tile_copy_done);
        /// Issue next transfer
        tile_bytes -= io_bytes;
        tile_offset += io_bytes;
        main_tile += io_bytes;
        io_bytes = (tile_bytes > slipchunk) ? slipchunk : tile_bytes;
        io_bytes = (io_bytes / config->vertex_size) * config->vertex_size;
        if (io_bytes > 0) {
          tile_copy_done = false;
          tile_copy_partial(main_tile, mc, tile_offset, io_bytes);
        }
      }

    }


    void tile_copy_partial(unsigned char *buffer,
                           unsigned long mc,
                           unsigned long offset,
                           unsigned long bytes) {
      slipstore::slipstore_req_t req;
      tile_copy_total_bytes += bytes;
      req.cmd = slipstore::CMD_FILL;
      req.stream = slipstore::STREAM_MEMBUFFER;
      req.partition = offset;
      req.size = bytes;
      if (!slipstore::slipstore_client_fill->
          access_store(&req, buffer, mc)) {
        BOOST_LOG_TRIVIAL(fatal) << "Unable to copy tile";
        exit(-1);
      }
      __sync_synchronize();
      tile_copy_done = true;
    }


    void inter_machine_barrier() {
      null_barrier_work null_obj;
      im_barrier_time.start();
      slipstore::slipstore_client_barrier->in_progress = true;
      slipstore::ioService.post
          (boost::bind(&slipstore::client_barrier::work_barrier,
                       slipstore::slipstore_client_barrier,
                       &null_obj));
      while (slipstore::slipstore_client_barrier->in_progress);
      im_barrier_time.stop();
    }

    streamIO() {
      config = new struct configuration();
      ext_mem_shuffle = (vm.count("ext_mem_shuffle") > 0);
      sorted_order      = (vm.count("sorted_order") > 0);
      centralized_order = (vm.count("centralized_order") > 0);
      ext_fanout = vm["ext_fanout"].as < unsigned
      long > ();
      make_config();
      memory_buffer::initialize_freelist(config,
                                         config->buffer_size,
                                         config->max_buffers);
      state_buffer = new memory_buffer(config,
                                       config->max_state_bufsize());

      streams = new slipstore::io(config->max_streams,
                                  config->super_partitions,
                                  config->tiles,
                                  state_buffer->get_buffer());
      slipstore::init(streams,
		      config->vertex_state_buffer_size / config->tiles,
		      config->super_partitions);
      slipstore::slipstore_server->help_handle()->setup(config->super_partitions);
      open_streams = 2; //vertex and input streams are auto opened
      ingest_buffers = new memory_buffer *[config->max_streams];
      for (unsigned long i = 0; i < config->max_streams; i++) {
        ingest_buffers[i] = 0;
      }
      workers = new x_thread *[config->processors];
      thread_array = new boost::thread *[config->processors];
      cpu_state_array = new algorithm::per_processor_data *[config->processors];
      for (unsigned long i = 0; i < config->processors; i++) {
        cpu_state_array[i] = A::create_per_processor_data(i);
        workers[i] = new x_thread(config, i, cpu_state_array[i]);
        if (i > 0) {
          thread_array[i] = new boost::thread(boost::ref(*workers[i]));
        }
      }
      state_loaded = false;
      take_checkpoints = (vm.count("checkpoints") > 0);
      first_touch = new bool[config->super_partitions];
      for (unsigned long i = 0; i < config->super_partitions; i++) {
        first_touch[i] = true;
      }
      unsigned long slipchunk =
          slipstore::slipstore_client_fill->get_slipchunk();
      tile_buffers[0] = new unsigned char[slipchunk];
      tile_buffers[1] = new unsigned char[slipchunk];
      tile_copy_total_bytes = 0;
      if (ext_mem_shuffle) {
        ext_tmps[0] = open_stream();
        ext_tmps[1] = open_stream();
      }
      else {
        ext_tmps[0] = ULONG_MAX;
        ext_tmps[1] = ULONG_MAX;
      }
      // Wait for other machines to start up
      inter_machine_barrier();
    }

    const configuration *get_config() {
      return config;
    }

    void prep_state_filter(filter *state_filter)
    {
      for (unsigned long i = 0; i < config->cached_partitions; i++) {
	state_filter->q(i);
      }
    }

    unsigned long open_stream() {
      unsigned long stream_id = (open_streams++);
      return stream_id;
    }

    unsigned long tile_offset(unsigned long superp, unsigned long tile) {
      unsigned long startp = config->tile2partition(superp, tile);
      return state_buffer->index[0][startp];
    }

    unsigned long tile_size(unsigned long superp, unsigned long tile) {
      unsigned long size;
      unsigned long startp = config->tile2partition(superp, tile);
      unsigned long tile_partitions = config->cached_partitions / config->tiles;
      unsigned long stopp = startp + tile_partitions;
      if (stopp < config->cached_partitions) {
        size = state_buffer->index[0][stopp] - state_buffer->index[0][startp];
      }
      else {
        size = state_buffer->index[0][stopp - 1] -
               state_buffer->index[0][startp];
        size += config->vertex_size * config->state_count(superp, stopp - 1);
      }
      return size;
    }

    void setup_pmap(unsigned long superp)
    {
      /* Setup pmap */
      unsigned long v_so_far = 0;
      for (unsigned long i = 0; i < config->cached_partitions; i++) {
        //Count entries in the partition
        state_buffer->index[0][i] = v_so_far;
        v_so_far += config->vertex_size * config->state_count(superp, i);
      }
    }
    
    void state_load(unsigned long superp)
    {
      // Make sure state is uptodate
      setup_pmap(superp);
      unsigned long local_tile_size;
      local_tile_size = tile_size(superp, 0);

      if (first_touch[superp]) {
        state_buffer->bufsize = local_tile_size;
      }
      else {
	tile_copy_done    = false;
	state_buffer->uptodate = false;
	io_wait_time.start();
	slipstore::ioService.post(boost::bind(&streamIO<A>::tile_copy,
					      this,
					      superp,
					      0));
	while (!tile_copy_done);
	io_wait_time.stop();
	state_buffer->bufsize = local_tile_size;
	state_buffer->uptodate = true;
      }
      state_loaded = true;
    }

    void state_store(unsigned long superp) {
      first_touch[superp] = false;
      unsigned long local_tile_off = tile_offset(superp, 0);
      tile_reset_work wk(slipstore::STREAM_VERTEX_STATE, superp);
      wk.in_progress = true;
      slipstore::ioService.post
	(boost::bind(&tile_reset_work::do_it,
		     &wk));
      while (wk.in_progress);
      state_buffer->uptodate = false;
      slipstore::ioService.post(boost::bind(&memory_buffer::drain_tile,
					    state_buffer,
					    slipstore::slipstore_client_drain,
					    superp,
					    local_tile_off));
      // Synchronous I/O
      io_wait_time.start();
      state_buffer->wait_uptodate();
      io_wait_time.stop();
      state_loaded = false;
    }

    bool stream_eof(int stream) {
      bool empty;
      check_empty_work wk(stream, streams);
      wk.in_progress = true;
      inter_machine_barrier();
      slipstore::ioService.post
          (boost::bind(&check_empty_work::do_it,
                       &wk));
      while (wk.in_progress);
      empty = wk.empty;
      inter_machine_barrier();
      return empty;
    }

    void send_semup(unsigned long mc) {
      semup_work wk(mc);
      wk.in_progress = true;
      slipstore::ioService.post
          (boost::bind(&semup_work::do_it,
                       &wk));
      while (wk.in_progress);
    }

    void terminate() {
      workers[0]->terminate = true;
      (*workers[0])();

      for (unsigned long i = 1; i < config->processors; i++) {
        thread_array[i]->join();
      }
      inter_machine_barrier(); // avoid killing servers while messages inflight
      slipstore::shutdown();
      delete streams;
      struct rusage ru;
      (void) getrusage(RUSAGE_SELF, &ru);
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAX_RSS_KB " << ru.ru_maxrss;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MINFLT " << ru.ru_minflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAJFLT " << ru.ru_majflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::MAJFLT " << ru.ru_majflt;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::INBLK " << ru.ru_inblock;
      BOOST_LOG_TRIVIAL(info) << "CORE::RUSAGE::OUBLK " << ru.ru_oublock;
      BOOST_LOG_TRIVIAL(info) << "CORE::UTILS::BYTES_READ " << stat_bytes_read;
      BOOST_LOG_TRIVIAL(info) << "CORE::UTILS::BYTES_WRITTEN " <<
      stat_bytes_written;
      BOOST_LOG_TRIVIAL(info) << "CORE::UTILS::TILE_COPY_BYTES " <<
      tile_copy_total_bytes;
      io_wait_time.print("CORE::TIME::IO_WAIT");
      tile_copy_time.print("CORE::TIME_TILE_COPY");
      tile_merge_time.print("CORE::TIME_TILE_MERGE");
      processing_stolen_time.print("CORE::TIME_PROCESSING_STOLEN");
      merge_wait_time.print("CORE::TIME_MERGE_WAIT");
      im_barrier_time.print("CORE::TIME_ALL_MC_BARRIER");
    }

    void rewind_stream(unsigned long stream) {
      for (unsigned long i = 0; i < config->super_partitions; i++) {
        rewind_barrier_work rewind_obj(stream, i, streams);
        slipstore::slipstore_client_barrier->in_progress = true;
        slipstore::ioService.post
            (boost::bind(&slipstore::client_barrier::work_barrier,
                         slipstore::slipstore_client_barrier,
                         &rewind_obj));
        while (slipstore::slipstore_client_barrier->in_progress);
      }
    }

    void reset_stream(unsigned long stream, unsigned long super_partition) {
      reset_barrier_work reset_obj(stream, super_partition, streams);
      slipstore::slipstore_client_barrier->in_progress = true;
      slipstore::ioService.post
          (boost::bind(&slipstore::client_barrier::work_barrier,
                       slipstore::slipstore_client_barrier,
                       &reset_obj));
      while (slipstore::slipstore_client_barrier->in_progress);
    }

    template<typename B, typename IN, typename OUT>
    friend
    void do_stream_internal(streamIO<B> *sio,
                            unsigned long superp,
                            unsigned long tile,
                            unsigned long stream_in,
                            unsigned long stream_out,
                            filter *override_input_filter,
			    bool sync);
    template<typename B, typename OBJ>
    friend
    void do_spcopy(streamIO<B> *sio,
                   unsigned long stream,
                   unsigned long superp,
                   unsigned long tile,
                   unsigned long stream_out);

    template<typename B, typename IN, typename OUT>
    friend
    bool do_stream_skip(streamIO<B> *sio,
                   unsigned long stream_in,
                   unsigned long stream_out,
                   filter *override_input_filter,
                   bool sync,
                   bool reset);

    template<typename B, typename IN, typename OUT>
    friend
    bool do_init_stream(streamIO<B> *sio,
			unsigned long stream_in,
			unsigned long stream_out);

    template<typename B>
    friend
    bool do_cpu(streamIO<B> *sio, unsigned long superp);

    template<typename B>
    friend
    void do_state_iter(streamIO<B> *sio);

    template<typename B>
    friend
    bool ingest(streamIO<B> *sio,
                unsigned long stream_in,
                ingest_t *ingest_segment);

    template<typename B>
    friend
    void merge_ingest(streamIO<B> *sio, unsigned long stream);

    template<typename B>
    friend
    void sync_tile(streamIO<B> *sio,
                   unsigned long superp,
		   unsigned long mc);

    template<typename B>
    friend
    bool load_checkpoint(streamIO<B> *sio, B *app);

    template<typename B>
    friend
    void take_checkpoint(streamIO<B> *sio, B *app);
  };

#define SETUP_STREAMOUT()            \
  do {                  \
    if(stream_out != ULONG_MAX) {          \
      work_item.stream_out =            \
  memory_buffer::get_free_buffer(sio->io_wait_time);    \
      work_item.disk_stream_out =stream_out;        \
      work_item.io_clock = &sio->io_wait_time;        \
    }                  \
    else {                \
      work_item.stream_out = NULL;          \
      work_item.disk_stream_out = ULONG_MAX;        \
      work_item.io_clock = NULL;          \
    }                  \
    sio->workers[0]->work_to_do = &work_item;        \
  }while(0)


  template<typename A, typename IN, typename OUT>
  static void do_stream_internal(streamIO<A> *sio,
                                 unsigned long superp,
                                 unsigned long tile,
                                 unsigned long stream_in,
                                 unsigned long stream_out,
                                 filter *override_input_filter,
                                 bool sync) {
    struct work<A, IN, OUT> work_item;
    work_item.superp = superp;
    work_item.state = sio->state_buffer;
    work_item.ingest = NULL;
    work_item.local_tile = (
    (superp % slipstore::slipstore_client_fill->get_machines())==
    slipstore::slipstore_client_fill->get_me()
			    );
    if (stream_in != ULONG_MAX) {
      if (sio->ingest_buffers[stream_in] != NULL) {
        work_item.ingest = sio->ingest_buffers[stream_in];
        work_item.stream_in = memory_buffer::get_free_buffer(sio->io_wait_time);
        work_item.input_filter = work_item.stream_in->work_queues;
        SETUP_STREAMOUT();
        (*sio->workers[0])();
        work_item.stream_in->cleanup();
        memory_buffer::release_buffer(work_item.stream_in);
      }
      memory_buffer *outstanding = memory_buffer::get_free_buffer(sio->io_wait_time);
      outstanding->uptodate = false;
      slipstore::ioService.post(boost::bind(&memory_buffer::fill,
                                            outstanding,
                                            slipstore::slipstore_client_fill,
                                            stream_in,
                                            superp,
                                            tile,
                                            IN::item_size()));
      while (true) {
        sio->io_wait_time.start();
        outstanding->wait_uptodate();
        sio->io_wait_time.stop();
        if (outstanding->bufsize == 0) {
          outstanding->cleanup();
          memory_buffer::release_buffer(outstanding);
          break;
        }
        work_item.stream_in = outstanding;
        outstanding = memory_buffer::get_free_buffer(sio->io_wait_time);
        outstanding->uptodate = false;
        slipstore::ioService.post(boost::bind(&memory_buffer::fill,
                                              outstanding,
                                              slipstore::slipstore_client_fill,
                                              stream_in,
                                              superp,
                                              tile,
                                              IN::item_size()));
        if (override_input_filter != NULL) {
          work_item.input_filter = override_input_filter;
        }
        else {
          work_item.input_filter = work_item.stream_in->work_queues;
        }
        SETUP_STREAMOUT();
        (*sio->workers[0])();
        work_item.stream_in->cleanup();
        memory_buffer::release_buffer(work_item.stream_in);
      }
    }
    else {
      BOOST_ASSERT_MSG(override_input_filter != NULL,
                       "Must have input stream or input filter !");
      work_item.input_filter = override_input_filter;
      work_item.stream_in = NULL;
      BOOST_ASSERT_MSG(stream_out != ULONG_MAX,
                       "Must have input stream or output stream !");
      work_item.stream_out = memory_buffer::get_free_buffer(sio->io_wait_time);
      work_item.disk_stream_out = stream_out;
      work_item.io_clock = &sio->io_wait_time;
      sio->workers[0]->work_to_do = &work_item;
      (*sio->workers[0])();
    }
  }

  template<typename A, typename OBJ>
  static void do_spcopy(streamIO<A> *sio,
                        unsigned long stream,
                        unsigned long superp,
                        unsigned long tile,
                        unsigned long stream_out) {
    struct sp_copy<OBJ> work_item;
    work_item.disk_stream_out = stream_out;
    work_item.io_clock = &sio->io_wait_time;
    work_item.handle = sio->streams;
    memory_buffer *outstanding = memory_buffer::get_free_buffer(sio->io_wait_time);
    outstanding->uptodate = false;
    slipstore::ioService.post(boost::bind(&memory_buffer::fill_local,
                                          outstanding,
                                          slipstore::slipstore_client_fill,
                                          sio->streams,
                                          stream,
                                          superp,
                                          tile,
                                          OBJ::item_size()));
    while (true) {
      sio->io_wait_time.start();
      outstanding->wait_uptodate();
      sio->io_wait_time.stop();
      if (outstanding->bufsize == 0) {
        outstanding->cleanup();
        memory_buffer::release_buffer(outstanding);
        break;
      }
      work_item.stream_out = outstanding;
      outstanding = memory_buffer::get_free_buffer(sio->io_wait_time);
      outstanding->uptodate = false;
      slipstore::ioService.post(boost::bind(&memory_buffer::fill_local,
                                            outstanding,
                                            slipstore::slipstore_client_fill,
                                            sio->streams,
                                            stream,
                                            superp,
                                            tile,
                                            OBJ::item_size()));
      sio->workers[0]->work_to_do = &work_item;
      (*sio->workers[0])();
    }
  }

  template<typename A>
  void sync_tile(streamIO<A> *sio,
                 unsigned long superp,
                 unsigned long mc) {
    vertex_merge_work<A> wk;
    unsigned long io_tile = 0;
    unsigned long tile_bytes = sio->tile_size(superp, 0);
    unsigned long slipchunk =
        slipstore::slipstore_client_fill->get_slipchunk();
    unsigned long tile_offset = sio->tile_offset(superp, 0);
    unsigned char *main_tile =
        sio->state_buffer->get_buffer() + tile_offset;
    unsigned long io_bytes;
    io_bytes = (tile_bytes > slipchunk) ? slipchunk : tile_bytes;
    io_bytes = (io_bytes / sio->config->vertex_size) * sio->config->vertex_size;
    // Issue first bit of I/O
    if (io_bytes > 0) {
      sio->tile_copy_done = false;
      slipstore::ioService.post(boost::bind(&streamIO<A>::tile_copy_partial,
                                            sio,
                                            sio->tile_buffers[io_tile],
                                            mc,
                                            tile_offset,
                                            io_bytes));
    }
    while (tile_bytes) {
      sio->io_wait_time.start();
      while (!sio->tile_copy_done);
      sio->io_wait_time.stop();
      wk.tile_vertices = io_bytes / sio->config->vertex_size;
      wk.main_tile = main_tile;
      wk.copy_tile = sio->tile_buffers[io_tile];
      wk.copy_machine = mc;
      /// Issue next transfer
      tile_bytes -= io_bytes;
      tile_offset += io_bytes;
      main_tile += io_bytes;
      io_tile = 1 - io_tile;
      io_bytes = (tile_bytes > slipchunk) ? slipchunk : tile_bytes;
      io_bytes = (io_bytes / sio->config->vertex_size) * sio->config->vertex_size;
      if (io_bytes > 0) {
        sio->tile_copy_done = false;
        slipstore::ioService.post(boost::bind(&streamIO<A>::tile_copy_partial,
                                              sio,
                                              sio->tile_buffers[io_tile],
                                              mc,
                                              tile_offset,
                                              io_bytes));
      }
      ///////////////////////
      sio->workers[0]->work_to_do = &wk;
      (*sio->workers[0])();
    }
  }

  template<typename A>
  static bool load_checkpoint(streamIO<A> *sio, A *app) {
    // Determine start checkpoint if any
    unsigned long checkpoint_meta_size =
        sio->streams->snap_metadata_size();
    unsigned long local_check = find_checkpoint(A::checkpoint_size(),
                                                checkpoint_meta_size);
    sync_start_checkpoint *obj =
        new sync_start_checkpoint(local_check);
    slipstore::sync_barrier<sync_start_checkpoint>(slipstore::slipstore_client_barrier,
                                                   obj);
    if (obj->checkpoint == ULONG_MAX) {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CHECKPOINTS NONE FOUND";
      delete obj;
      if (local_check != ULONG_MAX) {
        del_checkpoint(local_check);
        sio->streams->delete_snapshot(local_check);
        if (local_check > 0) {
          del_checkpoint(local_check - 1);
          sio->streams->delete_snapshot(local_check - 1);
        }
        del_checkpoint(local_check + 1);
        sio->streams->delete_snapshot(local_check + 1);
      }
      if (sio->take_checkpoints) {
        set_checkpoint_no(0);
      }
      return false;
    }
    else {
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::CHECKPOINT FOUND "
      << obj->checkpoint;
      // Cleaup any preceding and following checkpoints
      if (obj->checkpoint > 0) {
        del_checkpoint(obj->checkpoint - 1);
        sio->streams->delete_snapshot(obj->checkpoint - 1);
      }
      del_checkpoint(obj->checkpoint + 1);
      sio->streams->delete_snapshot(obj->checkpoint + 1);
      unsigned char *buffer = new unsigned char[A::checkpoint_size()];
      unsigned char *metabuf = new unsigned char[checkpoint_meta_size];
      read_checkpoint(buffer,
                      A::checkpoint_size(),
                      metabuf,
                      checkpoint_meta_size,
                      obj->checkpoint);
      app->restore_checkpoint(buffer);
      sio->streams->restore_snapshot(obj->checkpoint,
                                     metabuf);
      for (unsigned long i = 0; i < sio->config->super_partitions; i++) {
        sio->first_touch[i] = false;
      }
      if (!sio->take_checkpoints) {
        del_checkpoint(obj->checkpoint);
        sio->streams->delete_snapshot(obj->checkpoint);
      }
      else {
        set_checkpoint_no(obj->checkpoint + 1);
      }
      delete buffer;
      delete metabuf;
      delete obj;
      return true;
    }
  }

  template<typename A>
  static void take_checkpoint(streamIO<A> *sio, A *app) {
    if (sio->take_checkpoints) { // Take a checkpoint
      // Make sure updates have hit storage server
      sio->inter_machine_barrier();
      unsigned char *app_buffer = new unsigned char[A::checkpoint_size()];
      unsigned char *runtime_buffer;
      unsigned long cno = next_checkpoint();
      runtime_buffer = sio->streams->take_snapshot(cno);
      sio->inter_machine_barrier(); // Checkpoint complete
      // Write out checkpoint metadata
      app->take_checkpoint(app_buffer);
      write_checkpoint(cno,
                       app_buffer,
                       A::checkpoint_size(),
                       runtime_buffer,
                       sio->streams->snap_metadata_size());
      delete app_buffer;
      sio->inter_machine_barrier(); // Metadata write complete
      // Delete previous checkpoint
      if (cno > 0) {
        del_checkpoint(cno - 1);
        sio->streams->delete_snapshot(cno - 1);
      }
    }
  }


  template<typename A, typename IN, typename OUT>
  static bool do_stream_skip(streamIO<A> *sio,
                        unsigned long stream_in,
                        unsigned long stream_out,
                        filter *override_input_filter,
                        bool sync,
                        bool reset)
  {
    const bool log_phases = vm.count("log_phases") > 0;

    const unsigned long me = slipstore::slipstore_client_fill->get_me();
    const unsigned long m = slipstore::slipstore_client_fill->get_machines();
    order_work order_wk;
    
    bool reduce_result = true;
    slipstore::slipstore_server->help_handle()->reset();
    if(sio->sorted_order) {
      order_wk.cmd    = slipstore::CMD_ORDER_INIT;
      order_wk.stream = stream_in;
      order_wk.in_progress = true;
      slipstore::ioService.post(boost::bind(&order_work::do_it,
					  &order_wk));
      while(order_wk.in_progress);
    }
    // Make sure everyone is ready
    sio->inter_machine_barrier();

    unsigned long use_stream_out;
    unsigned long visible_bits = 0;
    if (sio->ext_mem_shuffle) {
      visible_bits = configuration::ext_fanout_bits;
      if (visible_bits > configuration::ext_mem_bits) {
	visible_bits = configuration::ext_mem_bits;
      }
    }
    if (stream_out != ULONG_MAX) {
      if (sio->ext_mem_shuffle) {
	map_spshift_wrap::map_spshift =
	  configuration::ext_mem_bits - visible_bits;
	if (map_spshift_wrap::map_spshift != 0) {
	  use_stream_out = sio->ext_tmps[0];
	}
	else {
	  use_stream_out = stream_out;
	}
      }
      else {
	use_stream_out = stream_out;
	map_spshift_wrap::map_spshift = 0;
        }
    }
    else {
      use_stream_out = ULONG_MAX;
    }
    
    
    // First do all of my work
    //while(true) {
    for(unsigned long index = 0;
	index <  sio->get_config()->super_partitions;
	index++) {
      unsigned long partition;
      if(sio->sorted_order) {
	order_wk.cmd    = slipstore::CMD_ORDER_NEXT;
	order_wk.index     = index;
	order_wk.master    = sio->centralized_order ? 0:me;
	order_wk.in_progress = true;
	slipstore::ioService.post(boost::bind(&order_work::do_it,
					      &order_wk));
	while(order_wk.in_progress);
	partition = order_wk.order_next;
      }
      else {
	partition = index;
      }
      if(partition % sio->get_config()->super_partitions != me) {
	continue;
      }

      if(log_phases) {
        if(sync) {
          BOOST_LOG_TRIVIAL(info)
          << clock::timestamp()
          << " Started gather ";
        } else {
          BOOST_LOG_TRIVIAL(info)
          << clock::timestamp()
          << " Started scatter ";
        }
      }

      //for(unsigned long partition=me;
      //partition < sio->get_config()->super_partitions;
      //partition += m) {
      sio->state_load(partition);
      do_stream_internal<A, IN, OUT>(sio, partition, 0, stream_in,
				     use_stream_out,
				     override_input_filter, sync);
      reduce_result = reduce_result &&
                      sio->cpu_state_array[0]->reduce(sio->cpu_state_array,
                                                      sio->config->processors);

      // Stop accepting help
      slipstore::slipstore_server->help_handle()->close(partition);
      // Synchronize with helpers for sync
      if(sync) {
	while(true) {
	  unsigned long mc = slipstore::slipstore_server->help_handle()->next_helper(partition);
	  if(mc == ULONG_MAX) {
	    break;
	  }
    sio->merge_wait_time.start();
	  slipstore::slipstore_server->help_handle()->sem_down(mc);
    sio->merge_wait_time.stop();
	  sio->tile_merge_time.start();
	  sync_tile<A>(sio, partition, mc);
	  sio->tile_merge_time.stop();
	  // Release mc
	  sio->send_semup(mc);
	}
	sio->state_store(partition);
      }
      if(log_phases) {
        if(sync) {
          BOOST_LOG_TRIVIAL(info)
          << clock::timestamp()
          << " Completed gather ";
        } else {
          BOOST_LOG_TRIVIAL(info)
          << clock::timestamp()
          << " Completed scatter ";
        }
      }
    }

    // Help others
    if (stream_in != ULONG_MAX) {
      //  while(true) {
      for(unsigned long index = 0;
	  index <  sio->get_config()->super_partitions;
	  index++) {
	unsigned long partition;
	if(sio->sorted_order) {
	  order_wk.cmd    = slipstore::CMD_ORDER_NEXT;
	  order_wk.index     = index;
	  order_wk.master    = sio->centralized_order ? 0:me;
	  order_wk.in_progress = true;
	  slipstore::ioService.post(boost::bind(&order_work::do_it,
						&order_wk));
	  while(order_wk.in_progress);
	  partition = order_wk.order_next;
	}
	else {
	  partition = index;
	}
	//for(unsigned long partition=0;
	//  partition < sio->get_config()->super_partitions;
	//  partition ++) {
	unsigned long beneficiary = partition % m;
	if (beneficiary  == me) {
	  continue;
	}
	help_offer offer(stream_in, partition, beneficiary, sync);
	slipstore::ioService.post(boost::bind(&help_offer::make_offer,
					      &offer));
	sio->io_wait_time.start();
	while (!offer.done);
	sio->io_wait_time.stop();
	if (offer.accepted) {
    if(log_phases) {
        BOOST_LOG_TRIVIAL(info)
        << clock::timestamp()
        << " Started helping " << beneficiary << " ";
    }
	  // Offer of help accepted
	  // fill the sp
	  sio->tile_copy_time.start();
	  sio->tile_copy_done = false;

	  // Setup the state buffer index
          sio->setup_pmap(partition);
          sio->state_buffer->bufsize = sio->tile_size(partition, 0);

	  slipstore::ioService.post(boost::bind(&streamIO<A>::tile_copy,
						sio,
						partition,
						0));
	  sio->io_wait_time.start();
	  while (!sio->tile_copy_done);
	  sio->io_wait_time.stop();
	  sio->tile_copy_time.stop();

	  // Now do the stream

    sio->processing_stolen_time.start();
	  do_stream_internal<A, IN, OUT>(sio, partition,
					 0, stream_in,
					 use_stream_out,
					 override_input_filter, sync);
    sio->processing_stolen_time.stop();
	  if(sync) { 
	    // Signal completion
	    sio->send_semup(beneficiary);
	    // Wait for the merge to happen
      sio->merge_wait_time.start();
	    slipstore::slipstore_server->help_handle()->sem_down(beneficiary);
      sio->merge_wait_time.stop();
	  }
    if(log_phases) {
      BOOST_LOG_TRIVIAL(info)
      << clock::timestamp()
      << " Completed helping " << beneficiary << " ";
    }
	}
      }
    }
    
    if(reset) {
      for(unsigned long partition=0;
	  partition < sio->get_config()->super_partitions;
	  partition ++) {
	sio->reset_stream(stream_in, partition); // Also a barrier
      }
    }
    if (stream_out != ULONG_MAX && sio->ext_mem_shuffle) {
      unsigned long input = 0;
      while (visible_bits < configuration::ext_mem_bits) {
	visible_bits += configuration::ext_fanout_bits;
	if (visible_bits > configuration::ext_mem_bits) {
	  visible_bits = configuration::ext_mem_bits;
	}
	map_spshift_wrap::map_spshift =
	  configuration::ext_mem_bits - visible_bits;
	sio->rewind_stream(sio->ext_tmps[input]); // also a barrier
	for (unsigned long i = 0; i < sio->get_config()->super_partitions; i++) {
	  for (unsigned long j = 0; j < sio->get_config()->tiles; j++) {
	    do_spcopy<A, OUT>(sio,
			      sio->ext_tmps[input],
			      i, j,
			      visible_bits == configuration::ext_mem_bits ?
			      stream_out : sio->ext_tmps[1 - input]);
	  }
	  sio->reset_stream(sio->ext_tmps[input], i);
	}
	input = 1 - input;
      }
    }
    sio->inter_machine_barrier(); // All done
    return reduce_result;
  }


  template<typename A, typename IN, typename OUT>
  static bool do_init_stream(streamIO<A> *sio,
			     unsigned long stream_in,
			     unsigned long stream_out)
  {
    // Make sure everyone is ready
    slipstore::slipstore_server->help_handle()->reset();
    sio->setup_pmap(0);
    sio->state_buffer->bufsize = sio->tile_size(0, 0);
    sio->inter_machine_barrier();
    unsigned long use_stream_out;
    unsigned long visible_bits = 0;
    if (sio->ext_mem_shuffle) {
      visible_bits = configuration::ext_fanout_bits;
      if (visible_bits > configuration::ext_mem_bits) {
	visible_bits = configuration::ext_mem_bits;
      }
    }
    if (stream_out != ULONG_MAX) {
      if (sio->ext_mem_shuffle) {
	map_spshift_wrap::map_spshift =
	  configuration::ext_mem_bits - visible_bits;
	if (map_spshift_wrap::map_spshift != 0) {
	  use_stream_out = sio->ext_tmps[0];
	}
	else {
	  use_stream_out = stream_out;
	}
      }
      else {
	use_stream_out = stream_out;
	map_spshift_wrap::map_spshift = 0;
      }
    }
    else {
      use_stream_out = ULONG_MAX;
    }
    do_stream_internal<A, IN, OUT>(sio, 0, 0, stream_in, use_stream_out,
				   NULL, false);
    sio->inter_machine_barrier();

    if (stream_out != ULONG_MAX && sio->ext_mem_shuffle) {
      unsigned long input = 0;
      while (visible_bits < configuration::ext_mem_bits) {
	visible_bits += configuration::ext_fanout_bits;
	if (visible_bits > configuration::ext_mem_bits) {
	  visible_bits = configuration::ext_mem_bits;
	}
	map_spshift_wrap::map_spshift =
	  configuration::ext_mem_bits - visible_bits;
	sio->rewind_stream(sio->ext_tmps[input]);
	for (unsigned long i = 0; i < sio->get_config()->super_partitions; i++) {
	  for (unsigned long j = 0; j < sio->get_config()->tiles; j++) {
	    do_spcopy<A, OUT>(sio,
			      sio->ext_tmps[input],
			      i, j,
			      visible_bits == configuration::ext_mem_bits ?
			      stream_out : sio->ext_tmps[1 - input]);
	  }
	  sio->reset_stream(sio->ext_tmps[input], i);
	}
	input = 1 - input;
      }
      sio->inter_machine_barrier();
    }
    return false;
  }
  

  class DUMMY_IN {
  public:
    static unsigned long item_size() {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }

    static unsigned long key(unsigned char *buffer) {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
  };

  class DUMMY_OUT {
  public:
    static unsigned long item_size() {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }

    static unsigned long key(unsigned char *buffer) {
      BOOST_ASSERT_MSG(false, "Should not be called !");
      return 0;
    }
  };

  template<typename A>
  static bool do_cpu(streamIO<A> *sio, unsigned long superp) {
    struct work<A, DUMMY_IN, DUMMY_OUT> work_item;
    map_spshift_wrap::map_spshift = 0;
    work_item.state = NULL;
    work_item.stream_in = NULL;
    work_item.ingest = NULL;
    work_item.stream_out = NULL;
    sio->workers[0]->work_to_do = &work_item;
    (*sio->workers[0])();
    bool reduce_result =
        sio->cpu_state_array[0]->reduce(sio->cpu_state_array,
                                        sio->config->processors);
    return reduce_result;
  }

  template<typename A>
  static void do_state_iter(streamIO<A> *sio) {
    state_iter_work<A> work;
    const unsigned long me = slipstore::slipstore_client_fill->get_me();
    const unsigned long m = slipstore::slipstore_client_fill->get_machines();
    for(unsigned long superp=me;
	superp < sio->get_config()->super_partitions;
	superp += m) {
      sio->state_load(superp);
      work.state_buffer = sio->state_buffer;
      work.superp = superp;
      sio->workers[0]->work_to_do = &work;
      (*sio->workers[0])();
      sio->state_store(superp);
    }
  }

  // Returns true on eof
  template<typename A>
  static bool ingest(streamIO<A> *sio,
                     unsigned long stream,
                     ingest_t *ingest_segment) {
    while (ingest_segment->avail == 0 && !ingest_segment->eof);
    if (ingest_segment->eof) {
      return true;
    }
    memory_buffer *ingest_buffer = memory_buffer::get_free_buffer(sio->io_wait_time);
    memcpy(ingest_buffer->buffer, ingest_segment->buffer, ingest_segment->avail);
    ingest_buffer->bufsize = ingest_segment->avail;
    ingest_segment->avail = 0; // Signal that the ingest segment is free
    sio->ingest_buffers[stream] = ingest_buffer;
    return false;
  }

  template<typename A>
  static void merge_ingest(streamIO<A> *sio, unsigned long stream) {
    if (sio->ingest_buffers[stream] != NULL) {
      sio->streams[stream]->ingest(sio->ingest_buffers[stream]);
      sio->ingest_buffers[stream] = NULL;
    }
  }
}
#endif
