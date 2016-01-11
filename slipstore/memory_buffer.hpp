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

#ifndef _SPLIT_STREAM_
#define _SPLIT_STREAM_

#include "interface.hpp"
#include "../core/autotuner.hpp"
#include "../core/x-splitter.hpp"
#include "../utils/options_utils.h"
#include "../utils/memory_utils.h"
#include "../utils/barrier.h"
#include<zlib.h>

#ifdef PYTHON_SUPPORT
#include</usr/include/python3.2/Python.h>
#include</usr/lib/python3/dist-packages/numpy/core/include/numpy/arrayobject.h>
#endif

namespace x_lib {
  class buffer_manager;

  class memory_buffer {
      unsigned long *per_cpu_sizes;
      static bool use_qsort;
      static bool do_batch_io;
      static bool gather_io_drain;
      static unsigned long **aux_index;
      static unsigned char *aux_buffer;
#ifdef PYTHON_SUPPORT
      static PyObject *auxpBuffer;
#endif
      volatile unsigned long index_entries; // How many partitions ?
      unsigned long max_partitions;
      // Buffer free list stuff
      static memory_buffer *volatile freelist;
      static volatile unsigned long freelist_lock;
      memory_buffer *volatile freelist_next;

      static void get_freelist_lock() {
        while (true) {
          while (freelist_lock == 1);
          if (__sync_bool_compare_and_swap(&freelist_lock, 0, 1)) {
            break;
          }
        }
        __sync_synchronize();
      }

      static void release_freelist_lock() {
        __sync_synchronize();
        freelist_lock = 0;
      }

      void switch_with_aux() {
        unsigned long **tmp_index;
        tmp_index = aux_index;
        aux_index = index;
        index = tmp_index;
        unsigned char *tmp_buffer;
        tmp_buffer = aux_buffer;
        aux_buffer = buffer;
        buffer = tmp_buffer;
#ifdef PYTHON_SUPPORT
        PyObject * tmp_pbuffer;
        tmp_pbuffer = auxpBuffer;
        auxpBuffer = pBuffer;
        pBuffer = tmp_pbuffer;
#endif
      }

  public:
      filter *work_queues;
      unsigned long **index;
      slipstore::substream_tracker *subtrack;
      //[cpu][partition]
      const unsigned long bufbytes;
      volatile bool indexed;
      // Accessed by IO
      unsigned char *buffer;
      unsigned long bufsize;
      volatile bool uptodate;   // I/O outstanding
      bool dirty; // Debug aid
      volatile unsigned long skip_superp;
      configuration *config;
#ifdef PYTHON_SUPPORT
      PyObject *pBuffer;
#endif

      memory_buffer(configuration *details,
                    unsigned long bufbytes_in)
          : bufbytes(bufbytes_in),
            indexed(false),
            bufsize(0),
            uptodate(true),
            dirty(false),
            skip_superp(-1UL),
            config(details) {
        max_partitions = (config->cached_partitions > config->super_partitions) ?
                         config->cached_partitions * config->tiles : config->super_partitions * config->tiles;
        work_queues = new filter(max_partitions, config->processors);
        index = new unsigned long *[config->processors];
        subtrack = new slipstore::substream_tracker();
        subtrack->per_cpu_end = new unsigned long[config->processors];
        per_cpu_sizes = new unsigned long[config->processors];
        for (unsigned long i = 0; i < config->processors; i++) {
          index[i] = new unsigned long[max_partitions];
          per_cpu_sizes[i] = 0;
          for (unsigned long j = 0; j < max_partitions; j++) {
            index[i][j] = 0;
          }
        }
        if (aux_index == NULL) {
          use_qsort = (vm.count("qsort") > 0);
          do_batch_io = (vm.count("request_batching") > 0);
          gather_io_drain = (vm.count("gather_io_drain") > 0);
          if (!use_qsort) {
            aux_index = new unsigned long *[config->processors];
            for (unsigned long i = 0; i < config->processors; i++) {
              aux_index[i] = new unsigned long[max_partitions];
              for (unsigned long j = 0; j < max_partitions; j++) {
                aux_index[i][j] = 0;
              }
            }
          }
        }
        buffer = (unsigned char *)
            map_anon_memory(bufbytes, true, "memory buffer");
#ifdef PYTHON_SUPPORT
        npy_intp dim[1]={bufbytes};
        pBuffer = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)buffer);
#endif
        if (aux_buffer == NULL && !use_qsort) {
          aux_buffer = (unsigned char *)
              map_anon_memory(bufbytes, true, "memory buffer");
#ifdef PYTHON_SUPPORT
          auxpBuffer = PyArray_SimpleNewFromData(1, dim, NPY_UINT8, (void *)aux_buffer);
#endif
        }
      }

      static
      memory_buffer *get_free_buffer(rtc_clock &wait_time) {
        memory_buffer *buf = NULL;
        // get_buffer serves as flow control point !
        wait_time.start();
        while (buf == NULL) {
          while (freelist == NULL);
          get_freelist_lock();
          buf = freelist;
          if (buf != NULL) {
            freelist = buf->freelist_next;
          }
          release_freelist_lock();
        }
        wait_time.stop();
        BOOST_ASSERT_MSG(buf->bufsize == 0, "Buffer size not zero !");
        return buf;
      }

      static
      void release_buffer(memory_buffer *buf) {
        BOOST_ASSERT_MSG(buf->bufsize == 0, "Freeing non-empty buffer !");
        get_freelist_lock();
        buf->freelist_next = freelist;
        freelist = buf;
        release_freelist_lock();
      }

      static void initialize_freelist(configuration *config,
                                      unsigned long bufbytes,
                                      unsigned long bufcount) {
        memory_buffer *new_buf;
        freelist = NULL;
        for (unsigned long i = 0; i < bufcount; i++) {
          new_buf = new memory_buffer(config, bufbytes);
          new_buf->freelist_next = freelist;
          freelist = new_buf;
        }
        freelist_lock = 0;
        __sync_synchronize();
      }

      // Reset the index
      void reset_index(unsigned long processor_id) {
        per_cpu_sizes[processor_id] = 0;
        memset(index[processor_id], 0,
               max_partitions * sizeof(unsigned long));
        work_queues->done(processor_id);
        skip_superp = -1UL;
      }

      void set_flat_index() {
        per_cpu_sizes[0] = bufsize;
        index[0][0] = 0;
        index_entries = 1;
        indexed = true;
      }

      void cleanup() {
        BOOST_ASSERT_MSG(uptodate == true, "Trying to free a busy buffer !");
        BOOST_ASSERT_MSG(dirty == false, "Trying to free a dirty buffer !");
        if (indexed) {
          for (unsigned long i = 0; i < config->processors; i++) {
            reset_index(i);
          }
          indexed = false;
        }
        bufsize = 0;
      }

      unsigned char *get_buffer() {
        return buffer;
      }

      unsigned char *get_substream(unsigned long processor,
                                   unsigned long partition,
                                   unsigned long *size) {
        BOOST_ASSERT_MSG(indexed, "Cannot extract substream without index");
        BOOST_ASSERT_MSG(partition < index_entries,
                         "Too few partitions in index !");
        if (partition < (index_entries - 1)) {
          *size = (index[processor][partition + 1] -
                   index[processor][partition]);
        }
        else {
          *size = (per_cpu_sizes[processor] + index[processor][0]) -
                  index[processor][partition];
        }
        BOOST_ASSERT_MSG((*size) <= per_cpu_sizes[processor],
                         "Substream size out of range");
        return buffer + index[processor][partition];
      }

      void wait_uptodate() {
        while (!uptodate);
        __sync_synchronize();
      }

      template<typename T, typename M>
      friend void
          make_index(memory_buffer *stream,
                     unsigned long processor_id,
                     unsigned long partitions,
                     x_barrier *thread_sync);

      // Slipstore functionality
      void fill(slipstore::client_fill *client,
                unsigned long stream,
                unsigned long partition,
                unsigned long tile,
                unsigned long align) {
        unsigned char *bufhead = buffer;
        slipstore::slipstore_req_t req;
        if (do_batch_io) {
          unsigned long csize = client->get_slipchunk();
          csize = (csize / align) * align;
          req.cmd = slipstore::CMD_FILL;
          req.stream = stream;
          req.partition = partition;
          req.tile = tile;
          bufsize = client->batch_access_store(&req,
                                               bufhead,
                                               csize,
                                               bufbytes);
        }
        else {
          bufsize = 0;
          while (bufbytes >= (bufsize + align)) {
            unsigned long bytes = (bufbytes - bufsize);
            if (bytes > client->get_slipchunk()) {
              bytes = client->get_slipchunk();
            }
            bytes = (bytes / align) * align;
            req.cmd = slipstore::CMD_FILL;
            req.stream = stream;
            req.partition = partition;
            req.tile = tile;
            req.size = bytes;
            bool ret;
            if (stream == slipstore::STREAM_INPUT) {
              ret = client->access_store(&req, bufhead, client->get_me());
            }
            else {
              ret = client->access_store(&req, bufhead);
            }
            if (!ret) {
              break;
            }
            bufhead += req.size;
            bufsize += req.size;
          }
        }
        __sync_synchronize();
        uptodate = true;
      }

      // Slipstore functionality
      void fill_local(slipstore::client_fill *client,
                      slipstore::io *handle,
                      unsigned long stream,
                      unsigned long partition,
                      unsigned long tile,
                      unsigned long align) {
        unsigned char *bufhead = buffer;
        slipstore::slipstore_req_t req;
        bufsize = 0;
        while (bufbytes >= (bufsize + align)) {
          unsigned long bytes = (bufbytes - bufsize);
          if (bytes > client->get_slipchunk()) {
            bytes = client->get_slipchunk();
          }
          bytes = (bytes / align) * align;
          req.cmd = slipstore::CMD_FILL;
          req.stream = stream;
          req.partition = partition;
          req.tile = tile;
          req.size = bytes;
          if (handle->eof(&req)) {
            break;
          }
          unsigned long fill_size = handle->fill(bufhead, &req);
          bufhead += fill_size;
          bufsize += fill_size;
        }
        __sync_synchronize();
        uptodate = true;
      }


      // Slipstore functionality
      void fill_tile(slipstore::client_fill *client,
                     unsigned long partition,
                     unsigned long buf_offset,
                     unsigned long fill_bytes) {
        // Are we striping vertices?
        bool striping = vm.count("use_vertex_striping") > 0;

        // Pointer to head of the buffer we're filling
        unsigned char *bufhead = buffer + buf_offset;

        // Size of each chunk
        unsigned long slipchunk = client->get_slipchunk();

        // Request object to fill vertex state for my tile
        slipstore::slipstore_req_t req;
        req.cmd = slipstore::CMD_FILL;
        req.stream = slipstore::STREAM_VERTEX_STATE;
        req.partition = partition;
        req.tile = 0;

        bufsize = 0;
        // Fill loop
        for (unsigned long i = (partition %
                                client->get_machines()); // i = machine id for the next stripe (used only if vertex striping enabled)
             bufsize < fill_bytes; // Until buffer is filled
             i = (i + 1) % client->get_machines() // i=me,me+1,... % m
            ) {
          unsigned long bytes = (fill_bytes - bufsize);
          req.size = bytes > slipchunk ? slipchunk : bytes;

          // Access server i's store if striping or else local store
          if (!client->access_store(&req, bufhead, striping ? i : client->get_me())) {
            BOOST_LOG_TRIVIAL(fatal) << "Unable to fill tile";
            exit(-1);
          }

          bufhead += req.size; // Move the buffer head by the size we filled
          bufsize += req.size; // Increase the filled amount by the size we just filled
        }

        __sync_synchronize();
        uptodate = true;
      }

      // Slipstore functionality
      void drain(slipstore::client_drain *client,
                 unsigned long stream,
                 unsigned long align,
                 unsigned long total_processors) {
        if (do_batch_io && gather_io_drain) {
          subtrack->buffer = buffer;
          subtrack->index_entries = index_entries;
          subtrack->skip_index = skip_superp;
          subtrack->cpus = total_processors;
          subtrack->index = index;
          subtrack->tiles = configuration::tiles;
          for (unsigned long i = 0; i < total_processors; i++) {
            subtrack->per_cpu_end[i] =
                index[i][0] + per_cpu_sizes[i];
          }
          subtrack->init();
          unsigned long csize = client->get_slipchunk();
          csize = (csize / align) * align;
          if (!client->batch_access_store_gather_io(subtrack, stream, csize)) {
            BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
            exit(-1);
          }
        }
        else {
          for (unsigned long i = 0; i < index_entries; i++) {
            if (i == skip_superp) {
              continue;
            }
            for (unsigned long j = 0; j < total_processors; j++) {
              unsigned long drain_bytes;
              unsigned char *bufhead = get_substream(j, i, &drain_bytes);
              slipstore::slipstore_req_t req;
              if (do_batch_io) {
                req.cmd = slipstore::CMD_DRAIN;
                req.stream = stream;
                req.partition = i / configuration::tiles;
                req.tile = i % configuration::tiles;
                unsigned long csize = client->get_slipchunk();
                csize = (csize / align) * align;
                if (client->batch_access_store(&req,
                                               bufhead,
                                               csize,
                                               drain_bytes)) {
                  BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
                  exit(-1);
                }
              }
              else {
                while (drain_bytes != 0) {
                  unsigned long bytes = drain_bytes;
                  if (bytes > client->get_slipchunk()) {
                    bytes = client->get_slipchunk();
                  }
                  bytes = (bytes / align) * align;
                  req.cmd = slipstore::CMD_DRAIN;
                  req.stream = stream;
                  req.partition = i / configuration::tiles;
                  req.tile = i % configuration::tiles;
                  req.size = bytes;
                  if (!client->access_store(&req, bufhead)) {
                    BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
                    exit(-1);
                  }
                  drain_bytes -= req.size;
                  bufhead += req.size;
                }
              }
            }
          }
        }
        cleanup();
        release_buffer(this);
      }

      // Slipstore functionality
      void drain_local(slipstore::client_drain *client,
                       slipstore::io *handle,
                       unsigned long stream,
                       unsigned long align,
                       unsigned long total_processors) {
        for (unsigned long i = 0; i < index_entries; i++) {
          if (i == skip_superp) {
            continue;
          }
          for (unsigned long j = 0; j < total_processors; j++) {
            unsigned long drain_bytes;
            unsigned char *bufhead = get_substream(j, i, &drain_bytes);
            slipstore::slipstore_req_t req;
            while (drain_bytes != 0) {
              unsigned long bytes = drain_bytes;
              if (bytes > client->get_slipchunk()) {
                bytes = client->get_slipchunk();
              }
              bytes = (bytes / align) * align;
              req.cmd = slipstore::CMD_DRAIN;
              req.stream = stream;
              req.partition = i / configuration::tiles;
              req.tile = i % configuration::tiles;
              req.size = bytes;
              if (!handle->have_drain_space(&req)) {
                BOOST_LOG_TRIVIAL(fatal) << "Unable to write to slipstore";
                exit(-1);
              }
              handle->drain(bufhead, &req);
              drain_bytes -= req.size;
              bufhead += req.size;
            }
          }
        }
        cleanup();
        release_buffer(this);
      }

      // Slipstore functionality
      void drain_tile(slipstore::client_drain *client,
                      unsigned long partition,
                      unsigned long buf_offset) {
        // Are we striping vertices?
        bool striping = vm.count("use_vertex_striping") > 0;

        // Pointer to head of the buffer we're filling
        unsigned char *bufhead = buffer + buf_offset;

        // Size of each chunk
        unsigned long slipchunk = client->get_slipchunk();

        // Request object to drain vertex state for my tile
        slipstore::slipstore_req_t req;
        req.cmd = slipstore::CMD_DRAIN;
        req.stream = slipstore::STREAM_VERTEX_STATE;
        req.partition = partition;
        req.tile = 0;

        // Drain loop
        for (unsigned long i = (partition %
                                client->get_machines()); // i = machine id for the next stripe (used only if vertex striping enabled)
             bufsize > 0; // Until buffer is drained
             i = (i + 1) % client->get_machines() // i=me,me+1,... % m
            ) {
          req.size = bufsize > slipchunk ? slipchunk : bufsize;

          // Access server i's store if striping or else local store
          if (!client->access_store(&req, bufhead, striping ? i : client->get_me())) {
            BOOST_LOG_TRIVIAL(fatal) << "Unable to store tile";
            exit(-1);
          }

          bufhead += req.size; // Move the buffer head by the size we drained
          bufsize -= req.size; // Decrease the amount we still have to drain
        }

        __sync_synchronize();
        uptodate = true;
      }
  };

  template<typename T, typename M>
  static void make_index(memory_buffer *stream,
                         unsigned long processor_id,
                         unsigned long partitions,
                         x_barrier *thread_sync) {
    if (stream->indexed) {
      thread_sync->wait();
      return;
    }
    unsigned long items = stream->bufsize / T::item_size();
    BOOST_ASSERT_MSG(stream->bufsize % T::item_size() == 0,
                     "Buffer does not contain an integral number of items !");
    unsigned long items_per_cpu = items / stream->config->processors;
    unsigned long remainder = items - items_per_cpu * stream->config->processors;
    stream->reset_index(processor_id);
    stream->per_cpu_sizes[processor_id] = items_per_cpu * T::item_size();
    if (processor_id == (stream->config->processors - 1)) {
      stream->per_cpu_sizes[processor_id] += remainder * T::item_size();
    }
    stream->index[processor_id][0] =
        items_per_cpu * processor_id * T::item_size();
    thread_sync->wait();
    bool twizzle;
    if (!stream->use_qsort) {
      twizzle = x_split<T, M>(stream->index[processor_id],
                              stream->buffer,
                              stream->aux_index[processor_id],
                              stream->aux_buffer,
                              stream->per_cpu_sizes[processor_id],
                              stream->config->fanout,
                              partitions,
                              stream->work_queues);
    }
    else {
      if (processor_id == 0) {
        qsort_keys = partitions;
      }
      thread_sync->wait();
      x_split_qsort<T, M>(stream->index[processor_id],
                          stream->buffer,
                          stream->per_cpu_sizes[processor_id],
                          stream->work_queues);
      twizzle = false;
    }
    thread_sync->wait();
    if (processor_id == 0) {
      if (twizzle) {
        stream->switch_with_aux();
      }
      stream->index_entries = partitions;
      stream->indexed = true;
    }
    // Stream in a steady state here
    thread_sync->wait();
  }
}
#endif
