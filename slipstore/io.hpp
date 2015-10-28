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

#ifndef _STORE_IO_
#define _STORE_IO_
// Handle Slipstream Disk I/O
#include <unistd.h>
#include <sys/types.h>
#include "checkpoints.hpp"
#include "boost/multi_array.hpp"
#include "../utils/memory_utils.h"
#include "../utils/clock_utils.h"
#include "sync_pagecache_io.hpp"
#include "async_direct_io.hpp"
#include "dummy_io.hpp"
namespace slipstore {
  // Requests to slipstore service
  static const unsigned long CMD_FILL            = 0;
  static const unsigned long CMD_SEEK_AND_FILL   = 1;
  static const unsigned long CMD_DRAIN           = 2;
  static const unsigned long CMD_EOF             = 3;
  static const unsigned long CMD_NOP             = 4;
  static const unsigned long CMD_MEMREAD         = 5;
  static const unsigned long CMD_OFFER_HELP      = 6;
  static const unsigned long CMD_TERMINATE       = 7;
  static const unsigned long CMD_SEMUP           = 8;
  static const unsigned long CMD_RESET           = 9;
  static const unsigned long CMD_ORDER_INIT      = 10;
  static const unsigned long CMD_ORDER_NEXT      = 11;

  // Responses from slipstore service
  static const unsigned long RESP_ACK            = 0;
  static const unsigned long RESP_NACK           = 1;

  struct slipstore_req_t {
    unsigned long source_mc;
    unsigned long cmd;
    unsigned long stream;
    unsigned long partition;
    unsigned long tile;
    unsigned long size;
    unsigned long offset; // Used only by seek and fill
  };

  const unsigned long STREAM_VERTEX_STATE = 0;
  const unsigned long STREAM_INPUT        = 1;
  const unsigned long STREAM_MEMBUFFER    = ULONG_MAX - 2; // arbitrary

#define FOR_EACH_TILE				\
  for(index i=0; i != streams; ++i) {		\
  for(index j=0; j != partitions; ++j) {	\
  for(index k=0; k != tiles; ++k) {		\

#define END_FOR_EACH_TILE }}}
  
  class io {
    // Create a 3D array[superp, partition, tile] for the buffers
    typedef boost::multi_array<slipstore_io*, 3> array_type;
    typedef array_type::index index;
    index streams;
    index partitions;
    index tiles;
    array_type buffers;
    fsnap *fsnaps;
    unsigned char *membuffer;
    rtc_clock slipstore_io_time;
    rtc_clock slipstore_snap_time;

    unsigned long fsnap_index(unsigned long stream,
			      unsigned long partition,
			      unsigned long tile)
    {
      return stream*partitions*tiles + partition*tiles + tile;
    }

  public:
    io(unsigned long streams_in, 
       unsigned long partitions_in,
       unsigned long tiles_in,
       unsigned char *membuffer_in)
      :streams(streams_in),
       partitions(partitions_in),
       tiles(tiles_in),
       buffers(boost::extents[streams][partitions][tiles]),
       fsnaps(NULL),
       membuffer(membuffer_in)
    {
      unsigned long me = pt_slipstore.get<unsigned long>("machines.me");
      FOR_EACH_TILE
	// Exactly one input stream
	if(i == (index)STREAM_INPUT && j == 0 && k == (index)(me % tiles)) {
	  std::string efile = pt.get<std::string>("graph.name");
	  if(vm.count("use_direct_io") > 0) {
	    buffers[i][j][k] = 
	      new async_direct_io(STREAM_INPUT,
				  0,
				  0,
				  vm["dio_unit"].as<unsigned long>(),
				  efile.c_str());
	  }
	  else if(vm.count("use_dummy_io") > 0) {
	    buffers[i][j][k] = 
	      new dummy_io(STREAM_INPUT,
			   0,
			   0,
			   efile.c_str());
	  }
	  else {
	    buffers[i][j][k] = new sync_pagecache_io(STREAM_INPUT,
						     0,
						     0,
						     efile.c_str());
	  }
	}
	else {
	  if(vm.count("use_direct_io") > 0) {
	    buffers[i][j][k] = new async_direct_io
	      (i, j, k, vm["dio_unit"].as<unsigned long>());
	  }
	  else if(vm.count("use_dummy_io") > 0) {
	    buffers[i][j][k] = new dummy_io(i, j, k);
	  }
	  else {
	    buffers[i][j][k] = new sync_pagecache_io(i, j, k);
	  }
	}
      END_FOR_EACH_TILE
      quota = vm["quota"].as<unsigned long>();
      usage = 0;
      max_quota_usage = 0;
    }
    
    bool eof(slipstore_req_t *req)
    {
      slipstore_io * buffer = buffers[req->stream][req->partition][req->tile];
      return buffer->eof();
    }

    bool eof_stream(slipstore_req_t *req)
    {
      bool empty = true;
      for(index i=0;i != partitions;i++) {
	for(index j=0;j != tiles;j++) {
	  empty = empty && buffers[req->stream][i][j]->eof();
	}
      }
      return empty;
    }
    
    unsigned long fill(unsigned char *buffer, slipstore_req_t *req)
    {
      if(req->stream == STREAM_MEMBUFFER) {
	// req->partition acts as offset
	memcpy(buffer, membuffer + req->partition, req->size);
	return req->size;
      }
      else {
	unsigned long size;
	slipstore_io_time.start();
	size = buffers[req->stream][req->partition][req->tile]->fill
	  (buffer, req->size);
	slipstore_io_time.stop();
	return size;
      }
    }

    unsigned long seek_and_fill(unsigned char *buffer, slipstore_req_t *req)
    {
      unsigned long size;
      slipstore_io_time.start();
      size = buffers[req->stream][req->partition][req->tile]->seek_and_fill
	(buffer, req->offset, req->size);
      slipstore_io_time.stop();
      return size;
    }

    // This is how Amitabha writes code
    bool have_drain_space(slipstore_req_t *req)
    {
      if((usage + req->size) <=  quota) {
	return true;
      }
      else {
	return false;
      }
    }

    // This is how Laurent writes code
    bool have_drain_space(unsigned long additional_size) {
      return usage + additional_size <= quota;
    }

    void drain(unsigned char *buffer, slipstore_req_t *req)
    {
      slipstore_io_time.start();
      buffers[req->stream][req->partition][req->tile]->drain(buffer, req->size);
      slipstore_io_time.stop();
    }

    void rewind(unsigned long stream, 
		unsigned long partition, 
		unsigned long tile)

    {
      slipstore_io_time.start();
      buffers[stream][partition][tile]->rewind();
      slipstore_io_time.stop();
    }

    unsigned long get_size(unsigned long stream,
			   unsigned long partition,
			   unsigned long tile)
    {
      return buffers[stream][partition][tile]->size();
    }

    unsigned long get_bytes_left(unsigned long stream,
			   unsigned long partition,
			   unsigned long tile)
    {
      return buffers[stream][partition][tile]->left();
    }
    
    void trunc(unsigned long stream, 
	       unsigned long partition,
	       unsigned long tile)
    {
      slipstore_io_time.start();
      buffers[stream][partition][tile]->trunc();
      slipstore_io_time.stop();
    }

    unsigned long snap_metadata_size()
    {
      return streams*partitions*tiles*sizeof(fsnap);
    }
    
    void restore_snapshot(unsigned long snapshot_no,
			  unsigned char *metadata)
    {
      slipstore_snap_time.start();
      if(fsnaps == NULL) {
	fsnaps = new fsnap[streams*partitions*tiles];
      }
      memcpy(fsnaps, metadata, snap_metadata_size());
      FOR_EACH_TILE
	if(i == (index)STREAM_INPUT) {
	  continue;
	}
        buffers[i][j][k]->restore_snap
	  (snapshot_no, &fsnaps[fsnap_index(i, j, k)]);
      END_FOR_EACH_TILE
      slipstore_snap_time.stop();
    }

    void delete_snapshot(unsigned long snapshot_no)
    {
      slipstore_snap_time.start();
      FOR_EACH_TILE
	if(i == (index)STREAM_INPUT) {
	  continue;
	}
        buffers[i][j][k]->delete_snap(snapshot_no);
      END_FOR_EACH_TILE
      slipstore_snap_time.stop();
    }

    unsigned char* take_snapshot(unsigned long snapshot_no)
    {
      slipstore_snap_time.start();
      if(fsnaps == NULL) {
	fsnaps = new fsnap[streams*partitions*tiles];
      }
      FOR_EACH_TILE
	if(i == (index)STREAM_INPUT) {
	  continue;
	}
      buffers[i][j][k]->take_snap(snapshot_no,
				  &fsnaps[fsnap_index(i, j, k)]);
      END_FOR_EACH_TILE
      sync(); // Ensure hard links hit disk
      slipstore_snap_time.stop();
      return (unsigned char *)fsnaps;
    }

    // For running fill speed test
    void fill_test_prep(unsigned long stream)
    {
      delete buffers[stream][0][0];
      if(vm.count("use_direct_io") > 0) {
	buffers[stream][0][0] = 
	  new async_direct_io(stream,
			      0,
			      0,
			      vm["slipchunk"].as<unsigned long>(),
			      "fill_test_data");
      }
      else if(vm.count("use_dummy_io") > 0) {
	dummy_io * dummy = 
	  new dummy_io(stream,
		       0,
		       0,
		       "fill_test_data");
	dummy->force_file_size(vm["slipbench_fsize"].as<unsigned long>());
	buffers[stream][0][0] = dummy;
      }
      else {
	buffers[stream][0][0] = new sync_pagecache_io(stream,
						      0,
						      0,
						      "fill_test_data");
      }
    }

    ~io()
    {
      slipstore_io_time.print("SLIPSTORE::IO::TIME");
      slipstore_snap_time.print("SLIPSTORE::SNAP::TIME");
      BOOST_LOG_TRIVIAL(info) << "SLIPSTORE::MAX_DISK_USAGE_BYTES "
			      << max_quota_usage;
      FOR_EACH_TILE
	delete buffers[i][j][k];
      END_FOR_EACH_TILE
    }
  };
}
#endif
