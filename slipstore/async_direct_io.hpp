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

#ifndef _ASYNC_DIRECT_IO_
#define _ASYNC_DIRECT_IO_
#include <boost/asio/io_service.hpp>
#include "generic_io.hpp"
#include "dio_buffer.hpp"
namespace slipstore {
  extern boost::asio::io_service dioService;
  extern boost::thread_group diopool;
  class async_direct_io:public slipstore_io {
    unsigned long stream;
    unsigned long partition;
    unsigned long tile;
    unsigned long seen_bytes;
    unsigned long data_bytes;
    char name[100];
    int fd;
    bool snapped;
    dio_buffer *iobuf;
  public:
    async_direct_io(unsigned long stream_in,
		    unsigned long partition_in,
		    unsigned long tile_in,
		    unsigned long dio_unit,
		    const char *name_in = NULL)
      :stream(stream_in),
       partition(partition_in),
       tile(tile_in),
       seen_bytes(0),
       snapped(false)
    {
      if(name_in != NULL) {
	strcpy(name, name_in);
	fd = open(name, O_RDWR|O_LARGEFILE|O_DIRECT, S_IRWXU);
	data_bytes = get_file_size(fd);
      }
      else{
	sprintf(name, "stream.%lu.%lu.%lu", stream, partition, tile);
	// This prevents us from over writing any snapshots
	unlink_file(name);
	fd = open(name, 
		  O_RDWR|O_LARGEFILE|O_CREAT|O_TRUNC|O_DIRECT,
		  S_IRWXU);
	data_bytes = 0;
      }
      iobuf = new dio_buffer(fd, dio_unit);
    }
    
    virtual unsigned long fill(unsigned char *buffer, 
			       unsigned long size)
    {
      unsigned long remaining_bytes = data_bytes - seen_bytes;
      if(size > remaining_bytes) {
	size = remaining_bytes;
      }
      seen_bytes += size;
      while(iobuf->get_busy());
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::read,
					     iobuf,
					     buffer,
					     size));
      while(iobuf->get_busy());
      return size;
    }

    virtual unsigned long seek_and_fill(unsigned char *buffer, 
					unsigned long offset,
					unsigned long size)
    {
      unsigned long remaining_bytes;
      while(iobuf->get_busy());
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::seek,
					     iobuf,
					     offset));
      while(iobuf->get_busy());
      seen_bytes = offset;
      remaining_bytes = data_bytes - seen_bytes;
      if(size > remaining_bytes) {
	size = remaining_bytes;
      }
      seen_bytes += size;
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::read,
					     iobuf,
					     buffer,
					     size));
      while(iobuf->get_busy());
      return size;
    }
    
    virtual void drain(unsigned char *buffer,
		       unsigned long size)
    {
      seen_bytes += size;
      while(iobuf->get_busy());
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::write,
					     iobuf,
					     buffer,
					     size));
      while(iobuf->get_busy());
      data_bytes += size;
      usage += size;
      if(usage > max_quota_usage) {
	max_quota_usage = usage;
      }
    }
    
    virtual bool eof()
    {
      return seen_bytes == data_bytes;
    }
    
    virtual unsigned long size()
    {
      return data_bytes;
    }

    virtual unsigned long left()
    {
      return data_bytes - seen_bytes;
    }
    
    virtual void rewind()
    {
      while(iobuf->get_busy());
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::flush,
					     iobuf));
      while(iobuf->get_busy());
      rewind_file(fd);
      seen_bytes = 0;
    }

    virtual void trunc()
    {
      while(iobuf->get_busy());
      iobuf->set_busy();
      slipstore::dioService.post(boost::bind(&dio_buffer::flush,
					     iobuf));
      while(iobuf->get_busy());
      if(!snapped){
	rewind_file(fd);
	truncate_file(fd);
      }
      else {
	close(fd);
	unlink_file(name);
	fd = open(name, 
		  O_RDWR|O_LARGEFILE|O_CREAT|O_TRUNC|O_DIRECT,
		  S_IRWXU);
	snapped = false;
      }
      usage     -= data_bytes;
      seen_bytes = 0;
      data_bytes = 0;
    }

    virtual void take_snap(unsigned long no, fsnap *meta)
    {
      char new_name[200];
      while(iobuf->get_busy());
      iobuf->flush();
      sprintf(new_name, "snap_%lu_",no);
      strcat(new_name, name);
      int ret = link(name, new_name);
      if(ret < 0) {
	BOOST_LOG_TRIVIAL(fatal) << "Unable to create snap link"
				 << strerror(errno);
	exit(-1);
      }
      snapped   = true;
      meta->pos  = seen_bytes;
      meta->size = data_bytes;
    }

    virtual void restore_snap(unsigned long no,
			      fsnap *meta)
    {
      char snap_name[200];
      close(fd);
      unlink_file(name);
      sprintf(snap_name, "snap_%lu_", no);
      strcat(snap_name, name);
      int ret = link(snap_name, name);
      if(ret < 0) {
	BOOST_LOG_TRIVIAL(fatal) << "Unable to create snap link"
				 << strerror(errno);
	exit(-1);
      }
      fd = open(name, O_RDWR|O_LARGEFILE|O_DIRECT);
      truncate_file_to_size(fd, meta->size);
      data_bytes = meta->size;
      set_filepos(fd, meta->pos);
      seen_bytes = meta->pos;
      snapped = true;
    }

    virtual void delete_snap(unsigned long no)
    {
      char snap_name[200];
      sprintf(snap_name, "snap_%lu_", no);
      strcat(snap_name, name);
      unlink_file(snap_name);
    }

    virtual ~async_direct_io()
    {
      delete iobuf;
      close(fd);
    }
  };
}
#endif
