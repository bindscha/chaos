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

#ifndef _DIO_BUFFER_
#define _DIO_BUFFER_
#include "../utils/memory_utils.h"
// Direct I/O buffer
class dio_buffer {
  unsigned char *buffer;
  unsigned long bufsize;
  unsigned long bufcontents;
  volatile bool busy;
  bool dirty;
  int fd;

  void flush_buffer_internal()
  {
    if(dirty && bufcontents > 0) {
      write_to_file(fd, buffer, bufsize);
    }
    bufcontents = 0;
    dirty = false;
  }

  void fill_buffer_internal()
  {
    read_from_file(fd, buffer, bufsize);
    bufcontents = bufsize;
  }
public:

  void set_busy()
  {
    busy = true;
    __sync_synchronize();
  }
  
  bool get_busy()
  {
    return busy;
  }
  
  unsigned long alignment()
  {
    return 4096;
  }
  
  void seek(unsigned long offset)
  {
    unsigned char dummy[128];
    unsigned long aligned_offset = (offset/4096)*4096;
    set_filepos(fd, aligned_offset);
    bufcontents = 0;
    unsigned long to_waste = offset - aligned_offset;
    while(to_waste) {
      unsigned long get_bytes = to_waste;
      if(get_bytes > 128) {
	get_bytes = 128;
      }
      dio_buffer::read(dummy, get_bytes);
      to_waste -= get_bytes;
    }
    __sync_synchronize();
    busy = false;
  }
      
  dio_buffer(int fd_in, unsigned long bufsize_in)
    :bufsize(bufsize_in),
     bufcontents(0),
     busy(false),
     dirty(false),
     fd(fd_in)
  {
    // Adjust to 4K boundary
    bufsize = ((bufsize + 4095)/4096)*4096;
    // Allocate and mlock buffer
    buffer = (unsigned char *)map_anon_memory(bufsize, true, "dio buffer");
  }
  
  void write(unsigned char *buf, unsigned long size)
  {
    while(size) {
      unsigned long space = bufsize - bufcontents;
      if(space == 0) {
	flush_buffer_internal();
	space = bufsize;
      }
      if(space > size) {
	space = size;
      }
      memcpy(buffer + bufcontents, buf, space);
      dirty = true;
      buf         += space;
      bufcontents += space;
      size        -= space;
    }
    __sync_synchronize();
    busy = false;
  }
  
  void read(unsigned char* buf, unsigned long size)
  {
    while(size) {
      if(bufcontents == 0) {
	fill_buffer_internal();
      }
      unsigned long avail = bufcontents;
      if(avail > size) {
	avail = size;
      }
      memcpy(buf, buffer + (bufsize - bufcontents), avail);
      buf         += avail;
      bufcontents -= avail;
      size        -= avail;
    }
    __sync_synchronize();
    busy = false;
  }
  
  void flush()
  {
    flush_buffer_internal();
    __sync_synchronize();
    busy = false;
  }
  
  ~dio_buffer()
  {
    flush_buffer_internal();
  }
};

#endif
