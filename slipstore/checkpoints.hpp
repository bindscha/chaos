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

#ifndef _CHECKPOINTS_
#define _CHECKPOINTS_

// Manage checkpoint metadata
// File format:
// <opaque app checkpoint data>
// <opaque runtime checkpoint data>
// MAGIC NUMBER
// Checkpoint number
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include "../utils/memory_utils.h"

/* Written out at the end of a stable checkpoint */
static const unsigned long CHK_MAGIC=0xCAFECAFECAFECAFE;
struct __attribute__((__packed__)) chkpnt_tail {
  unsigned long magic;
  unsigned long num;
};

static unsigned long chkpnt_no;

/* Set the next checkpoint number */
static void set_checkpoint_no(unsigned long chkpnt_no_in)
{
  chkpnt_no = chkpnt_no_in;
}

/* Load checkpoint */
static void read_checkpoint(unsigned char* app_data,
			    unsigned long app_data_size,
			    unsigned char* runtime_data,
			    unsigned long runtime_data_size,
			    unsigned long no)
{
  char fname[100];
  sprintf(fname, "chkpnt_meta.%lu", no);
  int fd = open(fname, O_RDONLY);
  if(fd == -1) {
    BOOST_LOG_TRIVIAL(fatal) 
      << "Unable to open checkpoint metadata file for reading: "
      << strerror(errno);
    exit(-1);
  }
  if(app_data_size > 0) {
    read_from_file(fd, app_data, app_data_size);
  }
  if(runtime_data_size > 0) {
    read_from_file(fd, runtime_data, runtime_data_size);
  }
  close(fd);
}

static unsigned long next_checkpoint()
{
  return (chkpnt_no++);
}

/* Write out a new checkpoint */
static void write_checkpoint(unsigned long no,
			     unsigned char *app_data,
			     unsigned long app_data_size,
			     unsigned char *runtime_data,
			     unsigned long runtime_data_size)
{
  char fname[100], fname_final[100];
  chkpnt_tail tail;
  sprintf(fname, "chkpnt_meta_prep.%lu", no);
  sprintf(fname_final, "chkpnt_meta.%lu", no);
  // Open the file in sync mode for append guarantees
  int fd = open(fname, O_CREAT|O_TRUNC|O_WRONLY|O_APPEND|O_SYNC, S_IRWXU);
  if(fd == -1) {
    BOOST_LOG_TRIVIAL(fatal) 
      << "Unable to open checkpoint metadata file for writing: "
      << strerror(errno);
    exit(-1);
  }
  if(app_data_size > 0) {
    write_to_file(fd, app_data, app_data_size);
  }
  if(runtime_data_size > 0) {
    write_to_file(fd, runtime_data, runtime_data_size);
  }
  tail.magic = CHK_MAGIC;
  tail.num   = no;
  write_to_file(fd, (unsigned char *)&tail, sizeof(chkpnt_tail));
  fsync(fd);// Ensure file is created and on disk
  int ret = rename(fname, fname_final);
  if(ret < 0) {
    BOOST_LOG_TRIVIAL(fatal) 
      << "Unable to save checkpoint "
      << strerror(errno);
    exit(-1);
  }
  fsync(fd);// Ensure all metadata changes are on disk
  close(fd);
}

/* Delete a checkpoint */
static void del_checkpoint(unsigned long no)
{
  char fname[100];
  sprintf(fname, "chkpnt_meta.%lu", no);
  unlink_file(fname);
}

/* Scan current working directory to locate the last stable checkpoint */
static unsigned long find_checkpoint(unsigned long app_data_size,
				     unsigned long runtime_data_size)
{
  unsigned long cmx = ULONG_MAX;
  DIR *dp;
  struct dirent *ep;
  chkpnt_tail tail;
  dp = opendir ("./");
  if(dp != NULL) {
    while ((ep = readdir (dp)) != NULL) {
      if(strncmp(ep->d_name, "chkpnt_meta.",12) == 0) {
	unsigned long cno = atol(ep->d_name + 12);
	if(cno > cmx || cmx == ULONG_MAX) {
	  cmx = cno;
	}
      }
    }
    (void) closedir (dp);
  }
  else {
    BOOST_LOG_TRIVIAL(fatal) << "Couldnt open current directory:"
			     << strerror(errno);
    exit(-1);
  }
  if(cmx != ULONG_MAX) {
    /* Verify that the checkpoint is stable */
    do {
      char fname[100];
      sprintf(fname, "chkpnt_meta.%lu", cmx);
      int fd = open(fname, O_RDONLY);
      if(fd != -1) {
	// Make sure the checkpoint is complete
	if(get_file_size(fd) == 
	   (app_data_size + runtime_data_size + sizeof(chkpnt_tail))) {
	  // Verify end of file
	  set_filepos(fd, app_data_size + runtime_data_size);
	  read_from_file(fd, (unsigned char *)&tail, sizeof(chkpnt_tail));
	  if(tail.magic != CHK_MAGIC || tail.num != cmx) {
	    BOOST_LOG_TRIVIAL(warn) 
	      << "SLIPSTREAM::CHECKPOINT Skipping corrupt checkpoint "
	      << cmx;
	  }
	  else {
	    close(fd);
	    break;
	  }
	} 
	else {
	  BOOST_LOG_TRIVIAL(warn) 
	    << "SLIPSTREAM::CHECKPOINT Skipping incomplete checkpoint "
	    << cmx;
	}
	close(fd);
      }
    } while((cmx--) != 0);
  }
  if(cmx != ULONG_MAX) {
    BOOST_LOG_TRIVIAL(info) 
      << "SLIPSTREAM::CHECKPOINT Found max checkpoint " 
      << cmx;
  }
  return cmx;
}

#endif
