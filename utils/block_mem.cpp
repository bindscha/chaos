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

#include<stdio.h>
#include "memory_utils.h"
#include "unistd.h"

#define TOLOCK (1024*1024UL)

int main() {
  fprintf(stderr, "Locking %lu bytes", TOLOCK);
  void *dummy = map_anon_memory(TOLOCK, true, "Dummy");
  (void) dummy;
  while (1) {
    pause();
  }
  fprintf(stderr, "Exiting .. this should not happen !");
  return 0;
}
