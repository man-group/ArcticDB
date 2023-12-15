// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "nanoarrow.h"

#define NANOARROW_VERSION_MAJOR 3
#define NANOARROW_VERSION_MINOR 11
#define NANOARROW_VERSION_PATCH 0

#define NANOARROW_VERSION "3.11"

#define NANOARROW_VERSION_INT                                        \
  (NANOARROW_VERSION_MAJOR * 10000 + NANOARROW_VERSION_MINOR * 100 + \
   NANOARROW_VERSION_PATCH)

const char* ArrowNanoarrowVersion(void) { return NANOARROW_VERSION; }

int ArrowNanoarrowVersionInt(void) { return NANOARROW_VERSION_INT; }

int ArrowErrorSet(struct ArrowError* error, const char* fmt, ...) {
  if (error == NULL) {
    return NANOARROW_OK;
  }

  memset(error->message, 0, sizeof(error->message));

  va_list args;
  va_start(args, fmt);
  int chars_needed = vsnprintf(error->message, sizeof(error->message), fmt, args);
  va_end(args);

  if (chars_needed < 0) {
    return EINVAL;
  } else if (((size_t)chars_needed) >= sizeof(error->message)) {
    return ERANGE;
  } else {
    return NANOARROW_OK;
  }
}

const char* ArrowErrorMessage(struct ArrowError* error) {
  if (error == NULL) {
    return "";
  } else {
    return error->message;
  }
}

void ArrowLayoutInit(struct ArrowLayout* layout, enum ArrowType storage_type) {
  layout->buffer_type[0] = NANOARROW_BUFFER_TYPE_VALIDITY;
  layout->buffer_data_type[0] = NANOARROW_TYPE_BOOL;
  layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA;
  layout->buffer_data_type[1] = storage_type;
  layout->buffer_type[2] = NANOARROW_BUFFER_TYPE_NONE;
  layout->buffer_data_type[2] = NANOARROW_TYPE_UNINITIALIZED;

  layout->element_size_bits[0] = 1;
  layout->element_size_bits[1] = 0;
  layout->element_size_bits[2] = 0;

  layout->child_size_elements = 0;

  switch (storage_type) {
    case NANOARROW_TYPE_UNINITIALIZED:
    case NANOARROW_TYPE_NA:
      layout->buffer_type[0] = NANOARROW_BUFFER_TYPE_NONE;
      layout->buffer_data_type[0] = NANOARROW_TYPE_UNINITIALIZED;
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_NONE;
      layout->buffer_data_type[1] = NANOARROW_TYPE_UNINITIALIZED;
      layout->element_size_bits[0] = 0;
      break;

    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_MAP:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT32;
      layout->element_size_bits[1] = 32;
      break;

    case NANOARROW_TYPE_LARGE_LIST:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT64;
      layout->element_size_bits[1] = 64;
      break;

    case NANOARROW_TYPE_STRUCT:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_NONE;
      layout->buffer_data_type[1] = NANOARROW_TYPE_UNINITIALIZED;
      break;

    case NANOARROW_TYPE_BOOL:
      layout->element_size_bits[1] = 1;
      break;

    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT8:
      layout->element_size_bits[1] = 8;
      break;

    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_HALF_FLOAT:
      layout->element_size_bits[1] = 16;
      break;

    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_FLOAT:
      layout->element_size_bits[1] = 32;
      break;
    case NANOARROW_TYPE_INTERVAL_MONTHS:
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT32;
      layout->element_size_bits[1] = 32;
      break;

    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_DOUBLE:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
      layout->element_size_bits[1] = 64;
      break;

    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      layout->element_size_bits[1] = 128;
      break;

    case NANOARROW_TYPE_DECIMAL256:
      layout->element_size_bits[1] = 256;
      break;

    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      layout->buffer_data_type[1] = NANOARROW_TYPE_BINARY;
      break;

    case NANOARROW_TYPE_DENSE_UNION:
      layout->buffer_type[0] = NANOARROW_BUFFER_TYPE_TYPE_ID;
      layout->buffer_data_type[0] = NANOARROW_TYPE_INT8;
      layout->element_size_bits[0] = 8;
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_UNION_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT32;
      layout->element_size_bits[1] = 32;
      break;

    case NANOARROW_TYPE_SPARSE_UNION:
      layout->buffer_type[0] = NANOARROW_BUFFER_TYPE_TYPE_ID;
      layout->buffer_data_type[0] = NANOARROW_TYPE_INT8;
      layout->element_size_bits[0] = 8;
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_NONE;
      layout->buffer_data_type[1] = NANOARROW_TYPE_UNINITIALIZED;
      break;

    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_BINARY:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT32;
      layout->element_size_bits[1] = 32;
      layout->buffer_type[2] = NANOARROW_BUFFER_TYPE_DATA;
      layout->buffer_data_type[2] = storage_type;
      break;

    case NANOARROW_TYPE_LARGE_STRING:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT64;
      layout->element_size_bits[1] = 64;
      layout->buffer_type[2] = NANOARROW_BUFFER_TYPE_DATA;
      layout->buffer_data_type[2] = NANOARROW_TYPE_STRING;
      break;
    case NANOARROW_TYPE_LARGE_BINARY:
      layout->buffer_type[1] = NANOARROW_BUFFER_TYPE_DATA_OFFSET;
      layout->buffer_data_type[1] = NANOARROW_TYPE_INT64;
      layout->element_size_bits[1] = 64;
      layout->buffer_type[2] = NANOARROW_BUFFER_TYPE_DATA;
      layout->buffer_data_type[2] = NANOARROW_TYPE_BINARY;
      break;

    default:
      break;
  }
}

void* ArrowMalloc(int64_t size) { return malloc(size); }

void* ArrowRealloc(void* ptr, int64_t size) { return realloc(ptr, size); }

void ArrowFree(void* ptr) { free(ptr); }

static uint8_t* ArrowBufferAllocatorMallocReallocate(
    struct ArrowBufferAllocator* allocator, uint8_t* ptr, int64_t old_size,
    int64_t new_size) {
  return (uint8_t*)ArrowRealloc(ptr, new_size);
}

static void ArrowBufferAllocatorMallocFree(struct ArrowBufferAllocator* allocator,
                                           uint8_t* ptr, int64_t size) {
  ArrowFree(ptr);
}

static struct ArrowBufferAllocator ArrowBufferAllocatorMalloc = {
    &ArrowBufferAllocatorMallocReallocate, &ArrowBufferAllocatorMallocFree, NULL};

struct ArrowBufferAllocator ArrowBufferAllocatorDefault(void) {
  return ArrowBufferAllocatorMalloc;
}

static uint8_t* ArrowBufferAllocatorNeverReallocate(
    struct ArrowBufferAllocator* allocator, uint8_t* ptr, int64_t old_size,
    int64_t new_size) {
  return NULL;
}

struct ArrowBufferAllocator ArrowBufferDeallocator(
    void (*custom_free)(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                        int64_t size),
    void* private_data) {
  struct ArrowBufferAllocator allocator;
  allocator.reallocate = &ArrowBufferAllocatorNeverReallocate;
  allocator.free = custom_free;
  allocator.private_data = private_data;
  return allocator;
}
