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

#include "nanoarrow.h"

struct BasicArrayStreamPrivate {
  struct ArrowSchema schema;
  int64_t n_arrays;
  struct ArrowArray* arrays;
  int64_t arrays_i;
};

static int ArrowBasicArrayStreamGetSchema(struct ArrowArrayStream* array_stream,
                                          struct ArrowSchema* schema) {
  if (array_stream == NULL || array_stream->release == NULL) {
    return EINVAL;
  }

  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)array_stream->private_data;
  return ArrowSchemaDeepCopy(&private_data->schema, schema);
}

static int ArrowBasicArrayStreamGetNext(struct ArrowArrayStream* array_stream,
                                        struct ArrowArray* array) {
  if (array_stream == NULL || array_stream->release == NULL) {
    return EINVAL;
  }

  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)array_stream->private_data;

  if (private_data->arrays_i == private_data->n_arrays) {
    array->release = NULL;
    return NANOARROW_OK;
  }

  ArrowArrayMove(&private_data->arrays[private_data->arrays_i++], array);
  return NANOARROW_OK;
}

static const char* ArrowBasicArrayStreamGetLastError(
    struct ArrowArrayStream* array_stream) {
  return NULL;
}

static void ArrowBasicArrayStreamRelease(struct ArrowArrayStream* array_stream) {
  if (array_stream == NULL || array_stream->release == NULL) {
    return;
  }

  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)array_stream->private_data;

  if (private_data->schema.release != NULL) {
    private_data->schema.release(&private_data->schema);
  }

  for (int64_t i = 0; i < private_data->n_arrays; i++) {
    if (private_data->arrays[i].release != NULL) {
      private_data->arrays[i].release(&private_data->arrays[i]);
    }
  }

  if (private_data->arrays != NULL) {
    ArrowFree(private_data->arrays);
  }

  ArrowFree(private_data);
  array_stream->release = NULL;
}

ArrowErrorCode ArrowBasicArrayStreamInit(struct ArrowArrayStream* array_stream,
                                         struct ArrowSchema* schema, int64_t n_arrays) {
  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)ArrowMalloc(
          sizeof(struct BasicArrayStreamPrivate));
  if (private_data == NULL) {
    return ENOMEM;
  }

  ArrowSchemaMove(schema, &private_data->schema);

  private_data->n_arrays = n_arrays;
  private_data->arrays = NULL;
  private_data->arrays_i = 0;

  if (n_arrays > 0) {
    private_data->arrays =
        (struct ArrowArray*)ArrowMalloc(n_arrays * sizeof(struct ArrowArray));
    if (private_data->arrays == NULL) {
      ArrowBasicArrayStreamRelease(array_stream);
      return ENOMEM;
    }
  }

  for (int64_t i = 0; i < private_data->n_arrays; i++) {
    private_data->arrays[i].release = NULL;
  }

  array_stream->get_schema = &ArrowBasicArrayStreamGetSchema;
  array_stream->get_next = &ArrowBasicArrayStreamGetNext;
  array_stream->get_last_error = ArrowBasicArrayStreamGetLastError;
  array_stream->release = ArrowBasicArrayStreamRelease;
  array_stream->private_data = private_data;
  return NANOARROW_OK;
}

void ArrowBasicArrayStreamSetArray(struct ArrowArrayStream* array_stream, int64_t i,
                                   struct ArrowArray* array) {
  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)array_stream->private_data;
  ArrowArrayMove(array, &private_data->arrays[i]);
}

ArrowErrorCode ArrowBasicArrayStreamValidate(struct ArrowArrayStream* array_stream,
                                             struct ArrowError* error) {
  struct BasicArrayStreamPrivate* private_data =
      (struct BasicArrayStreamPrivate*)array_stream->private_data;

  struct ArrowArrayView array_view;
  NANOARROW_RETURN_NOT_OK(
      ArrowArrayViewInitFromSchema(&array_view, &private_data->schema, error));

  for (int64_t i = 0; i < private_data->n_arrays; i++) {
    if (private_data->arrays[i].release != NULL) {
      int result = ArrowArrayViewSetArray(&array_view, &private_data->arrays[i], error);
      if (result != NANOARROW_OK) {
        ArrowArrayViewReset(&array_view);
        return result;
      }
    }
  }

  ArrowArrayViewReset(&array_view);
  return NANOARROW_OK;
}
