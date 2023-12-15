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

#ifndef NANOARROW_BUFFER_INLINE_H_INCLUDED
#define NANOARROW_BUFFER_INLINE_H_INCLUDED

#include <errno.h>
#include <stdint.h>
#include <string.h>

#include "nanoarrow_types.h"

#ifdef __cplusplus
extern "C" {
#endif

static inline int64_t _ArrowGrowByFactor(int64_t current_capacity, int64_t new_capacity) {
  int64_t doubled_capacity = current_capacity * 2;
  if (doubled_capacity > new_capacity) {
    return doubled_capacity;
  } else {
    return new_capacity;
  }
}

static inline void ArrowBufferInit(struct ArrowBuffer* buffer) {
  buffer->data = NULL;
  buffer->size_bytes = 0;
  buffer->capacity_bytes = 0;
  buffer->allocator = ArrowBufferAllocatorDefault();
}

static inline ArrowErrorCode ArrowBufferSetAllocator(
    struct ArrowBuffer* buffer, struct ArrowBufferAllocator allocator) {
  if (buffer->data == NULL) {
    buffer->allocator = allocator;
    return NANOARROW_OK;
  } else {
    return EINVAL;
  }
}

static inline void ArrowBufferReset(struct ArrowBuffer* buffer) {
  if (buffer->data != NULL) {
    buffer->allocator.free(&buffer->allocator, (uint8_t*)buffer->data,
                           buffer->capacity_bytes);
    buffer->data = NULL;
  }

  buffer->capacity_bytes = 0;
  buffer->size_bytes = 0;
}

static inline void ArrowBufferMove(struct ArrowBuffer* src, struct ArrowBuffer* dst) {
  memcpy(dst, src, sizeof(struct ArrowBuffer));
  src->data = NULL;
  ArrowBufferReset(src);
}

static inline ArrowErrorCode ArrowBufferResize(struct ArrowBuffer* buffer,
                                               int64_t new_capacity_bytes,
                                               char shrink_to_fit) {
  if (new_capacity_bytes < 0) {
    return EINVAL;
  }

  if (new_capacity_bytes > buffer->capacity_bytes || shrink_to_fit) {
    buffer->data = buffer->allocator.reallocate(
        &buffer->allocator, buffer->data, buffer->capacity_bytes, new_capacity_bytes);
    if (buffer->data == NULL && new_capacity_bytes > 0) {
      buffer->capacity_bytes = 0;
      buffer->size_bytes = 0;
      return ENOMEM;
    }

    buffer->capacity_bytes = new_capacity_bytes;
  }

  // Ensures that when shrinking that size <= capacity
  if (new_capacity_bytes < buffer->size_bytes) {
    buffer->size_bytes = new_capacity_bytes;
  }

  return NANOARROW_OK;
}

static inline ArrowErrorCode ArrowBufferReserve(struct ArrowBuffer* buffer,
                                                int64_t additional_size_bytes) {
  int64_t min_capacity_bytes = buffer->size_bytes + additional_size_bytes;
  if (min_capacity_bytes <= buffer->capacity_bytes) {
    return NANOARROW_OK;
  }

  return ArrowBufferResize(
      buffer, _ArrowGrowByFactor(buffer->capacity_bytes, min_capacity_bytes), 0);
}

static inline void ArrowBufferAppendUnsafe(struct ArrowBuffer* buffer, const void* data,
                                           int64_t size_bytes) {
  if (size_bytes > 0) {
    memcpy(buffer->data + buffer->size_bytes, data, size_bytes);
    buffer->size_bytes += size_bytes;
  }
}

static inline ArrowErrorCode ArrowBufferAppend(struct ArrowBuffer* buffer,
                                               const void* data, int64_t size_bytes) {
  NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(buffer, size_bytes));

  ArrowBufferAppendUnsafe(buffer, data, size_bytes);
  return NANOARROW_OK;
}

static inline ArrowErrorCode ArrowBufferAppendInt8(struct ArrowBuffer* buffer,
                                                   int8_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(int8_t));
}

static inline ArrowErrorCode ArrowBufferAppendUInt8(struct ArrowBuffer* buffer,
                                                    uint8_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(uint8_t));
}

static inline ArrowErrorCode ArrowBufferAppendInt16(struct ArrowBuffer* buffer,
                                                    int16_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(int16_t));
}

static inline ArrowErrorCode ArrowBufferAppendUInt16(struct ArrowBuffer* buffer,
                                                     uint16_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(uint16_t));
}

static inline ArrowErrorCode ArrowBufferAppendInt32(struct ArrowBuffer* buffer,
                                                    int32_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(int32_t));
}

static inline ArrowErrorCode ArrowBufferAppendUInt32(struct ArrowBuffer* buffer,
                                                     uint32_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(uint32_t));
}

static inline ArrowErrorCode ArrowBufferAppendInt64(struct ArrowBuffer* buffer,
                                                    int64_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(int64_t));
}

static inline ArrowErrorCode ArrowBufferAppendUInt64(struct ArrowBuffer* buffer,
                                                     uint64_t value) {
  return ArrowBufferAppend(buffer, &value, sizeof(uint64_t));
}

static inline ArrowErrorCode ArrowBufferAppendDouble(struct ArrowBuffer* buffer,
                                                     double value) {
  return ArrowBufferAppend(buffer, &value, sizeof(double));
}

static inline ArrowErrorCode ArrowBufferAppendFloat(struct ArrowBuffer* buffer,
                                                    float value) {
  return ArrowBufferAppend(buffer, &value, sizeof(float));
}

static inline ArrowErrorCode ArrowBufferAppendStringView(struct ArrowBuffer* buffer,
                                                         struct ArrowStringView value) {
  return ArrowBufferAppend(buffer, value.data, value.size_bytes);
}

static inline ArrowErrorCode ArrowBufferAppendBufferView(struct ArrowBuffer* buffer,
                                                         struct ArrowBufferView value) {
  return ArrowBufferAppend(buffer, value.data.data, value.size_bytes);
}

static inline ArrowErrorCode ArrowBufferAppendFill(struct ArrowBuffer* buffer,
                                                   uint8_t value, int64_t size_bytes) {
  NANOARROW_RETURN_NOT_OK(ArrowBufferReserve(buffer, size_bytes));

  memset(buffer->data + buffer->size_bytes, value, size_bytes);
  buffer->size_bytes += size_bytes;
  return NANOARROW_OK;
}

static const uint8_t _ArrowkBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};
static const uint8_t _ArrowkFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};
static const uint8_t _ArrowkPrecedingBitmask[] = {0, 1, 3, 7, 15, 31, 63, 127};
static const uint8_t _ArrowkTrailingBitmask[] = {255, 254, 252, 248, 240, 224, 192, 128};

static const uint8_t _ArrowkBytePopcount[] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
    4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
    4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4,
    5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
    4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2,
    3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
    5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
    5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

static inline int64_t _ArrowRoundUpToMultipleOf8(int64_t value) {
  return (value + 7) & ~((int64_t)7);
}

static inline int64_t _ArrowRoundDownToMultipleOf8(int64_t value) {
  return (value / 8) * 8;
}

static inline int64_t _ArrowBytesForBits(int64_t bits) {
  return (bits >> 3) + ((bits & 7) != 0);
}

static inline void _ArrowBitsUnpackInt8(const uint8_t word, int8_t* out) {
  out[0] = (word & 0x1) != 0;
  out[1] = (word & 0x2) != 0;
  out[2] = (word & 0x4) != 0;
  out[3] = (word & 0x8) != 0;
  out[4] = (word & 0x10) != 0;
  out[5] = (word & 0x20) != 0;
  out[6] = (word & 0x40) != 0;
  out[7] = (word & 0x80) != 0;
}

static inline void _ArrowBitsUnpackInt32(const uint8_t word, int32_t* out) {
  out[0] = (word & 0x1) != 0;
  out[1] = (word & 0x2) != 0;
  out[2] = (word & 0x4) != 0;
  out[3] = (word & 0x8) != 0;
  out[4] = (word & 0x10) != 0;
  out[5] = (word & 0x20) != 0;
  out[6] = (word & 0x40) != 0;
  out[7] = (word & 0x80) != 0;
}

static inline void _ArrowBitmapPackInt8(const int8_t* values, uint8_t* out) {
  *out = (values[0] | ((values[1] + 0x1) & 0x2) | ((values[2] + 0x3) & 0x4) |
          ((values[3] + 0x7) & 0x8) | ((values[4] + 0xf) & 0x10) |
          ((values[5] + 0x1f) & 0x20) | ((values[6] + 0x3f) & 0x40) |
          ((values[7] + 0x7f) & 0x80));
}

static inline void _ArrowBitmapPackInt32(const int32_t* values, uint8_t* out) {
  *out = (values[0] | ((values[1] + 0x1) & 0x2) | ((values[2] + 0x3) & 0x4) |
          ((values[3] + 0x7) & 0x8) | ((values[4] + 0xf) & 0x10) |
          ((values[5] + 0x1f) & 0x20) | ((values[6] + 0x3f) & 0x40) |
          ((values[7] + 0x7f) & 0x80));
}

static inline int8_t ArrowBitGet(const uint8_t* bits, int64_t i) {
  return (bits[i >> 3] >> (i & 0x07)) & 1;
}

static inline void ArrowBitsUnpackInt8(const uint8_t* bits, int64_t start_offset,
                                       int64_t length, int8_t* out) {
  if (length == 0) {
    return;
  }

  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const int64_t i_last_valid = i_end - 1;

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_last_valid = i_last_valid / 8;

  if (bytes_begin == bytes_last_valid) {
    for (int i = 0; i < length; i++) {
      out[i] = ArrowBitGet(&bits[bytes_begin], i + i_begin % 8);
    }

    return;
  }

  // first byte
  for (int i = 0; i < 8 - (i_begin % 8); i++) {
    *out++ = ArrowBitGet(&bits[bytes_begin], i + i_begin % 8);
  }

  // middle bytes
  for (int64_t i = bytes_begin + 1; i < bytes_last_valid; i++) {
    _ArrowBitsUnpackInt8(bits[i], out);
    out += 8;
  }

  // last byte
  const int bits_remaining = i_end % 8 == 0 ? 8 : i_end % 8;
  for (int i = 0; i < bits_remaining; i++) {
    *out++ = ArrowBitGet(&bits[bytes_last_valid], i);
  }
}

static inline void ArrowBitsUnpackInt32(const uint8_t* bits, int64_t start_offset,
                                        int64_t length, int32_t* out) {
  if (length == 0) {
    return;
  }

  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const int64_t i_last_valid = i_end - 1;

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_last_valid = i_last_valid / 8;

  if (bytes_begin == bytes_last_valid) {
    for (int i = 0; i < length; i++) {
      out[i] = ArrowBitGet(&bits[bytes_begin], i + i_begin % 8);
    }

    return;
  }

  // first byte
  for (int i = 0; i < 8 - (i_begin % 8); i++) {
    *out++ = ArrowBitGet(&bits[bytes_begin], i + i_begin % 8);
  }

  // middle bytes
  for (int64_t i = bytes_begin + 1; i < bytes_last_valid; i++) {
    _ArrowBitsUnpackInt32(bits[i], out);
    out += 8;
  }

  // last byte
  const int bits_remaining = i_end % 8 == 0 ? 8 : i_end % 8;
  for (int i = 0; i < bits_remaining; i++) {
    *out++ = ArrowBitGet(&bits[bytes_last_valid], i);
  }
}

static inline void ArrowBitSet(uint8_t* bits, int64_t i) {
  bits[i / 8] |= _ArrowkBitmask[i % 8];
}

static inline void ArrowBitClear(uint8_t* bits, int64_t i) {
  bits[i / 8] &= _ArrowkFlippedBitmask[i % 8];
}

static inline void ArrowBitSetTo(uint8_t* bits, int64_t i, uint8_t bit_is_set) {
  bits[i / 8] ^=
      ((uint8_t)(-((uint8_t)(bit_is_set != 0)) ^ bits[i / 8])) & _ArrowkBitmask[i % 8];
}

static inline void ArrowBitsSetTo(uint8_t* bits, int64_t start_offset, int64_t length,
                                  uint8_t bits_are_set) {
  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const uint8_t fill_byte = (uint8_t)(-bits_are_set);

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_end = i_end / 8 + 1;

  const uint8_t first_byte_mask = _ArrowkPrecedingBitmask[i_begin % 8];
  const uint8_t last_byte_mask = _ArrowkTrailingBitmask[i_end % 8];

  if (bytes_end == bytes_begin + 1) {
    // set bits within a single byte
    const uint8_t only_byte_mask =
        i_end % 8 == 0 ? first_byte_mask : (uint8_t)(first_byte_mask | last_byte_mask);
    bits[bytes_begin] &= only_byte_mask;
    bits[bytes_begin] |= (uint8_t)(fill_byte & ~only_byte_mask);
    return;
  }

  // set/clear trailing bits of first byte
  bits[bytes_begin] &= first_byte_mask;
  bits[bytes_begin] |= (uint8_t)(fill_byte & ~first_byte_mask);

  if (bytes_end - bytes_begin > 2) {
    // set/clear whole bytes
    memset(bits + bytes_begin + 1, fill_byte, (size_t)(bytes_end - bytes_begin - 2));
  }

  if (i_end % 8 == 0) {
    return;
  }

  // set/clear leading bits of last byte
  bits[bytes_end - 1] &= last_byte_mask;
  bits[bytes_end - 1] |= (uint8_t)(fill_byte & ~last_byte_mask);
}

static inline int64_t ArrowBitCountSet(const uint8_t* bits, int64_t start_offset,
                                       int64_t length) {
  if (length == 0) {
    return 0;
  }

  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const int64_t i_last_valid = i_end - 1;

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_last_valid = i_last_valid / 8;

  if (bytes_begin == bytes_last_valid) {
    // count bits within a single byte
    const uint8_t first_byte_mask = _ArrowkPrecedingBitmask[i_end % 8];
    const uint8_t last_byte_mask = _ArrowkTrailingBitmask[i_begin % 8];

    const uint8_t only_byte_mask =
        i_end % 8 == 0 ? last_byte_mask : (uint8_t)(first_byte_mask & last_byte_mask);

    const uint8_t byte_masked = bits[bytes_begin] & only_byte_mask;
    return _ArrowkBytePopcount[byte_masked];
  }

  const uint8_t first_byte_mask = _ArrowkPrecedingBitmask[i_begin % 8];
  const uint8_t last_byte_mask = i_end % 8 == 0 ? 0 : _ArrowkTrailingBitmask[i_end % 8];
  int64_t count = 0;

  // first byte
  count += _ArrowkBytePopcount[bits[bytes_begin] & ~first_byte_mask];

  // middle bytes
  for (int64_t i = bytes_begin + 1; i < bytes_last_valid; i++) {
    count += _ArrowkBytePopcount[bits[i]];
  }

  // last byte
  count += _ArrowkBytePopcount[bits[bytes_last_valid] & ~last_byte_mask];

  return count;
}

static inline void ArrowBitmapInit(struct ArrowBitmap* bitmap) {
  ArrowBufferInit(&bitmap->buffer);
  bitmap->size_bits = 0;
}

static inline void ArrowBitmapMove(struct ArrowBitmap* src, struct ArrowBitmap* dst) {
  ArrowBufferMove(&src->buffer, &dst->buffer);
  dst->size_bits = src->size_bits;
  src->size_bits = 0;
}

static inline ArrowErrorCode ArrowBitmapReserve(struct ArrowBitmap* bitmap,
                                                int64_t additional_size_bits) {
  int64_t min_capacity_bits = bitmap->size_bits + additional_size_bits;
  if (min_capacity_bits <= (bitmap->buffer.capacity_bytes * 8)) {
    return NANOARROW_OK;
  }

  NANOARROW_RETURN_NOT_OK(
      ArrowBufferReserve(&bitmap->buffer, _ArrowBytesForBits(additional_size_bits)));

  bitmap->buffer.data[bitmap->buffer.capacity_bytes - 1] = 0;
  return NANOARROW_OK;
}

static inline ArrowErrorCode ArrowBitmapResize(struct ArrowBitmap* bitmap,
                                               int64_t new_capacity_bits,
                                               char shrink_to_fit) {
  if (new_capacity_bits < 0) {
    return EINVAL;
  }

  int64_t new_capacity_bytes = _ArrowBytesForBits(new_capacity_bits);
  NANOARROW_RETURN_NOT_OK(
      ArrowBufferResize(&bitmap->buffer, new_capacity_bytes, shrink_to_fit));

  if (new_capacity_bits < bitmap->size_bits) {
    bitmap->size_bits = new_capacity_bits;
  }

  return NANOARROW_OK;
}

static inline ArrowErrorCode ArrowBitmapAppend(struct ArrowBitmap* bitmap,
                                               uint8_t bits_are_set, int64_t length) {
  NANOARROW_RETURN_NOT_OK(ArrowBitmapReserve(bitmap, length));

  ArrowBitmapAppendUnsafe(bitmap, bits_are_set, length);
  return NANOARROW_OK;
}

static inline void ArrowBitmapAppendUnsafe(struct ArrowBitmap* bitmap,
                                           uint8_t bits_are_set, int64_t length) {
  ArrowBitsSetTo(bitmap->buffer.data, bitmap->size_bits, length, bits_are_set);
  bitmap->size_bits += length;
  bitmap->buffer.size_bytes = _ArrowBytesForBits(bitmap->size_bits);
}

static inline void ArrowBitmapAppendInt8Unsafe(struct ArrowBitmap* bitmap,
                                               const int8_t* values, int64_t n_values) {
  if (n_values == 0) {
    return;
  }

  const int8_t* values_cursor = values;
  int64_t n_remaining = n_values;
  int64_t out_i_cursor = bitmap->size_bits;
  uint8_t* out_cursor = bitmap->buffer.data + bitmap->size_bits / 8;

  // First byte
  if ((out_i_cursor % 8) != 0) {
    int64_t n_partial_bits = _ArrowRoundUpToMultipleOf8(out_i_cursor) - out_i_cursor;
    for (int i = 0; i < n_partial_bits; i++) {
      ArrowBitSetTo(bitmap->buffer.data, out_i_cursor++, values[i]);
    }

    out_cursor++;
    values_cursor += n_partial_bits;
    n_remaining -= n_partial_bits;
  }

  // Middle bytes
  int64_t n_full_bytes = n_remaining / 8;
  for (int64_t i = 0; i < n_full_bytes; i++) {
    _ArrowBitmapPackInt8(values_cursor, out_cursor);
    values_cursor += 8;
    out_cursor++;
  }

  // Last byte
  out_i_cursor += n_full_bytes * 8;
  n_remaining -= n_full_bytes * 8;
  if (n_remaining > 0) {
    // Zero out the last byte
    *out_cursor = 0x00;
    for (int i = 0; i < n_remaining; i++) {
      ArrowBitSetTo(bitmap->buffer.data, out_i_cursor++, values_cursor[i]);
    }
    out_cursor++;
  }

  bitmap->size_bits += n_values;
  bitmap->buffer.size_bytes = out_cursor - bitmap->buffer.data;
}

static inline void ArrowBitmapAppendInt32Unsafe(struct ArrowBitmap* bitmap,
                                                const int32_t* values, int64_t n_values) {
  if (n_values == 0) {
    return;
  }

  const int32_t* values_cursor = values;
  int64_t n_remaining = n_values;
  int64_t out_i_cursor = bitmap->size_bits;
  uint8_t* out_cursor = bitmap->buffer.data + bitmap->size_bits / 8;

  // First byte
  if ((out_i_cursor % 8) != 0) {
    int64_t n_partial_bits = _ArrowRoundUpToMultipleOf8(out_i_cursor) - out_i_cursor;
    for (int i = 0; i < n_partial_bits; i++) {
      ArrowBitSetTo(bitmap->buffer.data, out_i_cursor++, values[i]);
    }

    out_cursor++;
    values_cursor += n_partial_bits;
    n_remaining -= n_partial_bits;
  }

  // Middle bytes
  int64_t n_full_bytes = n_remaining / 8;
  for (int64_t i = 0; i < n_full_bytes; i++) {
    _ArrowBitmapPackInt32(values_cursor, out_cursor);
    values_cursor += 8;
    out_cursor++;
  }

  // Last byte
  out_i_cursor += n_full_bytes * 8;
  n_remaining -= n_full_bytes * 8;
  if (n_remaining > 0) {
    // Zero out the last byte
    *out_cursor = 0x00;
    for (int i = 0; i < n_remaining; i++) {
      ArrowBitSetTo(bitmap->buffer.data, out_i_cursor++, values_cursor[i]);
    }
    out_cursor++;
  }

  bitmap->size_bits += n_values;
  bitmap->buffer.size_bytes = out_cursor - bitmap->buffer.data;
}

static inline void ArrowBitmapReset(struct ArrowBitmap* bitmap) {
  ArrowBufferReset(&bitmap->buffer);
  bitmap->size_bits = 0;
}

#ifdef __cplusplus
}
#endif

#endif
