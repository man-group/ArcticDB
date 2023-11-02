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

#include <cerrno>
#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "nanoarrow/nanoarrow.h"

// This test allocator guarantees that allocator->reallocate will return
// a new pointer so that we can test when reallocations happen whilst
// building buffers.
static uint8_t* TestAllocatorReallocate(struct ArrowBufferAllocator* allocator,
                                        uint8_t* ptr, int64_t old_size,
                                        int64_t new_size) {
  uint8_t* new_ptr = reinterpret_cast<uint8_t*>(malloc(new_size));

  int64_t copy_size = std::min<int64_t>(old_size, new_size);
  if (new_ptr != nullptr && copy_size > 0) {
    memcpy(new_ptr, ptr, copy_size);
  }

  if (ptr != nullptr) {
    free(ptr);
  }

  return new_ptr;
}

static void TestAllocatorFree(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                              int64_t size) {
  free(ptr);
}

static struct ArrowBufferAllocator test_allocator = {&TestAllocatorReallocate,
                                                     &TestAllocatorFree, nullptr};

TEST(BufferTest, BufferTestBasic) {
  struct ArrowBuffer buffer;

  // Init
  ArrowBufferInit(&buffer);
  ASSERT_EQ(ArrowBufferSetAllocator(&buffer, test_allocator), NANOARROW_OK);
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.size_bytes, 0);

  // Reserve where capacity > current_capacity * growth_factor
  EXPECT_EQ(ArrowBufferReserve(&buffer, 10), NANOARROW_OK);
  EXPECT_NE(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 10);
  EXPECT_EQ(buffer.size_bytes, 0);

  // Write without triggering a realloc
  uint8_t* first_data = buffer.data;
  EXPECT_EQ(ArrowBufferAppend(&buffer, "1234567890", 10), NANOARROW_OK);
  EXPECT_EQ(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 10);
  EXPECT_EQ(buffer.size_bytes, 10);

  // Write triggering a realloc
  EXPECT_EQ(ArrowBufferAppend(&buffer, "1", 2), NANOARROW_OK);
  EXPECT_NE(buffer.data, first_data);
  EXPECT_EQ(buffer.capacity_bytes, 20);
  EXPECT_EQ(buffer.size_bytes, 12);
  EXPECT_STREQ(reinterpret_cast<char*>(buffer.data), "12345678901");

  // Resize smaller without shrinking
  EXPECT_EQ(ArrowBufferResize(&buffer, 5, false), NANOARROW_OK);
  EXPECT_EQ(buffer.capacity_bytes, 20);
  EXPECT_EQ(buffer.size_bytes, 5);
  EXPECT_EQ(strncmp(reinterpret_cast<char*>(buffer.data), "12345", 5), 0);

  // Resize smaller with shrinking
  EXPECT_EQ(ArrowBufferResize(&buffer, 4, true), NANOARROW_OK);
  EXPECT_EQ(buffer.capacity_bytes, 4);
  EXPECT_EQ(buffer.size_bytes, 4);
  EXPECT_EQ(strncmp(reinterpret_cast<char*>(buffer.data), "1234", 4), 0);

  // Reset the buffer
  ArrowBufferReset(&buffer);
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.size_bytes, 0);
}

TEST(BufferTest, BufferTestMove) {
  struct ArrowBuffer buffer;

  ArrowBufferInit(&buffer);
  ASSERT_EQ(ArrowBufferSetAllocator(&buffer, test_allocator), NANOARROW_OK);
  ASSERT_EQ(ArrowBufferAppend(&buffer, "1234567", 7), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 7);
  EXPECT_EQ(buffer.capacity_bytes, 7);

  struct ArrowBuffer buffer_out;
  ArrowBufferMove(&buffer, &buffer_out);
  EXPECT_EQ(buffer.size_bytes, 0);
  EXPECT_EQ(buffer.capacity_bytes, 0);
  EXPECT_EQ(buffer.data, nullptr);
  EXPECT_EQ(buffer_out.size_bytes, 7);
  EXPECT_EQ(buffer_out.capacity_bytes, 7);

  ArrowBufferReset(&buffer_out);
}

TEST(BufferTest, BufferTestFill) {
  struct ArrowBuffer buffer;
  ArrowBufferInit(&buffer);

  EXPECT_EQ(ArrowBufferAppendFill(&buffer, 0xff, 10), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 10);
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(buffer.data[i], 0xff);
  }

  buffer.size_bytes = 0;
  EXPECT_EQ(ArrowBufferAppendFill(&buffer, 0, 10), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 10);
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(buffer.data[i], 0);
  }

  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendFill(&buffer, 0, std::numeric_limits<int64_t>::max()),
            ENOMEM);
}

TEST(BufferTest, BufferTestResize0) {
  struct ArrowBuffer buffer;

  ArrowBufferInit(&buffer);
  ASSERT_EQ(ArrowBufferSetAllocator(&buffer, test_allocator), NANOARROW_OK);
  ASSERT_EQ(ArrowBufferAppend(&buffer, "1234567", 7), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 7);
  EXPECT_EQ(buffer.capacity_bytes, 7);

  EXPECT_EQ(ArrowBufferResize(&buffer, 0, false), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 0);
  EXPECT_EQ(buffer.capacity_bytes, 7);

  EXPECT_EQ(ArrowBufferResize(&buffer, 0, true), NANOARROW_OK);
  EXPECT_EQ(buffer.size_bytes, 0);
  EXPECT_EQ(buffer.capacity_bytes, 0);

  ArrowBufferReset(&buffer);
}

TEST(BufferTest, BufferTestError) {
  struct ArrowBuffer buffer;
  ArrowBufferInit(&buffer);
  EXPECT_EQ(ArrowBufferResize(&buffer, std::numeric_limits<int64_t>::max(), false),
            ENOMEM);
  EXPECT_EQ(ArrowBufferAppend(&buffer, nullptr, std::numeric_limits<int64_t>::max()),
            ENOMEM);

  ASSERT_EQ(ArrowBufferAppend(&buffer, "abcd", 4), NANOARROW_OK);
  EXPECT_EQ(ArrowBufferSetAllocator(&buffer, ArrowBufferAllocatorDefault()), EINVAL);

  EXPECT_EQ(ArrowBufferResize(&buffer, -1, false), EINVAL);

  ArrowBufferReset(&buffer);
}

TEST(BufferTest, BufferTestAppendHelpers) {
  struct ArrowBuffer buffer;
  ArrowBufferInit(&buffer);

  EXPECT_EQ(ArrowBufferAppendInt8(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<int8_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendUInt8(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<uint8_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendInt16(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<int16_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendUInt16(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<uint16_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendInt32(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<int32_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendUInt32(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<uint32_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendInt64(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<int64_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendUInt64(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<uint64_t*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendDouble(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<double*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendFloat(&buffer, 123), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<float*>(buffer.data)[0], 123);
  ArrowBufferReset(&buffer);

  EXPECT_EQ(ArrowBufferAppendStringView(&buffer, ArrowCharView("a")), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<char*>(buffer.data)[0], 'a');
  EXPECT_EQ(buffer.size_bytes, 1);
  ArrowBufferReset(&buffer);

  struct ArrowBufferView buffer_view;
  buffer_view.data.data = "a";
  buffer_view.size_bytes = 1;
  EXPECT_EQ(ArrowBufferAppendBufferView(&buffer, buffer_view), NANOARROW_OK);
  EXPECT_EQ(reinterpret_cast<char*>(buffer.data)[0], 'a');
  EXPECT_EQ(buffer.size_bytes, 1);
  ArrowBufferReset(&buffer);
}

TEST(BitmapTest, BitmapTestElement) {
  uint8_t bitmap[10];

  memset(bitmap, 0xff, sizeof(bitmap));
  for (int i = 0; i < sizeof(bitmap) * 8; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap, i), 1);
  }

  bitmap[2] = 0xfd;
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 0), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 1), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 2), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 3), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 4), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 5), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 6), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 7), 1);

  memset(bitmap, 0x00, sizeof(bitmap));
  for (int i = 0; i < sizeof(bitmap) * 8; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap, i), 0);
  }

  bitmap[2] = 0x02;
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 0), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 1), 1);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 2), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 3), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 4), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 5), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 6), 0);
  EXPECT_EQ(ArrowBitGet(bitmap, 16 + 7), 0);
}

template <int offset, int length>
void TestArrowBitmapUnpackUnsafe(const uint8_t* bitmap, std::vector<int8_t> expected) {
  int8_t out[length];
  int32_t out32[length];
  memset(out, 0, sizeof(out));
  memset(out32, 0, sizeof(out32));

  ASSERT_EQ(length, expected.size());

  ArrowBitsUnpackInt8(bitmap, offset, length, out);
  for (int i = 0; i < length; i++) {
    EXPECT_EQ(out[i], expected[i]);
  }

  ArrowBitsUnpackInt32(bitmap, offset, length, out32);
  for (int i = 0; i < length; i++) {
    EXPECT_EQ(out32[i], expected[i]);
  }
}

TEST(BitmapTest, BitmapTestBitmapUnpack) {
  uint8_t bitmap[3];
  int8_t result[sizeof(bitmap) * 8];
  int32_t result32[sizeof(result)];
  int64_t n_values = sizeof(result);

  // Basic test of a validity buffer that is all true
  memset(bitmap, 0xff, sizeof(bitmap));
  memset(result, 0, sizeof(result));
  memset(result32, 0, sizeof(result32));

  ArrowBitsUnpackInt8(bitmap, 0, sizeof(result), result);
  for (int i = 0; i < n_values; i++) {
    EXPECT_EQ(result[i], 1);
  }

  ArrowBitsUnpackInt32(bitmap, 0, sizeof(result), result32);
  for (int i = 0; i < n_values; i++) {
    EXPECT_EQ(result32[i], 1);
  }

  // Ensure that the first byte/middle byte/last byte logic is correct
  // Note that TestArrowBitmapUnpack tests both the int8 and int32 version
  bitmap[0] = 0x93;  // 10010011
  bitmap[1] = 0x55;  // 01010101
  bitmap[2] = 0xaa;  // 10101010

  // offset 0, length boundary, one byte
  TestArrowBitmapUnpackUnsafe<0, 8>(bitmap, {1, 1, 0, 0, 1, 0, 0, 1});

  // offset 0, length boundary, different bytes
  TestArrowBitmapUnpackUnsafe<0, 16>(bitmap,
                                     {1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0});

  // offset 0, length non-boundary, one byte
  TestArrowBitmapUnpackUnsafe<0, 5>(bitmap, {1, 1, 0, 0, 1});

  // offset boundary, length boundary, one byte
  TestArrowBitmapUnpackUnsafe<8, 8>(bitmap, {1, 0, 1, 0, 1, 0, 1, 0});

  // offset boundary, length boundary, different bytes
  TestArrowBitmapUnpackUnsafe<8, 16>(bitmap,
                                     {1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1});

  // offset boundary, length non-boundary, one byte
  TestArrowBitmapUnpackUnsafe<8, 5>(bitmap, {1, 0, 1, 0, 1});

  // offset boundary, length non-boundary, different bytes
  TestArrowBitmapUnpackUnsafe<8, 13>(bitmap, {1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0});

  // offset non-boundary, length boundary, one byte
  TestArrowBitmapUnpackUnsafe<3, 5>(bitmap, {0, 1, 0, 0, 1});

  // offset non-boundary, length boundary, different bytes
  TestArrowBitmapUnpackUnsafe<3, 13>(bitmap, {0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0});

  // offset non-boundary, length non-boundary, one byte
  TestArrowBitmapUnpackUnsafe<3, 3>(bitmap, {0, 1, 0});

  // offset non-boundary, length non-boundary, different bytes
  TestArrowBitmapUnpackUnsafe<3, 11>(bitmap, {0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0});

  // offset non-boundary non-first byte, length boundary, one byte
  TestArrowBitmapUnpackUnsafe<11, 5>(bitmap, {0, 1, 0, 1, 0});

  // offset non-boundary non-first byte, length boundary, different bytes
  TestArrowBitmapUnpackUnsafe<11, 13>(bitmap, {0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 1});

  // offset non-boundary non-first byte, length non-boundary, one byte
  TestArrowBitmapUnpackUnsafe<11, 3>(bitmap, {0, 1, 0});

  // offset non-boundary non-first byte, length non-boundary, different bytes
  TestArrowBitmapUnpackUnsafe<11, 11>(bitmap, {0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1});

  // non-boundary, three byte span
  TestArrowBitmapUnpackUnsafe<7, 11>(bitmap, {1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1});
}

TEST(BitmapTest, BitmapTestSetTo) {
  uint8_t bitmap[10];

  memset(bitmap, 0xff, sizeof(bitmap));
  ArrowBitSetTo(bitmap, 16 + 1, 0);
  EXPECT_EQ(bitmap[2], 0xfd);
  ArrowBitSetTo(bitmap, 16 + 1, 1);
  EXPECT_EQ(bitmap[2], 0xff);

  memset(bitmap, 0xff, sizeof(bitmap));
  ArrowBitClear(bitmap, 16 + 1);
  EXPECT_EQ(bitmap[2], 0xfd);
  ArrowBitSet(bitmap, 16 + 1);
  EXPECT_EQ(bitmap[2], 0xff);

  memset(bitmap, 0x00, sizeof(bitmap));
  ArrowBitSetTo(bitmap, 16 + 1, 1);
  EXPECT_EQ(bitmap[2], 0x02);
  ArrowBitSetTo(bitmap, 16 + 1, 0);
  EXPECT_EQ(bitmap[2], 0x00);

  memset(bitmap, 0x00, sizeof(bitmap));
  ArrowBitSet(bitmap, 16 + 1);
  EXPECT_EQ(bitmap[2], 0x02);
  ArrowBitClear(bitmap, 16 + 1);
  EXPECT_EQ(bitmap[2], 0x00);
}

TEST(BitmapTest, BitmapTestCountSet) {
  uint8_t bitmap[10];
  memset(bitmap, 0x00, sizeof(bitmap));
  ArrowBitSet(bitmap, 18);
  ArrowBitSet(bitmap, 23);
  ArrowBitSet(bitmap, 74);

  // Check masking of the last byte
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 80), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 79), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 78), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 77), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 76), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 75), 3);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 74), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 73), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 72), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 0, 71), 2);

  // Check masking of the first byte
  EXPECT_EQ(ArrowBitCountSet(bitmap, 15, 17), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 16, 16), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 17, 15), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 18, 14), 2);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 19, 13), 1);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 20, 12), 1);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 21, 11), 1);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 22, 10), 1);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 23, 9), 1);
  EXPECT_EQ(ArrowBitCountSet(bitmap, 24, 8), 0);
}

TEST(BitmapTest, BitmapTestCountSetSingleByte) {
  uint8_t bitmap = 0xff;

  // Check starting on a byte boundary
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 0), 0);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 2), 2);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 3), 3);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 4), 4);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 5), 5);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 6), 6);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 7), 7);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 8), 8);

  // Check bits in the middle
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 1, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 2, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 3, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 4, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 5, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 6, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 7, 1), 1);

  // Check ending on a byte boundary
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 0, 8), 8);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 1, 7), 7);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 2, 6), 6);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 3, 5), 5);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 4, 4), 4);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 5, 3), 3);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 6, 2), 2);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 7, 1), 1);
  EXPECT_EQ(ArrowBitCountSet(&bitmap, 8, 0), 0);
}

TEST(BitmapTest, BitmapTestAppend) {
  int8_t test_values[65];
  memset(test_values, 0, sizeof(test_values));
  test_values[4] = 1;
  test_values[63] = 1;
  test_values[64] = 1;

  struct ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);

  for (int64_t i = 0; i < 65; i++) {
    ASSERT_EQ(ArrowBitmapAppend(&bitmap, test_values[i], 1), NANOARROW_OK);
  }

  EXPECT_EQ(bitmap.size_bits, 65);
  EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, 4), test_values[4]);
  for (int i = 0; i < 65; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }

  ArrowBitmapReset(&bitmap);
}

TEST(BitmapTest, BitmapTestMove) {
  struct ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);
  ASSERT_EQ(ArrowBitmapAppend(&bitmap, 1, 1), NANOARROW_OK);
  ASSERT_NE(bitmap.buffer.data, nullptr);
  ASSERT_EQ(bitmap.size_bits, 1);

  struct ArrowBitmap bitmap2;
  bitmap2.buffer.data = NULL;
  ArrowBitmapMove(&bitmap, &bitmap2);
  EXPECT_EQ(bitmap.buffer.data, nullptr);
  EXPECT_EQ(bitmap.size_bits, 0);
  EXPECT_NE(bitmap2.buffer.data, nullptr);
  EXPECT_EQ(bitmap2.size_bits, 1);

  ArrowBitmapReset(&bitmap2);
}

TEST(BitmapTest, BitmapTestResize) {
  struct ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);

  // Check normal usage, which is resize to the final length
  // after appending a bunch of values
  ArrowBitmapResize(&bitmap, 200, false);
  EXPECT_EQ(bitmap.buffer.size_bytes, 0);
  EXPECT_EQ(bitmap.buffer.capacity_bytes, 200 / 8);
  EXPECT_EQ(bitmap.size_bits, 0);

  ArrowBitmapAppendUnsafe(&bitmap, true, 100);
  EXPECT_EQ(bitmap.buffer.size_bytes, 100 / 8 + 1);
  EXPECT_EQ(bitmap.buffer.capacity_bytes, 200 / 8);
  EXPECT_EQ(bitmap.size_bits, 100);

  // Resize without shrinking
  EXPECT_EQ(ArrowBitmapResize(&bitmap, 100, false), NANOARROW_OK);
  EXPECT_EQ(bitmap.buffer.size_bytes, 100 / 8 + 1);
  EXPECT_EQ(bitmap.buffer.capacity_bytes, 200 / 8);
  EXPECT_EQ(bitmap.size_bits, 100);

  // Resize with shrinking
  EXPECT_EQ(ArrowBitmapResize(&bitmap, 100, true), NANOARROW_OK);
  EXPECT_EQ(bitmap.buffer.size_bytes, 100 / 8 + 1);
  EXPECT_EQ(bitmap.buffer.capacity_bytes, bitmap.buffer.size_bytes);
  EXPECT_EQ(bitmap.size_bits, 100);

  // Resize with shrinking when a reallocation isn't needed to shrink
  EXPECT_EQ(ArrowBitmapResize(&bitmap, 99, true), NANOARROW_OK);
  EXPECT_EQ(bitmap.buffer.size_bytes, 100 / 8 + 1);
  EXPECT_EQ(bitmap.buffer.capacity_bytes, bitmap.buffer.size_bytes);
  EXPECT_EQ(bitmap.size_bits, 99);

  ArrowBitmapReset(&bitmap);
}

TEST(BitmapTest, BitmapTestAppendInt8Unsafe) {
  struct ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);

  // 68 because this will end in the middle of a byte, and appending twice
  // will end exactly on the end of a byte
  int8_t test_values[68];
  memset(test_values, 0, sizeof(test_values));
  // Make it easy to check the answer without repeating sequential packed byte values
  for (int i = 0; i < 68; i++) {
    test_values[i] = (i % 5) == 0;
  }

  // Append starting at 0
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt8Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 68);
  EXPECT_EQ(bitmap.buffer.size_bytes, 9);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }

  // Append starting at a non-byte aligned value
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt8Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 68 * 2);
  EXPECT_EQ(bitmap.buffer.size_bytes, 17);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }
  for (int i = 69; i < (68 * 2); i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 68]);
  }

  // Append starting at a byte aligned but non-zero value
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt8Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 204);
  EXPECT_EQ(bitmap.buffer.size_bytes, 26);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }
  for (int i = 69; i < 136; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 68]);
  }
  for (int i = 136; i < 204; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 136]);
  }

  ArrowBitmapReset(&bitmap);
}

TEST(BitmapTest, BitmapTestAppendInt32Unsafe) {
  struct ArrowBitmap bitmap;
  ArrowBitmapInit(&bitmap);

  // 68 because this will end in the middle of a byte, and appending twice
  // will end exactly on the end of a byte
  int32_t test_values[68];
  memset(test_values, 0, sizeof(test_values));
  // Make it easy to check the answer without repeating sequential packed byte values
  for (int i = 0; i < 68; i++) {
    test_values[i] = (i % 5) == 0;
  }

  // Append starting at 0
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt32Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 68);
  EXPECT_EQ(bitmap.buffer.size_bytes, 9);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }

  // Append starting at a non-byte aligned value
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt32Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 68 * 2);
  EXPECT_EQ(bitmap.buffer.size_bytes, 17);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }
  for (int i = 69; i < (68 * 2); i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 68]);
  }

  // Append starting at a byte aligned but non-zero value
  ASSERT_EQ(ArrowBitmapReserve(&bitmap, 68), NANOARROW_OK);
  ArrowBitmapAppendInt32Unsafe(&bitmap, test_values, 68);

  EXPECT_EQ(bitmap.size_bits, 204);
  EXPECT_EQ(bitmap.buffer.size_bytes, 26);
  for (int i = 0; i < 68; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i]);
  }
  for (int i = 69; i < 136; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 68]);
  }
  for (int i = 136; i < 204; i++) {
    EXPECT_EQ(ArrowBitGet(bitmap.buffer.data, i), test_values[i - 136]);
  }

  ArrowBitmapReset(&bitmap);
}
