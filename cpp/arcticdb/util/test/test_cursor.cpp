/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h> // googletest header file
#include <numeric>
#include <vector>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/cursor.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/error_code.hpp>

TEST(Cursor, Behaviour) {
    using namespace arcticdb;
    CursoredBuffer<Buffer> b;
    ASSERT_EQ(b.buffer().bytes(), 0);
    ASSERT_NO_THROW(b.cursor()); // no free space
    ASSERT_NO_THROW(b.ensure_bytes(100));
    ASSERT_NO_THROW(b.cursor());
    ASSERT_EQ(b.size<uint8_t>(), 100);
    // ASSERT_THROW(b.ensure_bytes( 20), std::invalid_argument); // uncommitted data
    ASSERT_NO_THROW(b.commit());
    ASSERT_THROW(b.commit(), ArcticCategorizedException<ErrorCategory::INTERNAL>); // commit called twice
    ASSERT_EQ(b.size<uint8_t>(), 100);
    ASSERT_NO_THROW(b.ptr_cast<uint64_t>(5, sizeof(uint64_t)));
    ASSERT_NO_THROW(b.ptr_cast<uint64_t>(11, sizeof(uint64_t)));
    ASSERT_THROW(b.ptr_cast<uint64_t>(13, sizeof(uint64_t)), ArcticCategorizedException<ErrorCategory::INTERNAL>); // Cursor
                                                                                                                   // overflow
    ASSERT_NO_THROW(b.ensure_bytes(20));
    ASSERT_NO_THROW(b.commit());
    ASSERT_EQ(b.size<uint8_t>(), 120);
}

template<typename BufferType>
void test_cursor_backing() {
    using namespace arcticdb;
    CursoredBuffer<BufferType> b;
    std::vector<uint64_t> v(100);
    std::iota(std::begin(v), std::end(v), 0);
    b.ensure_bytes(400);
    memcpy(b.cursor(), v.data(), 400);
    b.commit();
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(0, sizeof(uint64_t)), 0);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(40 * sizeof(uint64_t), sizeof(uint64_t)), 40);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(49 * sizeof(uint64_t), sizeof(uint64_t)), 49);
    b.ensure_bytes(400);
    memcpy(b.cursor(), v.data() + 50, 400);
    b.commit();
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(0 * sizeof(uint64_t), sizeof(uint64_t)), 0);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(40 * sizeof(uint64_t), sizeof(uint64_t)), 40);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(49 * sizeof(uint64_t), sizeof(uint64_t)), 49);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(50 * sizeof(uint64_t), sizeof(uint64_t)), 50);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(73 * sizeof(uint64_t), sizeof(uint64_t)), 73);
    ASSERT_EQ(*b.buffer().template ptr_cast<uint64_t>(99 * sizeof(uint64_t), sizeof(uint64_t)), 99);
}

TEST(Cursor, Values) {
    using namespace arcticdb;
    // test_cursor_backing< std::vector<uint8_t>>();
    // test_cursor_backing<Buffer>();
    test_cursor_backing<ChunkedBuffer>();
}
