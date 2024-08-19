/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <array>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>

TEST(ChunkedBuffer, Iterator) {
    using namespace arcticdb;

    auto buff = CursoredBuffer<ChunkedBufferImpl<64>>{};
    for (auto i = 0; i < 10000; ++i) {
        buff.ensure<uint64_t>();
        buff.typed_cursor<uint64_t>() = i;
        buff.commit();
    }

    auto it = buff.buffer().iterator(8);
    uint64_t count = 0;
    while (!it.finished()) {
        ASSERT_EQ(*reinterpret_cast<uint64_t *>(it.value()), count++);
        it.next();
    }

    ASSERT_EQ(count, 10000);
}
TEST(ChunkedBuffer, Split) {
    using namespace arcticdb;
    std::array<uint8_t, 17> input{1, 0, 0, 2, 3, 4, 5, 1, 2, 6, 4, 5, 6, 2, 3, 4, 4};
    auto chunk_size = 5;
    auto split_size = 7;
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    auto n = input.size();
    auto top = n - (n % chunk_size);
    for (size_t i = 0; i < top; i += chunk_size) {
        cb.ensure_bytes(chunk_size);
        memcpy(cb.cursor(), &input[i], chunk_size);
        cb.commit();
    }

    auto buffers = ::arcticdb::split(cb.buffer(), split_size);
    auto buf = buffers.begin();
    for (size_t i = 0; i < top; ++i) {
        const auto where = i % split_size;
        ASSERT_NE(buf, buffers.end());
        ASSERT_EQ(buf->cast<uint8_t>(where), input[i]);
        if ((i + 1) % split_size == 0)
            ++buf;
    }
}
