/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include "gtest/gtest.h"
#include <arcticdb/util/test/rapidcheck.hpp>

#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>

#include <vector>
#include <algorithm>

TEST(ChunkedBuffer, Basic) {
    using namespace arcticdb;
    ChunkedBuffer cb;
    cb.ensure(4);
    cb.cast<uint64_t>(0) = std::numeric_limits<uint64_t>::max();
    auto out = cb.cast<uint64_t>(0);
    ASSERT_EQ(out, std::numeric_limits<uint64_t>::max());
}

RC_GTEST_PROP(ChunkedBuffer, ReadWriteRegular, (const std::vector<uint8_t> &input, uint8_t chunk_size)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    RC_PRE(chunk_size > 0u);
    RC_PRE(chunk_size < input.size());
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    auto n = input.size();
    auto top = n - (n % chunk_size);
    for (size_t i = 0; i < top; i += chunk_size) {
        cb.ensure_bytes(chunk_size);
        memcpy(cb.cursor(), &input[i], chunk_size);
        cb.commit();
    }

    for (size_t i = 0; i < top; i += chunk_size) {
        RC_ASSERT(cb.buffer().cast<uint8_t>(i) == input[i]);
        RC_ASSERT(*cb.buffer().ptr_cast<uint8_t>(i, sizeof(uint8_t)) == input[i]);
    }
}

RC_GTEST_PROP(ChunkedBuffer, SplitBuffer, (const std::vector<uint8_t> &input, uint8_t chunk_size, uint32_t split_size)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    RC_PRE(chunk_size > 0u);
    RC_PRE(split_size > 0u);
    RC_PRE(chunk_size < input.size());
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
        RC_ASSERT(buf != buffers.end());
        auto left = buf->cast<uint8_t>(where);
        auto right = input[i];
        auto& buf_obj = *buf;
        if(buf_obj.cast<uint8_t>(where) != input[i])
            log::version().info("Mismatch at {} ({}), {} != {}", i, where, left, right);
        RC_ASSERT(left == right);
        if(((i + 1) % split_size) == 0)
            ++buf;
    }
}

RC_GTEST_PROP(ChunkedBuffer, TruncateBuffer, (const std::vector<uint8_t> &input)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto n = input.size();
    const auto chunk_size = *rc::gen::inRange(size_t(1), n);
    auto top = n - (n % chunk_size);
    const auto start_byte = *rc::gen::inRange(size_t(0), top - 1);
    const auto end_byte = *rc::gen::inRange(start_byte + 1, top);
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    for (size_t i = 0; i < top; i += chunk_size) {
        cb.ensure_bytes(chunk_size);
        memcpy(cb.cursor(), &input[i], chunk_size);
        cb.commit();
    }

    auto buffer = ::arcticdb::truncate(cb.buffer(), start_byte, end_byte);
    auto buffer_idx = 0;
    for (auto input_idx = start_byte; input_idx < end_byte; input_idx++, buffer_idx++) {
        auto left = buffer.cast<uint8_t>(buffer_idx);
        auto right = input[input_idx];
        RC_ASSERT(left == right);
    }
}

RC_GTEST_PROP(ChunkedBuffer, ReadWriteIrregular, (const std::vector<std::vector<uint64_t>> &inputs)) {
    using namespace arcticdb;
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    for (auto &vec : inputs) {
        if (vec.empty())
            continue;

        auto data_size = vec.size() * sizeof(int64_t);
        cb.ensure_bytes(data_size);
        memcpy(cb.cursor(), vec.data(), data_size);
        cb.commit();
    }

    size_t count = 0;
    for (auto &vec : inputs) {
        for (auto val : vec) {
            auto pos = count * sizeof(uint64_t);
            RC_ASSERT(*cb.buffer().ptr_cast<uint64_t>(pos, sizeof(uint64_t)) == val);
            RC_ASSERT(cb.buffer().cast<uint64_t>(count) == val);
            ++count;
        }
    }
}

RC_GTEST_PROP(ChunkedBuffer,
              ReadWriteTransition,
              (const std::vector<std::vector<uint64_t>> &inputs, uint8_t regular_chunks)) {
    using namespace arcticdb;
    CursoredBuffer<ChunkedBufferImpl<64>> cb;
    for (uint8_t i = 0; i < regular_chunks; ++i) {
        cb.ensure_bytes(64);
        for (uint8_t j = 0; j < 64; ++j)
            cb.buffer().cast<uint8_t>((i * 64) + j) = (i * 64) + j;

        cb.commit();
    }

    for (auto &vec : inputs) {
        if (vec.empty())
            continue;

        auto data_size = vec.size() * sizeof(uint64_t);
        cb.ensure_bytes(data_size);
        memcpy(cb.cursor(), vec.data(), data_size);
        cb.commit();
    }

    uint8_t count = 0;
    for (uint8_t i = 0; i < regular_chunks; ++i) {
        for (uint8_t j = 0; j < 64; ++j) {
            RC_ASSERT(cb.buffer().cast<uint8_t>(count) == count);
            ++count;
        }
    }

    auto irregular_start_pos = regular_chunks * 64;
    size_t next_count = 0;
    for (auto &vec : inputs) {
        for (auto val : vec) {
            const auto pos = irregular_start_pos + (next_count * sizeof(uint64_t));
            RC_ASSERT(*cb.buffer().ptr_cast<uint64_t>(pos, sizeof(uint64_t)) == val);
            next_count++;
        }
    }
}

