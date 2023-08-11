/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <bitmagic/bm.h>
#include <bitmagic/bmserial.h>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/bitset.hpp>

#include <arcticdb/util/test/generators.hpp>

TEST(BitMagic, Basic) {
    using namespace arcticdb;
    util::BitMagic bv;
    bv[0] = true;
    bv[3] = true;

    auto count = bv.count_range(0,0);
    ASSERT_EQ(count, 1);
    count = bv.count_range(0,3);
    ASSERT_EQ(count, 2);
    auto num = bv.get_first();
    while(num) {
        num = bv.get_next(num);
    }
}

TEST(BitMagic, DensifyAndExpand) {
    using namespace arcticdb;
    std::vector<float> sample_data;
    const size_t SPARSE_ELEMENTS = 1000;
    util::BitMagic bv;
    size_t n_dense = 0;
    for (size_t idx = 0; idx < SPARSE_ELEMENTS; idx++) {
        if (idx % 2 == 0) {
            sample_data.push_back(0.0);
            bv[idx] = false;
        } else {
            ++n_dense;
            sample_data.push_back(float(idx));
            bv[idx] = true;
        }
    }
    auto dense_buffer = ChunkedBuffer::presized(sizeof(float) * n_dense);

    auto *ptr = reinterpret_cast<uint8_t *>(&sample_data[0]);

    arcticdb::util::densify_buffer_using_bitmap<float>(bv, dense_buffer, ptr);

    auto *dense_array = reinterpret_cast<float *>(dense_buffer.data());

    GTEST_ASSERT_EQ(*dense_array, sample_data[1]);
    ++dense_array;
    GTEST_ASSERT_EQ(*dense_array, sample_data[3]);
    Buffer sparse_buffer;
    sparse_buffer.ensure(sizeof(float) * SPARSE_ELEMENTS);
    memset(sparse_buffer.data(), 0, sparse_buffer.bytes());

    // Now expand it back
    arcticdb::util::expand_dense_buffer_using_bitmap<float>(bv, dense_buffer.data(), sparse_buffer.data());
    auto *sparse_array = reinterpret_cast<float *>(sparse_buffer.data());

    for (auto &data: sample_data) {
        GTEST_ASSERT_EQ(data, *sparse_array);
        ++sparse_array;
    }
}
