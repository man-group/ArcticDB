/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/column_data_random_accessor.hpp>
#include <arcticdb/util/test/rapidcheck.hpp>

// Tricky to construct columns using a ChunkedBuffer with the testing size of 64 bytes, so only use rapidcheck for
// single block tests, and test regualr and irregular accessors in test_column_data_random_accessor.cpp
RC_GTEST_PROP(ColumnDataRandomAccessor, DenseSingleBlock, (const std::vector<int64_t>& input)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto n = input.size();
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), n, AllocationType::PRESIZED, Sparsity::NOT_PERMITTED);
    RC_ASSERT(column.num_blocks() == 1);
    memcpy(column.ptr(), input.data(), n * sizeof(int64_t));

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    for (size_t idx = 0; idx < n; ++idx) {
        RC_ASSERT(accessor.at(idx) == input[idx]);
    }
}

RC_GTEST_PROP(ColumnDataRandomAccessor, SparseSingleBlock, (const std::vector<int64_t>& input)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto n = input.size();
    auto mask = *rc::gen::container<std::vector<bool>>(n, rc::gen::arbitrary<bool>());
    // The last value in the bitset will always be true in a sparse column
    mask[n - 1] = true;
    auto on_bits = std::count(mask.begin(), mask.end(), true);
    util::BitSet sparse_map;
    for (size_t idx = 0; idx < mask.size(); ++idx) {
        if (mask[idx]) {
            sparse_map.set_bit(idx);
        }
    }

    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), on_bits, AllocationType::PRESIZED, Sparsity::PERMITTED);
    RC_ASSERT(column.num_blocks() == 1);
    memcpy(column.ptr(), input.data(), on_bits * sizeof(int64_t));
    column.set_sparse_map(std::move(sparse_map));

    auto column_data = column.data();
    auto accessor = random_accessor<TDT>(&column_data);
    size_t input_idx{0};
    for (size_t idx = 0; idx < n; ++idx) {
        if (mask[idx]) {
            RC_ASSERT(accessor.at(idx) == input[input_idx++]);
        }
    }
}