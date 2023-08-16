/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <limits>

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/test/test_utils.hpp>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/rapidcheck.hpp>

struct ColumnModel {
    std::vector<uint64_t> data;
};

struct ColumnAppend : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void apply(ColumnModel &s0) const override {
        s0.data.push_back(value_);
    }

    void run(const ColumnModel &, arcticdb::Column &sut) const override {
        auto next_row_id = sut.last_row() + 1;
        sut.set_scalar<uint64_t>(next_row_id, value_);
        RC_ASSERT(*sut.ptr_cast<uint64_t>(next_row_id, sizeof(uint64_t)) == value_);
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};

struct ColumnLowerBound : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void run(const ColumnModel& m, arcticdb::Column &sut) const override {
        using namespace arcticdb;
        using TagType = TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>;
        const auto model_it = std::lower_bound(std::begin(m.data), std::end(m.data), value_);
        const auto sut_it = std::lower_bound(sut.template begin<TagType>(), sut.template end<TagType>(), value_);
        RC_ASSERT(std::distance(std::begin(m.data), model_it) == std::distance(sut.template begin<TagType>(), sut_it));
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};

struct ColumnUpperBound : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t value_;

    void run(const ColumnModel& m, arcticdb::Column &sut) const override {
        using namespace arcticdb;
        using TagType = TypeDescriptorTag<DataTypeTag<DataType::UINT64>, DimensionTag<Dimension::Dim0>>;
        const auto model_it = std::upper_bound(std::begin(m.data), std::end(m.data), value_);
        const auto sut_it = std::upper_bound(sut.template begin<TagType>(), sut.template end<TagType>(), value_);
        RC_ASSERT(std::distance(std::begin(m.data), model_it) == std::distance(sut.template begin<TagType>(), sut_it));
    }

    void show(std::ostream &os) const override {
        os << "Append(" << value_ << ")";
    }
};
struct ColumnRead : rc::state::Command<ColumnModel, arcticdb::Column> {
    uint64_t position_;

    void checkPreconditions(const ColumnModel &s0) const override {
        RC_PRE(position_ < s0.data.size());
    }

    void run(const ColumnModel &s0, arcticdb::Column &sut) const override {
        RC_ASSERT(*sut.ptr_cast<uint64_t>(position_, sizeof(uint64_t)) == s0.data[position_]);
    }

    void show(std::ostream &os) const override {
        os << "Get(" << position_ << ")";
    }
};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

RC_GTEST_PROP(Column, Rapidcheck, ()) {
    ColumnModel initial_state;
    arcticdb::Column sut(TypeDescriptor(DataType::UINT64, Dimension::Dim0), 0, false, false);
    rc::state::check(initial_state,
                     sut,
                     &rc::state::gen::execOneOf<ColumnAppend, ColumnRead, ColumnLowerBound, ColumnUpperBound>);
}

RC_GTEST_PROP(Column, TruncateDense, (const std::vector<int64_t> &input)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto n = input.size();
    const auto start_row = *rc::gen::inRange(size_t(0), n - 1);
    const auto end_row = *rc::gen::inRange(start_row + 1, n);
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_shared<Column>(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(size_t idx = 0; idx < n; ++idx) {
        column->set_scalar<int64_t>(idx, input[idx]);
    }
    auto truncated_column = Column::truncate(column, start_row, end_row);
    auto truncated_column_idx = 0;
    for (auto input_idx = start_row; input_idx < end_row; input_idx++, truncated_column_idx++) {
        auto left = truncated_column->scalar_at<int64_t>(truncated_column_idx);
        auto right = input[input_idx];
        RC_ASSERT(left.has_value());
        RC_ASSERT(*left == right);
    }
}

RC_GTEST_PROP(Column, TruncateSparse, (const std::vector<int64_t> &input)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto n = input.size();
    auto start_row = *rc::gen::inRange(size_t(0), n - 1);
    auto end_row = *rc::gen::inRange(start_row + 1, n);
    auto mask = *rc::gen::container<std::vector<bool>>(n, rc::gen::arbitrary<bool>());
    // The last value in the bitset will always be true in a sparse column
    mask[n - 1] = true;
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto column = std::make_shared<Column>(static_cast<TypeDescriptor>(TDT{}), 0, false, true);
    for(size_t idx = 0; idx < n; ++idx) {
        if (mask[idx]) {
            column->set_scalar<int64_t>(idx, input[idx]);
        }
    }
    column->set_row_data(n - 1);
    auto truncated_column = Column::truncate(column, start_row, end_row);
    auto truncated_column_idx = 0;
    for (auto input_idx = start_row; input_idx < end_row; input_idx++, truncated_column_idx++) {
        auto left = truncated_column->scalar_at<int64_t>(truncated_column_idx);
        if (mask[input_idx]) {
            auto right = input[input_idx];
            RC_ASSERT(left.has_value());
            RC_ASSERT(*left == right);
        } else {
            RC_ASSERT_FALSE(left.has_value());
        }
    }
}

RC_GTEST_PROP(Column, SearchSorted, (const std::vector<int64_t>& input, const int64_t& value_to_find)) {
    using namespace arcticdb;
    RC_PRE(input.size() > 0u);
    auto sorted_input = input;
    std::sort(sorted_input.begin(), sorted_input.end());
    auto n = sorted_input.size();
    auto smallest_value = sorted_input[0];
    auto largest_value = sorted_input[n - 1];
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for (size_t idx = 0; idx < n; ++idx) {
        column.set_scalar<int64_t>(idx, sorted_input[idx]);
    }
    auto left_idx = column.search_sorted<int64_t>(value_to_find, false);
    auto right_idx = column.search_sorted<int64_t>(value_to_find, true);
    RC_ASSERT(left_idx >= 0);
    RC_ASSERT(left_idx <= n);
    RC_ASSERT(right_idx >= 0);
    RC_ASSERT(right_idx <= n);
    if (left_idx == 0) {
        RC_ASSERT(value_to_find <= smallest_value);
    } else if (left_idx == n) {
        RC_ASSERT(value_to_find > largest_value);
    } else {
        RC_ASSERT(value_to_find > sorted_input[left_idx - 1]);
        RC_ASSERT(value_to_find <= sorted_input[left_idx]);
    }
    if (right_idx == 0) {
        RC_ASSERT(value_to_find <= smallest_value);
    } else if (right_idx == n) {
        RC_ASSERT(value_to_find >= largest_value);
    } else {
        RC_ASSERT(value_to_find >= sorted_input[right_idx - 1]);
        RC_ASSERT(value_to_find < sorted_input[right_idx]);
    }
}

#pragma GCC diagnostic pop