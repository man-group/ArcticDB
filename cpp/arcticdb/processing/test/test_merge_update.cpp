/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>
#include <stream/test/stream_test_common.hpp>
#include <arcticdb/entity/types.hpp>

using namespace arcticdb;

constexpr static std::array non_string_fields = {
        FieldRef(TypeDescriptor(DataType::INT8, Dimension::Dim0), "int8"),
        FieldRef(TypeDescriptor(DataType::UINT32, Dimension::Dim0), "uint32"),
        FieldRef(TypeDescriptor(DataType::BOOL8, Dimension::Dim0), "bool8"),
        FieldRef(TypeDescriptor(DataType::FLOAT32, Dimension::Dim0), "float32"),
        FieldRef(TypeDescriptor(DataType::NANOSECONDS_UTC64, Dimension::Dim0), "timestamp")
};

TEST(MergeUpdateUpdateTimeseries, SourceIndexMatchesAllSegments) {
    using stream::TimeseriesIndex;

    std::vector<std::string> on{};
    constexpr static auto strategy =
            MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::DO_NOTHING};
    StreamDescriptor source_descriptor =
            TimeseriesIndex::default_index().create_stream_descriptor("Source", non_string_fields);
    constexpr std::array<timestamp, 3> source_index{1, 12, 25};
    constexpr std::array<int8_t, 3> source_int8{10, 20, 30};
    constexpr std::array<uint32_t, 3> source_uint32{100, 200, 300};
    constexpr std::array source_bool{true, false, true};
    constexpr std::array source_float{11.1f, 22.2f, 33.3f};
    constexpr std::array<timestamp, 3> source_timestamp{1000, 2000, 3000};
    InputFrame source(
            std::move(source_descriptor),
            create_one_dimensional_tensors(
                    std::pair{source_int8, TypeDescriptor::scalar_type(DataType::INT8)},
                    std::pair{source_uint32, TypeDescriptor::scalar_type(DataType::UINT32)},
                    std::pair{source_bool, TypeDescriptor::scalar_type(DataType::BOOL8)},
                    std::pair{source_float, TypeDescriptor::scalar_type(DataType::FLOAT32)},
                    std::pair{source_timestamp, TypeDescriptor::scalar_type(DataType::NANOSECONDS_UTC64)}
            ),
            NativeTensor::one_dimensional_tensor(source_index, DataType::NANOSECONDS_UTC64)
    );
    [[maybe_unused]] MergeUpdateClause clause(
            std::move(on), strategy, std::make_shared<InputFrame>(std::move(source)), true
    );
}