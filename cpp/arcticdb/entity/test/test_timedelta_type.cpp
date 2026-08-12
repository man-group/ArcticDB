/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/types_proto.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/sparse_utils.hpp>

#include <gtest/gtest.h>

using namespace arcticdb;
using namespace arcticdb::entity;

namespace {
constexpr auto timedelta = DataType::TIMEDELTA_NS64;
constexpr auto timestamp_type = DataType::NANOSECONDS_UTC64;

TypeDescriptor scalar(DataType data_type) { return TypeDescriptor{data_type, Dimension::Dim0}; }
} // namespace

TEST(TimedeltaType, ProtoValueTypeMatches) {
    ASSERT_EQ(
            static_cast<int>(ValueType::TIMEDELTA),
            static_cast<int>(arcticdb::proto::descriptors::TypeDescriptor::TIMEDELTA)
    );
}

TEST(TimedeltaType, Predicates) {
    ASSERT_TRUE(is_timedelta_type(timedelta));
    ASSERT_TRUE(has_nat_sentinel(timedelta));
    ASSERT_TRUE(has_nat_sentinel(timestamp_type));

    ASSERT_FALSE(is_time_type(timedelta));
    ASSERT_FALSE(is_timedelta_type(timestamp_type));
    ASSERT_FALSE(is_numeric_type(timedelta));
    ASSERT_FALSE(is_integer_type(timedelta));
    ASSERT_FALSE(is_sequence_type(timedelta));
    ASSERT_FALSE(is_bool_type(timedelta));
    ASSERT_FALSE(is_empty_type(timedelta));
    ASSERT_FALSE(is_floating_point_type(timedelta));
}

TEST(TimedeltaType, Naming) {
    ASSERT_EQ(datatype_to_str(timedelta), "TIMEDELTA_NS64");
    ASSERT_EQ(value_type_to_str(ValueType::TIMEDELTA), "TIMEDELTA");
    ASSERT_EQ(get_dtype_specifier(timedelta), 'm');
    ASSERT_EQ(get_value_type('m'), ValueType::TIMEDELTA);
    ASSERT_EQ(get_type_size(timedelta), 8u);
}

TEST(TimedeltaType, VisitTypeDispatches) {
    bool visited = false;
    details::visit_type(timedelta, [&visited](auto tag) {
        using TagType = std::decay_t<decltype(tag)>;
        if constexpr (is_timedelta_type(TagType::data_type)) {
            static_assert(std::is_same_v<typename TagType::raw_type, timestamp>);
            visited = true;
        }
    });
    ASSERT_TRUE(visited);
}

TEST(TimedeltaType, ProtoRoundTrip) {
    arcticdb::proto::descriptors::TypeDescriptor proto;
    set_data_type(timedelta, proto);
    ASSERT_EQ(type_desc_from_proto(proto).data_type(), timedelta);
}

TEST(TimedeltaType, TypePromotion) {
    ASSERT_TRUE(is_valid_type_promotion_to_target(scalar(timedelta), scalar(timedelta)));
    ASSERT_TRUE(is_valid_type_promotion_to_target(scalar(DataType::EMPTYVAL), scalar(timedelta)));

    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(timedelta), scalar(timestamp_type)));
    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(timestamp_type), scalar(timedelta)));
    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(timedelta), scalar(DataType::INT64)));
    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(DataType::INT64), scalar(timedelta)));
    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(timedelta), scalar(DataType::FLOAT64)));
    ASSERT_FALSE(is_valid_type_promotion_to_target(scalar(timedelta), scalar(DataType::EMPTYVAL)));
}

TEST(TimedeltaType, CommonType) {
    ASSERT_EQ(has_valid_common_type(scalar(timedelta), scalar(timedelta)), scalar(timedelta));
    ASSERT_EQ(has_valid_common_type(scalar(DataType::EMPTYVAL), scalar(timedelta)), scalar(timedelta));
    ASSERT_FALSE(has_valid_common_type(scalar(timedelta), scalar(timestamp_type)).has_value());
    ASSERT_FALSE(has_valid_common_type(scalar(timedelta), scalar(DataType::INT64)).has_value());
    ASSERT_FALSE(has_valid_common_type(scalar(timedelta), scalar(DataType::UINT64)).has_value());
}

TEST(TimedeltaType, DefaultInitializeFillsNaT) {
    constexpr size_t num_rows = 4;
    std::array<timestamp, num_rows> buffer{1, 2, 3, 4};
    using Tag = ScalarTagType<DataTypeTag<DataType::TIMEDELTA_NS64>>;
    util::default_initialize<Tag>(reinterpret_cast<uint8_t*>(buffer.data()), num_rows * sizeof(timestamp));
    for (const auto value : buffer) {
        ASSERT_EQ(value, NaT);
    }
}
