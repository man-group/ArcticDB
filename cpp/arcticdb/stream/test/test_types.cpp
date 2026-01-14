/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/index.hpp>

#include <gtest/gtest.h>

#include <fmt/format.h>
#include <iostream>
#include <type_traits>

#define GTEST_COUT std::cerr << "[          ] [ INFO ]"
#define PRINT_TYPE(TYPE)                                                                                               \
    GTEST_COUT << fmt::format("{}: {}", datatype_to_str(DataType::TYPE), static_cast<int>(DataType::TYPE)) << std::endl;

TEST(Types, Print) {
    using namespace arcticdb;
    PRINT_TYPE(UINT8)
    PRINT_TYPE(UINT16)
    PRINT_TYPE(UINT32)
    PRINT_TYPE(UINT64)
    PRINT_TYPE(INT8)
    PRINT_TYPE(INT16)
    PRINT_TYPE(INT32)
    PRINT_TYPE(INT64)
    PRINT_TYPE(FLOAT32)
    PRINT_TYPE(FLOAT64)
    PRINT_TYPE(BOOL8)
    PRINT_TYPE(NANOSECONDS_UTC64)
    PRINT_TYPE(ASCII_FIXED64)
    PRINT_TYPE(ASCII_DYNAMIC64)
    PRINT_TYPE(UTF_FIXED64)
    PRINT_TYPE(UTF_DYNAMIC64)
}

TEST(TickStreamDesc, FromFields) {
    using namespace arcticdb::entity;
    using namespace arcticdb;
    StreamDescriptor tsd{stream_descriptor(
            NumericId{123},
            stream::TimeseriesIndex::default_index(),
            {scalar_field(DataType::UINT8, "uint8"), scalar_field(DataType::INT8, "int8")}
    )};
    ASSERT_EQ(
            fmt::format("{}", tsd),
            "TSD<tsid=123, idx=IDX<size=1, kind=T>, fields=FD<name=time, type=TD<type=NANOSECONDS_UTC64, dim=0>, "
            "idx=0>, FD<name=uint8, type=TD<type=UINT8, dim=0>, idx=1>, FD<name=int8, type=TD<type=INT8, dim=0>, "
            "idx=2>>"
    );
}

TEST(DataTypeVisit, VisitTag) {
    using namespace arcticdb::entity;
    using namespace arcticdb;
    TypeDescriptor td(DataType::UINT8, 1);
    td.visit_tag([&](auto type_desc_tag) {
        auto td2 = static_cast<TypeDescriptor>(type_desc_tag);
        ASSERT_EQ(td, td2);
        using TD = TypeDescriptorTag<DataTypeTag<DataType::UINT8>, DimensionTag<Dimension::Dim1>>;
        bool b = std::is_same_v<TD, std::decay_t<decltype(type_desc_tag)>>;
        bool c = std::is_same_v<TD::DataTypeTag::raw_type, uint8_t>;
        ASSERT_TRUE(b);
        ASSERT_TRUE(c);
    });
}
