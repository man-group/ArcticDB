/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/configs_map.hpp>


TEST(RuntimeConfig, PersistToProtobuf) {
    using namespace arcticdb;

    ConfigsMap test_map;
    test_map.set_int("int_value", 4);
    test_map.set_string("string_value", "value");
    test_map.set_double("double_value", 1.3);

    auto proto = test_map.to_proto();
    auto read_map = ConfigsMap::from_proto(proto);
    ASSERT_EQ(read_map.get_int("int_value"), 4);
    ASSERT_EQ(read_map.get_string("string_value"), "value");
    ASSERT_EQ(read_map.get_double("double_value"), 1.3);
}