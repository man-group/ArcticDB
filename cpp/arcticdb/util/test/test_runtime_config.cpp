/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/configs_map.hpp>
#include <gtest/gtest.h>

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

TEST(RuntimeConfig, PersistToProtobufCaseInsensitive) {
  using namespace arcticdb;

  ConfigsMap test_map;
  test_map.set_int("INT_value", 4);
  test_map.set_string("STRING_value", "value");
  test_map.set_double("DOUBLE_value", 1.3);

  auto proto = test_map.to_proto();
  auto read_map = ConfigsMap::from_proto(proto);
  ASSERT_EQ(read_map.get_int("int_VALUE"), 4);
  ASSERT_EQ(read_map.get_string("string_VALUE"), "value");
  ASSERT_EQ(read_map.get_double("double_VALUE"), 1.3);
}