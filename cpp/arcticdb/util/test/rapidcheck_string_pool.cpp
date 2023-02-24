/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>

#include <map>
#include <string>

RC_GTEST_PROP(StringPool, WriteAndRead, (const std::map<size_t, std::string> &input)) {
    using namespace arcticdb;
    StringPool pool;
    std::unordered_map<size_t, OffsetString> strings;
    for (auto &item : input) {
        strings.insert(std::make_pair(item.first, pool.get(item.second)));
    }

    for (auto &stored : strings) {
        const std::string_view view(stored.second);
        auto it = input.find(stored.first);
        RC_ASSERT(view == it->second);
    }
}