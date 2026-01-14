/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>

#include <map>
#include <string>

RC_GTEST_PROP(StringPool, WriteAndRead, (const std::map<size_t, std::string>& input)) {
    using namespace arcticdb;
    StringPool pool;
    std::unordered_map<size_t, OffsetString> strings;
    for (auto& item : input) {
        strings.try_emplace(item.first, pool.get(item.second));
    }

    for (auto& stored : strings) {
        const std::string_view view(stored.second);
        auto it = input.find(stored.first);
        RC_ASSERT(view == it->second);
    }
}