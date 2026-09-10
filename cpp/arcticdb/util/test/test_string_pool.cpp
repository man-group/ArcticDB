/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h> // googletest header file
#include <unordered_map>
#include <fmt/format.h>

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/util/timer.hpp>

using namespace arcticdb;
#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

TEST(StringPool, MultipleReadWrite) {
    StringPool pool;

    const size_t VectorSize = 0x100;
    init_random(32);
    auto strings = random_string_vector(VectorSize);
    using map_t = std::unordered_map<std::string, position_t>;
    map_t positions;

    for (auto& s : strings) {
        OffsetString str = pool.get(std::string_view(s));
        map_t::const_iterator it;
        if ((it = positions.find(s)) != positions.end())
            ASSERT_EQ(str.offset(), it->second);
        else
            positions.try_emplace(s, str.offset());
    }

    const size_t NumTests = 100;
    for (size_t i = 0; i < NumTests; ++i) {
        auto& s = strings[random_int() & (VectorSize - 1)];
        StringPool::StringType comp_fs(s.data(), s.size());
        OffsetString str = pool.get(s.data(), s.size());
        ASSERT_EQ(str.offset(), positions[s]);
        auto view = pool.get_view(str.offset());
        StringPool::StringType fs(view.data(), view.size());
        ASSERT_EQ(fs, comp_fs);
    }
}

// The pool stores its keys as views. They must be rebound to the copies it owns, or a later lookup
// reads freed memory. Enough strings to span several blocks and force many rehashes.
TEST(StringPool, KeysDoNotAliasCallerBuffers) {
    StringPool pool;

    const size_t num_strings = 20000;
    std::vector<position_t> offsets;
    offsets.reserve(num_strings);
    for (size_t idx = 0; idx < num_strings; ++idx) {
        const std::string transient = fmt::format("string_number_{}", idx);
        offsets.emplace_back(pool.get(std::string_view(transient)).offset());
    }
    ASSERT_GT(pool.num_blocks(), 1);

    for (size_t idx = 0; idx < num_strings; ++idx) {
        const std::string expected = fmt::format("string_number_{}", idx);
        ASSERT_EQ(pool.get_view(offsets[idx]), expected);
        ASSERT_EQ(pool.get(std::string_view(expected)).offset(), offsets[idx]);
        ASSERT_EQ(pool.get(expected.data(), expected.size()).offset(), offsets[idx]);
    }
}

TEST(StringPool, DeduplicationEdgeCases) {
    StringPool pool;

    const auto empty = pool.get(std::string_view(""));
    ASSERT_EQ(pool.get(std::string_view("")).offset(), empty.offset());
    ASSERT_EQ(pool.get_view(empty.offset()), "");

    // Longer than the inline StringHead, and a prefix of it, so neither can be confused for the other
    const std::string long_string(1000, 'x');
    const auto long_offset = pool.get(std::string_view(long_string)).offset();
    const auto prefix_offset = pool.get(std::string_view(long_string).substr(0, 999)).offset();
    ASSERT_NE(long_offset, prefix_offset);
    ASSERT_EQ(pool.get_view(long_offset).size(), 1000);
    ASSERT_EQ(pool.get_view(prefix_offset).size(), 999);

    // Embedded nulls are part of the string
    const std::string_view with_null("a\0b", 3);
    const auto null_offset = pool.get(with_null).offset();
    ASSERT_NE(null_offset, pool.get(std::string_view("a")).offset());
    ASSERT_EQ(pool.get_view(null_offset), with_null);
}

TEST(StringPool, WithoutDeduplication) {
    StringPool pool;

    const auto first = pool.get(std::string_view("abc"), false);
    const auto second = pool.get(std::string_view("abc"), false);
    ASSERT_NE(first.offset(), second.offset());
    ASSERT_EQ(pool.get_view(first.offset()), "abc");
    ASSERT_EQ(pool.get_view(second.offset()), "abc");

    // Deduplicated inserts of the same string are still stable with one another
    const auto deduplicated = pool.get(std::string_view("abc"));
    ASSERT_EQ(pool.get(std::string_view("abc")).offset(), deduplicated.offset());
    ASSERT_EQ(pool.get_view(deduplicated.offset()), "abc");
}

TEST(StringPool, StressTest) {
    StringPool pool;

    const size_t VectorSize = 0x10000;
    init_random(42);
    auto strings = random_string_vector(VectorSize);

    auto temp = 0;
    std::string timer_name("ingestion_stress");
    interval_timer timer(timer_name);
    for (auto& s : strings) {
        OffsetString str = pool.get(std::string_view(s));
        temp += str.offset();
    }
    std::cout << temp << std::endl;
    timer.stop_timer(timer_name);
    GTEST_COUT << " " << timer.display_all() << std::endl;
}
//
// TEST(StringPool, BitMagicTest) {
//    bm::bvector<>   bv;
//    bv[10] = true;
//    GTEST_COUT << "done" << std::endl;
//}
