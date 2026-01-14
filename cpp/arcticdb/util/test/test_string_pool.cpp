/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h> // googletest header file
#include <unordered_map>

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
