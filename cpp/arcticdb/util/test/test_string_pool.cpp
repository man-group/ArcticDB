/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h> // googletest header file
#include <cstring>
#include <memory>
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
// A clone's dedup map must refer to the clone's own string storage, not the source's. Here the source's
// bytes are changed in place after cloning: the clone must still find "alpha" and must not see "omega".
TEST(StringPool, CloneKeysDoNotAliasSource) {
    StringPool source;
    const auto alpha = source.get(std::string_view{"alpha"}).offset();
    const auto beta = source.get(std::string_view{"beta"}).offset();
    auto clone = source.clone();

    auto source_bytes = source.get_view(alpha);
    std::memcpy(const_cast<char*>(source_bytes.data()), "omega", source_bytes.size());

    EXPECT_EQ(clone->get(std::string_view{"alpha"}).offset(), alpha);
    EXPECT_EQ(clone->get(std::string_view{"beta"}).offset(), beta);
    const auto omega = clone->get(std::string_view{"omega"}).offset();
    EXPECT_NE(omega, alpha);
    EXPECT_EQ(clone->get_view(omega), "omega");
    EXPECT_EQ(clone->get_view(alpha), "alpha");
}

// A clone that outlives its source and is then deduplicated into. Before the fix the clone's map keys
// pointed into the destroyed source's blocks: AddressSanitizer reports heap-use-after-free, and on macOS
// MallocScribble=1 or the reallocation below makes the lookups miss.
TEST(StringPool, CloneOutlivesSource) {
    std::vector<std::string> strings;
    for (size_t i = 0; i < 1000; ++i)
        strings.emplace_back("string-" + std::to_string(i) + "-" + std::string(i % 40, 'x'));

    std::vector<position_t> offsets;
    std::shared_ptr<StringPool> clone;
    {
        StringPool source;
        for (const auto& s : strings)
            offsets.push_back(source.get(std::string_view{s}).offset());
        clone = source.clone();
    }
    // Reallocate and scribble over memory of similar sizes so the freed source blocks are likely to be reused.
    std::vector<std::unique_ptr<char[]>> churn;
    for (size_t size = 64; size <= (size_t{1} << 20); size *= 2) {
        for (size_t i = 0; i < 8; ++i) {
            churn.emplace_back(new char[size]);
            std::memset(churn.back().get(), '#', size);
        }
    }

    const auto size_before = clone->size();
    for (size_t i = 0; i < strings.size(); ++i)
        ASSERT_EQ(clone->get(std::string_view{strings[i]}).offset(), offsets[i]) << strings[i];
    EXPECT_EQ(clone->size(), size_before);
}

//
// TEST(StringPool, BitMagicTest) {
//    bm::bvector<>   bv;
//    bv[10] = true;
//    GTEST_COUT << "done" << std::endl;
//}
