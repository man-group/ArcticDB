/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/memory_tracing.hpp>

TEST(Allocator, Tracing) {
    using AllocType = arcticdb::AllocatorImpl<arcticdb::InMemoryTracingPolicy>;
    AllocType::clear();
    std::vector<std::pair<uint8_t*, arcticdb::entity::timestamp>> blocks;

    ASSERT_EQ(AllocType::allocated_bytes(), 0);
    blocks.emplace_back(AllocType::alloc(10));
    ASSERT_EQ(AllocType::allocated_bytes(), 10);
    blocks.emplace_back(AllocType::alloc(10));
    ASSERT_EQ(AllocType::allocated_bytes(), 20);

    auto last = *blocks.rbegin();
    AllocType::free(last);
    ASSERT_EQ(AllocType::allocated_bytes(), 10);
    blocks.pop_back();

    blocks.emplace_back(AllocType::alloc(30));
    ASSERT_EQ(AllocType::allocated_bytes(), 40);

    auto new_ptr = AllocType::realloc(blocks[0], 100);
    blocks[0] = new_ptr;
    ASSERT_EQ(AllocType::allocated_bytes(), 130);

    for(auto block : blocks)
        AllocType::free(block);

    ASSERT_EQ(AllocType::allocated_bytes(), 0);
    ASSERT_TRUE(AllocType::empty());
}

TEST(Allocator, PrintMemUsage) {
    arcticdb::log::memory().set_level(spdlog::level::trace);
    arcticdb::util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);
}
