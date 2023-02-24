/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/magic_num.hpp>

TEST(Allocator, Tracing) {
    using AllocType = arcticdb::AllocatorImpl<arcticdb::InMemoryTracingPolicy>;
    AllocType::clear();
    std::vector<std::pair<uint8_t*, arcticdb::entity::timestamp>> blocks;

    ASSERT_EQ(AllocType::allocated_bytes(), 0);
    blocks.push_back(AllocType::alloc(10));
    ASSERT_EQ(AllocType::allocated_bytes(), 10);
    blocks.push_back(AllocType::alloc(10));
    ASSERT_EQ(AllocType::allocated_bytes(), 20);

    auto last = *blocks.rbegin();
    AllocType::free(last);
    ASSERT_EQ(AllocType::allocated_bytes(), 10);
    blocks.pop_back();

    blocks.push_back(AllocType::alloc(30));
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
