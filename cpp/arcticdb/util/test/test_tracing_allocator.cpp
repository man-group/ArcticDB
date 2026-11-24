/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/magic_num.hpp>
#include <arcticdb/util/memory_tracing.hpp>
#include <arcticdb/util/allocator_backend.hpp>

#include <memory>
#include <vector>

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

    for (auto block : blocks)
        AllocType::free(block);

    ASSERT_EQ(AllocType::allocated_bytes(), 0);
    ASSERT_TRUE(AllocType::empty());
}

TEST(Allocator, PrintMemUsage) {
    arcticdb::log::memory().set_level(spdlog::level::debug);
    arcticdb::util::print_total_mem_usage(__FILE__, __LINE__, __FUNCTION__);

    auto summary = arcticdb::util::get_memory_use_summary();
#if defined(_WIN32) || defined(__APPLE__)
    ASSERT_EQ(summary.size.value_, 0);
    ASSERT_EQ(summary.resident.value_, 0);
    ASSERT_EQ(summary.max_resident.value_, 0);
    ASSERT_EQ(summary.shared.value_, 0);
    ASSERT_EQ(summary.text.value_, 0);
    ASSERT_EQ(summary.data_stack.value_, 0);
#else
    ASSERT_GT(summary.size.value_, 0);
    ASSERT_GT(summary.resident.value_, 0);
    ASSERT_GT(summary.max_resident.value_, 0);
    ASSERT_GT(summary.shared.value_, 0);
    ASSERT_GT(summary.text.value_, 0);
    ASSERT_GT(summary.data_stack.value_, 0);
#endif
}

#if defined(ARCTICDB_ALLOCATOR_MIMALLOC) || defined(ARCTICDB_ALLOCATOR_JEMALLOC) || defined(ARCTICDB_ALLOCATOR_TCMALLOC)
namespace {
#if defined(ARCTICDB_ALLOCATOR_JEMALLOC)
uint64_t jemalloc_thread_allocated() {
    uint64_t* allocated = nullptr;
    size_t size = sizeof(allocated);
    je_mallctl("thread.allocatedp", &allocated, &size, nullptr, 0);
    return *allocated;
}
#endif

// Calls allocate(size), checks that the backend served it, then passes the pointer to release.
template<typename Allocate, typename Release>
void expect_from_backend(size_t size, Allocate&& allocate, Release&& release) {
#if defined(ARCTICDB_ALLOCATOR_JEMALLOC)
    const auto before = jemalloc_thread_allocated();
    auto* ptr = allocate(size);
    EXPECT_GE(jemalloc_thread_allocated() - before, size);
#else
    auto* ptr = allocate(size);
    EXPECT_TRUE(arcticdb::allocator_backend::owns(ptr));
#endif
    release(ptr);
}
} // namespace

TEST(Allocator, UsesConfiguredBackend) {
    constexpr size_t size = 1 << 20;
#if !defined(__APPLE__) // macOS keeps operator new/delete on the system allocator
    // Explicit operator calls, because compilers may elide a new-expression whose result is unused
    expect_from_backend(
            size,
            [](size_t n) { return static_cast<uint8_t*>(::operator new(n)); },
            [](uint8_t* p) { ::operator delete(p); }
    );

    std::vector<uint8_t> vec;
    expect_from_backend(
            size,
            [&vec](size_t n) {
                vec.resize(n);
                return vec.data();
            },
            [&vec](uint8_t*) { std::vector<uint8_t>().swap(vec); }
    );
#endif

    expect_from_backend(
            size,
            [](size_t n) { return arcticdb::allocate_detachable_memory(n); },
            [](uint8_t* p) { arcticdb::free_detachable_memory(p, size); }
    );

    std::pair<uint8_t*, arcticdb::entity::timestamp> block;
    expect_from_backend(
            size,
            [&block](size_t n) {
                block = arcticdb::Allocator::alloc(n);
                block = arcticdb::Allocator::realloc(block, 2 * n);
                return block.first;
            },
            [&block](uint8_t*) { arcticdb::Allocator::free(block); }
    );
    arcticdb::Allocator::trim();
}
#endif
