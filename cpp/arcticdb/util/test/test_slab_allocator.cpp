/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <unordered_set>
#include <future>
#include <chrono>
#include <iostream>
#include <vector>
#include <cstring>
#include <latch>
#include <arcticdb/util/slab_allocator.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>

using namespace arcticdb;

template<typename MemoryChunk>
using pointer_set = std::unordered_set<typename MemoryChunk::pointer>;

// We limit the number of concurrent threads to 8 to avoid the slab allocator poor performance with large amounts of
// threads:
// https://manwiki.maninvestments.com/display/AlphaTech/Slab+Allocator+poor+multi-threaded+performance+with+aggressive+allocations
size_t num_threads = std::min(std::thread::hardware_concurrency() - 1, 8u);
std::size_t const num_blocks_per_thread = 10000;

template<typename MemoryChunk>
pointer_set<MemoryChunk> call_alloc(MemoryChunk& mc, std::size_t n, int64_t& execution_time_ms) {

    pointer_set<MemoryChunk> mcps;
    mcps.reserve(n);

    auto time_begin = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < n; ++i)
        mcps.insert(mc.allocate());

    auto time_end = std::chrono::high_resolution_clock::now();
    execution_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_end - time_begin).count();
    return mcps;
}

template<typename MemoryChunk>
void check_sets(const pointer_set<MemoryChunk>& s1, const pointer_set<MemoryChunk>& s2) {
    auto end = std::cend(s2);
    for (auto* p : s1)
        if (s2.find(p) != end)
            throw std::runtime_error("two sets have the same address");
}

template<typename MemoryChunk>
void run_test(MemoryChunk& mc, unsigned int K) {
    std::vector<int64_t> execution_times(num_threads);
    int64_t avg = 0;
    for (size_t k = 0; k < K; ++k) {
        std::vector<std::future<pointer_set<MemoryChunk>>> v;
        for (size_t i = 0; i < num_threads; ++i) {
            v.emplace_back(std::async(
                    std::launch::async,
                    call_alloc<MemoryChunk>,
                    std::ref(mc),
                    num_blocks_per_thread,
                    std::ref(execution_times[i])
            ));
        }
        for (auto& t : v)
            t.wait();

        for (size_t i = 0; i < num_threads; ++i) {
            avg += execution_times[i];
        }

        std::vector<pointer_set<MemoryChunk>> comparisons;
        for (size_t i = 0; i < num_threads; ++i) {
            comparisons.emplace_back(std::move(v[i].get()));
        }

        std::vector<std::future<void>> exceptions;
        for (size_t i = 0; i < num_threads; ++i) {
            for (size_t j = i + 1; j < num_threads; ++j) {
                exceptions.emplace_back(std::async(
                        std::launch::async, check_sets<MemoryChunk>, std::ref(comparisons[i]), std::ref(comparisons[j])
                ));
            }
        }
        for (auto& f : exceptions)
            f.get();

        for (const auto& pointers : comparisons) {
            for (auto* p : pointers) {
                mc.deallocate(p);
            }
        }
    }
    std::cout << "Average execution time: " << avg / (num_threads * K) << "ms\n";
}

TEST(SlabAlloc, CacheLine128) {
    SKIP_WIN("Slab allocator not supported");
    SlabAllocator<int, 128> mc128{num_blocks_per_thread * num_threads};
    std::cout << "BEGIN: mc128 tests\n";
    for (size_t i = 0; i < 5; i++)
        run_test(mc128, 100);
    std::cout << "END:   mc128 tests\n\n";
}

TEST(SlabAlloc, CacheLine32) {
    SKIP_WIN("Slab allocator not supported");
    SlabAllocator<int, 32> mc32{num_blocks_per_thread * num_threads};
    std::cout << "BEGIN: mc32 tests\n";
    for (size_t i = 0; i < 5; i++)
        run_test(mc32, 100);
    std::cout << "END:   mc32 tests\n";
}

TEST(SlabAlloc, PageSizeMem) {
    SKIP_WIN("Slab allocator not supported");
    SlabAllocator<std::byte[4096], 128> mc_page{num_blocks_per_thread * num_threads};
    std::cout << "BEGIN: mc_page tests\n";
    for (size_t i = 0; i < 5; i++)
        run_test(mc_page, 100);
    std::cout << "END:   mc_page tests\n\n";
}

TEST(SlabAlloc, Integer) {
    SlabAllocator<int, 128> mc128{1};
    for (size_t i = 0; i < 100; i++) {
        auto p = mc128.allocate();
        ASSERT_NE(p, nullptr);
        *p = i;
        ASSERT_EQ(*p, i);
        mc128.deallocate(p);
    }
}

TEST(SlabAlloc, Char32) {
    using SlabAllocatorType = SlabAllocator<char[32], 128>;
    SlabAllocatorType mc128{1};
    for (size_t i = 0; i < 100; i++) {
        auto p = reinterpret_cast<char*>(mc128.allocate());
        ASSERT_NE(p, nullptr);
        char src[32] = "1234567812345678123456781234567";
        strcpy(p, src);
        ASSERT_EQ(0, strcmp(p, src));
        mc128.deallocate(reinterpret_cast<SlabAllocatorType::pointer>(p));
    }
}

TEST(SlabAlloc, Bytes4096) {
    using SlabAllocatorType = SlabAllocator<std::byte[4096], 128>;
    SlabAllocatorType mc128{1};
    for (size_t i = 0; i < 100; i++) {
        auto p = reinterpret_cast<std::byte*>(mc128.allocate());
        ASSERT_NE(p, nullptr);
        memset(p, 0, 4096);
        for (size_t j = 0; j < 4096; j++) {
            ASSERT_EQ(p[j], std::byte('\0'));
        }
        mc128.deallocate(reinterpret_cast<SlabAllocatorType::pointer>(p));
    }
}

TEST(SlabAlloc, AddrInSlab) {
    SlabAllocator<std::byte[4096], 128> mc128{100};
    auto p = mc128.allocate();

    ASSERT_TRUE(mc128.is_addr_in_slab(p));
    ASSERT_TRUE(mc128.is_addr_in_slab(p + 50));
    ASSERT_TRUE(mc128.is_addr_in_slab(p + 99));
    ASSERT_FALSE(mc128.is_addr_in_slab(p + 100));
    ASSERT_FALSE(mc128.is_addr_in_slab(p + 101));
    ASSERT_FALSE(mc128.is_addr_in_slab(p + 1000));
}

using SlabAllocType = SlabAllocator<std::byte[4096], 64>;
std::vector<SlabAllocType::pointer> perform_allocations_with_latches(
        SlabAllocType& mc, size_t num, std::shared_ptr<std::latch> decrease_available_blocks_done,
        std::shared_ptr<std::latch> start_callbacks
) {
    std::vector<SlabAllocType::pointer> res;
    for (size_t i = 0; i < num; i++) {
        res.push_back(mc.allocate(decrease_available_blocks_done, start_callbacks));
    }
    return res;
}

std::vector<SlabAllocType::pointer> perform_allocations(SlabAllocType& mc, size_t num) {
    std::vector<SlabAllocType::pointer> res;
    for (size_t i = 0; i < num; i++) {
        res.push_back(mc.allocate());
    }
    return res;
}

TEST(SlabAlloc, MultipleThreads) {
    size_t cap = 10000;
    SlabAllocType mc(cap);
    size_t num_thds = 4;
    size_t rounds = 10;
    for (size_t k = 0; k < rounds; k++) {
        ASSERT_EQ(mc.get_approx_free_blocks(), cap);
        std::vector<std::future<std::vector<SlabAllocType::pointer>>> res;
        for (size_t i = 0; i < num_thds; i++) {
            res.emplace_back(std::async(std::launch::async, perform_allocations, std::ref(mc), cap / num_thds));
        }

        for (size_t i = 0; i < num_thds; i++) {
            for (auto& p : res[i].get()) {
                mc.deallocate(p);
            }
        }
        ASSERT_EQ(mc.get_approx_free_blocks(), cap);
    }
}

TEST(SlabAlloc, Callbacks) {
    size_t cap = 10000;
    size_t num_thds = 4;
    SlabAllocType mc(cap);
    std::atomic<int> cb_called = 0;
    mc.add_cb_when_full([&cb_called]() { cb_called.fetch_add(1); });
    std::vector<std::future<std::vector<SlabAllocType::pointer>>> res;
    for (size_t i = 0; i < num_thds; i++) {
        res.emplace_back(std::async(std::launch::async, perform_allocations, std::ref(mc), cap / num_thds));
    }
    std::vector<std::vector<SlabAllocType::pointer>> res2;
    for (size_t i = 0; i < num_thds; i++) {
        res2.emplace_back(res[i].get());
    }
    ASSERT_EQ(mc.get_approx_free_blocks(), 0);
    for (size_t i = 0; i < num_thds; i++) {
        for (auto& p : res2[i]) {
            mc.deallocate(p);
        }
    }
    // Monday 11143606523 Should be ASSERT_EQ(cb_called, 1) after bug is fixed
    ASSERT_GE(cb_called, 1);
    mc.allocate();
    ASSERT_FALSE(mc._get_cb_activated());
}

/**
 * Test for a race condition in the callbacks logic. Monday: 11143606523
 *
 * Capacity 20
 * Thread B: Allocate block 17 -> sees n=4, 20% left, so will try_changing_cb(false) later. Currently just finished
 *   try_decrease_available_blocks
 * Thread A: Allocate block 19 -> triggers slab_activate_cb_cutoff as only 2 blocks (10%) left. Sets cb_activated=true
 * Thread B: Set cb_activated = false
 * Thread A: Allocate block 20 -> triggers slab_activate_cb_cutoff as only 1 block left. Sets cb_activated=true
 **/
TEST(SlabAlloc, CallbacksRace) {
    size_t cap = 20;
    size_t num_thds = 3;
    SlabAllocType mc(cap);
    std::atomic cb_called = 0;
    mc.add_cb_when_full([&cb_called]() { cb_called.fetch_add(1); });
    perform_allocations(mc, 16);

    auto decrease_available_blocks_done = std::make_shared<std::latch>(1);
    auto start_callbacks = std::make_shared<std::latch>(1);

    auto thread_b = std::async(
            std::launch::async,
            perform_allocations_with_latches,
            std::ref(mc),
            1,
            decrease_available_blocks_done,
            start_callbacks
    );
    decrease_available_blocks_done->wait();

    std::vector<std::vector<SlabAllocType::pointer>> collected_allocations;

    auto thread_a_alloc = perform_allocations(mc, 2);
    collected_allocations.push_back(thread_a_alloc);

    start_callbacks->count_down(1);
    collected_allocations.emplace_back(thread_b.get());

    thread_a_alloc = perform_allocations(mc, 1);
    collected_allocations.push_back(thread_a_alloc);

    ASSERT_EQ(mc.get_approx_free_blocks(), 0);
    for (size_t i = 0; i < num_thds; i++) {
        for (auto& p : collected_allocations.at(i)) {
            mc.deallocate(p);
        }
    }
    // Monday 11143606523 Should be ASSERT_EQ(cb_called, 1) after bug is fixed
    ASSERT_EQ(cb_called, 2);

    mc.allocate();
    ASSERT_FALSE(mc._get_cb_activated());
}
