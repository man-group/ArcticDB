/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include <arcticdb/util/claim_flags.hpp>

using namespace arcticdb::util;

TEST(ClaimFlags, ClaimsOnce) {
    ClaimFlags flags(3);
    ASSERT_EQ(flags.size(), 3);
    ASSERT_TRUE(flags.claim(1));
    ASSERT_FALSE(flags.claim(1));
    ASSERT_TRUE(flags.claim(0));
    ASSERT_FALSE(flags.claim(0));
    ASSERT_FALSE(flags.claim(1));
}

// Regression test for #3381. Neighbouring positions are claimed by different threads at the same
// time, so an implementation that packs the flags into shared words loses claims and a position
// ends up claimable a second time. That is what made read workers add an entity to the
// ComponentManager twice and abort on an EnTT assertion.
TEST(ClaimFlags, NeighbouringPositionsAreIndependent) {
    constexpr size_t num_positions = 1024;
    constexpr size_t num_threads = 8;
    constexpr size_t num_iterations = 200;

    for (size_t iteration = 0; iteration < num_iterations; ++iteration) {
        ClaimFlags flags(num_positions);
        std::atomic<size_t> claimed{0};
        std::atomic<size_t> ready{0};
        std::atomic<bool> go{false};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
            threads.emplace_back([&, thread_id]() {
                ++ready;
                while (!go.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                size_t local_claims{0};
                // Stride by the thread count so that adjacent positions are always claimed by
                // different threads, maximising the chance of a lost update within one word.
                for (size_t pos = thread_id; pos < num_positions; pos += num_threads) {
                    if (flags.claim(pos)) {
                        ++local_claims;
                    }
                }
                claimed += local_claims;
            });
        }
        while (ready.load() != num_threads) {
            std::this_thread::yield();
        }
        go.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }

        ASSERT_EQ(claimed.load(), num_positions) << "iteration " << iteration;
        for (size_t pos = 0; pos < num_positions; ++pos) {
            ASSERT_FALSE(flags.claim(pos)) << "claim for position " << pos << " lost in iteration " << iteration;
        }
    }
}
