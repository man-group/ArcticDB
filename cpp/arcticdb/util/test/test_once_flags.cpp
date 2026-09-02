/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <arcticdb/util/once_flags.hpp>

using namespace arcticdb::util;

namespace {
// Spin rather than sleep, so the winner holds the flag for a while without yielding the core.
void busy_wait(std::chrono::microseconds duration) {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < deadline) {
    }
}
} // namespace

TEST(OnceFlags, RunsOnce) {
    OnceFlags flags(3);
    ASSERT_EQ(flags.size(), 3);
    size_t calls{0};
    flags.call_once(1, [&]() { ++calls; });
    flags.call_once(1, [&]() { ++calls; });
    flags.call_once(0, [&]() { ++calls; });
    ASSERT_EQ(calls, 2);
}

// Regression test for #3381. Neighbouring positions are used by different threads at the same time, so
// an implementation that packs the flags into shared words loses one and runs the callable twice. That
// is what made read workers add a segment to the ComponentManager twice and abort on an EnTT assertion.
TEST(OnceFlags, NeighbouringPositionsAreIndependent) {
    constexpr size_t num_positions = 1024;
    constexpr size_t num_threads = 8;
    constexpr size_t num_iterations = 200;

    for (size_t iteration = 0; iteration < num_iterations; ++iteration) {
        OnceFlags flags(num_positions);
        std::atomic<size_t> calls{0};
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
                // Stride by the thread count so that adjacent positions are always used by different
                // threads, maximising the chance of a lost update within one word.
                for (size_t pos = thread_id; pos < num_positions; pos += num_threads) {
                    flags.call_once(pos, [&]() { ++calls; });
                }
            });
        }
        while (ready.load() != num_threads) {
            std::this_thread::yield();
        }
        go.store(true, std::memory_order_release);
        for (auto& thread : threads) {
            thread.join();
        }

        ASSERT_EQ(calls.load(), num_positions) << "iteration " << iteration;
        for (size_t pos = 0; pos < num_positions; ++pos) {
            bool called{false};
            flags.call_once(pos, [&]() { called = true; });
            ASSERT_FALSE(called) << "flag for position " << pos << " lost in iteration " << iteration;
        }
    }
}

// The other half of #3381: a caller that does not run the callable must not continue until the caller
// that did has finished. In the pipeline the loser goes straight on to process the entity, so if it is
// released while the winner is still adding the components it reads components that do not exist yet.
TEST(OnceFlags, LaterCallersWaitForTheFirst) {
    constexpr size_t num_positions = 64;
    constexpr size_t num_threads = 8;
    constexpr int payload_value = 42;

    OnceFlags flags(num_positions);
    std::vector<std::atomic<int>> payloads(num_positions);
    for (auto& payload : payloads) {
        payload.store(0);
    }
    std::atomic<size_t> ready{0};
    std::atomic<bool> go{false};
    std::atomic<size_t> unset_seen{0};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
        threads.emplace_back([&]() {
            ++ready;
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (size_t pos = 0; pos < num_positions; ++pos) {
                flags.call_once(pos, [&]() {
                    // The work the winner does before the flag is set. Anyone released early sees 0.
                    busy_wait(std::chrono::microseconds(200));
                    payloads[pos].store(payload_value);
                });
                if (payloads[pos].load() != payload_value) {
                    ++unset_seen;
                }
            }
        });
    }
    while (ready.load() != num_threads) {
        std::this_thread::yield();
    }
    go.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(unset_seen.load(), 0) << "a caller returned from call_once before the first caller had finished";
}
