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
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

// Regression tests for #3381. The slice_added guard is inline in schedule_first_iteration
// (version_core.cpp) rather than a reusable utility, so call_guard below reproduces the pattern and these
// tests pin its properties, not the shipping code. Change the two together.

namespace {

using SliceAddedMutexes = std::vector<std::mutex>;
using SliceAddedFlags = std::vector<uint8_t>;

template<typename Work>
void call_guard(SliceAddedMutexes& slice_added_mtx, SliceAddedFlags& slice_added, size_t pos, Work&& work) {
    std::lock_guard lock{slice_added_mtx.at(pos)};
    if (!slice_added[pos]) {
        work();
        slice_added[pos] = 1;
    }
}

void busy_wait(std::chrono::microseconds duration) {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < deadline) {
    }
}

} // namespace

// Neighbouring positions are independent: with a bit-packed std::vector<bool> adjacent flags share a word,
// so an update under one position's mutex is lost and the work runs twice.
TEST(SliceAddedGuard, NeighbouringPositions) {
    constexpr size_t num_positions = 1024;
    constexpr size_t num_threads = 8;
    constexpr size_t num_iterations = 200;

    for (size_t iteration = 0; iteration < num_iterations; ++iteration) {
        SliceAddedMutexes slice_added_mtx(num_positions);
        SliceAddedFlags slice_added(num_positions, 0);
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
                for (size_t pos = thread_id; pos < num_positions; pos += num_threads) {
                    call_guard(slice_added_mtx, slice_added, pos, [&]() { ++calls; });
                }
                for (size_t pos = 0; pos < num_positions; ++pos) {
                    call_guard(slice_added_mtx, slice_added, pos, [&]() { ++calls; });
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
            call_guard(slice_added_mtx, slice_added, pos, [&]() { called = true; });
            ASSERT_FALSE(called) << "flag for position " << pos << " lost in iteration " << iteration;
        }
    }
}

// A caller that finds the flag already set sees what the caller that set it published first, which is why
// the lock is held across the work rather than just the flag.
TEST(SliceAddedGuard, LaterCallersWait) {
    constexpr size_t num_positions = 64;
    constexpr size_t num_threads = 8;
    constexpr int payload_value = 42;

    SliceAddedMutexes slice_added_mtx(num_positions);
    SliceAddedFlags slice_added(num_positions, 0);
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
                call_guard(slice_added_mtx, slice_added, pos, [&]() {
                    // Stands in for add_slice_to_component_manager.
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

    ASSERT_EQ(unset_seen.load(), size_t{0}) << "a caller was released before the first caller had finished the work";
}
