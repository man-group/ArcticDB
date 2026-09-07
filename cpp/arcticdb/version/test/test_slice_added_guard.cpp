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

// Tests for the slice_added guard in schedule_first_iteration (cpp/arcticdb/version/version_core.cpp),
// which makes sure each entity is added to the ComponentManager exactly once and that a unit which skips
// the add does not process the entity before it is there. Regression tests for #3381.
//
// The guard is a few lines inline in schedule_first_iteration rather than a reusable utility, so these
// tests cannot call it directly. Be honest about what that means: what is tested below is the *pattern*,
// reproduced verbatim in call_guard, not the code that ships. If you change the guard in
// schedule_first_iteration, change call_guard to match -- there is a comment there pointing back here.

namespace {

// Copied verbatim from the slice_added guard in schedule_first_iteration. Both storage types are the
// point of the exercise: std::mutex per position, and a uint8_t (NOT bool) flag per position.
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

// Spin rather than sleep, so the thread inside the guard holds it for a while without yielding the core.
void busy_wait(std::chrono::microseconds duration) {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < deadline) {
    }
}

} // namespace

// Half one of #3381: neighbouring positions must be independent. The flags were std::vector<bool>, which
// is bit-packed, so adjacent positions shared a word and the read-modify-writes of threads holding
// *different* position mutexes lost each other. A lost flag runs the work twice, which is what made two
// read workers add the same segment to the ComponentManager and abort inside EnTT.
//
// Written so that it fails if the flags are changed back to bool: threads stride by the thread count, so
// adjacent positions are always worked by different threads, and every thread then sweeps every position,
// so a flag lost during the strided pass is run a second time and shows up in the count. A flag lost
// during the sweep itself leaves that flag clear and is caught by the single-threaded pass at the end,
// so neither failure mode relies on only one of the two assertions.
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

// Half two of #3381: a caller that finds the flag already set must see everything the caller that set it
// published beforehand. This is why the mutex is held across add_slice_to_component_manager rather than
// just around the flag -- in the pipeline the unit that skips the add goes straight on to
// MemSegmentProcessingTask, which gathers those components, so being released early means reading
// components that do not exist yet.
//
// A claim-first implementation (an atomic_flag test_and_set before doing the work) fails this: it sets
// the flag before the payload is published, so a later caller returns while the payload is still 0.
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
                    // Stands in for add_slice_to_component_manager: work done before the flag is set.
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
