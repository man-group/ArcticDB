/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>

namespace arcticdb {

#ifdef WIN32
#define PAUSE _mm_pause()
#elif defined(__arm__) || defined(__ARM_ARCH)
#define PAUSE asm volatile("yield")
#else
#define PAUSE __builtin_ia32_pause()
#endif

struct SpinLock {
    std::atomic<bool> lock_ = false;

    void lock() noexcept {
        do {
            if (!lock_.exchange(true, std::memory_order_acquire))
                return;

            while (lock_.load(std::memory_order_relaxed))
                PAUSE;

        } while (true);
    }

    bool try_lock() noexcept {
        return !lock_.load(std::memory_order_relaxed) && !lock_.exchange(true, std::memory_order_acquire);
    }

    void unlock() { lock_.store(false, std::memory_order_release); }
};

} // namespace arcticdb