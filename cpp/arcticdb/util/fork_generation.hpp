/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <cstdint>

namespace arcticdb::util {

namespace detail {
inline std::atomic<uint64_t> fork_generation_{0};
}

/* Counts how many times this process has been forked from. Storage clients record the generation they were built
 * in, so they can detect that their threads, sockets and connection pool entries belong to a process that is no
 * longer this one. Reading this is on the per-IO-operation path, hence an atomic rather than getpid(), which glibc
 * has not cached since 2.25. */
inline uint64_t fork_generation() { return detail::fork_generation_.load(std::memory_order_relaxed); }

/* Registered as a pthread_atfork child handler. An atomic increment is safe to run in that context. */
inline void increment_fork_generation() { detail::fork_generation_.fetch_add(1, std::memory_order_relaxed); }

} // namespace arcticdb::util
