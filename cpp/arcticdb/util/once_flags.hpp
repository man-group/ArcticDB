/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util {

// One flag per position, each with std::call_once semantics: the first caller for a position runs the
// callable, and every later caller returns only once that callable has finished, so it is guaranteed to
// see everything the callable did.
class OnceFlags {
  public:
    explicit OnceFlags(size_t size) : mutexes_(size), done_(size, 0) {}

    template<typename Callable>
    void call_once(size_t pos, Callable&& callable) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                pos < done_.size(), "OnceFlags::call_once position {} out of range {}", pos, done_.size()
        );
        // Held across the call, not just around the flag, so that a caller which does not run the
        // callable is ordered after the one that did.
        std::lock_guard lock{mutexes_[pos]};
        if (!done_[pos]) {
            callable();
            done_[pos] = 1;
        }
    }

    [[nodiscard]] size_t size() const { return done_.size(); }

  private:
    std::vector<std::mutex> mutexes_;
    // uint8_t rather than bool: std::vector<bool> is bit-packed, so neighbouring positions share a word
    // and their read-modify-writes under different mutexes lose each other. See #3381.
    std::vector<uint8_t> done_;
};

} // namespace arcticdb::util
