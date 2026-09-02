/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <vector>

#include <arcticdb/util/preconditions.hpp>

namespace arcticdb::util {

// A fixed-size set of flags that can be claimed exactly once each, from any number of threads.
// claim(pos) returns true for the first caller for that position and false for every later one.
// Do not replace the flags with std::vector<bool>: it is bit-packed, so neighbouring positions
// share a word and concurrent writes lose each other, even under a per-position lock. See #3381.
class ClaimFlags {
  public:
    explicit ClaimFlags(size_t size) : flags_(size) {}

    [[nodiscard]] bool claim(size_t pos) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                pos < flags_.size(), "ClaimFlags::claim position {} out of range {}", pos, flags_.size()
        );
        return !flags_[pos].test_and_set(std::memory_order_acq_rel);
    }

    [[nodiscard]] size_t size() const { return flags_.size(); }

  private:
    std::vector<std::atomic_flag> flags_;
};

} // namespace arcticdb::util
