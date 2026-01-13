/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 *
 * The code in this file is derived from https://github.com/facebook/folly/blob/main/folly/ThreadCachedInt.h under the
 * Apache 2.0 license which is available in full at https://github.com/facebook/folly/blob/main/LICENSE.
 */

#pragma once

#include <atomic>
#include <boost/thread/tss.hpp>

namespace arcticdb {

template<class IntT>
class ThreadCachedInt {
  public:
    explicit ThreadCachedInt(IntT initialVal = 0, uint32_t cacheSize = 1000) :
        target_(initialVal),
        cacheSize_(cacheSize) {}

    ThreadCachedInt(const ThreadCachedInt&) = delete;
    ThreadCachedInt& operator=(const ThreadCachedInt&) = delete;

    void increment(IntT inc) {
        auto cache = cache_.get();
        if (cache == nullptr) {
            cache = new IntCache(*this);
            cache_.reset(cache);
        }
        cache->increment(inc);
    }

    // Quickly grabs the current value which may not include some cached increments.
    IntT readFast() const { return target_.load(std::memory_order_relaxed); }

    // Quickly reads and resets current value (doesn't reset cached increments).
    IntT readFastAndReset() { return target_.exchange(0, std::memory_order_release); }

  private:
    struct IntCache;

    std::atomic<IntT> target_;
    std::atomic<uint32_t> cacheSize_;
    boost::thread_specific_ptr<IntCache> cache_; // Must be last for dtor ordering

    struct IntCache {
        ThreadCachedInt* parent_;
        mutable IntT val_;
        mutable uint32_t numUpdates_;

        explicit IntCache(ThreadCachedInt& parent) : parent_(&parent), val_(0), numUpdates_(0) {}

        void increment(IntT inc) {
            val_ += inc;
            ++numUpdates_;
            if (numUpdates_ > parent_->cacheSize_.load(std::memory_order_acquire)) {
                flush();
            }
        }

        void flush() const {
            parent_->target_.fetch_add(val_, std::memory_order_release);
            val_ = 0;
            numUpdates_ = 0;
        }

        ~IntCache() { flush(); }
    };
};

} // namespace arcticdb
