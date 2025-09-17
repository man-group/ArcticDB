/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preprocess.hpp>

#include <random>

namespace arcticdb {

template<typename HandledExceptionType>
struct ExponentialBackoff {

    size_t min_wait_ms_;
    size_t max_wait_ms_;
    size_t curr_wait_ms_;

    ExponentialBackoff(size_t min_wait_ms, size_t max_wait_ms) :
        min_wait_ms_(min_wait_ms),
        max_wait_ms_(max_wait_ms),
        curr_wait_ms_(min_wait_ms_) {}

    void sleep_ms(size_t ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

    bool wait() {
        thread_local std::uniform_int_distribution<size_t> dist;
        thread_local std::minstd_rand gen(std::random_device{}());
        const size_t wait = dist(gen, decltype(dist)::param_type{0, curr_wait_ms_});
        sleep_ms(wait);
        curr_wait_ms_ = std::min(curr_wait_ms_ * 2, max_wait_ms_);
        return curr_wait_ms_ != max_wait_ms_;
    }

    template<typename Callable>
    auto go(Callable&& callable) {
        // Throw exception with error msg, as user may turn off warn or in juypter notebook
        return go(std::forward<Callable>(callable), [](const HandledExceptionType& e) {
            util::raise_rte("Exhausted retry attempts, likely due to errors given by the storage: {}", e.what());
        });
    }

    template<typename Callable, typename FailurePolicy>
    auto go(Callable&& c, FailurePolicy&& failure_policy) {
        std::optional<HandledExceptionType> last_exception; // HandledExceptionType may have the default ctor deleted
        do {
            try {
                return c();
            } catch (HandledExceptionType& e) {
                log::storage().warn(
                        "Caught error in backoff, retrying, likely due to errors given by the storage {}", e.what()
                );
                last_exception = e;
            }
        } while (wait());

        failure_policy(last_exception.value());
        ARCTICDB_UNREACHABLE
    }
};
} // namespace arcticdb