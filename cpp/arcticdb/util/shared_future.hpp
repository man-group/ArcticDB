/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <arcticdb/util/constructors.hpp>
#include <folly/executors/FutureExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

namespace arcticdb {

template<typename T>
class SharedFuture {
public:
    SharedFuture()
        : promise_(std::make_shared<folly::SharedPromise<T>>())
    {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(SharedFuture)

    void setValue(T&& val)
    {
        promise_->setValue(std::move(val));
    }

    const T get(folly::FutureExecutor<folly::IOThreadPoolExecutor>& exec)
    {
        auto f = promise_->getSemiFuture().via(&exec);
        const auto val = std::move(f).get();
        return val;
    }

private:
    std::shared_ptr<folly::SharedPromise<T>> promise_;
};

} //namespace arcticdb