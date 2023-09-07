/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <folly/futures/Future.h>
#include <folly/futures/FutureSplitter.h>

#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

template<typename T>
class ObjectOrFuture {
    /*
     * Either contains an object of type T, or a folly::FutureSplitter on the same type.
     * Can be constructed from either a T or a folly::Future<T>.
     * Has a single public method get() which will return a future on type T.
     * If T is held directly then the future will be trivially constructed from the object.
     * If future<T> is held, then folly::FutureSplitter will be used so that get() can be called multiple times.
     */
public:
    ObjectOrFuture(T&& object):
            object_or_future_splitter_(std::move(object)) {
    }

    ObjectOrFuture(folly::Future<T>&& future):
            object_or_future_splitter_(folly::FutureSplitter(std::move(future))) {
    }

    ObjectOrFuture() = delete;

    ARCTICDB_MOVE_ONLY_DEFAULT(ObjectOrFuture)

    folly::Future<T> get() {
        return util::variant_match(
                object_or_future_splitter_,
                [](T& object) {
                    return folly::Future<T>(object);
                },
                [](folly::FutureSplitter<T>& future_splitter) {
                    return future_splitter.getFuture();
                }
        );
    }
private:
    std::variant<T, folly::FutureSplitter<T>> object_or_future_splitter_;
};

} // namespace arcticdb