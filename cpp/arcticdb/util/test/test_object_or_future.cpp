/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/executors/ThreadedExecutor.h>
#include <gtest/gtest.h>

#include <arcticdb/util/object_or_future.hpp>

using namespace arcticdb;


TEST(ObjectOrFuture, SharedPtr) {
    auto ptr = std::make_shared<int>(5);
    ASSERT_EQ(ptr.use_count(), 1);
    {
        ObjectOrFuture<std::shared_ptr<int>> object_or_future(ptr);
        ASSERT_EQ(ptr.use_count(), 2);
        // Can call get multiple times
        ASSERT_EQ(object_or_future.get().get(), ptr);
        ASSERT_EQ(object_or_future.get().get(), ptr);
    }
    ASSERT_EQ(ptr.use_count(), 1);
}

TEST(ObjectOrFuture, Future) {
    auto ptr = std::make_shared<int>(5);
    {
        folly::ThreadedExecutor executor;
        auto [promise, future] = folly::makePromiseContract<std::shared_ptr<int>>(&executor);
        ObjectOrFuture<std::shared_ptr<int>> object_or_future(std::move(future));
        auto f1 = object_or_future.get();
        auto f2 = object_or_future.get();
        ASSERT_FALSE(f1.isReady());
        ASSERT_FALSE(f2.isReady());
        promise.setValue(ptr);
        auto ptr1 = std::move(f1).get();
        auto ptr2 = std::move(f2).get();
        // One in the promise
        ASSERT_EQ(ptr.use_count(), 4);
        ASSERT_EQ(ptr, ptr1);
        ASSERT_EQ(ptr, ptr2);
    }
    ASSERT_EQ(ptr.use_count(), 1);
}


