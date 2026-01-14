/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/util/exponential_backoff.hpp>

uint8_t g_called;

struct MySpecialError : public std::runtime_error {
    MySpecialError(const char* msg) : std::runtime_error(msg) {}
};

struct MyEvenMoreSpecialError : public std::runtime_error {
    MyEvenMoreSpecialError(const char* msg) : std::runtime_error(msg) {}
};

template<typename ExceptionType>
struct ThrowNTimes {

    const uint8_t limit_;

    ThrowNTimes(uint8_t limit) : limit_(limit) { g_called = 0; }

    void operator()() {
        if (++g_called < limit_)
            throw ExceptionType("it's all rubbish");
    }
};

TEST(ExponentialBackoff, Succeeds) {
    ThrowNTimes<MySpecialError> test{3};
    arcticdb::ExponentialBackoff<MySpecialError>(100, 2000).go(test);
    ASSERT_EQ(g_called, 3);
}

TEST(ExponentialBackoff, Fails) {
    ThrowNTimes<MySpecialError> test{232};
    ASSERT_THROW(arcticdb::ExponentialBackoff<MySpecialError>(100, 1000).go(test), std::runtime_error);
    ASSERT_TRUE(g_called < 10);
}

TEST(ExponentialBackoff, FailsSpecificError) {
    ThrowNTimes<MySpecialError> test{232};
    ASSERT_THROW(
            arcticdb::ExponentialBackoff<MySpecialError>(100, 1000).go(
                    test, [](const auto&) { throw MyEvenMoreSpecialError("arg"); }
            ),
            MyEvenMoreSpecialError
    );
    ASSERT_TRUE(g_called < 10);
}

TEST(ExponentialBackoff, UncaughtExceptionEscapes) {
    ThrowNTimes<std::runtime_error> test{232};
    ASSERT_THROW(
            arcticdb::ExponentialBackoff<MySpecialError>(100, 1000).go(
                    test, [](const auto&) { throw MyEvenMoreSpecialError("bad news bear"); }
            ),
            std::runtime_error
    );
    ASSERT_EQ(g_called, 1);
}
