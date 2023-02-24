/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <arcticdb/util/buffer_pool.hpp>

namespace arcticdb {
    TEST(BufferPool, Basic) {
        auto buffer = BufferPool::instance()->allocate();
        buffer->ensure(40);
        buffer.reset();
        auto new_buffer = BufferPool::instance()->allocate();
        ASSERT_EQ(new_buffer->bytes(), 0);
    }
}
