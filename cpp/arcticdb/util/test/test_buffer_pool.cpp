/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
} // namespace arcticdb
