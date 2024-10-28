/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/buffer.hpp>

#ifdef ARCTICDB_USING_CONDA
#include <recycle/shared_pool.hpp>
#else
#include <third_party/recycle/src/recycle/shared_pool.hpp>
#endif

namespace arcticdb {

struct lock_policy {
    using mutex_type = std::mutex;
    using lock_type = std::lock_guard<mutex_type>;
};

class BufferPool {
    static std::shared_ptr<BufferPool> instance_;
    static std::once_flag init_flag_;

    static void init();

    recycle::shared_pool<Buffer, lock_policy> pool_;

  public:
    static std::shared_ptr<BufferPool> instance();
    static void destroy_instance();

    BufferPool();

    auto allocate() {
        auto output = pool_.allocate();
        ARCTICDB_DEBUG(log::version(), "Pool returning {}", uintptr_t(output.get()));
        return output;
    }

    void clear() { pool_.free_unused(); }
};

} // namespace arcticdb