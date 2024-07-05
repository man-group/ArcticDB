/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#pragma once

#include <arcticdb/column_store/column.hpp>
#include <arcticdb/util/spinlock.hpp>
#include <arcticdb/util/constructors.hpp>

#include <vector>
#include <folly/concurrency/ConcurrentHashMap.h>

namespace arcticdb {
struct BufferHolder {
    boost::container::small_vector<std::shared_ptr<Column>, 1> columns_;
    std::mutex mutex_;

    std::shared_ptr<Column> get_buffer(const TypeDescriptor& td, bool allow_sparse) {
        std::lock_guard lock(mutex_);
        auto column = std::make_shared<Column>(td, allow_sparse);
        columns_.emplace_back(column);
        return column;
    }
};

using UniqueStringMapType = folly::ConcurrentHashMap<std::string_view, PyObject*>;

template<typename T>
class LazyInit {
public:
    const std::shared_ptr<T>& instance() const {
        std::call_once(init_, [&]() {
            instance_ = std::make_shared<T>();
        });
        return instance_;
    }

private:
    mutable std::shared_ptr<T> instance_;
    mutable std::once_flag init_;
};

struct DecodePathDataImpl {
    LazyInit<BufferHolder> buffer_holder_;
    LazyInit<SpinLock> spin_lock_;
    LazyInit<UniqueStringMapType> unique_string_map_;
};

struct DecodePathData {
public:
    const std::shared_ptr<BufferHolder>& buffers() const {
        return data_->buffer_holder_.instance();
    }

    const std::shared_ptr<SpinLock>& spin_lock() const {
        return data_->spin_lock_.instance();
    }

    const std::shared_ptr<UniqueStringMapType>& unique_string_map() const {
        return data_->unique_string_map_.instance();
    }
private:
    std::shared_ptr<DecodePathDataImpl> data_ = std::make_shared<DecodePathDataImpl>();
};
}  //namespace arcticdb
