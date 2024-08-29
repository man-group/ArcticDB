/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#pragma once

#include <arcticdb/util/spinlock.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/lazy.hpp>

#include <vector>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <boost/container/small_vector.hpp>
#include <Python.h>

namespace arcticdb {
class Column;

namespace entity {
struct TypeDescriptor;
}

struct BufferHolder {
    boost::container::small_vector<std::shared_ptr<Column>, 1> columns_;
    std::mutex mutex_;

    std::shared_ptr<Column> get_buffer(const entity::TypeDescriptor& td, bool allow_sparse);
};

using UniqueStringMapType = folly::ConcurrentHashMap<std::string_view, PyObject*>;


struct DecodePathDataImpl {
    LazyInit<BufferHolder> buffer_holder_;
    LazyInit<UniqueStringMapType> unique_string_map_;
    bool optimize_for_memory_ = false;
};

struct DecodePathData {
public:
    const std::shared_ptr<BufferHolder>& buffers() const {
        return data_->buffer_holder_.instance();
    }

    const std::shared_ptr<UniqueStringMapType>& unique_string_map() const {
        return data_->unique_string_map_.instance();
    }

    bool optimize_for_memory() const {
        return data_->optimize_for_memory_;
    }

    void set_optimize_for_memory() {
        data_->optimize_for_memory_ = true;
    }
private:
    std::shared_ptr<DecodePathDataImpl> data_ = std::make_shared<DecodePathDataImpl>();
};
}  //namespace arcticdb
