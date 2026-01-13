/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/spinlock.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/entity/types.hpp>
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

using UniqueStringMapType = folly::ConcurrentHashMap<std::string_view, PyObject*>;

struct DecodePathDataImpl {
    LazyInit<UniqueStringMapType> unique_string_map_;
    bool optimize_for_memory_ = false;
};

struct DecodePathData {
  public:
    [[nodiscard]] const std::shared_ptr<UniqueStringMapType>& unique_string_map() const {
        return data_->unique_string_map_.instance();
    }

    [[nodiscard]] bool optimize_for_memory() const { return data_->optimize_for_memory_; }

    void set_optimize_for_memory() { data_->optimize_for_memory_ = true; }

  private:
    std::shared_ptr<DecodePathDataImpl> data_ = std::make_shared<DecodePathDataImpl>();
};
} // namespace arcticdb
