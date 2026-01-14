/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/offset_string.hpp>

#include <folly/container/Enumerate.h>

#include <ankerl/unordered_dense.h>

#include <optional>
#include <string_view>

namespace arcticdb {

using namespace arcticdb::entity;

class ColumnMap {
    ankerl::unordered_dense::map<std::string_view, size_t> column_offsets_;
    StringPool pool_;

  public:
    ColumnMap(size_t size) : column_offsets_(size) {}

    void clear() {
        column_offsets_.clear();
        pool_.clear();
    }

    void insert(std::string_view name, size_t index) {
        auto off_str = pool_.get(name);
        column_offsets_.insert({pool_.get_view(off_str.offset()), index});
        column_offsets_[pool_.get_view(off_str.offset())] = index;
    }

    void erase(std::string_view name) {
        auto it = column_offsets_.find(name);
        internal::check<ErrorCode::E_INVALID_ARGUMENT>(
                it != column_offsets_.end(), "Cannot drop column with name '{}' as it doesn't exist", name
        );
        auto dropped_offset = it->second;
        column_offsets_.erase(it);
        for (auto& [_, offset] : column_offsets_) {
            if (offset > dropped_offset) {
                offset--;
            }
        }
    }

    void set_from_descriptor(const StreamDescriptor& descriptor) {
        for (const auto& field : folly::enumerate(descriptor.fields())) {
            insert(field->name(), field.index);
        }
    }

    std::optional<std::size_t> column_index(std::string_view name) {
        auto it = column_offsets_.find(name);
        if (it != column_offsets_.end())
            return it->second;
        else {
            ARCTICDB_TRACE(log::version(), "Column {} not found in map of size {}", name, column_offsets_.size());
            return std::nullopt;
        }
    }
};

} // namespace arcticdb
