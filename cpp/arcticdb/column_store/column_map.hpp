/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <folly/container/Enumerate.h>
#include <arcticdb/util/third_party/robin_hood.hpp>

#include <string>
#include <unordered_map>

namespace arcticdb {

using namespace arcticdb::entity;

class ColumnMap {
    robin_hood::unordered_flat_map<std::string_view, size_t> column_offsets_;
    StringPool pool_;

public:
    ColumnMap(size_t size) :
        column_offsets_(size) {}

    void clear() {
        column_offsets_.clear();
        pool_.clear();
    }

    void insert(std::string_view name, size_t index) {
        auto off_str = pool_.get(name);
        column_offsets_.insert(robin_hood::pair<std::string_view, size_t>(pool_.get_view(off_str.offset()), index));
        column_offsets_[pool_.get_view(off_str.offset())] = index;
    }

    void set_from_descriptor(const StreamDescriptor& descriptor) {
        for(const auto& field : folly::enumerate(descriptor.fields())) {
            insert(field->name(), field.index);
        }
    }

    std::optional<std::size_t> column_index(std::string_view name) {
        auto it = column_offsets_.find(name);
        if(it != column_offsets_.end())
            return it->second;
        else {
            ARCTICDB_TRACE(log::version(), "Column {} not found in map of size {}", name, column_offsets_.size());
            return std::nullopt;
        }
    }
};

} // namespace arcticdb