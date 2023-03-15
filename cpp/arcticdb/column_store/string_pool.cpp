/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/segment_utils.hpp>
#include <arcticdb/util/third_party/emilib_set.hpp>
#include <arcticdb/util/third_party/robin_hood.hpp>

namespace arcticdb {
py::buffer_info StringPool::as_buffer_info() const {
    return py::buffer_info{
        (void *) block_.at(0).data(),
        1,
        py::format_descriptor<char>::format(),
        ssize_t(block_.at(0).size())
    };
}

bool StringPool::string_exists(const std::string_view& str) {
    return map_.find(str)  != map_.end();
}

OffsetString StringPool::get(const std::string_view &s, bool deduplicate) {
    if(deduplicate) {
        if (auto it = map_.find(s); it != map_.end())
            return OffsetString(it->second, this);
    }

    OffsetString str(block_.insert(s.data(), s.size()), this);

    if(deduplicate)
        map_.insert(robin_hood::pair(block_.at(str.offset()), str.offset()));

    return str;
}

OffsetString StringPool::get(const char *data, size_t size, bool deduplicate) {
    StringType s(data, size);
    if(deduplicate) {
        if (auto it = map_.find(s); it != map_.end())
            return OffsetString(it->second, this);
    }

    OffsetString str(block_.insert(s.data(), s.size()), this);
    if(deduplicate)
        map_.insert(robin_hood::pair(StringType(str), str.offset()));

    return str;
}

std::string_view StringPool::get_view(const offset_t &o) {
    return block_.at(o);
}

std::string_view StringPool::get_const_view(const offset_t &o) const {
    return block_.const_at(o);
}

std::optional<position_t> StringPool::get_offset_for_column(std::string_view string, const Column& column) {
    auto unique_values = unique_values_for_string_column(column);
    remove_nones_and_nans(unique_values);
    robin_hood::unordered_flat_map<std::string_view, offset_t> col_values;
    col_values.reserve(unique_values.size());
    for(auto pos : unique_values) {
        col_values.emplace(block_.const_at(pos), pos);
    }

    std::optional<position_t> output;
    if(auto loc = col_values.find(string); loc != col_values.end())
        output = loc->second;
    return output;
}

emilib::HashSet<position_t> StringPool::get_offsets_for_column(const std::shared_ptr<std::unordered_set<std::string>>& strings, const Column& column) {
    auto unique_values = unique_values_for_string_column(column);
    remove_nones_and_nans(unique_values);
    robin_hood::unordered_flat_map<std::string_view, offset_t> col_values;
    col_values.reserve(unique_values.size());
    for(auto pos : unique_values) {
        col_values.emplace(block_.const_at(pos), pos);
    }

    emilib::HashSet<position_t> output;
    for(const auto& string : *strings) {
        auto loc = col_values.find(string);
        if(loc != col_values.end())
            output.insert(loc->second);
    }
    return output;
}
}