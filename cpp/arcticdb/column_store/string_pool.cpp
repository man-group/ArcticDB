/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/column_store/segment_utils.hpp>
#ifdef ARCTICDB_USING_CONDA
    #include <robin_hood.h>
#else
    #include <arcticdb/util/third_party/robin_hood.hpp>
#endif

#include <pybind11/pybind11.h>

namespace arcticdb {

/*****************
 * StringBlock *
*****************/

StringBlock::StringBlock(StringBlock &&that) noexcept
    : data_(std::move(that.data_))
{}

StringBlock& StringBlock::operator=(StringBlock &&that) noexcept {
    data_ = std::move(that.data_);
    return *this;
}

StringBlock StringBlock::clone() const {
    StringBlock output;
    output.data_ = data_.clone();
    return output;
}

position_t StringBlock::insert(const char *str, size_t size) {
    auto bytes_required = StringHead::calc_size(size);
    auto ptr = data_.ensure_aligned_bytes(bytes_required);
    reinterpret_cast<StringHead*>(ptr)->copy(str, size);
    data_.commit();
    return data_.cursor_pos() - bytes_required;
}

std::string_view StringBlock::at(position_t pos) {
    auto head(head_at(pos));
    return {head->data(), head->size()};
}

std::string_view StringBlock::const_at(position_t pos) const {
    auto head(const_head_at(pos));
    return {head->data(), head->size()};
}

void StringBlock::reset() {
    data_.reset();
}

void StringBlock::clear() {
    data_.clear();
}

void StringBlock::allocate(size_t size) {
    data_.ensure_bytes(size);
}

[[nodiscard]] position_t StringBlock::cursor_pos() const {
    return data_.cursor_pos();
}

void StringBlock::advance(size_t size) {
    data_.advance(size);
}

[[nodiscard]] size_t StringBlock::size() const {
    return data_.size<uint8_t>();
}

[[nodiscard]] const ChunkedBuffer& StringBlock::buffer() const {
    return data_.buffer();
}

uint8_t* StringBlock::pos_data(size_t required_size) {
    return data_.pos_cast<uint8_t>(required_size);
}

/*****************
 *  StringPool  *
*****************/

std::shared_ptr<StringPool> StringPool::clone() const {
    auto output = std::make_shared<StringPool>();
    output->block_ = block_.clone();
    output->map_ = map_;
    output->shapes_ = shapes_.clone();
    return output;
}

StringPool& StringPool::operator=(StringPool &&that) noexcept {
    if (this != &that) {
        block_ = std::move(that.block_);
        map_ = std::move(that.map_);
        shapes_ = std::move(that.shapes_);
    }
    return *this;
}

ColumnData StringPool::column_data() const {
    return {
        &block_.buffer(),
        &shapes_.buffer(),
        string_pool_descriptor().type(),
        nullptr
    };
}

shape_t* StringPool::allocate_shapes(size_t size) {
    shapes_.ensure_bytes(size);
    return shapes_.pos_cast<shape_t>(size);
}

uint8_t* StringPool::allocate_data(size_t size) {
    block_.allocate(size);
    return block_.pos_data(size);
}

void StringPool::advance_data(size_t size) {
    block_.advance(size);
}

void StringPool::advance_shapes(size_t) {
    // Not used
}

void StringPool::set_allow_sparse(bool) {
    // Not used
}

OffsetString StringPool::get(std::string_view s, bool deduplicate) {
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

const ChunkedBuffer& StringPool::data() const {
    return block_.buffer();
}

std::string_view StringPool::get_view(offset_t o) {
    return block_.at(o);
}

std::string_view StringPool::get_const_view(offset_t o) const {
    return block_.const_at(o);
}

void StringPool::clear() {
    map_.clear();
    block_.clear();
}

const Buffer& StringPool::shapes() const {
    auto& blocks = block_.buffer().blocks();
    shapes_.ensure_bytes(blocks.size() * sizeof(shape_t));
    auto ptr = shapes_.buffer().ptr_cast<shape_t>(0, sizeof(shape_t));
    for(auto& block : blocks) {
        *ptr++ = static_cast<shape_t>(block->bytes());
    }
    ARCTICDB_TRACE(log::inmem(), "String pool shapes array has {} blocks", blocks.size());
    return shapes_.buffer();
}

size_t StringPool::size() const {
    return block_.size();
}

py::buffer_info StringPool::as_buffer_info() const {
    return py::buffer_info{
        (void *) block_.at(0).data(),
        1,
        py::format_descriptor<char>::format(),
        ssize_t(block_.at(0).size())
    };
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

robin_hood::unordered_set<position_t> StringPool::get_offsets_for_column(const std::shared_ptr<std::unordered_set<std::string>>& strings, const Column& column) {
    auto unique_values = unique_values_for_string_column(column);
    remove_nones_and_nans(unique_values);
    robin_hood::unordered_flat_map<std::string_view, offset_t> col_values;
    col_values.reserve(unique_values.size());
    for(auto pos : unique_values) {
        col_values.emplace(block_.const_at(pos), pos);
    }

    robin_hood::unordered_set<position_t> output;
    for(const auto& string : *strings) {
        auto loc = col_values.find(string);
        if(loc != col_values.end())
            output.insert(loc->second);
    }
    return output;
}
}
