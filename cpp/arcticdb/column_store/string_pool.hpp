/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/util/regex_filter.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <string_view>
#include <unordered_set>

#include <ankerl/unordered_dense.h>

namespace arcticdb {

class Column;

static FieldRef ARCTICDB_UNUSED string_pool_descriptor() {
    static TypeDescriptor type{DataType::UINT8, Dimension::Dim1};
    static std::string_view name{"__string_pool__"};
    return FieldRef{type, name};
}

/*****************
 * StringBlock *
 *****************/

class StringBlock {
    friend class StringPool;

    ~StringBlock() = default;

    struct StringHead {
        StringHead() = default;

        ARCTICDB_NO_MOVE_OR_COPY(StringHead)

        static const size_t DataBytes = 4;
        static size_t calc_size(size_t size) { return std::max(sizeof(size_) + size, sizeof(StringHead)); }

        void copy(const char* str, size_t size) {
            size_ = static_cast<uint32_t>(size);
            memset(data_, 0, DataBytes);
            memcpy(data(), str, size);
        }

        [[nodiscard]] size_t size() const { return static_cast<size_t>(size_); }
        char* data() { return data_; }
        [[nodiscard]] const char* data() const { return data_; }

      private:
        uint32_t size_ = 0;
        char data_[DataBytes] = {};
    };

  public:
    StringBlock() = default;
    StringBlock(StringBlock&& that) noexcept;
    StringBlock(const StringBlock&) = delete;

    StringBlock& operator=(StringBlock&& that) noexcept;
    StringBlock& operator=(const StringBlock&) = delete;

    [[nodiscard]] StringBlock clone() const;

    position_t insert(const char* str, size_t size);

    std::string_view at(position_t pos);
    [[nodiscard]] std::string_view const_at(position_t pos) const;
    [[nodiscard]] std::u32string_view u32_const_at(position_t pos) const;

    void reset();

    void clear();

    void allocate(size_t size);

    [[nodiscard]] position_t cursor_pos() const;

    void advance(size_t size);

    [[nodiscard]] size_t size() const;

    [[nodiscard]] const ChunkedBuffer& buffer() const;

    uint8_t* pos_data(size_t required_size);

    [[nodiscard]] size_t num_blocks() { return data_.buffer().num_blocks(); }

    StringHead* head_at(position_t pos) {
        auto data = data_.buffer().ptr_cast<uint8_t>(pos, sizeof(StringHead));
        return reinterpret_cast<StringHead*>(data);
    }

    [[nodiscard]] const StringHead* const_head_at(position_t pos) const {
        auto data = data_.buffer().internal_ptr_cast<uint8_t>(pos, sizeof(StringHead));
        auto head = reinterpret_cast<const StringHead*>(data);
        data_.buffer().assert_size(pos + StringHead::calc_size(head->size()));
        return reinterpret_cast<const StringHead*>(data);
    }

  private:
    CursoredBuffer<ChunkedBuffer> data_;
};

class OffsetString;

/*****************
 *  StringPool  *
 *****************/

class StringPool {
  public:
    using offset_t = position_t;
    using StringType = std::string_view;
    using MapType = ankerl::unordered_dense::map<StringType, offset_t>;

    StringPool() = default;
    ~StringPool() = default;
    StringPool& operator=(const StringPool&) = delete;
    StringPool(const StringPool&) = delete;
    StringPool(StringPool&& that) noexcept = default;

    std::shared_ptr<StringPool> clone() const;

    StringPool& operator=(StringPool&& that) noexcept = default;

    ColumnData column_data() const;

    shape_t* allocate_shapes(size_t size);
    uint8_t* allocate_data(size_t size);

    void advance_data(size_t size);

    // Neither used nor defined
    void advance_shapes(size_t);

    void set_allow_sparse(Sparsity);

    OffsetString get(std::string_view s, bool deduplicate = true);
    OffsetString get(const char* data, size_t size, bool deduplicate = true);

    const ChunkedBuffer& data() const;

    std::string_view get_view(offset_t o);

    std::string_view get_const_view(offset_t o) const;

    void clear();

    const Buffer& shapes() const;

    size_t size() const;

    [[nodiscard]] size_t num_blocks() const;

    std::optional<position_t> get_offset_for_column(std::string_view str, const Column& column) const;
    ankerl::unordered_dense::set<position_t> get_offsets_for_column(
            const std::shared_ptr<std::unordered_set<std::string>>& strings, const Column& column
    ) const;
    ankerl::unordered_dense::set<position_t> get_regex_match_offsets_for_column(
            const util::RegexGeneric& regex_generic, const Column& column
    ) const;

  private:
    MapType map_;
    mutable StringBlock block_;
    mutable CursoredBuffer<Buffer> shapes_;
};

} // namespace arcticdb
