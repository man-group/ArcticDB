/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/cursor.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>

#include <string_view>
#include <unordered_map>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <arcticdb/util/third_party/emilib_set.hpp>
#include <arcticdb/util/third_party/robin_hood.hpp>

namespace arcticdb {
class StringPool;
class Column;

class StringBlock {
    friend class StringPool;

    ~StringBlock() = default;

    struct StringHead {
        StringHead() = default;

        ARCTICDB_NO_MOVE_OR_COPY(StringHead)

        static const size_t DataBytes = 4;
        static size_t calc_size(size_t size)
        {
            return std::max(sizeof(size_) + size, sizeof(StringHead));
        }

        void copy(const char* str, size_t size)
        {
            size_ = static_cast<uint32_t>(size);
            memset(data_, 0, DataBytes);
            memcpy(data(), str, size);
        }

        [[nodiscard]] size_t size() const
        {
            return static_cast<size_t>(size_);
        }
        char* data()
        {
            return data_;
        }
        [[nodiscard]] const char* data() const
        {
            return data_;
        }

    private:
        uint32_t size_ = 0;
        char data_[DataBytes] = {};
    };

public:
    StringBlock() = default;
    StringBlock(StringBlock&& that) noexcept
        : data_(std::move(that.data_))
    {
    }

    StringBlock& operator=(StringBlock&& that) noexcept
    {
        data_ = std::move(that.data_);
        return *this;
    }

    StringBlock& operator=(const StringBlock&) = delete;

    StringBlock(const StringBlock&) = delete;

    [[nodiscard]] StringBlock clone() const
    {
        StringBlock output;
        output.data_ = data_.clone();
        return output;
    }

    position_t insert(const char* str, size_t size)
    {
        auto bytes_required = StringHead::calc_size(size);
        auto ptr = data_.ensure_aligned_bytes(bytes_required);
        reinterpret_cast<StringHead*>(ptr)->copy(str, size);
        data_.commit();
        return data_.cursor_pos() - bytes_required;
    }

    std::string_view at(position_t pos)
    {
        auto head(head_at(pos));
        return {head->data(), head->size()};
    }

    [[nodiscard]] std::string_view const_at(position_t pos) const
    {
        auto head(const_head_at(pos));
        return {head->data(), head->size()};
    }

    StringHead* head_at(position_t pos)
    {
        auto data = data_.buffer().ptr_cast<uint8_t>(pos, sizeof(StringHead));
        return reinterpret_cast<StringHead*>(data);
    }

    [[nodiscard]] const StringHead* const_head_at(position_t pos) const
    {
        auto data = data_.buffer().internal_ptr_cast<uint8_t>(pos, sizeof(StringHead));
        return reinterpret_cast<const StringHead*>(data);
    }

    void reset()
    {
        data_.reset();
    }

    void clear()
    {
        data_.clear();
    }

    void allocate(size_t size)
    {
        data_.ensure_bytes(size);
    }

    [[nodiscard]] position_t cursor_pos() const
    {
        return data_.cursor_pos();
    }

    void advance(size_t size)
    {
        data_.advance(size);
    }

    [[nodiscard]] size_t size() const
    {
        return data_.size<uint8_t>();
    }

    [[nodiscard]] const ChunkedBuffer& buffer() const
    {
        return data_.buffer();
    }

    uint8_t* pos_data(size_t required_size)
    {
        return data_.pos_cast<uint8_t>(required_size);
    }

private:
    CursoredBuffer<ChunkedBuffer> data_;
};

class OffsetString;

class StringPool {
public:
    using offset_t = position_t;
    using StringType = std::string_view;
    using MapType = robin_hood::unordered_flat_map<StringType, offset_t>;

    StringPool() = default;
    ~StringPool() = default;
    StringPool& operator=(const StringPool&) = delete;
    StringPool(const StringPool&) = delete;
    StringPool(StringPool&& that) = delete;

    std::shared_ptr<StringPool> clone() const
    {
        auto output = std::make_shared<StringPool>();
        output->block_ = block_.clone();
        output->map_ = map_;
        output->shapes_ = shapes_.clone();
        return output;
    }

    StringPool& operator=(StringPool&& that) noexcept
    {
        if (this != &that) {
            block_ = std::move(that.block_);
            map_ = std::move(that.map_);
            shapes_ = std::move(that.shapes_);
        }
        return *this;
    }

    OffsetString get(const char* data, size_t size, bool deduplicate = true);

    shape_t* allocate_shapes(size_t size)
    {
        shapes_.ensure_bytes(size);
        return shapes_.pos_cast<shape_t>(size);
    }

    uint8_t* allocate_data(size_t size)
    {
        block_.allocate(size);
        return block_.pos_data(size);
    }

    void advance_shapes(size_t)
    {
        // Not used
    }

    void advance_data(size_t size)
    {
        block_.advance(size);
    }

    void set_allow_sparse(bool)
    {
        // Not used
    }

    OffsetString get(const std::string_view& s, bool deduplicate = true);

    const ChunkedBuffer& data() const
    {
        return block_.buffer();
    }

    std::string_view get_view(const offset_t& o);

    std::string_view get_const_view(const offset_t& o) const;

    bool string_exists(const std::string_view& str);

    void clear()
    {
        map_.clear();
        block_.clear();
    }

    const auto& shapes() const
    {
        auto& blocks = block_.buffer().blocks();
        shapes_.ensure_bytes(blocks.size() * sizeof(shape_t));
        auto ptr = shapes_.buffer().ptr_cast<shape_t>(0, sizeof(shape_t));
        for (auto& block : blocks) {
            *ptr++ = static_cast<shape_t>(block->bytes());
        }
        ARCTICDB_TRACE(log::inmem(), "String pool shapes array has {} blocks", blocks.size());
        return shapes_.buffer();
    }

    size_t size() const
    {
        return block_.size();
    }

    py::buffer_info as_buffer_info() const;

    std::optional<position_t> get_offset_for_column(std::string_view str, const Column& column);
    emilib::HashSet<position_t> get_offsets_for_column(const std::shared_ptr<std::unordered_set<std::string>>& strings,
        const Column& column);

private:
    MapType map_;
    mutable StringBlock block_;
    mutable CursoredBuffer<Buffer> shapes_; //TODO MemBlock::MinSize
};

} //namespace arcticdb
