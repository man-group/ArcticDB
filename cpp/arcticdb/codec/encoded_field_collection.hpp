/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/util/buffer.hpp>

namespace arcticdb {

using namespace arcticdb::entity;

constexpr TypeDescriptor encoded_fields_type_desc() {
    using namespace arcticdb::entity;
    return TypeDescriptor{DataType::UINT8, Dimension::Dim0};
}

class EncodedFieldCollection {
    ChunkedBuffer data_;
    Buffer offsets_;
    size_t count_ = 0U;
    size_t offset_ = 0U;

  public:
    struct EncodedFieldCollectionIterator {
        size_t pos_ = 0UL;
        ChunkedBuffer* buffer_ = nullptr;

        explicit EncodedFieldCollectionIterator(ChunkedBuffer* buffer) : buffer_(buffer) {}

        [[nodiscard]] EncodedFieldImpl& current() const {
            return *reinterpret_cast<EncodedFieldImpl*>(buffer_->ptr_cast<uint8_t>(pos_, EncodedFieldImpl::Size));
        }

        EncodedFieldImpl& operator*() { return current(); }

        void operator++() { pos_ += encoded_field_bytes(current()); }

        EncodedFieldImpl* operator->() { return &(current()); }
    };

    EncodedFieldCollection(ChunkedBuffer&& data, Buffer&& offsets) :
        data_(std::move(data)),
        offsets_(std::move(offsets)) {}

    void reserve(size_t bytes, size_t num_fields) {
        data_.reserve(bytes);
        offsets_.reserve(num_fields * sizeof(uint64_t));
    }

    EncodedFieldCollection() = default;

    [[nodiscard]] EncodedFieldCollection clone() const {
        auto output = EncodedFieldCollection{data_.clone(), offsets_.clone()};
        output.count_ = count_;
        output.offset_ = offset_;
        return output;
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodedFieldCollection)

    [[nodiscard]] EncodedFieldCollectionIterator begin() const {
        return EncodedFieldCollectionIterator{const_cast<ChunkedBuffer*>(&data_)};
    }

    [[nodiscard]] size_t num_blocks() const { return data_.num_blocks(); }

    [[nodiscard]] bool empty() const { return data_.empty(); }

    [[nodiscard]] size_t data_bytes() const { return data_.bytes(); }

    [[nodiscard]] const uint8_t* data_buffer() const { return data_.data(); }

    [[nodiscard]] size_t offset_bytes() const { return offsets_.bytes(); }

    [[nodiscard]] const uint8_t* offsets_buffer() const { return offsets_.data(); }

    [[nodiscard]] uint64_t get_offset(size_t pos) const {
        const auto offset = *offsets_.ptr_cast<uint64_t>(pos * sizeof(uint64_t), sizeof(uint64_t));
        return offset;
    }

    void write_offset(size_t pos, uint64_t value) {
        *offsets_.ptr_cast<uint64_t>(pos * sizeof(uint64_t), sizeof(uint64_t)) = value;
    }

    [[nodiscard]] const EncodedFieldImpl& to_field(size_t bytes_pos) const {
        return *reinterpret_cast<const EncodedFieldImpl*>(
                data_.ptr_cast<const uint8_t>(bytes_pos, EncodedFieldImpl::Size)
        );
    }

    [[nodiscard]] EncodedFieldImpl& to_field(size_t bytes_pos) {
        return *reinterpret_cast<EncodedFieldImpl*>(data_.ptr_cast<uint8_t>(bytes_pos, EncodedFieldImpl::Size));
    }

    [[nodiscard]] const EncodedFieldImpl& at(size_t pos) const { return to_field(get_offset(pos)); }

    [[nodiscard]] EncodedFieldImpl& at(size_t pos) { return to_field(get_offset(pos)); }

    void write_data_to(uint8_t*& dst) const {
        for (auto block : data_.blocks()) {
            memcpy(dst, block->data(), block->bytes());
            dst += block->bytes();
        }
    }

    [[nodiscard]] size_t size() const { return offsets_.bytes() / sizeof(uint64_t); }

    void regenerate_offsets() {
        if (!offsets_.empty())
            return;

        auto pos = 0UL;
        count_ = 0UL;
        while (pos < data_.bytes()) {
            const auto& field = to_field(pos);
            offsets_.ensure((count_ + 1) * sizeof(uint64_t));
            write_offset(count_, pos);
            ++count_;
            pos += encoded_field_bytes(field);
        }
        util::check(pos == data_.bytes(), "Size mismatch in regenerate_offsets, {} != {}", pos, data_.bytes());
    }

    [[nodiscard]] EncodedFieldImpl* add_field(size_t num_blocks) {
        offsets_.ensure((count_ + 1) * sizeof(uint64_t));
        write_offset(count_, offset_);
        const auto required_bytes = calc_field_bytes(num_blocks);
        util::check(required_bytes >= EncodedFieldImpl::Size, "Unexpectedly small allocation size: {}", required_bytes);
        data_.ensure(offset_ + required_bytes);
        auto* field = new (data_.ptr_cast<uint8_t>(offset_, required_bytes)) EncodedFieldImpl;
        ARCTICDB_DEBUG(
                log::codec(),
                "Adding encoded field with {} blocks at position {}, {} bytes required",
                num_blocks,
                offset_,
                required_bytes
        );
        ++count_;
        offset_ += required_bytes;
        return field;
    }

    Buffer&& release_offsets() { return std::move(offsets_); }

    ChunkedBuffer&& release_data() { return std::move(data_); }
};

} // namespace arcticdb
