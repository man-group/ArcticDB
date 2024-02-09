/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
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
    return TypeDescriptor{
        DataType::UINT8, Dimension::Dim1
    };
}

class EncodedFieldCollection {
    ChunkedBuffer data_;
    Buffer offsets_;

public:
    EncodedFieldCollection(ChunkedBuffer&& data, Buffer&& offsets) :
        data_(std::move(data)),
        offsets_(std::move(offsets)) {
    }

    EncodedFieldCollection(size_t bytes, size_t num_fields) :
        data_(bytes),
        offsets_(num_fields * sizeof(uint64_t)){
    }

    EncodedFieldCollection() = default;

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodedFieldCollection)

    [[nodiscard]] bool empty() const {
        return data_.empty();
    }

    [[nodiscard]] size_t data_bytes() const {
        return data_.bytes();
    }

    [[nodiscard]] const uint8_t* data_buffer() const {
        return data_.data();
    }

    [[nodiscard]] size_t offset_bytes() const {
        return offsets_.bytes();
    }

    [[nodiscard]] const uint8_t* offsets_buffer() const {
        return offsets_.data();
    }

    [[nodiscard]] uint64_t get_offset(size_t pos) const {
        return *offsets_.ptr_cast<uint64_t>(pos, sizeof(uint64_t));
    }

    [[nodiscard]] const EncodedFieldImpl &at(size_t pos) const {
        return *reinterpret_cast<const EncodedFieldImpl*>(data_.data() + get_offset(pos));
    }

    [[nodiscard]] EncodedFieldImpl &at(size_t pos) {
        return *reinterpret_cast<EncodedFieldImpl*>(data_.data() + get_offset(pos));
    }

    [[nodiscard]] size_t size() const {
        return offsets_.bytes() / sizeof(uint64_t);
    }

    [[nodiscard]] EncodedFieldImpl* add_field(size_t pos, uint64_t offset) {
        *offsets_.ptr_cast<uint64_t>(pos, sizeof(uint64_t)) = offset;
        return new (data_.data() + offset)EncodedFieldImpl;
    }

    Buffer&& release_offsets() {
        return std::move(offsets_);
    }

    ChunkedBuffer release_data() {
        return std::move(data_);
    }

};

} //namespace arcticdb
