/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/util/buffer.hpp>

namespace arcticdb {

class EncodedFieldCollection {
    Buffer buffer_;
    std::vector<size_t> offsets_;

public:
    explicit EncodedFieldCollection(Buffer &&buffer) :
        buffer_(std::move(buffer)) {
        regenerate_offsets();
    }

    EncodedFieldCollection() = default;

    ARCTICDB_MOVE_ONLY_DEFAULT(EncodedFieldCollection)

    [[nodiscard]] bool empty() const {
        return buffer_.empty();
    }

    [[nodiscard]] size_t bytes() const {
        return buffer_.bytes();
    }

    [[nodiscard]] const uint8_t* data() const {
        return buffer_.data();
    }

    [[nodiscard]] size_t get_offset(size_t pos) const {
        util::check(pos < offsets_.size(), "Offset {} exceeds offsets size {}", pos, offsets_.size());
        return offsets_[pos];
    }

    [[nodiscard]] const EncodedFieldImpl &at(size_t pos) const {
        return *(buffer_.ptr_cast<EncodedFieldImpl>(get_offset(pos), sizeof(EncodedField)));
    }

    [[nodiscard]] EncodedFieldImpl &at(size_t pos) {
        return *(buffer_.ptr_cast<EncodedFieldImpl>(get_offset(pos), sizeof(EncodedField)));
    }

    [[nodiscard]] size_t size() const {
        return offsets_.size();
    }

    void regenerate_offsets() {
        if (!offsets_.empty())
            return;

        auto field_pos = 0u;
        while (field_pos < buffer_.bytes()) {
            offsets_.push_back(field_pos);
            field_pos += encoded_field_bytes(*reinterpret_cast<const EncodedFieldImpl*>(buffer_.data() + field_pos));
        }
    }
};

} //namespace arcticdb
