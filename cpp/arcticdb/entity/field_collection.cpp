/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/entity/field_collection.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <cstring>

namespace arcticdb {

std::string_view FieldCollection::add_field(const TypeDescriptor& type, std::string_view name) {
    const auto total_size = Field::calc_size(name);
    buffer_.ensure_bytes(total_size);
    auto field = reinterpret_cast<Field*>(buffer_.cursor());
    field->set(type, name);
    buffer_.commit();
    offsets_.ensure<shape_t>();
    *reinterpret_cast<shape_t*>(offsets_.cursor()) = buffer_.cursor_pos();
    offsets_.commit();
    shapes_.ensure<shape_t>();
    *reinterpret_cast<shape_t*>(shapes_.cursor()) = total_size;
    shapes_.commit();
    util::check(field->name() == name, "Name mismatch in field: {} != {}", field->name(), name);
    return field->name();
}

void FieldCollection::regenerate_offsets() {
    if (!offsets_.empty() || shapes_.empty())
        return;

    offsets_.ensure_bytes(shapes_.bytes());
    auto shape_ptr = reinterpret_cast<shape_t*>(shapes_.data());
    auto offset_ptr = reinterpret_cast<shape_t*>(offsets_.data());
    auto end = shape_ptr + shapes_.bytes() / sizeof(shape_t);
    // Field sizes and name lengths may come from a stored descriptor. Each field's start comes from the sizes before
    // it; check the field header and name at that start lie within the buffer before anything reads them. The last size
    // only marks the end of the buffer and is never used to read.
    const size_t total = buffer_.bytes();
    constexpr size_t field_header = offsetof(Field, name_);
    size_t offset = 0;
    while (shape_ptr != end) {
        codec::check<ErrorCode::E_DECODE_ERROR>(
                offset <= total && field_header <= total - offset,
                "Corrupt segment: descriptor field at {} does not fit its {}-byte buffer",
                offset,
                total
        );
        uint32_t name_size;
        std::memcpy(&name_size, buffer_.buffer().ptr_cast<uint8_t>(offset, sizeof(name_size)), sizeof(name_size));
        codec::check<ErrorCode::E_DECODE_ERROR>(
                name_size <= total - offset - field_header,
                "Corrupt segment: descriptor field name of {} bytes at {} exceeds its {}-byte buffer",
                name_size,
                offset,
                total
        );
        // Unsigned wraparound of a corrupt size is caught by the start check of the next field.
        offset += static_cast<size_t>(*shape_ptr);
        *offset_ptr = static_cast<shape_t>(offset);
        ++shape_ptr;
        ++offset_ptr;
    }
    offsets_.commit();
}

bool operator==(const FieldCollection& left, const FieldCollection& right) {
    if (left.size() != right.size())
        return false;

    auto l = left.begin();
    auto r = right.begin();
    for (; l != left.end(); ++l, ++r) {
        if (*l != *r)
            return false;
    }

    return true;
}

} // namespace arcticdb