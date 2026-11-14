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

#include <algorithm>
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
    if (!offsets_.empty())
        return;

    // Field sizes and name lengths may come from a stored descriptor. Fields are reached two ways: at offsets built
    // from the stored sizes, and by the field iterator, which steps by each field's own name length. Check that every
    // field either way reaches lies within the buffer before anything reads it. The last stored size only marks the end
    // of the buffer and is never used to read.
    const size_t total = buffer_.bytes();
    constexpr size_t field_header = offsetof(Field, name_);
    // Field::calc_size(name) is field_header + max(NameSize, name size).
    static_assert(field_header == sizeof(uint32_t) + sizeof(TypeDescriptor));
    const auto check_field_at = [this, total](size_t pos) {
        codec::check<ErrorCode::E_DECODE_ERROR>(
                pos <= total && field_header <= total - pos,
                "Corrupt segment: descriptor field at {} does not fit its {}-byte buffer",
                pos,
                total
        );
        uint32_t name_size;
        std::memcpy(&name_size, buffer_.buffer().ptr_cast<uint8_t>(pos, sizeof(name_size)), sizeof(name_size));
        const size_t field_size = field_header + std::max(Field::NameSize, static_cast<size_t>(name_size));
        codec::check<ErrorCode::E_DECODE_ERROR>(
                field_size <= total - pos,
                "Corrupt segment: descriptor field of {} bytes at {} exceeds its {}-byte buffer",
                field_size,
                pos,
                total
        );
        return field_size;
    };
    // Mirrors FieldCollectionIterator::increment, which never steps less than sizeof(Field). This runs even with no
    // stored sizes, since the iterator walks the buffer regardless.
    static_assert(sizeof(Field) <= field_header + Field::NameSize);
    size_t pos = 0;
    while (pos < total)
        pos += check_field_at(pos);

    if (shapes_.empty())
        return;

    offsets_.ensure_bytes(shapes_.bytes());
    auto shape_ptr = reinterpret_cast<shape_t*>(shapes_.data());
    auto offset_ptr = reinterpret_cast<shape_t*>(offsets_.data());
    auto end = shape_ptr + shapes_.bytes() / sizeof(shape_t);
    size_t offset = 0;
    while (shape_ptr != end) {
        check_field_at(offset);
        // Unsigned wraparound of a corrupt size is caught by the check of the next field.
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

// Type-size computations need size bits S8 to S64. DataType::UNKNOWN is exempt here and rejected where columns are
// created.
void check_stored_field_types(const FieldCollection& fields) {
    for (const auto& field : fields) {
        const auto data_type = field.type().data_type();
        const auto size_bits = slice_bit_size(data_type);
        codec::check<ErrorCode::E_DECODE_ERROR>(
                data_type == DataType::UNKNOWN ||
                        (size_bits != SizeBits::UNKNOWN_SIZE_BITS && size_bits < SizeBits::COUNT),
                "Corrupt segment: field '{}' has data type {} with invalid size bits",
                field.name(),
                static_cast<int>(data_type)
        );
    }
}

} // namespace arcticdb
