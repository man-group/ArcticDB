/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/cursored_buffer.hpp>
#include <arcticdb/entity/field_collection.hpp>

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
    auto offset = 0;
    while (shape_ptr != end) {
        offset += *shape_ptr;
        *offset_ptr = offset;
        ++shape_ptr;
        ++offset_ptr;
    }
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