/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include "arcticdb/storage/memory_layout.hpp"
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/codec/input_bounds.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb {

static constexpr size_t SegmentIdentifierSize = sizeof(SegmentIdentifierHeader);

struct SegmentIdentifier {
    SegmentIdentifierHeader header_;
    std::array<char, 2> data_;
};

[[nodiscard]] inline size_t identifier_bytes(const StreamId& stream_id) {
    return util::variant_match(
            stream_id,
            [](const NumericId&) { return SegmentIdentifierSize; },
            [](const StringId& str_id) { return SegmentIdentifierSize + str_id.size(); }
    );
}

inline void write_identifier(Buffer& buffer, std::ptrdiff_t& pos, const StreamId& stream_id) {
    auto data = new (buffer.data() + pos) SegmentDescriptorImpl{};
    util::variant_match(
            stream_id,
            [data, &pos](const NumericId& num_id) {
                SegmentIdentifierHeader header{IdentifierType::NUMERIC, static_cast<uint32_t>(num_id)};
                *reinterpret_cast<SegmentIdentifierHeader*>(data) = header;
                pos += SegmentIdentifierSize;
            },
            [data, &pos](const StringId& str_id) {
                auto* identifier_impl = reinterpret_cast<SegmentIdentifier*>(data);
                identifier_impl->header_.type_ = IdentifierType::STRING;
                identifier_impl->header_.size_ = static_cast<uint32_t>(str_id.size());
                memcpy(&identifier_impl->data_[0], str_id.data(), str_id.size());
                pos += SegmentIdentifierSize + str_id.size();
            }
    );
}

// Checks that the identifier at data, including a string identifier's body, lies within [data, end), and returns the
// bytes it occupies.
inline size_t checked_identifier_bytes(const uint8_t* data, const uint8_t* end) {
    check_input_available(data, end, SegmentIdentifierSize, "segment identifier");
    const auto* identifier = reinterpret_cast<const SegmentIdentifier*>(data);
    switch (identifier->header_.type_) {
    case IdentifierType::STRING:
        check_input_available(
                data + SegmentIdentifierSize, end, identifier->header_.size_, "string segment identifier"
        );
        return SegmentIdentifierSize + identifier->header_.size_;
    case IdentifierType::NUMERIC:
        return SegmentIdentifierSize;
    default:
        util::raise_rte("Unknown identifier type {}", static_cast<int>(identifier->header_.type_));
    }
}

inline StreamId read_identifier(const uint8_t*& data, const uint8_t* end) {
    const auto* identifier = reinterpret_cast<const SegmentIdentifier*>(data);
    data += checked_identifier_bytes(data, end);
    if (identifier->header_.type_ == IdentifierType::STRING)
        return StringId(&identifier->data_[0], identifier->header_.size_);
    return NumericId(identifier->header_.size_);
}

inline void skip_identifier(const uint8_t*& data, const uint8_t* end) { data += checked_identifier_bytes(data, end); }

} // namespace arcticdb