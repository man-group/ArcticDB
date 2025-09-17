/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include "arcticdb/storage/memory_layout.hpp"
#include <arcticdb/entity/types.hpp>

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

inline StreamId read_identifier(const uint8_t*& data) {
    auto* identifier = reinterpret_cast<const SegmentIdentifier*>(data);

    switch (identifier->header_.type_) {
    case IdentifierType::STRING:
        data += SegmentIdentifierSize + identifier->header_.size_;
        return StringId(&identifier->data_[0], identifier->header_.size_);
    case IdentifierType::NUMERIC:
        data += SegmentIdentifierSize;
        return NumericId(identifier->header_.size_);
    default:
        util::raise_rte("Unknown identifier type in read_identifier");
    }
}

inline void skip_identifier(const uint8_t*& data) {
    auto* identifier = reinterpret_cast<const SegmentIdentifier*>(data);
    switch (identifier->header_.type_) {
    case IdentifierType::STRING:
        data += SegmentIdentifierSize + identifier->header_.size_;
        break;
    case IdentifierType::NUMERIC:
        data += SegmentIdentifierSize;
        break;
    default:
        util::raise_rte("Unknown identifier type in skip_identifier");
    }
}

} // namespace arcticdb