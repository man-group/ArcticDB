/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/encoding_sizes.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>

namespace arcticdb::encoding_size {

std::size_t compressed_size(const arcticdb::proto::encoding::SegmentHeader &sh) {
    std::size_t total = 0;
    for (auto &field : sh.fields()) {
        switch (field.encoding_case()) {
            case arcticdb::proto::encoding::EncodedField::kNdarray: {
                auto compressed_sz = compressed_size(field.ndarray());
                ARCTICDB_TRACE(log::storage(), "From segment header: compressed: {}", compressed_sz);
                total += compressed_sz;
                break;
            }
            case arcticdb::proto::encoding::EncodedField::kDictionary:
                total += compressed_size(field.dictionary());
                break;
            default:
                util::raise_rte("Unsupported encoding in {}",  util::format(field));
        }
    }
    return total;
}

std::size_t uncompressed_size(const arcticdb::proto::encoding::SegmentHeader &sh) {
    std::size_t total = 0;
    for (auto &field : sh.fields()) {
        switch (field.encoding_case()) {
        case arcticdb::proto::encoding::EncodedField::kNdarray: {
            auto uncompressed_sz = uncompressed_size(field.ndarray());
            ARCTICDB_TRACE(log::storage(), "From segment header: uncompressed: {}", uncompressed_sz);
            total += uncompressed_sz;
            break;
        }
        case arcticdb::proto::encoding::EncodedField::kDictionary:
            total += uncompressed_size(field.dictionary());
            break;
        default:
            util::raise_rte("Unsupported encoding in {}",  util::format(field));
        }
    }
    return total;
}

std::size_t represented_size(const arcticdb::proto::encoding::SegmentHeader& sh, size_t total_rows) {
    std::size_t total = 0;

    for(const auto& field : sh.stream_descriptor().fields()) {
        total += total_rows * get_type_size(entity::data_type_from_proto(field.type_desc()));
    }

    return total;
}
}