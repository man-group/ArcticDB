/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/entity/protobufs.hpp>
#include <cstddef>
#include <variant>
namespace arcticdb {
    class ColumnData;
    class Buffer;
    class EncodedField;

    void add_bitmagic_compressed_size(
        const ColumnData& column_data,
        size_t& max_compressed_bytes,
        size_t& uncompressed_bytes
    );

    void encode_sparse_map(
        ColumnData& column_data,
        std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
        Buffer& out,
        std::ptrdiff_t& pos
    );
}
