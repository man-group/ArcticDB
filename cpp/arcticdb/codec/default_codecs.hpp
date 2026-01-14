/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>

namespace arcticdb::codec {

inline arcticdb::proto::encoding::VariantCodec default_lz4_codec() {
    arcticdb::proto::encoding::VariantCodec codec;
    auto lz4ptr = codec.mutable_lz4();
    lz4ptr->set_acceleration(1);
    return codec;
}

inline arcticdb::proto::encoding::VariantCodec default_passthrough_codec() {
    arcticdb::proto::encoding::VariantCodec codec;
    auto lz4ptr = codec.mutable_passthrough();
    lz4ptr->set_mark(true);
    return codec;
}

inline arcticdb::proto::encoding::VariantCodec default_shapes_codec() { return codec::default_lz4_codec(); }
} // namespace arcticdb::codec
