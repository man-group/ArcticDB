/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/codec/encoded_field.hpp>

namespace arcticdb::codec {

inline BlockCodecImpl default_lz4_codec() {
    BlockCodecImpl codec;
    auto lz4ptr = codec.mutable_lz4();
    lz4ptr->acceleration_ = 1;
    return codec;
}

inline BlockCodecImpl default_adaptive_codec() {
    BlockCodecImpl codec;
    codec.type_ = Codec::ADAPTIVE;
    return codec;
}

constexpr inline BlockCodecImpl default_passthrough_codec() {
    BlockCodecImpl codec;
    codec.type_ = Codec::PASS;
    return codec;
}

inline BlockCodecImpl default_shapes_codec() {
    return codec::default_lz4_codec();
}
}
