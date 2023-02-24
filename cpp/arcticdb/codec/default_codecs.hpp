/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
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
}
