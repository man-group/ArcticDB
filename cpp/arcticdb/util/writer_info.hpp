/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/entity/protobufs.hpp>
#include "version_number.hpp"

namespace arcticdb::util {

inline void set_static_writer_info_fields(proto::descriptors::WriterInfo& proto) {
    using Proto = proto::descriptors::WriterInfo;

#if defined(__linux__)
    proto.set_platform(Proto::LINUX);
#elif defined(_WIN32)
    proto.set_platform(Proto::WINDOWS);
#elif defined(__APPLE__)
    proto.set_platform(Proto::APPLE);
#else
#warning "Please add your OS to the protobuf"
    proto.set_platform(Proto::UNKNOWN_PLATFORM);
#endif

#if defined(__clang_major__)
    proto.set_cc(Proto::CLANG);
    proto.set_cc_version((__clang_major__ * 1000 + __clang_minor__) * 1000 + __clang_patchlevel__);
#elif defined(__GNUC__)
    proto.set_cc(Proto::GCC);
    proto.set_cc_version((__GNUC__ * 1000 + __GNUC_MINOR__) * 1000 + __GNUC_PATCHLEVEL__);
#elif defined(_MSC_VER)
    proto.set_cc(Proto::MSVC);
    proto.set_cc_version(_MSC_FULL_VER);
#else
#warning "Please add your compiler to the protobuf"
    proto.set_cc(Proto::UNKNOWN_CC);
#endif

    proto.set_version(ARCTICDB_VERSION_INT);
}
}