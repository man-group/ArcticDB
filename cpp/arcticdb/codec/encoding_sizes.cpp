/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/types_proto.hpp>

namespace arcticdb::encoding_sizes {

std::size_t represented_size(const arcticdb::proto::encoding::SegmentHeader& sh, size_t total_rows) {
    std::size_t total = 0;

    for(const auto& field : sh.stream_descriptor().fields()) {
        total += total_rows * get_type_size(entity::data_type_from_proto(field.type_desc()));
    }

    return total;
}

}