/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/types_proto.hpp>
#include <google/protobuf/util/message_differencer.h>

namespace arcticdb::entity {


    bool operator==(const FieldProto& left, const FieldProto& right) {
        google::protobuf::util::MessageDifferencer diff;
        return diff.Compare(left, right);
    }

    bool operator<(const FieldProto& left, const FieldProto& right) {
        return left.name() < right.name();
    }


} // namespace arcticdb
