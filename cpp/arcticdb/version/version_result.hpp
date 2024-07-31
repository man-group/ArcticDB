/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

struct VersionResult {
    StreamId stream_id_;
    entity::VersionId version_id_;
    entity::timestamp timestamp_;
    std::vector<entity::SnapshotId> snapshots_;
    bool is_deleted_;

    friend bool operator<(const VersionResult& left, const VersionResult& right) {
        return std::tie(left.stream_id_, left.timestamp_) >
            std::tie(right.stream_id_, right.timestamp_);
    };
};
} // namespace arcticdb
