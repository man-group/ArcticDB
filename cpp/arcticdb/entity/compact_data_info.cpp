/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/compact_data_info.hpp>

namespace arcticdb {
namespace entity {

CompactDataInfo::CompactDataInfo(
        std::vector<uint64_t>&& row_slices_before, std::vector<uint64_t>&& row_slices_after,
        VersionId version_id_before, VersionId version_id_after
) :
    row_slices_before_(std::move(row_slices_before)),
    row_slices_after_(std::move(row_slices_after)),
    version_id_before_(version_id_before),
    version_id_after_(version_id_after) {}

std::vector<uint64_t> CompactDataInfo::row_slices_before() const { return row_slices_before_; }

std::vector<uint64_t> CompactDataInfo::row_slices_after() const { return row_slices_after_; }

size_t CompactDataInfo::num_row_slices_before() const {
    // By construction, row_slices_before_.size() != 1. If there is a single row-slice, the vector will look like [0, n]
    // where n is the number of rows
    return row_slices_before_.empty() ? 0 : row_slices_before_.size() - 1;
}

size_t CompactDataInfo::num_row_slices_after() const {
    // By construction, row_slices_after_.size() != 1. If there is a single row-slice, the vector will look like [0, n]
    // where n is the number of rows
    return row_slices_after_.empty() ? 0 : row_slices_after_.size() - 1;
}

VersionId CompactDataInfo::version_id_before() const { return version_id_before_; }

VersionId CompactDataInfo::version_id_after() const { return version_id_after_; }

bool CompactDataInfo::will_do_work() const { return version_id_after_ != version_id_before_; }

std::string CompactDataInfo::to_string() const {
    return fmt::format(
            "CompactDataInfo(will_do_work={}, version_id_before={}, version_id_after={}, num_row_slices_before={}, "
            "num_row_slices_after={})",
            will_do_work(),
            version_id_before(),
            version_id_after(),
            num_row_slices_before(),
            num_row_slices_after()
    );
}

} // namespace entity
} // namespace arcticdb