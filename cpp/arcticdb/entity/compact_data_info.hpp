/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <vector>

#include <arcticdb/entity/types.hpp>

namespace arcticdb {
namespace entity {

class CompactDataInfo {
  public:
    CompactDataInfo(
            std::vector<uint64_t>&& row_slices_before, std::vector<uint64_t>&& row_slices_after,
            VersionId version_id_before, VersionId version_id_after
    );

    ARCTICDB_MOVE_COPY_DEFAULT(CompactDataInfo);

    std::vector<uint64_t> row_slices_before() const;
    std::vector<uint64_t> row_slices_after() const;
    size_t num_row_slices_before() const;
    size_t num_row_slices_after() const;
    VersionId version_id_before() const;
    VersionId version_id_after() const;
    bool will_do_work() const;
    std::string to_string() const;

  private:
    std::vector<uint64_t> row_slices_before_;
    std::vector<uint64_t> row_slices_after_;
    VersionId version_id_before_;
    VersionId version_id_after_;
};
} // namespace entity
} // namespace arcticdb