/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <vector>

#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>

namespace arcticdb::utils {

struct OffsetAndMangledName {
    size_t offset;            // in to the field collection in the TimeseriesDescriptor
    std::string mangled_name; // the name of the field
    std::string unmangled_name;
};

enum class MissingColumnsBehavior { RAISE, IGNORE_MISSING };

/**
 * Given a "normal" column name "col" return the offset in to the descriptor's field collection and mangled name
 * corresponding to that column.
 *
 * Note: gives error messages that are specific to column stats. Considers multi-index columns.
 */
std::vector<OffsetAndMangledName> find_offset_and_mangled_name(
        std::unordered_set<std::string> column_names, const StreamDescriptor& descriptor,
        MissingColumnsBehavior missing_columns = MissingColumnsBehavior::RAISE
);

} // namespace arcticdb::utils
