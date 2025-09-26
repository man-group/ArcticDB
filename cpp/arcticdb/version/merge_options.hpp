/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

namespace arcticdb {
enum class MergeAction : uint8_t { DO_NOTHING, UPDATE, INSERT };
struct MergeStrategy {
    MergeAction matched;
    MergeAction not_matched_by_target;
};

} // namespace arcticdb