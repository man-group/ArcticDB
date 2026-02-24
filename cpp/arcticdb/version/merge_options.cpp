/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/merge_options.hpp>

namespace arcticdb {
bool MergeStrategy::update_only() const {
    return matched == MergeAction::UPDATE && not_matched_by_target == MergeAction::DO_NOTHING;
}

bool MergeStrategy::insert_only() const {
    return matched == MergeAction::DO_NOTHING && not_matched_by_target == MergeAction::INSERT;
}
} // namespace arcticdb