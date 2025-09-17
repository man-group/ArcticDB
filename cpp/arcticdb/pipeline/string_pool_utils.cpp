/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb {
size_t first_context_row(const pipelines::SliceAndKey& slice_and_key, size_t first_row_in_frame) {
    return slice_and_key.slice_.row_range.first - first_row_in_frame;
}

} // namespace arcticdb