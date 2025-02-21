/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

namespace arcticdb::pipelines::index {
enum class Fields : uint32_t {
    start_index = 0,
    end_index,
    version_id,
    stream_id,
    creation_ts,
    content_hash,
    index_type,
    key_type,
    start_col,
    end_col,
    start_row,
    end_row,
    hash_bucket,
    num_buckets
};

enum class LegacyFields : uint32_t {
    version_id = 0,
    stream_id,
    creation_ts,
    content_hash,
    index_type,
    start_index,
    end_index,
    key_type
};

} // namespace arcticdb::pipelines::index