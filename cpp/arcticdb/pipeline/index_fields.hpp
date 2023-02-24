/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

namespace arcticdb::pipelines::index {
enum class Fields : uint32_t {
    start_index = 0, end_index, version_id, stream_id, creation_ts, content_hash, index_type, key_type,
    start_col, end_col, start_row, end_row, hash_bucket, num_buckets
};

enum class LegacyFields : uint32_t {
    version_id = 0, stream_id, creation_ts, content_hash, index_type, start_index, end_index, key_type
};

}