/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/streaming_read_generator.hpp>

namespace arcticdb {
StreamingReadGenerator::StreamingReadGenerator(
        version_store::LocalVersionedEngine* engine, const StreamId& stream_id, const VersionQuery& version_query,
        const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
        std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
) :
    engine_(engine),
    stream_id_(stream_id),
    version_query_(version_query),
    read_query_(read_query),
    read_options_(read_options),
    prefetch_rows_(prefetch_rows),
    handler_data_(handler_data) {}
} // namespace arcticdb