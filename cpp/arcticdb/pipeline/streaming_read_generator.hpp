/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <any>
#include <cstdint>
#include <optional>

#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>

namespace arcticdb::version_store {
class StreamingReadGenerator {
  public:
    StreamingReadGenerator(
            LocalVersionedEngine* engine, const StreamId& stream_id, const VersionQuery& version_query,
            const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
            std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
    );

    ReadResult read_result();
    std::optional<ArrowOutputFrame> next();

  private:
    void ensure_index_information();

    size_t idx_{0};
    LocalVersionedEngine* engine_;
    StreamId stream_id_;
    VersionQuery version_query_;
    std::shared_ptr<ReadQuery> read_query_;
    ReadOptions read_options_;
    ARCTICDB_UNUSED std::optional<uint64_t> prefetch_rows_;
    std::shared_ptr<std::any> handler_data_;
    std::shared_ptr<IndexInformation> index_information_;
    std::vector<RowRange> row_ranges_;
};
} // namespace arcticdb::version_store