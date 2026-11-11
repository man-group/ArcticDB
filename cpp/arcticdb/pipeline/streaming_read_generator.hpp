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
#include <list>
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

    ARCTICDB_MOVE_ONLY_DEFAULT(StreamingReadGenerator);

    ReadResult read_result();
    std::optional<ArrowOutputFrame> next();

  private:
    struct RowSliceInfo {
        RowRange row_range_;
        // TODO: What about other index types
        timestamp start_index_;
        timestamp end_index_;
    };

    void ensure_index_information();
    void fetch_next_slice();
    void prefetch_slices();

    bool started_{false};
    uint64_t in_flight_rows_{0};
    LocalVersionedEngine* engine_;
    StreamId stream_id_;
    VersionQuery version_query_;
    std::shared_ptr<ReadQuery> read_query_;
    ReadOptions read_options_;
    std::optional<uint64_t> prefetch_rows_;
    std::shared_ptr<std::any> handler_data_;
    std::shared_ptr<IndexInformation> index_information_;
    std::list<RowSliceInfo> row_slices_;
    std::list<std::pair<RowRange, folly::Future<ArrowOutputFrame>>> frame_futs_;
};
} // namespace arcticdb::version_store