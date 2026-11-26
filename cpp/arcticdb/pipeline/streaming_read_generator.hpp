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
#include <arcticdb/storage/store.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_tasks.hpp>

namespace arcticdb::version_store {
class StreamingReadGenerator {
  public:
    StreamingReadGenerator(
            std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map, const StreamId& stream_id,
            const VersionQuery& version_query, std::shared_ptr<ReadQuery> read_query, const ReadOptions& read_options,
            std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
    );

    ARCTICDB_MOVE_ONLY_DEFAULT(StreamingReadGenerator);

    ReadResult read_result();
    std::optional<ReadResult> next();
    uint64_t in_flight_rows() const { return in_flight_rows_; };

  private:
    struct RowSlice {
        RowRange row_range_;
        timestamp start_index_;
        timestamp end_index_;
        std::optional<folly::Future<ReadResult>> opt_read_result_fut_;

        size_t row_count() const { return row_range_.diff(); }
    };

    void ensure_index_information();
    void validate_versioned_item(const std::optional<VersionedItem>& opt_versioned_item) const;
    void validate_data_format_and_query(const pipelines::index::IndexSegmentReader& isr);
    void fetch_next_slice();
    void prefetch_slices();

    bool started_{false};
    uint64_t in_flight_rows_{0};
    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_;
    StreamId stream_id_;
    VersionQuery version_query_;
    std::shared_ptr<ReadQuery> read_query_;
    std::shared_ptr<ReadOptions> read_options_;
    std::optional<uint64_t> prefetch_rows_;
    std::shared_ptr<std::any> handler_data_;
    std::shared_ptr<IndexInformation> index_information_;
    std::list<RowSlice> to_process_;
    std::list<RowSlice> being_processed_;
};
} // namespace arcticdb::version_store