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
#include <memory>

#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/read_query.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>

namespace arcticdb {
class StreamingReadGenerator {
  public:
    // TODO: What is the right move/copy policy?
    StreamingReadGenerator(
            version_store::LocalVersionedEngine* engine, const StreamId& stream_id, const VersionQuery& version_query,
            const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options,
            std::optional<uint64_t> prefetch_rows, std::shared_ptr<std::any> handler_data
    );

  private:
    // TODO: Make this shared_ptr
    ARCTICDB_UNUSED version_store::LocalVersionedEngine* engine_;
    ARCTICDB_UNUSED StreamId stream_id_;
    ARCTICDB_UNUSED VersionQuery version_query_;
    ARCTICDB_UNUSED std::shared_ptr<ReadQuery> read_query_;
    ARCTICDB_UNUSED ReadOptions read_options_;
    ARCTICDB_UNUSED std::optional<uint64_t> prefetch_rows_;
    ARCTICDB_UNUSED std::shared_ptr<std::any> handler_data_;
};
} // namespace arcticdb