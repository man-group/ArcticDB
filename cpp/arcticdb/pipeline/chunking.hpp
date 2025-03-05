/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <optional>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_frame.hpp>
#include <arcticdb/version/read_version_output.hpp>

#include <arcticdb/version/read_version_output.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/pipeline/read_options.hpp>
#include <arcticdb/pipeline/query.hpp>

namespace arcticdb {

class ChunkIterator {
    pipelines::index::IndexSegmentReader index_segment_reader_;
    std::shared_ptr<pipelines::PipelineContext> pipeline_context_;
    AtomKey index_key_;
    std::shared_ptr<Store> store_;
    std::any handler_data_;
    DecodePathData shared_data_;
    std::shared_ptr<pipelines::ReadQuery> read_query_;
    ReadOptions read_options_;
    size_t row_pos_ = 0;
    size_t scheduled_row_pos_ = 0;
    size_t read_ahead_;
    const size_t row_ranges_to_read_;
    size_t slice_and_key_pos_ = 0;
    std::vector<folly::Future<ReadVersionOutput>> results_;

public:
    ChunkIterator(
        pipelines::index::IndexSegmentReader&& index_segment_reader,
        std::shared_ptr<pipelines::PipelineContext> pipeline_context,
        AtomKey index_key,
        std::shared_ptr<Store> store,
        pipelines::ReadQuery& read_query,
        const ReadOptions& read_options,
        std::any& handler_data,
        DecodePathData shared_data);

    std::optional<ReadResult> next();
private:
    void do_read_ahead();

    void schedule_row_range();

};

} // namespace arcticdb