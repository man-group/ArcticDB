/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/streaming_read_generator.hpp>
#include <arcticdb/version/version_core.hpp>

// TODO: Consider putting in version_store namespace
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
    handler_data_(handler_data) {
    // TODO: Call this lazily rather than eagerly
    ensure_index_information();
    log::version().debug("StreamingReadGenerator initialised");
}

void StreamingReadGenerator::ensure_index_information() {
    if (index_information_) {
        return;
    }
    // TODO: This is definitely the wrong place to be releasing the GIL
    py::gil_scoped_release release_gil;
    auto opt_versioned_item = engine_->get_version_to_read(stream_id_, version_query_);
    // TODO: Check this is the same exception type as the regular read path
    missing_data::check<ErrorCode::E_NO_SUCH_VERSION>(
            opt_versioned_item.has_value(), "TODO: Copy message from regular read path"
    );
    // TODO: Does fetch_index_and_column_stats always return IndexInformation? If so, change API
    index_information_ = std::get<std::shared_ptr<IndexInformation>>(
            version_store::fetch_index_and_column_stats(engine_->store(), *opt_versioned_item, *read_query_).get()
    );
}
} // namespace arcticdb