/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/version_store_objects.hpp>

namespace arcticdb::version_store {

using namespace arcticdb::entity;
using namespace arcticdb::pipelines;

struct ReadVersionOutput {
    ReadVersionOutput() = delete;
    ReadVersionOutput(VersionedItem&& versioned_item, FrameAndDescriptor&& frame_and_descriptor):
            versioned_item_(std::move(versioned_item)),
            frame_and_descriptor_(std::move(frame_and_descriptor)) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(ReadVersionOutput)

    VersionedItem versioned_item_;
    FrameAndDescriptor frame_and_descriptor_;
};

/**
 * The VersionedEngine interface contains methods that are portable between languages.
 *
 * It is currently implemented by LocalVersionedEngine, but could also be implemented with a RemoteVersionedEngine that
 * sends Protobufs down a wire (to a server with a LocalVersionedEngine talking directly to the storage).
 */
class VersionedEngine {

public:
    virtual VersionedItem update_internal(
        const StreamId& stream_id,
        const UpdateQuery & query,
        InputTensorFrame&& frame,
        bool upsert,
        bool dynamic_schema,
        bool prune_previous_versions) = 0;

    virtual VersionedItem append_internal(
        const StreamId& stream_id,
        InputTensorFrame&& frame,
        bool upsert,
        bool prune_previous_versions,
        bool validate_index) = 0;

    virtual VersionedItem delete_range_internal(
        const StreamId& stream_id,
        const UpdateQuery& query,
        bool dynamic_schema)= 0;

    virtual VersionedItem sort_index(
        const StreamId& stream_id, bool dynamic_schema) = 0;

    virtual void append_incomplete_frame(
        const StreamId& stream_id,
        InputTensorFrame&& frame) const = 0;

    virtual void append_incomplete_segment(
        const StreamId& stream_id,
        SegmentInMemory &&seg) = 0;

    virtual void remove_incomplete(
        const StreamId& stream_id
    ) = 0;

    virtual void write_parallel_frame(
        const StreamId& stream_id,
        InputTensorFrame&& frame) const = 0;

    virtual bool has_stream(
        const StreamId & stream_id
    ) = 0;

    /**
     * Delete the given index keys, and their associated data excluding those shared with keys not in the argument.
     *
     * @param checks Checks to perform on each key to find shared data
     */
    virtual void delete_tree(
        const std::vector<IndexTypeKey>& idx_to_be_deleted,
        const PreDeleteChecks& checks = default_pre_delete_checks
    ) = 0;

    virtual std::pair<VersionedItem,   TimeseriesDescriptor> restore_version(
        const StreamId& id,
        const VersionQuery& version_query
        ) = 0;

    virtual FrameAndDescriptor read_dataframe_internal(
        const std::variant<VersionedItem, StreamId>& identifier,
        ReadQuery& read_query,
        const ReadOptions& read_options) = 0;

    virtual ReadVersionOutput read_dataframe_version_internal(
        const StreamId &stream_id,
        const VersionQuery& version_query,
        ReadQuery& read_query,
        const ReadOptions& read_options) = 0;

    virtual VersionedItem write_versioned_dataframe_internal(
        const StreamId& stream_id,
        InputTensorFrame&& frame,
        bool prune_previous_versions,
        bool allow_sparse,
        bool validate_index
    ) = 0;

    /** Test-specific cut-down version of write_versioned_dataframe_internal */
    virtual VersionedItem write_individual_segment(
        const StreamId& stream_id,
        SegmentInMemory&& segment,
        bool prune_previous_versions
    ) = 0;

    virtual std::set<StreamId> list_streams_internal(
        std::optional<SnapshotId> snap_name,
        const std::optional<std::string>& regex,
        const std::optional<std::string>& prefix,
        const std::optional<bool>& opt_use_symbol_list,
        const std::optional<bool>& opt_all_symbols
    ) = 0;

    virtual IndexRange get_index_range(
        const StreamId &stream_id,
        const VersionQuery& version_query) = 0;

    virtual std::set<StreamId> get_incomplete_symbols() = 0;

    virtual std::set<StreamId> get_incomplete_refs() = 0;

    virtual std::set<StreamId> get_active_incomplete_refs() = 0;

    virtual void push_incompletes_to_symbol_list(const std::set<StreamId>& incompletes) = 0;

    virtual bool is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) = 0;

    virtual VersionedItem defragment_symbol_data(const StreamId& stream_id, std::optional<size_t> segment_size) = 0;

    virtual void move_storage(
        KeyType key_type,
        timestamp horizon,
        size_t storage_index) = 0;

    virtual StorageLockWrapper get_storage_lock(
        const StreamId& stream_id) = 0;

    virtual void delete_storage() = 0;

    virtual void configure(
        const storage::LibraryDescriptor::VariantStoreConfig & cfg) = 0;

    virtual WriteOptions get_write_options() const = 0;

    virtual std::shared_ptr<Store>& store() = 0;
    virtual const arcticdb::proto::storage::VersionStoreConfig& cfg() const  = 0;
    virtual std::shared_ptr<VersionMap>& version_map() = 0;
    virtual SymbolList& symbol_list() = 0;
    virtual void set_store(std::shared_ptr<Store> store)= 0;
    virtual timestamp latest_timestamp(const std::string& symbol) = 0;
    virtual void flush_version_map() = 0;
};

} // arcticdb::version_store
