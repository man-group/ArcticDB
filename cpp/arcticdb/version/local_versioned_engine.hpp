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
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/versioned_engine.hpp>
#include <arcticdb/entity/descriptor_item.hpp>
#include <arcticdb/entity/data_error.hpp>

#include <sstream>
namespace arcticdb::version_store {

/**
 * Implements the parent interface and provides additional methods not needed/suitable by a RemoteVersionedEngine.
 *
 * Requirements for the latter is fluid, so methods here could be lifted.
 */
using SpecificAndLatestVersionKeys = std::pair<std::shared_ptr<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>>,
                                                std::shared_ptr<std::unordered_map<StreamId, AtomKey>>>;
struct VersionIdAndDedupMapInfo{
    VersionId version_id;
    std::shared_ptr<DeDupMap> de_dup_map;
    version_store::UpdateInfo update_info;
};

struct IndexKeyAndUpdateInfo{
    entity::AtomKey index_key;
    version_store::UpdateInfo update_info;
};
class LocalVersionedEngine : public VersionedEngine {

public:
    template<class ClockType = util::SysClock>
    explicit LocalVersionedEngine(
        const std::shared_ptr<storage::Library>& library,
        const ClockType& = util::SysClock{} // Only used to allow the template variable to be inferred
        );

    virtual ~LocalVersionedEngine() = default;

    VersionedItem update_internal(
        const StreamId& stream_id,
        const UpdateQuery & query,
        InputTensorFrame&& frame,
        bool upsert,
        bool dynamic_schema,
        bool prune_previous_versions) override;

    VersionedItem append_internal(
        const StreamId& stream_id,
        InputTensorFrame&& frame,
        bool upsert,
        bool prune_previous_versions,
        bool validate_index) override;

    VersionedItem delete_range_internal(
        const StreamId& stream_id,
        const UpdateQuery& query,
        bool dynamic_schema) override;

    void append_incomplete_segment(
        const StreamId& stream_id,
        SegmentInMemory &&seg) override;

    std::pair<VersionedItem, TimeseriesDescriptor> restore_version(
        const StreamId& id,
        const VersionQuery& version_query
        ) override;

    void append_incomplete_frame(
        const StreamId& stream_id,
        InputTensorFrame&& frame) const override;

    void remove_incomplete(
        const StreamId& stream_id
    ) override;

    std::optional<VersionedItem> get_latest_version(
        const StreamId &stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    std::optional<VersionedItem> get_specific_version(
        const StreamId &stream_id,
        SignedVersionId signed_version_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    std::optional<VersionedItem> get_version_at_time(
        const StreamId& stream_id,
        timestamp as_of,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    std::optional<VersionedItem> get_version_from_snapshot(
        const StreamId& stream_id,
        const SnapshotId& snap_name
    );

    IndexRange get_index_range(
        const StreamId &stream_id,
        const VersionQuery& version_query) override;

    std::optional<VersionedItem> get_version_to_read(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options
    );

    FrameAndDescriptor read_dataframe_internal(
        const std::variant<VersionedItem, StreamId>& identifier,
        ReadQuery& read_query,
        const ReadOptions& read_options) override;

    ReadVersionOutput read_dataframe_version_internal(
        const StreamId &stream_id,
        const VersionQuery& version_query,
        ReadQuery& read_query,
        const ReadOptions& read_options) override;

    DescriptorItem read_descriptor_internal(
            const StreamId& stream_id,
            const VersionQuery& version_query,
            const ReadOptions& read_options);

    void write_parallel_frame(
        const StreamId& stream_id,
        InputTensorFrame&& frame) const override;

    bool has_stream(
        const StreamId & stream_id
    ) override;

    void delete_tree(
        const std::vector<IndexTypeKey>& idx_to_be_deleted,
        const PreDeleteChecks& checks = default_pre_delete_checks
    ) override {
        auto snapshot_map = get_master_snapshots_map(store());
        delete_trees_responsibly(idx_to_be_deleted, snapshot_map, std::nullopt, checks).get();
    };

    /**
     * Locally extends delete_tree() with more features.
     *
     * @param snapshot_map Result from get_master_snapshots_map()
     * @param snapshot_being_deleted Pass in the name and content of a SNAPSHOT(_REF) whose contents are being deleted
     * to exclude it from shared data check
     * @param dry_run Only do the check, but don't actually delete anything.
     */
    folly::Future<folly::Unit> delete_trees_responsibly(
        const std::vector<IndexTypeKey>& idx_to_be_deleted,
        const arcticdb::MasterSnapshotMap& snapshot_map,
        const std::optional<SnapshotId>& snapshot_being_deleted = std::nullopt,
        const PreDeleteChecks& check = default_pre_delete_checks,
        bool dry_run = false
    );

    std::set<StreamId> list_streams_internal(
        std::optional<SnapshotId> snap_name,
        const std::optional<std::string>& regex,
        const std::optional<std::string>& prefix,
        const std::optional<bool>& opt_use_symbol_list,
        const std::optional<bool>& opt_all_symbols
    ) override;

    VersionedItem  write_versioned_dataframe_internal(
        const StreamId& stream_id,
        InputTensorFrame&& frame,
        bool prune_previous_versions,
        bool allow_sparse,
        bool validate_index
    ) override;

    VersionedItem write_versioned_metadata_internal(
        const StreamId& stream_id,
        bool prune_previous_versions,
        arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta
    );

    folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> get_metadata(
        std::optional<AtomKey>&& key);

    folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>> get_metadata_async(
        folly::Future<std::optional<AtomKey>>&& version_fut,
        const StreamId& stream_id,
        const VersionQuery& version_query);

    folly::Future<DescriptorItem> get_descriptor(
        AtomKey&& key);

    folly::Future<DescriptorItem> get_descriptor_async(
        folly::Future<std::optional<AtomKey>>&& version_fut,
        const StreamId& stream_id,
        const VersionQuery& version_query);

    void create_column_stats_internal(
        const VersionedItem& versioned_item,
        ColumnStats& column_stats,
        const ReadOptions& read_options);

    void create_column_stats_version_internal(
        const StreamId& stream_id,
        ColumnStats& column_stats,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    void drop_column_stats_internal(
        const VersionedItem& versioned_item,
        const std::optional<ColumnStats>& column_stats_to_drop);

    void drop_column_stats_version_internal(
        const StreamId& stream_id,
        const std::optional<ColumnStats>& column_stats_to_drop,
        const VersionQuery& version_query);

    FrameAndDescriptor read_column_stats_internal(
        const VersionedItem& versioned_item);

    ReadVersionOutput read_column_stats_version_internal(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    ColumnStats get_column_stats_info_internal(
        const VersionedItem& versioned_item);

    ColumnStats get_column_stats_info_version_internal(
        const StreamId& stream_id,
        const VersionQuery& version_query);

    VersionedItem write_individual_segment(
        const StreamId& stream_id,
        SegmentInMemory&& segment,
        bool prune_previous_versions
    ) override;

    std::set<StreamId> get_incomplete_symbols() override;
    std::set<StreamId> get_incomplete_refs() override;
    std::set<StreamId> get_active_incomplete_refs() override;

    void push_incompletes_to_symbol_list(const std::set<StreamId>& incompletes) override;

    void flush_version_map() override;

    VersionedItem sort_merge_internal(
        const StreamId& stream_id,
        const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
        bool append,
        bool convert_int_to_float,
        bool via_iteration,
        bool sparsify
        );

    std::vector<folly::Future<AtomKey>> batch_write_internal(
        std::vector<VersionId> version_ids,
        const std::vector<StreamId>& stream_ids,
        std::vector<InputTensorFrame>&& frames,
        std::vector<std::shared_ptr<DeDupMap>> de_dup_maps,
        bool validate_index
    );

    std::vector<std::variant<VersionedItem, DataError>> batch_write_versioned_metadata_internal(
        const std::vector<StreamId>& stream_ids,
        bool prune_previous_versions,
        bool throw_on_error,
        std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>&& user_meta_protos);

    std::vector<std::variant<VersionedItem, DataError>> batch_append_internal(
        const std::vector<StreamId>& stream_ids,
        std::vector<InputTensorFrame>&& frames,
        bool prune_previous_versions,
        bool validate_index,
        bool upsert,
        bool throw_on_error);

    std::vector<ReadVersionOutput> batch_read_keys(
        const std::vector<AtomKey> &keys,
        const std::vector<ReadQuery> &read_queries,
        const ReadOptions& read_options);

    std::vector<std::variant<ReadVersionOutput, DataError>> batch_read_internal(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        std::vector<ReadQuery>& read_queries,
        const ReadOptions& read_options);

    std::vector<std::variant<ReadVersionOutput, DataError>> temp_batch_read_internal_direct(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        std::vector<ReadQuery>& read_queries,
        const ReadOptions& read_options);

    std::vector<std::variant<DescriptorItem, DataError>> batch_read_descriptor_internal(
            const std::vector<StreamId>& stream_ids,
            const std::vector<VersionQuery>& version_queries,
            const ReadOptions& read_options);

    std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> batch_restore_version_internal(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries);

    timestamp get_update_time_internal(const StreamId &stream_id, const VersionQuery &version_query);

    std::vector<timestamp> batch_get_update_times(
            const std::vector<StreamId>& stream_ids,
            const std::vector<VersionQuery>& version_queries);

    std::vector<std::variant<std::pair<VariantKey, std::optional<google::protobuf::Any>>, DataError>> batch_read_metadata_internal(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries,
        const ReadOptions& read_options);

    std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> read_metadata_internal(
        const StreamId& stream_id,
        const VersionQuery& version_query,
        const ReadOptions& read_options);

    bool is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) override;

    VersionedItem defragment_symbol_data(const StreamId& stream_id, std::optional<size_t> segment_size) override;
    
    StorageLockWrapper get_storage_lock(const StreamId& stream_id) override;

    void delete_storage() override;

    void configure(
        const storage::LibraryDescriptor::VariantStoreConfig & cfg) final;

    WriteOptions get_write_options() const override;

    std::string dump_versions(const StreamId& stream_id);

    timestamp latest_timestamp(const std::string& symbol) override;

    VersionedItem sort_index(const StreamId& stream_id, bool dynamic_schema) override;

    void move_storage(
        KeyType key_type,
        timestamp horizon,
        size_t storage_index) override;

    void force_release_lock(const StreamId& name);

    std::shared_ptr<DeDupMap> get_de_dup_map(
        const StreamId& stream_id,
        const std::optional<AtomKey>& maybe_prev,
        const WriteOptions& write_options
    );

    folly::Future<VersionedItem> write_index_key_to_version_map_async(
        const std::shared_ptr<VersionMap> &version_map,
        AtomKey&& index_key,
        UpdateInfo&& stream_update_info,
        bool prune_previous_versions,
        bool add_new_symbol);

    void write_version_and_prune_previous_if_needed(
        bool prune_previous_versions,
        const AtomKey& new_version,
        const std::optional<IndexTypeKey>& previous_key);

    std::vector<folly::Future<folly::Unit>> batch_write_version_and_prune_if_needed(
        const std::vector<AtomKey>& index_keys,
        const std::vector<UpdateInfo>& stream_update_info_vector,
        bool prune_previous_versions);

    std::vector<std::variant<VersionedItem, DataError>> batch_write_versioned_dataframe_internal(
        const std::vector<StreamId>& stream_ids,
        std::vector<InputTensorFrame>&& frames,
        bool prune_previous_versions,
        bool validate_index,
        bool throw_on_error
    );

    VersionIdAndDedupMapInfo create_version_id_and_dedup_map(
        const version_store::UpdateInfo&& update_info, 
        const StreamId& stream_id, 
        const WriteOptions& write_options);

    std::unordered_map<KeyType, std::pair<size_t, size_t>> scan_object_sizes();
    std::shared_ptr<Store>& _test_get_store() { return store_; }
    void _test_set_validate_version_map() {
        version_map()->set_validate(true);
    }
    void _test_set_store(std::shared_ptr<Store> store);
    std::shared_ptr<VersionMap> _test_get_version_map();

    /** Get the time used by the Store (e.g. that would be used in the AtomKey).
        For testing purposes only. */
    entity::timestamp get_store_current_timestamp_for_tests() {
        return store()->current_timestamp();
    }

protected:
    VersionedItem compact_incomplete_dynamic(
            const StreamId& stream_id,
            const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
            bool append,
            bool convert_int_to_float,
            bool via_iteration,
            bool sparsify,
            bool prune_previous_versions);

    /**
     * Take tombstoned indexes that have been pruned in the version map and perform the actual deletion
     * for indexes that are safe to delete (eg indexes contained in a snapshot are skipped).
     *
     * @param pruned_indexes Must all share the same id() and should be tombstoned.
     */
    folly::Future<folly::Unit> delete_unreferenced_pruned_indexes(
            const std::vector<AtomKey> &pruned_indexes,
            const AtomKey& key_to_keep
    );

    std::shared_ptr<Store>& store() override { return store_; }
    const arcticdb::proto::storage::VersionStoreConfig& cfg() const override { return cfg_; }
    std::shared_ptr<VersionMap>& version_map() override { return version_map_; }
    SymbolList& symbol_list() override { return *symbol_list_; }
    std::shared_ptr<SymbolList> symbol_list_ptr() { return symbol_list_; }

    void set_store(std::shared_ptr<Store> store) override {
        store_ = std::move(store) ;
    }

    /**
     * Get the queried, if specified, otherwise the latest, versions of index keys for each specified stream.
     * @param version_queries Only explicit versions are supported at the moment. The implementation currently
     * accepts deleted versions (e.g. to support reading snapshots) and it's the caller's responsibility to verify.
     * A pair of std unordered maps are returned. The first one contains all the Atom keys for those queries that we 
     * have specified a version. The second one contains all the Atom keys of the last undeleted version for those 
     * queries that we haven't specified any version.
     */
    SpecificAndLatestVersionKeys get_stream_index_map(
        const std::vector<StreamId>& stream_ids,
        const std::vector<VersionQuery>& version_queries);

private:

    std::shared_ptr<Store> store_;
    arcticdb::proto::storage::VersionStoreConfig cfg_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;
    std::optional<std::string> license_key_;
};

} // arcticdb::version_store
