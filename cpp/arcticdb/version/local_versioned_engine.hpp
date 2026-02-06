/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/schema_item.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/write_options.hpp>
#include <arcticdb/entity/versioned_item.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/versioned_engine.hpp>
#include <arcticdb/version/version_functions.hpp>
#include <arcticdb/entity/descriptor_item.hpp>
#include <arcticdb/entity/data_error.hpp>

namespace arcticdb::version_store {

using VersionedItemOrError = std::variant<VersionedItem, DataError>;

/**
 * Implements the parent interface and provides additional methods not needed/suitable by a RemoteVersionedEngine.
 *
 * Requirements for the latter is fluid, so methods here could be lifted.
 */
using SpecificAndLatestVersionKeys = std::pair<
        std::shared_ptr<std::unordered_map<std::pair<StreamId, VersionId>, AtomKey>>,
        std::shared_ptr<std::unordered_map<StreamId, AtomKey>>>;
struct VersionIdAndDedupMapInfo {
    VersionId version_id;
    std::shared_ptr<DeDupMap> de_dup_map;
    version_store::UpdateInfo update_info;
};

struct IndexKeyAndUpdateInfo {
    entity::AtomKey index_key;
    version_store::UpdateInfo update_info;
};

struct KeySizesInfo {
    size_t count;
    size_t compressed_size; // bytes
};

folly::Future<folly::Unit> delete_trees_responsibly(
        std::shared_ptr<Store> store, std::shared_ptr<VersionMap>& version_map,
        const std::vector<IndexTypeKey>& orig_keys_to_delete, const arcticdb::MasterSnapshotMap& snapshot_map,
        const std::optional<SnapshotId>& snapshot_being_deleted = std::nullopt,
        const PreDeleteChecks& check = default_pre_delete_checks, const bool dry_run = false
);

namespace details {

struct ListStreamsVersionStoreObjects {
    std::shared_ptr<Store>& store_;
    SymbolList& symbol_list_;
    const arcticc::pb2::storage_pb2::VersionStoreConfig& cfg_;
    std::shared_ptr<VersionMap>& version_map_;
};

template<StreamIdSet R>
R list_streams_internal_implementation(
        std::optional<SnapshotId> snap_name, const std::optional<std::string>& regex,
        const std::optional<std::string>& prefix, const std::optional<bool>& use_symbol_list,
        const std::optional<bool>& all_symbols, ListStreamsVersionStoreObjects& version_store_objects
) {
    ARCTICDB_SAMPLE(ListStreamsInternal, 0)
    R res{};

    if (snap_name) {
        res = list_streams_in_snapshot<R>(version_store_objects.store_, *snap_name);
    } else {
        if (use_symbol_list.value_or(version_store_objects.cfg_.symbol_list()))
            res = version_store_objects.symbol_list_.get_symbol_set<R>(version_store_objects.store_);
        else
            res = list_streams<R>(
                    version_store_objects.store_,
                    version_store_objects.version_map_,
                    prefix,
                    all_symbols.value_or(false)
            );
    }

    if (regex) {
        return filter_by_regex(res, regex);
    } else if (prefix) {
        return filter_by_regex(res, std::optional("^" + *prefix));
    } else {
        return res;
    }
}

} // namespace details

class LocalVersionedEngine : public VersionedEngine {

  public:
    LocalVersionedEngine() = default;

    template<class ClockType = util::SysClock>
    explicit LocalVersionedEngine(const std::shared_ptr<storage::Library>& library, const ClockType& = ClockType{});

    virtual ~LocalVersionedEngine() = default;

    VersionedItem update_internal(
            const StreamId& stream_id, const UpdateQuery& query, const std::shared_ptr<InputFrame>& frame, bool upsert,
            bool dynamic_schema, bool prune_previous_versions
    ) override;

    VersionedItem append_internal(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool upsert,
            bool prune_previous_versions, bool validate_index
    ) override;

    VersionedItem delete_range_internal(
            const StreamId& stream_id, const UpdateQuery& query, const DeleteRangeOptions& option
    ) override;

    void append_incomplete_segment(const StreamId& stream_id, SegmentInMemory&& seg) override;

    std::pair<VersionedItem, TimeseriesDescriptor> restore_version(
            const StreamId& id, const VersionQuery& version_query
    ) override;

    void append_incomplete_frame(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool validate_index
    ) const override;

    void remove_incomplete(const StreamId& stream_id) override;

    void remove_incompletes(const std::unordered_set<StreamId>& sids, const std::string& common_prefix);

    std::optional<VersionedItem> get_latest_version(const StreamId& stream_id);

    std::optional<VersionedItem> get_specific_version(
            const StreamId& stream_id, SignedVersionId signed_version_id, const VersionQuery& version_query
    );

    std::optional<VersionedItem> get_version_at_time(
            const StreamId& stream_id, timestamp as_of, const VersionQuery& version_query
    );

    std::optional<VersionedItem> get_version_from_snapshot(const StreamId& stream_id, const SnapshotId& snap_name);

    IndexRange get_index_range(const StreamId& stream_id, const VersionQuery& version_query) override;

    std::optional<VersionedItem> get_version_to_read(const StreamId& stream_id, const VersionQuery& version_query);

    ReadVersionWithNodesOutput read_dataframe_version_internal(
            const StreamId& stream_id, const VersionQuery& version_query, const std::shared_ptr<ReadQuery>& read_query,
            const ReadOptions& read_options, std::any& handler_data
    ) override;

    SchemaItem read_schema_internal(
            const StreamId& stream_id, const VersionQuery& version_query, const ReadOptions& read_options,
            const std::shared_ptr<ReadQuery>& read_query
    ) override;

    VersionedItem read_modify_write_internal(
            const StreamId& stream_id, const StreamId& target_stream, const VersionQuery& version_query,
            const std::shared_ptr<ReadQuery>& read_query, const ReadOptions& read_options, bool prune_previous_versions,
            std::optional<proto::descriptors::UserDefinedMetadata>&& user_meta_proto
    ) override;

    DescriptorItem read_descriptor_internal(
            const StreamId& stream_id, const VersionQuery& version_query, bool include_index_segment
    );

    StageResult write_parallel_frame(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool validate_index,
            bool sort_on_index, const std::optional<std::vector<std::string>>& sort_columns
    ) const override;

    void delete_tree(
            const std::vector<IndexTypeKey>& idx_to_be_deleted,
            const PreDeleteChecks& checks = default_pre_delete_checks
    ) override {
        auto snapshot_map = get_master_snapshots_map(store());
        delete_trees_responsibly(store(), version_map(), idx_to_be_deleted, snapshot_map, std::nullopt, checks).get();
    };

    std::set<StreamId> list_streams_internal(
            std::optional<SnapshotId> snap_name, const std::optional<std::string>& regex,
            const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
            const std::optional<bool>& opt_all_symbols
    ) override;

    std::unordered_set<StreamId> list_streams_unordered_internal(
            std::optional<SnapshotId> snap_name, const std::optional<std::string>& regex,
            const std::optional<std::string>& prefix, const std::optional<bool>& opt_use_symbol_list,
            const std::optional<bool>& opt_all_symbols
    ) override;

    size_t compact_symbol_list_internal() override;

    VersionedItem write_versioned_dataframe_internal(
            const StreamId& stream_id, const std::shared_ptr<InputFrame>& frame, bool prune_previous_versions,
            bool allow_sparse, bool validate_index
    ) override;

    VersionedItem write_segment(
            const StreamId& stream_id, SegmentInMemory&& segment, bool prune_previous_versions, Slicing slicing
    ) override;

    VersionedItem write_versioned_metadata_internal(
            const StreamId& stream_id, bool prune_previous_versions,
            arcticdb::proto::descriptors::UserDefinedMetadata&& user_meta
    );

    folly::Future<std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>>> get_metadata(
            std::optional<AtomKey>&& key
    );

    folly::Future<std::pair<VariantKey, std::optional<google::protobuf::Any>>> get_metadata_async(
            folly::Future<std::optional<AtomKey>>&& opt_index_key_fut, const StreamId& stream_id,
            const VersionQuery& version_query
    );

    folly::Future<SchemaItem> get_index(AtomKey&& k, const ReadQuery& read_query);

    folly::Future<DescriptorItem> get_descriptor(AtomKey&& key);

    folly::Future<DescriptorItem> get_descriptor_async(
            folly::Future<std::optional<AtomKey>>&& opt_index_key_fut, const StreamId& stream_id,
            const VersionQuery& version_query
    );

    void create_column_stats_internal(
            const VersionedItem& versioned_item, ColumnStats& column_stats, const ReadOptions& read_options
    );

    void create_column_stats_version_internal(
            const StreamId& stream_id, ColumnStats& column_stats, const VersionQuery& version_query,
            const ReadOptions& read_options
    );

    void drop_column_stats_internal(
            const VersionedItem& versioned_item, const std::optional<ColumnStats>& column_stats_to_drop
    );

    void drop_column_stats_version_internal(
            const StreamId& stream_id, const std::optional<ColumnStats>& column_stats_to_drop,
            const VersionQuery& version_query
    );

    FrameAndDescriptor read_column_stats_internal(const VersionedItem& versioned_item);

    ReadVersionOutput read_column_stats_version_internal(const StreamId& stream_id, const VersionQuery& version_query);

    ColumnStats get_column_stats_info_internal(const VersionedItem& versioned_item);

    ColumnStats get_column_stats_info_version_internal(const StreamId& stream_id, const VersionQuery& version_query);

    std::set<StreamId> get_incomplete_symbols() override;
    std::set<StreamId> get_incomplete_refs() override;
    std::set<StreamId> get_active_incomplete_refs() override;

    void flush_version_map() override;

    std::variant<VersionedItem, CompactionError> sort_merge_internal(
            const StreamId& stream_id,
            const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
            const CompactIncompleteParameters& parameters
    );

    folly::Future<std::vector<AtomKey>> batch_write_internal(
            std::vector<VersionId>&& version_ids, const std::vector<StreamId>& stream_ids,
            std::vector<std::shared_ptr<pipelines::InputFrame>>&& frames,
            const std::vector<std::shared_ptr<DeDupMap>>& de_dup_maps, bool validate_index
    );

    std::vector<std::variant<VersionedItem, DataError>> batch_write_versioned_metadata_internal(
            const std::vector<StreamId>& stream_ids, bool prune_previous_versions, bool throw_on_error,
            std::vector<arcticdb::proto::descriptors::UserDefinedMetadata>&& user_meta_protos
    );

    std::vector<std::variant<VersionedItem, DataError>> batch_append_internal(
            const std::vector<StreamId>& stream_ids, std::vector<std::shared_ptr<pipelines::InputFrame>>&& frames,
            bool prune_previous_versions, bool validate_index, bool upsert, bool throw_on_error
    );

    std::vector<std::variant<VersionedItem, DataError>> batch_update_internal(
            const std::vector<StreamId>& stream_ids, std::vector<std::shared_ptr<InputFrame>>&& frames,
            const std::vector<UpdateQuery>& update_queries, bool prune_previous_versions, bool upsert
    );

    std::vector<std::variant<ReadVersionWithNodesOutput, DataError>> batch_read_internal(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            std::vector<std::shared_ptr<ReadQuery>>& read_queries, const BatchReadOptions& batch_read_options,
            std::any& handler_data
    );

    MultiSymbolReadOutput batch_read_and_join_internal(
            std::shared_ptr<std::vector<StreamId>> stream_ids,
            std::shared_ptr<std::vector<VersionQuery>> version_queries,
            std::vector<std::shared_ptr<ReadQuery>>& read_queries, const ReadOptions& read_options,
            std::vector<std::shared_ptr<Clause>>&& clauses, std::any& handler_data
    );

    std::vector<std::variant<DescriptorItem, DataError>> batch_read_descriptor_internal(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            const BatchReadOptions& batch_read_options
    );

    std::vector<std::pair<VersionedItem, TimeseriesDescriptor>> batch_restore_version_internal(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
    );

    timestamp get_update_time_internal(const StreamId& stream_id, const VersionQuery& version_query);

    std::vector<timestamp> batch_get_update_times(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
    );

    std::vector<std::variant<std::pair<VariantKey, std::optional<google::protobuf::Any>>, DataError>>
    batch_read_metadata_internal(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries,
            const BatchReadOptions& batch_read_options
    );

    std::pair<std::optional<VariantKey>, std::optional<google::protobuf::Any>> read_metadata_internal(
            const StreamId& stream_id, const VersionQuery& version_query
    );

    bool is_symbol_fragmented(const StreamId& stream_id, std::optional<size_t> segment_size) override;

    VersionedItem defragment_symbol_data(
            const StreamId& stream_id, std::optional<size_t> segment_size, bool prune_previous_versions
    ) override;

    StorageLockWrapper get_storage_lock(const StreamId& stream_id) override;

    void delete_storage(const bool continue_on_error = true) override;

    void configure(const storage::LibraryDescriptor::VariantStoreConfig& cfg) final;

    WriteOptions get_write_options() const override;

    std::string dump_versions(const StreamId& stream_id);

    timestamp latest_timestamp(const std::string& symbol) override;

    VersionedItem sort_index(const StreamId& stream_id, bool dynamic_schema, bool prune_previous_versions) override;

    void move_storage(KeyType key_type, timestamp horizon, size_t storage_index) override;

    void force_release_lock(const StreamId& name);

    std::shared_ptr<DeDupMap> get_de_dup_map(
            const StreamId& stream_id, const std::optional<AtomKey>& maybe_prev, const WriteOptions& write_options
    );

    folly::Future<VersionedItem> write_index_key_to_version_map_async(
            const std::shared_ptr<VersionMap>& version_map, AtomKey&& index_key, UpdateInfo&& stream_update_info,
            bool prune_previous_versions, bool add_new_symbol
    );

    void write_version_and_prune_previous(
            bool prune_previous_versions, const AtomKey& new_version, const std::optional<IndexTypeKey>& previous_key
    );

    std::vector<std::variant<VersionedItem, DataError>> batch_write_versioned_dataframe_internal(
            const std::vector<StreamId>& stream_ids, std::vector<std::shared_ptr<pipelines::InputFrame>>&& frames,
            bool prune_previous_versions, bool validate_index, bool throw_on_error
    );

    std::vector<std::variant<version_store::TombstoneVersionResult, DataError>> batch_delete_internal(
            const std::vector<StreamId>& stream_ids, const std::vector<std::vector<VersionId>>& version_ids
    );

    std::vector<std::variant<folly::Unit, DataError>> batch_delete_symbols_internal(
            const std::vector<std::pair<StreamId, VersionId>>& symbols_to_delete
    );

    VersionIdAndDedupMapInfo create_version_id_and_dedup_map(
            const version_store::UpdateInfo&& update_info, const StreamId& stream_id, const WriteOptions& write_options
    );

    std::vector<storage::ObjectSizes> scan_object_sizes();

    std::vector<storage::ObjectSizes> scan_object_sizes_for_stream(const StreamId& stream_id);

    std::unordered_map<StreamId, std::unordered_map<KeyType, KeySizesInfo>> scan_object_sizes_by_stream();

    std::shared_ptr<Store>& _test_get_store() { return store_; }
    void _test_set_validate_version_map() { version_map()->set_validate(true); }
    void _test_set_store(std::shared_ptr<Store> store);
    std::shared_ptr<VersionMap> _test_get_version_map();

    /** Get the time used by the Store (e.g. that would be used in the AtomKey).
        For testing purposes only. */
    entity::timestamp get_store_current_timestamp_for_tests() { return store()->current_timestamp(); }

    template<typename ClockType>
    static LocalVersionedEngine _test_init_from_store(const std::shared_ptr<Store>& store, const ClockType& clock) {
        return LocalVersionedEngine(store, clock);
    }

    const arcticdb::proto::storage::VersionStoreConfig& cfg() const override { return cfg_; }

    VersionedItem merge_internal(
            const StreamId& stream_id, std::shared_ptr<InputFrame> source, bool prune_previous_versions,
            const bool upsert, const MergeStrategy& strategy, std::vector<std::string>&& on
    ) override;

  protected:
    template<class ClockType = util::SysClock>
    explicit LocalVersionedEngine(const std::shared_ptr<Store>& store, const ClockType& = ClockType{});

    std::variant<VersionedItem, CompactionError> compact_incomplete_dynamic(
            const StreamId& stream_id,
            const std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>& user_meta,
            const CompactIncompleteParameters& parameters
    ) override;

    /**
     * Take tombstoned indexes that have been pruned in the version map and perform the actual deletion
     * for indexes that are safe to delete (eg indexes contained in a snapshot are skipped).
     *
     * @param pruned_indexes Must all share the same id() and should be tombstoned.
     */
    folly::Future<folly::Unit> delete_unreferenced_pruned_indexes(
            std::vector<AtomKey>&& pruned_indexes, const AtomKey& key_to_keep
    );

    std::shared_ptr<Store>& store() override { return store_; }
    std::shared_ptr<VersionMap>& version_map() override { return version_map_; }
    SymbolList& symbol_list() override { return *symbol_list_; }
    std::shared_ptr<SymbolList> symbol_list_ptr() { return symbol_list_; }

    void set_store(std::shared_ptr<Store> store) override { store_ = std::move(store); }

    /**
     * Get the queried, if specified, otherwise the latest, versions of index keys for each specified stream.
     * @param version_queries Only explicit versions are supported at the moment. The implementation currently
     * accepts deleted versions (e.g. to support reading snapshots) and it's the caller's responsibility to verify.
     * A pair of std unordered maps are returned. The first one contains all the Atom keys for those queries that we
     * have specified a version. The second one contains all the Atom keys of the last undeleted version for those
     * queries that we haven't specified any version.
     */
    SpecificAndLatestVersionKeys get_stream_index_map(
            const std::vector<StreamId>& stream_ids, const std::vector<VersionQuery>& version_queries
    );

  private:
    void initialize(const std::shared_ptr<storage::Library>& library);
    void add_to_symbol_list_on_compaction(
            const StreamId& stream_id, const CompactIncompleteParameters& parameters, const UpdateInfo& update_info
    );

    std::shared_ptr<Store> store_;
    arcticdb::proto::storage::VersionStoreConfig cfg_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;
    std::optional<std::string> license_key_;
};

} // namespace arcticdb::version_store
