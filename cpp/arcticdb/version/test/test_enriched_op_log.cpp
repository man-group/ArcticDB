/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <functional>
#include <mutex>
#include <stdexcept>

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/enriched_op_log_payload.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/version/version_map.hpp>

namespace arcticdb {

namespace {

proto::encoding::VariantCodec make_lz4_codec() {
    proto::encoding::VariantCodec codec;
    codec.mutable_lz4();
    return codec;
}

std::shared_ptr<storage::Library> make_memory_library(EncodingVersion enc_ver) {
    arcticdb::proto::storage::VersionStoreConfig vs_cfg;
    vs_cfg.set_encoding_version(static_cast<uint32_t>(enc_ver));
    vs_cfg.mutable_write_options()->mutable_sync_passive()->set_enabled(true);

    auto path = storage::LibraryPath::from_delim_path("test_enriched_op_log", '.');
    storage::Storages::StorageVector vec{std::make_shared<storage::memory::MemoryStorage>(
            path, storage::OpenMode::DELETE, storage::memory::MemoryStorage::Config{}
    )};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    return std::make_shared<storage::Library>(path, std::move(storages), vs_cfg);
}

Segment make_encoded_test_segment(const StreamId& id, size_t rows, EncodingVersion enc_ver) {
    auto desc = stream::stream_descriptor(
            id, stream::RowCountIndex(), {entity::scalar_field(entity::DataType::INT64, "val")}
    );
    SegmentInMemory seg{desc};
    for (size_t i = 0; i < rows; ++i) {
        seg.set_scalar(0, static_cast<int64_t>(i * 7 + 1));
        seg.end_row();
    }
    return encode_dispatch(std::move(seg), make_lz4_codec(), enc_ver);
}

uint64_t memory_storage_op_count(
        const query_stats::StorageStatsMap& smap, query_stats::StorageOp op, const std::string& key_type
) {
    auto bit = smap.find(query_stats::StorageBackend::Memory);
    if (bit == smap.end()) {
        return 0;
    }
    auto oit = bit->second.find(op);
    if (oit == bit->second.end()) {
        return 0;
    }
    auto kit = oit->second.find(key_type);
    if (kit == oit->second.end()) {
        return 0;
    }
    return kit->second.total_count();
}

class RecordingInMemoryStore : public InMemoryStore {
  public:
    using InMemoryStore::write;

    struct WriteEvent {
        KeyType key_type;
        StreamId stream_id;
        std::optional<VersionId> version_id;
        std::optional<IndexValue> start_index;
    };

    folly::Future<VariantKey> write(
            KeyType key_type, VersionId gen_id, const StreamId& stream_id, IndexValue start_index, IndexValue end_index,
            SegmentInMemory&& segment
    ) override {
        if (on_before_write_) {
            on_before_write_(key_type, stream_id, gen_id, start_index);
        }
        {
            std::lock_guard lock(events_mutex_);
            write_events_.push_back(WriteEvent{key_type, stream_id, gen_id, start_index});
        }
        return InMemoryStore::write(
                key_type, gen_id, stream_id, std::move(start_index), std::move(end_index), std::move(segment)
        );
    }

    folly::Future<entity::VariantKey> write(
            stream::KeyType key_type, const StreamId& stream_id, SegmentInMemory&& segment
    ) override {
        if (on_before_write_) {
            on_before_write_(key_type, stream_id, std::nullopt, std::nullopt);
        }
        {
            std::lock_guard lock(events_mutex_);
            write_events_.push_back(WriteEvent{key_type, stream_id, std::nullopt, std::nullopt});
        }
        return InMemoryStore::write(key_type, stream_id, std::move(segment));
    }

    void set_on_before_write(
            std::function<void(KeyType, const StreamId&, std::optional<VersionId>, std::optional<IndexValue>)> cb
    ) {
        on_before_write_ = std::move(cb);
    }

    void clear_write_events() {
        std::lock_guard lock(events_mutex_);
        write_events_.clear();
    }

    std::vector<WriteEvent> write_events() const {
        std::lock_guard lock(events_mutex_);
        return write_events_;
    }

  private:
    mutable std::mutex events_mutex_;
    std::vector<WriteEvent> write_events_;
    std::function<void(KeyType, const StreamId&, std::optional<VersionId>, std::optional<IndexValue>)> on_before_write_;
};

} // namespace

TEST(EnrichedOpLog, PayloadEncodeDecodeRoundTripV1AndV2) {
    for (EncodingVersion enc_ver : {EncodingVersion::V1, EncodingVersion::V2}) {
        auto idx_seg = make_encoded_test_segment("sym_rt", 5, enc_ver);
        auto cstats_seg = make_encoded_test_segment("sym_rt", 2, enc_ver);

        auto prev_key = atom_key_builder()
                                .version_id(0)
                                .creation_ts(1000)
                                .content_hash(0x1111)
                                .start_index(0)
                                .end_index(10)
                                .build("sym_rt", KeyType::TABLE_INDEX);
        auto new_key = atom_key_builder()
                               .version_id(1)
                               .creation_ts(2000)
                               .content_hash(0x2222)
                               .start_index(0)
                               .end_index(20)
                               .build("sym_rt", KeyType::TABLE_INDEX);
        auto data_key = atom_key_builder()
                                .version_id(1)
                                .creation_ts(1999)
                                .content_hash(0x3333)
                                .start_index(10)
                                .end_index(20)
                                .build("sym_rt", KeyType::TABLE_DATA);
        auto cstats_key = atom_key_builder()
                                  .version_id(1)
                                  .creation_ts(2000)
                                  .content_hash(0x2222)
                                  .start_index(0)
                                  .end_index(20)
                                  .build("sym_rt", KeyType::COLUMN_STATS);

        EnrichedOpLogPayload in;
        in.new_index_key = new_key;
        in.prev_index_key = prev_key;
        in.is_clean_chain_successor = true;
        in.compressed_index_bytes = serialize_segment_to_bytes(idx_seg);
        in.newly_written_data_keys = {data_key};
        in.has_column_stats = true;
        in.column_stats_key = cstats_key;
        in.column_stats_bytes = serialize_segment_to_bytes(cstats_seg);

        auto wrapped = make_enriched_op_log_segment(WriteVersionId, in);
        auto out_opt = extract_enriched_op_log_payload(wrapped);
        ASSERT_TRUE(out_opt.has_value());
        const auto& out = *out_opt;

        EXPECT_EQ(out.new_index_key, in.new_index_key);
        EXPECT_EQ(out.prev_index_key, in.prev_index_key);
        EXPECT_TRUE(out.is_clean_chain_successor);
        EXPECT_EQ(out.compressed_index_bytes, in.compressed_index_bytes);
        EXPECT_EQ(out.newly_written_data_keys, in.newly_written_data_keys);
        EXPECT_TRUE(out.has_column_stats);
        EXPECT_EQ(out.column_stats_key, cstats_key);
        EXPECT_EQ(out.column_stats_bytes, in.column_stats_bytes);

        // Verify deserialized compressed segments decode cleanly and preserve serialize_header() preamble.
        auto restored_idx = deserialize_segment_from_bytes(out.compressed_index_bytes);
        ASSERT_TRUE(restored_idx.has_value());
        auto [hdr_ptr, hdr_sz, hdr_buf] = restored_idx->serialize_header();
        EXPECT_NE(hdr_ptr, nullptr);
        EXPECT_EQ(hdr_sz, out.compressed_index_bytes.size());
        auto decoded_idx = decode_segment(*restored_idx);
        EXPECT_EQ(decoded_idx.row_count(), 5u);

        auto restored_cstats = deserialize_segment_from_bytes(out.column_stats_bytes);
        ASSERT_TRUE(restored_cstats.has_value());
        auto decoded_cstats = decode_segment(*restored_cstats);
        EXPECT_EQ(decoded_cstats.row_count(), 2u);
        EXPECT_EQ(decoded_cstats.scalar_at<int64_t>(1, 0).value(), 8);
    }
}

TEST(EnrichedOpLog, CorruptTruncatedAndLegacyEmptyPayloadsReturnNullopt) {
    // Legacy empty 0-row op-log segment returns std::nullopt.
    SegmentInMemory legacy_seg{log_stream_descriptor(WriteVersionId)};
    EXPECT_FALSE(extract_enriched_op_log_payload(legacy_seg).has_value());

    // Valid payload, then mutate / truncate / append garbage.
    auto idx_seg = make_encoded_test_segment("sym_corrupt", 3, EncodingVersion::V2);
    EnrichedOpLogPayload in;
    in.new_index_key = atom_key_builder().version_id(0).creation_ts(100).content_hash(99).build(
            "sym_corrupt", KeyType::TABLE_INDEX
    );
    in.is_clean_chain_successor = true;
    in.compressed_index_bytes = serialize_segment_to_bytes(idx_seg);

    auto encoded = encode_enriched_op_log_payload(in);
    ASSERT_TRUE(decode_enriched_op_log_payload(encoded).has_value());

    // Flip 1 bit in body -> checksum mismatch.
    auto flipped = encoded;
    flipped.back() ^= 0x01u;
    EXPECT_FALSE(decode_enriched_op_log_payload(flipped).has_value());

    // Bad magic.
    auto bad_magic = encoded;
    bad_magic[0] = 'X';
    EXPECT_FALSE(decode_enriched_op_log_payload(bad_magic).has_value());

    // Truncated at every prefix.
    for (size_t cut = 0; cut < encoded.size(); cut += std::max<size_t>(1, encoded.size() / 17)) {
        EXPECT_FALSE(decode_enriched_op_log_payload(std::span<const uint8_t>(encoded.data(), cut)).has_value());
    }

    // Trailing garbage byte.
    auto trailing = encoded;
    trailing.push_back(0xAB);
    EXPECT_FALSE(decode_enriched_op_log_payload(trailing).has_value());
}

TEST(EnrichedOpLog, WriterMetadataOnlyBackwardsCompatAndZeroExtraStorageWrites) {
    auto write_symbol = [](const std::shared_ptr<Store>& store,
                           const std::shared_ptr<VersionMap>& vm,
                           const std::string& sym,
                           VersionId vid,
                           size_t rows,
                           const std::optional<AtomKey>& prev_key) -> AtomKey {
        auto wrapper = get_test_simple_frame(sym, rows, 0);
        auto key = pipelines::write_frame(
                           pipelines::IndexPartialKey{sym, vid}, wrapper.frame_, pipelines::FixedSlicer{}, store
                   )
                           .get();
        vm->write_version(store, key, prev_key);
        return key;
    };

    auto collect_log_keys = [](const std::shared_ptr<Store>& store) {
        std::vector<AtomKey> logs;
        static_cast<Store&>(*store).iterate_type(KeyType::LOG, [&](VariantKey&& vk) { logs.push_back(to_atom(std::move(vk))); });
        return logs;
    };

    // 1. Legacy default (VersionStore.EnrichedOpLog == 0): emits 0-row empty LOG segment.
    {
        ScopedConfig cfg({{kEnrichedOpLogConfigKey, 0}});
        auto lib = make_memory_library(EncodingVersion::V2);
        auto store = std::make_shared<async::AsyncStore<util::SysClock>>(lib, make_lz4_codec(), EncodingVersion::V2);
        auto vm = std::make_shared<VersionMap>();
        vm->set_log_changes(true);

        query_stats::CollectorOptions opts;
        opts.isolate = true;
        query_stats::QueryStatsSnapshot legacy_snap;
        {
            query_stats::CollectorSession session(opts);
            (void)write_symbol(store, vm, "sym_legacy", 0, 16, std::nullopt);
            legacy_snap = session.snapshot();
        }

        auto logs = collect_log_keys(store);
        ASSERT_EQ(logs.size(), 1u);
        auto [_, seg] = store->read_sync(logs[0], storage::ReadKeyOpts{});
        EXPECT_EQ(seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(seg).has_value());

        // 2. Enriched enabled (metadata + compressed TABLE_INDEX, ZERO extra storage writes or reads!).
        ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
        auto lib_enr = make_memory_library(EncodingVersion::V2);
        auto store_enr =
                std::make_shared<async::AsyncStore<util::SysClock>>(lib_enr, make_lz4_codec(), EncodingVersion::V2);
        auto vm_enr = std::make_shared<VersionMap>();
        vm_enr->set_log_changes(true);

        query_stats::QueryStatsSnapshot enr_snap;
        AtomKey v0_key;
        {
            query_stats::CollectorSession session(opts);
            v0_key = write_symbol(store_enr, vm_enr, "sym_small", 0, 16, std::nullopt);
            enr_snap = session.snapshot();
        }

        // Exact same number of storage writes as legacy (0 extra writer PUTs, 0 extra writer GETs!).
        EXPECT_EQ(
                memory_storage_op_count(enr_snap.storage, query_stats::StorageOp::Write, "LOG"),
                memory_storage_op_count(legacy_snap.storage, query_stats::StorageOp::Write, "LOG")
        );
        EXPECT_EQ(
                memory_storage_op_count(enr_snap.storage, query_stats::StorageOp::Write, "TABLE_INDEX"),
                memory_storage_op_count(legacy_snap.storage, query_stats::StorageOp::Write, "TABLE_INDEX")
        );
        EXPECT_EQ(
                memory_storage_op_count(enr_snap.storage, query_stats::StorageOp::Write, "TABLE_DATA"),
                memory_storage_op_count(legacy_snap.storage, query_stats::StorageOp::Write, "TABLE_DATA")
        );
        EXPECT_EQ(memory_storage_op_count(enr_snap.storage, query_stats::StorageOp::Read, "TABLE_INDEX"), 0u);
        EXPECT_EQ(memory_storage_op_count(enr_snap.storage, query_stats::StorageOp::Read, "TABLE_DATA"), 0u);

        auto enr_logs = collect_log_keys(store_enr);
        ASSERT_EQ(enr_logs.size(), 1u);
        EXPECT_EQ(enr_logs[0].id(), StreamId{WriteVersionId});
        EXPECT_EQ(enr_logs[0].version_id(), 0u);
        EXPECT_EQ(enr_logs[0].start_index(), IndexValue{std::string{"sym_small"}});

        auto [__, enr_seg] = store_enr->read_sync(enr_logs[0], storage::ReadKeyOpts{});
        auto payload_v0 = extract_enriched_op_log_payload(enr_seg);
        ASSERT_TRUE(payload_v0.has_value());
        EXPECT_EQ(payload_v0->new_index_key, v0_key);
        EXPECT_FALSE(payload_v0->prev_index_key.has_value());
        EXPECT_TRUE(payload_v0->is_clean_chain_successor);
        EXPECT_FALSE(payload_v0->compressed_index_bytes.empty());
        ASSERT_EQ(payload_v0->newly_written_data_keys.size(), 1u);

        // Write v1 -> carries prev_index_key == v0_key and compressed TABLE_INDEX.
        auto v1_key = write_symbol(store_enr, vm_enr, "sym_small", 1, 16, v0_key);

        auto logs_after_v1 = collect_log_keys(store_enr);
        auto v1_log_it = std::find_if(logs_after_v1.begin(), logs_after_v1.end(), [](const AtomKey& k) {
            return k.version_id() == 1u;
        });
        ASSERT_NE(v1_log_it, logs_after_v1.end());
        auto [___, v1_log_seg] = store_enr->read_sync(*v1_log_it, storage::ReadKeyOpts{});
        auto payload_v1 = extract_enriched_op_log_payload(v1_log_seg);
        ASSERT_TRUE(payload_v1.has_value());
        EXPECT_EQ(payload_v1->new_index_key, v1_key);
        ASSERT_TRUE(payload_v1->prev_index_key.has_value());
        EXPECT_EQ(*payload_v1->prev_index_key, v0_key);
        EXPECT_TRUE(payload_v1->is_clean_chain_successor);
        EXPECT_FALSE(payload_v1->compressed_index_bytes.empty());
        ASSERT_EQ(payload_v1->newly_written_data_keys.size(), 1u);

        // Write v2 with tiny MaxIndexBytes (16 bytes) -> overflows index cap and falls back to legacy empty LOG!
        ScopedConfig tiny_idx_cfg({{kEnrichedOpLogConfigKey, 1}, {kEnrichedOpLogMaxIndexBytesConfigKey, 16}});
        (void)write_symbol(store_enr, vm_enr, "sym_small", 2, 64, v1_key);

        auto logs_after_v2 = collect_log_keys(store_enr);
        auto v2_log_it = std::find_if(logs_after_v2.begin(), logs_after_v2.end(), [](const AtomKey& k) {
            return k.version_id() == 2u;
        });
        ASSERT_NE(v2_log_it, logs_after_v2.end());
        auto [____, v2_log_seg] = store_enr->read_sync(*v2_log_it, storage::ReadKeyOpts{});
        EXPECT_EQ(v2_log_seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(v2_log_seg).has_value());
    }
}

TEST(EnrichedOpLog, WriteAndPrunePreviousAfterDeleteFallsBackToLegacyOpLog) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
    auto lib = make_memory_library(EncodingVersion::V2);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(lib, make_lz4_codec(), EncodingVersion::V2);
    auto vm = std::make_shared<VersionMap>();
    vm->set_log_changes(true);

    auto write_frame_key = [&](const std::string& sym, VersionId vid, size_t rows) -> AtomKey {
        auto wrapper = get_test_simple_frame(sym, rows, 0);
        return pipelines::write_frame(
                       pipelines::IndexPartialKey{sym, vid}, wrapper.frame_, pipelines::FixedSlicer{}, store
               )
                .get();
    };

    auto find_write_log = [&](const std::string& sym, VersionId vid) -> std::optional<AtomKey> {
        std::optional<AtomKey> found;
        static_cast<Store&>(*store).iterate_type(KeyType::LOG, [&](VariantKey&& vk) {
            auto k = to_atom(std::move(vk));
            if (k.id() == StreamId{WriteVersionId} && k.start_index() == IndexValue{sym} && k.version_id() == vid) {
                found = std::move(k);
            }
        });
        return found;
    };

    // 1. Brand-new symbol with prune_previous_versions still emits an enriched op-log.
    {
        const std::string sym = "sym_prune_brand_new";
        auto v0_key = write_frame_key(sym, 0, 8);
        auto pruned = vm->write_and_prune_previous(store, v0_key, std::nullopt);
        EXPECT_TRUE(pruned.empty());

        auto log_key = find_write_log(sym, 0);
        ASSERT_TRUE(log_key.has_value());
        auto [_, log_seg] = store->read_sync(*log_key, storage::ReadKeyOpts{});
        auto payload = extract_enriched_op_log_payload(log_seg);
        ASSERT_TRUE(payload.has_value());
        EXPECT_EQ(payload->new_index_key, v0_key);
        EXPECT_FALSE(payload->prev_index_key.has_value());
        EXPECT_TRUE(payload->is_clean_chain_successor);
    }

    // 2. Writing with prune_previous_versions after delete_all_versions must NOT emit an enriched
    //    is_clean_chain_successor=true payload (both with warm cache and after cold cache reload).
    {
        const std::string sym = "sym_prune_after_delete";
        auto v0_key = write_frame_key(sym, 0, 8);
        vm->write_version(store, v0_key, std::nullopt);
        vm->delete_all_versions(store, sym);

        auto v1_key = write_frame_key(sym, 1, 8);
        auto pruned = vm->write_and_prune_previous(store, v1_key, std::nullopt);
        EXPECT_TRUE(pruned.empty());

        auto v1_log = find_write_log(sym, 1);
        ASSERT_TRUE(v1_log.has_value());
        auto [_, v1_log_seg] = store->read_sync(*v1_log, storage::ReadKeyOpts{});
        EXPECT_EQ(v1_log_seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(v1_log_seg).has_value());
        EXPECT_FALSE(store->take_captured_version_write(sym, 1).has_value());

        // Delete again and flush VersionMap cache so write_and_prune_previous reloads cold from storage.
        vm->delete_all_versions(store, sym);
        vm->flush();

        auto v2_key = write_frame_key(sym, 2, 8);
        auto pruned_cold = vm->write_and_prune_previous(store, v2_key, std::nullopt);
        EXPECT_TRUE(pruned_cold.empty());

        auto v2_log = find_write_log(sym, 2);
        ASSERT_TRUE(v2_log.has_value());
        auto [__, v2_log_seg] = store->read_sync(*v2_log, storage::ReadKeyOpts{});
        EXPECT_EQ(v2_log_seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(v2_log_seg).has_value());
        EXPECT_FALSE(store->take_captured_version_write(sym, 2).has_value());
    }

    // 3. Writing with prune_previous_versions after an individual tombstone also falls back to legacy op-log.
    {
        const std::string sym = "sym_prune_after_tombstone";
        auto v0_key = write_frame_key(sym, 0, 8);
        vm->write_version(store, v0_key, std::nullopt);
        auto entry = vm->check_reload(
                store, sym, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__
        );
        vm->write_tombstones(store, {v0_key}, sym, entry);

        auto v1_key = write_frame_key(sym, 1, 8);
        auto pruned = vm->write_and_prune_previous(store, v1_key, std::nullopt);
        EXPECT_TRUE(pruned.empty());

        auto v1_log = find_write_log(sym, 1);
        ASSERT_TRUE(v1_log.has_value());
        auto [_, v1_log_seg] = store->read_sync(*v1_log, storage::ReadKeyOpts{});
        EXPECT_EQ(v1_log_seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(v1_log_seg).has_value());
        EXPECT_FALSE(store->take_captured_version_write(sym, 1).has_value());
    }
}

TEST(EnrichedOpLog, TombstoneFromKeyOrAllWritesVersionRefBeforeTombstoneAllLog) {
    auto store = std::make_shared<RecordingInMemoryStore>();
    auto vm = std::make_shared<VersionMap>();
    vm->set_log_changes(true);
    vm->set_validate(true);

    auto make_index_key = [](const StreamId& sym, VersionId vid) {
        return atom_key_builder()
                .version_id(vid)
                .creation_ts(static_cast<timestamp>(vid + 1) * 1000)
                .content_hash(vid + 10)
                .start_index(0)
                .end_index(10)
                .build(sym, KeyType::TABLE_INDEX);
    };

    // 1. Full tombstone_from_key_or_all must commit VERSION_REF before writing KeyType::LOG (TOMBSTONE_ALL).
    const StreamId sym{"sym_tomb_order"};
    auto v0 = make_index_key(sym, 0);
    auto v1 = make_index_key(sym, 1);
    vm->write_version(store, v0, std::nullopt);
    vm->write_version(store, v1, v0);
    store->clear_write_events();

    bool saw_tombstone_all_log = false;
    bool ref_already_tombstoned = false;
    store->set_on_before_write([&](KeyType kt, const StreamId& id, std::optional<VersionId> vid,
                                   std::optional<IndexValue>) {
        if (kt == KeyType::LOG && id == StreamId{TombstoneAllVersionId}) {
            saw_tombstone_all_log = true;
            EXPECT_EQ(vid, std::optional<VersionId>{1});
            VersionMapEntry ref_entry(sym);
            read_symbol_ref(store, sym, ref_entry);
            ref_already_tombstoned = !ref_entry.empty() && !ref_entry.keys_.empty() &&
                                     ref_entry.keys_[0].type() == KeyType::TOMBSTONE_ALL &&
                                     ref_entry.keys_[0].version_id() == 1u;
        }
    });

    vm->tombstone_from_key_or_all(store, sym);
    store->set_on_before_write(nullptr);

    EXPECT_TRUE(saw_tombstone_all_log);
    EXPECT_TRUE(ref_already_tombstoned);

    auto events = store->write_events();
    ASSERT_EQ(events.size(), 3u);
    EXPECT_EQ(events[0].key_type, KeyType::VERSION);
    EXPECT_EQ(events[1].key_type, KeyType::VERSION_REF);
    EXPECT_EQ(events[1].stream_id, sym);
    EXPECT_EQ(events[2].key_type, KeyType::LOG);
    EXPECT_EQ(events[2].stream_id, StreamId{TombstoneAllVersionId});
    EXPECT_EQ(events[2].version_id, std::optional<VersionId>{1});
    EXPECT_EQ(events[2].start_index, std::optional<IndexValue>{IndexValue{sym}});

    // 2. Repeating tombstone_from_key_or_all when no undeleted versions remain emits no TOMBSTONE_ALL log.
    store->clear_write_events();
    vm->tombstone_from_key_or_all(store, sym);
    for (const auto& ev : store->write_events()) {
        EXPECT_NE(ev.key_type, KeyType::LOG);
    }

    // 3. Partial tombstone_from_key_or_all (explicit first_key_to_tombstone) also writes VERSION_REF before LOG.
    const StreamId sym_partial{"sym_tomb_partial"};
    auto pv0 = make_index_key(sym_partial, 0);
    auto pv1 = make_index_key(sym_partial, 1);
    vm->write_version(store, pv0, std::nullopt);
    vm->write_version(store, pv1, pv0);
    store->clear_write_events();

    vm->tombstone_from_key_or_all(store, sym_partial, pv0);
    auto partial_events = store->write_events();
    ASSERT_EQ(partial_events.size(), 3u);
    EXPECT_EQ(partial_events[0].key_type, KeyType::VERSION);
    EXPECT_EQ(partial_events[1].key_type, KeyType::VERSION_REF);
    EXPECT_EQ(partial_events[2].key_type, KeyType::LOG);
    EXPECT_EQ(partial_events[2].stream_id, StreamId{TombstoneAllVersionId});
    EXPECT_EQ(partial_events[2].version_id, std::optional<VersionId>{0});

    // 4. If VERSION_REF write fails, no TOMBSTONE_ALL op-log may be emitted.
    const StreamId sym_fail{"sym_tomb_ref_fail"};
    auto fv0 = make_index_key(sym_fail, 0);
    vm->write_version(store, fv0, std::nullopt);
    store->clear_write_events();

    store->set_on_before_write([](KeyType kt, const StreamId&, std::optional<VersionId>, std::optional<IndexValue>) {
        if (kt == KeyType::VERSION_REF) {
            throw std::runtime_error("injected VERSION_REF write failure");
        }
    });
    EXPECT_THROW(vm->tombstone_from_key_or_all(store, sym_fail), std::runtime_error);
    store->set_on_before_write(nullptr);

    for (const auto& ev : store->write_events()) {
        EXPECT_NE(ev.key_type, KeyType::LOG);
    }
}

} // namespace arcticdb
