/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <xxhash.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <set>
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
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_core.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/version/version_map.hpp>

namespace arcticdb {

namespace {

proto::encoding::VariantCodec make_lz4_codec() {
    proto::encoding::VariantCodec codec;
    codec.mutable_lz4();
    return codec;
}

std::shared_ptr<storage::Library> make_memory_library(EncodingVersion enc_ver, bool sync_passive = true) {
    arcticdb::proto::storage::VersionStoreConfig vs_cfg;
    vs_cfg.set_encoding_version(static_cast<uint32_t>(enc_ver));
    vs_cfg.mutable_write_options()->mutable_sync_passive()->set_enabled(sync_passive);

    auto path = storage::LibraryPath::from_delim_path("test_enriched_op_log", '.');
    storage::Storages::StorageVector vec{std::make_shared<storage::memory::MemoryStorage>(
            path, storage::OpenMode::DELETE, storage::memory::MemoryStorage::Config{}
    )};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    return std::make_shared<storage::Library>(path, std::move(storages), vs_cfg);
}

std::shared_ptr<async::AsyncStore<util::SysClock>> make_test_async_store(
        EncodingVersion enc_ver = EncodingVersion::V2, bool sync_passive = true
) {
    return std::make_shared<async::AsyncStore<util::SysClock>>(
            make_memory_library(enc_ver, sync_passive), make_lz4_codec(), enc_ver
    );
}

Segment make_encoded_test_segment(const StreamId& id, size_t rows, EncodingVersion enc_ver) {
    auto desc = stream::stream_descriptor(
            id, stream::RowCountIndex(), {entity::scalar_field(entity::DataType::INT64, "val")}
    );
    SegmentInMemory seg{desc};
    for (size_t i = 0; i < rows; ++i) {
        seg.set_scalar(0, static_cast<int64_t>(i) * 7 + 1);
        seg.end_row();
    }
    return encode_dispatch(std::move(seg), make_lz4_codec(), enc_ver);
}

AtomKey make_test_data_key(const std::string& sym, VersionId vid, uint64_t hash) {
    return atom_key_builder()
            .version_id(vid)
            .creation_ts(static_cast<timestamp>(hash) * 100)
            .content_hash(hash)
            .start_index(0)
            .end_index(10)
            .build(sym, KeyType::TABLE_DATA);
}

AtomKey make_test_index_key(const StreamId& sym, VersionId vid) {
    return atom_key_builder()
            .version_id(vid)
            .creation_ts(static_cast<timestamp>(vid + 1) * 1000)
            .content_hash(vid + 10)
            .start_index(0)
            .end_index(10)
            .build(sym, KeyType::TABLE_INDEX);
}

std::optional<AtomKey> find_write_log(const std::shared_ptr<Store>& store, const std::string& sym, VersionId vid) {
    std::optional<AtomKey> found;
    static_cast<Store&>(*store).iterate_type(KeyType::LOG, [&](VariantKey&& vk) {
        auto k = to_atom(std::move(vk));
        if (k.id() == StreamId{WriteVersionId} && k.start_index() == IndexValue{sym} && k.version_id() == vid) {
            found = std::move(k);
        }
    });
    return found;
}

SegmentInMemory read_write_log_segment(const std::shared_ptr<Store>& store, const std::string& sym, VersionId vid) {
    auto log_key = find_write_log(store, sym, vid);
    util::check(log_key.has_value(), "Expected write op-log for symbol {} version {}", sym, vid);
    auto [ignored_key, seg] = store->read_sync(*log_key, storage::ReadKeyOpts{});
    (void)ignored_key;
    return std::move(seg);
}

AtomKey write_simple_frame(const std::shared_ptr<Store>& store, const std::string& sym, VersionId vid, size_t rows) {
    auto wrapper = get_test_simple_frame(sym, rows, 0);
    return pipelines::write_frame(pipelines::IndexPartialKey{sym, vid}, wrapper.frame_, pipelines::FixedSlicer{}, store)
            .get();
}

uint64_t memory_storage_op_count(
        const query_stats::QueryStats::QueryStatsOutput& stats, const std::string& op_name, const std::string& key_type
) {
    auto sit = stats.find("storage_operations");
    if (sit == stats.end()) {
        return 0;
    }
    auto oit = sit->second.find(op_name);
    if (oit == sit->second.end()) {
        return 0;
    }
    auto kit = oit->second.find(key_type);
    if (kit == oit->second.end()) {
        return 0;
    }
    auto cit = kit->second.find("count");
    return cit == kit->second.end() ? 0 : cit->second;
}

class ScopedQueryStatsCapture {
  public:
    ScopedQueryStatsCapture() {
        auto qs = query_stats::QueryStats::instance();
        was_enabled_ = qs->is_enabled();
        qs->reset_stats();
        qs->enable();
    }
    ~ScopedQueryStatsCapture() {
        auto qs = query_stats::QueryStats::instance();
        if (!was_enabled_) {
            qs->disable();
        }
        qs->reset_stats();
    }
    query_stats::QueryStats::QueryStatsOutput snapshot() const {
        return query_stats::QueryStats::instance()->get_stats();
    }

  private:
    bool was_enabled_{false};
};

class RecordingInMemoryStore : public InMemoryStore {
  public:
    using InMemoryStore::write;

    struct WriteEvent {
        KeyType key_type;
        StreamId stream_id;
        std::optional<VersionId> version_id;
        std::optional<IndexValue> start_index;
    };

    using BeforeWriteCallback =
            std::function<void(KeyType, const StreamId&, std::optional<VersionId>, const std::optional<IndexValue>&)>;

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

    void set_on_before_write(BeforeWriteCallback cb) { on_before_write_ = std::move(cb); }

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
    BeforeWriteCallback on_before_write_;
};

class FailingAsyncInMemoryStore : public InMemoryStore {
  public:
    using InMemoryStore::write;

    folly::Future<pipelines::SliceAndKey>
    async_write(folly::Future<std::tuple<PartialKey, SegmentInMemory, pipelines::FrameSlice>>&& input_fut, const std::shared_ptr<DeDupMap>&)
            override {
        return std::move(input_fut).thenValue([this](auto&& input) {
            auto [pk, seg, slice] = std::forward<decltype(input)>(input);
            const size_t call_idx = async_write_count_.fetch_add(1);
            if (fail_on_async_write_index_.has_value() && call_idx == *fail_on_async_write_index_) {
                throw std::runtime_error("injected async_write failure");
            }
            // Assign distinct content_hash per call so orphaned vs retried TABLE_DATA keys are distinguishable.
            auto key = atom_key_builder()
                               .gen_id(pk.version_id)
                               .content_hash(0x1000 + call_idx)
                               .creation_ts(PilotedClock::nanos_since_epoch())
                               .start_index(pk.start_index)
                               .end_index(pk.end_index)
                               .build(pk.stream_id, pk.key_type);
            add_segment(key, std::move(seg));
            recorded_data_count_.fetch_add(1);
            return pipelines::SliceAndKey{std::move(slice), std::move(key)};
        });
    }

    folly::Future<VariantKey> write(
            KeyType key_type, VersionId gen_id, const StreamId& stream_id, IndexValue start_index, IndexValue end_index,
            SegmentInMemory&& segment
    ) override {
        if (fail_key_type_.has_value() && key_type == *fail_key_type_) {
            throw std::runtime_error("injected sync/async key write failure");
        }
        return InMemoryStore::write(
                key_type, gen_id, stream_id, std::move(start_index), std::move(end_index), std::move(segment)
        );
    }

    void set_fail_on_async_write_index(std::optional<size_t> idx) { fail_on_async_write_index_ = idx; }
    void set_fail_key_type(std::optional<KeyType> kt) { fail_key_type_ = kt; }
    size_t recorded_data_count() const { return recorded_data_count_.load(); }
    size_t captured_cache_size() const { return write_capture_cache()->size(); }

  private:
    std::atomic<size_t> async_write_count_{0};
    std::atomic<size_t> recorded_data_count_{0};
    std::optional<size_t> fail_on_async_write_index_;
    std::optional<KeyType> fail_key_type_;
};

} // namespace

TEST(EnrichedOpLog, PayloadEncodeDecodeRoundTripV1AndV2) {
    for (EncodingVersion enc_ver : {EncodingVersion::V1, EncodingVersion::V2}) {
        auto idx_seg = make_encoded_test_segment("sym_rt", 5, enc_ver);
        auto cstats_seg = make_encoded_test_segment("sym_rt", 2, enc_ver);

        auto prev_key = make_test_index_key("sym_rt", 0);
        auto new_key = make_test_index_key("sym_rt", 1);
        auto data_key = make_test_data_key("sym_rt", 1, 0x3333);
        auto cstats_key = version_store::index_key_to_column_stats_key(new_key);

        EnrichedOpLogPayload in;
        in.new_index_key = new_key;
        in.prev_index_key = prev_key;
        in.is_clean_chain_successor = true;
        in.compressed_index_bytes = serialize_segment_to_bytes(idx_seg);
        in.newly_written_data_keys = {data_key};
        in.column_stats_key = cstats_key;
        in.column_stats_bytes = serialize_segment_to_bytes(cstats_seg);

        auto wrapped = make_enriched_op_log_segment(WriteVersionId, encode_enriched_op_log_payload(in));
        auto out_opt = extract_enriched_op_log_payload(wrapped);
        ASSERT_TRUE(out_opt.has_value());
        const auto& out = *out_opt;

        EXPECT_EQ(out.new_index_key, in.new_index_key);
        EXPECT_EQ(out.prev_index_key, in.prev_index_key);
        EXPECT_TRUE(out.is_clean_chain_successor);
        EXPECT_EQ(out.compressed_index_bytes, in.compressed_index_bytes);
        EXPECT_EQ(out.newly_written_data_keys, in.newly_written_data_keys);
        EXPECT_EQ(out.column_stats_key, cstats_key);
        EXPECT_EQ(out.column_stats_bytes, in.column_stats_bytes);

        // Verify deserialized compressed segments decode cleanly and preserve serialize_header() preamble.
        auto restored_idx = deserialize_segment_from_bytes(out.compressed_index_bytes);
        ASSERT_TRUE(restored_idx.has_value());
        auto [hdr_ptr, hdr_sz, hdr_buf] = restored_idx->serialize_header();
        EXPECT_NE(hdr_ptr, nullptr);
        EXPECT_EQ(hdr_sz, out.compressed_index_bytes.size());
        EXPECT_EQ(decode_segment(*restored_idx).row_count(), 5u);

        auto restored_cstats = deserialize_segment_from_bytes(out.column_stats_bytes);
        ASSERT_TRUE(restored_cstats.has_value());
        auto decoded_cstats = decode_segment(*restored_cstats);
        EXPECT_EQ(decoded_cstats.row_count(), 2u);
        EXPECT_EQ(decoded_cstats.scalar_at<int64_t>(1, 0).value(), 8);
    }
}

TEST(EnrichedOpLog, CorruptTruncatedVersionFlagsAndWrongColumnTypeReturnNullopt) {
    // Legacy empty 0-row op-log segment returns std::nullopt.
    SegmentInMemory legacy_seg{log_stream_descriptor(WriteVersionId)};
    EXPECT_FALSE(extract_enriched_op_log_payload(legacy_seg).has_value());

    // Wrong DataType on column 0 must return std::nullopt rather than throwing.
    {
        auto wrong_desc = stream::stream_descriptor(
                WriteVersionId,
                stream::RowCountIndex(),
                {entity::scalar_field(entity::DataType::INT64, std::string{kEnrichedOpLogPayloadColumn})}
        );
        SegmentInMemory wrong_type_seg{std::move(wrong_desc)};
        wrong_type_seg.set_scalar(0, int64_t{42});
        wrong_type_seg.end_row();
        EXPECT_FALSE(extract_enriched_op_log_payload(wrong_type_seg).has_value());
    }

    auto idx_seg = make_encoded_test_segment("sym_corrupt", 3, EncodingVersion::V2);
    EnrichedOpLogPayload in;
    in.new_index_key = make_test_index_key("sym_corrupt", 0);
    in.is_clean_chain_successor = true;
    in.compressed_index_bytes = serialize_segment_to_bytes(idx_seg);

    auto encoded = encode_enriched_op_log_payload(in);
    ASSERT_TRUE(decode_enriched_op_log_payload(encoded).has_value());

    // Flip 1 bit in body -> checksum mismatch.
    auto flipped = encoded;
    flipped.back() ^= 0x01u;
    EXPECT_FALSE(decode_enriched_op_log_payload(flipped).has_value());

    // Flip kFlagIsCleanChainSuccessor in header flags -> checksum covers flags and rejects mutation.
    auto flipped_flag = encoded;
    flipped_flag[12] ^= static_cast<uint8_t>(1u << 3);
    EXPECT_FALSE(decode_enriched_op_log_payload(flipped_flag).has_value());

    // Unknown flag bit and unsupported format version (> 1).
    auto unknown_flag = encoded;
    unknown_flag[12] |= static_cast<uint8_t>(1u << 7);
    EXPECT_FALSE(decode_enriched_op_log_payload(unknown_flag).has_value());

    auto bad_version = encoded;
    bad_version[8] = 2;
    EXPECT_FALSE(decode_enriched_op_log_payload(bad_version).has_value());

    // Bad magic.
    auto bad_magic = encoded;
    bad_magic[0] = 'X';
    EXPECT_FALSE(decode_enriched_op_log_payload(bad_magic).has_value());

    // Truncated at prefixes and trailing garbage byte.
    for (size_t cut = 0; cut < encoded.size(); cut += std::max<size_t>(1, encoded.size() / 17)) {
        EXPECT_FALSE(decode_enriched_op_log_payload(std::span<const uint8_t>(encoded.data(), cut)).has_value());
    }
    auto trailing = encoded;
    trailing.push_back(0xAB);
    EXPECT_FALSE(decode_enriched_op_log_payload(trailing).has_value());
}

TEST(EnrichedOpLog, ShortPayloadWithHugeDataKeyCountAndTruncatedSegmentHeadersRejected) {
    // 1. Short valid-checksum payload claiming kMaxEnrichedOpLogKeysPerPayload keys must be rejected
    //    before reserving memory.
    {
        auto idx_seg = make_encoded_test_segment("sym_prealloc", 1, EncodingVersion::V2);
        EnrichedOpLogPayload in;
        in.new_index_key = make_test_index_key("sym_prealloc", 0);
        in.is_clean_chain_successor = true;
        in.compressed_index_bytes = serialize_segment_to_bytes(idx_seg);

        auto encoded = encode_enriched_op_log_payload(in);
        // The last 4 bytes of this payload (no column stats, 0 data keys) are num_data_keys == 0.
        ASSERT_GE(encoded.size(), 28u);
        const uint32_t huge_count = kMaxEnrichedOpLogKeysPerPayload;
        const size_t pos = encoded.size() - 4;
        encoded[pos + 0] = static_cast<uint8_t>(huge_count & 0xFFu);
        encoded[pos + 1] = static_cast<uint8_t>((huge_count >> 8) & 0xFFu);
        encoded[pos + 2] = static_cast<uint8_t>((huge_count >> 16) & 0xFFu);
        encoded[pos + 3] = static_cast<uint8_t>((huge_count >> 24) & 0xFFu);
        // Recompute XXH64 over version+flags+body so checksum passes and bounds check is exercised.
        XXH64_state_t st;
        XXH64_reset(&st, 0);
        XXH64_update(&st, encoded.data() + 8, 8);
        XXH64_update(&st, encoded.data() + 24, encoded.size() - 24);
        const uint64_t h = XXH64_digest(&st);
        for (size_t i = 0; i < 8; ++i) {
            encoded[16 + i] = static_cast<uint8_t>((h >> (i * 8)) & 0xFFu);
        }
        EXPECT_FALSE(decode_enriched_op_log_payload(encoded).has_value());
    }

    // 2. deserialize_segment_from_bytes must safely reject truncated / oversized FixedHeaders without over-read.
    for (uint16_t hdr_ver : {HEADER_VERSION_V1, HEADER_VERSION_V2, static_cast<uint16_t>(99)}) {
        FixedHeader huge_hdr{MAGIC_NUMBER, hdr_ver, 1024u * 1024u};
        std::array<uint8_t, sizeof(FixedHeader)> raw{};
        std::memcpy(raw.data(), &huge_hdr, sizeof(FixedHeader));
        EXPECT_FALSE(deserialize_segment_from_bytes(raw).has_value());
    }
    {
        FixedHeader int_overflow_hdr{MAGIC_NUMBER, HEADER_VERSION_V1, 0x80000010u};
        std::array<uint8_t, sizeof(FixedHeader)> raw{};
        std::memcpy(raw.data(), &int_overflow_hdr, sizeof(FixedHeader));
        EXPECT_FALSE(deserialize_segment_from_bytes(raw).has_value());
    }
    // Truncated valid V1 and V2 segments at every prefix must return std::nullopt cleanly.
    for (EncodingVersion enc_ver : {EncodingVersion::V1, EncodingVersion::V2}) {
        auto seg = make_encoded_test_segment("sym_trunc", 4, enc_ver);
        auto bytes = serialize_segment_to_bytes(seg);
        ASSERT_TRUE(deserialize_segment_from_bytes(bytes).has_value());
        for (size_t cut = 0; cut < bytes.size(); cut += std::max<size_t>(1, bytes.size() / 23)) {
            EXPECT_FALSE(deserialize_segment_from_bytes(std::span<const uint8_t>(bytes.data(), cut)).has_value());
        }
    }
}

TEST(EnrichedOpLog, WriterMetadataOnlyBackwardsCompatAndZeroExtraStorageWrites) {
    auto write_symbol = [](const std::shared_ptr<Store>& store,
                           const std::shared_ptr<VersionMap>& vm,
                           const std::string& sym,
                           VersionId vid,
                           size_t rows,
                           const std::optional<AtomKey>& prev_key) -> AtomKey {
        auto key = write_simple_frame(store, sym, vid, rows);
        vm->write_version(store, key, prev_key);
        return key;
    };

    // 1. Legacy default (VersionStore.EnrichedOpLog == 0): emits 0-row empty LOG segment.
    query_stats::QueryStats::QueryStatsOutput legacy_snap;
    {
        ScopedConfig cfg({{kEnrichedOpLogConfigKey, 0}});
        auto store = make_test_async_store(EncodingVersion::V2);
        auto vm = std::make_shared<VersionMap>();
        vm->set_log_changes(true);

        ScopedQueryStatsCapture capture;
        (void)write_symbol(store, vm, "sym_legacy", 0, 16, std::nullopt);
        legacy_snap = capture.snapshot();

        auto seg = read_write_log_segment(store, "sym_legacy", 0);
        EXPECT_EQ(seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(seg).has_value());
    }

    // 2. Enriched enabled (metadata + compressed TABLE_INDEX, ZERO extra storage writes or reads).
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
    auto store_enr = make_test_async_store(EncodingVersion::V2);
    auto vm_enr = std::make_shared<VersionMap>();
    vm_enr->set_log_changes(true);

    query_stats::QueryStats::QueryStatsOutput enr_snap;
    AtomKey v0_key;
    {
        ScopedQueryStatsCapture capture;
        v0_key = write_symbol(store_enr, vm_enr, "sym_small", 0, 16, std::nullopt);
        enr_snap = capture.snapshot();
    }

    EXPECT_EQ(
            memory_storage_op_count(enr_snap, "Memory_PutObject", "LOG"),
            memory_storage_op_count(legacy_snap, "Memory_PutObject", "LOG")
    );
    EXPECT_EQ(
            memory_storage_op_count(enr_snap, "Memory_PutObject", "TABLE_INDEX"),
            memory_storage_op_count(legacy_snap, "Memory_PutObject", "TABLE_INDEX")
    );
    EXPECT_EQ(
            memory_storage_op_count(enr_snap, "Memory_PutObject", "TABLE_DATA"),
            memory_storage_op_count(legacy_snap, "Memory_PutObject", "TABLE_DATA")
    );
    EXPECT_EQ(memory_storage_op_count(enr_snap, "Memory_GetObject", "TABLE_INDEX"), 0u);
    EXPECT_EQ(memory_storage_op_count(enr_snap, "Memory_GetObject", "TABLE_DATA"), 0u);

    auto payload_v0 = extract_enriched_op_log_payload(read_write_log_segment(store_enr, "sym_small", 0));
    ASSERT_TRUE(payload_v0.has_value());
    EXPECT_EQ(payload_v0->new_index_key, v0_key);
    EXPECT_FALSE(payload_v0->prev_index_key.has_value());
    EXPECT_TRUE(payload_v0->is_clean_chain_successor);
    EXPECT_FALSE(payload_v0->compressed_index_bytes.empty());
    ASSERT_EQ(payload_v0->newly_written_data_keys.size(), 1u);

    // Write v1 -> carries prev_index_key == v0_key.
    auto v1_key = write_symbol(store_enr, vm_enr, "sym_small", 1, 16, v0_key);
    auto payload_v1 = extract_enriched_op_log_payload(read_write_log_segment(store_enr, "sym_small", 1));
    ASSERT_TRUE(payload_v1.has_value());
    EXPECT_EQ(payload_v1->new_index_key, v1_key);
    EXPECT_EQ(payload_v1->prev_index_key, std::optional<AtomKey>{v0_key});
    EXPECT_TRUE(payload_v1->is_clean_chain_successor);
    ASSERT_EQ(payload_v1->newly_written_data_keys.size(), 1u);

    // Write v2 with tiny MaxIndexBytes (16 bytes) -> overflows cap and falls back to legacy empty LOG.
    {
        ScopedConfig huge_cap_cfg({{kEnrichedOpLogMaxIndexBytesConfigKey, int64_t{256} * 1024 * 1024}});
        EXPECT_EQ(enriched_op_log_max_index_bytes(), kMaxEnrichedOpLogBlobBytes);
    }
    ScopedConfig tiny_idx_cfg({{kEnrichedOpLogConfigKey, 1}, {kEnrichedOpLogMaxIndexBytesConfigKey, 16}});
    (void)write_symbol(store_enr, vm_enr, "sym_small", 2, 64, v1_key);
    auto v2_seg = read_write_log_segment(store_enr, "sym_small", 2);
    EXPECT_EQ(v2_seg.row_count(), 0u);
    EXPECT_FALSE(extract_enriched_op_log_payload(v2_seg).has_value());
}

TEST(EnrichedOpLog, WriteAndPrunePreviousAfterDeleteFallsBackToLegacyOpLog) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
    auto store = make_test_async_store(EncodingVersion::V2);
    auto vm = std::make_shared<VersionMap>();
    vm->set_log_changes(true);

    auto expect_legacy_fallback = [&](const std::string& sym, VersionId vid) {
        auto seg = read_write_log_segment(store, sym, vid);
        EXPECT_EQ(seg.row_count(), 0u);
        EXPECT_FALSE(extract_enriched_op_log_payload(seg).has_value());
        EXPECT_FALSE(store->take_captured_version_write(sym, vid).has_value());
    };

    // 1. Brand-new symbol with prune_previous_versions still emits an enriched op-log.
    {
        const std::string sym = "sym_prune_brand_new";
        auto v0_key = write_simple_frame(store, sym, 0, 8);
        EXPECT_TRUE(vm->write_and_prune_previous(store, v0_key, std::nullopt).empty());

        auto payload = extract_enriched_op_log_payload(read_write_log_segment(store, sym, 0));
        ASSERT_TRUE(payload.has_value());
        EXPECT_EQ(payload->new_index_key, v0_key);
        EXPECT_FALSE(payload->prev_index_key.has_value());
        EXPECT_TRUE(payload->is_clean_chain_successor);
    }

    // 2. Writing with prune_previous_versions after delete_all_versions falls back (warm and cold cache).
    {
        const std::string sym = "sym_prune_after_delete";
        auto v0_key = write_simple_frame(store, sym, 0, 8);
        vm->write_version(store, v0_key, std::nullopt);
        vm->delete_all_versions(store, sym);

        auto v1_key = write_simple_frame(store, sym, 1, 8);
        EXPECT_TRUE(vm->write_and_prune_previous(store, v1_key, std::nullopt).empty());
        expect_legacy_fallback(sym, 1);

        vm->delete_all_versions(store, sym);
        vm->flush();

        auto v2_key = write_simple_frame(store, sym, 2, 8);
        EXPECT_TRUE(vm->write_and_prune_previous(store, v2_key, std::nullopt).empty());
        expect_legacy_fallback(sym, 2);
    }

    // 3. Writing with prune_previous_versions after an individual tombstone also falls back.
    {
        const std::string sym = "sym_prune_after_tombstone";
        auto v0_key = write_simple_frame(store, sym, 0, 8);
        vm->write_version(store, v0_key, std::nullopt);
        auto entry =
                vm->check_reload(store, sym, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, __FUNCTION__);
        vm->write_tombstones(store, {v0_key}, sym, entry);

        auto v1_key = write_simple_frame(store, sym, 1, 8);
        EXPECT_TRUE(vm->write_and_prune_previous(store, v1_key, std::nullopt).empty());
        expect_legacy_fallback(sym, 1);
    }
}

TEST(EnrichedOpLog, TombstoneFromKeyOrAllWritesVersionRefBeforeTombstoneAllLog) {
    auto store = std::make_shared<RecordingInMemoryStore>();
    auto vm = std::make_shared<VersionMap>();
    vm->set_log_changes(true);
    vm->set_validate(true);

    // 1. Full tombstone_from_key_or_all must commit VERSION_REF before writing KeyType::LOG (TOMBSTONE_ALL).
    const StreamId sym{"sym_tomb_order"};
    auto v0 = make_test_index_key(sym, 0);
    auto v1 = make_test_index_key(sym, 1);
    vm->write_version(store, v0, std::nullopt);
    vm->write_version(store, v1, v0);
    store->clear_write_events();

    bool saw_tombstone_all_log = false;
    bool ref_already_tombstoned = false;
    store->set_on_before_write(
            [&](KeyType kt, const StreamId& id, std::optional<VersionId> vid, const std::optional<IndexValue>&) {
                if (kt == KeyType::LOG && id == StreamId{TombstoneAllVersionId}) {
                    saw_tombstone_all_log = true;
                    EXPECT_EQ(vid, std::optional<VersionId>{1});
                    VersionMapEntry ref_entry(sym);
                    read_symbol_ref(store, sym, ref_entry);
                    ref_already_tombstoned = !ref_entry.empty() && !ref_entry.keys_.empty() &&
                                             ref_entry.keys_[0].type() == KeyType::TOMBSTONE_ALL &&
                                             ref_entry.keys_[0].version_id() == 1u;
                }
            }
    );

    vm->tombstone_from_key_or_all(store, sym);
    store->set_on_before_write(nullptr);

    EXPECT_TRUE(saw_tombstone_all_log);
    EXPECT_TRUE(ref_already_tombstoned);

    auto events = store->write_events();
    ASSERT_EQ(events.size(), 3u);
    EXPECT_EQ(events[0].key_type, KeyType::VERSION);
    EXPECT_EQ(events[1].key_type, KeyType::VERSION_REF);
    EXPECT_EQ(events[2].key_type, KeyType::LOG);
    EXPECT_EQ(events[2].stream_id, StreamId{TombstoneAllVersionId});
    EXPECT_EQ(events[2].version_id, std::optional<VersionId>{1});

    // 2. Repeating tombstone_from_key_or_all when no undeleted versions remain emits no TOMBSTONE_ALL log.
    store->clear_write_events();
    vm->tombstone_from_key_or_all(store, sym);
    EXPECT_TRUE(std::ranges::none_of(store->write_events(), [](const auto& ev) { return ev.key_type == KeyType::LOG; })
    );

    // 3. Partial tombstone_from_key_or_all also writes VERSION_REF before LOG.
    const StreamId sym_partial{"sym_tomb_partial"};
    auto pv0 = make_test_index_key(sym_partial, 0);
    auto pv1 = make_test_index_key(sym_partial, 1);
    vm->write_version(store, pv0, std::nullopt);
    vm->write_version(store, pv1, pv0);
    store->clear_write_events();

    vm->tombstone_from_key_or_all(store, sym_partial, pv0);
    auto partial_events = store->write_events();
    ASSERT_EQ(partial_events.size(), 3u);
    EXPECT_EQ(partial_events[0].key_type, KeyType::VERSION);
    EXPECT_EQ(partial_events[1].key_type, KeyType::VERSION_REF);
    EXPECT_EQ(partial_events[2].key_type, KeyType::LOG);
    EXPECT_EQ(partial_events[2].version_id, std::optional<VersionId>{0});

    // 4. If VERSION_REF write fails, no TOMBSTONE_ALL op-log may be emitted.
    const StreamId sym_fail{"sym_tomb_ref_fail"};
    vm->write_version(store, make_test_index_key(sym_fail, 0), std::nullopt);
    store->clear_write_events();

    store->set_on_before_write(
            [](KeyType kt, const StreamId&, std::optional<VersionId>, const std::optional<IndexValue>&) {
                if (kt == KeyType::VERSION_REF) {
                    throw std::runtime_error("injected VERSION_REF write failure");
                }
            }
    );
    EXPECT_THROW(vm->tombstone_from_key_or_all(store, sym_fail), std::runtime_error);
    store->set_on_before_write(nullptr);
    EXPECT_TRUE(std::ranges::none_of(store->write_events(), [](const auto& ev) { return ev.key_type == KeyType::LOG; })
    );
}

TEST(EnrichedOpLog, WriteCaptureCacheLruEvictionAndTaintingPreventsPartialResurrection) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
    WriteCaptureCache cache;

    // 1. 2,500 record_data + take cycles leave cache empty; reused key is not prematurely evicted.
    for (size_t i = 0; i < 2500; ++i) {
        const std::string sym = "sym_cycle_" + std::to_string(i);
        const auto dk = make_test_data_key(sym, 0, i + 1);
        cache.record_data(dk);
        auto taken = cache.take(sym, 0);
        ASSERT_TRUE(taken.has_value());
        ASSERT_EQ(taken->newly_written_data_keys.size(), 1u);
        EXPECT_EQ(taken->newly_written_data_keys[0], dk);
    }
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.total_bytes(), 0u);

    // 2. Interleaved > kMaxCachedEntries record_data calls BEFORE record_index:
    //    evicted keys are tainted so subsequent record_index does NOT resurrect a partial entry
    //    with missing TABLE_DATA keys.
    constexpr size_t kTotalSymbols = WriteCaptureCache::kMaxCachedEntries + 128;
    constexpr size_t kEvictedCount = kTotalSymbols - WriteCaptureCache::kMaxCachedEntries;
    auto idx_seg = make_encoded_test_segment("sym_interleaved", 2, EncodingVersion::V2);

    for (size_t i = 0; i < kTotalSymbols; ++i) {
        cache.record_data(make_test_data_key("sym_inter_" + std::to_string(i), 0, 5000 + i));
    }
    EXPECT_EQ(cache.size(), WriteCaptureCache::kMaxCachedEntries);

    // Now record_index arrives for every symbol (including evicted sym_inter_0..127).
    for (size_t i = 0; i < kTotalSymbols; ++i) {
        const std::string sym = "sym_inter_" + std::to_string(i);
        cache.record_index(make_test_index_key(sym, 0), idx_seg);
    }

    // Evicted symbols MUST return std::nullopt on take() (never partial payload with 0 data keys!).
    for (size_t i = 0; i < kEvictedCount; ++i) {
        EXPECT_FALSE(cache.take("sym_inter_" + std::to_string(i), 0).has_value());
    }
    // Non-evicted symbols MUST still have both their data key and compressed index bytes.
    for (size_t i = kEvictedCount; i < kTotalSymbols; ++i) {
        auto taken = cache.take("sym_inter_" + std::to_string(i), 0);
        ASSERT_TRUE(taken.has_value());
        ASSERT_TRUE(taken->index_key.has_value());
        ASSERT_EQ(taken->newly_written_data_keys.size(), 1u);
        EXPECT_EQ(taken->newly_written_data_keys[0].content_hash(), 5000 + i);
    }
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.total_bytes(), 0u);

    // After take() clears the taint, a fresh write for the same (sym, v0) works normally.
    const auto fresh_dk = make_test_data_key("sym_inter_0", 0, 99999);
    cache.record_data(fresh_dk);
    cache.record_index(make_test_index_key("sym_inter_0", 0), idx_seg);
    auto fresh_taken = cache.take("sym_inter_0", 0);
    ASSERT_TRUE(fresh_taken.has_value());
    ASSERT_EQ(fresh_taken->newly_written_data_keys.size(), 1u);
    EXPECT_EQ(fresh_taken->newly_written_data_keys[0], fresh_dk);
}

TEST(EnrichedOpLog, FailedWriteDiscardsCaptureBeforeRetry) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});

    // 1. Failure mid-write_frame (slice 0 succeeds & is captured, slice 1 throws).
    {
        auto store = std::make_shared<FailingAsyncInMemoryStore>();
        auto vm = std::make_shared<VersionMap>();
        vm->set_log_changes(true);

        const std::string sym = "sym_failed_write_retry";
        auto fail_wrapper = get_test_simple_frame(sym, 20, 0);
        store->set_fail_on_async_write_index(1);
        EXPECT_THROW(
                pipelines::write_frame(
                        pipelines::IndexPartialKey{sym, 0}, fail_wrapper.frame_, pipelines::FixedSlicer{127, 10}, store
                )
                        .get(),
                std::runtime_error
        );
        EXPECT_EQ(store->recorded_data_count(), 1u);
        EXPECT_EQ(store->captured_cache_size(), 0u);

        store->set_fail_on_async_write_index(std::nullopt);
        auto retry_wrapper = get_test_simple_frame(sym, 10, 0);
        auto retry_key =
                pipelines::write_frame(
                        pipelines::IndexPartialKey{sym, 0}, retry_wrapper.frame_, pipelines::FixedSlicer{127, 10}, store
                )
                        .get();
        vm->write_version(store, retry_key, std::nullopt);

        auto payload = extract_enriched_op_log_payload(read_write_log_segment(store, sym, 0));
        ASSERT_TRUE(payload.has_value());
        EXPECT_EQ(payload->new_index_key, retry_key);
        ASSERT_EQ(payload->newly_written_data_keys.size(), 1u);
        EXPECT_NE(payload->newly_written_data_keys[0].content_hash(), 0x1000u);

        auto restored_idx = deserialize_segment_from_bytes(payload->compressed_index_bytes);
        ASSERT_TRUE(restored_idx.has_value());
        auto decoded_idx = decode_segment(*restored_idx);
        ASSERT_EQ(decoded_idx.row_count(), 1u);
        EXPECT_EQ(payload->newly_written_data_keys[0], read_key_row(decoded_idx, 0));
        EXPECT_EQ(store->captured_cache_size(), 0u);
    }

    // 2. Failure during LocalVersionedEngine commit (TABLE_DATA + TABLE_INDEX written, VERSION write throws).
    {
        auto lve_store = std::make_shared<FailingAsyncInMemoryStore>();
        auto engine = version_store::LocalVersionedEngine::_test_init_from_store(lve_store, util::SysClock{});
        engine._test_get_version_map()->set_log_changes(true);

        const std::string lve_sym = "sym_lve_commit_fail_retry";
        auto lve_fail_wrapper = get_test_simple_frame(lve_sym, 8, 0);
        lve_store->set_fail_key_type(KeyType::VERSION);
        EXPECT_THROW(
                engine.write_versioned_dataframe_internal(lve_sym, lve_fail_wrapper.frame_, false, false, false),
                std::runtime_error
        );
        EXPECT_EQ(lve_store->recorded_data_count(), 1u);
        EXPECT_EQ(lve_store->captured_cache_size(), 0u);

        lve_store->set_fail_key_type(std::nullopt);
        auto lve_retry_wrapper = get_test_simple_frame(lve_sym, 8, 0);
        auto item = engine.write_versioned_dataframe_internal(lve_sym, lve_retry_wrapper.frame_, false, false, false);
        EXPECT_EQ(item.key_.version_id(), 0u);
        EXPECT_EQ(lve_store->captured_cache_size(), 0u);

        auto payload = extract_enriched_op_log_payload(read_write_log_segment(lve_store, lve_sym, 0));
        ASSERT_TRUE(payload.has_value());
        ASSERT_EQ(payload->newly_written_data_keys.size(), 1u);
        EXPECT_NE(payload->newly_written_data_keys[0].content_hash(), 0x1000u);
    }
}

TEST(EnrichedOpLog, AsyncStoreWriteCompressedCopyAndNonPassiveSyncCaptureBehavior) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});

    // 1. AsyncStore with sync_passive enabled captures write_compressed, write_compressed_sync,
    //    batch_write_compressed, copy, and copy_sync TABLE_DATA keys.
    {
        auto store = make_test_async_store(EncodingVersion::V2, /*sync_passive=*/true);
        const std::string sym = "sym_compressed_copy";

        auto make_pair = [&](VersionId vid, uint64_t hash) {
            auto seg = std::make_shared<Segment>(make_encoded_test_segment(sym, 3, EncodingVersion::V2));
            return storage::KeySegmentPair{VariantKey{make_test_data_key(sym, vid, hash)}, std::move(seg)};
        };

        auto p1 = make_pair(0, 101);
        const auto k1 = p1.atom_key();
        store->write_compressed(std::move(p1)).get();

        auto p2 = make_pair(0, 102);
        const auto k2 = p2.atom_key();
        store->write_compressed_sync(std::move(p2));

        auto p3 = make_pair(0, 103);
        const auto k3 = p3.atom_key();
        std::vector<storage::KeySegmentPair> batch;
        batch.push_back(std::move(p3));
        store->batch_write_compressed(std::move(batch)).get();

        auto captured_v0 = store->take_captured_version_write(sym, 0);
        ASSERT_TRUE(captured_v0.has_value());
        ASSERT_EQ(captured_v0->newly_written_data_keys.size(), 3u);
        EXPECT_EQ(captured_v0->newly_written_data_keys[0], k1);
        EXPECT_EQ(captured_v0->newly_written_data_keys[1], k2);
        EXPECT_EQ(captured_v0->newly_written_data_keys[2], k3);

        // copy() and copy_sync() into version 1 capture the newly copied TABLE_DATA keys.
        const auto copied_async = to_atom(store->copy(KeyType::TABLE_DATA, sym, 1, k1).get());
        const auto copied_sync = to_atom(store->copy_sync(KeyType::TABLE_DATA, sym, 1, k2));
        auto captured_v1 = store->take_captured_version_write(sym, 1);
        ASSERT_TRUE(captured_v1.has_value());
        ASSERT_EQ(captured_v1->newly_written_data_keys.size(), 2u);
        EXPECT_EQ(captured_v1->newly_written_data_keys[0], copied_async);
        EXPECT_EQ(captured_v1->newly_written_data_keys[1], copied_sync);
    }

    // 2. AsyncStore with sync_passive disabled skips capture completely even when EnrichedOpLog == 1.
    {
        auto non_passive_store = make_test_async_store(EncodingVersion::V2, /*sync_passive=*/false);
        EXPECT_FALSE(non_passive_store->is_write_capture_enabled());
        (void)write_simple_frame(non_passive_store, "sym_no_passive", 0, 8);
        EXPECT_FALSE(non_passive_store->take_captured_version_write("sym_no_passive", 0).has_value());
    }
}

TEST(EnrichedOpLog, InMemoryStoreMultiVersionEnrichedFastPathVersionIds) {
    ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
    auto store = std::make_shared<InMemoryStore>();
    auto vm = std::make_shared<VersionMap>();
    vm->set_log_changes(true);

    const std::string sym = "sym_inmem_multiver";
    std::vector<AtomKey> index_keys;
    std::optional<AtomKey> prev_key;

    for (VersionId vid = 0; vid < 3; ++vid) {
        auto wrapper = get_test_simple_frame(sym, 16, static_cast<size_t>(vid) * 100);
        auto idx_key = pipelines::write_frame(
                               pipelines::IndexPartialKey{sym, vid}, wrapper.frame_, pipelines::FixedSlicer{}, store
        )
                               .get();
        EXPECT_EQ(idx_key.version_id(), vid);
        vm->write_version(store, idx_key, prev_key);
        index_keys.push_back(idx_key);
        prev_key = idx_key;
    }

    // Verify TABLE_DATA keys in InMemoryStore carry the real version_id == 0, 1, 2 (never hardcoded 0).
    std::set<VersionId> stored_data_versions;
    static_cast<Store&>(*store).iterate_type(KeyType::TABLE_DATA, [&](VariantKey&& vk) {
        auto k = to_atom(std::move(vk));
        if (k.id() == StreamId{sym}) {
            stored_data_versions.insert(k.version_id());
        }
    });
    EXPECT_EQ(stored_data_versions, (std::set<VersionId>{0, 1, 2}));

    for (VersionId vid = 0; vid < 3; ++vid) {
        auto payload = extract_enriched_op_log_payload(read_write_log_segment(store, sym, vid));
        ASSERT_TRUE(payload.has_value()) << "Missing enriched payload for version " << vid;
        EXPECT_EQ(payload->new_index_key, index_keys[vid]);
        EXPECT_TRUE(payload->is_clean_chain_successor);
        if (vid == 0) {
            EXPECT_FALSE(payload->prev_index_key.has_value());
        } else {
            EXPECT_EQ(payload->prev_index_key, std::optional<AtomKey>{index_keys[vid - 1]});
        }
        ASSERT_FALSE(payload->newly_written_data_keys.empty());
        for (const auto& dk : payload->newly_written_data_keys) {
            EXPECT_EQ(dk.type(), KeyType::TABLE_DATA);
            EXPECT_EQ(dk.version_id(), vid);
        }
        auto restored_idx = deserialize_segment_from_bytes(payload->compressed_index_bytes);
        ASSERT_TRUE(restored_idx.has_value());
        auto decoded_idx = decode_segment(*restored_idx);
        ASSERT_GE(decoded_idx.row_count(), 1u);
        for (size_t row = 0; row < decoded_idx.row_count(); ++row) {
            EXPECT_EQ(read_key_row(decoded_idx, static_cast<ssize_t>(row)).version_id(), vid);
        }
    }

    // Also verify compress_and_schedule_async_write preserves pk.version_id (non-zero).
    auto seg_desc = stream::stream_descriptor(
            "sym_compress_async", stream::RowCountIndex(), {entity::scalar_field(entity::DataType::INT64, "val")}
    );
    SegmentInMemory seg{seg_desc};
    seg.set_scalar(0, int64_t{42});
    seg.end_row();
    stream::PartialKey pk{
            KeyType::TABLE_DATA,
            7,
            StreamId{"sym_compress_async"},
            IndexValue{NumericIndex{0}},
            IndexValue{NumericIndex{1}}
    };
    pipelines::FrameSlice slice{pipelines::ColRange{0, 1}, pipelines::RowRange{0, 1}};
    auto sk =
            store->compress_and_schedule_async_write(
                         std::make_tuple(std::move(pk), std::move(seg), std::move(slice)), std::make_shared<DeDupMap>()
            )
                    .get();
    EXPECT_EQ(sk.key().version_id(), 7u);
    store->discard_captured_version_write("sym_compress_async", 7);
}

TEST(EnrichedOpLog, CreateColumnStatsImplCapturedByEnrichedOpLog) {
    // 1. AsyncStore::update() / update_sync() captures KeyType::COLUMN_STATS when EnrichedOpLog is enabled.
    {
        ScopedConfig enriched_cfg({{kEnrichedOpLogConfigKey, 1}});
        auto store = make_test_async_store(EncodingVersion::V2);
        auto vm = std::make_shared<VersionMap>();
        vm->set_log_changes(true);

        const std::string sym = "sym_cstats_capture";
        auto wrapper = get_test_simple_frame(sym, 16, 0);
        auto idx_key = pipelines::write_frame(
                               pipelines::IndexPartialKey{sym, 0}, wrapper.frame_, pipelines::FixedSlicer{127, 8}, store
        )
                               .get();

        auto read_query = std::make_shared<pipelines::ReadQuery>();
        version_store::create_column_stats_impl(store, VersionedItem{idx_key}, ReadOptions{}, read_query);

        const auto expected_cstats_key = version_store::index_key_to_column_stats_key(idx_key);
        ASSERT_TRUE(store->key_exists_sync(expected_cstats_key));

        vm->write_version(store, idx_key, std::nullopt);
        EXPECT_FALSE(store->take_captured_version_write(sym, 0).has_value());

        auto payload = extract_enriched_op_log_payload(read_write_log_segment(store, sym, 0));
        ASSERT_TRUE(payload.has_value());
        EXPECT_EQ(payload->new_index_key, idx_key);
        EXPECT_TRUE(payload->is_clean_chain_successor);
        EXPECT_EQ(payload->column_stats_key, std::optional<AtomKey>{expected_cstats_key});
        ASSERT_FALSE(payload->column_stats_bytes.empty());

        auto restored_cstats = deserialize_segment_from_bytes(payload->column_stats_bytes);
        ASSERT_TRUE(restored_cstats.has_value());
        auto decoded_cstats = decode_segment(*restored_cstats);
        auto [ignored_cs_key, stored_cstats] = store->read_sync(expected_cstats_key, storage::ReadKeyOpts{});
        (void)ignored_cs_key;
        EXPECT_EQ(decoded_cstats.row_count(), stored_cstats.row_count());

        const auto decoded_rows = decode_column_stats_segment(decoded_cstats);
        const auto stored_rows = decode_column_stats_segment(stored_cstats);
        ASSERT_EQ(decoded_rows.size(), 2u);
        ASSERT_EQ(decoded_rows.size(), stored_rows.size());
        for (size_t i = 0; i < decoded_rows.size(); ++i) {
            EXPECT_EQ(decoded_rows[i].row_range, stored_rows[i].row_range);
            EXPECT_EQ(decoded_rows[i].stats.size(), stored_rows[i].stats.size());
        }

        // 2. AsyncStore::update_sync() also captures KeyType::COLUMN_STATS.
        const auto sync_cstats_key = atom_key_builder()
                                             .version_id(1)
                                             .creation_ts(idx_key.creation_ts() + 1)
                                             .content_hash(idx_key.content_hash() + 1)
                                             .build(sym, KeyType::COLUMN_STATS);
        storage::UpdateOpts update_opts;
        update_opts.upsert_ = true;
        (void)store->update_sync(sync_cstats_key, stored_cstats.clone(), update_opts);
        auto captured_sync = store->take_captured_version_write(sym, 1);
        ASSERT_TRUE(captured_sync.has_value());
        EXPECT_EQ(captured_sync->column_stats_key, std::optional<AtomKey>{sync_cstats_key});
        EXPECT_FALSE(captured_sync->column_stats_bytes.empty());
    }

    // 3. When EnrichedOpLog is disabled (0), create_column_stats_impl() / store->update() does not capture.
    {
        ScopedConfig disabled_cfg({{kEnrichedOpLogConfigKey, 0}});
        auto store = make_test_async_store(EncodingVersion::V2);

        const std::string sym = "sym_cstats_disabled";
        auto idx_key = write_simple_frame(store, sym, 0, 8);
        auto read_query = std::make_shared<pipelines::ReadQuery>();
        version_store::create_column_stats_impl(store, VersionedItem{idx_key}, ReadOptions{}, read_query);

        ASSERT_TRUE(store->key_exists_sync(version_store::index_key_to_column_stats_key(idx_key)));
        EXPECT_FALSE(store->take_captured_version_write(sym, 0).has_value());
    }
}

TEST(EnrichedOpLog, MalformedV2SegmentHeaderOverflowSparseMapAndOomRejected) {
    auto base_seg = make_encoded_test_segment("sym_v2_fuzz", 4, EncodingVersion::V2);
    const auto valid_bytes = serialize_segment_to_bytes(base_seg);
    ASSERT_TRUE(deserialize_segment_from_bytes(valid_bytes).has_value());

    FixedHeader fixed_hdr{};
    ASSERT_GE(valid_bytes.size(), FIXED_HEADER_SIZE + sizeof(HeaderData));
    std::memcpy(&fixed_hdr, valid_bytes.data(), sizeof(FixedHeader));
    ASSERT_EQ(fixed_hdr.encoding_version, HEADER_VERSION_V2);

    HeaderData hdata{};
    std::memcpy(&hdata, valid_bytes.data() + FIXED_HEADER_SIZE, sizeof(HeaderData));
    const size_t fields_base = FIXED_HEADER_SIZE + sizeof(HeaderData);
    const size_t offsets_base = fields_base + hdata.field_buffer_.fields_bytes_;
    const size_t idx_base = offsets_base + hdata.field_buffer_.offset_bytes_;
    std::array<uint32_t, 5> field_indices{};
    std::memcpy(field_indices.data(), valid_bytes.data() + idx_base, sizeof(field_indices));

    auto field_offset_in_bytes = [&](FieldOffset which) -> size_t {
        const auto mask = SegmentHeader::field_mask(which);
        util::check((hdata.fields_ & mask) != 0, "Expected field {} to be present", static_cast<size_t>(which));
        uint64_t field_pos = 0;
        std::memcpy(
                &field_pos,
                valid_bytes.data() + offsets_base + field_indices[static_cast<size_t>(which)] * sizeof(uint64_t),
                sizeof(uint64_t)
        );
        return fields_base + static_cast<size_t>(field_pos);
    };

    for (FieldOffset which : {FieldOffset::DESCRIPTOR, FieldOffset::COLUMN}) {
        const size_t foff = field_offset_in_bytes(which);

        // 1. 32-bit wraparound in shapes_count_ + values_count_ (0xFFFF0000 + 0x00010000 wraps to 0 in uint32_t).
        {
            auto mutated = valid_bytes;
            const uint32_t shapes = 0xFFFF0000u;
            const uint32_t values = 0x00010000u;
            std::memcpy(mutated.data() + foff + offsetof(EncodedField, shapes_count_), &shapes, sizeof(shapes));
            std::memcpy(mutated.data() + foff + offsetof(EncodedField, values_count_), &values, sizeof(values));
            EXPECT_FALSE(deserialize_segment_from_bytes(mutated).has_value());
        }

        // 2. Non-zero sparse_map_bytes_ on descriptor_field / column_fields (e.g. 4 or 12 bytes -> bm::deserialize
        // OOB).
        for (uint32_t bad_sparse : {uint32_t{4}, uint32_t{12}}) {
            auto mutated = valid_bytes;
            std::memcpy(
                    mutated.data() + foff + offsetof(EncodedField, sparse_map_bytes_), &bad_sparse, sizeof(bad_sparse)
            );
            EXPECT_FALSE(deserialize_segment_from_bytes(mutated).has_value());
        }

        // 3. Unbounded in_bytes_ amplification (in_bytes_ = 0xFFFFFFFF, out_bytes_ = 0) rejected before allocation.
        {
            auto mutated = valid_bytes;
            const uint32_t huge_in = 0xFFFFFFFFu;
            const uint32_t zero_out = 0u;
            const size_t blk_off = foff + EncodedFieldImpl::Size;
            std::memcpy(mutated.data() + blk_off + offsetof(Block, in_bytes_), &huge_in, sizeof(huge_in));
            std::memcpy(mutated.data() + blk_off + offsetof(Block, out_bytes_), &zero_out, sizeof(zero_out));
            EXPECT_FALSE(deserialize_segment_from_bytes(mutated).has_value());
        }
    }
}

} // namespace arcticdb
