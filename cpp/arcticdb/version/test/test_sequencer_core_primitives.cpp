/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mock/s3_mock_client.hpp>
#include <arcticdb/storage/s3/s3_client_wrapper.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/commit_router.hpp>
#include <arcticdb/version/enriched_op_log_payload.hpp>
#include <arcticdb/version/http_commit_router.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_utils.hpp>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace arcticdb {

namespace {

std::shared_ptr<storage::Library> make_memory_library(bool enable_log = true) {
    proto::storage::VersionStoreConfig cfg;
    cfg.set_symbol_list(true);
    cfg.set_encoding_version(static_cast<uint32_t>(EncodingVersion::V2));
    cfg.mutable_write_options()->mutable_sync_passive()->set_enabled(enable_log);

    auto path = storage::LibraryPath::from_delim_path("test_seq_core", '.');
    storage::Storages::StorageVector vec{std::make_shared<storage::memory::MemoryStorage>(
            path, storage::OpenMode::DELETE, storage::memory::MemoryStorage::Config{}
    )};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    return std::make_shared<storage::Library>(path, std::move(storages), cfg);
}

// Minimal delegating Storage wrapper over MemoryStorage to override conditional PUT semantics for dual-probe tests.
class ProbeOverrideStorage : public storage::Storage {
  public:
    enum class Mode { VAST_SILENT_IGNORE, PURE_NOT_IMPLEMENTED };

    ProbeOverrideStorage(const storage::LibraryPath& path, Mode mode) :
        storage::Storage(path, storage::OpenMode::DELETE),
        inner_(std::make_shared<storage::memory::MemoryStorage>(
                path, storage::OpenMode::DELETE, storage::memory::MemoryStorage::Config{}
        )),
        mode_(mode) {}

    std::string name() const final { return "probe_override"; }

  protected:
    void do_write(storage::KeySegmentPair& kv) final { inner_->write(kv); }
    void do_write_if_none(storage::KeySegmentPair& kv) final { inner_->write_if_none(kv); }
    std::string do_write_if_none_with_etag(storage::KeySegmentPair& kv) final {
        if (mode_ == Mode::VAST_SILENT_IGNORE) {
            inner_->write(kv);
            return "\"vast-etag\"";
        }
        return inner_->write_if_none_with_etag(kv);
    }
    std::string do_write_if_match(storage::KeySegmentPair& kv, const std::string& expected_etag) final {
        if (mode_ == Mode::VAST_SILENT_IGNORE) {
            inner_->write(kv);
            return "\"vast-etag-2\"";
        }
        if (mode_ == Mode::PURE_NOT_IMPLEMENTED) {
            throw NotImplementedException("Conditional If-Match header not implemented");
        }
        return inner_->write_if_match(kv, expected_etag);
    }
    void do_update(storage::KeySegmentPair& kv, storage::UpdateOpts opts) final { inner_->update(kv, opts); }
    void do_read(entity::VariantKey&& vk, const fmt::ReadVisitor& visitor, storage::ReadKeyOpts opts) final {
        inner_->read(std::move(vk), visitor, opts);
    }
    storage::KeySegmentPair do_read(entity::VariantKey&& vk, storage::ReadKeyOpts opts) final {
        return inner_->read(std::move(vk), opts);
    }
    void do_remove(entity::VariantKey&& vk, storage::RemoveOpts opts) final { inner_->remove(std::move(vk), opts); }
    void do_remove(std::span<entity::VariantKey> vks, storage::RemoveOpts opts) final { inner_->remove(vks, opts); }
    bool do_key_exists(const entity::VariantKey& vk) final { return inner_->key_exists(vk); }
    bool do_supports_prefix_matching() const final { return false; }
    storage::SupportsAtomicWrites do_supports_atomic_writes() const final {
        return storage::SupportsAtomicWrites::NO;
    }
    bool do_fast_delete() final { return true; }
    bool do_iterate_type_until_match(
            KeyType /*key_type*/, const IterateTypePredicate& /*visitor*/, const std::string& /*prefix*/
    ) final {
        return false;
    }
    std::string do_key_path(const entity::VariantKey&) const final { return {}; }

  private:
    std::shared_ptr<storage::memory::MemoryStorage> inner_;
    Mode mode_;
};

std::shared_ptr<storage::s3::S3Storage> make_mock_s3_storage() {
    proto::s3_storage::Config proto_cfg;
    proto_cfg.set_bucket_name("test-cas-bucket");
    proto_cfg.set_prefix("cas_test");
    proto_cfg.set_use_mock_storage_for_testing(true);
    storage::s3::S3Settings settings(storage::s3::AWSAuthMethod::DISABLED, "", false);
    settings.update(proto_cfg);
    auto path = storage::LibraryPath::from_delim_path("test_s3_cas", '.');
    return std::make_shared<storage::s3::S3Storage>(path, storage::OpenMode::DELETE, settings);
}

class RecordingCommitRouter final : public version::CommitRouter {
  public:
    std::vector<proto::sequencer::CommitRequest> requests;
    AtomKey next_index_key;
    AtomKey next_version_key;

    proto::sequencer::CommitResponse submit(const proto::sequencer::CommitRequest& request) override {
        requests.push_back(request);
        proto::sequencer::CommitResponse resp;
        resp.set_status(proto::sequencer::OK);
        resp.set_version_id(next_index_key.version_id());
        *resp.mutable_index_key() = key_to_proto(next_index_key);
        *resp.mutable_version_key() = key_to_proto(next_version_key);
        resp.set_creation_ts(next_index_key.creation_ts());
        resp.set_coalesced_batch_size(1);
        return resp;
    }

    proto::sequencer::HealthResponse health() override {
        proto::sequencer::HealthResponse h;
        h.set_healthy(true);
        return h;
    }

    std::string endpoint() const override { return "inproc://recording"; }
};

} // namespace

TEST(ConditionalPutTest, DualStartupProbeAwsVastPure) {
    // Upstream test_reliable_storage_lock.cpp leaks Storage.AtomicSupportTestSuffix without resetting it.
    const std::string prev_suffix = ConfigsMap::instance()->get_string("Storage.AtomicSupportTestSuffix", "");
    ConfigsMap::instance()->set_string("Storage.AtomicSupportTestSuffix", "");
    auto restore_suffix = folly::makeGuard([prev_suffix]() {
        ConfigsMap::instance()->set_string("Storage.AtomicSupportTestSuffix", prev_suffix);
    });

    auto aws_storage = make_mock_s3_storage();
    EXPECT_TRUE(aws_storage->supports_cas_writes());
    // Cached on second call
    EXPECT_TRUE(aws_storage->supports_cas_writes());

    auto path = storage::LibraryPath::from_delim_path("test_probe_fallback", '.');
    auto vast_storage = std::make_shared<ProbeOverrideStorage>(path, ProbeOverrideStorage::Mode::VAST_SILENT_IGNORE);
    EXPECT_FALSE(vast_storage->supports_cas_writes());

    auto pure_storage = std::make_shared<ProbeOverrideStorage>(path, ProbeOverrideStorage::Mode::PURE_NOT_IMPLEMENTED);
    EXPECT_FALSE(pure_storage->supports_cas_writes());
}

TEST(ConditionalPutTest, WriteIfNoneAndWriteIfMatchWithQueryStats) {
    auto s3_storage = make_mock_s3_storage();

    proto::storage::VersionStoreConfig cfg;
    cfg.set_encoding_version(static_cast<uint32_t>(EncodingVersion::V2));
    auto path = storage::LibraryPath::from_delim_path("cas_stats_lib", '.');
    storage::Storages::StorageVector vec{s3_storage};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    auto lib = std::make_shared<storage::Library>(path, std::move(storages), cfg);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );

    auto qs = query_stats::QueryStats::instance();
    qs->reset_stats();
    qs->enable();

    const StreamId sym{"cas_sym"};
    auto make_seg = [&](int64_t val) {
        auto desc = stream::stream_descriptor(sym, stream::RowCountIndex(), {scalar_field(DataType::INT64, "v")});
        SegmentInMemory seg{desc};
        seg.set_scalar(0, val);
        seg.end_row();
        return seg;
    };

    auto [vk1, etag1] = store->write_if_none_with_etag_sync(KeyType::VERSION_REF, sym, make_seg(1));
    EXPECT_FALSE(etag1.empty());

    // Repeat If-None-Match:* must fail with AtomicOperationFailedException
    EXPECT_THROW(
            (void)store->write_if_none_with_etag_sync(KeyType::VERSION_REF, sym, make_seg(2)),
            AtomicOperationFailedException
    );

    // Stale If-Match must fail
    EXPECT_THROW(
            (void)store->write_if_match_sync(KeyType::VERSION_REF, sym, make_seg(3), "\"stale-etag\""),
            AtomicOperationFailedException
    );

    // Matching If-Match must succeed and update ETag
    auto [vk2, etag2] = store->write_if_match_sync(KeyType::VERSION_REF, sym, make_seg(4), etag1);
    EXPECT_FALSE(etag2.empty());
    EXPECT_NE(etag1, etag2);

    auto [rvk, rseg, retag] = store->read_with_etag_sync(RefKey{sym, KeyType::VERSION_REF}, storage::ReadKeyOpts{});
    EXPECT_EQ(retag, etag2);
    EXPECT_EQ(*rseg.scalar_at<int64_t>(0, 0), 4);

    auto snap = qs->get_structured_snapshot();
    qs->disable();

    auto bit = snap.storage.find(query_stats::StorageBackend::S3);
    ASSERT_NE(bit, snap.storage.end());
    auto wim_it = bit->second.find(query_stats::StorageOp::WriteIfMatch);
    ASSERT_NE(wim_it, bit->second.end());
    auto kt_it = wim_it->second.find("VERSION_REF");
    ASSERT_NE(kt_it, wim_it->second.end());
    EXPECT_EQ(kt_it->second.total_count(), 2u); // 1 stale failure + 1 success
}

TEST(HlcTimestampClampTest, MonotonicVersionCreationTs) {
    auto lib = make_memory_library(/*enable_log=*/false);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );
    auto vm = std::make_shared<VersionMap>();

    const StreamId sym{"hlc_sym"};
    const timestamp future_ts = store->current_timestamp() + 60'000'000'000LL; // +60s in future
    AtomKey idx0 = AtomKeyBuilder()
                           .version_id(0)
                           .creation_ts(future_ts)
                           .content_hash(111)
                           .start_index(0)
                           .end_index(1)
                           .build(sym, KeyType::TABLE_INDEX);
    auto [ver0, etag0] = vm->write_version_cas(store, idx0, std::nullopt, true, std::nullopt);
    EXPECT_GE(ver0.creation_ts(), future_ts);

    // Next index key has an older wall-clock timestamp; VERSION creation_ts MUST clamp strictly > ver0.creation_ts().
    AtomKey idx1 = AtomKeyBuilder()
                           .version_id(1)
                           .creation_ts(future_ts - 5'000'000'000LL)
                           .content_hash(222)
                           .start_index(0)
                           .end_index(2)
                           .build(sym, KeyType::TABLE_INDEX);
    auto [ver1, etag1] = vm->write_version_cas(store, idx1, idx0, true, etag0, std::nullopt, ver0);
    EXPECT_GT(ver1.creation_ts(), ver0.creation_ts());
}

TEST(InjectLatestVersionTest, ZeroRttReadYourOwnWrites) {
    auto lib = make_memory_library(/*enable_log=*/false);
    version_store::LocalVersionedEngine engine(lib);

    auto store = engine._test_get_store();
    const StreamId sym{"ryw_sym"};
    auto wrapper = get_test_simple_frame(sym, 8, 100);
    AtomKey idx0 = pipelines::write_frame(IndexPartialKey{sym, 0}, wrapper.frame_, pipelines::FixedSlicer{}, store).get();
    auto vm_writer = std::make_shared<VersionMap>();
    auto [ver0, etag0] = vm_writer->write_version_cas(store, idx0, std::nullopt, false, std::nullopt);
    (void)etag0;

    // Client engine has NOT read VERSION_REF yet; inject committed version directly.
    engine.inject_committed_version(sym, idx0, ver0);

    auto qs = query_stats::QueryStats::instance();
    qs->reset_stats();
    qs->enable();
    auto latest = engine.get_latest_version(sym);
    auto snap = qs->get_structured_snapshot();
    qs->disable();

    ASSERT_TRUE(latest.has_value());
    EXPECT_EQ(latest->key_, idx0);

    // Zero VERSION_REF or VERSION reads on S3/Memory!
    auto bit = snap.storage.find(query_stats::StorageBackend::Memory);
    if (bit != snap.storage.end()) {
        auto rit = bit->second.find(query_stats::StorageOp::Read);
        if (rit != bit->second.end()) {
            EXPECT_EQ(rit->second.count("VERSION_REF"), 0u);
            EXPECT_EQ(rit->second.count("VERSION"), 0u);
        }
    }
}

TEST(WriteCaptureCacheAdoptedDataTest, AdoptedZeroVersionKeysRecorded) {
    WriteCaptureCache cache;
    const StreamId sym{"adopted_sym"};
    AtomKey k0 = AtomKeyBuilder()
                         .version_id(0)
                         .creation_ts(1000)
                         .content_hash(0xdeadbeef)
                         .start_index(0)
                         .end_index(10)
                         .build(sym, KeyType::TABLE_DATA);
    std::vector<AtomKey> adopted{k0};
    cache.record_adopted_data(sym, VersionId{5}, adopted);
    auto taken = cache.take(sym, VersionId{5});
    ASSERT_TRUE(taken.has_value());
    ASSERT_EQ(taken->newly_written_data_keys.size(), 1u);
    EXPECT_EQ(taken->newly_written_data_keys[0], k0);
}

TEST(RowCountIndexSegmentRangeTest, AdoptedClientLocalOffsetsReturnTotalRows) {
    auto lib = make_memory_library(/*enable_log=*/false);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );

    const StreamId sym{"rc_adopted_sym"};
    // Two client-preuploaded TABLE_DATA keys both having client-local [0, 5) in AtomKey start/end_index,
    // spliced into consecutive row ranges [0, 5) and [5, 10) in TABLE_INDEX.
    AtomKey dk1 = AtomKeyBuilder()
                          .version_id(0)
                          .creation_ts(1000)
                          .content_hash(1)
                          .start_index(0)
                          .end_index(5)
                          .build(sym, KeyType::TABLE_DATA);
    AtomKey dk2 = AtomKeyBuilder()
                          .version_id(0)
                          .creation_ts(1001)
                          .content_hash(2)
                          .start_index(0)
                          .end_index(5)
                          .build(sym, KeyType::TABLE_DATA);

    std::vector<pipelines::SliceAndKey> slices;
    slices.emplace_back(pipelines::FrameSlice(pipelines::ColRange{0, 1}, pipelines::RowRange{0, 5}), dk1);
    slices.emplace_back(pipelines::FrameSlice(pipelines::ColRange{0, 1}, pipelines::RowRange{5, 10}), dk2);

    auto sd = stream::stream_descriptor(sym, stream::RowCountIndex(), {scalar_field(DataType::INT64, "a")});
    auto tsd = make_timeseries_descriptor(10, sd, proto::descriptors::NormalizationMetadata{}, std::nullopt, std::nullopt, false);

    AtomKey idx_key = pipelines::index::write_index(
                              stream::RowCountIndex{}, tsd, std::move(slices), pipelines::IndexPartialKey{sym, 0}, store
                      )
                              .get();

    IndexRange ir = pipelines::index::get_index_segment_range(idx_key, store);
    EXPECT_TRUE(ir.specified_);
    EXPECT_EQ(std::get<NumericIndex>(ir.start_), 0);
    EXPECT_EQ(std::get<NumericIndex>(ir.end_), 10); // half-open [0, 10) for 10 rows
}

TEST(CommitRouterClientTest, InlineVsSplitPlaneThresholdAndSaltedKeys) {
    auto lib = make_memory_library(/*enable_log=*/false);
    version_store::LocalVersionedEngine engine(lib);

    auto router = std::make_shared<RecordingCommitRouter>();
    const StreamId sym{"router_sym"};
    router->next_index_key = AtomKeyBuilder()
                                     .version_id(0)
                                     .creation_ts(123456)
                                     .content_hash(1)
                                     .start_index(0)
                                     .end_index(10)
                                     .build(sym, KeyType::TABLE_INDEX);
    router->next_version_key = AtomKeyBuilder()
                                       .version_id(0)
                                       .creation_ts(123456)
                                       .content_hash(2)
                                       .start_index(0)
                                       .end_index(0)
                                       .build(sym, KeyType::VERSION);

    version::SequencerClientConfig scfg;
    scfg.endpoint = "inproc://recording";
    scfg.inline_threshold_bytes = 512; // Small threshold to exercise both inline and split-plane
    engine.set_commit_router(router, scfg);

    // 1. Small append (<= 512B) -> inline_payload
    auto small_w = get_test_simple_frame(sym, 4, 0);
    version_store::AppendOptions aopts{.upsert = true};
    auto vit1 = engine.append_internal(sym, small_w.frame_, aopts);
    EXPECT_EQ(vit1.key_, router->next_index_key);
    ASSERT_EQ(router->requests.size(), 1u);
    EXPECT_TRUE(router->requests[0].has_inline_payload());
    EXPECT_FALSE(router->requests[0].has_preuploaded_slices());

    // 2. Large append (> 512B) -> preuploaded_slices with VersionId(0) and salted content_hash!
    auto large_w1 = get_test_simple_frame(sym, 200, 0);
    auto large_w2 = get_test_simple_frame(sym, 200, 0); // Identical payload
    engine.append_internal(sym, large_w1.frame_, aopts);
    engine.append_internal(sym, large_w2.frame_, aopts);
    ASSERT_EQ(router->requests.size(), 3u);
    ASSERT_TRUE(router->requests[1].has_preuploaded_slices());
    ASSERT_TRUE(router->requests[2].has_preuploaded_slices());

    AtomKey k1 = key_from_proto(router->requests[1].preuploaded_slices().slices(0).data_key());
    AtomKey k2 = key_from_proto(router->requests[2].preuploaded_slices().slices(0).data_key());
    EXPECT_EQ(k1.version_id(), 0u);
    EXPECT_EQ(k2.version_id(), 0u);
    // Salted hashes must be distinct even for identical content!
    EXPECT_NE(k1.content_hash(), k2.content_hash());
}

TEST(SequencerCorePhase3Test, ComputeVirtualShardDeterministicAndDistributed) {
    const uint32_t s1 = version::compute_virtual_shard("lib_a", "AAPL", 256);
    const uint32_t s2 = version::compute_virtual_shard("lib_a", "AAPL", 256);
    EXPECT_EQ(s1, s2);
    EXPECT_LT(s1, 256u);

    // Distinct symbols spread across multiple virtual shards
    std::set<uint32_t> distinct;
    for (int i = 0; i < 64; ++i) {
        distinct.insert(version::compute_virtual_shard("lib_a", "SYM_" + std::to_string(i), 256));
    }
    EXPECT_GT(distinct.size(), 30u);
}

TEST(SequencerCorePhase3Test, TombstoneFromKeyOrAllCasEnforcesEtag) {
    auto s3_storage = make_mock_s3_storage();
    proto::storage::VersionStoreConfig cfg;
    cfg.set_encoding_version(static_cast<uint32_t>(EncodingVersion::V2));
    auto path = storage::LibraryPath::from_delim_path("tombstone_cas_lib", '.');
    storage::Storages::StorageVector vec{s3_storage};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    auto lib = std::make_shared<storage::Library>(path, std::move(storages), cfg);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );

    auto vm = std::make_shared<VersionMap>();
    const StreamId sym{"tomb_cas_sym"};
    AtomKey idx0 = AtomKeyBuilder()
                           .version_id(0)
                           .creation_ts(store->current_timestamp())
                           .content_hash(1)
                           .start_index(0)
                           .end_index(1)
                           .build(sym, KeyType::TABLE_INDEX);
    auto [ver0, etag0] = vm->write_version_cas(store, idx0, std::nullopt, true, std::nullopt);

    // Stale ETag MUST fail with AtomicOperationFailedException!
    EXPECT_THROW(
            (void)vm->tombstone_from_key_or_all_cas(store, sym, true, std::string("\"wrong-etag\"")),
            AtomicOperationFailedException
    );

    // Matching ETag succeeds and writes a tombstone!
    auto [latest_ver, keys_to_del, new_etag] = vm->tombstone_from_key_or_all_cas(store, sym, true, etag0);
    EXPECT_EQ(latest_ver, ver0.version_id());
    EXPECT_FALSE(keys_to_del.empty());
    EXPECT_FALSE(new_etag.empty());
    EXPECT_NE(new_etag, etag0);
}

TEST(SequencerCorePhase3Test, SequencerOnlyInlineModeSupportsUpTo16MiB) {
    auto lib = make_memory_library(/*enable_log=*/false);
    version_store::LocalVersionedEngine engine(lib);
    auto router = std::make_shared<RecordingCommitRouter>();
    const StreamId sym{"inline_only_sym"};
    router->next_index_key = AtomKeyBuilder()
                                     .version_id(0)
                                     .creation_ts(123456)
                                     .content_hash(10)
                                     .start_index(0)
                                     .end_index(200)
                                     .build(sym, KeyType::TABLE_INDEX);
    router->next_version_key = AtomKeyBuilder()
                                       .version_id(0)
                                       .creation_ts(123456)
                                       .content_hash(11)
                                       .start_index(0)
                                       .end_index(0)
                                       .build(sym, KeyType::VERSION);

    // Configure inline_threshold_bytes = 16 MiB (sequencer-only inline tenant without object-store write creds)
    version::SequencerClientConfig scfg;
    scfg.endpoint = "inproc://recording";
    scfg.inline_threshold_bytes = 16ULL * 1024 * 1024;
    engine.set_commit_router(router, scfg);

    auto medium_w = get_test_simple_frame(sym, 200, 0);
    version_store::AppendOptions aopts{.upsert = true};
    engine.append_internal(sym, medium_w.frame_, aopts);
    ASSERT_EQ(router->requests.size(), 1u);
    EXPECT_EQ(router->requests[0].client_protocol_version(), 2u);
    EXPECT_TRUE(router->requests[0].has_inline_payload());
    EXPECT_FALSE(router->requests[0].has_preuploaded_slices());
}

TEST(SequencerCoreHardeningTest, RewriteAfterDeleteWriteVersionCasNoCrash) {
    // P0-8: Rewriting a tombstoned symbol via synthetic known_prev_head must NOT crash in VersionMapEntry::validate().
    auto lib = make_memory_library(/*enable_log=*/false);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );
    auto vm = std::make_shared<VersionMap>();
    const StreamId sym{"rewrite_after_del_sym"};

    AtomKey idx0 = AtomKeyBuilder()
                           .version_id(0)
                           .creation_ts(store->current_timestamp())
                           .content_hash(101)
                           .start_index(0)
                           .end_index(1)
                           .build(sym, KeyType::TABLE_INDEX);
    auto [ver0, etag0] = vm->write_version_cas(store, idx0, std::nullopt, true, std::nullopt);
    auto [del_ver, del_keys, etag1] =
            vm->tombstone_from_key_or_all_cas(store, sym, true, etag0, std::nullopt, std::nullopt, /*force_reload=*/true);
    (void)del_ver;
    (void)del_keys;

    // Fresh VersionMap (cold) rewriting after tombstone using known_prev_head (ver0 / tombstone)
    auto vm2 = std::make_shared<VersionMap>();
    AtomKey idx1 = AtomKeyBuilder()
                           .version_id(1)
                           .creation_ts(store->current_timestamp() + 1000)
                           .content_hash(202)
                           .start_index(0)
                           .end_index(1)
                           .build(sym, KeyType::TABLE_INDEX);
    auto [ver1, etag2] = vm2->write_version_cas(store, idx1, std::nullopt, true, etag1, std::nullopt, ver0);
    EXPECT_EQ(ver1.version_id(), 1u);
    EXPECT_FALSE(etag2.empty());
}

TEST(SequencerCoreHardeningTest, CraftedTruncatedSegmentBoundsAndUncompressedEstimate) {
    // P0-12: Crafted 16-byte buffer with huge header_bytes must throw cleanly (never heap OOB SIGSEGV).
    auto bad_buf = std::make_shared<Buffer>();
    bad_buf->ensure(16);
    FixedHeader fh{};
    fh.magic_number = MAGIC_NUMBER;
    fh.header_bytes = 0xFFFFFFFFu;
    std::memcpy(bad_buf->data(), &fh, sizeof(fh));
    bad_buf->set_bytes(16);
    EXPECT_THROW((void)Segment::from_buffer(bad_buf), std::exception);

    // P0-12: estimate_uncompressed_bytes accurately reports pre-decode uncompressed size on LZ4 segment.
    auto sd = stream::stream_descriptor(StreamId{"est_sym"}, stream::RowCountIndex{}, {scalar_field(DataType::FLOAT64, "v")});
    SegmentInMemory seg{sd};
    for (int i = 0; i < 2048; ++i) {
        seg.set_scalar(0, 0.0);
        seg.end_row();
    }
    Segment enc = encode_v2(std::move(seg), codec::default_lz4_codec());
    auto wire = serialize_segment_to_bytes(enc);
    auto parsed = deserialize_segment_from_bytes(wire);
    ASSERT_TRUE(parsed.has_value());
    EXPECT_GE(parsed->estimate_uncompressed_bytes(), 2048u * sizeof(double));
    EXPECT_LT(wire.size(), parsed->estimate_uncompressed_bytes());

    // P0-12d: Valid V2 string segment round-trips cleanly, while truncating body / corrupting footer throws cleanly.
    auto str_sd = stream::stream_descriptor(
            StreamId{"str_bounds_sym"}, stream::RowCountIndex{}, {scalar_field(DataType::ASCII_DYNAMIC64, "s")}
    );
    SegmentInMemory str_seg{str_sd};
    for (int i = 0; i < 8; ++i) {
        str_seg.set_string(0, "hello_bounds_" + std::to_string(i));
        str_seg.end_row();
    }
    Segment str_enc = encode_v2(std::move(str_seg), codec::default_lz4_codec());
    auto str_wire = serialize_segment_to_bytes(str_enc);
    auto str_parsed = deserialize_segment_from_bytes(str_wire);
    ASSERT_TRUE(str_parsed.has_value());
    auto decoded = decode_segment(*str_parsed);
    EXPECT_EQ(decoded.row_count(), 8u);

    // Truncated wire bytes at multiple offsets must fail closed (never heap OOB).
    for (size_t cut : {size_t{24}, str_wire.size() / 2, str_wire.size() - 4}) {
        if (cut < str_wire.size()) {
            auto maybe_seg = deserialize_segment_from_bytes(std::span<const uint8_t>(str_wire.data(), cut));
            if (maybe_seg.has_value()) {
                EXPECT_THROW((void)decode_segment(*maybe_seg), std::exception);
            }
        }
    }
}

TEST(SequencerCoreHardeningTest, MockS3HeadObjectMetadataAndUnroutedFailClosed) {
    // P1-3 & P0-30: MockS3Client::head_object_metadata returns metadata on existing key and throws KeyNotFound on 404.
    auto s3_storage = make_mock_s3_storage();
    proto::storage::VersionStoreConfig cfg;
    cfg.set_encoding_version(static_cast<uint32_t>(EncodingVersion::V2));
    auto path = storage::LibraryPath::from_delim_path("meta_test_lib", '.');
    storage::Storages::StorageVector vec{s3_storage};
    auto storages = std::make_shared<storage::Storages>(std::move(vec), storage::OpenMode::DELETE);
    auto lib = std::make_shared<storage::Library>(path, std::move(storages), cfg);
    auto store = std::make_shared<async::AsyncStore<util::SysClock>>(
            lib, codec::default_lz4_codec(), EncodingVersion::V2
    );

    const StreamId sym{"meta_sym"};
    auto sd = stream::stream_descriptor(sym, stream::RowCountIndex{}, {scalar_field(DataType::INT64, "v")});
    SegmentInMemory seg{sd};
    seg.set_scalar(0, int64_t{1});
    seg.end_row();
    AtomKey present_key = to_atom(store->write_sync(
            PartialKey{KeyType::TABLE_DATA, VersionId{0}, sym, NumericIndex{0}, NumericIndex{1}},
            store->current_timestamp(),
            std::move(seg)
    ));
    auto meta = store->get_object_metadata_sync(VariantKey{present_key});
    ASSERT_TRUE(meta.has_value());
    EXPECT_GT(meta->size, 0u);
    EXPECT_GT(meta->last_modified_ms, 0);

    AtomKey missing_key = AtomKeyBuilder()
                                  .version_id(0)
                                  .creation_ts(123)
                                  .content_hash(0xDEAD)
                                  .start_index(0)
                                  .end_index(1)
                                  .build(sym, KeyType::TABLE_DATA);
    EXPECT_THROW((void)store->get_object_metadata_sync(VariantKey{missing_key}), storage::KeyNotFoundException);

    // P0-7: Unrouted mutating APIs fail closed when commit_router_ is attached; batch_delete_internal routes through it.
    auto mem_lib = make_memory_library(/*enable_log=*/false);
    version_store::LocalVersionedEngine engine(mem_lib);
    auto router = std::make_shared<RecordingCommitRouter>();
    router->next_index_key = AtomKeyBuilder().version_id(0).creation_ts(1).content_hash(1).start_index(0).end_index(1).build(sym, KeyType::TABLE_INDEX);
    router->next_version_key = AtomKeyBuilder().version_id(0).creation_ts(1).content_hash(2).start_index(0).end_index(0).build(sym, KeyType::VERSION);
    version::SequencerClientConfig scfg;
    scfg.endpoint = "inproc://recording";
    engine.set_commit_router(router, scfg);

    EXPECT_THROW(engine.compact_data_internal(sym, std::nullopt, false), ArcticException);
    EXPECT_THROW(engine.sort_index(sym, false, false), ArcticException);

    std::vector<StreamId> del_syms{sym};
    engine.batch_delete_internal(del_syms, std::vector<std::vector<VersionId>>(1));
    ASSERT_EQ(router->requests.size(), 1u);
    EXPECT_EQ(router->requests[0].op(), proto::sequencer::DELETE_ALL_VERSIONS);

    // P0-19: Uppercase HTTPS:// scheme normalized to lowercase https://
    version::SequencerClientConfig upper_cfg;
    upper_cfg.endpoint = "HTTPS://seq-0.svc.cluster.local:9100";
    upper_cfg.allowed_redirect_hosts = {".svc.cluster.local"};
    version::HttpCommitRouter upper_router(upper_cfg);
    EXPECT_EQ(upper_router.endpoint(), "https://seq-0.svc.cluster.local:9100");
    EXPECT_FALSE(upper_router.is_valid_redirect_endpoint("http://seq-0.svc.cluster.local:9100"));
}

} // namespace arcticdb
