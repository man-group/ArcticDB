/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/s3/s3_storage.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/version/snapshot.hpp>

#include <folly/futures/Promise.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>

namespace arcticdb {

namespace {

AtomKey make_test_atom_key(std::string sym, VersionId version, KeyType type, timestamp ts) {
    return atom_key_builder().version_id(version).creation_ts(ts).build(std::move(sym), type);
}

SegmentInMemory make_snapshot_segment(const std::string& name, const std::vector<AtomKey>& keys) {
    SegmentInMemory out;
    stream::IndexAggregator<stream::RowCountIndex> agg(name, [&out](SegmentInMemory&& seg) { out = std::move(seg); });
    for (const auto& k : keys) {
        agg.add_key(k);
    }
    agg.finalize();
    return out;
}

void write_ref_snapshot(
        const std::shared_ptr<Store>& store, const std::string& name, const std::vector<AtomKey>& keys
) {
    store->write(KeyType::SNAPSHOT_REF, SnapshotId{name}, make_snapshot_segment(name, keys)).get();
}

void write_legacy_snapshot(
        const std::shared_ptr<Store>& store, const std::string& name, const std::vector<AtomKey>& keys,
        timestamp creation_ts = 100
) {
    store->write(KeyType::SNAPSHOT,
                 0,
                 SnapshotId{name},
                 creation_ts,
                 int64_t{0},
                 int64_t{0},
                 make_snapshot_segment(name, keys))
            .get();
}

void seed_mixed_snapshots(const std::shared_ptr<Store>& store) {
    const AtomKey idx = make_test_atom_key("sym_a", 0, KeyType::TABLE_INDEX, 100);
    const AtomKey idx2 = make_test_atom_key("sym_b", 1, KeyType::TABLE_INDEX, 200);
    // snap_both has both SNAPSHOT_REF (2 keys) and legacy SNAPSHOT (1 key): ref key and its metadata must win.
    write_ref_snapshot(store, "snap_both", {idx, idx2});
    write_legacy_snapshot(store, "snap_both", {idx});
    // snap_legacy has only legacy SNAPSHOT.
    write_legacy_snapshot(store, "snap_legacy", {idx});
    // snap_ref has only SNAPSHOT_REF.
    write_ref_snapshot(store, "snap_ref", {idx});
}

void expect_mixed_snapshot_keys(const std::vector<VariantKey>& keys) {
    ASSERT_EQ(keys.size(), 3u);
    std::unordered_map<std::string, KeyType> by_name;
    for (const auto& k : keys) {
        by_name.emplace(std::get<StringId>(variant_key_id(k)), variant_key_type(k));
    }
    EXPECT_EQ(by_name.at("snap_both"), KeyType::SNAPSHOT_REF);
    EXPECT_EQ(by_name.at("snap_legacy"), KeyType::SNAPSHOT);
    EXPECT_EQ(by_name.at("snap_ref"), KeyType::SNAPSHOT_REF);
}

std::shared_ptr<Store> make_mock_s3_async_store(const std::string& lib_name) {
    proto::s3_storage::Config config;
    config.set_use_mock_storage_for_testing(true);
    storage::LibraryPath lp{lib_name};
    auto s3 = std::make_shared<storage::s3::S3Storage>(lp, storage::OpenMode::DELETE, storage::s3::S3Settings(config));
    auto storages =
            std::make_shared<storage::Storages>(storage::Storages::StorageVector{s3}, storage::OpenMode::DELETE);
    auto library = std::make_shared<storage::Library>(lp, std::move(storages));
    return std::make_shared<async::AsyncStore<>>(
            std::move(library), proto::encoding::VariantCodec{}, EncodingVersion::V2
    );
}

class RecordingReadStore : public InMemoryStore {
  public:
    explicit RecordingReadStore(bool fail_reads) : fail_reads_(fail_reads) {}

    folly::Future<storage::KeySegmentPair> read_compressed(const VariantKey& key, storage::ReadKeyOpts opts) override {
        last_opts_ = opts;
        if (fail_reads_) {
            return folly::makeFuture<storage::KeySegmentPair>(
                    folly::make_exception_wrapper<StorageException>("Injected read failure")
            );
        }
        return InMemoryStore::read_compressed(key, opts);
    }

    storage::ReadKeyOpts last_opts_{};

  private:
    bool fail_reads_;
};

// Rejects SNAPSHOT_REF immediately without deadlocking a single-threaded io_executor() pool, while
// SNAPSHOT finishes after a brief delay so collectAll settles both legs before rethrowing.
class SingleLegFailingIterateStore : public InMemoryStore {
  public:
    void iterate_type(KeyType type, const entity::IterateTypeVisitor& func, const std::string& prefix) override {
        if (type == KeyType::SNAPSHOT_REF) {
            throw StorageException("Injected SNAPSHOT_REF listing failure");
        }
        if (type == KeyType::SNAPSHOT) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            sibling_finished_.store(true, std::memory_order_release);
            return;
        }
        InMemoryStore::iterate_type(type, func, prefix);
    }

    std::atomic<bool> sibling_finished_{false};
};

// Exposes manual promises for both visit_object_metadata() legs for deterministic collectAll testing.
class PromiseMetadataListStore : public InMemoryStore {
  public:
    [[nodiscard]] bool supports_object_metadata_listing() const override { return true; }

    folly::Future<folly::Unit>
    visit_object_metadata(KeyType type, const storage::ObjectMetadataVisitor&, const std::string&) override {
        return type == KeyType::SNAPSHOT_REF ? ref_promise_.getFuture() : atom_promise_.getFuture();
    }

    folly::Promise<folly::Unit> ref_promise_;
    folly::Promise<folly::Unit> atom_promise_;
};

class MalformedSnapshotKeyStore : public InMemoryStore {
  public:
    MalformedSnapshotKeyStore(KeyType bad_type, bool native_metadata) :
        bad_type_(bad_type),
        native_metadata_(native_metadata) {}

    [[nodiscard]] bool supports_object_metadata_listing() const override { return native_metadata_; }

    void iterate_type(KeyType type, const entity::IterateTypeVisitor& func, const std::string& prefix) override {
        if (type == bad_type_) {
            func(make_malformed_key());
            return;
        }
        InMemoryStore::iterate_type(type, func, prefix);
    }

    folly::Future<folly::Unit>
    visit_object_metadata(KeyType type, const storage::ObjectMetadataVisitor& visitor, const std::string&) override {
        return folly::makeFutureWith([this, type, visitor]() {
            if (type == bad_type_) {
                visitor(make_malformed_key(), storage::ObjectMetadata{"\"etag\"", 10u, 1000});
            }
        });
    }

  private:
    VariantKey make_malformed_key() const {
        if (bad_type_ == KeyType::SNAPSHOT_REF) {
            return VariantKey{make_test_atom_key("bad_ref", 0, KeyType::SNAPSHOT, 100)};
        }
        return VariantKey{RefKey{SnapshotId{"bad_atom"}, KeyType::SNAPSHOT_REF}};
    }

    KeyType bad_type_;
    bool native_metadata_;
};

} // namespace

TEST(TestSnapshotPrimitives, ExtractSnapshotIndexKeysFiltersNonIndexTypes) {
    const AtomKey idx1 = make_test_atom_key("sym_a", 0, KeyType::TABLE_INDEX, 100);
    const AtomKey idx2 = make_test_atom_key("sym_b", 2, KeyType::MULTI_KEY, 200);
    const AtomKey data_key = make_test_atom_key("sym_c", 0, KeyType::TABLE_DATA, 300);

    auto seg = make_snapshot_segment("snap_extract", {idx1, idx2, data_key});
    const auto extracted = extract_snapshot_index_keys(seg);
    EXPECT_EQ(extracted.size(), 2u);
    EXPECT_TRUE(extracted.contains(idx1));
    EXPECT_TRUE(extracted.contains(idx2));
    EXPECT_FALSE(extracted.contains(data_key));

    const auto empty_seg = make_snapshot_segment("snap_empty", {});
    EXPECT_TRUE(extract_snapshot_index_keys(empty_seg).empty());
}

TEST(TestSnapshotPrimitives, FindLegacySnapshotKeysMatchesExactNameOnly) {
    auto store = std::make_shared<InMemoryStore>();
    const AtomKey idx = make_test_atom_key("sym_a", 0, KeyType::TABLE_INDEX, 100);

    write_legacy_snapshot(store, "snap", {idx}, 100);
    write_legacy_snapshot(store, "snap", {idx}, 200);
    write_legacy_snapshot(store, "snap_2", {idx}, 100);
    write_ref_snapshot(store, "snap_with_ref", {idx});
    write_legacy_snapshot(store, "snap_with_ref", {idx}, 100);

    const auto found = find_legacy_snapshot_keys(store, SnapshotId{"snap"});
    ASSERT_EQ(found.size(), 2u);
    EXPECT_NE(found[0], found[1]);
    for (const auto& k : found) {
        EXPECT_EQ(variant_key_type(k), KeyType::SNAPSHOT);
        EXPECT_EQ(variant_key_id(k), StreamId{"snap"});
    }

    // get_snapshot_key() falls back to find_legacy_snapshot_keys().back() when no SNAPSHOT_REF exists.
    EXPECT_EQ(get_snapshot_key(store, SnapshotId{"snap"}), std::optional<VariantKey>{found.back()});
    EXPECT_EQ(get_snapshot_key(store, SnapshotId{"missing_snap"}), std::nullopt);

    // find_legacy_snapshot_keys() ignores same-named SNAPSHOT_REF, while get_snapshot_key() prefers it.
    const auto with_ref_legacy = find_legacy_snapshot_keys(store, SnapshotId{"snap_with_ref"});
    ASSERT_EQ(with_ref_legacy.size(), 1u);
    EXPECT_EQ(variant_key_type(with_ref_legacy[0]), KeyType::SNAPSHOT);
    const auto with_ref_key = get_snapshot_key(store, SnapshotId{"snap_with_ref"});
    ASSERT_TRUE(with_ref_key.has_value());
    EXPECT_EQ(variant_key_type(*with_ref_key), KeyType::SNAPSHOT_REF);

    // Also exercise prefix-matching stores (S3-backed AsyncStore) where "snap" is a prefix of "snap_2".
    auto s3_store = make_mock_s3_async_store("snap_legacy_prefix_lib");
    write_legacy_snapshot(s3_store, "snap", {idx}, 100);
    write_legacy_snapshot(s3_store, "snap", {idx}, 200);
    write_legacy_snapshot(s3_store, "snap_2", {idx}, 100);
    const auto s3_found = find_legacy_snapshot_keys(s3_store, SnapshotId{"snap"});
    ASSERT_EQ(s3_found.size(), 2u);
    EXPECT_NE(s3_found[0], s3_found[1]);
    for (const auto& k : s3_found) {
        EXPECT_EQ(variant_key_type(k), KeyType::SNAPSHOT);
        EXPECT_EQ(variant_key_id(k), StreamId{"snap"});
    }
}

TEST(TestSnapshotPrimitives, ReadCompressedOptionalHandlesFoundMissingAndUnexpectedErrors) {
    auto store = std::make_shared<RecordingReadStore>(false);
    const AtomKey idx = make_test_atom_key("sym_a", 0, KeyType::TABLE_INDEX, 100);
    const RefKey present_key{SnapshotId{"snap_present"}, KeyType::SNAPSHOT_REF};
    const RefKey missing_key{SnapshotId{"snap_missing"}, KeyType::SNAPSHOT_REF};

    write_ref_snapshot(store, "snap_present", {idx});

    auto found = store->read_compressed_optional(VariantKey{present_key}).get();
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->variant_key(), VariantKey{present_key});
    EXPECT_TRUE(store->last_opts_.dont_warn_about_missing_key);

    auto missing = store->read_compressed_optional(VariantKey{missing_key}).get();
    EXPECT_FALSE(missing.has_value());
    EXPECT_TRUE(store->last_opts_.dont_warn_about_missing_key);

    auto explicit_missing =
            store->read_compressed_optional(
                         VariantKey{missing_key}, storage::ReadKeyOpts{.dont_warn_about_missing_key = false}
            )
                    .get();
    EXPECT_FALSE(explicit_missing.has_value());
    EXPECT_FALSE(store->last_opts_.dont_warn_about_missing_key);

    auto async_store = make_mock_s3_async_store("snap_read_opt_lib");
    write_ref_snapshot(async_store, "snap_present", {idx});
    EXPECT_TRUE(async_store->read_compressed_optional(VariantKey{present_key}).get().has_value());
    EXPECT_FALSE(async_store->read_compressed_optional(VariantKey{missing_key}).get().has_value());

    auto failing_store = std::make_shared<RecordingReadStore>(true);
    EXPECT_THROW((void)failing_store->read_compressed_optional(VariantKey{present_key}).get(), StorageException);
}

TEST(TestSnapshotPrimitives, ListSnapshotKeysParallelDeduplicatesRefOverLegacyAtom) {
    auto store = std::make_shared<InMemoryStore>();
    seed_mixed_snapshots(store);

    expect_mixed_snapshot_keys(list_snapshot_keys_parallel(store).get());

    std::vector<VariantKey> iterated_keys;
    iterate_snapshots(store, [&iterated_keys](VariantKey& vk) { iterated_keys.push_back(vk); });
    expect_mixed_snapshot_keys(iterated_keys);

    // Unsupported store falls back via list_snapshot_keys_with_metadata() with metadata == std::nullopt.
    const auto entries = list_snapshot_keys_with_metadata(store).get();
    std::vector<VariantKey> fallback_keys;
    fallback_keys.reserve(entries.size());
    for (const auto& entry : entries) {
        EXPECT_FALSE(entry.metadata.has_value());
        fallback_keys.push_back(entry.key);
    }
    expect_mixed_snapshot_keys(fallback_keys);

    // Ref-only store exercises the empty-legacy fast paths in iterate_snapshots() and merge_snapshot_entries().
    auto ref_only = std::make_shared<InMemoryStore>();
    const AtomKey idx = make_test_atom_key("sym_a", 0, KeyType::TABLE_INDEX, 100);
    write_ref_snapshot(ref_only, "snap_only_ref", {idx});
    const auto ref_only_parallel = list_snapshot_keys_parallel(ref_only).get();
    ASSERT_EQ(ref_only_parallel.size(), 1u);
    EXPECT_EQ(variant_key_type(ref_only_parallel[0]), KeyType::SNAPSHOT_REF);
    EXPECT_EQ(variant_key_id(ref_only_parallel[0]), StreamId{"snap_only_ref"});

    std::vector<VariantKey> ref_only_iterated;
    iterate_snapshots(ref_only, [&ref_only_iterated](VariantKey& vk) { ref_only_iterated.push_back(vk); });
    EXPECT_EQ(ref_only_iterated, ref_only_parallel);

    // Legacy-only and empty stores exercise the empty-ref fast path in merge_snapshot_entries().
    auto legacy_only = std::make_shared<InMemoryStore>();
    EXPECT_TRUE(list_snapshot_keys_parallel(legacy_only).get().empty());
    write_legacy_snapshot(legacy_only, "snap_only_legacy", {idx});
    const auto legacy_only_parallel = list_snapshot_keys_parallel(legacy_only).get();
    ASSERT_EQ(legacy_only_parallel.size(), 1u);
    EXPECT_EQ(variant_key_type(legacy_only_parallel[0]), KeyType::SNAPSHOT);
    EXPECT_EQ(variant_key_id(legacy_only_parallel[0]), StreamId{"snap_only_legacy"});
    std::vector<VariantKey> legacy_only_iterated;
    iterate_snapshots(legacy_only, [&legacy_only_iterated](VariantKey& vk) { legacy_only_iterated.push_back(vk); });
    EXPECT_EQ(legacy_only_iterated, legacy_only_parallel);
}

TEST(TestSnapshotPrimitives, ListSnapshotKeysWithMetadataUsesVisitObjectMetadataNatively) {
    auto store = make_mock_s3_async_store("snap_meta_listing_lib");
    ASSERT_TRUE(store->supports_object_metadata_listing());
    EXPECT_TRUE(list_snapshot_keys_with_metadata(store).get().empty());

    seed_mixed_snapshots(store);
    const size_t expected_ref_both_size =
            store->read_compressed_sync(VariantKey{RefKey{SnapshotId{"snap_both"}, KeyType::SNAPSHOT_REF}})
                    .segment()
                    .size();
    const auto legacy_both_keys = find_legacy_snapshot_keys(store, SnapshotId{"snap_both"});
    ASSERT_EQ(legacy_both_keys.size(), 1u);
    const size_t legacy_both_size = store->read_compressed_sync(legacy_both_keys[0]).segment().size();
    ASSERT_NE(expected_ref_both_size, legacy_both_size);

    const auto entries = list_snapshot_keys_with_metadata(store).get();
    std::vector<VariantKey> listed_keys;
    listed_keys.reserve(entries.size());
    for (const auto& entry : entries) {
        ASSERT_TRUE(entry.metadata.has_value());
        EXPECT_FALSE(entry.metadata->etag.empty());
        EXPECT_GT(entry.metadata->size, 0u);
        EXPECT_GT(entry.metadata->last_modified_ms, 0);
        if (variant_key_id(entry.key) == StreamId{"snap_both"}) {
            EXPECT_EQ(entry.metadata->size, expected_ref_both_size);
        }
        listed_keys.push_back(entry.key);
    }
    expect_mixed_snapshot_keys(listed_keys);
}

TEST(TestSnapshotPrimitives, ParallelListingWaitsForSiblingLegBeforeRethrowing) {
    auto fallback_store = std::make_shared<SingleLegFailingIterateStore>();
    EXPECT_THROW((void)list_snapshot_keys_parallel(fallback_store).get(), StorageException);
    EXPECT_TRUE(fallback_store->sibling_finished_.load(std::memory_order_acquire));

    auto ref_fail_store = std::make_shared<PromiseMetadataListStore>();
    auto ref_fail_fut = list_snapshot_keys_with_metadata(ref_fail_store);
    ref_fail_store->ref_promise_.setException(StorageException("Injected SNAPSHOT_REF metadata listing failure"));
    EXPECT_FALSE(ref_fail_fut.isReady());
    ref_fail_store->atom_promise_.setValue(folly::Unit{});
    EXPECT_THROW((void)std::move(ref_fail_fut).get(), StorageException);

    auto atom_fail_store = std::make_shared<PromiseMetadataListStore>();
    auto atom_fail_fut = list_snapshot_keys_with_metadata(atom_fail_store);
    atom_fail_store->atom_promise_.setException(StorageException("Injected SNAPSHOT metadata listing failure"));
    EXPECT_FALSE(atom_fail_fut.isReady());
    atom_fail_store->ref_promise_.setValue(folly::Unit{});
    EXPECT_THROW((void)std::move(atom_fail_fut).get(), StorageException);
}

TEST(TestSnapshotPrimitives, ParallelListingRejectsMalformedSnapshotKeys) {
    for (KeyType bad_type : {KeyType::SNAPSHOT_REF, KeyType::SNAPSHOT}) {
        auto bad_store = std::make_shared<MalformedSnapshotKeyStore>(bad_type, false);
        EXPECT_THROW((void)list_snapshot_keys_parallel(bad_store).get(), ArcticException);
        EXPECT_THROW((void)list_snapshot_keys_with_metadata(bad_store).get(), ArcticException);
        EXPECT_THROW(iterate_snapshots(bad_store, [](VariantKey&) {}), ArcticException);

        auto bad_meta_store = std::make_shared<MalformedSnapshotKeyStore>(bad_type, true);
        EXPECT_THROW((void)list_snapshot_keys_with_metadata(bad_meta_store).get(), ArcticException);
    }

    auto bad_legacy_store = std::make_shared<MalformedSnapshotKeyStore>(KeyType::SNAPSHOT, false);
    EXPECT_THROW((void)find_legacy_snapshot_keys(bad_legacy_store, SnapshotId{"bad_atom"}), ArcticException);
}

} // namespace arcticdb
