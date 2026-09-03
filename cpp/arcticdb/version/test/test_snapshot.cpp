/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/test/generators.hpp>

#include <folly/futures/Future.h>
#include <pybind11/pybind11.h>

#include <functional>
#include <utility>
#include <vector>

using namespace arcticdb;
using namespace arcticdb::entity;

namespace {

AtomKey make_index_key(const StreamId& stream_id, VersionId version_id) {
    return atom_key_builder()
            .version_id(version_id)
            .creation_ts(PilotedClock::nanos_since_epoch())
            .content_hash(version_id)
            .start_index(NumericIndex{0})
            .end_index(NumericIndex{1})
            .build(stream_id, KeyType::TABLE_INDEX);
}

/**
 * Store that fails the read of chosen keys with a caller-supplied exception.
 *
 * The snapshot code paths under test decide which read failures may be swallowed, so the tests need to control
 * both which key fails and what it fails with. InMemoryStore::read() dispatches through the virtual read_sync(),
 * so overriding read_sync() covers the windowed concurrent reads as well as the blocking ones.
 */
class ReadFailureStore : public InMemoryStore {
  public:
    void fail_read_with(VariantKey key, std::function<void()> thrower) {
        failures_.emplace_back(std::move(key), std::move(thrower));
    }

    std::pair<VariantKey, SegmentInMemory> read_sync(const VariantKey& key, storage::ReadKeyOpts opts) override {
        for (const auto& [failing_key, thrower] : failures_) {
            if (failing_key == key) {
                thrower();
            }
        }
        return InMemoryStore::read_sync(key, opts);
    }

  private:
    // Set up before the reads are issued and only read afterwards, so no locking is needed.
    std::vector<std::pair<VariantKey, std::function<void()>>> failures_;
};

struct SnapshotFixture {
    std::shared_ptr<ReadFailureStore> store = std::make_shared<ReadFailureStore>();
    StreamId sym_a{"sym_a"};
    StreamId sym_b{"sym_b"};
    AtomKey index_key_a = make_index_key(sym_a, 0);
    AtomKey index_key_b = make_index_key(sym_b, 0);
    SnapshotId snap_a{"snap_a"};
    SnapshotId snap_b{"snap_b"};

    SnapshotFixture() {
        std::vector<AtomKey> keys_a{index_key_a};
        std::vector<AtomKey> keys_b{index_key_b};
        write_snapshot_entry(store, keys_a, snap_a, py::none{}, false);
        write_snapshot_entry(store, keys_b, snap_b, py::none{}, false);
    }

    [[nodiscard]] VariantKey snapshot_key_a() const { return RefKey{snap_a, KeyType::SNAPSHOT_REF}; }

    [[nodiscard]] VariantKey snapshot_key_b() const { return RefKey{snap_b, KeyType::SNAPSHOT_REF}; }
};

folly::Try<folly::Unit> success() { return folly::Try<folly::Unit>{folly::Unit{}}; }

template<typename Exception, typename... Args>
folly::Try<folly::Unit> failure(Args&&... args) {
    return folly::Try<folly::Unit>{folly::make_exception_wrapper<Exception>(std::forward<Args>(args)...)};
}

} // namespace

// === check_only_deleted_snapshots_failed(), the filter itself ===
//
// This decides which snapshot read failures are swallowed, and it gates get_master_snapshots_map_with_stats(),
// the map that decides which index keys a delete is allowed to remove. If the filter ever widens, a snapshot
// silently drops out of that map and delete_version()/prune_previous_versions() will delete data that the
// snapshot still protects, so each shape of failure is pinned down here.

TEST(CheckOnlyDeletedSnapshotsFailed, AcceptsAllSuccesses) {
    std::vector<VariantKey> snapshot_keys{
            RefKey{"snap_a", KeyType::SNAPSHOT_REF}, RefKey{"snap_b", KeyType::SNAPSHOT_REF}
    };
    std::vector<folly::Try<folly::Unit>> results;
    results.emplace_back(success());
    results.emplace_back(success());

    EXPECT_NO_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys));
}

TEST(CheckOnlyDeletedSnapshotsFailed, SwallowsKeyNotFoundNamingOnlyTheSnapshotKey) {
    std::vector<VariantKey> snapshot_keys{
            RefKey{"snap_a", KeyType::SNAPSHOT_REF}, RefKey{"snap_b", KeyType::SNAPSHOT_REF}
    };
    std::vector<folly::Try<folly::Unit>> results;
    results.emplace_back(failure<storage::KeyNotFoundException>(snapshot_keys[0]));
    results.emplace_back(success());

    EXPECT_NO_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys));
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesKeyNotFoundNamingAnIndexKey) {
    std::vector<VariantKey> snapshot_keys{RefKey{"snap_a", KeyType::SNAPSHOT_REF}};
    std::vector<folly::Try<folly::Unit>> results;
    // The failure names an index key, not the snapshot key, so the snapshot has NOT gone: swallowing this would
    // understate what the snapshots protect.
    results.emplace_back(failure<storage::KeyNotFoundException>(VariantKey{make_index_key(StreamId{"sym_a"}, 0)}));

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), storage::KeyNotFoundException);
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesKeyNotFoundNamingAnotherSnapshotKey) {
    std::vector<VariantKey> snapshot_keys{RefKey{"snap_a", KeyType::SNAPSHOT_REF}};
    std::vector<folly::Try<folly::Unit>> results;
    results.emplace_back(failure<storage::KeyNotFoundException>(VariantKey{RefKey{"snap_b", KeyType::SNAPSHOT_REF}}));

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), storage::KeyNotFoundException);
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesKeyNotFoundNamingTheSnapshotKeyAndAnother) {
    std::vector<VariantKey> snapshot_keys{RefKey{"snap_a", KeyType::SNAPSHOT_REF}};
    std::vector<folly::Try<folly::Unit>> results;
    std::vector<VariantKey> missing{snapshot_keys[0], VariantKey{make_index_key(StreamId{"sym_a"}, 0)}};
    results.emplace_back(failure<storage::KeyNotFoundException>(std::move(missing)));

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), storage::KeyNotFoundException);
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesNonKeyNotFoundStorageErrors) {
    std::vector<VariantKey> snapshot_keys{RefKey{"snap_a", KeyType::SNAPSHOT_REF}};
    std::vector<folly::Try<folly::Unit>> results;
    results.emplace_back(failure<UnexpectedS3ErrorException>(std::string{"S3 is having a bad day"}));

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), UnexpectedS3ErrorException);
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesKeyNotFoundThatNamesNoKey) {
    std::vector<VariantKey> snapshot_keys{RefKey{"snap_a", KeyType::SNAPSHOT_REF}};
    std::vector<folly::Try<folly::Unit>> results;
    // raise_s3_exception() raises a NoSuchKey from the async read path in this message-only form, which names no
    // key at all. It is not evidence that this snapshot has gone, and it must not be read as if it were.
    results.emplace_back(
            failure<storage::KeyNotFoundException>(std::string{"Key Not Found Error: S3Error:15, HttpResponseCode:404"})
    );

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), storage::KeyNotFoundException);
}

TEST(CheckOnlyDeletedSnapshotsFailed, PropagatesAFailureThatIsNotTheFirstResult) {
    std::vector<VariantKey> snapshot_keys{
            RefKey{"snap_a", KeyType::SNAPSHOT_REF}, RefKey{"snap_b", KeyType::SNAPSHOT_REF}
    };
    std::vector<folly::Try<folly::Unit>> results;
    results.emplace_back(failure<storage::KeyNotFoundException>(snapshot_keys[0]));
    results.emplace_back(failure<storage::KeyNotFoundException>(VariantKey{make_index_key(StreamId{"sym_b"}, 0)}));

    EXPECT_THROW(check_only_deleted_snapshots_failed(results, snapshot_keys), storage::KeyNotFoundException);
}

// === get_master_snapshots_map_with_stats(), through a failure-injecting store ===

TEST(MasterSnapshotsMap, MapsEverySnapshotWhenNoReadFails) {
    SnapshotFixture f;

    auto result = get_master_snapshots_map_with_stats(f.store);

    EXPECT_EQ(result.total_snapshots, 2u);
    ASSERT_TRUE(result.map.contains(f.sym_a));
    ASSERT_TRUE(result.map.contains(f.sym_b));
    EXPECT_TRUE(result.map[f.sym_a][f.index_key_a].contains(f.snap_a));
    EXPECT_TRUE(result.map[f.sym_b][f.index_key_b].contains(f.snap_b));
}

TEST(MasterSnapshotsMap, SkipsASnapshotDeletedBetweenTheListingAndTheRead) {
    SnapshotFixture f;
    auto missing = f.snapshot_key_a();
    f.store->fail_read_with(missing, [missing]() { throw storage::KeyNotFoundException(missing); });

    MasterSnapshotMapWithStats result;
    EXPECT_NO_THROW(result = get_master_snapshots_map_with_stats(f.store));

    // The snapshot that has genuinely gone is dropped, and every other snapshot is still mapped.
    EXPECT_FALSE(result.map.contains(f.sym_a));
    ASSERT_TRUE(result.map.contains(f.sym_b));
    EXPECT_TRUE(result.map[f.sym_b][f.index_key_b].contains(f.snap_b));
    // The listing still saw both snapshots.
    EXPECT_EQ(result.total_snapshots, 2u);
}

TEST(MasterSnapshotsMap, PropagatesKeyNotFoundNamingAnIndexKey) {
    SnapshotFixture f;
    // The snapshot is still there; something underneath it is missing. Dropping the snapshot from the map here
    // would let delete_version()/prune_previous_versions() remove index keys the snapshot still protects.
    auto index_key = VariantKey{f.index_key_a};
    f.store->fail_read_with(f.snapshot_key_a(), [index_key]() { throw storage::KeyNotFoundException(index_key); });

    EXPECT_THROW(get_master_snapshots_map_with_stats(f.store), storage::KeyNotFoundException);
}

TEST(MasterSnapshotsMap, PropagatesKeyNotFoundThatNamesNoKey) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw storage::KeyNotFoundException(std::string{"Key Not Found Error: S3Error:15, HttpResponseCode:404"});
    });

    EXPECT_THROW(get_master_snapshots_map_with_stats(f.store), storage::KeyNotFoundException);
}

TEST(MasterSnapshotsMap, PropagatesNonKeyNotFoundStorageErrors) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw UnexpectedS3ErrorException("Unexpected error from storage");
    });

    EXPECT_THROW(get_master_snapshots_map_with_stats(f.store), UnexpectedS3ErrorException);
}

TEST(MasterSnapshotsMap, PropagatesStorageErrorsWhenFilteringBySymbol) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw UnexpectedS3ErrorException("Unexpected error from storage");
    });

    // The failing snapshot holds sym_a, which is not in the requested set, but the read still has to happen and
    // its failure still has to be reported.
    std::unordered_set<StreamId> stream_ids{f.sym_b};
    EXPECT_THROW(get_master_snapshots_map_with_stats(f.store, stream_ids), UnexpectedS3ErrorException);
}

// === PythonVersionStore::list_snapshots(load_metadata=true), through the same filter ===

namespace {
version_store::PythonVersionStore version_store_with(
        const std::shared_ptr<Store>& store, const std::string& library_name
) {
    auto pvs = get_test_engine<version_store::PythonVersionStore>({}, library_name);
    pvs._test_set_store(store);
    return pvs;
}
} // namespace

TEST(ListSnapshotsWithMetadata, ListsEverySnapshotWhenNoReadFails) {
    SnapshotFixture f;
    auto pvs = version_store_with(f.store, "list_snapshots_ok");

    auto snapshots = pvs.list_snapshots(true);

    EXPECT_EQ(snapshots.size(), 2u);
}

TEST(ListSnapshotsWithMetadata, SkipsASnapshotDeletedBetweenTheListingAndTheRead) {
    SnapshotFixture f;
    auto missing = f.snapshot_key_a();
    f.store->fail_read_with(missing, [missing]() { throw storage::KeyNotFoundException(missing); });
    auto pvs = version_store_with(f.store, "list_snapshots_deleted");

    std::vector<std::pair<SnapshotId, py::object>> snapshots;
    EXPECT_NO_THROW(snapshots = pvs.list_snapshots(true));

    ASSERT_EQ(snapshots.size(), 1u);
    EXPECT_EQ(snapshots[0].first, SnapshotId{"snap_b"});
}

TEST(ListSnapshotsWithMetadata, PropagatesKeyNotFoundNamingAnIndexKey) {
    SnapshotFixture f;
    auto index_key = VariantKey{f.index_key_a};
    f.store->fail_read_with(f.snapshot_key_a(), [index_key]() { throw storage::KeyNotFoundException(index_key); });
    auto pvs = version_store_with(f.store, "list_snapshots_index_key_missing");

    EXPECT_THROW(pvs.list_snapshots(true), storage::KeyNotFoundException);
}

TEST(ListSnapshotsWithMetadata, PropagatesKeyNotFoundThatNamesNoKey) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw storage::KeyNotFoundException(std::string{"Key Not Found Error: S3Error:15, HttpResponseCode:404"});
    });
    auto pvs = version_store_with(f.store, "list_snapshots_keyless_not_found");

    EXPECT_THROW(pvs.list_snapshots(true), storage::KeyNotFoundException);
}

TEST(ListSnapshotsWithMetadata, PropagatesNonKeyNotFoundStorageErrors) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw UnexpectedS3ErrorException("Unexpected error from storage");
    });
    auto pvs = version_store_with(f.store, "list_snapshots_storage_error");

    // A genuine storage error must raise rather than silently returning a short list.
    EXPECT_THROW(pvs.list_snapshots(true), UnexpectedS3ErrorException);
}

TEST(ListSnapshotsWithMetadata, DoesNotReadTheSegmentsWhenMetadataIsNotRequested) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw UnexpectedS3ErrorException("Unexpected error from storage");
    });
    auto pvs = version_store_with(f.store, "list_snapshots_no_metadata");

    // Without metadata the snapshot segments are never read, so the read failure is irrelevant and both
    // snapshots are listed.
    auto snapshots = pvs.list_snapshots(false);

    EXPECT_EQ(snapshots.size(), 2u);
}

// === iterate_snapshots(), which filters the same failures for the blocking callers ===

TEST(IterateSnapshots, SkipsASnapshotDeletedDuringIteration) {
    SnapshotFixture f;
    auto missing = f.snapshot_key_a();
    f.store->fail_read_with(missing, [missing]() { throw storage::KeyNotFoundException(missing); });

    std::vector<SnapshotId> visited;
    EXPECT_NO_THROW(iterate_snapshots(f.store, [&f, &visited](VariantKey& vk) {
        f.store->read_sync(vk, storage::ReadKeyOpts{});
        visited.emplace_back(variant_key_id(vk));
    }));

    ASSERT_EQ(visited.size(), 1u);
    EXPECT_EQ(visited[0], SnapshotId{"snap_b"});
}

TEST(IterateSnapshots, PropagatesKeyNotFoundNamingAnIndexKey) {
    SnapshotFixture f;
    auto index_key = VariantKey{f.index_key_a};
    f.store->fail_read_with(f.snapshot_key_a(), [index_key]() { throw storage::KeyNotFoundException(index_key); });

    EXPECT_THROW(
            iterate_snapshots(f.store, [&f](VariantKey& vk) { f.store->read_sync(vk, storage::ReadKeyOpts{}); }),
            storage::KeyNotFoundException
    );
}

TEST(IterateSnapshots, PropagatesKeyNotFoundThatNamesNoKey) {
    SnapshotFixture f;
    f.store->fail_read_with(f.snapshot_key_a(), []() {
        throw storage::KeyNotFoundException(std::string{"Key Not Found Error: S3Error:15, HttpResponseCode:404"});
    });

    EXPECT_THROW(
            iterate_snapshots(f.store, [&f](VariantKey& vk) { f.store->read_sync(vk, storage::ReadKeyOpts{}); }),
            storage::KeyNotFoundException
    );
}
