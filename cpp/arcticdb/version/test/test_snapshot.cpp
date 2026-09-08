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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
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

/**
 * Store that runs a caller-supplied hook before listing a chosen key type.
 *
 * list_snapshot_keys() lists SNAPSHOT_REF on the calling thread and the legacy SNAPSHOT type on the IO executor,
 * so either listing can fail on its own - real storages fail per prefix, e.g. a bucket policy that denies one of
 * them, or a listing that times out. Throwing from the hook fails that listing; blocking in it holds it up.
 */
class ListingStore : public InMemoryStore {
  public:
    void on_listing(KeyType key_type, std::function<void()> hook) { hooks_.emplace_back(key_type, std::move(hook)); }

    void iterate_type(KeyType key_type, const entity::IterateTypeVisitor& func, const std::string& prefix = "")
            override {
        for (const auto& [hooked_type, hook] : hooks_) {
            if (hooked_type == key_type) {
                hook();
            }
        }
        InMemoryStore::iterate_type(key_type, func, prefix);
    }

  private:
    // Set up before the listings are issued and only read afterwards, so no locking is needed.
    std::vector<std::pair<KeyType, std::function<void()>>> hooks_;
};

bool mentions(const char* message, std::string_view fragment) {
    return std::string_view{message}.find(fragment) != std::string_view::npos;
}

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

// === list_snapshot_keys(), the two concurrent listings ===
//
// The legacy SNAPSHOT listing runs on the IO executor and appends into a vector owned by list_snapshot_keys()'
// own frame, so it has to be joined before that frame unwinds however the function leaves it. A regression that
// let a listing failure escape ahead of the join would be a use-after-scope on real storage rather than a wrong
// answer, and a listing failure that went unreported would silently shorten the snapshot list - which on the
// delete paths means index keys losing their snapshot protection.

namespace {
std::shared_ptr<ListingStore> store_with_two_snapshots() {
    auto store = std::make_shared<ListingStore>();
    std::vector<AtomKey> keys_a{make_index_key(StreamId{"sym_a"}, 0)};
    std::vector<AtomKey> keys_b{make_index_key(StreamId{"sym_b"}, 0)};
    write_snapshot_entry(store, keys_a, SnapshotId{"snap_a"}, py::none{}, false);
    write_snapshot_entry(store, keys_b, SnapshotId{"snap_b"}, py::none{}, false);
    return store;
}
} // namespace

TEST(ListSnapshotKeys, ListsEverySnapshotWhenNeitherListingFails) {
    auto store = store_with_two_snapshots();

    auto keys = list_snapshot_keys(store);

    EXPECT_EQ(keys.size(), 2u);
}

TEST(ListSnapshotKeys, PropagatesAFailureOfTheRefListing) {
    auto store = store_with_two_snapshots();
    store->on_listing(KeyType::SNAPSHOT_REF, []() { throw UnexpectedS3ErrorException("ref listing failed"); });

    EXPECT_THROW(list_snapshot_keys(store), UnexpectedS3ErrorException);
}

TEST(ListSnapshotKeys, PropagatesAFailureOfTheLegacyListing) {
    auto store = store_with_two_snapshots();
    store->on_listing(KeyType::SNAPSHOT, []() { throw UnexpectedS3ErrorException("legacy listing failed"); });

    EXPECT_THROW(list_snapshot_keys(store), UnexpectedS3ErrorException);
}

TEST(ListSnapshotKeys, ReportsTheRefFailureWhenBothListingsFail) {
    auto store = store_with_two_snapshots();
    store->on_listing(KeyType::SNAPSHOT_REF, []() { throw UnexpectedS3ErrorException("ref listing failed"); });
    store->on_listing(KeyType::SNAPSHOT, []() { throw UnexpectedS3ErrorException("legacy listing failed"); });

    try {
        list_snapshot_keys(store);
        FAIL() << "Expected list_snapshot_keys() to throw";
    } catch (const UnexpectedS3ErrorException& e) {
        // Either failure is fatal, so which one is reported is a choice rather than a requirement - but it has
        // to be a deterministic one, not whichever listing happened to finish first.
        EXPECT_TRUE(mentions(e.what(), "ref listing failed")) << e.what();
    }
}

TEST(ListSnapshotKeys, JoinsTheLegacyListingBeforeARefFailureEscapes) {
    std::atomic<bool> legacy_listing_running{false};
    auto store = store_with_two_snapshots();
    store->on_listing(KeyType::SNAPSHOT, [&legacy_listing_running]() {
        legacy_listing_running = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        legacy_listing_running = false;
    });
    store->on_listing(KeyType::SNAPSHOT_REF, []() { throw UnexpectedS3ErrorException("ref listing failed"); });

    EXPECT_THROW(list_snapshot_keys(store), UnexpectedS3ErrorException);

    // The legacy listing is still writing into list_snapshot_keys()' locals until it returns, so it must be
    // joined before the ref failure unwinds them.
    EXPECT_FALSE(legacy_listing_running.load());
}

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
//
// The names are pinned here; the pairing of each name with its own metadata is pinned by
// test_list_snapshots_pairs_metadata_with_its_own_snapshot in
// python/tests/integration/arcticdb/version_store/test_snapshot.py, because turning a snapshot's user metadata
// into the py::object list_snapshots() returns goes through python_util::pb_to_python(), which imports
// arcticc.pb2.descriptors_pb2 - a module this gtest binary has no path to.

namespace {
version_store::PythonVersionStore version_store_with(
        const std::shared_ptr<Store>& store, const std::string& library_name
) {
    auto pvs = get_test_engine<version_store::PythonVersionStore>({}, library_name);
    pvs._test_set_store(store);
    return pvs;
}

std::vector<std::string> snapshot_names(const std::vector<std::pair<SnapshotId, py::object>>& snapshots) {
    std::vector<std::string> names;
    names.reserve(snapshots.size());
    for (const auto& [snapshot_id, metadata] : snapshots) {
        names.emplace_back(std::get<std::string>(snapshot_id));
    }
    std::ranges::sort(names);
    return names;
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

TEST(ListSnapshotsWithMetadata, KeepsTheNamesOfTheSnapshotsAfterOneThatWasDeleted) {
    auto store = std::make_shared<ReadFailureStore>();
    for (const auto* name : {"snap_a", "snap_b", "snap_c"}) {
        std::vector<AtomKey> keys{make_index_key(StreamId{std::string{"sym_"} + name}, 0)};
        write_snapshot_entry(store, keys, SnapshotId{name}, py::none{}, false);
    }
    auto missing = VariantKey{RefKey{SnapshotId{"snap_b"}, KeyType::SNAPSHOT_REF}};
    store->fail_read_with(missing, [missing]() { throw storage::KeyNotFoundException(missing); });
    auto pvs = version_store_with(store, "list_snapshots_middle_deleted");

    auto snapshots = pvs.list_snapshots(true);

    // The results are assembled by walking the read results and the listed keys in lockstep, so dropping one
    // snapshot from the middle must not shift the names of the ones around it.
    EXPECT_EQ(snapshot_names(snapshots), (std::vector<std::string>{"snap_a", "snap_c"}));
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
