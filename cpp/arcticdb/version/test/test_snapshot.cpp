/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/version/snapshot.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/async/task_scheduler.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/version/test/snapshot_test_utils.hpp>

#include <atomic>

using namespace arcticdb;

namespace {

AtomKey index_key(const StreamId& stream_id, VersionId version_id) {
    return atom_key_builder()
            .version_id(version_id)
            .creation_ts(static_cast<timestamp>(version_id))
            .content_hash(version_id)
            .start_index(NumericIndex{0})
            .end_index(NumericIndex{1})
            .build<KeyType::TABLE_INDEX>(stream_id);
}

void write_ref_snapshot(const std::shared_ptr<Store>& store, const SnapshotId& snapshot_id, std::vector<AtomKey> keys) {
    write_snapshot_entry(store, keys, snapshot_id, py::none(), false);
}

// The old snapshot format: an atom key, with rows not necessarily sorted by symbol
void write_atom_snapshot(
        const std::shared_ptr<Store>& store, const SnapshotId& snapshot_id, const std::vector<AtomKey>& keys
) {
    stream::IndexAggregator<stream::RowCountIndex> agg(snapshot_id, [&store, &snapshot_id](SegmentInMemory&& segment) {
        store->write(KeyType::SNAPSHOT, 0, snapshot_id, NumericIndex{0}, NumericIndex{0}, std::move(segment)).get();
    });
    for (const auto& key : keys) {
        agg.add_key(key);
    }
    agg.finalize();
}

// A ref key snapshot with rows in the given order, which write_snapshot_entry would have sorted
void write_unsorted_ref_snapshot(
        const std::shared_ptr<Store>& store, const SnapshotId& snapshot_id, const std::vector<AtomKey>& keys
) {
    stream::IndexAggregator<stream::RowCountIndex> agg(snapshot_id, [&store, &snapshot_id](SegmentInMemory&& segment) {
        store->write(KeyType::SNAPSHOT_REF, snapshot_id, std::move(segment)).get();
    });
    for (const auto& key : keys) {
        agg.add_key(key);
    }
    agg.finalize();
}

class SnapshotDeletedAfterListingStore : public InMemoryStore {
  public:
    void iterate_type(KeyType kt, const entity::IterateTypeVisitor& func, const std::string& prefix = "") override {
        InMemoryStore::iterate_type(kt, func, prefix);
        // Snapshot listing iterates SNAPSHOT keys last, so this deletes between listing and reading
        if (kt == KeyType::SNAPSHOT && to_delete_) {
            remove_key_sync(*to_delete_, storage::RemoveOpts{});
            to_delete_.reset();
        }
    }

    std::optional<VariantKey> to_delete_;
};

// Fails reads of one snapshot with an error other than a missing key, and counts the reads of snapshots
class FailingSnapshotReadStore : public InMemoryStore {
  public:
    std::pair<VariantKey, SegmentInMemory> read_sync(const VariantKey& key, storage::ReadKeyOpts opts) override {
        if (const auto type = variant_key_type(key); type == KeyType::SNAPSHOT_REF || type == KeyType::SNAPSHOT) {
            ++snapshot_reads_;
        }
        if (failing_ && key == *failing_) {
            throw PermissionException(fmt::format("Cannot read {}", key));
        }
        return InMemoryStore::read_sync(key, opts);
    }

    std::optional<VariantKey> failing_;
    std::atomic<size_t> snapshot_reads_{0};
};

std::vector<AtomKey> sorted(const std::unordered_set<AtomKey>& keys) {
    std::vector<AtomKey> res(keys.begin(), keys.end());
    std::ranges::sort(res, std::less{});
    return res;
}

} // namespace

TEST(SnapshottedIndexKeys, FiltersSymbolsAndIgnoresSnapshot) {
    auto store = std::make_shared<InMemoryStore>();
    constexpr auto num_symbols = 50;
    std::vector<AtomKey> many_symbols;
    many_symbols.reserve(num_symbols);
    for (auto i = 0; i < num_symbols; ++i) {
        many_symbols.push_back(index_key(fmt::format("sym_{:02}", i), 1));
    }
    write_ref_snapshot(store, "snap_many", many_symbols);
    write_ref_snapshot(store, "snap_a", {index_key("sym_07", 2), index_key("sym_30", 3)});
    write_ref_snapshot(store, "snap_ignored", {index_key("sym_07", 4)});

    auto keys = get_snapshotted_index_keys(store, {"sym_07"}, SnapshotId{"snap_ignored"});
    ASSERT_EQ(keys.size(), 1u);
    EXPECT_EQ(sorted(keys["sym_07"]), (std::vector{index_key("sym_07", 1), index_key("sym_07", 2)}));

    std::unordered_set<StreamId> wanted;
    for (auto i = 0; i < 40; ++i) {
        wanted.insert(fmt::format("sym_{:02}", i));
    }
    wanted.insert("not_snapshotted");
    keys = get_snapshotted_index_keys(store, wanted);
    ASSERT_EQ(keys.size(), 40u);
    EXPECT_EQ(
            sorted(keys["sym_07"]),
            (std::vector{index_key("sym_07", 1), index_key("sym_07", 2), index_key("sym_07", 4)})
    );
    EXPECT_EQ(sorted(keys["sym_30"]), (std::vector{index_key("sym_30", 1), index_key("sym_30", 3)}));
    EXPECT_FALSE(keys.contains("sym_45"));
    EXPECT_FALSE(keys.contains("not_snapshotted"));

    EXPECT_TRUE(get_snapshotted_index_keys(store, {}).empty());
}

TEST(SnapshottedIndexKeys, ReadsOldAtomKeySnapshots) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> unsorted;
    for (auto i = 49; i >= 0; --i) {
        unsorted.push_back(index_key(fmt::format("sym_{:02}", i), 1));
    }
    write_atom_snapshot(store, "old_snap", unsorted);
    write_ref_snapshot(store, "new_snap", {index_key("sym_03", 2)});

    auto keys = get_snapshotted_index_keys(store, {"sym_03", "sym_48"});
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(sorted(keys["sym_03"]), (std::vector{index_key("sym_03", 1), index_key("sym_03", 2)}));
    EXPECT_EQ(sorted(keys["sym_48"]), (std::vector{index_key("sym_48", 1)}));

    keys = get_snapshotted_index_keys(store, {"sym_03"}, SnapshotId{"old_snap"});
    EXPECT_EQ(sorted(keys["sym_03"]), (std::vector{index_key("sym_03", 2)}));

    EXPECT_EQ(sorted(get_index_keys_in_snapshots(store, "sym_48")), (std::vector{index_key("sym_48", 1)}));
    auto map = get_master_snapshots_map(store, std::unordered_set<StreamId>{"sym_03"});
    ASSERT_EQ(map["sym_03"].size(), 2u);
    EXPECT_EQ(map["sym_03"][index_key("sym_03", 1)], std::unordered_set<SnapshotId>{"old_snap"});
}

TEST(SnapshottedIndexKeys, SkipsSnapshotDeletedAfterListing) {
    // Deletes a ref key snapshot, then an atom key one, between listing and reading
    for (const bool delete_atom_key_snapshot : {false, true}) {
        auto make_store = [delete_atom_key_snapshot]() {
            auto store = std::make_shared<SnapshotDeletedAfterListingStore>();
            write_ref_snapshot(store, "snap_kept", {index_key("sym", 1)});
            if (delete_atom_key_snapshot) {
                write_atom_snapshot(store, "snap_deleted", {index_key("sym", 2)});
                store->InMemoryStore::iterate_type(KeyType::SNAPSHOT, [&store](VariantKey&& vk) {
                    store->to_delete_ = std::move(vk);
                });
            } else {
                write_ref_snapshot(store, "snap_deleted", {index_key("sym", 2)});
                store->to_delete_ = RefKey{"snap_deleted", KeyType::SNAPSHOT_REF};
            }
            return store;
        };
        const std::vector expected{index_key("sym", 1)};

        auto store = make_store();
        EXPECT_EQ(sorted(get_snapshotted_index_keys(store, {"sym"})["sym"]), expected);
        EXPECT_FALSE(store->to_delete_.has_value());

        store = make_store();
        EXPECT_EQ(sorted(get_index_keys_in_snapshots(store, "sym")), expected);

        store = make_store();
        auto on_io_thread = folly::via(&async::io_executor(), [&store]() {
                                return get_index_keys_in_snapshots(store, "sym");
                            }).get(std::chrono::seconds(60));
        EXPECT_EQ(sorted(on_io_thread), expected);

        store = make_store();
        auto with_stats = get_master_snapshots_map_with_stats(store);
        EXPECT_EQ(with_stats.total_snapshots, 2u);
        ASSERT_EQ(with_stats.map["sym"].size(), 1u);
        EXPECT_EQ(with_stats.map["sym"][index_key("sym", 1)], std::unordered_set<SnapshotId>{"snap_kept"});

        store = make_store();
        EXPECT_EQ(find_index_key_in_snapshots(store, "sym", 1), index_key("sym", 1));
        store = make_store();
        EXPECT_FALSE(find_index_key_in_snapshots(store, "sym", 2).has_value());

        store = make_store();
        auto versions = get_versions_from_snapshots(store, std::nullopt);
        EXPECT_EQ(versions[SnapshotId{"snap_kept"}], expected);
        EXPECT_TRUE(versions[SnapshotId{"snap_deleted"}].empty());

        store = make_store();
        auto map_and_keys =
                get_master_snapshots_map_and_keys_in_given_snapshot(store, RefKey{"snap_kept", KeyType::SNAPSHOT_REF});
        EXPECT_EQ(map_and_keys.index_keys_in_given_snapshot, expected);
        EXPECT_EQ(map_and_keys.map["sym"].size(), 1u);
    }
}

TEST(SnapshottedIndexKeys, LoadersOfManySymbolsScanEveryRow) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> unsorted;
    for (auto i = 49; i >= 0; --i) {
        unsorted.push_back(index_key(fmt::format("sym_{:02}", i), 1));
    }
    write_unsorted_ref_snapshot(store, "snap", unsorted);

    for (const auto* sym : {"sym_03", "sym_25", "sym_46"}) {
        const std::vector expected{index_key(sym, 1)};
        // The loaders of many symbols scan every row. Single-symbol lookups binary search ref key snapshots, which
        // write_snapshot_entry always sorts.
        EXPECT_EQ(sorted(get_snapshotted_index_keys(store, {sym})[sym]), expected);
        auto map = get_master_snapshots_map(store, std::unordered_set<StreamId>{sym});
        EXPECT_EQ(map[sym].size(), 1u);
    }
}

TEST(SnapshottedIndexKeys, ReadsSnapshotsOnIoThreadWithOneIoThread) {
    // An async store, whose reads run on the IO pool
    auto pvs = get_test_engine<version_store::PythonVersionStore>({}, "snapshots_on_io_thread");
    auto store = pvs._test_get_store();
    for (auto i = 0; i < 5; ++i) {
        write_ref_snapshot(store, fmt::format("snap_{}", i), {index_key("sym", i)});
    }
    {
        ScopedConfig config({{"VersionStore.NumIOThreads", 1}, {"VersionStore.NumCPUThreads", 1}});
        async::TaskScheduler::reattach_instance();
        // Reading in parallel would block the only IO thread on IO work, so these must read on the calling thread
        auto results =
                folly::via(&async::io_executor(), [&store]() {
                    auto in_snapshots = get_index_keys_in_snapshots(store, "sym");
                    auto snapshotted = get_snapshotted_index_keys(store, {"sym"}, SnapshotId{"snap_0"});
                    auto with_stats = get_master_snapshots_map_with_stats(store);
                    return std::make_tuple(in_snapshots.size(), snapshotted["sym"].size(), with_stats.total_snapshots);
                }).get(std::chrono::seconds(60));
        EXPECT_EQ(std::get<0>(results), 5u);
        EXPECT_EQ(std::get<1>(results), 4u);
        EXPECT_EQ(std::get<2>(results), 5u);
        EXPECT_FALSE(async::is_io_pool_thread());
        EXPECT_TRUE(folly::via(&async::io_executor(), []() { return async::is_io_pool_thread(); }).get());
        EXPECT_FALSE(folly::via(&async::cpu_executor(), []() { return async::is_io_pool_thread(); }).get());
    }
    async::TaskScheduler::reattach_instance();
}

TEST(SnapshottedIndexKeys, MergesManySnapshotsAsTheSequentialReadDoes) {
    // An async store, so reads off the IO pool run in parallel and merge as they arrive
    auto pvs = get_test_engine<version_store::PythonVersionStore>({}, "merges_many_snapshots");
    auto store = pvs._test_get_store();
    constexpr auto num_snapshots = 40;
    constexpr auto num_symbols = 60;
    MasterSnapshotMap expected;
    std::vector<AtomKey> keys_in_snap_7;
    for (auto s = 0; s < num_snapshots; ++s) {
        const auto snapshot_id = fmt::format("snap_{:02}", s);
        std::vector<AtomKey> keys;
        for (auto i = 0; i < num_symbols; ++i) {
            auto key = index_key(fmt::format("sym_{:02}", i), (i + s) % 7);
            expected[key.id()][key].insert(snapshot_id);
            keys.push_back(std::move(key));
        }
        if (s == 7) {
            keys_in_snap_7 = keys;
        }
        write_ref_snapshot(store, snapshot_id, keys);
    }
    const auto given = RefKey{"snap_07", KeyType::SNAPSHOT_REF};

    auto with_stats = get_master_snapshots_map_with_stats(store);
    EXPECT_EQ(with_stats.total_snapshots, static_cast<size_t>(num_snapshots));
    EXPECT_EQ(with_stats.map, expected);
    auto map_and_keys = get_master_snapshots_map_and_keys_in_given_snapshot(store, given);
    EXPECT_EQ(map_and_keys.map, expected);
    std::ranges::sort(map_and_keys.index_keys_in_given_snapshot, std::less{});
    std::ranges::sort(keys_in_snap_7, std::less{});
    EXPECT_EQ(map_and_keys.index_keys_in_given_snapshot, keys_in_snap_7);

    // On an IO thread the snapshots are read one at a time
    auto sequential = folly::via(&async::io_executor(), [&store, &given]() {
                          return std::make_pair(
                                  get_master_snapshots_map(store),
                                  get_master_snapshots_map_and_keys_in_given_snapshot(store, given)
                          );
                      }).get();
    EXPECT_EQ(sequential.first, expected);
    EXPECT_EQ(sequential.second.map, expected);
    std::ranges::sort(sequential.second.index_keys_in_given_snapshot, std::less{});
    EXPECT_EQ(sequential.second.index_keys_in_given_snapshot, keys_in_snap_7);
}

TEST(SnapshottedIndexKeys, ReadsLegacyLayoutRefSnapshot) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> keys;
    for (auto i = 9; i >= 0; --i) {
        keys.push_back(index_key(fmt::format("sym_{}", i), 1));
    }
    write_legacy_layout_ref_snapshot(store, "legacy_snap", keys);
    ASSERT_EQ(
            stream::read_key_row(
                    store->read_sync(RefKey{"legacy_snap", KeyType::SNAPSHOT_REF}, storage::ReadKeyOpts{}).second, 0
            ),
            keys[0]
    );

    EXPECT_EQ(sorted(get_snapshotted_index_keys(store, {"sym_4"})["sym_4"]), (std::vector{index_key("sym_4", 1)}));
    EXPECT_EQ(sorted(get_index_keys_in_snapshots(store, "sym_7")), (std::vector{index_key("sym_7", 1)}));
    EXPECT_EQ(find_index_key_in_snapshots(store, "sym_7", 1), std::optional{index_key("sym_7", 1)});
    auto map = get_master_snapshots_map(store, std::unordered_set<StreamId>{"sym_2"});
    EXPECT_EQ(map["sym_2"][index_key("sym_2", 1)], std::unordered_set<SnapshotId>{"legacy_snap"});
}

TEST(ReadSnapshots, PassesEachSnapshotWithItsPosition) {
    // An async store, so the reads run in parallel on the IO pool
    auto pvs = get_test_engine<version_store::PythonVersionStore>({}, "read_snapshots_positions");
    auto store = pvs._test_get_store();
    constexpr size_t num_snapshots = 30;
    for (size_t i = 0; i < num_snapshots; ++i) {
        write_ref_snapshot(store, fmt::format("snap_{:02}", i), {index_key("sym", i)});
    }
    auto snapshot_keys = list_snapshot_keys(store);
    ASSERT_EQ(snapshot_keys.size(), num_snapshots);

    std::vector<std::optional<VersionId>> seen(num_snapshots);
    std::mutex seen_mutex;
    read_snapshots(store, snapshot_keys, [&](size_t idx, const VariantKey& snapshot_key, SegmentInMemory&& segment) {
        EXPECT_EQ(snapshot_key, snapshot_keys[idx]);
        const auto version_id = stream::read_key_row(segment, 0).version_id();
        std::lock_guard lock(seen_mutex);
        EXPECT_FALSE(seen[idx].has_value());
        seen[idx] = version_id;
        return true;
    });
    for (size_t idx = 0; idx < num_snapshots; ++idx) {
        ASSERT_TRUE(seen[idx].has_value());
        EXPECT_EQ(fmt::format("snap_{:02}", *seen[idx]), fmt::format("{}", variant_key_id(snapshot_keys[idx])));
    }
}

TEST(ReadSnapshots, StopsReadingOnceTheCallbackReturnsFalse) {
    auto store = std::make_shared<InMemoryStore>();
    constexpr size_t num_snapshots = 10;
    for (size_t i = 0; i < num_snapshots; ++i) {
        write_ref_snapshot(store, fmt::format("snap_{}", i), {index_key("sym", i)});
    }
    const auto snapshot_keys = list_snapshot_keys(store);
    // On an IO thread the snapshots are read one at a time, so exactly one is read
    const auto calls = folly::via(&async::io_executor(), [&store, &snapshot_keys]() {
                           std::atomic<size_t> count{0};
                           read_snapshots(store, snapshot_keys, [&count](size_t, const VariantKey&, SegmentInMemory&&) {
                               ++count;
                               return false;
                           });
                           return count.load();
                       }).get(std::chrono::seconds(60));
    EXPECT_EQ(calls, 1u);

    const std::optional<AtomKey> expected{index_key("sym", 6)};
    EXPECT_EQ(find_index_key_in_snapshots(store, "sym", 6), expected);
    EXPECT_FALSE(find_index_key_in_snapshots(store, "sym", 10).has_value());
    EXPECT_FALSE(find_index_key_in_snapshots(store, "other_sym", 6).has_value());
}

TEST(ReadSnapshots, RethrowsErrorsOtherThanTheSnapshotBeingDeleted) {
    auto store = std::make_shared<InMemoryStore>();
    write_ref_snapshot(store, "snap", {index_key("sym", 1)});
    const auto snapshot_keys = list_snapshot_keys(store);
    const auto missing_data_key = index_key("other_sym", 1);
    EXPECT_THROW(
            read_snapshots(
                    store,
                    snapshot_keys,
                    [&missing_data_key](size_t, const VariantKey&, SegmentInMemory&&) -> bool {
                        throw storage::KeyNotFoundException(VariantKey{missing_data_key});
                    }
            ),
            storage::KeyNotFoundException
    );
    EXPECT_THROW(
            read_snapshots(
                    store,
                    snapshot_keys,
                    [](size_t, const VariantKey&, SegmentInMemory&&) -> bool {
                        throw storage::KeyNotFoundException(std::string("No keys named"));
                    }
            ),
            storage::KeyNotFoundException
    );
}

TEST(SnapshottedIndexKeys, MatchesNumericStreamIds) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> keys;
    for (NumericId id = 20; id > 0; --id) {
        keys.push_back(index_key(StreamId{id}, 1));
    }
    // A snapshot's key rows store stream ids with the type of the snapshot's own id
    const SnapshotId numeric_snap{NumericId{42}};
    write_ref_snapshot(store, numeric_snap, keys);
    write_ref_snapshot(store, "string_snap", {index_key("7", 2)});

    const StreamId seven{NumericId{7}};
    auto snapshotted = get_snapshotted_index_keys(store, {seven});
    ASSERT_EQ(snapshotted.size(), 1u);
    EXPECT_EQ(sorted(snapshotted[seven]), (std::vector{index_key(seven, 1)}));
    EXPECT_EQ(sorted(get_index_keys_in_snapshots(store, seven)), (std::vector{index_key(seven, 1)}));
    EXPECT_EQ(get_versions_from_snapshots(store, seven)[numeric_snap], (std::vector{index_key(seven, 1)}));
}

TEST(ReadSnapshots, RaisesOtherReadErrorsAndStopsReading) {
    auto store = std::make_shared<FailingSnapshotReadStore>();
    constexpr size_t num_snapshots = 10;
    for (size_t i = 0; i < num_snapshots; ++i) {
        write_ref_snapshot(store, fmt::format("snap_{}", i), {index_key("sym", i)});
    }
    const auto snapshot_keys = list_snapshot_keys(store);
    store->failing_ = snapshot_keys[0];

    EXPECT_THROW(get_versions_from_snapshots(store, std::nullopt), PermissionException);
    EXPECT_THROW(find_index_key_in_snapshots(store, "sym", 5), PermissionException);
    EXPECT_THROW(get_snapshotted_index_keys(store, {"sym"}), PermissionException);

    // One at a time on an IO thread, so the failure stops the remaining reads
    store->snapshot_reads_ = 0;
    EXPECT_THROW(
            folly::via(
                    &async::io_executor(), [&store]() { return get_index_keys_in_snapshots(store, "sym"); }
            ).get(std::chrono::seconds(60)),
            PermissionException
    );
    EXPECT_EQ(store->snapshot_reads_.load(), 1u);
}
