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

std::vector<AtomKey> sorted(const std::unordered_set<AtomKey>& keys) {
    std::vector<AtomKey> res(keys.begin(), keys.end());
    std::sort(res.begin(), res.end());
    return res;
}

} // namespace

TEST(SnapshottedIndexKeys, FiltersSymbolsAndIgnoresSnapshot) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> many_symbols;
    for (auto i = 0; i < 50; ++i) {
        many_symbols.push_back(index_key(fmt::format("sym_{:02}", i), 1));
    }
    write_ref_snapshot(store, "snap_many", many_symbols);
    write_ref_snapshot(store, "snap_a", {index_key("sym_07", 2), index_key("sym_30", 3)});
    write_ref_snapshot(store, "snap_ignored", {index_key("sym_07", 4)});

    // One symbol, so binary searched
    auto keys = get_snapshotted_index_keys(store, {"sym_07"}, SnapshotId{"snap_ignored"});
    ASSERT_EQ(keys.size(), 1u);
    EXPECT_EQ(sorted(keys["sym_07"]), (std::vector{index_key("sym_07", 1), index_key("sym_07", 2)}));

    // Most symbols, so scanned
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
        auto map_and_keys =
                get_master_snapshots_map_and_keys_in_given_snapshot(store, RefKey{"snap_kept", KeyType::SNAPSHOT_REF});
        EXPECT_EQ(map_and_keys.index_keys_in_given_snapshot, expected);
        EXPECT_EQ(map_and_keys.map["sym"].size(), 1u);
    }
}

TEST(SnapshottedIndexKeys, FindsKeysInUnsortedRefKeySnapshot) {
    auto store = std::make_shared<InMemoryStore>();
    std::vector<AtomKey> unsorted;
    for (auto i = 49; i >= 0; --i) {
        unsorted.push_back(index_key(fmt::format("sym_{:02}", i), 1));
    }
    write_unsorted_ref_snapshot(store, "snap", unsorted);

    for (const auto* sym : {"sym_03", "sym_25", "sym_46"}) {
        const std::vector expected{index_key(sym, 1)};
        EXPECT_EQ(sorted(get_snapshotted_index_keys(store, {sym})[sym]), expected);
        EXPECT_EQ(sorted(get_index_keys_in_snapshots(store, sym)), expected);
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
