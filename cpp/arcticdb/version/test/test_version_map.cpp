/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/version/version_functions.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/version/version_log.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb {

using ::testing::UnorderedElementsAre;

#define THREE_SIMPLE_KEYS \
    auto key1 = atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index( \
        4).end_index(5).build(id, KeyType::TABLE_INDEX); \
    auto key2 = atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(  \
        5).end_index(6).build(id, KeyType::TABLE_INDEX); \
    auto key3 = atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(  \
        6).end_index(7).build(id, KeyType::TABLE_INDEX);

struct VersionMapStore : TestStore {
protected:
    std::string get_name() override {
        return "version_map";
    }
};

TEST(VersionMap, Basic) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test"};
    auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
        4).end_index(5).build(id, KeyType::TABLE_INDEX);

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    ASSERT_EQ(store->num_atom_keys(), 1);
    ASSERT_EQ(store->num_ref_keys(), 1);

    RefKey ref_key{id,  KeyType::VERSION_REF};
    auto ref_fut = store->read(ref_key, storage::ReadKeyOpts{});
    auto [key, seg] = std::move(ref_fut).get();

    ASSERT_EQ(seg.row_count(), 2);
    auto key_row_1 = read_key_row(seg, 0);
    ASSERT_EQ(key_row_1, key1);
    auto key_row_2 = read_key_row(seg, 1);
    auto version_fut = store->read(key_row_2, storage::ReadKeyOpts{});
    auto [ver_key, ver_seg] = std::move(version_fut).get();
    ASSERT_EQ(ver_seg.row_count(), 1);
    auto ver_key_row_1 = read_key_row(ver_seg, 0);
    ASSERT_EQ(ver_key_row_1, key1);
}

TEST(VersionMap, WithPredecessors) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};

    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    auto latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest.value(), key1);
    version_map->write_version(store, key2, key1);

    latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_EQ(latest.value(), key2);
    version_map->write_version(store, key3, key2);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);

}

TEST(VersionMap, TombstoneDelete) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS
    auto key4 = atom_key_builder().version_id(4).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(6).start_index(
        7).end_index(8).build(id, KeyType::TABLE_INDEX);

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    auto latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest.value(), key1);
    version_map->write_version(store, key2, key1);

    latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_EQ(latest.value(), key2);
    version_map->write_version(store, key3, key2);

    latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_EQ(latest.value(), key3);
    version_map->write_version(store, key4, key3);

    auto del_res = tombstone_version(store, version_map, id, VersionId{2});

    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key2);
    ASSERT_THAT(del_res.could_share_data, UnorderedElementsAre(key1, key3));

    std::vector<AtomKey> expected{key4, key3, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);

    del_res = tombstone_version(store, version_map, id, VersionId{3});
    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key3);
    ASSERT_THAT(del_res.could_share_data, UnorderedElementsAre(key1, key4));

    latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_EQ(latest.value(), key4);

    del_res = tombstone_version(store, version_map, id, VersionId{4});
    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key4);
    ASSERT_EQ(*del_res.could_share_data.begin(), key1);

    latest = get_latest_undeleted_version(store, version_map, id);
    ASSERT_EQ(latest.value(), key1);

    del_res = tombstone_version(store, version_map, id, VersionId{1});
    ASSERT_TRUE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key1);
    ASSERT_TRUE(del_res.could_share_data.empty());
}

TEST(VersionMap, PingPong) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    auto left = std::make_shared<VersionMap>();
    left->set_validate(true);
    auto right = std::make_shared<VersionMap>();
    right->set_validate(true);

    ScopedConfig sc("VersionMap.ReloadInterval", 0); // always reload

    auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
        4).end_index(5).build(id, KeyType::TABLE_INDEX);

    left->write_version(store, key1, std::nullopt);
    auto latest = get_latest_undeleted_version(store, right, id);
    ASSERT_EQ(latest.value(), key1);

    auto key2 = atom_key_builder().version_id(2).creation_ts(3).content_hash(4).start_index(
        5).end_index(6).build(id, KeyType::TABLE_INDEX);

    right->write_version(store, key2, key1);

    auto key3 = atom_key_builder().version_id(3).creation_ts(4).content_hash(5).start_index(
        6).end_index(7).build(id, KeyType::TABLE_INDEX);

    left->write_version(store, key3, key2);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto left_result = get_all_versions(store, left, id);
    ASSERT_EQ(left_result, expected);
    auto right_result = get_all_versions(store, right, id);
    ASSERT_EQ(right_result, expected);
}

TEST(VersionMap, TestLoadsRefAndIteration) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);

    auto key1 = atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index( \
        4).end_index(5).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key1, std::nullopt);

    auto key2 = atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(  \
        5).end_index(6).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key2, key1);

    auto key3 = atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(  \
        6).end_index(7).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key3, key2);

    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);

    auto entry_iteration = std::make_shared<VersionMapEntry>();
    version_map->load_via_iteration(store, id, entry_iteration);

    auto entry_ref = std::make_shared<VersionMapEntry>();
    version_map->load_via_ref_key(store, id, LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, entry_ref);

    ASSERT_EQ(entry_iteration->head_, entry_ref->head_);
    ASSERT_EQ(entry_iteration->keys_.size(), entry_ref->keys_.size());
    for(size_t idx = 0; idx<entry_iteration->keys_.size(); idx++)
        if(entry_iteration->keys_[idx] != entry_ref->keys_[idx]) {
            util::raise_rte("Keys Mismatch on idx {}: {} != {}", idx, entry_iteration->keys_[idx], entry_ref->keys_[idx]);
        }
    entry_iteration->validate();
    entry_ref->validate();
}

TEST(VersionMap, TestCompact) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);

    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload
    version_map->compact(store, id);

    ASSERT_EQ(store->num_atom_keys(), 2);
    ASSERT_EQ(store->num_ref_keys(), 1);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}

TEST(VersionMap, TestCompactWithDelete) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);
    tombstone_version(store, version_map, id, 2);

    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload
    version_map->compact(store, id);

    ASSERT_EQ(store->num_atom_keys(), 2);
    ASSERT_EQ(store->num_ref_keys(), 1);

    std::vector<AtomKey> expected{ key3, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}


TEST(VersionMap, TestLatestVersionWithDeleteTombstones) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);

    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);
    tombstone_version(store, version_map, id, 2);
    auto [maybe_prev, deleted] = get_latest_version(store, version_map, id);
    auto version_id = get_next_version_from_key(maybe_prev);
    ASSERT_EQ(version_id, 4);
}

TEST(VersionMap, TestCompactWithDeleteTombstones) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);
    tombstone_version(store, version_map, id, 2);

    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload
    version_map->compact(store, id);

    std::vector<AtomKey> expected{ key3, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}

TEST(VersionMap, TombstoneAllTwice) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);
    version_map->delete_all_versions(store, id);
    version_map->delete_all_versions(store, id);
    // Don't need a check condition, checking validation
}

void write_old_style_journal_entry(const AtomKey &key, std::shared_ptr<StreamSink> store) {
    IndexAggregator<RowCountIndex> journal_agg(key.id(), [&](auto &&segment) {
        store->write(KeyType::VERSION_JOURNAL,
                          key.version_id(),
                          key.id(),
                          IndexValue(NumericIndex{0}),
                          IndexValue(NumericIndex{0}),
                          std::move(segment)).wait();
    });
    journal_agg.add_key(key);
    journal_agg.commit();
}

TEST(VersionMap, GetNextVersionInEntry) {
    using namespace arcticdb;

    auto entry = std::make_shared<VersionMapEntry>();

    ASSERT_FALSE(get_next_version_in_entry(entry, 5));
    ASSERT_FALSE(get_prev_version_in_entry(entry, 0));

    auto& keys = entry->keys_;
    keys.push_back(AtomKeyBuilder().version_id(5).build<KeyType::TABLE_INDEX>(""));
    keys.push_back(AtomKeyBuilder().version_id(3).build<KeyType::TABLE_INDEX>(""));
    keys.push_back(AtomKeyBuilder().version_id(1).build<KeyType::TABLE_INDEX>(""));
    keys.push_back(AtomKeyBuilder().version_id(0).build<KeyType::TABLE_INDEX>(""));

    ASSERT_EQ(get_prev_version_in_entry(entry, 5).value(), 3);
    ASSERT_FALSE(get_next_version_in_entry(entry, 5));

    ASSERT_EQ(get_prev_version_in_entry(entry, 4).value(), 3);
    ASSERT_EQ(get_next_version_in_entry(entry, 4).value(), 5);

    ASSERT_EQ(get_prev_version_in_entry(entry, 3).value(), 1);
    ASSERT_EQ(get_next_version_in_entry(entry, 3).value(), 5);

    ASSERT_EQ(get_prev_version_in_entry(entry, 2).value(), 1);
    ASSERT_EQ(get_next_version_in_entry(entry, 2).value(), 3);

    ASSERT_EQ(get_prev_version_in_entry(entry, 1).value(), 0);
    ASSERT_EQ(get_next_version_in_entry(entry, 1).value(), 3);

    ASSERT_FALSE(get_prev_version_in_entry(entry, 0));
    ASSERT_EQ(get_next_version_in_entry(entry, 0).value(), 1);
}

TEST(VersionMap, FixRefKey) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test_fix_ref"};

    auto version_map = std::make_shared<VersionMap>();
    auto key1 = atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index( \
        4).end_index(5).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key1, std::nullopt);

    auto key2 = atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(  \
        5).end_index(6).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key2, key1);

    auto key3 = atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(  \
        6).end_index(7).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key3, key2);
    ASSERT_TRUE(version_map->check_ref_key(store, id));

    auto key4 = key3;
    version_map->write_version(store, key4, key3);

    ASSERT_FALSE(version_map->check_ref_key(store, id));

    store->remove_key_sync(RefKey{id, KeyType::VERSION_REF}, storage::RemoveOpts{});
    ASSERT_FALSE(version_map->check_ref_key(store, id));

    version_map->fix_ref_key(store, id);
    ASSERT_TRUE(version_map->check_ref_key(store, id));

    std::vector<AtomKey> expected{key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}

AtomKey atom_key_with_version(const StreamId& id, VersionId version_id, timestamp ts) {
    return atom_key_builder().version_id(version_id).creation_ts(ts).content_hash(3).start_index(4).end_index(5).build(id, KeyType::TABLE_INDEX);
}

TEST(VersionMap, FixRefKeyTombstones) {
    using namespace arcticdb;
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test_fix_ref"};

    auto version_map = std::make_shared<VersionMap>();

    auto key1 = atom_key_with_version(id, 0, 1696590624524585339);
    version_map->write_version(store, key1, std::nullopt);
    auto key2 = atom_key_with_version(id, 0, 1696590624387628801);
    version_map->write_version(store, key2, key1);
    auto key3 = atom_key_with_version(id, 0, 1696590624532320286);
    version_map->write_version(store, key3, key2);
    auto key4 = atom_key_with_version(id, 0, 1696590624554476875);
    version_map->write_version(store, key4, key3);
    auto key5 = atom_key_with_version(id, 1, 1696590624590123209);
    version_map->write_version(store, key5, key4);
    auto key6 = atom_key_with_version(id, 0, 1696590624612743245);
    auto entry = version_map->check_reload(store, id, LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, __FUNCTION__);
    version_map->journal_single_key(store, key5, entry->head_.value());

    auto valid = version_map->check_ref_key(store, id);
    ASSERT_EQ(valid, false);
}

TEST(VersionMap, RewriteVersionKeys) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test_rewrite_version_keys"};

    auto version_map = std::make_shared<VersionMap>();
    auto key1 = atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index( \
        4).end_index(5).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key1, std::nullopt);

    auto key2 = atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(  \
        5).end_index(6).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key2, key1);

    auto key3 = atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(  \
        6).end_index(7).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key3, key2);

    // the above write_version wont write index keys - only version keys
    storage::UpdateOpts update_opts;
    update_opts.upsert_ = true;
    store->update(key1, SegmentInMemory(), update_opts);
    store->update(key2, SegmentInMemory(), update_opts);
    store->update(key3, SegmentInMemory(), update_opts);
    ASSERT_TRUE(version_map->check_ref_key(store, id));

    // This will just write tombstone key
    arcticdb::tombstone_version(store, version_map, id, 2);
    arcticdb::tombstone_version(store, version_map, id, 1);

    auto index_key1 = arcticdb::get_specific_version(store, version_map, id, 1);
    auto index_key2 = arcticdb::get_specific_version(store, version_map, id, 2);

    // will be null since they were tombstoned (but not actually deleted
    ASSERT_FALSE(index_key1.has_value());
    ASSERT_FALSE(index_key2.has_value());

    version_map->remove_and_rewrite_version_keys(store, id);

    auto final_index_key1 = arcticdb::get_specific_version(store, version_map, id, 1);
    auto final_index_key2 = arcticdb::get_specific_version(store, version_map, id, 2);

    // will not be null anymore since the data actually existed
    ASSERT_TRUE(final_index_key1.has_value());
    ASSERT_TRUE(final_index_key2.has_value());

    std::vector<AtomKey> expected{key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}

TEST(VersionMap, RecoverDeleted) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test_recover"};
    THREE_SIMPLE_KEYS

    store->add_segment(key1, SegmentInMemory{});
    store->add_segment(key2, SegmentInMemory{});
    store->add_segment(key3, SegmentInMemory{});

    auto version_map = std::make_shared<VersionMap>();
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);
    auto deleted = version_map->find_deleted_version_keys(store, id);
    ASSERT_TRUE(deleted.empty());

    delete_all_keys_of_type(KeyType::VERSION, store, false);

    deleted = version_map->find_deleted_version_keys(store, id);
    ASSERT_EQ(deleted.size(), 3);
    EXPECT_THROW({ get_all_versions(store, version_map, id); }, std::runtime_error);
    version_map->recover_deleted(store, id);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id);
    ASSERT_EQ(result, expected);
}

TEST(VersionMap, StorageLogging) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test_storage_logging"};
    THREE_SIMPLE_KEYS

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_log_changes(true);
    version_map->write_version(store, key1, std::nullopt);
    version_map->write_version(store, key2, key1);
    version_map->write_version(store, key3, key2);

    tombstone_version(store, version_map, id, key1.version_id());
    tombstone_version(store, version_map, id, key3.version_id());
    tombstone_version(store, version_map, id, key2.version_id());

    std::unordered_set<AtomKey> log_keys;

    store->iterate_type(KeyType::LOG, [&](VariantKey &&vk) {
        log_keys.emplace(std::get<AtomKey>(vk));
    }, "");

    ASSERT_EQ(log_keys.size(), 6u);
    size_t write_keys = 0;
    size_t tomb_keys = 0;
    for (const auto& key: log_keys) {
        if (std::get<StringId>(key.id()) == arcticdb::WriteVersionId) {
            write_keys++;
        } else if(std::get<StringId>(key.id()) == arcticdb::TombstoneVersionId) {
            tomb_keys++;
        } else {
            FAIL();
        }
    }
    ASSERT_EQ(write_keys, 3u);
    ASSERT_EQ(tomb_keys, 3u);
}

std::shared_ptr<VersionMapEntry> write_two_versions(
    std::shared_ptr<InMemoryStore> store,
    std::shared_ptr<VersionMap> version_map,
    const StreamId& id) {
    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::NOT_LOADED, LoadObjective::INCLUDE_DELETED},
            __FUNCTION__);

    auto key1 = atom_key_with_version(id, 0, 0);
    version_map->do_write(store, key1, entry);
    write_symbol_ref(store, key1, std::nullopt, entry->head_.value());
    auto key2 = atom_key_with_version(id, 1, 1);
    version_map->do_write(store, key2, entry);
    // We override the symbol ref without a prev_key on purpose. This way we'll only load the version=1 from the ref key
    write_symbol_ref(store, key2, std::nullopt, entry->head_.value());

    return entry;
}

// Produces the following version chain: v0 <- tombstone_all <- v1 <- v2 <- tombstone
void write_alternating_deleted_undeleted(std::shared_ptr<InMemoryStore> store, std::shared_ptr<VersionMap> version_map, StreamId id) {
    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::NOT_LOADED, LoadObjective::INCLUDE_DELETED},
            __FUNCTION__);

    auto key1 = atom_key_with_version(id, 0, 0);
    auto key2 = atom_key_with_version(id, 1, 1);
    auto key3 = atom_key_with_version(id, 2, 2);

    // Write version 0
    version_map->do_write(store, key1, entry);
    write_symbol_ref(store, key1, std::nullopt, entry->head_.value());

    // Tombstone_all on version 0
    version_map->delete_all_versions(store, id);

    // Write version 1
    version_map->do_write(store, key2, entry);
    write_symbol_ref(store, key2, std::nullopt, entry->head_.value());

    // Write version 2
    version_map->do_write(store, key3, entry);
    write_symbol_ref(store, key3, std::nullopt, entry->head_.value());

    // Tombstone version 2
    version_map->write_tombstone(store, VersionId{2}, id, entry, timestamp{3});
}

TEST(VersionMap, FollowingVersionChain){
    // Set up the version chain v0(tombstone_all) <- v1 <- v2(tombstoned)
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};
    write_alternating_deleted_undeleted(store, version_map, id);

    auto check_strategy_loads_to = [&](LoadStrategy load_strategy, VersionId should_load_to){
        auto ref_entry = VersionMapEntry{};
        read_symbol_ref(store, id, ref_entry);
        auto follow_result = std::make_shared<VersionMapEntry>();

        version_map->follow_version_chain(store, ref_entry, follow_result, load_strategy);
        EXPECT_EQ(follow_result->load_progress_.oldest_loaded_index_version_, VersionId{should_load_to});
    };

    check_strategy_loads_to(LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(0)}, 0);
    check_strategy_loads_to(LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(-2)}, 1);
    // DOWN_TO will not skip through tombstoned versions even when include_deleted=false
    check_strategy_loads_to(LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-1)}, 2);
    check_strategy_loads_to(LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(0)}, 0);

    // FROM_TIME when include_deleted=false will skip through deleted versions to go to the latest undeleted version before the timestamp.
    check_strategy_loads_to(LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(10)}, 1);
    check_strategy_loads_to(LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)}, 0);
    check_strategy_loads_to(LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(2)}, 2);
    check_strategy_loads_to(LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(0)}, 0);

    check_strategy_loads_to(LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, 2);
    check_strategy_loads_to(LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}, 1);

    check_strategy_loads_to(LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}, 0);
    check_strategy_loads_to(LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY}, 0);
}

TEST(VersionMap, FollowingVersionChainWithCaching){
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    // Set up the version chain v0(tombstone_all) <- v1 <- v2(tombstoned)
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};
    write_alternating_deleted_undeleted(store, version_map, id);
    // We create an empty version map after populating the versions
    version_map = std::make_shared<VersionMap>();

    auto check_loads_versions = [&](LoadStrategy load_strategy, uint32_t should_load_any, uint32_t should_load_undeleted){
        auto loaded = version_map->check_reload(store, id, load_strategy, __FUNCTION__);
        EXPECT_EQ(loaded->get_indexes(true).size(), should_load_any);
        EXPECT_EQ(loaded->get_indexes(false).size(), should_load_undeleted);
    };

    check_loads_versions(LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(-1)}, 1, 0);
    // FROM_TIME should not be cached by the DOWNTO and should reload from storage up to the latest undeleted version, hence loading 2 versions, 1 of which is undeleted.
    check_loads_versions(LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(10)}, 2, 1);
    // LATEST should be cached by the FROM_TIME, so we still have the same 2 loaded versions
    check_loads_versions(LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, 2, 1);
    // This FROM_TIME should still use the cached 2 versions
    check_loads_versions(LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(1)}, 2, 1);

    // We just get the entry to use for the tombstone and the write
    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::NOT_LOADED, LoadObjective::INCLUDE_DELETED},
            __FUNCTION__);
    // We delete the only undeleted key
    version_map->write_tombstone(store, VersionId{1}, id, entry, timestamp{4});

    // LATEST should still be cached, but the cached entry now needs to have no undeleted keys
    check_loads_versions(LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, 2, 0);
    // FROM_TIME UNDELETED_ONLY should no longer be cached even though we used the same request before because the undeleted key it went to got deleted. So it will load the entire version chain
    check_loads_versions(LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(10)}, 3, 0);

    // We add a new undeleted key
    auto key4 = atom_key_with_version(id, 3, 5);
    version_map->do_write(store, key4, entry);
    write_symbol_ref(store, key4, std::nullopt, entry->head_.value());

    // LATEST should still be cached, but the cached entry now needs to have one more undeleted version
    check_loads_versions(LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, 4, 1);

    // We delete everything with a tombstone_all
    version_map->delete_all_versions(store, id);

    // LATEST should still be cached, but now have no undeleted versions
    check_loads_versions(LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}, 4, 0);
}

TEST(VersionMap, FollowingVersionChainEndEarlyOnTombstoneAll) {
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};

    write_two_versions(store, version_map, id);
    // Deleting should add a TOMBSTONE_ALL which should end searching for undeleted versions early.
    version_map->delete_all_versions(store, id);

    auto ref_entry = VersionMapEntry{};
    read_symbol_ref(store, id, ref_entry);
    auto follow_result = std::make_shared<VersionMapEntry>();

    for (auto load_strategy: {
        LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(0)},
        LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)},
        LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY},
        LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}
    }) {
        follow_result->clear();
        version_map->follow_version_chain(store, ref_entry, follow_result, load_strategy);
        // When loading with any of the specified load strategies with include_deleted=false we should end following the version chain early
        // at version 1 because that's when we encounter the TOMBSTONE_ALL.
        EXPECT_EQ(follow_result->load_progress_.oldest_loaded_index_version_, VersionId{1});
    }

    for (auto load_strategy: {
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(0)},
            LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(0)},
            LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED}
    }) {
        follow_result->clear();
        version_map->follow_version_chain(store, ref_entry, follow_result, load_strategy);
        // When loading with any of the specified load strategies with include_deleted=true we should continue to the beginning
        // at version 0 even though it was deleted.
        EXPECT_EQ(follow_result->load_progress_.oldest_loaded_index_version_, VersionId{0});
    }
}

TEST(VersionMap, CacheInvalidation) {
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    // Set up the version chain v0(tombstone_all) <- v1 <- v2(tombstoned)
    auto store = std::make_shared<InMemoryStore>();
    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};
    write_alternating_deleted_undeleted(store, version_map, id);

    auto check_caching = [&](LoadStrategy to_load, LoadStrategy to_check_if_cached, bool expected_outcome){
        auto clean_version_map = std::make_shared<VersionMap>();
        // Load to_load inside the clean version map cache
        clean_version_map->check_reload(store, id, to_load, __FUNCTION__);
        // Check whether to_check_if_cached is being cached by to_load
        EXPECT_EQ(clean_version_map->has_cached_entry(id, to_check_if_cached), expected_outcome);
    };

    auto check_all_caching = [&](const std::vector<LoadStrategy>& to_load, const std::vector<LoadStrategy>& to_check_if_cached, bool expected_result){
        for (auto to_load_strategy : to_load) {
            for (auto to_check_if_cached_param : to_check_if_cached){
                check_caching(to_load_strategy, to_check_if_cached_param, expected_result);
            }
        }
    };

    auto load_all_param = LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED};
    auto load_all_undeleted_param = LoadStrategy{LoadType::ALL, LoadObjective::UNDELETED_ONLY};
    check_caching(load_all_param, load_all_undeleted_param, true);
    check_caching(load_all_undeleted_param, load_all_param, false);

    constexpr auto num_versions = 3u;
    std::vector<LoadStrategy> should_load_to_v[num_versions] = {
        // Different parameters which should all load to v0
        std::vector<LoadStrategy>{
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(0)},
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(-3)},
            LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(0)},
        },

        // Different parameters which should all load to v1
        std::vector<LoadStrategy>{
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(1)},
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(-2)},
            LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(1)},
            LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY,
                         static_cast<timestamp>(2)}, // when include_deleted=false FROM_TIME searches for an undeleted version
            LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY},
        },

        // Different parameters which should all load to v2
        std::vector<LoadStrategy>{
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(2)},
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(-1)},
            LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(2)},
            LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED},
        }
    };

    for (auto i=0u; i<num_versions; ++i){
        for (auto j=0u; j<num_versions; ++j){
            // For every two versions we check that all load params for earlier versions cache load paramse for later versions:
            check_all_caching(should_load_to_v[i], should_load_to_v[j], i<=j);
        }

        // ALL and LOAD_UNDELETED because they both load to v0 (UNDELETED_ONLY will load v0 because only there it will load
        check_all_caching({load_all_param, load_all_undeleted_param}, should_load_to_v[i], true);
        check_all_caching(should_load_to_v[i], {load_all_param, load_all_undeleted_param}, false);
    }
}


TEST(VersionMap, CacheInvalidationWithTombstoneAfterLoad) {
    using namespace arcticdb;
    // Given - symbol with 2 versions - load downto version 1
    // never time-invalidate the cache so we can test our other cache invalidation logic
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    // Use a clean version_map
    version_map = std::make_shared<VersionMap>();

    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(1)},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(1)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-1)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-2)}));

    // When - we delete version 1 and reload
    version_map->write_tombstone(store, VersionId{1}, id, entry);

    // Now when the cached version is deleted, we should invalidate the cache for load parameters which look for undeleted.
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(1)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::INCLUDE_DELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::INCLUDE_DELETED, static_cast<timestamp>(1)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-1)}));

    LoadStrategy load_strategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY};
    const auto latest_undeleted_entry = version_map->check_reload(store, id, load_strategy, __FUNCTION__);

    // Then - version 0 should be returned
    ASSERT_TRUE(latest_undeleted_entry->get_first_index(false).first.has_value());
}

TEST(VersionMap, CacheInvalidationWithTombstoneAllAfterLoad) {
    using namespace arcticdb;
    // Given - symbol with 2 versions - load downto version 0
    // never time-invalidate the cache so we can test our other cache invalidation logic
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();
    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    // Use a clean version_map
    version_map = std::make_shared<VersionMap>();

    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::DOWNTO, LoadObjective::INCLUDE_DELETED, static_cast<SignedVersionId>(0)},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(1)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-1)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::DOWNTO, LoadObjective::UNDELETED_ONLY, static_cast<SignedVersionId>(-2)}));

    // When - we delete version 1
    auto tombstone_key = version_map->write_tombstone(store, VersionId{1}, id, entry);

    // We should not invalidate the cache because the version we loaded to is still undeleted
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(1)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)}));

    // When - we delete all versions without reloading
    version_map->write_tombstone_all_key_internal(store, tombstone_key, entry);

    // We should invalidate cached undeleted checks
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::LATEST, LoadObjective::UNDELETED_ONLY}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(1)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadStrategy{LoadType::FROM_TIME, LoadObjective::UNDELETED_ONLY, static_cast<timestamp>(0)}));
}

TEST(VersionMap, CompactionUpdateCache) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test"};

    auto version_map = std::make_shared<VersionMap>();

    // We load all to keep everything in the cache
    auto entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED},
            __FUNCTION__);

    // Write 10 versions
    for (auto i=0; i<10; ++i) {
        auto key = atom_key_with_version(id, i, i);
        version_map->write_version(store, key, std::nullopt);
    }

    auto assert_keys_in_entry_and_store = [&store, &id](std::shared_ptr<VersionMapEntry> entry, int expected_version_keys, int expected_index_keys, int expected_tombstone_keys){
        int present_version_keys = 0, present_index_keys = 0, present_tombstone_keys = 0;
        auto all_entry_keys = entry->keys_;
        if (entry->head_) all_entry_keys.push_back(entry->head_.value());
        for (const auto& key : all_entry_keys) {
            if (key.type() == KeyType::VERSION) ++present_version_keys;
            if (key.type() == KeyType::TABLE_INDEX) ++present_index_keys;
            if (key.type() == KeyType::TOMBSTONE) ++present_tombstone_keys;
        }
        ASSERT_EQ(present_version_keys, expected_version_keys);
        ASSERT_EQ(present_index_keys, expected_index_keys);
        ASSERT_EQ(present_tombstone_keys, expected_tombstone_keys);

        int version_keys_in_store = 0;
        store->iterate_type(KeyType::VERSION, [&](VariantKey &&){++version_keys_in_store;});
        ASSERT_EQ(version_keys_in_store, expected_version_keys);
    };

    assert_keys_in_entry_and_store(entry, 10, 10, 0);
    version_map->compact(store, id);
    assert_keys_in_entry_and_store(entry, 2, 10, 0);


    // Write 10 more versions but delete some
    for (auto i=10; i<20; ++i) {
        auto key = atom_key_with_version(id, i, i);
        version_map->write_version(store, key, std::nullopt);
        if (i%3 == 0) {
            version_map->write_tombstone(store, VersionId{static_cast<uint64_t>(i)}, id, entry);
        }
    }
    assert_keys_in_entry_and_store(entry, 15, 20, 3);
    version_map->compact(store, id);
    assert_keys_in_entry_and_store(entry, 2, 20, 3);
    // TODO: If we ever use compact_and_remove_deleted_indexes fix the below assertions (method is currently unused with TODOs to fix):
    // version_map->compact_and_remove_deleted_indexes(store, id);
    // assert_keys_in_entry_and_store(entry, 2, 17, 3);

    // Flush and reload to see that what we have in storage also matches what we have in the cache.
    version_map->flush();
    entry = version_map->check_reload(
            store,
            id,
            LoadStrategy{LoadType::ALL, LoadObjective::INCLUDE_DELETED},
            __FUNCTION__);
    assert_keys_in_entry_and_store(entry, 2, 20, 3);
}

#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

TEST_F(VersionMapStore, StressTestWrite) {
    using namespace arcticdb;
    std::vector<AtomKey> keys;
    const size_t num_tests = 999;
    StreamId id{"test"};
    for (auto i = 0ULL; i < num_tests; ++i) {
        keys.emplace_back(
                atom_key_builder().version_id(i).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(i).start_index( \
                4).end_index(5).build(id, KeyType::TABLE_INDEX));
    }

    auto version_map = std::make_shared<VersionMap>();
    std::string timer_name("write_stress");
    interval_timer timer(timer_name);
    std::optional<AtomKey> previous_key;
    for(const auto& key : keys) {
        version_map->write_version(test_store_->_test_get_store(), key, previous_key);
        previous_key = key;
    }
    timer.stop_timer(timer_name);
    GTEST_COUT << timer.display_all() << std::endl;
}

} // namespace arcticdb
