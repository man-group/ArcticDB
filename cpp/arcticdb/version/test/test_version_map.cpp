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
    pipelines::VersionQuery version_query;
    version_query.set_iterate_on_failure(false);
    auto latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest.value(), key1);
    version_map->write_version(store, key2, key1);

    latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_EQ(latest.value(), key2);
    version_map->write_version(store, key3, key2);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id, version_query);
    ASSERT_EQ(result, expected);

}

TEST(VersionMap, TombstoneDelete) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};
    THREE_SIMPLE_KEYS
    auto key4 = atom_key_builder().version_id(4).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(6).start_index(
        7).end_index(8).build(id, KeyType::TABLE_INDEX);

    pipelines::VersionQuery version_query;
    version_query.set_iterate_on_failure(false);

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    version_map->write_version(store, key1, std::nullopt);
    auto latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_TRUE(latest);
    ASSERT_EQ(latest.value(), key1);
    version_map->write_version(store, key2, key1);

    latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_EQ(latest.value(), key2);
    version_map->write_version(store, key3, key2);

    latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_EQ(latest.value(), key3);
    version_map->write_version(store, key4, key3);

    auto del_res = tombstone_version(store, version_map, id, VersionId{2}, pipelines::VersionQuery{});

    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key2);
    ASSERT_THAT(del_res.could_share_data, UnorderedElementsAre(key1, key3));

    std::vector<AtomKey> expected{key4, key3, key1};
    auto result = get_all_versions(store, version_map, id, version_query);
    ASSERT_EQ(result, expected);

    del_res = tombstone_version(store, version_map, id, VersionId{3}, pipelines::VersionQuery{});
    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key3);
    ASSERT_THAT(del_res.could_share_data, UnorderedElementsAre(key1, key4));

    latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_EQ(latest.value(), key4);

    del_res = tombstone_version(store, version_map, id, VersionId{4}, pipelines::VersionQuery{});
    ASSERT_FALSE(del_res.no_undeleted_left);
    ASSERT_EQ(del_res.keys_to_delete.front(), key4);
    ASSERT_EQ(*del_res.could_share_data.begin(), key1);

    latest = get_latest_undeleted_version(store, version_map, id, version_query);
    ASSERT_EQ(latest.value(), key1);

    del_res = tombstone_version(store, version_map, id, VersionId{1}, pipelines::VersionQuery{});
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
    auto latest = get_latest_undeleted_version(store, right, id, pipelines::VersionQuery{});
    ASSERT_EQ(latest.value(), key1);

    auto key2 = atom_key_builder().version_id(2).creation_ts(3).content_hash(4).start_index(
        5).end_index(6).build(id, KeyType::TABLE_INDEX);

    right->write_version(store, key2, key1);

    auto key3 = atom_key_builder().version_id(3).creation_ts(4).content_hash(5).start_index(
        6).end_index(7).build(id, KeyType::TABLE_INDEX);

    left->write_version(store, key3, key2);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto left_result = get_all_versions(store, left, id, pipelines::VersionQuery{});
    ASSERT_EQ(left_result, expected);
    auto right_result = get_all_versions(store, right, id, pipelines::VersionQuery{});
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
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
    ASSERT_EQ(result, expected);

    auto entry_iteration = std::make_shared<VersionMapEntry>();
    version_map->load_via_iteration(store, id, entry_iteration);

    auto entry_ref = std::make_shared<VersionMapEntry>();
    version_map->load_via_ref_key(store, id, LoadParameter{LoadType::LOAD_ALL}, entry_ref);

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
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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
    tombstone_version(store, version_map, id, 2, pipelines::VersionQuery{});

    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload
    version_map->compact(store, id);

    ASSERT_EQ(store->num_atom_keys(), 2);
    ASSERT_EQ(store->num_ref_keys(), 1);

    std::vector<AtomKey> expected{ key3, key1};
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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
    tombstone_version(store, version_map, id, 2, pipelines::VersionQuery{});
    auto [maybe_prev, deleted] = get_latest_version(store, version_map, id, pipelines::VersionQuery{});
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
    tombstone_version(store, version_map, id, 2, pipelines::VersionQuery{});

    ScopedConfig max_blocks("VersionMap.MaxVersionBlocks", 1);
    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0); // always reload
    version_map->compact(store, id);

    std::vector<AtomKey> expected{ key3, key1};
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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

TEST(VersionMap, IterateOnFailure) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test1"};

    auto version_map = std::make_shared<VersionMap>();
    version_map->set_validate(true);
    auto key1 =
        atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index(
            4).end_index(5).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key1, std::nullopt);
    auto key2 =
        atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(
            5).end_index(6).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key2, key1);
    auto key3 =
        atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(
            6).end_index(7).build(id, KeyType::TABLE_INDEX);
    version_map->write_version(store, key3, key2);

    RefKey ref_key{id, KeyType::VERSION_REF};
    store->remove_key_sync(ref_key, storage::RemoveOpts{});

    std::vector<AtomKey> expected{ key3, key2, key1};
    pipelines::VersionQuery version_query;
    version_query.set_iterate_on_failure(true);
    auto result = get_all_versions(store, version_map, id, version_query);
    ASSERT_EQ(result, expected);
}

TEST(VersionMap, GetNextVersionInEntry) {
    using namespace arcticdb;

    auto entry = std::make_shared<VersionMapEntry>();

    ASSERT_FALSE(get_next_version_in_entry(entry, 5));
    ASSERT_FALSE(get_prev_version_in_entry(entry, 0));

    entry->keys_.push_back(AtomKeyBuilder().version_id(5).build<KeyType::TABLE_INDEX>(""));
    entry->keys_.push_back(AtomKeyBuilder().version_id(3).build<KeyType::TABLE_INDEX>(""));
    entry->keys_.push_back(AtomKeyBuilder().version_id(1).build<KeyType::TABLE_INDEX>(""));
    entry->keys_.push_back(AtomKeyBuilder().version_id(0).build<KeyType::TABLE_INDEX>(""));

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
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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
    auto entry = version_map->check_reload(store, id, LoadParameter{LoadType::LOAD_LATEST},  __FUNCTION__);
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
    storage::StorageUpdateOptions update_opts;
    update_opts.upsert_ = true;
    store->update(key1, SegmentInMemory(), update_opts);
    store->update(key2, SegmentInMemory(), update_opts);
    store->update(key3, SegmentInMemory(), update_opts);
    ASSERT_TRUE(version_map->check_ref_key(store, id));

    // This will just write tombstone key
    arcticdb::tombstone_version(store, version_map, id, 2, pipelines::VersionQuery{});
    arcticdb::tombstone_version(store, version_map, id, 1, pipelines::VersionQuery{});

    auto index_key1 = arcticdb::get_specific_version(store, version_map, id, 1, pipelines::VersionQuery{});
    auto index_key2 = arcticdb::get_specific_version(store, version_map, id, 2, pipelines::VersionQuery{});

    // will be null since they were tombstoned (but not actually deleted
    ASSERT_FALSE(index_key1.has_value());
    ASSERT_FALSE(index_key2.has_value());

    version_map->remove_and_rewrite_version_keys(store, id);

    auto final_index_key1 = arcticdb::get_specific_version(store, version_map, id, 1, pipelines::VersionQuery{});
    auto final_index_key2 = arcticdb::get_specific_version(store, version_map, id, 2, pipelines::VersionQuery{});

    // will not be null anymore since the data actually existed
    ASSERT_TRUE(final_index_key1.has_value());
    ASSERT_TRUE(final_index_key2.has_value());

    std::vector<AtomKey> expected{key3, key2, key1};
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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
    EXPECT_THROW({ get_all_versions(store, version_map, id, pipelines::VersionQuery{}); }, std::runtime_error);
    version_map->recover_deleted(store, id);

    std::vector<AtomKey> expected{ key3, key2, key1};
    auto result = get_all_versions(store, version_map, id, pipelines::VersionQuery{});
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

    tombstone_version(store, version_map, id, key1.version_id(), pipelines::VersionQuery{});
    tombstone_version(store, version_map, id, key3.version_id(), pipelines::VersionQuery{});
    tombstone_version(store, version_map, id, key2.version_id(), pipelines::VersionQuery{});

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

std::shared_ptr<VersionMapEntry> write_two_versions(std::shared_ptr<InMemoryStore> store, std::shared_ptr<VersionMap> version_map, StreamId id) {
    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::NOT_LOADED},
            __FUNCTION__);

    auto key1 = atom_key_with_version(id, 0, 0);
    version_map->do_write(store, key1, entry);
    write_symbol_ref(store, key1, std::nullopt, entry->head_.value());
    auto key2 = atom_key_with_version(id, 1, 1);
    version_map->do_write(store, key2, entry);
    write_symbol_ref(store, key2, std::nullopt, entry->head_.value());

    return entry;
}

TEST(VersionMap, CacheInvalidationLoadDownTo) {
    using namespace arcticdb;
    // Given - symbol with 2 versions - load downto version 1
    // never time-invalidate the cache so we can test our other cache invalidation logic
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));

    // When - we delete version 1 and reload
    uint64_t ver_to_delete = 1;
    auto tombstone = version_map->write_tombstone(store, ver_to_delete, id, entry);
    entry->tombstones_.try_emplace(ver_to_delete, std::move(tombstone));

    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));

    LoadParameter load_param{LoadType::LOAD_LATEST_UNDELETED};
    const auto latest_undeleted_entry = version_map->check_reload(store, id, load_param, __FUNCTION__);

    // Then - version 0 should be returned
    ASSERT_TRUE(latest_undeleted_entry->get_first_index(false).first.has_value());
}

TEST(VersionMap, CacheInvalidationLoadDownToFurther) {
    using namespace arcticdb;
    // Given - symbol with 2 versions - load downto version 1
    // never time-invalidate the cache so we can test our other cache invalidation logic
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)},
            __FUNCTION__);

    ASSERT_FALSE(version_map->has_cached_entry(id,
                                               LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)}));
    ASSERT_EQ(entry->get_indexes(false).size(), 1);

    // When - we load down to version 0
    const auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)},
            __FUNCTION__);

    // Then - version 0 should be returned
    ASSERT_EQ(entry->get_indexes(false).size(), 2);
    ASSERT_TRUE(
            version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)}));
}

TEST(VersionMap, CacheInvalidationLoadDownToAfterLoadLatest) {
    using namespace arcticdb;
    // Given - symbol with 2 versions - load down to version 1
    // never time-invalidate the cache so we can test our other cache invalidation logic
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test"};

    auto version_map = std::make_shared<VersionMap>();

    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_LATEST},
            __FUNCTION__);

    auto key = atom_key_with_version(id, 0, 0);
    version_map->do_write(store, key, entry);
    write_symbol_ref(store, key, std::nullopt, entry->head_.value());

    // When - we load latest then down to version 0
    auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)},
            __FUNCTION__);

    // Then - version 0 should be returned
    ASSERT_EQ(reloaded->get_indexes(false).size(), 1);
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)}));
}

TEST(VersionMap, CacheInvalidationAfterLoadLatestUndeleted) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_LATEST_UNDELETED},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::NOT_LOADED}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_ALL}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(1234)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)}));

    const auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)},
            __FUNCTION__);

    ASSERT_EQ(reloaded->get_indexes(false).size(), 2);
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)}));
}

TEST(VersionMap, CacheInvalidationAfterLoadFromTime) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(123)},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::NOT_LOADED}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_ALL}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(1234)}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_UNDELETED}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)}));

    const auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(0)},
            __FUNCTION__);

    ASSERT_EQ(reloaded->get_indexes(false).size(), 2);
}

TEST(VersionMap, CacheInvalidationAfterLoadFromTimeTombstoned) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    auto entry = write_two_versions(store, version_map, id);

    uint64_t ver_to_delete = 1;
    auto tombstone = version_map->write_tombstone(store, ver_to_delete, id, entry);

    version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(2)},
            __FUNCTION__);

    // LOAD_FROM_TIME always goes at least as far back as the latest undeleted version, even if that is earlier than
    // the specified load_from_time_or_until
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST}));

    const auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_LATEST_UNDELETED},
            __FUNCTION__);

    ASSERT_EQ(reloaded->get_indexes(false).size(), 1);
    ASSERT_EQ(reloaded->get_indexes(true).size(), 2);
}

TEST(VersionMap, CacheInvalidationLoadAllAfterLoadFromTime) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();

    auto version_map = std::make_shared<VersionMap>();

    StreamId id{"test"};
    write_two_versions(store, version_map, id);

    auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(1)},
            __FUNCTION__);

    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_ALL}));
    ASSERT_EQ(reloaded->get_indexes(false).size(), 1);

    reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_ALL},
            __FUNCTION__);

    ASSERT_EQ(reloaded->get_indexes(false).size(), 2);
}

TEST(VersionMap, CacheInvalidationAfterLoadUndeleted) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test"};

    auto version_map = std::make_shared<VersionMap>();

    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::NOT_LOADED},
            __FUNCTION__);

    auto key1 = atom_key_with_version(id, 0, 0);
    version_map->do_write(store, key1, entry);
    write_symbol_ref(store, key1, std::nullopt, entry->head_.value());
    auto key2 = atom_key_with_version(id, 1, 1);
    version_map->do_write(store, key2, entry);
    write_symbol_ref(store, key2, std::nullopt, entry->head_.value());
    auto key3 = atom_key_with_version(id, 2, 2);
    version_map->do_write(store, key3, entry);
    write_symbol_ref(store, key3, std::nullopt, entry->head_.value());

    auto tombstone_all = get_tombstone_all_key(key2, 3);
    entry->try_set_tombstone_all(tombstone_all);
    version_map->do_write(store, tombstone_all, entry);
    write_symbol_ref(store, key2, std::nullopt, entry->head_.value());

    entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_UNDELETED},
            __FUNCTION__);

    ASSERT_EQ(entry->get_indexes(true).size(), 2);
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::NOT_LOADED}));
    ASSERT_FALSE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_ALL}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(1234)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)}));

    // Should not rely on the cache upon (eg) a LOAD_ALL here hence pick up an extra INDEX
    auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_ALL},
            __FUNCTION__);

    ASSERT_EQ(reloaded->get_indexes(true).size(), 3);
}

TEST(VersionMap, CacheInvalidationAfterLoadAll) {
    using namespace arcticdb;
    ScopedConfig sc("VersionMap.ReloadInterval", std::numeric_limits<int64_t>::max());
    auto store = std::make_shared<InMemoryStore>();
    StreamId id{"test"};

    auto version_map = std::make_shared<VersionMap>();

    auto entry = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_ALL},
            __FUNCTION__);

    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_LATEST}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::NOT_LOADED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_ALL}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_FROM_TIME, static_cast<timestamp>(1234)}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_UNDELETED}));
    ASSERT_TRUE(version_map->has_cached_entry(id, LoadParameter{LoadType::LOAD_DOWNTO, static_cast<SignedVersionId>(1)}));

    // Write a key to storage but not the in memory version map
    auto key = atom_key_with_version(id, 0, 0);
    auto other_version_map = std::make_shared<VersionMap>();
    auto other_entry = other_version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_ALL},
            __FUNCTION__);
    other_version_map->do_write(store, key, other_entry);

    // Then - it should not be visible to the original map as we rely on the cache
    auto reloaded = version_map->check_reload(
            store,
            id,
            LoadParameter{LoadType::LOAD_UNDELETED},
            __FUNCTION__);

    ASSERT_TRUE(reloaded->get_indexes(false).empty());
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
