/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/pipeline/write_frame.hpp>

using namespace arcticdb;

static auto write_version_frame_with_three_segments(
        const arcticdb::StreamId& stream_id, arcticdb::VersionId v_id, arcticdb::version_store::PythonVersionStore& pvs
) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    auto de_dup_map = std::make_shared<arcticdb::DeDupMap>();
    SlicingPolicy slicing = FixedSlicer{100, 10}; // 100 cols per segment, 10 rows per segment
    IndexPartialKey pk{stream_id, v_id};
    auto wrapper = get_test_simple_frame(stream_id, 30, 0); // 30 rows -> 3 segments
    auto& frame = wrapper.frame_;
    auto store = pvs._test_get_store();
    auto key = write_frame(std::move(pk), frame, slicing, store, de_dup_map).get();
    pvs.write_version_and_prune_previous(true, key, std::nullopt);
    return key;
}

TEST(KeyUtils, RecurseIndexKeyNothingMissing) {
    // Given
    auto [version_store, mock_store] = python_version_store_in_memory();
    storage::ReadKeyOpts opts;

    StreamId first_id{"first"};
    write_version_frame_with_three_segments(first_id, 0, version_store);
    StreamId second_id{"second"};
    write_version_frame_with_three_segments(second_id, 0, version_store);

    std::vector<AtomKeyImpl> index_keys;
    mock_store->iterate_type(KeyType::TABLE_INDEX, [&](auto&& vk) {
        index_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    std::vector<AtomKeyImpl> data_keys;
    mock_store->iterate_type(KeyType::TABLE_DATA, [&](auto&& vk) {
        data_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    // When
    auto res = recurse_index_keys(mock_store, index_keys, opts);

    // Then
    ASSERT_EQ(res.size(), 6);

    // When
    auto first_key = index_keys.at(0);
    auto keys = std::vector<AtomKeyImpl>{first_key};
    res = recurse_index_keys(mock_store, keys, opts);

    // Then
    ASSERT_EQ(res.size(), 3);
}

TEST(KeyUtils, RecurseIndexKeyDoNotIgnoreMissing) {
    // Given
    auto [version_store, mock_store] = python_version_store_in_memory();
    storage::ReadKeyOpts opts;

    StreamId first_id{"first"};
    write_version_frame_with_three_segments(first_id, 0, version_store);
    StreamId second_id{"second"};
    write_version_frame_with_three_segments(second_id, 0, version_store);

    std::vector<AtomKeyImpl> index_keys;
    mock_store->iterate_type(KeyType::TABLE_INDEX, [&](auto&& vk) {
        index_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    storage::RemoveOpts remove_opts;
    mock_store->remove_key(index_keys.at(1), remove_opts);

    std::vector<AtomKeyImpl> data_keys;
    mock_store->iterate_type(KeyType::TABLE_DATA, [&](auto&& vk) {
        data_keys.emplace_back(std::get<AtomKeyImpl>(std::move(vk)));
    });

    // When & Then
    ASSERT_THROW(recurse_index_keys(mock_store, index_keys, opts), storage::KeyNotFoundException);
}

TEST(KeyUtils, RecurseIndexKeyIgnoreMissing) {
    // Given
    auto [version_store, mock_store] = python_version_store_in_memory();

    const StreamId first_id{"first"};
    write_version_frame_with_three_segments(first_id, 0, version_store);
    const StreamId second_id{"second"};
    write_version_frame_with_three_segments(second_id, 0, version_store);
    const StreamId third_id{"third"};
    write_version_frame_with_three_segments(third_id, 0, version_store);

    std::vector<AtomKeyImpl> index_keys;
    AtomKeyImpl index_for_second;
    mock_store->iterate_type(KeyType::TABLE_INDEX, [&](auto&& vk) {
        AtomKey ak = std::get<AtomKeyImpl>(vk);
        index_keys.push_back(ak);
        if (ak.id() == second_id) {
            index_for_second = ak;
        }
    });
    ASSERT_EQ(index_keys.size(), 3);
    ASSERT_EQ(index_for_second.id(), second_id);

    mock_store->remove_key(index_for_second, storage::RemoveOpts{});

    // When
    storage::ReadKeyOpts opts;
    opts.ignores_missing_key_ = true;
    auto res = recurse_index_keys(mock_store, index_keys, opts);

    // Then
    ASSERT_EQ(res.size(), 6);
    int count_first = 0;
    int count_third = 0;
    for (const AtomKey& atom_key : res) {
        if (atom_key.id() == first_id) {
            count_first++;
        } else if (atom_key.id() == third_id) {
            count_third++;
        }
    }
    ASSERT_EQ(3, count_first);
    ASSERT_EQ(3, count_third);
}
