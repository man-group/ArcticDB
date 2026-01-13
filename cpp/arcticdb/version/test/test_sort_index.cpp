/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>
#include <arcticdb/pipeline/index_writer.hpp>
#include "storage/test/in_memory_store.hpp"

TEST(SortIndex, Basic) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    using namespace arcticdb::pipelines;

    std::vector<SliceAndKey> keys_and_slices;
    StreamId stream_id{"sort_index"};

    for (auto i = 0; i < 20; ++i) {
        auto key =
                atom_key_builder().start_index(i).end_index(i + 1).creation_ts(i).build<KeyType::TABLE_DATA>(stream_id);
        FrameSlice slice{ColRange{0, 5}, RowRange{i, i + 1}};
        keys_and_slices.emplace_back(SliceAndKey{slice, key});
    }

    auto copy = keys_and_slices;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(std::begin(keys_and_slices), std::end(keys_and_slices), g);
    const auto version_id = VersionId{0};

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    auto desc = stream_descriptor(stream_id, stream::RowCountIndex(), {scalar_field(DataType::UTF_DYNAMIC64, "col_1")});
    TimeseriesDescriptor timeseries_desc;
    timeseries_desc.set_stream_descriptor(desc);
    index::IndexWriter<stream::RowCountIndex> index_writer(mock_store, partial_key, std::move(timeseries_desc));
    for (const auto& [maybe_seg, slice, key] : keys_and_slices)
        index_writer.add_unchecked(*key, slice);

    auto key_fut = index_writer.commit();
    auto index_key = std::move(key_fut).get();
    VersionMap version_map;
    version_map.write_version(mock_store, index_key, std::nullopt);

    auto engine = version_store::LocalVersionedEngine::_test_init_from_store(mock_store, util::SysClock{});
    auto versioned_item = engine.sort_index(stream_id, false, false);

    auto seg = mock_store->read(versioned_item.key_, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto vec = unfiltered_index(isr);

    ASSERT_EQ(vec, copy);
}

TEST(SortIndex, Nonzero) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    using namespace arcticdb::pipelines;

    std::vector<SliceAndKey> keys_and_slices;
    StreamId stream_id{"sort_index_non_zero"};

    for (auto i = 0; i < 20; ++i) {
        auto key =
                atom_key_builder().start_index(i).end_index(i + 1).creation_ts(i).build<KeyType::TABLE_DATA>(stream_id);
        FrameSlice slice{ColRange{0, 5}, RowRange{i + 5, i + 6}};
        keys_and_slices.emplace_back(SliceAndKey{slice, key});
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(std::begin(keys_and_slices), std::end(keys_and_slices), g);
    const auto version_id = VersionId{0};

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    auto desc = stream_descriptor(stream_id, stream::RowCountIndex(), {scalar_field(DataType::UTF_DYNAMIC64, "col_1")});
    TimeseriesDescriptor timeseries_desc;
    timeseries_desc.set_stream_descriptor(desc);
    index::IndexWriter<stream::RowCountIndex> index_writer(mock_store, partial_key, std::move(timeseries_desc));
    for (const auto& [maybe_seg, slice, key] : keys_and_slices)
        index_writer.add_unchecked(*key, slice);

    auto key_fut = index_writer.commit();
    auto index_key = std::move(key_fut).get();
    VersionMap version_map;
    version_map.write_version(mock_store, index_key, std::nullopt);

    auto engine = version_store::LocalVersionedEngine::_test_init_from_store(mock_store, util::SysClock{});
    auto versioned_item = engine.sort_index(stream_id, false, false);

    auto seg = mock_store->read(versioned_item.key_, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto vec = unfiltered_index(isr);

    std::vector<SliceAndKey> expected;

    for (auto i = 0; i < 20; ++i) {
        auto key =
                atom_key_builder().start_index(i).end_index(i + 1).creation_ts(i).build<KeyType::TABLE_DATA>(stream_id);
        FrameSlice slice{ColRange{0, 5}, RowRange{i, i + 1}};
        expected.emplace_back(SliceAndKey{slice, key});
    }

    ASSERT_EQ(vec, expected);
}