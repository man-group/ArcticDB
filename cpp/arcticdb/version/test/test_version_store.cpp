/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/codec/default_codecs.hpp>

#include <filesystem>
#include <chrono>
#include <thread>
#include <folly/futures/Barrier.h>

struct VersionStoreTest : arcticdb::TestStore {
protected:
    std::string get_name() override {
        return "test.version_store";
    }
};

auto write_version_frame(
    const StreamId& stream_id,
    VersionId v_id,
    arcticdb::version_store::PythonVersionStore& pvs,
    size_t rows = 1000000,
    bool update_version_map = false,
    size_t start_val = 0,
    const std::shared_ptr<arcticdb::DeDupMap>& de_dup_map = std::make_shared<arcticdb::DeDupMap>()
) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    SlicingPolicy slicing = FixedSlicer{};
    IndexPartialKey pk{stream_id, v_id};
    auto wrapper = get_test_simple_frame(stream_id, rows, start_val);
    auto& frame = wrapper.frame_;
    auto store = pvs._test_get_store();
    auto var_key = write_frame(std::move(pk), std::move(frame), slicing, store, de_dup_map).get();
    auto key = to_atom(var_key); // Moves
    if (update_version_map) {
        pvs._test_get_version_map()->write_version(store, key);
    }

    return key;
}

TEST(PythonVersionStore, FreesMemory) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    {
        auto version_store = get_test_engine<version_store::PythonVersionStore>();

        write_version_frame({"test_versioned_engine_write"}, 0, version_store, 10);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT_TRUE(Allocator::empty());
}

TEST(PythonVersionStore, DeleteDatabase) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    auto [version_store, mock_store] = python_version_store_in_memory();

    write_version_frame({"test_versioned_engine_delete"}, 0, version_store, 50, true);

    version_store.clear();
    ASSERT_EQ(mock_store->num_atom_keys(), 0);
    ASSERT_EQ(mock_store->num_ref_keys(), 0);
}

TEST(PythonVersionStore, WriteWithPruneVersions) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    auto [version_store, mock_store] = python_version_store_in_memory();

    auto key = write_version_frame({"test_versioned_engine_delete"}, 0, version_store, 30, true);
    version_store._test_get_version_map()->write_and_prune_previous(mock_store, key, std::nullopt);
    // Should have pruned the previous version and have just one version
    ASSERT_EQ(mock_store->num_atom_keys_of_type(KeyType::TABLE_INDEX), 1);
}

TEST(PythonVersionStore, DeleteAllVersions) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    auto [version_store, mock_store] = python_version_store_in_memory();

    StreamId stream_id{"test_versioned_engine_delete_sym"};
    write_version_frame(stream_id, 0, version_store, 15, true, 2);
    write_version_frame(stream_id, 1, version_store, 1, true, 0);

    version_store.delete_all_versions(stream_id);
    ASSERT_EQ(mock_store->num_atom_keys_of_type(KeyType::TABLE_INDEX), 0);
}

TEST(PythonVersionStore, IterationVsRefWrite) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    auto [version_store, mock_store] = python_version_store_in_memory();

    std::string stream_id("test_iter_vs_ref");

    auto version_map = version_store._test_get_version_map();

    write_version_frame(stream_id, 0, version_store, 1000000, true);
    write_version_frame(stream_id, 1, version_store, 1000000, true);
    write_version_frame(stream_id, 2, version_store, 1000000, true);

    auto iter_entry = std::make_shared<VersionMapEntry>();
    auto ref_entry = std::make_shared<VersionMapEntry>();

    version_map->load_via_iteration(mock_store, stream_id, iter_entry);
    version_map->load_via_ref_key(mock_store, stream_id, LoadParameter{LoadType::LOAD_ALL}, ref_entry);

    EXPECT_EQ(std::string(iter_entry->head_.value().view()), std::string(ref_entry->head_.value().view()));
    ASSERT_EQ(iter_entry->keys_.size(), ref_entry->keys_.size());
    for (size_t idx = 0; idx != iter_entry->keys_.size(); idx++) {
        EXPECT_EQ(std::string(iter_entry->keys_[idx].view()), std::string(ref_entry->keys_[idx].view()));
    }

    //Testing the method after compaction
    version_map->compact(mock_store, stream_id);

    auto iter_entry_compact = std::make_shared<VersionMapEntry>();
    auto ref_entry_compact = std::make_shared<VersionMapEntry>();

    version_map->load_via_iteration(mock_store, stream_id, iter_entry_compact);
    version_map->load_via_ref_key(mock_store, stream_id, LoadParameter{LoadType::LOAD_ALL}, ref_entry_compact);

    EXPECT_EQ(std::string(iter_entry_compact->head_.value().view()), std::string(ref_entry_compact->head_.value().view()));
    ASSERT_EQ(iter_entry_compact->keys_.size(), ref_entry_compact->keys_.size());
    for (size_t idx = 0; idx != iter_entry_compact->keys_.size(); idx++) {
        EXPECT_EQ(std::string(iter_entry_compact->keys_[idx].view()), std::string(ref_entry_compact->keys_[idx].view()));
    }
}

TEST_F(VersionStoreTest, SortMerge) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    size_t count = 0;

    std::vector<SegmentToInputFrameAdapter> data;
    StreamId symbol{"compact_me"};

    for (auto i = 0; i < 10; ++i) {
        auto wrapper = SinkWrapper(symbol, {
            scalar_field(DataType::UINT64, "thing1"),
            scalar_field(DataType::UINT64,  "thing2")
        });

        for(auto j = 0; j < 20; ++j ) {
            wrapper.aggregator_.start_row(timestamp(count++))([&](auto &&rb) {
                rb.set_scalar(1, j);
                rb.set_scalar(2, i + j);
            });
        }

        wrapper.aggregator_.commit();
        data.emplace_back( SegmentToInputFrameAdapter{std::move(wrapper.segment())});
    }
    std::mt19937 mt{42};
    std::shuffle(data.begin(), data.end(), mt);

    for(auto&& frame : data) {
        test_store_->append_incomplete_frame(symbol, std::move(frame.input_frame_));
    }

    test_store_->sort_merge_internal(symbol, std::nullopt, true, false, false, false);
}

TEST_F(VersionStoreTest, CompactIncompleteDynamicSchema) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    size_t count = 0;

    std::vector<SegmentToInputFrameAdapter> data;
    StreamId symbol{"compact_me_dynamic"};

    for (size_t i = 0; i < 10; ++i) {
        auto wrapper = SinkWrapper(symbol, {
            scalar_field(DataType::UINT64, "thing1"),
            scalar_field(DataType::UINT64, "thing2"),
            scalar_field(DataType::UINT64, "thing3"),
            scalar_field(DataType::UINT64, "thing4")
        });

        for(size_t j = 0; j < 20; ++j ) {
            wrapper.aggregator_.start_row(timestamp(count++))([&](auto &&rb) {
                rb.set_scalar(1, j);
                rb.set_scalar(2, i);
                rb.set_scalar(3, i + j);
                rb.set_scalar(4, i * j);
            });
        }

        wrapper.aggregator_.commit();
        wrapper.segment().drop_column(fmt::format("thing{}", (i % 3) + 1));
        data.emplace_back( SegmentToInputFrameAdapter{std::move(wrapper.segment())});
    }
    std::mt19937 mt{42};
    std::shuffle(data.begin(), data.end(), mt);

    for(auto& frame : data) {
        test_store_->write_parallel_frame(symbol, std::move(frame.input_frame_));
    }

    auto vit = test_store_->compact_incomplete(symbol, false, false, true, false);
    ReadQuery read_query;
    auto read_result = test_store_->read_dataframe_version(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto& seg = read_result.frame_data.frame();

    count = 0;
    auto col1_pos = seg.column_index( "thing1").value();
    auto col2_pos = seg.column_index( "thing2").value();
    auto col3_pos = seg.column_index( "thing3").value();
    auto col4_pos = seg.column_index( "thing4").value();

    for (size_t i = 0; i < 10; ++i) {
        auto dropped_column = (i % 3) + 1;
        for(size_t j = 0; j < 20; ++j ) {
            auto idx = seg.scalar_at<uint64_t>(count, 0);
            ASSERT_EQ(idx.value(), count);
            auto v1 = seg.scalar_at<uint64_t>(count, col1_pos);
            auto expected = dropped_column == 1 ? 0 : j;
            ASSERT_EQ(v1.value(), expected);
            auto v2 = seg.scalar_at<uint64_t>(count , col2_pos);
            expected = dropped_column == 2 ? 0 : i;
            ASSERT_EQ(v2.value(), expected);
            auto v3 = seg.scalar_at<uint64_t>(count, col3_pos);
            expected = dropped_column == 3 ? 0 : i + j;
            ASSERT_EQ(v3.value(), expected);
            auto v4 = seg.scalar_at<uint64_t>(count , col4_pos);
            expected = dropped_column == 4 ? 0 : i * j;
            ASSERT_EQ(v4.value(), expected);
            ++count;
        }
    }
}

TEST_F(VersionStoreTest, GetIncompleteSymbols) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    std::string stream_id1{"thing1"};
    auto wrapper1 = get_test_simple_frame(stream_id1, 15, 2);
    auto& frame1 = wrapper1.frame_;
    test_store_->append_incomplete_frame(stream_id1, std::move(frame1));

    std::string stream_id2{"thing2"};
    auto wrapper2 = get_test_simple_frame(stream_id2, 15, 2);
    auto& frame2 = wrapper2.frame_;
    test_store_->append_incomplete_frame(stream_id2, std::move(frame2));

    std::string stream_id3{"thing3"};
    auto wrapper3 = get_test_simple_frame(stream_id3, 15, 2);
    auto& frame3 = wrapper3.frame_;
    test_store_->append_incomplete_frame(stream_id3, std::move(frame3));

    std::set<StreamId> expected{ stream_id1, stream_id2, stream_id3};
    auto result = test_store_->get_incomplete_symbols();
    ASSERT_EQ(result, expected);
}

TEST_F(VersionStoreTest, StressBatchWrite) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    std::vector<StreamId> symbols;
    std::vector<TestTensorFrame> wrappers;
    std::vector<InputTensorFrame> frames;
    std::vector<VersionId> version_ids;
    std::vector<std::shared_ptr<DeDupMap>> dedup_maps;

    for(int i = 0; i < 1000; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        symbols.emplace_back(symbol);
        version_ids.push_back(0);
        dedup_maps.push_back(std::shared_ptr<DeDupMap>{});

        auto wrapper = get_test_simple_frame(symbol, 10000, i);
        wrappers.push_back(wrapper);
        frames.push_back(wrapper.frame_);
    }

    folly::collect(test_store_->batch_write_internal(version_ids, symbols, std::move(frames), dedup_maps, false)).get();
}

TEST_F(VersionStoreTest, StressBatchReadUncompressed) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    std::vector<StreamId> symbols;
    for(int i = 0; i < 10; ++i) {
        auto symbol = fmt::format("symbol_{}", i);
        symbols.emplace_back(symbol);

        for(int j = 0; j < 10; ++j) {
            auto wrapper = get_test_simple_frame(symbol, 10, i + j);
            test_store_->write_versioned_dataframe_internal(symbol, std::move(wrapper.frame_), false, false, false);
        }

        for(int k = 1; k < 10; ++k) {
            test_store_->delete_version(symbol, k);
        }
    }

    std::vector<ReadQuery> read_queries;
    ReadOptions read_options;
    read_options.set_batch_throw_on_error(true);
    auto latest_versions = test_store_->batch_read(symbols, std::vector<VersionQuery>(10), read_queries, read_options);
    for(auto&& [idx, version] : folly::enumerate(latest_versions)) {
        auto expected = get_test_simple_frame(std::get<ReadResult>(version).item.symbol(), 10, idx);
        bool equal = expected.segment_ == std::get<ReadResult>(version).frame_data.frame();
        ASSERT_EQ(equal, true);
    }
}

#define THREE_SIMPLE_KEYS \
    auto key1 = atom_key_builder().version_id(1).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(3).start_index( \
        4).end_index(5).build(id, KeyType::TABLE_INDEX); \
    auto key2 = atom_key_builder().version_id(2).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(4).start_index(  \
        5).end_index(6).build(id, KeyType::TABLE_INDEX); \
    auto key3 = atom_key_builder().version_id(3).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(5).start_index(  \
        6).end_index(7).build(id, KeyType::TABLE_INDEX);


TEST(VersionStore, TestReadTimestampAt) {

  using namespace arcticdb;
  using namespace arcticdb::storage;
  using namespace arcticdb::stream;
  using namespace arcticdb::pipelines;
  PilotedClock::reset();

  StreamId id{"test"};
  THREE_SIMPLE_KEYS

  auto [version_store, mock_store] = python_version_store_in_memory();

  auto version_map = version_store._test_get_version_map();
  version_map->write_version(mock_store, key1);
  auto key = load_index_key_from_time(mock_store, version_map, id, timestamp(0), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 3);

  version_map->write_version(mock_store, key2);
  key = load_index_key_from_time(mock_store, version_map, id, timestamp(0), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 3);
  key = load_index_key_from_time(mock_store, version_map, id, timestamp(1), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 4);

  version_map->write_version(mock_store, key3);
  key = load_index_key_from_time(mock_store, version_map, id, timestamp(0), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 3);
  key = load_index_key_from_time(mock_store, version_map, id, timestamp(1), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 4);
  key = load_index_key_from_time(mock_store, version_map, id, timestamp(2), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(key.value().content_hash(), 5);
}

TEST(VersionStore, TestReadTimestampAtInequality) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

  PilotedClock::reset();
  StreamId id{"test"};

  THREE_SIMPLE_KEYS
  auto [version_store, mock_store] = python_version_store_in_memory();

  auto version_map = version_store._test_get_version_map();
  version_map->write_version(mock_store, key1);
  auto key = load_index_key_from_time(mock_store, version_map, id, timestamp(1), pipelines::VersionQuery{}, ReadOptions{});
  ASSERT_EQ(static_cast<bool>(key), true);
  ASSERT_EQ(key.value().content_hash(), 3);
}

TEST(VersionStore, UpdateWithin) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    ScopedConfig reload_interval("VersionMap.ReloadInterval", 0);

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{0};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{10, 15};
    size_t update_val{1};
    auto update_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, false, false);

    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto& seg = read_result.frame_and_descriptor_.frame_;

    for(auto i = 0u; i < num_rows; ++i) {
        auto expected = i;
        if(update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t >(i, 1);
        ASSERT_EQ(val1.value(), expected);
    }
}

TEST(VersionStore, UpdateBefore) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{10};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{0, 10};
    size_t update_val{1};
    auto update_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, false, false);

    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto& seg = read_result.frame_and_descriptor_.frame_;

    for(auto i = 0u; i < num_rows + update_range.diff(); ++i) {
        auto expected = i;
        if(update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t >(i, 1);
        ASSERT_EQ(val1.value(), expected);
    }
}

TEST(VersionStore, UpdateAfter) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{0};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{100, 110};
    size_t update_val{1};
    auto update_frame =  get_test_frame<stream::TimeseriesIndex>(symbol, fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, false, false);

    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto& seg = read_result.frame_and_descriptor_.frame_;

    for(auto i = 0u; i < num_rows + update_range.diff(); ++i) {
        auto expected = i;
        if(update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t >(i, 1);
        ASSERT_EQ(val1.value(), expected);
    }
}

TEST(VersionStore, UpdateIntersectBefore) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{5};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame = get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{0, 10};
    size_t update_val{1};
    auto update_frame =
        get_test_frame<stream::TimeseriesIndex>(symbol, fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, false, false);

    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto &seg = read_result.frame_and_descriptor_.frame_;

    for (auto i = 0u; i < num_rows + 5; ++i) {
        auto expected = i;
        if (update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t>(i, 1);
        ASSERT_EQ(val1.value(), expected);
    }
}

TEST(VersionStore, UpdateIntersectAfter) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{0};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame = get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{95, 105};
    size_t update_val{1};
    auto update_frame =
        get_test_frame<stream::TimeseriesIndex>(symbol, fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, false, false);

    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{});
    const auto &seg = read_result.frame_and_descriptor_.frame_;

    for (auto i = 0u; i < num_rows + 5; ++i) {
        auto expected = i;
        if (update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t>(i, 1);
        ASSERT_EQ(val1.value(), expected);
    }
}

TEST(VersionStore, UpdateWithinSchemaChange) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.update_schema_change");
    StreamId symbol("update_schema");
    auto version_store = get_test_engine();
    size_t num_rows{100};
    size_t start_val{0};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame = get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.
        write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{10, 15};
    size_t update_val{1};

    std::vector<FieldRef> update_fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing5")
    };

    auto update_frame = get_test_frame<stream::TimeseriesIndex>(symbol, update_fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, true, false);

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, read_options);
    const auto &seg = read_result.frame_and_descriptor_.frame_;

    for (auto i = 0u;i < num_rows; ++i) {
        auto expected = i;
        if (update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t>(i, 1);
        check_value(val1.value(), expected);

        expected = update_range.contains(i) ? 0 : i;
        auto val2 = seg.scalar_at<uint16_t>(i, 4);
        check_value(val2.value(), expected);

        expected = !update_range.contains(i) ? 0 : i + update_val;
        auto val3 = seg.scalar_at<uint16_t>(i, 5);
        check_value(val3.value(), expected);
    }
}

TEST(VersionStore, UpdateWithinTypeAndSchemaChange) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    StreamId symbol("update_schema");
    arcticdb::proto::storage::VersionStoreConfig version_store_cfg;
    version_store_cfg.mutable_write_options()->set_dynamic_schema(true);
    auto version_store = get_test_engine({version_store_cfg});
    size_t num_rows{100};
    size_t start_val{0};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto test_frame = get_test_frame<stream::TimeseriesIndex>(symbol, fields, num_rows, start_val);
    version_store.write_versioned_dataframe_internal(symbol, std::move(test_frame.frame_), false, false, false);

    RowRange update_range{10, 15};
    size_t update_val{1};

    std::vector<FieldRef> update_fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT16, "thing2"),
        scalar_field(DataType::UINT32, "thing3"),
        scalar_field(DataType::UINT32, "thing5")
    };

    auto update_frame = get_test_frame<stream::TimeseriesIndex>(symbol, update_fields, update_range.diff(), update_range.first, update_val);
    version_store.update_internal(symbol, UpdateQuery{}, std::move(update_frame.frame_), false, true, false);

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    ReadQuery read_query;
    auto read_result = version_store.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, read_options);
    const auto &seg = read_result.frame_and_descriptor_.frame_;

    for (auto i = 0u;i < num_rows; ++i) {
        auto expected = i;
        if (update_range.contains(i))
            expected += update_val;

        auto val1 = seg.scalar_at<uint8_t>(i, 1);
        check_value(val1.value(), expected);

        expected = update_range.contains(i) ? 0 : i;
        auto val2 = seg.scalar_at<uint16_t>(i, 4);
        check_value(val2.value(), expected);

        expected = !update_range.contains(i) ? 0 : i + update_val;
        auto val3 = seg.scalar_at<uint32_t>(i, 5);
        check_value(val3.value(), expected);
    }
}

TEST(VersionStore, TestWriteAppendMapHead) {

    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    PilotedClock::reset();
    std::string lib_name("test.write_append_map_head");
    StreamId symbol("append");
    auto version_store = get_test_engine();
    size_t num_rows{100};

    std::vector<FieldRef> fields{
        scalar_field(DataType::UINT8, "thing1"),
        scalar_field(DataType::UINT8, "thing2"),
        scalar_field(DataType::UINT16, "thing3"),
        scalar_field(DataType::UINT16, "thing4")
    };

    auto key = atom_key_builder().version_id(0).creation_ts(PilotedClock::nanos_since_epoch()).content_hash(0).build(symbol, KeyType::APPEND_DATA);

    auto descriptor = StreamDescriptor{symbol, IndexDescriptor{1u, IndexDescriptor::TIMESTAMP}, std::make_shared<FieldCollection>(fields_from_range(fields))};
    write_head(version_store._test_get_store(), key, num_rows);
    auto [next_key, total_rows] = read_head(version_store._test_get_store(), symbol);
    ASSERT_EQ(next_key, key);
    ASSERT_EQ(total_rows, num_rows);
}
