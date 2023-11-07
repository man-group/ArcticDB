/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/pipeline/pipeline_common.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/pipeline/index_writer.hpp>

namespace arcticdb {
using namespace arcticdb::pipelines;

std::pair<TimeseriesDescriptor , std::vector<SliceAndKey>> get_sample_slice_and_key(StreamId stream_id, VersionId version_id, size_t col_slices = 1, size_t row_slices = 10) {
    StreamDescriptor stream_desc{
        stream_id,
        IndexDescriptor{1, IndexDescriptor::TIMESTAMP}
    };

    stream_desc.add_field(scalar_field(DataType::NANOSECONDS_UTC64, "time"));

    const auto step = 10;
    auto start_col = 1;
    auto end_col = start_col + step;

    for (auto i = 0u; i < step * col_slices; ++i) {
        stream_desc.add_field(scalar_field(DataType::UINT8, fmt::format("col_{}", i)));
    }

    TimeseriesDescriptor metadata;
    metadata.set_stream_descriptor(stream_desc);
    std::vector<SliceAndKey> slice_and_keys;

    for(auto col_range = 0u; col_range < col_slices; ++col_range) {

        auto start_val = 0;
        auto end_val = start_val + step;
        for (auto i = 0u; i < row_slices; ++i) {
            slice_and_keys.push_back(
                SliceAndKey{
                    FrameSlice{
                        ColRange{start_col, end_col},
                        RowRange{start_val, end_val}
                    },
                    AtomKey{
                        stream_id,
                        version_id,
                        i,
                        col_range,
                        IndexValue{start_val},
                        IndexValue{end_val},
                        KeyType::TABLE_DATA
                    }
                }
            );
            start_val = end_val;
            end_val += step;
        }

        start_col = end_col;
        end_col += step;
    }
    return std::make_pair(metadata, slice_and_keys);
}
}

TEST(IndexFilter, Static) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    const auto stream_id = StreamId{"thing"};
    const auto version_id = VersionId{0};

    auto [metadata, slice_and_keys] = get_sample_slice_and_key(stream_id, version_id);

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    index::IndexWriter<stream::RowCountIndex> writer(mock_store, partial_key, std::move(metadata));

    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice());
    }
    auto key_fut = writer.commit();
    auto key = std::move(key_fut).get();
    auto seg = mock_store->read(key, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor{isr.tsd().as_stream_descriptor()});

    ReadQuery read_query{};
    read_query.row_filter = IndexRange{25, 65};

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        false,
        false);

    pipeline_context->slice_and_keys_ = filter_index(isr, combine_filter_functions(queries));
    ASSERT_EQ(pipeline_context->slice_and_keys_[0].key_, slice_and_keys[2].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[4].key_, slice_and_keys[6].key_);
}

TEST(IndexFilter, Dynamic) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    const auto stream_id = StreamId{"thing"};
    const auto version_id = VersionId{0};

    auto [metadata, slice_and_keys] = get_sample_slice_and_key(stream_id, version_id);

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    index::IndexWriter<stream::RowCountIndex> writer(mock_store, partial_key, std::move(metadata));

    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice());
    }
    auto key_fut = writer.commit();
    auto key = std::move(key_fut).get();
    auto seg = mock_store->read(key, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor(isr.tsd().as_stream_descriptor()));

    ReadQuery read_query{};
    read_query.row_filter = IndexRange{25, 65};

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        true,
        false);

    pipeline_context->slice_and_keys_ = filter_index(isr, combine_filter_functions(queries));
    ASSERT_EQ(pipeline_context->slice_and_keys_[0].key(), slice_and_keys[2].key());
    ASSERT_EQ(pipeline_context->slice_and_keys_[4].key(), slice_and_keys[6].key());
}

TEST(IndexFilter, StaticMulticolumn) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    const auto stream_id = StreamId{"thing"};
    const auto version_id = VersionId{0};

    auto [metadata, slice_and_keys] = get_sample_slice_and_key(stream_id, version_id, 10);

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    index::IndexWriter<stream::RowCountIndex> writer(mock_store, partial_key, std::move(metadata));

    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice());
    }
    auto key_fut = writer.commit();
    auto key = std::move(key_fut).get();
    auto seg = mock_store->read(key, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor(isr.tsd().as_stream_descriptor()));

    ReadQuery read_query{};
    read_query.row_filter = IndexRange{25, 65};

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        false,
        false);

    pipeline_context->slice_and_keys_ = filter_index(isr, combine_filter_functions(queries));
    ASSERT_EQ(pipeline_context->slice_and_keys_[0].key_, slice_and_keys[2].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[4].key_, slice_and_keys[6].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[5].key_, slice_and_keys[12].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[9].key_, slice_and_keys[16].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[45].key_, slice_and_keys[92].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[49].key_, slice_and_keys[96].key_);
}

TEST(IndexFilter, MultiColumnSelectAll) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    const auto stream_id = StreamId{"thing"};
    const auto version_id = VersionId{0};

    auto [metadata, slice_and_keys] = get_sample_slice_and_key(stream_id, version_id, 10);

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    index::IndexWriter<stream::RowCountIndex> writer(mock_store, partial_key, std::move(metadata));

    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice());
    }
    auto key_fut = writer.commit();
    auto key = std::move(key_fut).get();
    auto seg = mock_store->read(key, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor{isr.tsd().as_stream_descriptor()});

    ReadQuery read_query{};
    read_query.row_filter = IndexRange{0, 100};

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        false,
        false);

    pipeline_context->slice_and_keys_ = filter_index(isr, combine_filter_functions(queries));
    ASSERT_EQ(pipeline_context->slice_and_keys_, slice_and_keys);
}

TEST(IndexFilter, StaticMulticolumnFilterColumns) {
    using namespace arcticdb;
    using namespace arcticdb::pipelines;

    const auto stream_id = StreamId{"thing"};
    const auto version_id = VersionId{0};

    auto [metadata, slice_and_keys] = get_sample_slice_and_key(stream_id, version_id, 10);

    const IndexPartialKey& partial_key{stream_id, version_id};
    auto mock_store = std::make_shared<InMemoryStore>();
    index::IndexWriter<stream::RowCountIndex> writer(mock_store, partial_key, std::move(metadata));

    for (auto &slice_and_key : slice_and_keys) {
        writer.add(slice_and_key.key(), slice_and_key.slice());
    }
    auto key_fut = writer.commit();
    auto key = std::move(key_fut).get();
    auto seg = mock_store->read(key, storage::ReadKeyOpts{}).get();
    pipelines::index::IndexSegmentReader isr{std::move(seg.second)};
    auto pipeline_context = std::make_shared<PipelineContext>(StreamDescriptor{isr.tsd().as_stream_descriptor()});

    ReadQuery read_query{};
    read_query.row_filter = IndexRange{25, 65};
    read_query.columns = std::vector<std::string> {"col_10", "col_91"};

    auto queries = get_column_bitset_and_query_functions<index::IndexSegmentReader>(
        read_query,
        pipeline_context,
        false,
        false);

    pipeline_context->slice_and_keys_ = filter_index(isr, combine_filter_functions(queries));
    ASSERT_EQ(pipeline_context->slice_and_keys_[0].key_, slice_and_keys[12].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[1].key_, slice_and_keys[13].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[4].key_, slice_and_keys[16].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[5].key_, slice_and_keys[92].key_);
    ASSERT_EQ(pipeline_context->slice_and_keys_[9].key_, slice_and_keys[96].key_);
}