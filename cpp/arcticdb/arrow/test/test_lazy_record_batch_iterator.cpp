/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/arrow/lazy_record_batch_iterator.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb {

namespace {

// Write a segment to the store and return a SliceAndKey referencing it.
pipelines::SliceAndKey write_segment_to_store(
        const std::shared_ptr<InMemoryStore>& store, const StreamId& stream_id, SegmentInMemory&& segment,
        size_t row_start, size_t row_end, size_t col_start, size_t col_end
) {
    auto key = store->write(KeyType::TABLE_DATA,
                            0,
                            stream_id,
                            static_cast<timestamp>(row_start),
                            static_cast<timestamp>(row_end),
                            std::move(segment))
                       .get();

    pipelines::FrameSlice slice{pipelines::ColRange{col_start, col_end}, pipelines::RowRange{row_start, row_end}};
    return pipelines::SliceAndKey{std::move(slice), to_atom(key)};
}

// Create a segment with an int64 index and a float64 data column.
SegmentInMemory make_numeric_segment(size_t num_rows, timestamp start_ts = 0) {
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);
    SegmentInMemory seg(std::move(desc), num_rows);

    auto& idx_col = seg.column(0);
    auto& data_col = seg.column(1);
    for (size_t i = 0; i < num_rows; ++i) {
        auto ts = static_cast<timestamp>(start_ts + static_cast<timestamp>(i));
        idx_col.set_scalar(static_cast<ssize_t>(i), ts);
        data_col.set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }
    seg.set_row_data(num_rows - 1);
    return seg;
}

} // anonymous namespace

class LazyRecordBatchIteratorTest : public ::testing::Test {};

TEST_F(LazyRecordBatchIteratorTest, EmptySliceAndKeys) {
    auto store = std::make_shared<InMemoryStore>();
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);

    LazyRecordBatchIterator iter({}, std::move(desc), store, nullptr, FilterRange{}, nullptr, "");

    EXPECT_FALSE(iter.has_next());
    EXPECT_EQ(iter.num_batches(), 0u);

    auto batch = iter.next();
    EXPECT_FALSE(batch.has_value());
}

TEST_F(LazyRecordBatchIteratorTest, SingleSegmentNumericRoundTrip) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t num_rows = 50;
    auto segment = make_numeric_segment(num_rows, 0);
    auto desc = segment.descriptor().clone();

    auto sk = write_segment_to_store(store, stream_id, std::move(segment), 0, num_rows, 0, 2);

    LazyRecordBatchIterator iter({std::move(sk)}, std::move(desc), store, nullptr, FilterRange{}, nullptr, "");

    EXPECT_TRUE(iter.has_next());
    EXPECT_EQ(iter.num_batches(), 1u);

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());

    // Verify we got valid Arrow structures
    EXPECT_NE(batch->array_.release, nullptr);
    EXPECT_NE(batch->schema_.release, nullptr);
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));
    // 2 children: index column + data column
    EXPECT_EQ(batch->array_.n_children, 2);

    // No more batches
    EXPECT_FALSE(iter.has_next());
    auto batch2 = iter.next();
    EXPECT_FALSE(batch2.has_value());
}

TEST_F(LazyRecordBatchIteratorTest, MultipleSegmentsInSequence) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t rows_per_seg = 20;
    constexpr size_t num_segments = 5;

    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    for (size_t seg_idx = 0; seg_idx < num_segments; ++seg_idx) {
        auto start = seg_idx * rows_per_seg;
        auto end = start + rows_per_seg;
        auto segment = make_numeric_segment(rows_per_seg, static_cast<timestamp>(start));
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(segment), start, end, 0, 2));
    }

    LazyRecordBatchIterator iter(
            std::move(slice_and_keys), desc.clone(), store, nullptr, FilterRange{}, nullptr, "", 2
    );

    EXPECT_EQ(iter.num_batches(), num_segments);

    size_t batch_count = 0;
    while (auto batch = iter.next()) {
        EXPECT_NE(batch->array_.release, nullptr);
        EXPECT_EQ(batch->array_.length, static_cast<int64_t>(rows_per_seg));
        ++batch_count;
    }
    EXPECT_EQ(batch_count, num_segments);
}

TEST_F(LazyRecordBatchIteratorTest, DateRangeTruncation) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    // One segment with timestamps [0, 100)
    constexpr size_t num_rows = 100;
    auto segment = make_numeric_segment(num_rows, 0);
    auto desc = segment.descriptor().clone();
    auto sk = write_segment_to_store(store, stream_id, std::move(segment), 0, num_rows, 0, 2);

    // Truncate to [25, 75] (ArcticDB date ranges are inclusive on both ends)
    TimestampRange date_range{25, 75};
    FilterRange filter = entity::IndexRange(date_range);

    LazyRecordBatchIterator iter({std::move(sk)}, std::move(desc), store, nullptr, std::move(filter), nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    // Inclusive range: rows 25,26,...,75 = 51 rows
    EXPECT_EQ(batch->array_.length, 51);
}

TEST_F(LazyRecordBatchIteratorTest, RowRangeTruncation) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t num_rows = 100;
    auto segment = make_numeric_segment(num_rows, 0);
    auto desc = segment.descriptor().clone();
    auto sk = write_segment_to_store(store, stream_id, std::move(segment), 0, num_rows, 0, 2);

    // Only want rows [10, 30) out of segment covering [0, 100)
    FilterRange filter = pipelines::RowRange{10, 30};

    LazyRecordBatchIterator iter({std::move(sk)}, std::move(desc), store, nullptr, std::move(filter), nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.length, 20);
}

TEST_F(LazyRecordBatchIteratorTest, PrefetchBufferSizeRespected) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t rows_per_seg = 10;
    constexpr size_t num_segments = 10;

    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    for (size_t i = 0; i < num_segments; ++i) {
        auto start = i * rows_per_seg;
        auto end = start + rows_per_seg;
        auto segment = make_numeric_segment(rows_per_seg, static_cast<timestamp>(start));
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(segment), start, end, 0, 2));
    }

    // Prefetch size = 1 (minimum), should still work correctly
    LazyRecordBatchIterator iter(
            std::move(slice_and_keys), desc.clone(), store, nullptr, FilterRange{}, nullptr, "", 1
    );

    size_t count = 0;
    while (iter.next()) {
        ++count;
    }
    EXPECT_EQ(count, num_segments);
}

TEST_F(LazyRecordBatchIteratorTest, ColumnProjection) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    // Create segment with multiple data columns
    auto fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
            scalar_field(DataType::INT32, "col_c"),
    };
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    constexpr size_t num_rows = 30;
    SegmentInMemory seg(desc.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        auto ts = static_cast<timestamp>(i);
        seg.column(0).set_scalar(static_cast<ssize_t>(i), ts);
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i));
        seg.column(2).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.1);
        seg.column(3).set_scalar(static_cast<ssize_t>(i), static_cast<int32_t>(i));
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 4);

    // Only request col_b
    auto columns = std::make_shared<std::unordered_set<std::string>>();
    columns->insert("col_b");

    LazyRecordBatchIterator iter({std::move(sk)}, desc.clone(), store, columns, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));
}

TEST_F(LazyRecordBatchIteratorTest, DescriptorAccessible) {
    auto store = std::make_shared<InMemoryStore>();
    auto fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto desc = get_test_descriptor<stream::TimeseriesIndex>("test", fields);
    auto desc_copy = desc.clone();

    LazyRecordBatchIterator iter({}, std::move(desc), store, nullptr, FilterRange{}, nullptr, "");

    // descriptor() should be accessible even when there are no segments
    EXPECT_EQ(iter.descriptor().field_count(), desc_copy.field_count());
}

TEST_F(LazyRecordBatchIteratorTest, SliceAndKeyAccessors) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t rows_per_seg = 10;
    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    for (size_t i = 0; i < 3; ++i) {
        auto start = i * rows_per_seg;
        auto end = start + rows_per_seg;
        auto segment = make_numeric_segment(rows_per_seg, static_cast<timestamp>(start));
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(segment), start, end, 0, 2));
    }

    LazyRecordBatchIterator iter(std::move(slice_and_keys), desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    // Before consuming any batches, current_index is 0
    EXPECT_EQ(iter.current_index(), 0u);

    // peek_slice_and_key(0) should return the first segment
    auto* first = iter.peek_slice_and_key(0);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->slice_.row_range.first, 0u);

    // peek_slice_and_key(1) should return the second segment
    auto* second = iter.peek_slice_and_key(1);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(second->slice_.row_range.first, 10u);

    // peek_slice_and_key(3) should return nullptr (out of range)
    auto* oob = iter.peek_slice_and_key(3);
    EXPECT_EQ(oob, nullptr);

    // Consume first batch, current_index advances
    iter.next();
    EXPECT_EQ(iter.current_index(), 1u);
}

TEST_F(LazyRecordBatchIteratorTest, DualCapBackpressure) {
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};

    constexpr size_t rows_per_seg = 10;
    constexpr size_t num_segments = 10;

    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    for (size_t i = 0; i < num_segments; ++i) {
        auto start = i * rows_per_seg;
        auto end = start + rows_per_seg;
        auto segment = make_numeric_segment(rows_per_seg, static_cast<timestamp>(start));
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(segment), start, end, 0, 2));
    }

    // High count cap (100) but very low byte cap (1 byte) — should limit prefetch
    // Each segment estimate: 10 rows × 2 cols × 8 = 160 bytes
    // With 1-byte cap, only 1 segment should be prefetched at a time (first one always goes through)
    LazyRecordBatchIterator iter(
            std::move(slice_and_keys),
            desc.clone(),
            store,
            nullptr,
            FilterRange{},
            nullptr,
            "",
            100, // prefetch_size
            1    // max_prefetch_bytes — tiny, forces byte-cap to kick in
    );

    // Should still read all segments correctly despite aggressive byte cap
    size_t count = 0;
    while (iter.next()) {
        ++count;
    }
    EXPECT_EQ(count, num_segments);
}

TEST_F(LazyRecordBatchIteratorTest, HorizontalMergeArrowBatches) {
    // Create two segments with overlapping index columns but different data columns.
    // Segment A: index + col_a (2 children)
    // Segment B: index + col_b (2 children)
    // After merge: index + col_a + col_b (3 children, index deduplicated)
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 20;

    // Segment A: index + col_a
    {
        auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
        auto desc_a = get_test_descriptor<stream::TimeseriesIndex>("test", fields_a);
        SegmentInMemory seg_a(desc_a.clone(), num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            seg_a.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
            seg_a.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 10));
        }
        seg_a.set_row_data(num_rows - 1);

        auto sk_a = write_segment_to_store(store, stream_id, std::move(seg_a), 0, num_rows, 0, 2);
        LazyRecordBatchIterator iter_a({std::move(sk_a)}, desc_a.clone(), store, nullptr, FilterRange{}, nullptr, "");
        auto batch_a = iter_a.next();
        ASSERT_TRUE(batch_a.has_value());
        EXPECT_EQ(batch_a->array_.n_children, 2);

        // Segment B: index + col_b
        auto fields_b = std::array{scalar_field(DataType::FLOAT64, "col_b")};
        auto desc_b = get_test_descriptor<stream::TimeseriesIndex>("test", fields_b);
        SegmentInMemory seg_b(desc_b.clone(), num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            seg_b.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
            seg_b.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
        }
        seg_b.set_row_data(num_rows - 1);

        auto sk_b = write_segment_to_store(store, stream_id, std::move(seg_b), 0, num_rows, 0, 2);
        LazyRecordBatchIterator iter_b({std::move(sk_b)}, desc_b.clone(), store, nullptr, FilterRange{}, nullptr, "");
        auto batch_b = iter_b.next();
        ASSERT_TRUE(batch_b.has_value());
        EXPECT_EQ(batch_b->array_.n_children, 2);

        // Merge horizontally
        auto merged = horizontal_merge_arrow_batches(std::move(*batch_a), std::move(*batch_b));

        // Verify merged result
        EXPECT_NE(merged.array_.release, nullptr);
        EXPECT_NE(merged.schema_.release, nullptr);
        EXPECT_EQ(merged.array_.length, static_cast<int64_t>(num_rows));
        // 3 children: index (from A) + col_a + col_b (index from B deduplicated)
        EXPECT_EQ(merged.array_.n_children, 3);
        EXPECT_EQ(merged.schema_.n_children, 3);
    }
}

TEST_F(LazyRecordBatchIteratorTest, ColumnSliceMergingInIterator) {
    // Simulate a wide table split into 2 column slices per row group.
    // Row group 0: slice A (index + col_a, cols 0-2), slice B (index + col_b, cols 2-4)
    // Row group 1: slice A (index + col_a, cols 0-2), slice B (index + col_b, cols 2-4)
    // The iterator should merge slices within each row group and yield 2 merged batches.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t rows_per_group = 25;
    constexpr size_t num_groups = 2;

    // We need a descriptor that covers all columns for the iterator
    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;
    for (size_t group = 0; group < num_groups; ++group) {
        auto row_start = group * rows_per_group;
        auto row_end = row_start + rows_per_group;

        // Slice A: index + col_a (columns 0-2)
        auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
        auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
        SegmentInMemory seg_a(desc_a.clone(), rows_per_group);
        for (size_t i = 0; i < rows_per_group; ++i) {
            auto ts = static_cast<timestamp>(row_start + i);
            seg_a.column(0).set_scalar(static_cast<ssize_t>(i), ts);
            seg_a.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 10));
        }
        seg_a.set_row_data(rows_per_group - 1);
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(seg_a), row_start, row_end, 0, 2));

        // Slice B: index + col_b (columns 2-4)
        auto fields_b = std::array{scalar_field(DataType::FLOAT64, "col_b")};
        auto desc_b = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_b);
        SegmentInMemory seg_b(desc_b.clone(), rows_per_group);
        for (size_t i = 0; i < rows_per_group; ++i) {
            auto ts = static_cast<timestamp>(row_start + i);
            seg_b.column(0).set_scalar(static_cast<ssize_t>(i), ts);
            seg_b.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
        }
        seg_b.set_row_data(rows_per_group - 1);
        slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(seg_b), row_start, row_end, 2, 4));
    }

    // Slices should already be in (row_range, col_range) order from how we built them
    LazyRecordBatchIterator iter(
            std::move(slice_and_keys), full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "", 4
    );

    // 4 segments total, but grouped into 2 row groups
    EXPECT_EQ(iter.num_batches(), 4u);

    // First merged batch: row group 0
    auto batch1 = iter.next();
    ASSERT_TRUE(batch1.has_value());
    EXPECT_EQ(batch1->array_.length, static_cast<int64_t>(rows_per_group));
    // 3 children: index + col_a + col_b (index from slice B deduplicated)
    EXPECT_EQ(batch1->array_.n_children, 3);

    // Second merged batch: row group 1
    auto batch2 = iter.next();
    ASSERT_TRUE(batch2.has_value());
    EXPECT_EQ(batch2->array_.length, static_cast<int64_t>(rows_per_group));
    EXPECT_EQ(batch2->array_.n_children, 3);

    // No more batches
    EXPECT_FALSE(iter.next().has_value());
}

TEST_F(LazyRecordBatchIteratorTest, ThreeColumnSlicesMerging) {
    // Three column slices per row group, single row group
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 15;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
            scalar_field(DataType::INT32, "col_c"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    std::vector<pipelines::SliceAndKey> slice_and_keys;

    // Slice A: index + col_a
    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg_a(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg_a.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg_a.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i));
    }
    seg_a.set_row_data(num_rows - 1);
    slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(seg_a), 0, num_rows, 0, 2));

    // Slice B: index + col_b
    auto fields_b = std::array{scalar_field(DataType::FLOAT64, "col_b")};
    auto desc_b = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_b);
    SegmentInMemory seg_b(desc_b.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg_b.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg_b.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.1);
    }
    seg_b.set_row_data(num_rows - 1);
    slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(seg_b), 0, num_rows, 2, 4));

    // Slice C: index + col_c
    auto fields_c = std::array{scalar_field(DataType::INT32, "col_c")};
    auto desc_c = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_c);
    SegmentInMemory seg_c(desc_c.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg_c.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg_c.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int32_t>(i * 100));
    }
    seg_c.set_row_data(num_rows - 1);
    slice_and_keys.push_back(write_segment_to_store(store, stream_id, std::move(seg_c), 0, num_rows, 4, 6));

    // Prefetch size=2 means not all slices are prefetched at once — tests the
    // refill-during-merge path in next()
    LazyRecordBatchIterator iter(
            std::move(slice_and_keys), full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "", 2
    );

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));
    // 4 children: index + col_a + col_b + col_c (index deduplicated twice)
    EXPECT_EQ(batch->array_.n_children, 4);

    // No more batches
    EXPECT_FALSE(iter.next().has_value());
}

TEST_F(LazyRecordBatchIteratorTest, DefaultArrowFormatForType) {
    EXPECT_EQ(default_arrow_format_for_type(DataType::INT64), "l");
    EXPECT_EQ(default_arrow_format_for_type(DataType::FLOAT64), "g");
    EXPECT_EQ(default_arrow_format_for_type(DataType::BOOL8), "b");
    EXPECT_EQ(default_arrow_format_for_type(DataType::NANOSECONDS_UTC64), "tsn:");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UTF_DYNAMIC64), "U");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UTF_DYNAMIC32), "u");
    EXPECT_EQ(default_arrow_format_for_type(DataType::INT32), "i");
    EXPECT_EQ(default_arrow_format_for_type(DataType::FLOAT32), "f");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UINT64), "L");
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchFastPath) {
    // When a batch already matches the target schema exactly, pad_batch_to_schema
    // should return it unchanged (fast path, zero overhead).
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 10;

    auto fields = std::array{scalar_field(DataType::FLOAT64, "value")};
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    SegmentInMemory seg(desc.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 2);
    LazyRecordBatchIterator iter({std::move(sk)}, desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    // The batch should have 2 children matching the 2 target fields
    EXPECT_EQ(batch->array_.n_children, 2);
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));

    // Verify the names match the descriptor
    EXPECT_STREQ(batch->schema_.children[0]->name, "time");
    EXPECT_STREQ(batch->schema_.children[1]->name, "value");
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchMissingColumns) {
    // Dynamic schema: segment has only {index, col_a} but descriptor has {index, col_a, col_b}.
    // The iterator should pad col_b with nulls.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 15;

    // Full descriptor has 2 data columns
    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    // Segment only has col_a
    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 10));
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 2);

    // Use full_desc for the iterator (which has col_a AND col_b)
    LazyRecordBatchIterator iter({std::move(sk)}, full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    // 3 children: time + col_a + col_b (col_b padded with nulls)
    EXPECT_EQ(batch->array_.n_children, 3);
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));

    // Verify column names in target order
    EXPECT_STREQ(batch->schema_.children[0]->name, "time");
    EXPECT_STREQ(batch->schema_.children[1]->name, "col_a");
    EXPECT_STREQ(batch->schema_.children[2]->name, "col_b");

    // The padded column (col_b) should be all nulls
    auto& padded_arr = *batch->array_.children[2];
    EXPECT_EQ(padded_arr.length, static_cast<int64_t>(num_rows));
    EXPECT_EQ(padded_arr.null_count, static_cast<int64_t>(num_rows));
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchColumnReordering) {
    // Test that padding reorders columns to match target schema order.
    // Segment has {index, col_b, col_a} but descriptor says {index, col_a, col_b}.
    // After padding, columns should be in {index, col_a, col_b} order.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 10;

    // Full descriptor: col_a before col_b
    auto full_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, full_fields);

    // Two column slices: first has col_b, second has col_a
    // After merging, the batch will have {index, col_b, col_a} (wrong order)
    // Schema padding should reorder to {index, col_a, col_b}

    // Slice 1: index + col_b (cols 2-4)
    auto fields_b = std::array{scalar_field(DataType::FLOAT64, "col_b")};
    auto desc_b = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_b);
    SegmentInMemory seg_b(desc_b.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg_b.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg_b.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }
    seg_b.set_row_data(num_rows - 1);

    // Slice 2: index + col_a (cols 0-2)
    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg_a(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg_a.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg_a.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 100));
    }
    seg_a.set_row_data(num_rows - 1);

    std::vector<pipelines::SliceAndKey> sks;
    // Intentionally put col_b slice first so horizontal merge produces {index, col_b, col_a}
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg_b), 0, num_rows, 0, 2));
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg_a), 0, num_rows, 2, 4));

    LazyRecordBatchIterator iter(std::move(sks), full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "", 4);

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.n_children, 3);

    // Verify columns are reordered to match descriptor: time, col_a, col_b
    EXPECT_STREQ(batch->schema_.children[0]->name, "time");
    EXPECT_STREQ(batch->schema_.children[1]->name, "col_a");
    EXPECT_STREQ(batch->schema_.children[2]->name, "col_b");
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchDynamicSchemaTwoSegments) {
    // Dynamic schema: two segments with different columns.
    // Segment 1: {index, col_a}
    // Segment 2: {index, col_b}
    // Descriptor: {index, col_a, col_b}
    // Each batch should be padded to have all 3 children.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 10;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    std::vector<pipelines::SliceAndKey> sks;

    // Segment 1: index + col_a
    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg1(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg1.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg1.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 10));
    }
    seg1.set_row_data(num_rows - 1);
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg1), 0, num_rows, 0, 2));

    // Segment 2: index + col_b
    auto fields_b = std::array{scalar_field(DataType::FLOAT64, "col_b")};
    auto desc_b = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_b);
    SegmentInMemory seg2(desc_b.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg2.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i + num_rows));
        seg2.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }
    seg2.set_row_data(num_rows - 1);
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg2), num_rows, num_rows * 2, 0, 2));

    LazyRecordBatchIterator iter(std::move(sks), full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    // Batch 1: has col_a, col_b padded with nulls
    auto batch1 = iter.next();
    ASSERT_TRUE(batch1.has_value());
    EXPECT_EQ(batch1->array_.n_children, 3);
    EXPECT_STREQ(batch1->schema_.children[0]->name, "time");
    EXPECT_STREQ(batch1->schema_.children[1]->name, "col_a");
    EXPECT_STREQ(batch1->schema_.children[2]->name, "col_b");
    // col_b should be all-null in batch 1
    EXPECT_EQ(batch1->array_.children[2]->null_count, static_cast<int64_t>(num_rows));

    // Batch 2: has col_b, col_a padded with nulls
    auto batch2 = iter.next();
    ASSERT_TRUE(batch2.has_value());
    EXPECT_EQ(batch2->array_.n_children, 3);
    EXPECT_STREQ(batch2->schema_.children[0]->name, "time");
    EXPECT_STREQ(batch2->schema_.children[1]->name, "col_a");
    EXPECT_STREQ(batch2->schema_.children[2]->name, "col_b");
    // col_a should be all-null in batch 2
    EXPECT_EQ(batch2->array_.children[1]->null_count, static_cast<int64_t>(num_rows));

    EXPECT_FALSE(iter.next().has_value());
}

// =============================================================================
// Coverage gap tests for arrow_utils.cpp
// =============================================================================

TEST_F(LazyRecordBatchIteratorTest, DefaultArrowFormatForAllNumericTypes) {
    // Cover all numeric types in default_arrow_format_for_type that weren't
    // explicitly tested: INT8, INT16, UINT8, UINT16, UINT32.
    EXPECT_EQ(default_arrow_format_for_type(DataType::INT8), "c");
    EXPECT_EQ(default_arrow_format_for_type(DataType::INT16), "s");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UINT8), "C");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UINT16), "S");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UINT32), "I");
    // String types
    EXPECT_EQ(default_arrow_format_for_type(DataType::ASCII_DYNAMIC64), "U");
    EXPECT_EQ(default_arrow_format_for_type(DataType::ASCII_FIXED64), "U");
    EXPECT_EQ(default_arrow_format_for_type(DataType::UTF_FIXED64), "U");
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchAllColumnsMissing) {
    // Target schema has {index, col_a, col_b} but batch only has {index}.
    // Both data columns should be padded with nulls.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 8;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    // Segment with only the index column (no data columns)
    auto empty_fields = std::array<FieldRef, 0>{};
    auto desc_idx_only = get_test_descriptor<stream::TimeseriesIndex>(stream_id, empty_fields);
    SegmentInMemory seg(desc_idx_only.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 1);
    LazyRecordBatchIterator iter({std::move(sk)}, full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    // 3 children: index + col_a (null) + col_b (null)
    EXPECT_EQ(batch->array_.n_children, 3);
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));
    // Both padded columns should be all-null
    EXPECT_EQ(batch->array_.children[1]->null_count, static_cast<int64_t>(num_rows));
    EXPECT_EQ(batch->array_.children[2]->null_count, static_cast<int64_t>(num_rows));
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchTimestampNullColumn) {
    // Target schema has a timestamp column that's missing from the segment.
    // The null column should have timestamp format.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 5;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::NANOSECONDS_UTC64, "ts_col"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    // Segment only has col_a
    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i));
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 2);
    LazyRecordBatchIterator iter({std::move(sk)}, full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.n_children, 3);
    // ts_col should be padded with nulls
    EXPECT_EQ(batch->array_.children[2]->null_count, static_cast<int64_t>(num_rows));
    // Verify the format starts with "ts" (timestamp)
    std::string format(batch->schema_.children[2]->format);
    EXPECT_TRUE(format.find("ts") == 0) << "Expected timestamp format, got: " << format;
}

TEST_F(LazyRecordBatchIteratorTest, PadBatchBoolNullColumn) {
    // Target schema has a bool column that's missing from the segment.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 10;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::BOOL8, "flag"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    auto fields_a = std::array{scalar_field(DataType::INT64, "col_a")};
    auto desc_a = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_a);
    SegmentInMemory seg(desc_a.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i));
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 2);
    LazyRecordBatchIterator iter({std::move(sk)}, full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.n_children, 3);
    // Bool column padded with nulls
    EXPECT_EQ(batch->array_.children[2]->null_count, static_cast<int64_t>(num_rows));
    EXPECT_STREQ(batch->schema_.children[2]->format, "b");
}

// =============================================================================
// Coverage gap tests for arrow_output_frame.cpp
// =============================================================================

TEST_F(LazyRecordBatchIteratorTest, EmptyStringPoolSegment) {
    // Write a segment with only numeric columns (no strings).
    // The prepare_segment_for_arrow path should handle empty string pool.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 20;

    auto fields = std::array{
            scalar_field(DataType::INT64, "int_col"),
            scalar_field(DataType::FLOAT64, "float_col"),
    };
    auto desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields);

    SegmentInMemory seg(desc.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i * 100));
        seg.column(2).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.5);
    }
    seg.set_row_data(num_rows - 1);

    auto sk = write_segment_to_store(store, stream_id, std::move(seg), 0, num_rows, 0, 3);
    LazyRecordBatchIterator iter({std::move(sk)}, desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    auto batch = iter.next();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->array_.n_children, 3);
    EXPECT_EQ(batch->array_.length, static_cast<int64_t>(num_rows));
}

TEST_F(LazyRecordBatchIteratorTest, MultipleRowGroupsWithPadding) {
    // Two row groups where each has different columns, exercising schema padding
    // across multiple batches with the same target schema.
    auto store = std::make_shared<InMemoryStore>();
    StreamId stream_id{"test_symbol"};
    constexpr size_t num_rows = 10;

    auto all_fields = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
            scalar_field(DataType::INT32, "col_c"),
    };
    auto full_desc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, all_fields);

    std::vector<pipelines::SliceAndKey> sks;

    // Row group 0: has col_a and col_b (no col_c)
    auto fields_ab = std::array{
            scalar_field(DataType::INT64, "col_a"),
            scalar_field(DataType::FLOAT64, "col_b"),
    };
    auto desc_ab = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_ab);
    SegmentInMemory seg1(desc_ab.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg1.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(i));
        seg1.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<int64_t>(i));
        seg1.column(2).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.1);
    }
    seg1.set_row_data(num_rows - 1);
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg1), 0, num_rows, 0, 3));

    // Row group 1: has col_b and col_c (no col_a)
    auto fields_bc = std::array{
            scalar_field(DataType::FLOAT64, "col_b"),
            scalar_field(DataType::INT32, "col_c"),
    };
    auto desc_bc = get_test_descriptor<stream::TimeseriesIndex>(stream_id, fields_bc);
    SegmentInMemory seg2(desc_bc.clone(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        seg2.column(0).set_scalar(static_cast<ssize_t>(i), static_cast<timestamp>(num_rows + i));
        seg2.column(1).set_scalar(static_cast<ssize_t>(i), static_cast<double>(i) + 0.2);
        seg2.column(2).set_scalar(static_cast<ssize_t>(i), static_cast<int32_t>(i * 100));
    }
    seg2.set_row_data(num_rows - 1);
    sks.push_back(write_segment_to_store(store, stream_id, std::move(seg2), num_rows, num_rows * 2, 0, 3));

    LazyRecordBatchIterator iter(std::move(sks), full_desc.clone(), store, nullptr, FilterRange{}, nullptr, "");

    // Batch 1: col_c padded with nulls
    auto batch1 = iter.next();
    ASSERT_TRUE(batch1.has_value());
    EXPECT_EQ(batch1->array_.n_children, 4); // time + col_a + col_b + col_c
    EXPECT_STREQ(batch1->schema_.children[3]->name, "col_c");
    EXPECT_EQ(batch1->array_.children[3]->null_count, static_cast<int64_t>(num_rows));

    // Batch 2: col_a padded with nulls
    auto batch2 = iter.next();
    ASSERT_TRUE(batch2.has_value());
    EXPECT_EQ(batch2->array_.n_children, 4);
    EXPECT_STREQ(batch2->schema_.children[1]->name, "col_a");
    EXPECT_EQ(batch2->array_.children[1]->null_count, static_cast<int64_t>(num_rows));
}

} // namespace arcticdb
