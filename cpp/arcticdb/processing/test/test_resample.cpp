/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/test/test_utils.hpp>

using namespace arcticdb;

auto generate_bucket_boundaries(std::vector<timestamp>&& bucket_boundaries) {
    return [bucket_boundaries = std::move(bucket_boundaries)](
                   timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, ResampleOrigin
           ) { return bucket_boundaries; };
}

template<ResampleBoundary resample_boundary>
ResampleClause<resample_boundary> generate_resample_clause(
        ResampleBoundary label_boundary, std::vector<timestamp>&& bucket_boundaries
) {
    ResampleClause<resample_boundary> res{
            "dummy", label_boundary, generate_bucket_boundaries(std::move(bucket_boundaries)), 0, 0
    };
    ProcessingConfig processing_config{false, 0, IndexDescriptor::Type::TIMESTAMP};
    res.set_processing_config(processing_config);
    return res;
}

TEST(Resample, StructureForProcessingBasic) {
    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    auto resample_clause =
            generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {1, 500, 1500, 2500, 2999});
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0}, {1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingColumnSlicing) {
    // Bucket boundaries such that the first processing unit does not need any rows from the second row slice
    // Two column slices
    StreamId sym{"sym"};
    ColRange col_range_1{1, 2};
    ColRange col_range_2{2, 3};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_3 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    auto key_4 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top_left(row_range_1, col_range_1, key_1);
    RangesAndKey top_right(row_range_1, col_range_2, key_2);
    RangesAndKey bottom_left(row_range_2, col_range_1, key_3);
    RangesAndKey bottom_right(row_range_2, col_range_2, key_4);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{top_right, bottom_left, bottom_right, top_left};

    auto resample_clause =
            generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {1, 500, 1500, 2500, 2999});
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 4);
    ASSERT_EQ(ranges_and_keys[0], top_left);
    ASSERT_EQ(ranges_and_keys[1], top_right);
    ASSERT_EQ(ranges_and_keys[2], bottom_left);
    ASSERT_EQ(ranges_and_keys[3], bottom_right);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {2, 3}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingOverlap) {
    // Bucket boundaries such that the first processing unit needs rows from the second row slice
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    auto resample_clause =
            generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {1, 500, 2500, 2999});
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1}, {1}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingSubsumed) {
    // Bucket boundaries such that the first processing unit needs all of the rows from the second and third row slices,
    // such that there is only one element in the returned vector
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    RowRange row_range_3{200, 300};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    auto key_3 = AtomKeyBuilder().start_index(3000).end_index(4000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey middle(row_range_2, col_range, key_2);
    RangesAndKey bottom(row_range_3, col_range, key_3);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, middle, top};

    auto resample_clause = generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {1, 500, 4500});
    auto proc_unit_ids = resample_clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 3);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], middle);
    ASSERT_EQ(ranges_and_keys[2], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids{{0, 1, 2}};
    ASSERT_EQ(expected_proc_unit_ids, proc_unit_ids);
}

TEST(Resample, StructureForProcessingExactBoundary) {
    // Bucket boundaries such that the first processing unit needs rows from the second row slice when the right
    // boundary is closed, but does not when the left boundary is closed
    // No column slicing
    StreamId sym{"sym"};
    ColRange col_range{1, 2};
    RowRange row_range_1{0, 100};
    RowRange row_range_2{100, 200};
    auto key_1 = AtomKeyBuilder().start_index(0).end_index(1000).build<KeyType::TABLE_DATA>(sym);
    auto key_2 = AtomKeyBuilder().start_index(2000).end_index(3000).build<KeyType::TABLE_DATA>(sym);
    RangesAndKey top(row_range_1, col_range, key_1);
    RangesAndKey bottom(row_range_2, col_range, key_2);
    // Insert into vector "out of order" to ensure structure_for_processing reorders correctly
    std::vector<RangesAndKey> ranges_and_keys{bottom, top};

    auto resample_clause_left =
            generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {1, 500, 2000, 2500, 2999});
    auto proc_unit_ids = resample_clause_left.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids_left{{0}, {1}};
    ASSERT_EQ(expected_proc_unit_ids_left, proc_unit_ids);

    auto resample_clause_right =
            generate_resample_clause<ResampleBoundary::RIGHT>(ResampleBoundary::LEFT, {1, 500, 2000, 2500, 2999});
    proc_unit_ids = resample_clause_right.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(ranges_and_keys.size(), 2);
    ASSERT_EQ(ranges_and_keys[0], top);
    ASSERT_EQ(ranges_and_keys[1], bottom);
    std::vector<std::vector<size_t>> expected_proc_unit_ids_right{{0, 1}, {1}};
    ASSERT_EQ(expected_proc_unit_ids_right, proc_unit_ids);
}

TEST(Resample, FindBuckets) {
    // Enough bucket boundaries to test all the interesting cases
    auto resample_left = generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {0, 10, 20, 30, 40});
    auto resample_right =
            generate_resample_clause<ResampleBoundary::RIGHT>(ResampleBoundary::RIGHT, {0, 10, 20, 30, 40});

    resample_left.bucket_boundaries_ =
            resample_left.generate_bucket_boundaries_(0, 0, "dummy", ResampleBoundary::LEFT, 0, 0);
    resample_right.bucket_boundaries_ =
            resample_right.generate_bucket_boundaries_(0, 0, "dummy", ResampleBoundary::RIGHT, 0, 0);

    std::vector<timestamp> res;

    // Wholly contained in first bucket
    res = resample_left.generate_bucket_boundaries(5, 6, true);
    ASSERT_EQ(res.front(), 0);
    ASSERT_EQ(res.back(), 10);
    // Wholly contained in middle bucket
    res = resample_left.generate_bucket_boundaries(12, 13, true);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 20);
    // Wholly contained in last bucket
    res = resample_left.generate_bucket_boundaries(35, 37, true);
    ASSERT_EQ(res.front(), 30);
    ASSERT_EQ(res.back(), 40);

    // Spanning multiple buckets
    res = resample_left.generate_bucket_boundaries(5, 15, true);
    ASSERT_EQ(res.front(), 0);
    ASSERT_EQ(res.back(), 20);
    res = resample_left.generate_bucket_boundaries(15, 25, true);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 30);
    res = resample_left.generate_bucket_boundaries(15, 35, true);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 40);

    // Spanning multiple buckets, not responsible for the first bucket
    res = resample_left.generate_bucket_boundaries(5, 15, false);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 20);
    res = resample_left.generate_bucket_boundaries(15, 25, false);
    ASSERT_EQ(res.front(), 20);
    ASSERT_EQ(res.back(), 30);
    res = resample_left.generate_bucket_boundaries(15, 35, false);
    ASSERT_EQ(res.front(), 20);
    ASSERT_EQ(res.back(), 40);

    // First bucket starts after the first timestamp
    res = resample_left.generate_bucket_boundaries(-5, 15, true);
    ASSERT_EQ(res.front(), 0);
    ASSERT_EQ(res.back(), 20);
    // Last bucket ends before the last timestamp
    res = resample_left.generate_bucket_boundaries(15, 45, true);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 40);

    // Bucket boundary matching first and last timestamps
    res = resample_left.generate_bucket_boundaries(10, 20, true);
    ASSERT_EQ(res.front(), 10);
    ASSERT_EQ(res.back(), 30);
    res = resample_right.generate_bucket_boundaries(10, 20, true);
    ASSERT_EQ(res.front(), 0);
    ASSERT_EQ(res.back(), 20);
}

TEST(Resample, ProcessOneSegment) {
    auto component_manager = std::make_shared<ComponentManager>();

    auto resample = generate_resample_clause<ResampleBoundary::LEFT>(ResampleBoundary::LEFT, {-1, 2, 5});
    resample.bucket_boundaries_ = resample.generate_bucket_boundaries_(0, 0, "dummy", ResampleBoundary::LEFT, 0, 0);
    resample.date_range_ = {0, 5};
    resample.set_component_manager(component_manager);
    resample.set_aggregations({{"sum", "sum_column", "sum_column"}});

    using index_TDT = TypeDescriptorTag<DataTypeTag<DataType::NANOSECONDS_UTC64>, DimensionTag<Dimension ::Dim0>>;
    auto index_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(index_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    using col_TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    auto sum_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(col_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    size_t num_rows{5};
    for (size_t idx = 0; idx < num_rows; ++idx) {
        index_column->set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
        sum_column->set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
    }
    SegmentInMemory seg;
    seg.add_column(scalar_field(index_column->type().data_type(), "index"), index_column);
    seg.add_column(scalar_field(sum_column->type().data_type(), "sum_column"), sum_column);
    seg.set_row_id(num_rows - 1);

    auto proc_unit = ProcessingUnit{std::move(seg)};
    auto entity_ids = push_entities(*component_manager, std::move(proc_unit));

    auto resampled =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, resample.process(std::move(entity_ids))
            );
    ASSERT_TRUE(resampled.segments_.has_value());
    auto segments = resampled.segments_.value();
    ASSERT_EQ(1, segments.size());
    auto resampled_seg = *segments[0];

    auto index_column_index = resampled_seg.column_index("index");
    ASSERT_TRUE(index_column_index.has_value());
    auto& resampled_index_column = resampled_seg.column(*index_column_index);
    ASSERT_EQ(-1, resampled_index_column.scalar_at<int64_t>(0));
    ASSERT_EQ(2, resampled_index_column.scalar_at<int64_t>(1));

    auto sum_column_index = resampled_seg.column_index("sum_column");
    ASSERT_TRUE(sum_column_index.has_value());
    auto& resampled_sum_column = resampled_seg.column(*sum_column_index);
    ASSERT_EQ(1, resampled_sum_column.scalar_at<int64_t>(0));
    ASSERT_EQ(9, resampled_sum_column.scalar_at<int64_t>(1));
}

TEST(Resample, ProcessMultipleSegments) {
    auto component_manager = std::make_shared<ComponentManager>();

    auto resample = generate_resample_clause<ResampleBoundary::LEFT>(
            ResampleBoundary::LEFT, {-15, -5, 5, 6, 25, 35, 45, 46, 55, 65}
    );
    resample.bucket_boundaries_ = resample.generate_bucket_boundaries_(0, 0, "dummy", ResampleBoundary::LEFT, 0, 0);
    resample.date_range_ = {0, 51};
    resample.set_component_manager(component_manager);
    resample.set_aggregations({{"sum", "sum_column", "sum_column"}});
    // Index values of segments will be as follows:
    // 0, 10
    // 20, 30, 40
    // 50
    // Therefore the buckets will be structured such that:
    // -15 - -5: Before the range of the segments
    //  -5 -  5: Covers just the first value in the first segment
    //   5 -  6: Within the range of the first segment, but no index values in this range
    //   6 - 25: Covers the last value from the first segment and the first value of the second segment
    //  25 - 35: Covers just the middle value of the second segment
    //  35 - 45: Covers just the last value of the second segment
    //  45 - 46: Covers a gap between two segments
    //  46 - 55: Covers the third segment
    //  55 - 65: After the range of the segments

    using index_TDT = TypeDescriptorTag<DataTypeTag<DataType::NANOSECONDS_UTC64>, DimensionTag<Dimension ::Dim0>>;
    using col_TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;

    auto index_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(index_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    auto sum_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(col_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    index_column->set_scalar<int64_t>(0, 0);
    index_column->set_scalar<int64_t>(1, 10);
    sum_column->set_scalar<int64_t>(0, 0);
    sum_column->set_scalar<int64_t>(1, 10);
    auto seg_0 = std::make_shared<SegmentInMemory>();
    seg_0->add_column(scalar_field(index_column->type().data_type(), "index"), index_column);
    seg_0->add_column(scalar_field(sum_column->type().data_type(), "sum_column"), sum_column);
    seg_0->set_row_id(1);
    auto row_range_0 = std::make_shared<RowRange>(0, 2);
    auto col_range_0 = std::make_shared<ColRange>(1, 2);

    index_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(index_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    sum_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(col_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    index_column->set_scalar<int64_t>(0, 20);
    index_column->set_scalar<int64_t>(1, 30);
    index_column->set_scalar<int64_t>(2, 40);
    sum_column->set_scalar<int64_t>(0, 20);
    sum_column->set_scalar<int64_t>(1, 30);
    sum_column->set_scalar<int64_t>(2, 40);
    auto seg_1 = std::make_shared<SegmentInMemory>();
    seg_1->add_column(scalar_field(index_column->type().data_type(), "index"), index_column);
    seg_1->add_column(scalar_field(sum_column->type().data_type(), "sum_column"), sum_column);
    seg_1->set_row_id(2);
    auto row_range_1 = std::make_shared<RowRange>(2, 5);
    auto col_range_1 = std::make_shared<ColRange>(1, 2);

    index_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(index_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    sum_column = std::make_shared<Column>(
            static_cast<TypeDescriptor>(col_TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    index_column->set_scalar<int64_t>(0, 50);
    sum_column->set_scalar<int64_t>(0, 50);
    auto seg_2 = std::make_shared<SegmentInMemory>();
    seg_2->add_column(scalar_field(index_column->type().data_type(), "index"), index_column);
    seg_2->add_column(scalar_field(sum_column->type().data_type(), "sum_column"), sum_column);
    seg_2->set_row_id(2);
    auto row_range_2 = std::make_shared<RowRange>(5, 6);
    auto col_range_2 = std::make_shared<ColRange>(1, 2);

    auto ids = component_manager->get_new_entity_ids(3);
    component_manager->add_entity(ids[0], seg_0, row_range_0, col_range_0, EntityFetchCount(1));
    component_manager->add_entity(ids[1], seg_1, row_range_1, col_range_1, EntityFetchCount(2));
    component_manager->add_entity(ids[2], seg_2, row_range_2, col_range_2, EntityFetchCount(1));

    std::vector<EntityId> ids_0{ids[0], ids[1]};
    std::vector<EntityId> ids_1{ids[1]};
    std::vector<EntityId> ids_2{ids[2]};

    auto resampled_0 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, resample.process(std::move(ids_0))
            );
    auto resampled_seg_0 = *resampled_0.segments_.value()[0];
    auto& resampled_index_column_0 = resampled_seg_0.column(0);
    auto& resampled_sum_column_0 = resampled_seg_0.column(1);
    ASSERT_EQ(-5, resampled_index_column_0.scalar_at<int64_t>(0));
    ASSERT_EQ(6, resampled_index_column_0.scalar_at<int64_t>(1));
    ASSERT_EQ(0, resampled_sum_column_0.scalar_at<int64_t>(0));
    ASSERT_EQ(30, resampled_sum_column_0.scalar_at<int64_t>(1));

    auto resampled_1 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, resample.process(std::move(ids_1))
            );
    auto resampled_seg_1 = *resampled_1.segments_.value()[0];
    auto& resampled_index_column_1 = resampled_seg_1.column(0);
    auto& resampled_sum_column_1 = resampled_seg_1.column(1);
    ASSERT_EQ(25, resampled_index_column_1.scalar_at<int64_t>(0));
    ASSERT_EQ(35, resampled_index_column_1.scalar_at<int64_t>(1));
    ASSERT_EQ(30, resampled_sum_column_1.scalar_at<int64_t>(0));
    ASSERT_EQ(40, resampled_sum_column_1.scalar_at<int64_t>(1));

    auto resampled_2 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, resample.process(std::move(ids_2))
            );
    auto resampled_seg_2 = *resampled_2.segments_.value()[0];
    auto& resampled_index_column_2 = resampled_seg_2.column(0);
    auto& resampled_sum_column_2 = resampled_seg_2.column(1);
    ASSERT_EQ(46, resampled_index_column_2.scalar_at<int64_t>(0));
    ASSERT_EQ(50, resampled_sum_column_2.scalar_at<int64_t>(0));
}

template<typename SortedAggregatorType, ResampleBoundary label_>
struct AggregatorAndLabel {
    using SortedAggregator = SortedAggregatorType;
    constexpr static ResampleBoundary label = label_;
    using IndexTDT = ScalarTagType<DataTypeTag<DataType::NANOSECONDS_UTC64>>;
};

template<typename AggregatorAndLabel>
class SortedAggregatorSparseStructure : public ::testing::Test {};

template<typename T, size_t count>
constexpr std::array<T, count> linear_range(T start, T step) {
    std::array<T, start> arr;
    std::generate_n(arr.begin(), count, [i = T{0}, start, step]() mutable { return start + (i++) * step; });
    return arr;
}

template<typename T, size_t bucket_count>
constexpr std::array<T, bucket_count - 1> generate_labels(std::array<T, bucket_count> buckets, ResampleBoundary label) {
    std::array<T, bucket_count - 1> result;
    if (label == ResampleBoundary::LEFT) {
        std::copy_n(buckets.begin(), bucket_count - 1, result.begin());
    } else {
        std::copy_n(buckets.begin() + 1, bucket_count - 1, result.begin());
    }
    return result;
}

void assert_column_is_dense(const Column& c) {
    ASSERT_FALSE(c.sparse_permitted());
    ASSERT_FALSE(c.is_sparse());
    ASSERT_FALSE(c.opt_sparse_map().has_value());
}

void assert_column_is_sparse(const Column& c) {
    ASSERT_TRUE(c.sparse_permitted());
    ASSERT_TRUE(c.is_sparse());
    ASSERT_TRUE(c.opt_sparse_map().has_value());
}

// The aggregation operator does not matter for this case. Just pick one that's applicable to all column types.
using AggregatorTypes = ::testing::Types<
        AggregatorAndLabel<
                SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::LEFT>, ResampleBoundary::LEFT>,
        AggregatorAndLabel<
                SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::LEFT>, ResampleBoundary::RIGHT>,
        AggregatorAndLabel<
                SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::RIGHT>, ResampleBoundary::LEFT>,
        AggregatorAndLabel<
                SortedAggregator<AggregationOperator::COUNT, ResampleBoundary::RIGHT>, ResampleBoundary::RIGHT>>;

TYPED_TEST_SUITE(SortedAggregatorSparseStructure, AggregatorTypes); // Registers test suite with int and float

TYPED_TEST(SortedAggregatorSparseStructure, NoMissingInputColumnsProducesDenseColumn) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 6> bucket_boundaries{0, 10, 20, 30, 40, 50};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{1, 2, 3}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 4>{11, 21, 31, 41}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(
                            std::array{10, 35, 56, 1, 2}, TypeDescriptor::scalar_type(DataType::INT32)
                    ),
                    nullptr,
                    "col1"
            })
    };

    {
        // Test single input column
        const std::optional<Column> output = aggregator.generate_resampling_output_column(
                std::span{input_index_columns.begin(), 1},
                std::span{input_agg_columns.begin(), 1},
                output_index_column,
                label
        );
        EXPECT_TRUE(output.has_value());
        assert_column_is_dense(*output);
        ASSERT_EQ(output->row_count(), output_index.size());
    }

    {
        // Test multiple input columns
        const std::optional<Column> output = aggregator.generate_resampling_output_column(
                input_index_columns, input_agg_columns, output_index_column, label
        );
        EXPECT_TRUE(output.has_value());
        assert_column_is_dense(*output);
        ASSERT_EQ(output->row_count(), output_index.size());
    }
}

TYPED_TEST(SortedAggregatorSparseStructure, FirstColumnExistSecondIsMissing) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 3> bucket_boundaries{0, 10, 20};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{0, 2, 3}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 4>{11, 21, 22, 24}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::optional<ColumnWithStrings>{}
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    ASSERT_TRUE(output.has_value());
    assert_column_is_sparse(*output);
    const util::BitSet& sparse_map = output->sparse_map();
    ASSERT_EQ(output->row_count(), 1);
    ASSERT_EQ(output->last_row(), output_index.size() - 1);
    ASSERT_EQ(sparse_map.size(), 2);
    ASSERT_EQ(sparse_map.count(), 1);
    ASSERT_EQ(sparse_map[0], true);
    ASSERT_EQ(sparse_map[1], false);
}

TYPED_TEST(SortedAggregatorSparseStructure, FirstColumnExistWithValueOnRightBoundarySecondIsMissing) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr ResampleBoundary closed = TypeParam::SortedAggregator::closed;
    const Column output_index_column = []() {
        if constexpr (label == ResampleBoundary::LEFT) {
            return Column::create_dense_column(std::array<timestamp, 3>{0, 10, 30}, IndexTDT::type_descriptor());
        } else {
            return Column::create_dense_column(std::array<timestamp, 3>{10, 20, 40}, IndexTDT::type_descriptor());
        }
    }();
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{0, 2, 10}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 2>{35, 36}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::optional<ColumnWithStrings>{}
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    ASSERT_TRUE(output.has_value());
    assert_column_is_sparse(*output);
    const util::BitSet& sparse_map = output->sparse_map();

    if constexpr (closed == ResampleBoundary::LEFT) {
        ASSERT_EQ(sparse_map.count(), 2);
        ASSERT_EQ(sparse_map[0], true);
        ASSERT_EQ(sparse_map[1], true);
        ASSERT_EQ(sparse_map[2], false);
    } else if constexpr (closed == ResampleBoundary::RIGHT) {
        ASSERT_EQ(sparse_map.count(), 1);
        ASSERT_EQ(sparse_map[0], true);
        ASSERT_EQ(sparse_map[1], false);
        ASSERT_EQ(sparse_map[2], false);
    }
}

TYPED_TEST(SortedAggregatorSparseStructure, ReturnDenseInCaseOutputIndexIsFilledSecondColumnMissing) {
    // Even if there is nullopt inside input_agg_columns each output index bucket can be filled. In that case ensure
    // no sparse map is created and the column is dense.
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 3> bucket_boundaries{0, 10, 20};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{0, 2, 12}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 4>{15, 16, 18, 20}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::optional<ColumnWithStrings>{}
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    EXPECT_TRUE(output.has_value());
    assert_column_is_dense(*output);
    ASSERT_EQ(output->row_count(), output_index_column.row_count());
}

TYPED_TEST(SortedAggregatorSparseStructure, ReturnDenseInCaseOutputIndexIsFilledFirstColumnMissing) {
    // Even if there is nullopt inside input_agg_columns each output index bucket can be filled. In that case ensure
    // no sparse map is created and the column is dense.
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 3> bucket_boundaries{0, 10, 20};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{0, 2, 5}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 4>{7, 8, 9, 15}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::optional<ColumnWithStrings>{},
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6, 5}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    EXPECT_TRUE(output.has_value());
    assert_column_is_dense(*output);
    ASSERT_EQ(output->row_count(), output_index_column.row_count());
}

TYPED_TEST(SortedAggregatorSparseStructure, FirstColumnIsMissing) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 3> bucket_boundaries{0, 10, 20};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());
    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 3>{0, 2, 3}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 4>{11, 15, 16, 17}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::optional<ColumnWithStrings>{},
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{0, 5, 6, 5}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),

    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    ASSERT_TRUE(output.has_value());
    assert_column_is_sparse(*output);
    const util::BitSet& sparse_map = output->sparse_map();
    ASSERT_EQ(output->row_count(), 1);
    ASSERT_EQ(output->last_row(), output_index.size() - 1);
    ASSERT_EQ(sparse_map.size(), 2);
    ASSERT_EQ(sparse_map.count(), 1);
    ASSERT_EQ(sparse_map[0], false);
    ASSERT_EQ(sparse_map[1], true);
}

TYPED_TEST(SortedAggregatorSparseStructure, ThreeSegmentsInABucketMiddleIsMissing) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 2> bucket_boundaries{0, 10};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());

    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 2>{0, 1}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 1>{2}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 1>{3}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{1, 2}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::optional<ColumnWithStrings>{},
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{3, 4}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    ASSERT_TRUE(output.has_value());
    assert_column_is_dense(*output);
    ASSERT_EQ(output->row_count(), 1);
}

TYPED_TEST(SortedAggregatorSparseStructure, ThreeSegmentsInABuckeOnlyMiddleIsPresent) {
    using IndexTDT = typename TypeParam::IndexTDT;
    const typename TypeParam::SortedAggregator aggregator{
            ColumnName{"input_column_name"}, ColumnName{"output_column_name"}
    };
    constexpr ResampleBoundary label = TypeParam::label;
    constexpr static std::array<timestamp, 2> bucket_boundaries{0, 10};
    constexpr static std::array output_index = generate_labels(bucket_boundaries, label);
    const Column output_index_column = Column::create_dense_column(output_index, IndexTDT::type_descriptor());

    const std::array input_index_columns{
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 2>{0, 1}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 1>{2}, IndexTDT::type_descriptor())
            ),
            std::make_shared<Column>(
                    Column::create_dense_column(std::array<timestamp, 1>{3}, IndexTDT::type_descriptor())
            )
    };
    const std::array input_agg_columns{
            std::optional<ColumnWithStrings>{},
            std::make_optional(ColumnWithStrings{
                    Column::create_dense_column(std::array{1}, TypeDescriptor::scalar_type(DataType::INT32)),
                    nullptr,
                    "col1"
            }),
            std::optional<ColumnWithStrings>{}
    };
    const std::optional<Column> output = aggregator.generate_resampling_output_column(
            input_index_columns, input_agg_columns, output_index_column, label
    );
    ASSERT_TRUE(output.has_value());
    assert_column_is_dense(*output);
    ASSERT_EQ(output->row_count(), 1);
}