/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>
#include <stream/test/stream_test_common.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/segment_generation_utils.hpp>
#include <arcticdb/util/test/input_frame_utils.hpp>

using namespace arcticdb;
using namespace std::ranges;

constexpr static MergeStrategy update_only_strategy{
        .matched = MergeAction::UPDATE,
        .not_matched_by_target = MergeAction::DO_NOTHING
};

constexpr static std::array non_string_fields = {
        FieldRef(TypeDescriptor(DataType::INT8, Dimension::Dim0), "int8"),
        FieldRef(TypeDescriptor(DataType::UINT32, Dimension::Dim0), "uint32"),
        FieldRef(TypeDescriptor(DataType::BOOL8, Dimension::Dim0), "bool8"),
        FieldRef(TypeDescriptor(DataType::FLOAT32, Dimension::Dim0), "float32"),
        FieldRef(TypeDescriptor(DataType::NANOSECONDS_UTC64, Dimension::Dim0), "timestamp")
};

StreamDescriptor non_string_fields_ts_index_descriptor() {
    return TimeseriesIndex::default_index().create_stream_descriptor("Source", non_string_fields);
}

template<typename T>
std::vector<std::shared_ptr<T>> wrap_in_shared_ptr(std::vector<T>&& v) {
    std::vector<std::shared_ptr<T>> res;
    res.reserve(v.size());
    std::ranges::transform(v, std::back_inserter(res), [](T& x) { return std::make_shared<T>(std::move(x)); });
    return res;
}

std::vector<RangesAndKey> generate_ranges_and_keys(
        const StreamDescriptor& source_descriptor, const std::span<const SegmentInMemory> segments,
        const std::span<const ColRange> col_ranges, const std::span<const RowRange> row_ranges
) {
    std::vector<RangesAndKey> ranges_and_keys;
    ranges_and_keys.reserve(segments.size());
    for (size_t i = 0; i < segments.size(); ++i) {
        const timestamp start_ts = *segments[i].scalar_at<timestamp>(0, 0);
        const timestamp end_ts = *segments[i].scalar_at<timestamp>(segments[i].row_count() - 1, 0);
        ranges_and_keys.emplace_back(
                row_ranges[i],
                col_ranges[i],
                AtomKeyBuilder()
                        .start_index(start_ts)
                        .end_index(end_ts + 1)
                        .build<KeyType::TABLE_DATA>(source_descriptor.id())
        );
    }
    return ranges_and_keys;
}

template<std::ranges::random_access_range... Other>
void sort_by_rowslice(std::span<RowRange> rows, std::span<ColRange> cols, Other&... other) {
    std::vector<size_t> correct_positions(rows.size());
    std::iota(correct_positions.begin(), correct_positions.end(), 0);
    std::ranges::sort(correct_positions, [&](const size_t i, const size_t j) {
        return std::tie(rows[i], cols[i]) < std::tie(rows[j], cols[j]);
    });
    []<std::ranges::random_access_range... T>(const std::span<const size_t> positions, T&... ts) {
        util::BitSet used;
        used.resize(positions.size());
        for (size_t i = 0; i < positions.size(); ++i) {
            if (used[i] || positions[i] == i) {
                continue;
            }
            auto temp = std::tuple{std::move(ts[i])...};
            size_t current = i;
            while (positions[current] != i) {
                size_t next = positions[current];
                ((ts[current] = std::move(ts[next])), ...);
                used[current] = true;
                current = next;
            }
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                ((ts[current] = std::move(std::get<Is>(temp))), ...);
            }(std::make_index_sequence<sizeof...(T)>{});
            used[current] = true;
        }
    }(correct_positions, rows, cols, other...);
}

MergeUpdateClause create_clause(
        const MergeStrategy strategy, std::shared_ptr<ComponentManager> component_manager, InputFrame&& input_frame
) {
    MergeUpdateClause clause({}, strategy, std::make_shared<InputFrame>(std::move(input_frame)));
    clause.set_component_manager(std::move(component_manager));
    return clause;
}

std::vector<EntityId> push_selected_entities(
        ComponentManager& component_manager, const std::span<const RangesAndKey> ranges_and_keys,
        std::vector<SegmentInMemory>&& segments_in, std::vector<ColRange>&& col_ranges_in,
        std::vector<RowRange>&& row_ranges_in
) {
    size_t idx = 0;
    std::vector<std::shared_ptr<SegmentInMemory>> segments;
    std::vector<std::shared_ptr<ColRange>> col_ranges;
    std::vector<std::shared_ptr<RowRange>> row_ranges;
    std::vector<std::shared_ptr<AtomKey>> atom_keys;
    for (const RangesAndKey& range : ranges_and_keys) {
        while ((idx < segments_in.size()) &&
               (range.row_range() != row_ranges_in[idx] || range.col_range() != col_ranges_in[idx])) {
            ++idx;
        }
        segments.emplace_back(std::make_shared<SegmentInMemory>(std::move(segments_in[idx])));
        col_ranges.emplace_back(std::make_shared<ColRange>(std::move(col_ranges_in[idx])));
        row_ranges.emplace_back(std::make_shared<RowRange>(std::move(row_ranges_in[idx])));
        atom_keys.emplace_back(std::make_shared<AtomKey>(range.key_));
    }
    return component_manager.add_entities(
            std::move(segments),
            std::move(col_ranges),
            std::move(row_ranges),
            std::move(atom_keys),
            std::vector<EntityFetchCount>(ranges_and_keys.size(), 0)
    );
}

struct MergeUpdateClauseUpdateStrategyTestBase {
    MergeUpdateClauseUpdateStrategyTestBase(
            const StreamDescriptor& desc, MergeStrategy strategy,
            std::tuple<std::vector<SegmentInMemory>, std::vector<ColRange>, std::vector<RowRange>>&& data
    ) :
        descriptor_(desc),
        strategy_(strategy) {
        auto [segments, col_ranges, row_ranges] = std::move(data);
        sort_by_rowslice(row_ranges, col_ranges, segments);
        ranges_and_keys_ = generate_ranges_and_keys(descriptor_, segments, col_ranges, row_ranges);
        row_ranges_ = std::move(row_ranges);
        col_ranges_ = std::move(col_ranges);
        segments_ = std::move(segments);
    }

    MergeUpdateClause create_clause(InputFrame&& input_frame) const {
        return ::create_clause(strategy_, component_manager_, std::move(input_frame));
    }

    std::vector<EntityId> push_entities() {
        return push_selected_entities(
                *component_manager_,
                ranges_and_keys_,
                std::move(segments_),
                std::move(col_ranges_),
                std::move(row_ranges_)
        );
    }

    StreamDescriptor descriptor_;
    std::vector<RangesAndKey> ranges_and_keys_;
    std::shared_ptr<ComponentManager> component_manager_ = std::make_shared<ComponentManager>();
    MergeStrategy strategy_;
    std::vector<SegmentInMemory> segments_;
    std::vector<RowRange> row_ranges_;
    std::vector<ColRange> col_ranges_;
};

std::vector<std::vector<EntityId>> structure_entities(
        const std::span<const std::vector<size_t>> structure_indices, const std::vector<EntityId>& entities
) {
    std::vector<std::vector<EntityId>> structured_entities;
    structured_entities.reserve(structure_indices.size());
    std::ranges::transform(
            structure_indices,
            std::back_inserter(structured_entities),
            [&](std::span<const size_t> indices) {
                std::vector<EntityId> result;
                result.reserve(indices.size());
                std::ranges::transform(indices, std::back_inserter(result), [&](const size_t idx) {
                    return entities[idx];
                });
                return result;
            }
    );
    return structured_entities;
}

/// Param is a tuple of rows_per_segment and cols_per_segment
struct MergeUpdateClauseUpdateStrategyMatchAllSegTest : MergeUpdateClauseUpdateStrategyTestBase,
                                                        testing::TestWithParam<std::tuple<int, int>> {
    MergeUpdateClauseUpdateStrategyMatchAllSegTest() :
        MergeUpdateClauseUpdateStrategyTestBase(
                non_string_fields_ts_index_descriptor(),
                MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::DO_NOTHING},
                slice_data_into_segments<TimeseriesIndex>(
                        non_string_fields_ts_index_descriptor(), rows_per_segment(), cols_per_segment(),
                        iota_view(timestamp{0}, timestamp{num_rows_}),
                        iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows_)),
                        iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows_)),
                        iota_view(0, num_rows_) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
                        iota_view(0, num_rows_) | views::transform([](auto x) { return static_cast<float>(x); }),
                        iota_view(timestamp{0}, timestamp{num_rows_})
                )
        ) {
        // Shuffle the input ranges and keys to ensure structure_for_processing sorts correctly
        constexpr static size_t rand_seed = 0;
        std::mt19937 g(rand_seed);
        shuffle(ranges_and_keys_, g);
    }

    [[nodiscard]] static int rows_per_segment() { return std::get<0>(GetParam()); }
    [[nodiscard]] static int cols_per_segment() { return std::get<1>(GetParam()); }
    constexpr static int num_rows_ = 15;
    constexpr static int num_cols_ = non_string_fields.size();
    [[nodiscard]] static RowRange row_range_for_row_slice(const size_t row_slice) {
        const size_t start_row = row_slice * rows_per_segment();
        const size_t end_row = std::min(start_row + rows_per_segment(), size_t(num_rows_));
        return RowRange{start_row, end_row};
    }
    [[nodiscard]] static ColRange col_range_for_col_slice(const size_t col_slice_inside_row_slice) {
        const size_t start_col = TimeseriesIndex::field_count() + col_slice_inside_row_slice * cols_per_segment();
        const size_t end_col =
                std::min(start_col + cols_per_segment(), TimeseriesIndex::field_count() + size_t(num_cols_));
        return ColRange{start_col, end_col};
    }

    void assert_structure_for_processing_creates_ordered_row_sliced_data_containing_all_segments(
            const std::span<const std::vector<size_t>> structure_indices
    ) const {
        const size_t row_slices_to_process = (num_rows_ + rows_per_segment() - 1) / rows_per_segment();
        const size_t column_slices_per_row_slice = (num_cols_ + cols_per_segment() - 1) / cols_per_segment();
        ASSERT_EQ(structure_indices.size(), row_slices_to_process);
        ASSERT_EQ(ranges_and_keys_.size(), row_slices_to_process * column_slices_per_row_slice);
        for (size_t i = 0; i < structure_indices.size(); ++i) {
            SCOPED_TRACE(testing::Message() << fmt::format("structure index: {}", i));
            EXPECT_EQ(structure_indices[i].size(), column_slices_per_row_slice);
            const RowRange current_row_range = row_range_for_row_slice(i);
            for (size_t j = 0; j < structure_indices[i].size(); ++j) {
                SCOPED_TRACE(testing::Message() << fmt::format("column slice index: {}", j));
                EXPECT_EQ(ranges_and_keys_[structure_indices[i][j]].row_range(), current_row_range);
                EXPECT_EQ(ranges_and_keys_[structure_indices[i][j]].col_range(), col_range_for_col_slice(j));
            }
        }
    }

    void assert_process_results_match_expected(
            const MergeUpdateClause& clause, const std::span<const SegmentInMemory> expected_segments,
            const std::span<const std::vector<size_t>> structure_indices, const std::vector<EntityId>& entities
    ) const {
        std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
        const size_t column_slices_per_row_slice = (num_cols_ + cols_per_segment() - 1) / cols_per_segment();
        for (size_t i = 0; i < structured_entities.size(); ++i) {
            auto proc = gather_entities<
                    std::shared_ptr<SegmentInMemory>,
                    std::shared_ptr<RowRange>,
                    std::shared_ptr<ColRange>>(*component_manager_, clause.process(std::move(structured_entities[i])));
            SCOPED_TRACE(testing::Message() << "processing result (row slice) = " << i);
            EXPECT_EQ(proc.segments_->size(), column_slices_per_row_slice);
            EXPECT_EQ(proc.row_ranges_->size(), column_slices_per_row_slice);
            EXPECT_EQ(proc.col_ranges_->size(), column_slices_per_row_slice);
            for (size_t j = 0; j < column_slices_per_row_slice; ++j) {
                SCOPED_TRACE(testing::Message() << "processing result (col slice) = " << j);
                EXPECT_EQ(*(proc.row_ranges_.value())[j], ranges_and_keys_[structure_indices[i][j]].row_range());
                EXPECT_EQ(*(proc.col_ranges_.value())[j], ranges_and_keys_[structure_indices[i][j]].col_range());
                EXPECT_EQ(*(proc.segments_.value())[j], expected_segments[i * column_slices_per_row_slice + j]);
            }
        }
    }
};

TEST_P(MergeUpdateClauseUpdateStrategyMatchAllSegTest, SourceIndexMatchesAllSegments) {
    using stream::TimeseriesIndex;
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 3>{0, 7, 14},
            std::array<int8_t, 3>{10, 20, 30},
            std::array<uint32_t, 3>{100, 200, 300},
            std::array{true, false, true},
            std::array{11.1f, 22.2f, 33.3f},
            std::array<timestamp, 3>{1000, 2000, 3000}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    assert_structure_for_processing_creates_ordered_row_sliced_data_containing_all_segments(structure_indices);
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment(),
            cols_per_segment(),
            iota_view(timestamp{0}, timestamp{3 * rows_per_segment()}),
            std::array<int8_t, 15>{10, 1, 2, 3, 4, 5, 6, 20, 8, 9, 10, 11, 12, 13, 30},
            std::array<unsigned, 15>{100, 1, 2, 3, 4, 5, 6, 200, 8, 9, 10, 11, 12, 13, 300},
            std::array<bool, 15>{1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1},
            std::array<float, 15>{11.1f, 1, 2, 3, 4, 5, 6, 22.2f, 8, 9, 10, 11, 12, 13, 33.3f},
            std::array<timestamp, 15>{1000, 1, 2, 3, 4, 5, 6, 2000, 8, 9, 10, 11, 12, 13, 3000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segments);
    assert_process_results_match_expected(clause, expected_segments, structure_indices, push_entities());
}

TEST_P(MergeUpdateClauseUpdateStrategyMatchAllSegTest, SourceHasValuesOutsideOfTheDataFrame) {
    using stream::TimeseriesIndex;

    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 7>{-10, -9, 0, 7, 14, 20, 25},
            std::array<int8_t, 7>{-1, -2, 10, 20, 30, -10, -20},
            std::array<uint32_t, 7>{10, 20, 100, 200, 300, 30, 40},
            std::array{false, true, true, false, true, true, false},
            std::array{-10.f, -11.f, 11.1f, 22.2f, 33.3f, -12.f, -14.f},
            std::array<timestamp, 7>{-11, -12, 1000, 2000, 3000, -13, -14}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    assert_structure_for_processing_creates_ordered_row_sliced_data_containing_all_segments(structure_indices);
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment(),
            cols_per_segment(),
            iota_view(timestamp{0}, timestamp{3 * rows_per_segment()}),
            std::array<int8_t, 15>{10, 1, 2, 3, 4, 5, 6, 20, 8, 9, 10, 11, 12, 13, 30},
            std::array<unsigned, 15>{100, 1, 2, 3, 4, 5, 6, 200, 8, 9, 10, 11, 12, 13, 300},
            std::array<bool, 15>{1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1},
            std::array<float, 15>{11.1f, 1, 2, 3, 4, 5, 6, 22.2f, 8, 9, 10, 11, 12, 13, 33.3f},
            std::array<timestamp, 15>{1000, 1, 2, 3, 4, 5, 6, 2000, 8, 9, 10, 11, 12, 13, 3000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segments);
    assert_process_results_match_expected(clause, expected_segments, structure_indices, push_entities());
}

INSTANTIATE_TEST_SUITE_P(
        MergeUpdateUpdateStrategy, MergeUpdateClauseUpdateStrategyMatchAllSegTest,
        testing::Values(
                /* Default */ std::tuple{100000, 127},
                /* Row slices are multiple of the number of rows*/ std::tuple{5, 3},
                /*Single column slice*/ std::tuple{6, 5},
                /*Single row slice*/ std::tuple{20, 2}
        )
);

INSTANTIATE_TEST_SUITE_P(
        SourceHasValuesOutsideOfTheDataFrame, MergeUpdateClauseUpdateStrategyMatchAllSegTest,
        testing::Values(
                /* Default */ std::tuple{100000, 127},
                /* Row slices are multiple of the number of rows*/ std::tuple{5, 3},
                /*Single column slice*/ std::tuple{6, 5},
                /*Single row slice*/ std::tuple{20, 2}
        )
);

struct MergeUpdateClauseUpdateStrategyMatchSubsetTest : MergeUpdateClauseUpdateStrategyTestBase, testing::Test {
    MergeUpdateClauseUpdateStrategyMatchSubsetTest() :
        MergeUpdateClauseUpdateStrategyTestBase(
                non_string_fields_ts_index_descriptor(), update_only_strategy,
                slice_data_into_segments<TimeseriesIndex>(
                        non_string_fields_ts_index_descriptor(), rows_per_segment_, cols_per_segment_,
                        iota_view(timestamp{0}, timestamp{num_rows_}),
                        iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows_)),
                        iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows_)),
                        iota_view(0, num_rows_) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
                        iota_view(0, num_rows_) | views::transform([](auto x) { return static_cast<float>(x); }),
                        iota_view(timestamp{0}, timestamp{num_rows_})
                )
        ) {
        // Shuffle the input ranges and keys to ensure structure_for_processing sorts correctly
        constexpr static size_t rand_seed = 0;
        std::mt19937 g(rand_seed);
        shuffle(ranges_and_keys_, g);
    }
    constexpr static int num_rows_ = 14;
    constexpr static int num_cols_ = non_string_fields.size();
    constexpr static int rows_per_segment_ = 5;
    constexpr static int cols_per_segment_ = 3;
};

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, NoMatch) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 4>{-10, -9, 20, 25},
            std::array<int8_t, 4>{-1, -2, 10, 20},
            std::array<uint32_t, 4>{10, 20, 100, 200},
            std::array{false, true, true, false},
            std::array{-10.f, -11.f, 11.1f, 22.2f},
            std::array<timestamp, 4>{-11, -12, 1000, 2000}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(structure_indices.size(), 0);
    ASSERT_EQ(ranges_and_keys_.size(), 0);
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchFirst) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 1>{3},
            std::array<int8_t, 1>{-2},
            std::array<uint32_t, 1>{20},
            std::array{false},
            std::array{-11.f},
            std::array<timestamp, 1>{-12}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 2);
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result_entities
    );
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            iota_view<timestamp, timestamp>(0, 10),
            std::array<int8_t, 5>{0, 1, 2, -2, 4},
            std::array<unsigned, 5>{0, 1, 2, 20, 4},
            std::array<bool, 5>{0, 1, 0, 0, 0},
            std::array<float, 5>{0.f, 1, 2, -11.f, 4},
            std::array<timestamp, 5>{0, 1, 2, -12, 4}
    );
    ASSERT_EQ(*(*proc.segments_)[0], expected_segments[0]);
    ASSERT_EQ(*(*proc.segments_)[1], expected_segments[1]);
    ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
    ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
    ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchSecond) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 1>{6},
            std::array<int8_t, 1>{-2},
            std::array<uint32_t, 1>{20},
            std::array{true},
            std::array{-11.f},
            std::array<timestamp, 1>{-12}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 2);
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result_entities
    );
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            iota_view<timestamp, timestamp>(5, 10),
            std::array<int8_t, 5>{5, -2, 7, 8, 9},
            std::array<unsigned, 5>{5, 20, 7, 8, 9},
            std::array<bool, 5>{1, 1, 1, 0, 1},
            std::array<float, 5>{5, -11.f, 7, 8, 9},
            std::array<timestamp, 5>{5, -12, 7, 8, 9}
    );
    ASSERT_EQ(*(*proc.segments_)[0], expected_segments[0]);
    ASSERT_EQ(*(*proc.segments_)[1], expected_segments[1]);
    ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(5, 10));
    ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(5, 10));
    ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchThird) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 1>{10},
            std::array<int8_t, 1>{-2},
            std::array<uint32_t, 1>{20},
            std::array{true},
            std::array{-11.f},
            std::array<timestamp, 1>{-12}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 2);
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));

    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result_entities
    );
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            iota_view<timestamp, timestamp>(10, 14),
            std::array<int8_t, 4>{-2, 11, 12, 13},
            std::array<unsigned, 4>{20, 11, 12, 13},
            std::array<bool, 4>{1, 1, 0, 1},
            std::array<float, 4>{-11.f, 11, 12, 13},
            std::array<timestamp, 5>{-12, 11, 12, 13}
    );
    ASSERT_EQ(*(*proc.segments_)[0], expected_segments[0]);
    ASSERT_EQ(*(*proc.segments_)[1], expected_segments[1]);
    ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(10, 14));
    ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(10, 14));
    ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchFirstAndThird) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 2>{4, 10},
            std::array<int8_t, 2>{-2, -3},
            std::array<uint32_t, 2>{20, 30},
            std::array{true, false},
            std::array{-11.f, -12.f},
            std::array<timestamp, 2>{-12, -13}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 4);
    ASSERT_EQ(structure_indices.size(), 2);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(structure_indices[1].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].col_range(), ColRange(4, 6));
    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(structured_entities[0]));
    auto proc_0 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_0
            );
    auto [expected_segments_0, expected_col_ranges_0, expected_row_ranges_0] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(0, 5),
                    std::array<int8_t, 5>{0, 1, 2, 3, -2},
                    std::array<unsigned, 5>{0, 1, 2, 3, 20},
                    std::array<bool, 5>{0, 1, 0, 1, 1},
                    std::array<float, 5>{0, 1, 2, 3, -11},
                    std::array<timestamp, 5>{0, 1, 2, 3, -12}
            );
    ASSERT_EQ(*(*proc_0.segments_)[0], expected_segments_0[0]);
    ASSERT_EQ(*(*proc_0.segments_)[1], expected_segments_0[1]);
    ASSERT_EQ(*(*proc_0.row_ranges_)[0], RowRange(0, 5));
    ASSERT_EQ(*(*proc_0.row_ranges_)[1], RowRange(0, 5));
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], ColRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(structured_entities[1]));
    auto proc_1 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_1
            );
    auto [expected_segments_1, expected_col_ranges_1, expected_row_ranges_1] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(10, 14),
                    std::array<int8_t, 4>{-3, 11, 12, 13},
                    std::array<unsigned, 4>{30, 11, 12, 13},
                    std::array<bool, 4>{0, 1, 0, 1},
                    std::array<float, 4>{-12, 11, 12, 13},
                    std::array<timestamp, 4>{-13, 11, 12, 13}
            );
    ASSERT_EQ(*(*proc_1.segments_)[0], expected_segments_1[0]);
    ASSERT_EQ(*(*proc_1.segments_)[1], expected_segments_1[1]);
    ASSERT_EQ(*(*proc_1.row_ranges_)[0], RowRange(10, 14));
    ASSERT_EQ(*(*proc_1.row_ranges_)[1], RowRange(10, 14));
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], ColRange(4, 6));
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchFirstAndSecond) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 2>{0, 5},
            std::array<int8_t, 2>{-2, -3},
            std::array<uint32_t, 2>{20, 30},
            std::array{true, false},
            std::array{-11.f, -12.f},
            std::array<timestamp, 2>{-12, -13}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 4);
    ASSERT_EQ(structure_indices.size(), 2);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(structure_indices[1].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].col_range(), ColRange(4, 6));
    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(structured_entities[0]));
    auto proc_0 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_0
            );
    auto [expected_segments_0, expected_col_ranges_0, expected_row_ranges_0] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(0, 5),
                    std::array<int8_t, 5>{-2, 1, 2, 3, 4},
                    std::array<unsigned, 5>{20, 1, 2, 3, 4},
                    std::array<bool, 5>{1, 1, 0, 1, 0},
                    std::array<float, 5>{-11, 1, 2, 3, 4},
                    std::array<timestamp, 5>{-12, 1, 2, 3, 4}
            );
    ASSERT_EQ(*(*proc_0.segments_)[0], expected_segments_0[0]);
    ASSERT_EQ(*(*proc_0.segments_)[1], expected_segments_0[1]);
    ASSERT_EQ(*(*proc_0.row_ranges_)[0], RowRange(0, 5));
    ASSERT_EQ(*(*proc_0.row_ranges_)[1], RowRange(0, 5));
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], ColRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(structured_entities[1]));
    auto proc_1 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_1
            );
    auto [expected_segments_1, expected_col_ranges_1, expected_row_ranges_1] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(5, 10),
                    std::array<int8_t, 5>{-3, 6, 7, 8, 9},
                    std::array<unsigned, 5>{30, 6, 7, 8, 9},
                    std::array<bool, 5>{0, 0, 1, 0, 1},
                    std::array<float, 5>{-12, 6, 7, 8, 9},
                    std::array<timestamp, 5>{-13, 6, 7, 8, 9}
            );
    ASSERT_EQ(*(*proc_1.segments_)[0], expected_segments_1[0]);
    ASSERT_EQ(*(*proc_1.segments_)[1], expected_segments_1[1]);
    ASSERT_EQ(*(*proc_1.row_ranges_)[0], RowRange(5, 10));
    ASSERT_EQ(*(*proc_1.row_ranges_)[1], RowRange(5, 10));
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], ColRange(4, 6));
}

TEST_F(MergeUpdateClauseUpdateStrategyMatchSubsetTest, MatchSecondAndThird) {
    auto [input_frame, input_frame_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 2>{7, 12},
            std::array<int8_t, 2>{-2, -3},
            std::array<uint32_t, 2>{20, 30},
            std::array{true, true},
            std::array{-11.f, -12.f},
            std::array<timestamp, 2>{-12, -13}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    ASSERT_EQ(ranges_and_keys_.size(), 4);
    ASSERT_EQ(structure_indices.size(), 2);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(structure_indices[1].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].col_range(), ColRange(4, 6));
    std::vector<EntityId> entities = push_entities();
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(structured_entities[0]));
    auto proc_0 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_0
            );
    auto [expected_segments_0, expected_col_ranges_0, expected_row_ranges_0] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(5, 10),
                    std::array<int8_t, 5>{5, 6, -2, 8, 9},
                    std::array<unsigned, 5>{5, 6, 20, 8, 9},
                    std::array<bool, 5>{1, 0, 1, 0, 1},
                    std::array<float, 5>{5, 6, -11, 8, 9},
                    std::array<timestamp, 5>{5, 6, -12, 8, 9}
            );
    ASSERT_EQ(*(*proc_0.segments_)[0], expected_segments_0[0]);
    ASSERT_EQ(*(*proc_0.segments_)[1], expected_segments_0[1]);
    ASSERT_EQ(*(*proc_0.row_ranges_)[0], RowRange(5, 10));
    ASSERT_EQ(*(*proc_0.row_ranges_)[1], RowRange(5, 10));
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], ColRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(structured_entities[1]));
    auto proc_1 =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager_, result_entities_1
            );
    auto [expected_segments_1, expected_col_ranges_1, expected_row_ranges_1] =
            slice_data_into_segments<TimeseriesIndex>(
                    descriptor_,
                    rows_per_segment_,
                    cols_per_segment_,
                    iota_view<timestamp, timestamp>(10, 14),
                    std::array<int8_t, 4>{10, 11, -3, 13},
                    std::array<unsigned, 4>{10, 11, 30, 13},
                    std::array<bool, 4>{0, 1, 1, 1},
                    std::array<float, 4>{10, 11, -12, 13},
                    std::array<timestamp, 4>{10, 11, -13, 13}
            );
    ASSERT_EQ(*(*proc_1.segments_)[0], expected_segments_1[0]);
    ASSERT_EQ(*(*proc_1.segments_)[1], expected_segments_1[1]);
    ASSERT_EQ(*(*proc_1.row_ranges_)[0], RowRange(10, 14));
    ASSERT_EQ(*(*proc_1.row_ranges_)[1], RowRange(10, 14));
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], ColRange(4, 6));
}

TEST(IndexValueSpansMultipleSegments, SegmetStartsWithTheSameValueAsAnotherEnds) {
    constexpr static std::array fields{
            FieldRef({DataType::INT32, Dimension::Dim0}, "a"), FieldRef({DataType::INT32, Dimension::Dim0}, "b")
    };
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    constexpr static MergeStrategy strategy = update_only_strategy;
    constexpr static size_t rows_per_segment = 3;
    constexpr static size_t cols_per_segment = 1;
    auto [target_segments, target_column_ranges, target_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, 6>{1, 2, 2, 2, 2, 3},
            iota_view{0, 6},
            iota_view{0, 6}
    );
    sort_by_rowslice(target_row_ranges, target_column_ranges, target_segments);

    std::vector<RangesAndKey> ranges_and_keys =
            generate_ranges_and_keys(desc, target_segments, target_column_ranges, target_row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();
    auto [input_frame, source_data] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, 3>{1, 2, 3}, std::array{100, 200, 300}, std::array{100, 200, 300}
    );
    MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 2);
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            ranges_and_keys,
            std::move(target_segments),
            std::move(target_column_ranges),
            std::move(target_row_ranges)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    auto [expected_segments, expected_col_slices, expected_row_slices] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, 6>{1, 2, 2, 2, 2, 3},
            std::array{100, 200, 200, 200, 200, 300},
            std::array{100, 200, 200, 200, 200, 300}
    );
    sort_by_rowslice(expected_row_slices, expected_col_slices, expected_segments);
    for (size_t row_slice = 0; row_slice < structured_entities.size(); ++row_slice) {
        const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[row_slice]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, result_entities
                );
        constexpr static size_t column_slices = 2;
        ASSERT_EQ(proc.segments_->size(), column_slices);
        SCOPED_TRACE(testing::Message() << fmt::format("Row slice: {}", row_slice));
        for (size_t col_slice = 0; col_slice < proc.segments_->size(); ++col_slice) {
            SCOPED_TRACE(testing::Message() << fmt::format("Col slice: {}", col_slice));
            ASSERT_EQ(*proc.segments_->at(col_slice), expected_segments[col_slice + column_slices * row_slice]);
            ASSERT_EQ(*proc.col_ranges_->at(col_slice), expected_col_slices[col_slice + column_slices * row_slice]);
            ASSERT_EQ(*proc.row_ranges_->at(col_slice), expected_row_slices[col_slice + column_slices * row_slice]);
        }
    }
}

TEST(IndexValueSpansMultipleSegments, MultipleSegmentsConsistedOfTheSameValue) {
    for (timestamp source_index = 1; source_index < 4; ++source_index) {
        constexpr static std::array fields{
                FieldRef({DataType::INT32, Dimension::Dim0}, "a"), FieldRef({DataType::INT32, Dimension::Dim0}, "b")
        };
        constexpr static MergeStrategy strategy = update_only_strategy;
        const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
        auto [target_segments, target_column_ranges, target_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
                desc, 2, 1, std::array<timestamp, 6>{2, 2, 2, 2, 2, 2}, iota_view{0, 6}, iota_view{0, 6}
        );
        sort_by_rowslice(target_row_ranges, target_column_ranges, target_segments);
        std::vector<RangesAndKey> ranges_and_keys =
                generate_ranges_and_keys(desc, target_segments, target_column_ranges, target_row_ranges);
        auto component_manager = std::make_shared<ComponentManager>();
        auto [input_frame, _] = input_frame_from_tensors<TimeseriesIndex>(
                desc, std::array{source_index}, std::array{100}, std::array{200}
        );
        MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame));
        const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
        if (source_index != 2) {
            ASSERT_EQ(structure_indices.size(), 0);
        } else {
            ASSERT_EQ(structure_indices.size(), 3);
            std::vector<EntityId> entities = push_selected_entities(
                    *component_manager,
                    ranges_and_keys,
                    std::move(target_segments),
                    std::move(target_column_ranges),
                    std::move(target_row_ranges)
            );
            std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
            ASSERT_EQ(structured_entities.size(), 3);
            auto [expected_segments, expected_col_slices, expected_row_slices] =
                    slice_data_into_segments<TimeseriesIndex>(
                            desc,
                            2,
                            1,
                            std::array<timestamp, 6>{2, 2, 2, 2, 2, 2},
                            iota_view{0, 6} | views::transform([](auto) { return 100; }),
                            iota_view{0, 6} | views::transform([](auto) { return 200; })
                    );
            sort_by_rowslice(expected_row_slices, expected_col_slices, expected_segments);
            for (size_t row_slice = 0; row_slice < structured_entities.size(); ++row_slice) {
                const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[row_slice]));
                auto proc = gather_entities<
                        std::shared_ptr<SegmentInMemory>,
                        std::shared_ptr<RowRange>,
                        std::shared_ptr<ColRange>>(*component_manager, result_entities);
                constexpr static size_t column_slices = 2;
                ASSERT_EQ(proc.segments_->size(), column_slices);
                SCOPED_TRACE(testing::Message() << fmt::format("Row slice: {}", row_slice));
                for (size_t col_slice = 0; col_slice < proc.segments_->size(); ++col_slice) {
                    SCOPED_TRACE(testing::Message() << fmt::format("Col slice: {}", col_slice));
                    ASSERT_EQ(*proc.segments_->at(col_slice), expected_segments[col_slice + column_slices * row_slice]);
                    ASSERT_EQ(
                            *proc.col_ranges_->at(col_slice), expected_col_slices[col_slice + column_slices * row_slice]
                    );
                    ASSERT_EQ(
                            *proc.row_ranges_->at(col_slice), expected_row_slices[col_slice + column_slices * row_slice]
                    );
                }
            }
        }
    }
}