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

namespace {
constexpr MergeStrategy update_only_strategy{.matched = MergeAction::UPDATE};
constexpr MergeStrategy update_and_insert_strategy{
        .matched = MergeAction::UPDATE,
        .not_matched_by_target = MergeAction::INSERT
};

constexpr std::array non_string_fields = {
        FieldRef(TypeDescriptor(DataType::INT8, Dimension::Dim0), "int8"),
        FieldRef(TypeDescriptor(DataType::UINT32, Dimension::Dim0), "uint32"),
        FieldRef(TypeDescriptor(DataType::BOOL8, Dimension::Dim0), "bool8"),
        FieldRef(TypeDescriptor(DataType::FLOAT32, Dimension::Dim0), "float32"),
        FieldRef(TypeDescriptor(DataType::NANOSECONDS_UTC64, Dimension::Dim0), "timestamp")
};

StreamDescriptor non_string_fields_ts_index_descriptor() {
    return TimeseriesIndex::default_index().create_stream_descriptor("Source", non_string_fields);
}

StreamDescriptor non_string_fields_rowcount_index_descriptor() {
    return RowCountIndex::default_index().create_stream_descriptor("Source", non_string_fields);
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
        const auto [start_ts, end_ts] = [&]() -> std::pair<timestamp, timestamp> {
            if (source_descriptor.index().type() == IndexDescriptor::Type::ROWCOUNT) {
                return {0, 0};
            }
            return {*segments[i].scalar_at<timestamp>(0, 0),
                    *segments[i].scalar_at<timestamp>(segments[i].row_count() - 1, 0)};
        }();
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
        const MergeStrategy strategy, std::shared_ptr<ComponentManager> component_manager, InputFrame&& input_frame,
        std::vector<std::string> on = {}
) {
    MergeUpdateClause clause(std::move(on), strategy, std::make_shared<InputFrame>(std::move(input_frame)));
    clause.set_component_manager(std::move(component_manager));
    return clause;
}

std::vector<EntityId> push_selected_entities(
        ComponentManager& component_manager, const std::span<const std::vector<size_t>> structure_indices,
        std::vector<SegmentInMemory>&& segments_in, std::vector<ColRange>&& col_ranges_in,
        std::vector<RowRange>&& row_ranges_in, std::vector<RangesAndKey>&& ranges_and_keys
) {
    std::vector<std::shared_ptr<SegmentInMemory>> segments;
    std::vector<std::shared_ptr<ColRange>> col_ranges;
    std::vector<std::shared_ptr<RowRange>> row_ranges;
    std::vector<std::shared_ptr<AtomKey>> atom_keys;

    util::BitSet used(segments_in.size());
    for (const size_t i : structure_indices | std::views::join) {
        if (used[i]) {
            continue;
        }
        used[i] = true;
        RangesAndKey& range_and_key = ranges_and_keys[i];
        size_t segment_to_add = 0;
        while (segment_to_add < segments_in.size() && (col_ranges_in[segment_to_add] != range_and_key.col_range() ||
                                                       row_ranges_in[segment_to_add] != range_and_key.row_range())) {
            ++segment_to_add;
        }
        util::check(
                segment_to_add < segments_in.size(),
                "Could not find segment for row range {} and col range {}",
                range_and_key.row_range(),
                range_and_key.col_range()
        );
        segments.push_back(std::make_shared<SegmentInMemory>(std::move(segments_in[segment_to_add])));
        col_ranges.push_back(std::make_shared<ColRange>(range_and_key.col_range()));
        row_ranges.push_back(std::make_shared<RowRange>(range_and_key.row_range()));
        atom_keys.push_back(std::make_shared<AtomKey>(std::move(range_and_key.key_)));
    }
    const size_t num_entities = used.count();
    std::vector<EntityFetchCount> fetch_counts = *generate_segment_fetch_counts(structure_indices, num_entities);
    return component_manager.add_entities(
            std::move(segments),
            std::move(col_ranges),
            std::move(row_ranges),
            std::move(atom_keys),
            std::move(fetch_counts)
    );
}

std::vector<SegmentInMemory> clone_segments(std::span<const SegmentInMemory> segments) {
    std::vector<SegmentInMemory> result;
    result.reserve(segments.size());
    std::ranges::transform(segments, std::back_inserter(result), [](const auto& segment) { return segment.clone(); });
    return result;
}

} // namespace

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

    MergeUpdateClause create_clause(InputFrame&& input_frame, std::vector<std::string> on = {}) const {
        return ::create_clause(strategy_, component_manager_, std::move(input_frame), std::move(on));
    }

    std::vector<EntityId> push_entities(const std::span<const std::vector<size_t>> structure_indices) {
        return push_selected_entities(
                *component_manager_,
                structure_indices,
                std::move(segments_),
                std::move(col_ranges_),
                std::move(row_ranges_),
                std::move(ranges_and_keys_)
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
            iota_view<timestamp, timestamp>(0, 15),
            std::array<int8_t, 15>{10, 1, 2, 3, 4, 5, 6, 20, 8, 9, 10, 11, 12, 13, 30},
            std::array<unsigned, 15>{100, 1, 2, 3, 4, 5, 6, 200, 8, 9, 10, 11, 12, 13, 300},
            std::array{true, true, false, true, false, true, false, false, false, true, false, true, false, true, true},
            std::array<float, 15>{11.1f, 1, 2, 3, 4, 5, 6, 22.2f, 8, 9, 10, 11, 12, 13, 33.3f},
            std::array<timestamp, 15>{1000, 1, 2, 3, 4, 5, 6, 2000, 8, 9, 10, 11, 12, 13, 3000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segments);
    assert_process_results_match_expected(
            clause, expected_segments, structure_indices, push_entities(structure_indices)
    );
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
            iota_view<timestamp, timestamp>(0, 15),
            std::array<int8_t, 15>{10, 1, 2, 3, 4, 5, 6, 20, 8, 9, 10, 11, 12, 13, 30},
            std::array<unsigned, 15>{100, 1, 2, 3, 4, 5, 6, 200, 8, 9, 10, 11, 12, 13, 300},
            std::array{true, true, false, true, false, true, false, false, false, true, false, true, false, true, true},
            std::array<float, 15>{11.1f, 1, 2, 3, 4, 5, 6, 22.2f, 8, 9, 10, 11, 12, 13, 33.3f},
            std::array<timestamp, 15>{1000, 1, 2, 3, 4, 5, 6, 2000, 8, 9, 10, 11, 12, 13, 3000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segments);
    assert_process_results_match_expected(
            clause, expected_segments, structure_indices, push_entities(structure_indices)
    );
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
    std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities.front().size(), 2);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    ASSERT_EQ(result_entities.size(), 2);
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result_entities
    );
    auto [expected_segments, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            iota_view<timestamp, timestamp>(0, 5),
            std::array<int8_t, 5>{0, 1, 2, -2, 4},
            std::array<unsigned, 5>{0, 1, 2, 20, 4},
            std::array<bool, 5>{false, true, false, false, false},
            std::array<float, 5>{0.f, 1, 2, -11.f, 4},
            std::array<timestamp, 5>{0, 1, 2, -12, 4}
    );
    ASSERT_EQ(proc.segments_->size(), 2);
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
    std::vector<EntityId> entities = push_entities(structure_indices);
    ASSERT_EQ(entities.size(), 2);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), 2);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    ASSERT_EQ(result_entities.size(), 2);
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
            std::array<bool, 5>{true, true, true, false, true},
            std::array<float, 5>{5, -11.f, 7, 8, 9},
            std::array<timestamp, 5>{5, -12, 7, 8, 9}
    );
    ASSERT_EQ(proc.segments_->size(), 2);
    ASSERT_EQ(proc.row_ranges_->size(), 2);
    ASSERT_EQ(proc.col_ranges_->size(), 2);
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

    std::vector<EntityId> entities = push_entities(structure_indices);
    ASSERT_EQ(entities.size(), 2);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities.front().size(), 2);
    const std::vector<EntityId> result_entities = clause.process(std::move(structured_entities[0]));
    ASSERT_EQ(result_entities.size(), 2);
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
            std::array<bool, 4>{true, true, false, true},
            std::array<float, 4>{-11.f, 11, 12, 13},
            std::array<timestamp, 4>{-12, 11, 12, 13}
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
    std::vector<EntityId> entities = push_entities(structure_indices);
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
    std::vector<EntityId> entities = push_entities(structure_indices);
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
    std::vector<EntityId> entities = push_entities(structure_indices);
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

// ===========================================================================
// Tests for the on_ parameter of MergeUpdateClause
// ===========================================================================

/// Target: 10 rows, ts=[0..9], int8=[0..9], uint32=[0..9], bool8=[all zeros],
/// float32=[0.f..9.f], timestamp col=[0..9].
/// 2 row slices of 5 rows each; 2 col slices per row slice:
///   col slice 0 → ColRange(1, 4) covering "int8", "uint32", "bool8"
///   col slice 1 → ColRange(4, 6) covering "float32", "timestamp"
struct MergeUpdateClauseOnParameterTest : MergeUpdateClauseUpdateStrategyTestBase, testing::Test {
    MergeUpdateClauseOnParameterTest() :
        MergeUpdateClauseUpdateStrategyTestBase(
                non_string_fields_ts_index_descriptor(), update_only_strategy,
                slice_data_into_segments<TimeseriesIndex>(
                        non_string_fields_ts_index_descriptor(), rows_per_segment_, cols_per_segment_,
                        std::array<timestamp, num_rows_>{0, 1, 1, 1, 3, 3, 3, 3, 4, 5},
                        iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows_)),
                        iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows_)),
                        iota_view(0, num_rows_) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
                        iota_view(0, num_rows_) | views::transform([](auto x) { return static_cast<float>(x); }),
                        iota_view(timestamp{0}, timestamp{num_rows_})
                )
        ) {
        constexpr static size_t rand_seed = 0;
        std::mt19937 g(rand_seed);
        shuffle(ranges_and_keys_, g);
    }

    constexpr static int num_rows_ = 10;
    constexpr static int rows_per_segment_ = 5;
    constexpr static int cols_per_segment_ = 3;

    [[nodiscard]] size_t column_slices_per_row_slice() const {
        return (descriptor_.field_count() + cols_per_segment_ - 1) / cols_per_segment_;
    }
};

/// The source index matches ts=3 in the first segment, but "int8"=99 != target int8=3.
TEST_F(MergeUpdateClauseOnParameterTest, OneOnColumn_IndexMatchesButColumnDiffers) {
    auto [input_frame, source_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array{timestamp{1}},
            std::array<int8_t, 1>{99},
            std::array{0u},
            std::array{false},
            std::array{0.f},
            std::array{timestamp{0}}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    // The first row range is matched. It contains two column slices.
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 1);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(ranges_and_keys_.size(), segments_to_process);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));

    std::vector<EntityId> entities = push_entities(structure_indices);
    auto structured = structure_entities(structure_indices, entities);
    const std::vector<EntityId> result = clause.process(std::move(structured[0]));
    ASSERT_TRUE(result.empty());
}

/// Both the index and the single on_ column ("int8") match at ts=3.
/// "uint32", "bool8", "float32", "timestamp" (not in on_) are overwritten from source;
TEST_F(MergeUpdateClauseOnParameterTest, OneOnColumn_BothIndexAndColumnMatch) {
    auto [input_frame, source_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 1>{0},
            std::array<int8_t, 1>{0}, // int8=3 matches target int8=3 at ts=3
            std::array{99u},
            std::array{true},
            std::array{99.f},
            std::array{timestamp{999}}
    );

    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);

    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(structure_indices.size(), row_slices_to_process);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(ranges_and_keys_.size(), segments_to_process);

    std::vector<EntityId> entities = push_entities(structure_indices);
    auto structured = structure_entities(structure_indices, entities);
    const auto result = clause.process(std::move(structured[0]));
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result
    );

    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            std::array<timestamp, rows_per_segment_>{0, 1, 1, 1, 3},
            std::array<int8_t, rows_per_segment_>{0, 1, 2, 3, 4},
            std::array<uint32_t, rows_per_segment_>{99, 1, 2, 3, 4},
            std::array<bool, rows_per_segment_>{true, true, false, true, false},
            std::array<float, rows_per_segment_>{99.f, 1.f, 2.f, 3.f, 4.f},
            std::array<timestamp, rows_per_segment_>{999, 1, 2, 3, 4}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);

    ASSERT_EQ(proc.segments_->size(), 2);
    ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
    ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
    ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
    ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
}

/// ts=1 is repeated 3 times. Both columns match on the third repetition but differ on the previous. Only the one where
/// all columns are matching is updated
TEST_F(MergeUpdateClauseOnParameterTest, TwoOnColumns_BothMatch) {
    auto [input_frame, source_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 3>{1, 1, 1},
            std::array<int8_t, 3>{1, 100, 3},
            std::array<unsigned, 3>{100, 2, 3},
            std::array{false, false, false},
            std::array{1000.f, 1001.f, 1002.f},
            std::array<timestamp, 3>{100, 101, 102}
    );

    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8", "uint32"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 1);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 2);

    std::vector<EntityId> entities = push_entities(structure_indices);
    ASSERT_EQ(entities.size(), 2);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured.size(), 1);
    ASSERT_EQ(structured.front().size(), 2);
    const auto result = clause.process(std::move(structured[0]));
    ASSERT_EQ(result.size(), 2);
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, result
    );

    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            descriptor_,
            rows_per_segment_,
            cols_per_segment_,
            std::array<timestamp, rows_per_segment_>{0, 1, 1, 1, 3},
            std::array<int8_t, rows_per_segment_>{0, 1, 2, 3, 4},
            std::array<uint32_t, rows_per_segment_>{0, 1, 2, 3, 4},
            std::array<bool, rows_per_segment_>{false, true, false, false, false},
            std::array<float, rows_per_segment_>{0, 1.f, 2.f, 1002.f, 4.f},
            std::array<timestamp, rows_per_segment_>{0, 1, 2, 102, 4}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);

    ASSERT_EQ(proc.segments_->size(), 2);
    ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
    ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
    ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
    ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
}

/// Source has two rows whose timestamps and "int8" values match entries in different row segments: ts=3 in [0,5) and
/// ts=7 in [5,10). Both segments are selected by structure_for_processing and each is independently updated by process.
TEST_F(MergeUpdateClauseOnParameterTest, OneOnColumn_SourceSpansBothRowSegments) {
    auto [input_frame, source_data] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor_,
            std::array<timestamp, 3>{3, 3, 3},
            std::array<int8_t, 3>{4, 100, 6},
            std::array<unsigned, 3>{100, 200, 300},
            std::array{true, false, true},
            std::array{100.f, 200.f, 300.f},
            std::array<timestamp, 3>{1000, 2000, 3000}
    );

    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 2);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 4);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].col_range(), ColRange(4, 6));

    std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);

    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<timestamp, num_rows_>{0, 1, 1, 1, 3, 3, 3, 3, 4, 5},
            std::array<int8_t, num_rows_>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            std::array<unsigned, num_rows_>{0, 1, 2, 3, 100, 5, 300, 7, 8, 9},
            std::array<bool, num_rows_>{false, true, false, true, true, true, true, true, false, true},
            std::array<float, num_rows_>{0, 1, 2, 3, 100, 5, 300, 7, 8, 9},
            std::array<timestamp, num_rows_>{0, 1, 2, 3, 1000, 5, 3000, 7, 8, 9}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const auto result = clause.process(std::move(structured[0]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );

        ASSERT_EQ(proc.segments_->size(), 2);

        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
    }

    {
        const auto result = clause.process(std::move(structured[1]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );

        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[2]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[3]);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(5, 10));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(5, 10));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(1, 4));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(4, 6));
    }
}

struct MergeUpdateClauseUpdateStrategyRowRange : MergeUpdateClauseUpdateStrategyTestBase, testing::Test {
    MergeUpdateClauseUpdateStrategyRowRange() :
        MergeUpdateClauseUpdateStrategyTestBase(
                non_string_fields_rowcount_index_descriptor(), update_only_strategy,
                slice_data_into_segments<RowCountIndex>(
                        non_string_fields_rowcount_index_descriptor(), rows_per_segment_, cols_per_segment_,
                        std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 0},
                        iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows_)),
                        iota_view(0, num_rows_) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
                        iota_view(0, num_rows_) | views::transform([](auto x) {
                            return x == num_rows_ - 1 ? std::numeric_limits<float>::quiet_NaN() : static_cast<float>(x);
                        }),
                        iota_view(timestamp{0}, timestamp{num_rows_})
                )
        ) {
        // Shuffle the input ranges and keys to ensure structure_for_processing sorts correctly
        constexpr static size_t rand_seed = 0;
        std::mt19937 g(rand_seed);
        shuffle(ranges_and_keys_, g);
    }
    [[nodiscard]] size_t column_slices_per_row_slice() const {
        return (descriptor_.field_count() + cols_per_segment_ - 1) / cols_per_segment_;
    }

    void assert_structure_for_processing_result(std::span<const std::vector<size_t>> structure_indices) const {
        ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
        ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
        ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].row_range(), RowRange(5, 10));
        ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].row_range(), RowRange(5, 10));
        ASSERT_EQ(ranges_and_keys_[structure_indices[2][0]].row_range(), RowRange(10, 14));
        ASSERT_EQ(ranges_and_keys_[structure_indices[2][1]].row_range(), RowRange(10, 14));
        ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(0, 3));
        ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(3, 5));
        ASSERT_EQ(ranges_and_keys_[structure_indices[1][0]].col_range(), ColRange(0, 3));
        ASSERT_EQ(ranges_and_keys_[structure_indices[1][1]].col_range(), ColRange(3, 5));
        ASSERT_EQ(ranges_and_keys_[structure_indices[2][0]].col_range(), ColRange(0, 3));
        ASSERT_EQ(ranges_and_keys_[structure_indices[2][1]].col_range(), ColRange(3, 5));
    }

    constexpr static int num_rows_ = 14;
    constexpr static int num_cols_ = non_string_fields.size();
    constexpr static int rows_per_segment_ = 5;
    constexpr static int cols_per_segment_ = 3;
};

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, RequireNonEmptyOn) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 1>{99},
            std::array{0u},
            std::array{false},
            std::array{0.f},
            std::array{timestamp{0}}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    std::vector<EntityId> entities = push_entities(structure_indices);
    auto structured = structure_entities(structure_indices, entities);
    ASSERT_THROW((void)clause.process(std::move(structured[0])), UserInputException);
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, NonExistingOnThrows) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 2>{12, 9},
            std::array{100u, 101U},
            std::array{true, false},
            std::array{1000.f, 1001.f},
            std::array<timestamp, 2>{2000, 2001}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"nonexisting"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);
    const std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    ASSERT_THROW((void)clause.process(std::move(structured[0])), UserInputException);
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, MatchOneColumn_Segment1) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 2>{12, 9},
            std::array{100u, 101U},
            std::array{true, false},
            std::array{1000.f, 1001.f},
            std::array<timestamp, 2>{2000, 2001}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);

    const std::vector<EntityId> entities = push_entities(structure_indices);
    ASSERT_EQ(entities.size(), segments_to_process);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured.size(), 3);
    ASSERT_TRUE(std::ranges::all_of(structured, [](const auto& vec) { return vec.size() == 2; }));
    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<RowCountIndex>(
            non_string_fields_rowcount_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 0},
            std::array<unsigned, num_rows_>{101, 1, 100, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
            std::array{false, true, true, true, false, true, false, true, false, true, false, true, false, true},
            std::array{
                    1001.f,
                    1.f,
                    1000.f,
                    3.f,
                    4.f,
                    5.f,
                    6.f,
                    7.f,
                    8.f,
                    9.f,
                    10.f,
                    11.f,
                    12.f,
                    std::numeric_limits<float>::quiet_NaN()
            },
            std::array<timestamp, num_rows_>{2001, 1, 2000, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const auto result = clause.process(std::move(structured[0]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[1]));
        ASSERT_TRUE(result.empty());
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[2]));
        ASSERT_TRUE(result.empty());
    }
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, MatchOneColumn_ValueInSourceMatchesMultipleRowsAcrossSegments) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 1>{33},
            std::array{100u},
            std::array{true},
            std::array{1000.f},
            std::array<timestamp, 1>{2000}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);

    const std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<RowCountIndex>(
            non_string_fields_rowcount_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 0},
            std::array<unsigned, num_rows_>{0, 1, 2, 100, 100, 100, 100, 100, 100, 100, 100, 11, 12, 13},
            std::array<bool, num_rows_>{
                    false, true, false, true, true, true, true, true, true, true, true, true, false, true
            },
            std::array<float, num_rows_>{
                    0.f,
                    1.,
                    2.f,
                    1000.,
                    1000.,
                    1000.,
                    1000.,
                    1000.,
                    1000.,
                    1000.,
                    1000.,
                    11.0,
                    12.0,
                    std::numeric_limits<float>::quiet_NaN()
            },
            std::array<timestamp, num_rows_>{0, 1, 2, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 11, 12, 13}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const auto result = clause.process(std::move(structured[0]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    }
    {
        const auto result = clause.process(std::move(structured[1]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(5, 10));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(5, 10));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[2]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[3]);
    }
    {
        const auto result = clause.process(std::move(structured[2]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(10, 14));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(10, 14));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[4]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[5]);
    }
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, MatchOneColumn_Segment1_Segment3) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 2>{-9, 12},
            std::array{100u, 101U},
            std::array{true, true},
            std::array{1000.f, 1001.f},
            std::array<timestamp, 2>{2000, 2001}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);

    const std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<RowCountIndex>(
            non_string_fields_rowcount_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 0},
            std::array<unsigned, num_rows_>{0, 1, 101, 3, 4, 5, 6, 7, 8, 9, 10, 11, 100, 13},
            std::array<bool, num_rows_>{
                    false, true, true, true, false, true, false, true, false, true, false, true, true, true
            },
            std::array<float, num_rows_>{
                    0, 1., 1001.f, 3., 4., 5., 6., 7., 8., 9., 10., 11., 1000., std::numeric_limits<float>::quiet_NaN()
            },
            std::array<timestamp, num_rows_>{0, 1, 2001, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2000, 13}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[0]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[1]));
        ASSERT_TRUE(result.empty());
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[2]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(10, 14));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(10, 14));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[4]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[5]);
    }
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, MatchNaN) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 1>{100},
            std::array{200u},
            std::array{false},
            std::array{std::numeric_limits<float>::quiet_NaN()},
            std::array<timestamp, 1>{2000}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"float32"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);

    const std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<RowCountIndex>(
            non_string_fields_rowcount_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 100},
            std::array<unsigned, num_rows_>{0, 1, 100, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 200},
            std::array{false, true, false, true, false, true, false, true, false, true, false, true, false, false},
            std::array<float, num_rows_>{
                    0., 1., 2., 3., 4., 5., 6., 7., 8., 9., 10., 11., 12., std::numeric_limits<float>::quiet_NaN()
            },
            std::array<timestamp, num_rows_>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 2000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[0]));
        ASSERT_TRUE(result.empty());
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[1]));
        ASSERT_TRUE(result.empty());
    }
    {
        const auto result = clause.process(std::move(structured[2]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(10, 14));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(10, 14));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[4]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[5]);
    }
}

TEST_F(MergeUpdateClauseUpdateStrategyRowRange, MergeOnTwoColumns_Segment1_Segment2) {
    auto [input_frame, source_data] = input_frame_from_tensors<RowCountIndex>(
            descriptor_,
            std::array<int8_t, 2>{33, 33},
            std::array{3u, 7u},
            std::array{false, false},
            std::array{1000.f, 1001.f},
            std::array<timestamp, 2>{2000, 2001}
    );
    MergeUpdateClause clause = create_clause(std::move(input_frame), {"int8", "uint32"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys_);
    const size_t row_slices_to_process = structure_indices.size();
    ASSERT_EQ(row_slices_to_process, 3);
    const size_t segments_to_process = row_slices_to_process * column_slices_per_row_slice();
    ASSERT_EQ(segments_to_process, 6);
    assert_structure_for_processing_result(structure_indices);

    const std::vector<EntityId> entities = push_entities(structure_indices);
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    auto [expected_segs, expected_col_ranges, expected_row_ranges] = slice_data_into_segments<RowCountIndex>(
            non_string_fields_rowcount_index_descriptor(),
            rows_per_segment_,
            cols_per_segment_,
            std::array<int8_t, num_rows_>{9, 2, 12, 33, 33, 33, 33, 33, 33, 33, 33, 6, -9, 100},
            std::array<unsigned, num_rows_>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 5},
            std::array{false, true, false, false, false, true, false, false, false, true, false, true, false, true},
            std::array<float, num_rows_>{
                    0., 1., 2., 1000., 4., 5., 6., 1001., 8., 9., 10., 11., 12., std::numeric_limits<float>::quiet_NaN()
            },
            std::array<timestamp, num_rows_>{0, 1, 2, 2000, 4, 5, 6, 2001, 8, 9, 10, 11, 12, 13}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segs);
    {
        const auto result = clause.process(std::move(structured[0]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(0, 5));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(0, 5));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[0]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[1]);
    }
    {
        const auto result = clause.process(std::move(structured[1]));
        auto proc =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager_, result
                );
        ASSERT_EQ(proc.segments_->size(), 2);
        ASSERT_EQ(*(*proc.row_ranges_)[0], RowRange(5, 10));
        ASSERT_EQ(*(*proc.row_ranges_)[1], RowRange(5, 10));
        ASSERT_EQ(*(*proc.col_ranges_)[0], ColRange(0, 3));
        ASSERT_EQ(*(*proc.col_ranges_)[1], ColRange(3, 5));
        ASSERT_EQ(*(*proc.segments_)[0], expected_segs[2]);
        ASSERT_EQ(*(*proc.segments_)[1], expected_segs[3]);
    }
    {
        const std::vector<EntityId> result = clause.process(std::move(structured[2]));
        ASSERT_TRUE(result.empty());
    }
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
            structure_indices,
            std::move(target_segments),
            std::move(target_column_ranges),
            std::move(target_row_ranges),
            std::move(ranges_and_keys)
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
                    structure_indices,
                    std::move(target_segments),
                    std::move(target_column_ranges),
                    std::move(target_row_ranges),
                    std::move(ranges_and_keys)
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

TEST(MergeClauseDateRange, SourceMatchesAllIndexRangesButDoesNotMatchAnyRow) {
    constexpr static size_t rows_per_segment = 2;
    constexpr static size_t cols_per_segment = 3;
    constexpr static int num_rows = 6;
    const StreamDescriptor& descriptor = non_string_fields_ts_index_descriptor();
    auto [segments, cols, rows] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, num_rows>{1, 10, 11, 20, 21, 30},
            iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows)),
            iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows)),
            iota_view(0, num_rows) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
            iota_view(0, num_rows) | views::transform([](auto x) { return static_cast<float>(x); }),
            iota_view(timestamp{0}, timestamp{num_rows})
    );
    sort_by_rowslice(rows, cols, segments);
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(descriptor, segments, cols, rows);

    auto [input_frame, input_frame_data_owner] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor,
            std::array<timestamp, 3>{5, 12, 27},
            std::array<int8_t, 3>{100, 101, 102},
            std::array<unsigned, 3>{100, 101, 102},
            std::array<bool, 3>{true, false, true},
            std::array<float, 3>{100.f, 101.f, 102.f},
            std::array<timestamp, 3>{2000, 3000, 4000}
    );
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(update_only_strategy, component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 3);
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            std::move(segments),
            std::move(cols),
            std::move(rows),
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 3);
    for (std::vector<EntityId>& structured_entity : structured_entities) {
        const std::vector<EntityId> result = clause.process(std::move(structured_entity));
        ASSERT_TRUE(result.empty());
    }
}

struct MergeUpdateClauseInsertAndUpdate : testing::TestWithParam<MergeStrategy> {};

TEST_P(MergeUpdateClauseInsertAndUpdate, InsertBeforeFirstRow) {
    constexpr static int num_rows = 15;
    constexpr static int num_cols = non_string_fields.size();
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 3;
    constexpr static int col_slice_count = (num_cols + cols_per_segment - 1) / cols_per_segment;

    MergeStrategy strategy = GetParam();
    const StreamDescriptor& descriptor = non_string_fields_ts_index_descriptor();

    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            rows_per_segment,
            cols_per_segment,
            iota_view(timestamp{0}, timestamp{num_rows}),
            iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows)),
            iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows)),
            iota_view(0, num_rows) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
            iota_view(0, num_rows) | views::transform([](auto x) { return static_cast<float>(x); }),
            iota_view(timestamp{0}, timestamp{num_rows})
    );

    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(descriptor, segments, col_ranges, row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();

    auto [input_frame, input_frame_data_owner] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor,
            std::array<timestamp, 3>{-10, -5, -4},
            std::array<int8_t, 3>{100, 101, 102},
            std::array<unsigned, 3>{1000, 2000, 3000},
            std::array<bool, 3>{true, false, true},
            std::array<float, 3>{500.f, 600.f, 700.f},
            std::array<timestamp, 3>{111, 222, 333}
    );
    MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    // Inserting in the beginning of the DataFrame picks the first row slice
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), col_slice_count);
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::move(col_ranges),
            std::move(row_ranges),
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), col_slice_count);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), col_slice_count);
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, clause.process(std::move(structured_entities[0]))
    );

    auto [expected_segments, cols, rows] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            100000,
            cols_per_segment,
            std::array<timestamp, 8>{-10, -5, -4, 0, 1, 2, 3, 4},
            std::array<int8_t, 8>{100, 101, 102, 0, 1, 2, 3, 4},
            std::array<unsigned, 8>{1000, 2000, 3000, 0, 1, 2, 3, 4},
            std::array<bool, 8>{true, false, true, false, true, false, true, false},
            std::array<float, 8>{500.f, 600.f, 700.f, 0.0f, 1.f, 2.f, 3.f, 4.f},
            std::array<timestamp, 8>{111, 222, 333, 0, 1, 2, 3, 4}
    );
    for (size_t i = 0; i < expected_segments.size(); ++i) {
        ASSERT_EQ(*proc.segments_->at(i), expected_segments[i]);
    }
}

TEST_P(MergeUpdateClauseInsertAndUpdate, InsertAfterLastRow) {
    constexpr static int num_rows = 15;
    constexpr static int num_cols = non_string_fields.size();
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 3;
    constexpr static int col_slice_count = (num_cols + cols_per_segment - 1) / cols_per_segment;
    constexpr static int row_slice_count = (num_rows + rows_per_segment - 1) / rows_per_segment;

    const StreamDescriptor& descriptor = non_string_fields_ts_index_descriptor();

    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            rows_per_segment,
            cols_per_segment,
            iota_view(timestamp{10}, timestamp{10 + num_rows}),
            iota_view(static_cast<int8_t>(0), static_cast<int8_t>(num_rows)),
            iota_view(static_cast<unsigned>(0), static_cast<unsigned>(num_rows)),
            iota_view(0, num_rows) | views::transform([](auto x) -> bool { return x % 2 == 1; }),
            iota_view(0, num_rows) | views::transform([](auto x) { return static_cast<float>(x); }),
            iota_view(timestamp{0}, timestamp{num_rows})
    );

    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(descriptor, segments, col_ranges, row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();

    auto [input_frame, input_frame_data_owner] = input_frame_from_tensors<TimeseriesIndex>(
            descriptor,
            std::array<timestamp, 3>{100, 101, 102},
            std::array<int8_t, 3>{100, 101, 102},
            std::array<unsigned, 3>{1000, 2000, 3000},
            std::array<bool, 3>{true, false, true},
            std::array<float, 3>{500.f, 600.f, 700.f},
            std::array<timestamp, 3>{111, 222, 333}
    );
    MergeUpdateClause clause = create_clause(GetParam(), component_manager, std::move(input_frame));

    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    // Inserting in the beginning of the DataFrame picks the first row slice
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), col_slice_count);

    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), col_slice_count);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), col_slice_count);
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager, clause.process(std::move(structured_entities[0]))
    );

    auto [new_data_segments, cols, rows] = slice_data_into_segments<TimeseriesIndex>(
            non_string_fields_ts_index_descriptor(),
            100000,
            cols_per_segment,
            std::array<timestamp, 3>{100, 101, 102},
            std::array<int8_t, 3>{100, 101, 102},
            std::array<unsigned, 3>{1000, 2000, 3000},
            std::array<bool, 3>{true, false, true},
            std::array<float, 3>{500.f, 600.f, 700.f},
            std::array<timestamp, 3>{111, 222, 333}
    );
    sort_by_rowslice(row_ranges, col_ranges, segments);
    std::span last_row_slice{segments.begin() + (row_slice_count - 1) * col_slice_count, segments.end()};
    ASSERT_EQ(last_row_slice.size(), new_data_segments.size());
    for (size_t i = 0; i < new_data_segments.size(); ++i) {
        last_row_slice[i].append(new_data_segments[i]);
        ASSERT_EQ(last_row_slice[i], *(*proc.segments_)[i]);
    }
}

TEST_P(MergeUpdateClauseInsertAndUpdate, SourceDataEndsBeforeLastSegment) {
    constexpr static std::array fields{FieldRef({DataType::UINT64, Dimension::Dim0}, "a")};
    constexpr static size_t rows_per_segment = 5;
    constexpr static size_t cols_per_segment = 127;
    constexpr static size_t num_rows = 27;
    constexpr static size_t num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            iota_view{timestamp{0}, timestamp{num_rows}} | views::transform([](const timestamp x) { return x * 2; }),
            iota_view{size_t{0}, num_rows}
    );
    // Will insert in segments 0, 2, 3
    // Segments 1, 4, 5 are not processed
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, 3>{1, 21, 33}, std::array<size_t, 3>{100, 200, 300}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(GetParam(), component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 3);
    ASSERT_TRUE(std::ranges::all_of(structure_indices, [](const auto& indices) { return indices.size() == 1; }));

    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 3);
    ASSERT_TRUE(std::ranges::all_of(structured_entities, [](const auto& entts) { return entts.size() == 1; }));
    constexpr static std::array expected_row_ranges = {RowRange{0, 5}, RowRange{10, 15}, RowRange{15, 20}};
    constexpr static std::array expected_col_ranges = {ColRange{1, 2}, ColRange{1, 2}, ColRange{1, 2}};
    const std::array expected_segments = {
            create_dense_segment(
                    desc, std::array<timestamp, 6>{0, 1, 2, 4, 6, 8}, std::array<size_t, 6>{0, 100, 1, 2, 3, 4}
            ),
            create_dense_segment(
                    desc,
                    std::array<timestamp, 6>{20, 21, 22, 24, 26, 28},
                    std::array<size_t, 6>{10, 200, 11, 12, 13, 14}
            ),
            create_dense_segment(
                    desc,
                    std::array<timestamp, 6>{30, 32, 33, 34, 36, 38},
                    std::array<size_t, 6>{15, 16, 300, 17, 18, 19}
            ),
    };
    std::array<ProcessingUnit, 3> processing_units;
    for (size_t i = 0; i < structured_entities.size(); ++i) {
        processing_units[i] =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[i]))
                );
        ASSERT_EQ(processing_units[i].segments_->size(), 1);
        ASSERT_EQ(processing_units[i].row_ranges_->size(), 1);
        ASSERT_EQ(processing_units[i].col_ranges_->size(), 1);
        ASSERT_EQ(*(processing_units[i].row_ranges_->front()), expected_row_ranges[i]);
        ASSERT_EQ(*(processing_units[i].col_ranges_->front()), expected_col_ranges[i]);
        ASSERT_EQ(*(processing_units[i].segments_->front()), expected_segments[i]);
    }
}

TEST_P(MergeUpdateClauseInsertAndUpdate, SourceDataStartsAfterTheFirstSegment) {
    constexpr static std::array fields{FieldRef({DataType::UINT64, Dimension::Dim0}, "a")};
    constexpr static size_t rows_per_segment = 5;
    constexpr static size_t cols_per_segment = 127;
    constexpr static size_t num_rows = 27;
    constexpr static size_t num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            iota_view{timestamp{0}, timestamp{num_rows}} | views::transform([](const timestamp x) { return x * 2; }),
            iota_view{size_t{0}, num_rows}
    );
    // Will insert in segments 2, 3
    // Segments 0, 1, 4, 5 are not processed
    constexpr static size_t input_frame_rows = 2;
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, input_frame_rows>{21, 33}, std::array<size_t, input_frame_rows>{100, 200}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(GetParam(), component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    constexpr static size_t expected_segments_to_process = 2;
    ASSERT_EQ(structure_indices.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structure_indices, [](const auto& indices) { return indices.size() == 1; }));

    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structured_entities, [](const auto& entts) { return entts.size() == 1; }));
    constexpr static std::array expected_row_ranges = {RowRange{10, 15}, RowRange{15, 20}};
    constexpr static std::array expected_col_ranges = {ColRange{1, 2}, ColRange{1, 2}};
    const std::array expected_segments = {
            create_dense_segment(
                    desc,
                    std::array<timestamp, 6>{20, 21, 22, 24, 26, 28},
                    std::array<size_t, 6>{10, 100, 11, 12, 13, 14}
            ),
            create_dense_segment(
                    desc,
                    std::array<timestamp, 6>{30, 32, 33, 34, 36, 38},
                    std::array<size_t, 6>{15, 16, 200, 17, 18, 19}
            ),
    };
    std::array<ProcessingUnit, expected_segments_to_process> processing_units;
    for (size_t i = 0; i < structured_entities.size(); ++i) {
        processing_units[i] =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[i]))
                );
        ASSERT_EQ(processing_units[i].segments_->size(), 1);
        ASSERT_EQ(processing_units[i].row_ranges_->size(), 1);
        ASSERT_EQ(processing_units[i].col_ranges_->size(), 1);
        ASSERT_EQ(*(processing_units[i].row_ranges_->front()), expected_row_ranges[i]);
        ASSERT_EQ(*(processing_units[i].col_ranges_->front()), expected_col_ranges[i]);
        ASSERT_EQ(*(processing_units[i].segments_->front()), expected_segments[i]);
    }
}

TEST_P(MergeUpdateClauseInsertAndUpdate, SkipExpandedSegmentsInsertAtEnd) {
    constexpr static std::array fields{FieldRef({DataType::UINT64, Dimension::Dim0}, "a")};
    constexpr static size_t rows_per_segment = 5;
    constexpr static size_t cols_per_segment = 127;
    constexpr static size_t num_rows = 27;
    constexpr static size_t num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, num_rows>{0,  1,  2,  3,  4,  10, 11, 12, 13, 14, 20, 21, 22, 23,
                                            24, 30, 31, 32, 33, 34, 40, 41, 42, 43, 44, 50, 51},
            iota_view{size_t{0}, num_rows}
    );
    constexpr static size_t input_frame_rows = 3;
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, input_frame_rows>{35, 47}, std::array<size_t, input_frame_rows>{100, 200}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(GetParam(), component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    constexpr static size_t expected_segments_to_process = 2;
    ASSERT_EQ(structure_indices.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structure_indices, [](const auto& indices) { return indices.size() == 1; }));
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structured_entities, [](const auto& entts) { return entts.size() == 1; }));
    constexpr static std::array expected_row_ranges = {RowRange{15, 20}, RowRange{20, 25}};
    constexpr static std::array expected_col_ranges = {ColRange{1, 2}, ColRange{1, 2}};
    const std::array expected_segments = {
            create_dense_segment(
                    desc,
                    std::array<timestamp, 6>{30, 31, 32, 33, 34, 35},
                    std::array<size_t, 6>{15, 16, 17, 18, 19, 100}
            ),
            create_dense_segment(
                    desc,
                    std::array<timestamp, 7>{40, 41, 42, 43, 44, 47},
                    std::array<size_t, 7>{20, 21, 22, 23, 24, 200}
            ),
    };
    for (size_t i = 0; i < structured_entities.size(); ++i) {
        const ProcessingUnit& processing_unit =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[i]))
                );
        ASSERT_EQ(processing_unit.segments_->size(), 1);
        ASSERT_EQ(processing_unit.row_ranges_->size(), 1);
        ASSERT_EQ(processing_unit.col_ranges_->size(), 1);
        ASSERT_EQ(*(processing_unit.row_ranges_->front()), expected_row_ranges[i]);
        ASSERT_EQ(*(processing_unit.col_ranges_->front()), expected_col_ranges[i]);
        ASSERT_EQ(*(processing_unit.segments_->front()), expected_segments[i]);
    }
}

TEST_P(MergeUpdateClauseInsertAndUpdate, SkipExpandedSegmentsInsertInMiddle) {
    constexpr static std::array fields{FieldRef({DataType::UINT64, Dimension::Dim0}, "a")};
    constexpr static size_t rows_per_segment = 5;
    constexpr static size_t cols_per_segment = 127;
    constexpr static size_t num_rows = 27;
    constexpr static size_t num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            iota_view{timestamp{0}, timestamp{num_rows}} | views::transform([](const timestamp x) { return x * 5; }),
            iota_view{size_t{0}, num_rows}
    );
    constexpr static size_t input_frame_rows = 1;
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, input_frame_rows>{87}, std::array<size_t, input_frame_rows>{100}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    ASSERT_EQ(ranges_and_keys.size(), num_row_slices);
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(GetParam(), component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    constexpr static size_t expected_segments_to_process = 1;
    ASSERT_EQ(structure_indices.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structure_indices, [](const auto& indices) { return indices.size() == 1; }));
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structured_entities, [](const auto& entts) { return entts.size() == 1; }));
    constexpr static RowRange expected_row_range{15, 20};
    constexpr static ColRange expected_col_range{1, 2};
    const SegmentInMemory expected_segment = create_dense_segment(
            desc, std::array<timestamp, 6>{75, 80, 85, 87, 90, 95}, std::array<size_t, 6>{15, 16, 17, 100, 18, 19}
    );
    const ProcessingUnit& processing_unit =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, clause.process(std::move(structured_entities[0]))
            );
    ASSERT_EQ(processing_unit.segments_->size(), 1);
    ASSERT_EQ(processing_unit.row_ranges_->size(), 1);
    ASSERT_EQ(processing_unit.col_ranges_->size(), 1);
    ASSERT_EQ(*(processing_unit.row_ranges_->front()), expected_row_range);
    ASSERT_EQ(*(processing_unit.col_ranges_->front()), expected_col_range);
    ASSERT_EQ(*(processing_unit.segments_->front()), expected_segment);
}

INSTANTIATE_TEST_SUITE_P(
        MergeUpdateClauseInsertAndUpdate, MergeUpdateClauseInsertAndUpdate,
        testing::Values(
                MergeStrategy{.not_matched_by_target = MergeAction::INSERT},
                MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::INSERT}
        )
);

struct MergeUpdateClauseUpdateOffByOne : testing::TestWithParam<std::tuple<MergeStrategy, unsigned, int>> {
    enum { UPDATE_SECOND_TO_LAST = -2, UPDATE_LAST = -1, UPDATE_FIRST = 0, UPDATE_SECOND = 1 };
    static MergeStrategy strategy() { return std::get<0>(GetParam()); }
    static unsigned segment_to_update() { return std::get<1>(GetParam()); }
    static int row_in_segment_to_update() { return std::get<2>(GetParam()); }
};

/// Tests updating at the edges of row slices with all strategies allowing updates.
/// Input data consists of 6 segments. For each row slice test updating the first, second, second to last, and last row.
TEST_P(MergeUpdateClauseUpdateOffByOne, UpdateOffByOne) {
    constexpr static std::array fields{
            FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}, FieldRef{{DataType::INT32, Dimension::Dim0}, "b"}
    };
    constexpr static int index_step = 5;
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 1;
    constexpr static int num_rows = 27;
    constexpr static int num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const int row_within_segment = (rows_per_segment + row_in_segment_to_update()) % rows_per_segment;
    const int row_to_update =
            segment_to_update() * rows_per_segment + (rows_per_segment + row_in_segment_to_update()) % rows_per_segment;
    if (row_to_update >= num_rows) {
        GTEST_SKIP();
    }
    const timestamp index_value_to_update{row_to_update * index_step};
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            iota_view{timestamp{0}, timestamp{num_rows}} |
                    views::transform([](const timestamp x) { return timestamp{x * index_step}; }),
            iota_view{int64_t{0}, int64_t{num_rows}},
            iota_view{0, int{num_rows}}
    );
    ASSERT_EQ(segments.size(), 12);
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array{index_value_to_update}, std::array{int64_t{100}}, std::array{200}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    ASSERT_EQ(ranges_and_keys.size(), 12);
    auto component_manager = std::make_shared<ComponentManager>();
    MergeUpdateClause clause = create_clause(strategy(), component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    constexpr static size_t expected_segments_to_process = 1;
    ASSERT_EQ(structure_indices.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structure_indices, [](const auto& indices) { return indices.size() == 2; }));
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), expected_segments_to_process);
    ASSERT_TRUE(std::ranges::all_of(structured_entities, [](const auto& entts) { return entts.size() == 2; }));
    const RowRange row_range{
            segment_to_update() * rows_per_segment,
            std::min(segment_to_update() * rows_per_segment + rows_per_segment, unsigned{num_rows})
    };
    const std::array expected_row_ranges{row_range, row_range};
    constexpr static std::array expected_col_ranges{ColRange{1, 2}, ColRange{2, 3}};
    const ProcessingUnit& processing_unit =
            gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                    *component_manager, clause.process(std::move(structured_entities[0]))
            );
    ASSERT_EQ(processing_unit.segments_->size(), 2);
    ASSERT_TRUE(std::ranges::equal(
            *processing_unit.row_ranges_,
            expected_row_ranges,
            [](const std::shared_ptr<RowRange>& left, const RowRange& right) { return *left == right; }
    ));
    ASSERT_TRUE(std::ranges::equal(
            *processing_unit.col_ranges_,
            expected_col_ranges,
            [](const std::shared_ptr<ColRange>& left, const ColRange& right) { return *left == right; }
    ));
    std::vector<int64_t> expected_values_a(row_range.diff());
    std::iota(expected_values_a.begin(), expected_values_a.end(), row_range.start());
    expected_values_a[row_within_segment] = 100;

    std::vector<int32_t> expected_values_b(row_range.diff());
    std::iota(expected_values_b.begin(), expected_values_b.end(), row_range.start());
    expected_values_b[row_within_segment] = 200;

    auto [expected_segments, _cols, _rows] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            iota_view{row_range.start(), row_range.end()} |
                    views::transform([](const timestamp x) -> timestamp { return x * index_step; }),
            std::move(expected_values_a),
            std::move(expected_values_b)
    );
    ASSERT_EQ(expected_segments.size(), 2);
    for (size_t i = 0; i < expected_segments.size(); ++i) {
        ASSERT_EQ(*(processing_unit.segments_->at(i)), expected_segments[i]);
    }
}

INSTANTIATE_TEST_SUITE_P(
        MergeUpdateClauseUpdateOffByOne, MergeUpdateClauseUpdateOffByOne,
        testing::Combine(
                testing::Values(
                        MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::DO_NOTHING},
                        MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::INSERT}
                ),
                testing::Range(0u, 6u),
                testing::Values(
                        MergeUpdateClauseUpdateOffByOne::UPDATE_SECOND_TO_LAST,
                        MergeUpdateClauseUpdateOffByOne::UPDATE_LAST, MergeUpdateClauseUpdateOffByOne::UPDATE_FIRST,
                        MergeUpdateClauseUpdateOffByOne::UPDATE_SECOND
                )
        )
);
TEST(MergeUpdateInsertIndexSpansMultipleSegments, LastIndexValueSameAsNextSegmentFirstTwoOverlapingSegments) {
    constexpr static std::array fields{
            FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}, FieldRef{{DataType::INT32, Dimension::Dim0}, "b"}
    };
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 1;
    constexpr static int num_rows = 27;
    constexpr static int num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    // Assuming segment indexing starts from 0, segment 1 ends with 9 and segment 2 starts with 9
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, num_rows>{1,  2,  3,  4,  5,  6,  7,  8,  9,  9,  9,  9,  10, 11,
                                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
            iota_view{int64_t{0}, int64_t{num_rows}},
            iota_view{0, int{num_rows}}
    );
    ASSERT_EQ(segments.size(), 12);
    // Will be inserted in segment 2
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, 3>{9, 9, 9}, std::array<int64_t, 3>{10, 8, 120}, std::array{100, 200, 300}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    ASSERT_EQ(ranges_and_keys.size(), 12);
    auto component_manager = std::make_shared<ComponentManager>();
    constexpr static MergeStrategy strategy{
            .matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::INSERT
    };
    MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 2);
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 2);

    {
        ASSERT_EQ(structure_indices[0].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{5, 10}, RowRange{5, 10}, RowRange{10, 15}, RowRange{10, 15}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{6, 10}, TimestampRange{6, 10}, TimestampRange{9, 13}, TimestampRange{9, 13}}
        ));

        ASSERT_EQ(entities.size(), 4);
        ASSERT_EQ(structured_entities[0].size(), 4);
        const ProcessingUnit& processing_unit =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[0]))
                );
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(processing_unit.row_ranges_->size(), 2);
        ASSERT_EQ(processing_unit.col_ranges_->size(), 2);
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                std::numeric_limits<size_t>::max(),
                cols_per_segment,
                std::array<timestamp, 8>{6, 7, 8, 9, 9, 9, 9, 9},
                std::array<int64_t, 8>{5, 6, 7, 8, 9, 10, 11, 120},
                std::array{5, 6, 7, 200, 9, 100, 11, 300}
        );
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
    {
        ASSERT_EQ(structure_indices[1].size(), 2);
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{10, 15}, RowRange{10, 15}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{9, 13}, TimestampRange{9, 13}}
        ));
        ASSERT_EQ(structured_entities[1].size(), 2);
        const ProcessingUnit& processing_unit =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[1]))
                );
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(processing_unit.row_ranges_->size(), 2);
        ASSERT_EQ(processing_unit.col_ranges_->size(), 2);
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                std::numeric_limits<size_t>::max(),
                cols_per_segment,
                std::array<timestamp, 3>{10, 11, 12},
                std::array<int64_t, 3>{12, 13, 14},
                std::array{12, 13, 14}
        );
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
}

TEST(MergeUpdateInsertIndexSpansMultipleSegments, LastIndexValueSameAsNextSegmentFirstThreeOverlappingSegments) {
    constexpr static std::array fields{
            FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}, FieldRef{{DataType::INT32, Dimension::Dim0}, "b"}
    };
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 1;
    constexpr static int num_rows = 27;
    constexpr static int num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    // Assuming segment indexing starts from 0, segment 1 ends with 9 and segment 2 starts with 9
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, num_rows>{1, 2, 3, 4,  5,  6,  7,  8,  9,  9,  9,  9,  9, 9,
                                            9, 9, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19},
            iota_view{int64_t{0}, int64_t{num_rows}},
            iota_view{0, int{num_rows}}
    );
    ASSERT_EQ(segments.size(), 12);
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc,
            std::array<timestamp, 7>{8, 9, 9, 9, 9, 10, 11},
            std::array<int64_t, 7>{100, 10, 8, 120, 15, 17, 100},
            std::array{100, 200, 300, 400, 500, 600, 700}
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    ASSERT_EQ(ranges_and_keys.size(), 12);
    auto component_manager = std::make_shared<ComponentManager>();
    constexpr static MergeStrategy strategy{
            .matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::INSERT
    };
    MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 2);
    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 6);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 2);

    {
        ASSERT_EQ(structure_indices[0].size(), 6);
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{
                        RowRange{5, 10},
                        RowRange{5, 10},
                        RowRange{10, 15},
                        RowRange{10, 15},
                        RowRange{15, 20},
                        RowRange{15, 20}
                }
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{
                        ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}
                }
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{
                        TimestampRange{6, 10},
                        TimestampRange{6, 10},
                        TimestampRange{9, 10},
                        TimestampRange{9, 10},
                        TimestampRange{9, 13},
                        TimestampRange{9, 13}
                }
        ));

        ASSERT_EQ(structured_entities[0].size(), 6);
        const ProcessingUnit& processing_unit =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[0]))
                );
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(processing_unit.row_ranges_->size(), 2);
        ASSERT_EQ(processing_unit.col_ranges_->size(), 2);
        const auto expected = slice_data_into_segments<TimeseriesIndex>(
                desc,
                std::numeric_limits<size_t>::max(), // Slicing is not implemented for insertion
                cols_per_segment,
                std::array<timestamp, 14>{6, 7, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
                std::array<int64_t, 14>{5, 6, 7, 100, 8, 9, 10, 11, 12, 13, 14, 15, 16, 120},
                std::array{5, 6, 7, 100, 300, 9, 200, 11, 12, 13, 14, 500, 16, 400}
        );
        for (size_t i = 0; i < std::get<0>(expected).size(); ++i) {
            ASSERT_EQ(*(processing_unit.segments_->at(i)), std::get<0>(expected)[i]);
        }
    }
    {
        ASSERT_EQ(structure_indices[1].size(), 2);
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{15, 20}, RowRange{15, 20}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{9, 13}, TimestampRange{9, 13}}
        ));

        const ProcessingUnit& processing_unit =
                gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
                        *component_manager, clause.process(std::move(structured_entities[1]))
                );
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(processing_unit.row_ranges_->size(), 2);
        ASSERT_EQ(processing_unit.col_ranges_->size(), 2);
        const auto expected = slice_data_into_segments<TimeseriesIndex>(
                desc,
                std::numeric_limits<size_t>::max(), // Slicing is not implemented for insertion
                cols_per_segment,
                std::array<timestamp, 4>{10, 11, 11, 12},
                std::array<int64_t, 4>{17, 18, 100, 19},
                std::array{600, 18, 700, 19}
        );
        for (size_t i = 0; i < std::get<0>(expected).size(); ++i) {
            ASSERT_EQ(*(processing_unit.segments_->at(i)), std::get<0>(expected)[i]);
        }
    }
}

TEST(MergeUpdateInsertIndexSpansMultipleSegments, TwoGroupsOfSegmentsWithMatchingLastIndexValue) {
    constexpr static std::array fields{
            FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}, FieldRef{{DataType::INT32, Dimension::Dim0}, "b"}
    };
    constexpr static int rows_per_segment = 5;
    constexpr static int cols_per_segment = 1;
    constexpr static int num_rows = 27;
    constexpr static int num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    ASSERT_EQ(num_row_slices, 6);
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    // [1, 2, 3, 4, 5],      -> [1;6)
    // [6, 7, 8, 9, 9],      -> [6;10)
    // [9, 9, 9, 9, 9],      -> [9;10)
    // [9, 9, 10, 10, 10],   -> [9;11)
    // [10, 10, 10, 11, 12], -> [10;13)
    // [13, 14]              -> [13;15)
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            rows_per_segment,
            cols_per_segment,
            std::array<timestamp, num_rows>{1, 2, 3, 4,  5,  6,  7,  8,  9,  9,  9,  9,  9, 9,
                                            9, 9, 9, 10, 10, 10, 10, 10, 10, 11, 12, 13, 14},
            iota_view{int64_t{0}, int64_t{num_rows}},
            iota_view{0, int{num_rows}}
    );
    ASSERT_EQ(segments.size(), 12);
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc,
            std::array<timestamp, 15>{8, 9, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 11, 11, 13},
            std::array<int64_t, 15>{100, 11, 16, 13, 100, 8, 200, 19, 100, 17, 20, 300, 400, 23, 500},
            iota_view{0, 15} | std::views::transform([](const auto x) { return x * 100; })
    );
    std::vector<RangesAndKey> ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    ASSERT_EQ(ranges_and_keys.size(), 12);
    auto component_manager = std::make_shared<ComponentManager>();
    constexpr static MergeStrategy strategy{
            .matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::INSERT
    };
    MergeUpdateClause clause = create_clause(strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_EQ(structure_indices.size(), 4);

    std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 10);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 4);

    {
        ASSERT_EQ(structure_indices[0].size(), 6);
        ASSERT_TRUE(std::ranges::equal(structure_indices[0], std::array{0, 1, 2, 3, 4, 5}));
        ASSERT_EQ(structured_entities[0].size(), 6);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])),
                std::array{1, 1, 1, 1, 2, 2}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{
                        RowRange{5, 10},
                        RowRange{5, 10},
                        RowRange{10, 15},
                        RowRange{10, 15},
                        RowRange{15, 20},
                        RowRange{15, 20}

                }
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{
                        ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}
                }
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[0] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{
                        TimestampRange{6, 10},
                        TimestampRange{6, 10},
                        TimestampRange{9, 10},
                        TimestampRange{9, 10},
                        TimestampRange{9, 11},
                        TimestampRange{9, 11}
                }
        ));
        const ProcessingUnit& processing_unit = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured_entities[0])));
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 15>{6, 7, 8, 8, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9},
                std::array<int64_t, 15>{5, 6, 7, 100, 8, 9, 10, 11, 12, 13, 14, 15, 16, 100, 200},
                std::array{5, 6, 7, 0, 500, 9, 10, 100, 12, 300, 14, 15, 200, 400, 600}
        );
        ASSERT_EQ(processing_unit.segments_->size(), expected_segments.size());
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
    {
        ASSERT_EQ(structure_indices[1].size(), 4);
        ASSERT_TRUE(std::ranges::equal(structure_indices[1], std::array{4, 5, 6, 7}));
        ASSERT_EQ(structured_entities[1].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[1])),
                std::array{2, 2, 2, 2}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{15, 20}, RowRange{15, 20}, RowRange{20, 25}, RowRange{20, 25}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[1] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{9, 11}, TimestampRange{9, 11}, TimestampRange{10, 13}, TimestampRange{10, 13}}
        ));
        const ProcessingUnit& processing_unit = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured_entities[1])));
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 8>{10, 10, 10, 10, 10, 10, 10, 10},
                std::array<int64_t, 8>{17, 18, 19, 20, 21, 22, 100, 300},
                std::array{900, 18, 700, 1000, 21, 22, 800, 1100}
        );
        ASSERT_EQ(processing_unit.segments_->size(), expected_segments.size());
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
    {
        ASSERT_EQ(structure_indices[2].size(), 2);
        ASSERT_TRUE(std::ranges::equal(structure_indices[2], std::array{6, 7}));
        ASSERT_EQ(structured_entities[2].size(), 2);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[2])), std::array{2, 2}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[2] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{20, 25}, RowRange{20, 25}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[2] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[2] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{10, 13}, TimestampRange{10, 13}}
        ));
        const ProcessingUnit& processing_unit = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured_entities[2])));
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 3>{11, 11, 12},
                std::array<int64_t, 3>{23, 400, 24},
                std::array{1300, 1200, 24}
        );
        ASSERT_EQ(processing_unit.segments_->size(), expected_segments.size());
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
    {
        ASSERT_EQ(structure_indices[3].size(), 2);
        ASSERT_TRUE(std::ranges::equal(structure_indices[3], std::array{8, 9}));
        ASSERT_EQ(structured_entities[3].size(), 2);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[3])), std::array{1, 1}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[3] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].row_range(); }),
                std::array{RowRange{25, 27}, RowRange{25, 27}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[3] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].col_range(); }),
                std::array{ColRange{1, 2}, ColRange{2, 3}}
        ));
        ASSERT_TRUE(std::ranges::equal(
                structure_indices[3] |
                        std::views::transform([&](const size_t idx) { return ranges_and_keys[idx].key_.time_range(); }),
                std::array{TimestampRange{13, 15}, TimestampRange{13, 15}}
        ));
        const ProcessingUnit& processing_unit = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured_entities[3])));
        auto [expected_segments, _c, _r] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 3>{13, 13, 14},
                std::array<int64_t, 3>{25, 500, 26},
                std::array{25, 1400, 26}
        );
        ASSERT_EQ(processing_unit.segments_->size(), expected_segments.size());
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
    }
}

struct MergeUpdateInsertIndexSpansMultipleSegmentsChain : testing::Test {
    MergeUpdateInsertIndexSpansMultipleSegmentsChain() {
        std::tie(segments, col_ranges, row_ranges) = slice_data_into_segments<TimeseriesIndex>(
                desc,
                rows_per_segment,
                cols_per_segment,
                std::array<timestamp, num_rows>{0,  1,  2,  4,  5,  6,  6,  6,  6,  6, 10,
                                                11, 11, 13, 15, 15, 16, 17, 17, 19, 20},
                iota_view{int64_t{0}, int64_t{num_rows}},
                iota_view{0, int{num_rows}}
        );
        ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    }

    void assert_processing_result(
            const MergeUpdateClause& clause, std::vector<EntityId>&& entities,
            const std::span<const SegmentInMemory> expected_segments, const std::span<const RowRange> row_ranges
    ) const {
        const ProcessingUnit& processing_unit = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(entities)));
        ASSERT_EQ(processing_unit.segments_->size(), expected_segments.size());
        ASSERT_EQ(processing_unit.segments_->size(), 2);
        ASSERT_EQ(*processing_unit.segments_->at(0), expected_segments[0]);
        ASSERT_EQ(*processing_unit.segments_->at(1), expected_segments[1]);
        ASSERT_TRUE(std::ranges::equal(
                (*processing_unit.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), row_ranges
        ));
    }

    constexpr static int rows_per_segment = 3;
    constexpr static int cols_per_segment = 1;
    constexpr static int num_rows = 21;
    constexpr static int num_row_slices = (num_rows + rows_per_segment - 1) / rows_per_segment;
    static_assert(num_row_slices == 7, "Number of row slices should be 7");
    constexpr static std::array fields{
            FieldRef{{DataType::INT64, Dimension::Dim0}, "a"},
            FieldRef{{DataType::INT32, Dimension::Dim0}, "b"}
    };
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor("TestStream", fields);
    std::shared_ptr<ComponentManager> component_manager = std::make_shared<ComponentManager>();
    std::vector<SegmentInMemory> segments;
    std::vector<RowRange> row_ranges;
    std::vector<ColRange> col_ranges;
    std::vector<RangesAndKey> ranges_and_keys;
};

TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain, SourceInRowSlice0) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, 2>{0, 2}, std::array<int64_t, 2>{0, 10}, std::array{100, 200}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1}}));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{RowRange{0, 3}, RowRange{0, 3}}
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{ColRange{1, 2}, ColRange{2, 3}}
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 2);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), 2);
    ASSERT_TRUE(std::ranges::equal(
            std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])), std::array{1, 1}
    ));
    auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            100'000,
            cols_per_segment,
            std::array<timestamp, 4>{0, 1, 2, 2},
            std::array<int64_t, 4>{0, 1, 2, 10},
            std::array{100, 1, 2, 200}
    );
    constexpr static std::array expected_row_ranges{RowRange{0, 3}, RowRange{0, 3}};
    assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
}

// Even when the source value does not span multiple segments structure by processing will structure by time slice and
// load all segments in the time slice of the segment containing the value. This is because the strategy for slicing is
// selected globally based on the merge strategy. It can be refined so that structure by time slice is used only on
// subset of the segments, though this will result in more complicated logic for the structuring and processing.
// Related to Monday 12379148034
TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain, SourceInRowSlice1ValueIsNotInMultipleSegments) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array{timestamp{5}}, std::array{int64_t{4}}, std::array{100}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1, 2, 3, 4, 5}}));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{RowRange{3, 6}, RowRange{3, 6}, RowRange{6, 9}, RowRange{6, 9}, RowRange{9, 12}, RowRange{9, 12}}
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}, ColRange{1, 2}, ColRange{2, 3}}
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 6);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), 6);
    ASSERT_TRUE(std::ranges::equal(
            std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])),
            std::array{1, 1, 1, 1, 1, 1}
    ));
    auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            100'000,
            cols_per_segment,
            std::array<timestamp, 9>{4, 5, 6, 6, 6, 6, 6, 10, 11},
            std::array<int64_t, 9>{3, 4, 5, 6, 7, 8, 9, 10, 11},
            std::array{3, 100, 5, 6, 7, 8, 9, 10, 11}
    );
    constexpr static std::array expected_row_ranges{RowRange{3, 12}, RowRange{3, 12}};
    assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
}

TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain, SourceInRowSlice1ValueIsInMultipleSegments) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc,
            std::array<timestamp, 4>{6, 6, 6, 6},
            std::array<int64_t, 4>{9, 5, 6, 100},
            std::array{100, 200, 300, 400}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(
            std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1, 2, 3, 4, 5}, {4, 5, 6, 7}})
    );
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{
                    RowRange{3, 6},
                    RowRange{3, 6},
                    RowRange{6, 9},
                    RowRange{6, 9},
                    RowRange{9, 12},
                    RowRange{9, 12},
                    RowRange{12, 15},
                    RowRange{12, 15}
            }
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3}
            }
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 8);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 2);
    {
        ASSERT_EQ(structured_entities[0].size(), 6);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])),
                std::array{1, 1, 1, 1, 2, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 8>{4, 5, 6, 6, 6, 6, 6, 6},
                std::array<int64_t, 8>{3, 4, 5, 6, 7, 8, 9, 100},
                std::array{3, 4, 200, 300, 7, 8, 100, 400}
        );
        constexpr static std::array expected_row_ranges{RowRange{3, 10}, RowRange{3, 10}};
        assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
    }
    {
        ASSERT_EQ(structured_entities[1].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[1])),
                std::array{2, 2, 1, 1}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 5>{10, 11, 11, 13, 15},
                std::array<int64_t, 5>{10, 11, 12, 13, 14},
                std::array{10, 11, 12, 13, 14}
        );
        constexpr static std::array expected_row_ranges{RowRange{10, 15}, RowRange{10, 15}};
        assert_processing_result(clause, std::move(structured_entities[1]), expected_segments, expected_row_ranges);
    }
}

// This exhibits suboptimal behavior. This is because structure for processing structures by time slice and selects time
// slices that contain the source value. In this case the source value is contained in 3 time slices and time slices can
// overlap. So all segments for the first time slice are loaded even though there's nothing to process. Strategy for
// fixing this is laid out in Monday 12379148034.
// Note: In the worst case this will load 3 time slices and all segments in those time slices.
TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain, SourceInRowSlice3) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array{timestamp{11}}, std::array{int64_t{100}}, std::array{100}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(
            structure_indices, std::vector<std::vector<size_t>>{{0, 1, 2, 3, 4, 5}, {4, 5, 6, 7}, {6, 7, 8, 9}}
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{
                    RowRange{3, 6},
                    RowRange{3, 6},
                    RowRange{6, 9},
                    RowRange{6, 9},
                    RowRange{9, 12},
                    RowRange{9, 12},
                    RowRange{12, 15},
                    RowRange{12, 15},
                    RowRange{15, 18},
                    RowRange{15, 18}
            }
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3}
            }
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 10);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 3);

    {
        ASSERT_EQ(structured_entities[0].size(), 6);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])),
                std::array{1, 1, 1, 1, 2, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 7>{4, 5, 6, 6, 6, 6, 6},
                std::array<int64_t, 7>{3, 4, 5, 6, 7, 8, 9},
                std::array{3, 4, 5, 6, 7, 8, 9}
        );
        constexpr static std::array expected_row_ranges{RowRange{3, 10}, RowRange{3, 10}};
        assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
    }
    {
        ASSERT_EQ(structured_entities[1].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[1])),
                std::array{2, 2, 2, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 4>{10, 11, 11, 11},
                std::array<int64_t, 4>{10, 11, 12, 100},
                std::array{10, 11, 12, 100}
        );
        constexpr static std::array expected_row_ranges{RowRange{10, 13}, RowRange{10, 13}};
        assert_processing_result(clause, std::move(structured_entities[1]), expected_segments, expected_row_ranges);
    }
    {
        ASSERT_EQ(structured_entities[2].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[2])),
                std::array{2, 2, 1, 1}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 5>{13, 15, 15, 16, 17},
                std::array<int64_t, 5>{13, 14, 15, 16, 17},
                std::array{13, 14, 15, 16, 17}
        );
        constexpr static std::array expected_row_ranges{RowRange{13, 18}, RowRange{13, 18}};
        assert_processing_result(clause, std::move(structured_entities[2]), expected_segments, expected_row_ranges);
    }
}

TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain, SourceInRowSlice5) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array{timestamp{15}}, std::array{int64_t{100}}, std::array{100}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(
            structure_indices, std::vector<std::vector<size_t>>{{0, 1, 2, 3}, {2, 3, 4, 5}, {4, 5, 6, 7}}
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{
                    RowRange{9, 12},
                    RowRange{9, 12},
                    RowRange{12, 15},
                    RowRange{12, 15},
                    RowRange{15, 18},
                    RowRange{15, 18},
                    RowRange{18, 21},
                    RowRange{18, 21}
            }
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3},
                    ColRange{1, 2},
                    ColRange{2, 3}
            }
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 8);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 3);
    {
        ASSERT_EQ(structured_entities[0].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])),
                std::array{1, 1, 2, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 4>{6, 10, 11, 11},
                std::array<int64_t, 4>{9, 10, 11, 12},
                std::array{9, 10, 11, 12}
        );
        constexpr static std::array expected_row_ranges{RowRange{9, 13}, RowRange{9, 13}};
        assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
    }
    {
        ASSERT_EQ(structured_entities[1].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[1])),
                std::array{2, 2, 2, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 4>{13, 15, 15, 15},
                std::array<int64_t, 4>{13, 14, 15, 100},
                std::array{13, 14, 15, 100}
        );
        constexpr static std::array expected_row_ranges{RowRange{13, 16}, RowRange{13, 16}};
        assert_processing_result(clause, std::move(structured_entities[1]), expected_segments, expected_row_ranges);
    }
    {
        ASSERT_EQ(structured_entities[2].size(), 4);
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[2])),
                std::array{2, 2, 1, 1}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc,
                100'000,
                cols_per_segment,
                std::array<timestamp, 5>{16, 17, 17, 19, 20},
                std::array<int64_t, 5>{16, 17, 18, 19, 20},
                std::array{16, 17, 18, 19, 20}
        );
        constexpr static std::array expected_row_ranges{RowRange{16, 21}, RowRange{16, 21}};
        assert_processing_result(clause, std::move(structured_entities[2]), expected_segments, expected_row_ranges);
    }
}

TEST_F(MergeUpdateInsertIndexSpansMultipleSegmentsChain,
       InsertInRowSlice3WithoutColumnMatchingDoesNotTriggerGroupingByTimeSlice) {
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array{timestamp{7}}, std::array{int64_t{100}}, std::array{100}
    );
    MergeUpdateClause clause = create_clause(update_and_insert_strategy, component_manager, std::move(input_frame));
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1}}));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.row_range(); }),
            std::array{RowRange{9, 12}, RowRange{9, 12}}
    ));
    ASSERT_TRUE(std::ranges::equal(
            ranges_and_keys | std::views::transform([](const auto& rk) { return rk.col_range(); }),
            std::array{ColRange{1, 2}, ColRange{2, 3}}
    ));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    ASSERT_EQ(entities.size(), 2);
    std::vector<std::vector<EntityId>> structured_entities = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured_entities.size(), 1);
    ASSERT_EQ(structured_entities[0].size(), 2);
    ASSERT_TRUE(std::ranges::equal(
            std::get<0>(component_manager->get_entities<EntityFetchCount>(structured_entities[0])), std::array{1, 1}
    ));
    auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            100'000,
            cols_per_segment,
            std::array<timestamp, 4>{6, 7, 10, 11},
            std::array<int64_t, 4>{9, 100, 10, 11},
            std::array{9, 100, 10, 11}
    );
    constexpr static std::array expected_row_ranges{RowRange{9, 12}, RowRange{9, 12}};
    assert_processing_result(clause, std::move(structured_entities[0]), expected_segments, expected_row_ranges);
}
TEST(MergeUpdateInsertBackSharedGroup, InsertsIntoBackSharedMiddleGroup) {
    auto component_manager = std::make_shared<ComponentManager>();
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor(
            "s", std::array{FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}}
    );
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            2,
            1,
            std::array<timestamp, 8>{10, 20, 20, 30, 30, 40, 40, 50},
            std::array<int64_t, 8>{0, 1, 2, 3, 4, 5, 6, 7}
    );
    auto ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    auto [input_frame, input_frame_owner] = input_frame_from_tensors<TimeseriesIndex>(
            desc, std::array<timestamp, 3>{15, 25, 35}, std::array<int64_t, 3>{1000, 1001, 1002}
    );
    MergeUpdateClause clause =
            create_clause(update_and_insert_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1}, {1, 2}, {2, 3}}));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured.size(), 3);
    {
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc, 100'000, 1, std::array<timestamp, 4>{10, 15, 20, 20}, std::array<int64_t, 4>{0, 1000, 1, 2}
        );
        constexpr static std::array expected_row_ranges{RowRange{0, 3}};
        const ProcessingUnit proc = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured[0])));
        ASSERT_EQ(proc.segments_->size(), 1);
        EXPECT_EQ(*proc.segments_->at(0), expected_segments[0]);
        ASSERT_TRUE(std::ranges::equal(
                (*proc.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), expected_row_ranges
        ));
    }
    {
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc, 100'000, 1, std::array<timestamp, 3>{25, 30, 30}, std::array<int64_t, 3>{1001, 3, 4}
        );
        constexpr static std::array expected_row_ranges{RowRange{3, 5}};
        const ProcessingUnit proc = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured[1])));
        ASSERT_EQ(proc.segments_->size(), 1);
        EXPECT_EQ(*proc.segments_->at(0), expected_segments[0]);
        ASSERT_TRUE(std::ranges::equal(
                (*proc.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), expected_row_ranges
        ));
    }
    {
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc, 100'000, 1, std::array<timestamp, 4>{35, 40, 40, 50}, std::array<int64_t, 4>{1002, 5, 6, 7}
        );
        constexpr static std::array expected_row_ranges{RowRange{5, 8}};
        const ProcessingUnit proc = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured[2])));
        ASSERT_EQ(proc.segments_->size(), 1);
        EXPECT_EQ(*proc.segments_->at(0), expected_segments[0]);
        ASSERT_TRUE(std::ranges::equal(
                (*proc.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), expected_row_ranges
        ));
    }
}

TEST(MergeUpdateInsertOnlySharedSlice, InsertsIntoChainWithSharedBoundaries) {
    constexpr MergeStrategy insert_only_strategy{.not_matched_by_target = MergeAction::INSERT};
    auto component_manager = std::make_shared<ComponentManager>();
    const StreamDescriptor desc = TimeseriesIndex::default_index().create_stream_descriptor(
            "s", std::array{FieldRef{{DataType::INT64, Dimension::Dim0}, "a"}}
    );
    auto [segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex>(
            desc,
            2,
            1,
            std::array<timestamp, 8>{10, 20, 20, 30, 30, 40, 40, 50},
            std::array<int64_t, 8>{0, 1, 2, 3, 4, 5, 6, 7}
    );
    auto ranges_and_keys = generate_ranges_and_keys(desc, segments, col_ranges, row_ranges);
    auto [input_frame, input_frame_owner] =
            input_frame_from_tensors<TimeseriesIndex>(desc, std::array<timestamp, 1>{25}, std::array<int64_t, 1>{1000});
    MergeUpdateClause clause = create_clause(insert_only_strategy, component_manager, std::move(input_frame), {"a"});
    const std::vector<std::vector<size_t>> structure_indices = clause.structure_for_processing(ranges_and_keys);
    ASSERT_TRUE(std::ranges::equal(structure_indices, std::vector<std::vector<size_t>>{{0, 1}, {1, 2}}));
    const std::vector<EntityId> entities = push_selected_entities(
            *component_manager,
            structure_indices,
            clone_segments(segments),
            std::vector{col_ranges},
            std::vector{row_ranges},
            std::move(ranges_and_keys)
    );
    std::vector<std::vector<EntityId>> structured = structure_entities(structure_indices, entities);
    ASSERT_EQ(structured.size(), 2);
    {
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured[0])), std::array{1, 2}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc, 100'000, 1, std::array<timestamp, 3>{10, 20, 20}, std::array<int64_t, 3>{0, 1, 2}
        );
        constexpr static std::array expected_row_ranges{RowRange{0, 3}};
        const ProcessingUnit proc = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured[0])));
        ASSERT_EQ(proc.segments_->size(), 1);
        EXPECT_EQ(*proc.segments_->at(0), expected_segments[0]);
        ASSERT_TRUE(std::ranges::equal(
                (*proc.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), expected_row_ranges
        ));
    }
    {
        ASSERT_TRUE(std::ranges::equal(
                std::get<0>(component_manager->get_entities<EntityFetchCount>(structured[1])), std::array{2, 1}
        ));
        auto [expected_segments, _r, _c] = slice_data_into_segments<TimeseriesIndex>(
                desc, 100'000, 1, std::array<timestamp, 4>{25, 30, 30, 40}, std::array<int64_t, 4>{1000, 3, 4, 5}
        );
        constexpr static std::array expected_row_ranges{RowRange{3, 6}};
        const ProcessingUnit proc = gather_entities<
                std::shared_ptr<SegmentInMemory>,
                std::shared_ptr<RowRange>,
                std::shared_ptr<ColRange>,
                EntityFetchCount>(*component_manager, clause.process(std::move(structured[1])));
        ASSERT_EQ(proc.segments_->size(), 1);
        EXPECT_EQ(*proc.segments_->at(0), expected_segments[0]);
        ASSERT_TRUE(std::ranges::equal(
                (*proc.row_ranges_) | std::views::transform([](const auto& rr) { return *rr; }), expected_row_ranges
        ));
    }
}
