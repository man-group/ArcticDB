/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "util/ranges_from_future.hpp"

#include <gtest/gtest.h>
#include <arcticdb/processing/clause.hpp>
#include <stream/test/stream_test_common.hpp>
#include <arcticdb/entity/types.hpp>
#include <aws/core/utils/stream/ResponseStream.h>
#include <boost/fusion/sequence/intrinsic/segments.hpp>

using namespace arcticdb;
using namespace std::ranges;

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

std::vector<StreamDescriptor> split_descriptor(const StreamDescriptor& descriptor, const size_t cols_per_segment) {
    if (descriptor.fields().size() <= cols_per_segment) {
        return std::vector{descriptor};
    }
    const size_t num_segments = descriptor.fields().size() / cols_per_segment;
    std::vector<StreamDescriptor> res;
    res.reserve(num_segments);

    const unsigned field_count = descriptor.field_count();
    for (size_t i = 0, source_field = descriptor.index().field_count(); i < num_segments; ++i) {
        StreamDescriptor partial(descriptor.id());
        if (descriptor.index().field_count() > 0) {
            partial.set_index(descriptor.index());
            for (unsigned index_field = 0; index_field < descriptor.index().field_count(); ++index_field) {
                partial.add_field(descriptor.field(index_field));
            }
        }
        for (size_t field = 0; field < cols_per_segment && source_field < field_count; ++field) {
            partial.add_field(descriptor.field(source_field++));
        }
        res.push_back(std::move(partial));
    }
    return res;
}

template<typename T1, typename T2>
requires util::instantiation_of<T1, std::tuple> && util::instantiation_of<T2, std::tuple>
auto tuple_cat_ref(const T1& tuple1, const T2& tuple2) {
    return std::apply(
            [&](auto&&... args1) {
                return std::apply(
                        [&](auto&&... args2) {
                            return std::tie(
                                    std::forward<decltype(args1)>(args1)..., std::forward<decltype(args2)>(args2)...
                            );
                        },
                        tuple2
                );
            },
            tuple1
    );
}

template<std::ranges::sized_range... Index, std::ranges::sized_range... Data>
requires(sizeof...(Data) > 0)
void slice_column_slice_into_row_slices(
        const std::tuple<Index&&...>& index, const std::tuple<Data&&...>& column_slice, const StreamDescriptor& desc,
        const size_t rows_per_segment, const ColRange& col_range, std::vector<SegmentInMemory>& segments,
        std::vector<ColRange>& col_ranges, std::vector<RowRange>& row_ranges
) {
    const size_t total_rows = std::ranges::size(std::get<0>(column_slice));
    for (size_t start_row = 0; start_row < total_rows; start_row += rows_per_segment) {
        std::apply(
                [&]<std::ranges::sized_range... Cols>(const Cols&... cols) {
                    const size_t rows_to_take = std::min(rows_per_segment, total_rows - start_row);
                    segments.push_back(SegmentInMemory::create_dense_segment(
                            desc, std::ranges::take_view(std::ranges::drop_view(cols, start_row), rows_to_take)...
                    ));
                    row_ranges.emplace_back(start_row, start_row + rows_to_take);
                    col_ranges.emplace_back(col_range);
                },
                tuple_cat_ref(index, column_slice)
        );
    }
}

template<size_t N, typename... Data>
auto split(Data&&... data) {
    constexpr static auto non_index_columns = sizeof...(Data) - N;
    return [&]<typename T, size_t... Is1, size_t... Is2>(
                   std::index_sequence<Is1...>, std::index_sequence<Is2...>, T&& fwd_tuple
           ) {
        return std::pair{
                std::forward_as_tuple(std::get<Is1>(std::forward<T>(fwd_tuple))...),
                std::forward_as_tuple(std::get<Is2 + N>(std::forward<T>(fwd_tuple))...)
        };
    }(std::make_index_sequence<N>{},
           std::make_index_sequence<non_index_columns>{},
           std::forward_as_tuple(std::forward<Data>(data)...));
}

template<
        std::ranges::sized_range IndexCols, typename ColumnSlice, std::ranges::sized_range CurrentCol,
        std::ranges::sized_range... RestCols>
requires(
        util::instantiation_of<ColumnSlice, std::tuple> &&
        []<size_t... Is>(std::index_sequence<Is...>) {
            return (std::ranges::sized_range<std::tuple_element_t<Is, ColumnSlice>> && ...);
        }(std::make_index_sequence<std::tuple_size_v<ColumnSlice>>{})
)
void slice_data_into_segments(
        const std::span<const StreamDescriptor> descriptors, const size_t rows_per_segment,
        const size_t cols_per_segment, ColRange col_range, std::vector<SegmentInMemory>& segments,
        std::vector<ColRange>& col_ranges, std::vector<RowRange>& row_ranges, const std::tuple<IndexCols&&>& index,
        ColumnSlice&& column_slice, CurrentCol&& current_col, RestCols&&... data
) {
    auto new_column_slice = std::tuple_cat(
            std::forward<ColumnSlice>(column_slice), std::forward_as_tuple(std::forward<CurrentCol>(current_col))
    );

    constexpr size_t new_slice_size = std::tuple_size_v<ColumnSlice> + 1;

    if constexpr (sizeof...(RestCols) == 0) {
        slice_column_slice_into_row_slices(
                index,
                new_column_slice,
                descriptors.front(),
                rows_per_segment,
                col_range,
                segments,
                col_ranges,
                row_ranges
        );
    } else if (new_slice_size == cols_per_segment) {
        slice_column_slice_into_row_slices(
                index,
                new_column_slice,
                descriptors.front(),
                rows_per_segment,
                col_range,
                segments,
                col_ranges,
                row_ranges
        );
        col_range.first = col_range.second;
        col_range.second = std::min(col_range.first + cols_per_segment, col_range.first + sizeof...(RestCols));
        slice_data_into_segments(
                descriptors.last(descriptors.size() - 1),
                rows_per_segment,
                cols_per_segment,
                col_range,
                segments,
                col_ranges,
                row_ranges,
                index,
                std::tuple{},
                std::forward<RestCols>(data)...
        );
    } else {
        slice_data_into_segments(
                descriptors,
                rows_per_segment,
                cols_per_segment,
                col_range,
                segments,
                col_ranges,
                row_ranges,
                index,
                std::move(new_column_slice),
                std::forward<RestCols>(data)...
        );
    }
}

template<ValidIndex index, std::ranges::sized_range... Data>
std::tuple<std::vector<SegmentInMemory>, std::vector<ColRange>, std::vector<RowRange>> slice_data_into_segments(
        const StreamDescriptor& descriptor, const size_t rows_per_segment, const size_t cols_per_segment, Data&&... data
) {
    std::vector<StreamDescriptor> descriptors = split_descriptor(descriptor, cols_per_segment);
    std::vector<SegmentInMemory> segments;
    std::vector<ColRange> col_ranges;
    std::vector<RowRange> row_ranges;

    auto [index_columns, data_columns] = split<index::field_count()>(std::forward<Data>(data)...);
    std::apply(
            [&]<typename... Cols>(Cols&&... cols) {
                slice_data_into_segments(
                        descriptors,
                        rows_per_segment,
                        cols_per_segment,
                        ColRange{
                                index::field_count(), index::field_count() + std::min(cols_per_segment, sizeof...(Cols))
                        },
                        segments,
                        col_ranges,
                        row_ranges,
                        std::move(index_columns),
                        std::tuple{},
                        std::forward<Cols>(cols)...
                );
            },
            std::forward<decltype(data_columns)>(data_columns)
    );
    return std::make_tuple(std::move(segments), std::move(col_ranges), std::move(row_ranges));
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
                AtomKeyBuilder().start_index(start_ts).end_index(end_ts).build<KeyType::TABLE_DATA>(
                        source_descriptor.id()
                )
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

std::vector<std::vector<EntityId>> map_entities_to_structure_for_processing_output(
        const std::span<const std::vector<size_t>> structure_for_processing_out,
        const std::span<const EntityId> entities
) {
    std::vector<std::vector<EntityId>> process_input;
    process_input.reserve(structure_for_processing_out.size());
    std::ranges::transform(
            structure_for_processing_out,
            std::back_inserter(process_input),
            [&](const std::vector<size_t>& indices) {
                std::vector<EntityId> result;
                result.reserve(indices.size());
                std::ranges::transform(indices, std::back_inserter(result), [&](const size_t idx) {
                    return entities[idx];
                });
                return result;
            }
    );
    return process_input;
}

void print_segment(const SegmentInMemory& segment) {
    const StreamDescriptor& desc = segment.descriptor();
    for (unsigned i = 0; i < desc.field_count(); ++i) {
        std::cout << fmt::format("Print column[{}]: {}\n", i, desc.field(i));
        visit_field(desc.field(i), [&](auto tdt) {
            using TDT = decltype(tdt);
            ColumnData cd = segment.column_data(i);
            for (auto it = cd.begin<TDT>(); it != cd.end<TDT>(); ++it) {
                if constexpr (std::same_as<typename TDT::DataTypeTag::raw_type, int8_t>) {
                    std::cout << int(*it) << " ";
                } else {
                    std::cout << *it << " ";
                }
            }
            std::cout << "\n";
        });
    }
}

template<typename H, typename... T>
requires std::ranges::sized_range<H> && (std::ranges::sized_range<T> && ...)
auto materialize_ranges(H&& head, T&&... tail) {
    if constexpr (sizeof...(T) == 0) {
        if constexpr (!std::ranges::contiguous_range<H>) {
            return std::forward_as_tuple(std::vector<std::ranges::range_value_t<H>>(std::forward<H>(head)));
        } else {
            return std::forward_as_tuple(std::forward<H>(head));
        }
    } else {
        if constexpr (!std::ranges::contiguous_range<H>) {
            return std::tuple_cat(
                    std::forward_as_tuple(std::vector<std::ranges::range_value_t<H>>(std::forward<H>(head))),
                    materialize_ranges(std::forward<T>(tail)...)
            );
        } else {
            return std::tuple_cat(
                    std::forward_as_tuple(std::forward<H>(head)), materialize_ranges(std::forward<T>(tail)...)
            );
        }
    }
}

template<ValidIndex Index, typename... T>
requires((Index::field_count() == 0 || Index::field_count() == 1) && (std::ranges::sized_range<T> && ...))
auto input_frame_from_tensors(const StreamDescriptor& desc, T&&... input) {
    constexpr static size_t data_columns = sizeof...(T) - Index::field_count();
    auto materialized_input = materialize_ranges(std::forward<T>(input)...);
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        const size_t first_row_count = std::get<0>(materialized_input).size();
        util::check(
                ((std::ranges::size(std::get<Is>(materialized_input)) == first_row_count) && ...),
                "All input data must have the same number of rows"
        );
    }(std::make_index_sequence<sizeof...(T)>{});
    std::vector<NativeTensor> tensors = [&]<size_t... Is>(std::index_sequence<Is...>) {
        std::vector<NativeTensor> result_tensors;
        result_tensors.reserve(data_columns);
        (result_tensors.push_back(NativeTensor::one_dimensional_tensor(
                 std::get<Is + Index::field_count()>(materialized_input),
                 desc.field(Is + Index::field_count()).type().data_type()
         )),
         ...);
        return result_tensors;
    }(std::make_index_sequence<data_columns>{});
    const size_t num_rows = std::ranges::size(std::get<0>(materialized_input));
    if constexpr (Index::field_count() == 1) {
        InputFrame result_frame(
                desc,
                std::move(tensors),
                NativeTensor::one_dimensional_tensor(std::get<0>(materialized_input), desc.field(0).type().data_type())
        );
        result_frame.num_rows = num_rows;
        return std::pair{std::move(result_frame), std::move(materialized_input)};
    } else {
        InputFrame result_frame(desc, std::move(tensors), std::nullopt);
        result_frame.num_rows = num_rows;
        return std::pair{std::move(result_frame), std::move(materialized_input)};
    }
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
        const size_t entity_count = segments.size();
        initial_entities_ = component_manager_->add_entities(
                wrap_in_shared_ptr(std::move(col_ranges)),
                wrap_in_shared_ptr(std::move(row_ranges)),
                wrap_in_shared_ptr(std::move(segments)),
                std::vector<EntityFetchCount>(entity_count, 0)
        );
    }

    MergeUpdateClause create_clause(InputFrame&& input_frame) const {
        MergeUpdateClause clause({}, strategy_, std::make_shared<InputFrame>(std::move(input_frame)), true);
        clause.set_component_manager(component_manager_);
        return clause;
    }

    StreamDescriptor descriptor_;
    std::vector<RangesAndKey> ranges_and_keys_;
    std::shared_ptr<ComponentManager> component_manager_ = std::make_shared<ComponentManager>();
    MergeStrategy strategy_;
    std::vector<EntityId> initial_entities_;
};

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
            const std::span<const std::vector<size_t>> structure_indices,
            std::vector<std::vector<EntityId>>&& entities_to_process
    ) const {
        const size_t column_slices_per_row_slice = (num_cols_ + cols_per_segment() - 1) / cols_per_segment();
        for (size_t i = 0; i < entities_to_process.size(); ++i) {
            auto proc = gather_entities<
                    std::shared_ptr<SegmentInMemory>,
                    std::shared_ptr<RowRange>,
                    std::shared_ptr<ColRange>>(*component_manager_, clause.process(std::move(entities_to_process[i])));
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
    assert_process_results_match_expected(
            clause,
            expected_segments,
            structure_indices,
            map_entities_to_structure_for_processing_output(structure_indices, initial_entities_)
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
            iota_view(timestamp{0}, timestamp{3 * rows_per_segment()}),
            std::array<int8_t, 15>{10, 1, 2, 3, 4, 5, 6, 20, 8, 9, 10, 11, 12, 13, 30},
            std::array<unsigned, 15>{100, 1, 2, 3, 4, 5, 6, 200, 8, 9, 10, 11, 12, 13, 300},
            std::array<bool, 15>{1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1},
            std::array<float, 15>{11.1f, 1, 2, 3, 4, 5, 6, 22.2f, 8, 9, 10, 11, 12, 13, 33.3f},
            std::array<timestamp, 15>{1000, 1, 2, 3, 4, 5, 6, 2000, 8, 9, 10, 11, 12, 13, 3000}
    );
    sort_by_rowslice(expected_row_ranges, expected_col_ranges, expected_segments);
    assert_process_results_match_expected(
            clause,
            expected_segments,
            structure_indices,
            map_entities_to_structure_for_processing_output(structure_indices, initial_entities_)
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
                non_string_fields_ts_index_descriptor(),
                MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::DO_NOTHING},
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
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(0, 5));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    const std::vector<EntityId> result_entities = clause.process(
            std::move(map_entities_to_structure_for_processing_output(structure_indices, initial_entities_)[0])
    );
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
    ASSERT_EQ(*(*proc.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], RowRange(4, 6));
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
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(5, 10));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));
    const std::vector<EntityId> result_entities = clause.process(
            std::move(map_entities_to_structure_for_processing_output(structure_indices, initial_entities_)[0])
    );
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
    ASSERT_EQ(*(*proc.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], RowRange(4, 6));
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
    ASSERT_EQ(structure_indices.size(), 1);
    ASSERT_EQ(structure_indices[0].size(), 2);
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].row_range(), RowRange(10, 14));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][0]].col_range(), ColRange(1, 4));
    ASSERT_EQ(ranges_and_keys_[structure_indices[0][1]].col_range(), ColRange(4, 6));

    const std::vector<EntityId> result_entities = clause.process(
            std::move(map_entities_to_structure_for_processing_output(structure_indices, initial_entities_)[0])
    );
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
    ASSERT_EQ(*(*proc.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc.col_ranges_)[1], RowRange(4, 6));
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
    std::vector<std::vector<EntityId>> entities_to_process =
            map_entities_to_structure_for_processing_output(structure_indices, initial_entities_);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(entities_to_process[0]));
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
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], RowRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(entities_to_process[1]));
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
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], RowRange(4, 6));
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
    std::vector<std::vector<EntityId>> entities_to_process =
            map_entities_to_structure_for_processing_output(structure_indices, initial_entities_);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(entities_to_process[0]));
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
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], RowRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(entities_to_process[1]));
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
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], RowRange(4, 6));
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
    std::vector<std::vector<EntityId>> entities_to_process =
            map_entities_to_structure_for_processing_output(structure_indices, initial_entities_);
    const std::vector<EntityId> result_entities_0 = clause.process(std::move(entities_to_process[0]));
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
    ASSERT_EQ(*(*proc_0.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_0.col_ranges_)[1], RowRange(4, 6));

    const std::vector<EntityId> result_entities_1 = clause.process(std::move(entities_to_process[1]));
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
    ASSERT_EQ(*(*proc_1.col_ranges_)[0], RowRange(1, 4));
    ASSERT_EQ(*(*proc_1.col_ranges_)[1], RowRange(4, 6));
}