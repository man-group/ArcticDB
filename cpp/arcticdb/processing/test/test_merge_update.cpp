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

using namespace arcticdb;

constexpr static std::array non_string_fields = {
        FieldRef(TypeDescriptor(DataType::INT8, Dimension::Dim0), "int8"),
        FieldRef(TypeDescriptor(DataType::UINT32, Dimension::Dim0), "uint32"),
        FieldRef(TypeDescriptor(DataType::BOOL8, Dimension::Dim0), "bool8"),
        FieldRef(TypeDescriptor(DataType::FLOAT32, Dimension::Dim0), "float32"),
        FieldRef(TypeDescriptor(DataType::NANOSECONDS_UTC64, Dimension::Dim0), "timestamp")
};

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

template<size_t N, typename... Data>
auto take(Data&&... data) {
    constexpr size_t to_take = std::min(N, sizeof...(Data));
    return std::pair{
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                return std::tuple{std::get<Is>(std::forward_as_tuple(std::forward<Data>(data)...))...};
            }(std::make_index_sequence<to_take>{}),
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                return std::tuple{std::get<Is + to_take>(std::forward_as_tuple(std::forward<Data>(data)...))...};
            }(std::make_index_sequence<sizeof...(Data) - to_take>{})
    };
}

template<size_t N, typename TupleT>
requires(util::instantiation_of<TupleT, std::tuple>)
auto take(TupleT&& t) {
    constexpr size_t to_take = std::min(N, std::tuple_size_v<TupleT>);
    return std::pair{
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                return std::tuple{std::get<Is>(t)...};
            }(std::make_index_sequence<to_take>{}),
            [&]<size_t... Is>(std::index_sequence<Is...>) {
                return std::tuple{std::get<Is + to_take>(t)...};
            }(std::make_index_sequence<std::tuple_size_v<TupleT> - to_take>{}),
    };
}

template<size_t cols_per_segment, typename IndexCols, std::ranges::sized_range... Data>
requires(cols_per_segment > 0)
void slice_data_into_segments(
        const std::span<const StreamDescriptor> descriptors, const size_t col_slice, const size_t rows_per_segment,
        std::vector<SegmentInMemoryImpl>& segments, std::vector<ColRange>& col_ranges,
        std::vector<RowRange>& row_ranges, IndexCols&& index, Data&&... data
) {
    constexpr size_t index_columns_count = std::tuple_size_v<std::decay_t<IndexCols>>;
    if constexpr (sizeof...(Data) > 0) {
        const StreamDescriptor& desc = descriptors[col_slice];
        constexpr static size_t current_columns_count = std::min(cols_per_segment, sizeof...(Data));
        auto [current, rest] = take<current_columns_count>(std::forward<Data>(data)...);
        const size_t total_rows = std::ranges::size(std::get<0>(current));
        auto current_with_index =
                std::tuple_cat(std::forward<IndexCols>(index), std::forward<decltype(current)>(current));
        for (size_t start_row = 0; start_row < total_rows; start_row += rows_per_segment) {
            std::apply(
                    [&]<typename... Cols>(const Cols&... cols) {
                        const size_t rows_to_take = std::min(rows_per_segment, total_rows - start_row);
                        segments.push_back(SegmentInMemoryImpl::create_dense_segment(
                                desc, std::ranges::take_view(std::ranges::drop_view(cols, start_row), rows_to_take)...
                        ));
                        row_ranges.emplace_back(start_row, start_row + rows_to_take);
                        col_ranges.emplace_back(
                                index_columns_count + col_slice * cols_per_segment,
                                index_columns_count + col_slice * cols_per_segment +
                                        std::min(cols_per_segment, current_columns_count)
                        );
                    },
                    current_with_index
            );
        }
        auto [next_index, _] =
                take<index_columns_count>(std::forward<decltype(current_with_index)>(current_with_index));
        std::apply(
                [&](auto&&... cols) {
                    slice_data_into_segments<cols_per_segment>(
                            descriptors,
                            col_slice + 1,
                            rows_per_segment,
                            segments,
                            col_ranges,
                            row_ranges,
                            std::forward<decltype(next_index)>(next_index),
                            cols...
                    );
                },
                std::forward<decltype(rest)>(rest)
        );
    }
}

template<ValidIndex index, size_t cols_per_segment, std::ranges::sized_range... Data>
std::tuple<std::vector<SegmentInMemoryImpl>, std::vector<ColRange>, std::vector<RowRange>> slice_data_into_segments(
        size_t rows_per_segment, const StreamDescriptor& descriptor, Data&&... data
) {
    std::vector<StreamDescriptor> descriptors = split_descriptor(descriptor, cols_per_segment);
    auto [index_data, non_index] = take<index::field_count()>(std::forward<Data>(data)...);
    std::vector<SegmentInMemoryImpl> segments;
    std::vector<ColRange> col_ranges;
    std::vector<RowRange> row_ranges;
    std::apply(
            [&]<typename... NonIndexCols>(NonIndexCols&&... cols) {
                slice_data_into_segments<cols_per_segment>(
                        descriptors,
                        0,
                        rows_per_segment,
                        segments,
                        col_ranges,
                        row_ranges,
                        std::forward<decltype(index_data)>(index_data),
                        std::forward<NonIndexCols>(cols)...
                );
            },
            std::forward<decltype(non_index)>(non_index)
    );
    return {std::move(segments), std::move(col_ranges), std::move(row_ranges)};
}

template<ValidIndex index, std::ranges::sized_range... Data>
std::vector<SegmentInMemoryImpl> slice_data_into_segments(
        size_t rows_per_segment, const StreamDescriptor& descriptor, Data&&... data
) {
    return slice_data_into_segments<index, 127>(rows_per_segment, descriptor, std::forward<Data>(data)...);
}

TEST(MergeUpdateUpdateTimeseries, SourceIndexMatchesAllSegments) {
    using namespace std::ranges;
    using stream::TimeseriesIndex;
    constexpr static auto strategy =
            MergeStrategy{.matched = MergeAction::UPDATE, .not_matched_by_target = MergeAction::DO_NOTHING};
    constexpr static size_t columns_per_segment = 3;
    constexpr static size_t rows_per_segment = 10;
    StreamDescriptor source_descriptor =
            TimeseriesIndex::default_index().create_stream_descriptor("Source", non_string_fields);
    auto [target_segments, col_ranges, row_ranges] = slice_data_into_segments<TimeseriesIndex, columns_per_segment>(
            rows_per_segment,
            source_descriptor,
            iota_view(timestamp{0}, timestamp{30}),
            iota_view(static_cast<int8_t>(0), static_cast<int8_t>(30)),
            iota_view(static_cast<unsigned>(0), static_cast<unsigned>(30)),
            std::array{true,  false, true,  true,  false, false, true,  false, true,  false,
                       true,  true,  false, true,  false, false, true,  true,  false, true,
                       false, true,  false, false, true,  true,  false, true,  false, true},
            iota_view(0, 30) | views::transform([](auto x) { return static_cast<float>(x); }),
            iota_view(timestamp{0}, timestamp{30})
    );

    constexpr std::array<timestamp, 3> source_index{1, 12, 25};
    InputFrame source(
            std::move(source_descriptor),
            create_one_dimensional_tensors(
                    std::pair{std::array<int8_t, 3>{10, 20, 30}, TypeDescriptor::scalar_type(DataType::INT8)},
                    std::pair{std::array<uint32_t, 3>{100, 200, 300}, TypeDescriptor::scalar_type(DataType::UINT32)},
                    std::pair{std::array{true, false, true}, TypeDescriptor::scalar_type(DataType::BOOL8)},
                    std::pair{std::array{11.1f, 22.2f, 33.3f}, TypeDescriptor::scalar_type(DataType::FLOAT32)},
                    std::pair{
                            std::array<timestamp, 3>{1000, 2000, 3000},
                            TypeDescriptor::scalar_type(DataType::NANOSECONDS_UTC64)
                    }
            ),
            NativeTensor::one_dimensional_tensor(source_index, DataType::NANOSECONDS_UTC64)
    );
    MergeUpdateClause clause({}, strategy, std::make_shared<InputFrame>(std::move(source)), true);
}