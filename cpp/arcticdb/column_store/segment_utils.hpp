/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <ankerl/unordered_dense.h>
#include <arcticdb/util/type_traits.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <ranges>
#include <fmt/format.h>

namespace arcticdb {

class Column;

ankerl::unordered_dense::set<entity::position_t> unique_values_for_string_column(const Column& column);

std::vector<StreamDescriptor> split_descriptor(const StreamDescriptor& descriptor, size_t cols_per_segment);

template<std::ranges::sized_range T>
void fill_dense_column_data(SegmentInMemory& seg, const size_t column_index, const T& input_data) {
    using InputValueType = std::decay_t<std::ranges::range_value_t<T>>;
    constexpr static bool is_input_string_like = std::is_convertible_v<InputValueType, std::string_view>;
    const size_t row_count = std::ranges::size(input_data);
    details::visit_type(seg.descriptor().field(column_index).type().data_type(), [&](auto tdt) {
        using col_type_info = ScalarTypeInfo<decltype(tdt)>;
        using ColRawType = col_type_info::RawType;
        constexpr static bool is_sequence = is_sequence_type(col_type_info::data_type);
        if constexpr (is_input_string_like && is_sequence) {
            // Clang has a bug where it the for_each is just regular range-based for the constexpr if will not
            // the body of the if even if the condition is false. This leads to compile time errors because it tries
            // to call set_string with non-string values.
            // https://stackoverflow.com/questions/79817660/discarded-branch-of-c-constexpr-if-fails-compilation-because-it-calls-non-matc
            std::ranges::for_each(input_data, [&](const std::string_view& str) { seg.set_string(column_index, str); });
        } else if constexpr (!is_sequence && !is_input_string_like) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    std::is_same_v<ColRawType, InputValueType>,
                    "Type mismatch when setting data for Column[{}]. Column data type is {}.",
                    column_index,
                    col_type_info::data_type
            );
            Column& column_to_fill = seg.column(column_index);
            if constexpr (std::ranges::contiguous_range<T>) {
                std::memcpy(column_to_fill.ptr(), std::ranges::data(input_data), row_count * sizeof(InputValueType));
            } else {
                std::ranges::copy(input_data, column_to_fill.ptr_cast<ColRawType>(0, row_count));
            }
        } else {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    std::is_same_v<typename col_type_info::RawType, InputValueType>,
                    "Type mismatch when setting data for Column[{}]. Column data type is {}.",
                    column_index,
                    col_type_info::data_type
            );
        }
        seg.set_row_data(row_count - 1);
    });
}

template<std::ranges::sized_range... T>
static SegmentInMemory create_dense_segment(const StreamDescriptor& descriptor, const T&... columns) {
    constexpr static size_t input_column_count = sizeof...(T);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            input_column_count == descriptor.fields().size(),
            "When creating a dense segment in memory the number of columns ({}) must match the number of fields in "
            "the stream descriptor ({})",
            input_column_count,
            descriptor.fields().size()
    );
    if (input_column_count == 0) {
        return SegmentInMemory{};
    }
    const size_t expected_column_size = []<typename H, typename... Tail>(const H& head, const Tail&...) {
        return std::ranges::size(head);
    }(columns...);
    constexpr static AllocationType allocation_type = AllocationType::PRESIZED;
    constexpr static Sparsity sparsity = Sparsity::NOT_PERMITTED;
    auto result = SegmentInMemory(descriptor, expected_column_size, allocation_type, sparsity, std::nullopt);

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        (
                [&result, expected_column_size](size_t column_index, const auto& column_data) {
                    const size_t row_count = std::ranges::size(column_data);
                    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                            row_count == expected_column_size,
                            "When creating a dense segment all columns must have the same size. Column[0] has {} rows, "
                            "Column[{}] has {} rows",
                            expected_column_size,
                            column_index,
                            row_count
                    );
                    fill_dense_column_data(result, column_index, column_data);
                }(Is, std::get<Is>(std::tie(columns...))),
                ...
        );
    }(std::make_index_sequence<input_column_count>{});

    return result;
}

template<typename Tup1, typename Tup2>
requires util::instantiation_of<Tup1, std::tuple> && util::instantiation_of<Tup2, std::tuple>
auto tuple_cat_ref(const Tup1& tuple1, const Tup2& tuple2) {
    return std::apply(
            [&]<typename... T0>(T0&&... args1) {
                return std::apply(
                        [&]<typename... T1>(T1&&... args2) {
                            return std::tie(std::forward<T0>(args1)..., std::forward<T1>(args2)...);
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
        const size_t rows_per_segment, const pipelines::ColRange& col_range, std::vector<SegmentInMemory>& segments,
        std::vector<pipelines::ColRange>& col_ranges, std::vector<pipelines::RowRange>& row_ranges
) {
    const size_t total_rows = std::ranges::size(std::get<0>(column_slice));
    for (size_t start_row = 0; start_row < total_rows; start_row += rows_per_segment) {
        std::apply(
                [&]<std::ranges::sized_range... Cols>(const Cols&... cols) {
                    const size_t rows_to_take = std::min(rows_per_segment, total_rows - start_row);
                    segments.push_back(create_dense_segment(
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
auto split_pack(Data&&... data) {
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
        const size_t cols_per_segment, pipelines::ColRange col_range, std::vector<SegmentInMemory>& segments,
        std::vector<pipelines::ColRange>& col_ranges, std::vector<pipelines::RowRange>& row_ranges,
        const std::tuple<IndexCols&&>& index, ColumnSlice&& column_slice, CurrentCol&& current_col, RestCols&&... data
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

template<pipelines::ValidIndex index, std::ranges::sized_range... Data>
std::tuple<std::vector<SegmentInMemory>, std::vector<pipelines::ColRange>, std::vector<pipelines::RowRange>>
slice_data_into_segments(
        const StreamDescriptor& descriptor, const size_t rows_per_segment, const size_t cols_per_segment, Data&&... data
) {
    std::vector<StreamDescriptor> descriptors = split_descriptor(descriptor, cols_per_segment);
    std::vector<SegmentInMemory> segments;
    std::vector<pipelines::ColRange> col_ranges;
    std::vector<pipelines::RowRange> row_ranges;

    auto [index_columns, data_columns] = split_pack<index::field_count()>(std::forward<Data>(data)...);
    std::apply(
            [&]<typename... Cols>(Cols&&... cols) {
                slice_data_into_segments(
                        descriptors,
                        rows_per_segment,
                        cols_per_segment,
                        pipelines::ColRange{
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
} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::SegmentInMemory> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(const arcticdb::SegmentInMemory& segment, FormatContext& ctx) const {
        const StreamDescriptor& desc = segment.descriptor();
        fmt::format_to(ctx.out(), "Segment\n");
        for (unsigned i = 0; i < desc.field_count(); ++i) {
            fmt::format_to(ctx.out(), "\nColumn[{}]: {}\n", i, desc.field(i));
            visit_field(desc.field(i), [&](auto tdt) {
                using TDT = decltype(tdt);
                arcticdb::ColumnData cd = segment.column_data(i);
                for (auto it = cd.begin<TDT>(); it != cd.end<TDT>(); ++it) {
                    if constexpr (std::same_as<typename TDT::DataTypeTag::raw_type, int8_t>) {
                        fmt::format_to(ctx.out(), "{} ", i, int(*it));
                    } else {
                        fmt::format_to(ctx.out(), "{} ", i, *it);
                    }
                }
            });
        }
    }
};
} // namespace fmt