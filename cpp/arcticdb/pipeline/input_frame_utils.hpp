/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/pipeline/input_frame.hpp>
namespace arcticdb::pipelines {

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
} // namespace arcticdb::pipelines
