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

template<std::ranges::contiguous_range T>
NativeTensor one_dimensional_tensor(const T& data, const DataType data_type) {
    using ValueType = std::decay_t<std::ranges::range_value_t<T>>;
    constexpr static size_t element_size = sizeof(ValueType);
    constexpr shape_t shapes = 1;
    const int64_t byte_size = data.size() * element_size;
    return NativeTensor{byte_size, 1, nullptr, &shapes, data_type, element_size, std::ranges::data(data), 1};
}

/// Generate InputFrame from given descriptor and input data
/// @returns pair of the InputFrame and tuple of std::vector elements. Each element of the tuple is the owner of the
///     corresponding column in the InputFrame.
/// @note The InputFrame class does not own the data by design. The second element in the return value must be kept
///     alive as long as the InputFrame is used.
template<ValidIndex Index, typename... T>
requires(
        (Index::field_count() == 0 || Index::field_count() == 1) && (std::ranges::sized_range<T> && ...) &&
        // strings are not supported yet, in order to support them we need to initialise python strings
        (!std::convertible_to<std::ranges::range_value_t<T>, std::string_view> && ...)
)
auto input_frame_from_tensors(const StreamDescriptor& desc, T&&... input) {
    constexpr static size_t data_columns = sizeof...(T) - Index::field_count();
    // TODO: If the range is a vector move the vector in the materialized output
    std::tuple materialized_input{std::vector<std::conditional_t<
            std::same_as<std::ranges::range_value_t<T>, bool>,
            uint8_t,
            std::ranges::range_value_t<T>>>(
            std::make_move_iterator(std::begin(input)), std::make_move_iterator(std::end(input))
    )...};
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        const size_t first_row_count = std::get<0>(materialized_input).size();
        util::check(
                ((std::ranges::size(std::get<Is>(materialized_input)) == first_row_count) && ...),
                "All input data must have the same number of rows"
        );
        util::check(
                ((desc.field(Is).type().visit_tag([&](auto tag) {
                     using RawType = std::decay_t<decltype(tag)>::DataTypeTag::raw_type;
                     return sizeof(RawType);
                 }) == sizeof(std::ranges::range_value_t<std::tuple_element_t<Is, decltype(materialized_input)>>)) &&
                 ...),
                "Input data raw type sizes do not match descriptor field raw type sizes"
        );
    }(std::make_index_sequence<sizeof...(T)>{});
    std::vector<NativeTensor> tensors = [&]<size_t... Is>(std::index_sequence<Is...>) {
        std::vector<NativeTensor> result_tensors;
        result_tensors.reserve(data_columns);
        (result_tensors.push_back(one_dimensional_tensor(
                 std::get<Is + Index::field_count()>(materialized_input),
                 desc.field(Is + Index::field_count()).type().data_type()
         )),
         ...);
        return result_tensors;
    }(std::make_index_sequence<data_columns>{});
    const size_t num_rows = std::ranges::size(std::get<0>(materialized_input));
    if constexpr (Index::field_count() == 1) {
        NativeTensor index_tensor =
                one_dimensional_tensor(std::get<0>(materialized_input), desc.field(0).type().data_type());
        InputFrame result_frame(desc, std::move(tensors), Index{desc.field(0).name()}, std::move(index_tensor));
        result_frame.num_rows = num_rows;
        result_frame.set_index_range();
        return std::pair{std::move(result_frame), std::move(materialized_input)};
    } else {
        InputFrame result_frame(desc, std::move(tensors), Index{}, std::nullopt);
        result_frame.num_rows = num_rows;
        result_frame.set_index_range();
        return std::pair{std::move(result_frame), std::move(materialized_input)};
    }
}
} // namespace arcticdb::pipelines
