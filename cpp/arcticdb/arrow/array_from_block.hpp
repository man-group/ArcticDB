/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/column_store/memory_segment.hpp>

#include <sparrow.hpp>
#include <sparrow/layout/primitive_data_access.hpp>

namespace arcticdb {

inline std::optional<sparrow::validity_bitmap> create_validity_bitmap(size_t offset, const Column& column) {
    if(column.has_extra_buffer(offset, ExtraBufferType::BITMAP)) {
        auto &bitmap_buffer = column.get_extra_buffer(offset, ExtraBufferType::BITMAP);
        const auto bitmap_size = bitmap_buffer.block(0)->bytes() / sizeof(uint32_t);
        return sparrow::validity_bitmap{reinterpret_cast<uint8_t *>(bitmap_buffer.block(0)->release()), bitmap_size };
    } else {
        return std::nullopt;
    }
}

template <typename T>
sparrow::primitive_array<T> create_primitive_array(
        T* data_ptr,
        size_t data_size,
        std::optional<sparrow::validity_bitmap>& validity_bitmap) {
    sparrow::u8_buffer<T> buffer(data_ptr, data_size);
    if(validity_bitmap) {
        return sparrow::primitive_array<T>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<T>{std::move(buffer), data_size};
    }
}

template <>
inline sparrow::primitive_array<bool> create_primitive_array(
        bool* data_ptr,
        size_t data_size,
        std::optional<sparrow::validity_bitmap>& validity_bitmap) {
    // We need special handling for bools because arrow uses dense bool representation (i.e. 8 bools per byte)
    // Our internal representation is not dense. We use sparrow's `make_data_buffer` utility, but if needed, we can use
    // our own.
    auto buffer = sparrow::details::primitive_data_access<bool>::make_data_buffer(std::span{data_ptr, data_size});
    if(validity_bitmap) {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size};
    }
}

template <typename T>
sparrow::dictionary_encoded_array<T> create_dict_array(
    sparrow::array&& dict_values_array,
    sparrow::u8_buffer<T>&& dict_keys_buffer,
    std::optional<sparrow::validity_bitmap>& validity_bitmap
    ) {
    if(validity_bitmap) {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(dict_keys_buffer)),
            std::move(dict_values_array),
            std::move(*validity_bitmap)
        };
    } else {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(dict_keys_buffer)),
            std::move(dict_values_array),
        };
    }
}

template <typename TagType>
sparrow::array string_dict_from_block(
        TypedBlockData<TagType>& block,
        const Column& column,
        std::string_view name,
        std::optional<sparrow::validity_bitmap> maybe_bitmap) {
    const auto offset = block.offset();
    // We use 64-bit offsets and 32-bit keys because we use a layout where each row-segment has its own arrow array.
    // By default, the row-segments are 100k rows, so number of rows wouldn't exceed 32-bit ints.
    // However, string offsets could exceed 32-bits if total length of strings is big.
    // Hence, we're always using the bigstring arrow type.

    // String offsets must be signed `int64_t` even though they are unsigned indices due to arrow spec.
    // https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout
    auto &string_offsets = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
    const auto offset_buffer_value_count = string_offsets.block(0)->bytes() / sizeof(int64_t);
    sparrow::u8_buffer<int64_t> offset_buffer(reinterpret_cast<int64_t *>(string_offsets.block(0)->release()), offset_buffer_value_count);

    auto &strings = column.get_extra_buffer(offset, ExtraBufferType::STRING);
    const auto strings_buffer_size = strings.block(0)->bytes();
    sparrow::u8_buffer<char> strings_buffer(reinterpret_cast<char *>(strings.block(0)->release()), strings_buffer_size);

    // We use `int32_t` dictionary keys because pyarrow doesn't work with unsigned dictionary keys:
    // https://github.com/pola-rs/polars/issues/10977
    const auto block_size = block.row_count();
    sparrow::u8_buffer<int32_t> dict_keys_buffer{reinterpret_cast<int32_t *>(block.release()), block_size};

    sparrow::big_string_array dict_values_array(
        std::move(strings_buffer),
        std::move(offset_buffer)
    );

    auto dict_encoded = create_dict_array<int32_t>(
        sparrow::array{std::move(dict_values_array)},
        std::move(dict_keys_buffer),
        maybe_bitmap
    );

    sparrow::array arr{std::move(dict_encoded)};
    arr.set_name(name);
    return arr;
}

template <typename TagType>
sparrow::array arrow_array_from_block(
        TypedBlockData<TagType>& block,
        std::string_view name,
        std::optional<sparrow::validity_bitmap> maybe_bitmap) {
    using DataTagType = typename TagType::DataTypeTag;
    using RawType = typename DataTagType::raw_type;
    auto *data_ptr = block.release();
    const auto data_size = block.row_count();
    auto primitive_array = create_primitive_array<RawType>(data_ptr, data_size, maybe_bitmap);
    auto arr = sparrow::array{std::move(primitive_array)};
    arr.set_name(name);
    return arr;
}

}