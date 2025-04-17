/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/arrow/arrow_data.hpp>
#include <arcticdb/entity/frame_and_descriptor.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <arcticdb/arrow/include_sparrow.hpp>

namespace arcticdb {

std::optional<sparrow::validity_bitmap> create_validity_bitmap(size_t offset, const Column& column) {
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
        std::optional<sparrow::validity_bitmap> validity_bitmap) {
    // TODO: Sparrow seems to have a bug with u8_buffer destruction after move. Using copying with a vector for now
    // sparrow::u8_buffer<T> buffer(data_ptr, data_size);
    std::vector<T> data(data_ptr, data_ptr + data_size);
    if(validity_bitmap)
        return sparrow::primitive_array<T>{std::move(data), std::move(*validity_bitmap)};
    else
        return sparrow::primitive_array<T>{std::move(data)};
}

template <typename T>
sparrow::dictionary_encoded_array<T> create_dict_array(
    sparrow::array&& dict_array,
    // TODO: Must use u8_buffer to do this no-copy
    std::vector<T>&& string_offsets,
    std::optional<sparrow::validity_bitmap>& validity_bitmap
    ) {
    if(validity_bitmap) {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(string_offsets)),
            std::move(dict_array),
            std::move(*validity_bitmap)
        };
    } else {
        return sparrow::dictionary_encoded_array<T>{
            typename sparrow::dictionary_encoded_array<T>::keys_buffer_type(std::move(string_offsets)),
            std::move(dict_array),
            std::vector<size_t>{}
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
    auto &dict_keys = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
    const auto offset_buffer_size = dict_keys.block(0)->bytes() / sizeof(uint32_t);
    // sparrow::u8_buffer<uint32_t> offset_buffer(reinterpret_cast<uint32_t *>(dict_keys.block(0)->release()), offset_buffer_size);
    auto* offset_buffer_data = reinterpret_cast<uint32_t *>(dict_keys.block(0)->release());
    auto offset_buffer = std::vector<uint32_t>(offset_buffer_data, offset_buffer_data + offset_buffer_size);

    auto &dict_values = column.get_extra_buffer(offset, ExtraBufferType::STRING);
    const auto data_buffer_size = dict_values.block(0)->bytes();
    sparrow::u8_buffer<char> data_buffer(reinterpret_cast<char *>(dict_values.block(0)->release()), data_buffer_size);
    // auto* data_buffer_data = reinterpret_cast<char *>(dict_values.block(0)->release());
    // auto data_buffer = std::vector<char>(data_buffer_data, data_buffer_data + data_buffer_size);

    const auto block_size = block.row_count();
    // sparrow::u8_buffer<uint32_t> string_offsets{reinterpret_cast<uint32_t *>(block.release()), block_size};
    auto* string_offsets_data = reinterpret_cast<uint32_t *>(block.release());
    auto string_offsets = std::vector<uint32_t>(string_offsets_data, string_offsets_data+block_size);

    // TODO: This still segfaults even when using a string offsets and an offset buffer as vectors
    sparrow::string_array arrow_dict(
        std::move(data_buffer),
        std::move(offset_buffer)
    );

    auto dict_encoded = create_dict_array<uint32_t>(
        sparrow::array{std::move(arrow_dict)},
        std::move(string_offsets),
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