/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>

#include <sparrow/layout/primitive_data_access.hpp>
#include <sparrow/record_batch.hpp>

namespace arcticdb {

inline std::optional<sparrow::validity_bitmap> create_validity_bitmap(
        size_t offset, const Column& column, size_t bitmap_size
) {
    if (column.has_extra_buffer(offset, ExtraBufferType::BITMAP)) {
        auto& bitmap_buffer = column.get_extra_buffer(offset, ExtraBufferType::BITMAP);
        util::check(
                bitmap_buffer.blocks().size() == 1,
                "Expected a single block bitmap extra buffer but got {} blocks",
                bitmap_buffer.blocks().size()
        );
        return sparrow::validity_bitmap{reinterpret_cast<uint8_t*>(bitmap_buffer.block(0)->release()), bitmap_size};
    } else {
        return std::nullopt;
    }
}

template<typename T>
sparrow::primitive_array<T> create_primitive_array(
        T* data_ptr, size_t data_size, std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    sparrow::u8_buffer<T> buffer(data_ptr, data_size);
    if (validity_bitmap) {
        return sparrow::primitive_array<T>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<T>{std::move(buffer), data_size};
    }
}

template<>
inline sparrow::primitive_array<bool> create_primitive_array(
        bool* data_ptr, size_t data_size, std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    // We need special handling for bools because arrow uses dense bool representation (i.e. 8 bools per byte)
    // Our internal representation is not dense. We use sparrow's `make_data_buffer` utility, but if needed, we can use
    // our own.
    auto buffer = sparrow::details::primitive_data_access<bool>::make_data_buffer(std::span{data_ptr, data_size});
    if (validity_bitmap) {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<bool>{std::move(buffer), data_size};
    }
}

template<typename T>
sparrow::timestamp_without_timezone_nanoseconds_array create_timestamp_array(
        T* data_ptr, size_t data_size, std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    static_assert(sizeof(T) == sizeof(sparrow::zoned_time_without_timezone_nanoseconds));
    // We default to using timestamps without timezones. If the normalization metadata contains a timezone it will be
    // applied during normalization in python layer.
    sparrow::u8_buffer<sparrow::zoned_time_without_timezone_nanoseconds> buffer(
            reinterpret_cast<sparrow::zoned_time_without_timezone_nanoseconds*>(data_ptr), data_size
    );
    if (validity_bitmap) {
        return sparrow::timestamp_without_timezone_nanoseconds_array{
                std::move(buffer), data_size, std::move(*validity_bitmap)
        };
    } else {
        return sparrow::timestamp_without_timezone_nanoseconds_array{std::move(buffer), data_size};
    }
}

template<typename T>
sparrow::dictionary_encoded_array<T> create_dict_array(
        sparrow::array&& dict_values_array, sparrow::u8_buffer<T>&& dict_keys_buffer,
        std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    if (validity_bitmap) {
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

// TODO: This is super hacky. Our string column return type is dictionary encoded, and this should be
//  consistent when there are zero rows or the validity bitmap is all zeros. But sparrow (or possibly Arrow) requires at
//  least one key-value pair in the dictionary, even if there are zero rows
inline sparrow::big_string_array minimal_strings_dict() {
    std::unique_ptr<int64_t[]> offset_ptr(new int64_t[2]);
    offset_ptr[0] = 0;
    offset_ptr[1] = 1;
    sparrow::u8_buffer<int64_t> offsets_buffer(offset_ptr.release(), 2);
    std::unique_ptr<char[]> strings_ptr(new char[1]);
    strings_ptr[0] = 'a';
    sparrow::u8_buffer<char> strings_buffer(strings_ptr.release(), 1);
    return {std::move(strings_buffer), std::move(offsets_buffer)};
}

template<typename TagType>
sparrow::array string_dict_from_block(
        TypedBlockData<TagType>& block, const Column& column, std::string_view name,
        std::optional<sparrow::validity_bitmap>&& maybe_bitmap
) {
    const auto offset = block.offset();
    // We use 64-bit offsets and 32-bit keys because we use a layout where each row-segment has its own arrow array.
    // By default, the row-segments are 100k rows, so number of rows wouldn't exceed 32-bit ints.
    // However, string offsets could exceed 32-bits if total length of strings is big.
    // Hence, we're always using the bigstring arrow type.

    // String offsets must be signed `int64_t` even though they are unsigned indices due to arrow spec.
    // https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-layout
    // We use `int32_t` dictionary keys because pyarrow doesn't work with unsigned dictionary keys:
    // https://github.com/pola-rs/polars/issues/10977
    const auto block_size = block.row_count();
    sparrow::u8_buffer<int32_t> dict_keys_buffer{reinterpret_cast<int32_t*>(block.release()), block_size};

    const bool has_offset_buffer = column.has_extra_buffer(offset, ExtraBufferType::OFFSET);
    const bool has_string_buffer = column.has_extra_buffer(offset, ExtraBufferType::STRING);
    auto dict_values_array = [&]() -> sparrow::big_string_array {
        if (has_offset_buffer && has_string_buffer) {
            auto& string_offsets = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
            const auto offset_buffer_value_count = string_offsets.block(0)->bytes() / sizeof(int64_t);
            sparrow::u8_buffer<int64_t> offsets_buffer(
                    reinterpret_cast<int64_t*>(string_offsets.block(0)->release()), offset_buffer_value_count
            );
            auto& strings = column.get_extra_buffer(offset, ExtraBufferType::STRING);
            const auto strings_buffer_size = strings.block(0)->bytes();
            sparrow::u8_buffer<char> strings_buffer(
                    reinterpret_cast<char*>(strings.block(0)->release()), strings_buffer_size
            );
            return {std::move(strings_buffer), std::move(offsets_buffer)};
        } else if (!has_offset_buffer && !has_string_buffer) {
            return minimal_strings_dict();
        } else {
            util::raise_rte("Arrow output string creation expected either both or neither of OFFSET and STRING buffers "
                            "to be present");
        }
    }();

    auto dict_encoded = create_dict_array<int32_t>(
            sparrow::array{std::move(dict_values_array)}, std::move(dict_keys_buffer), std::move(maybe_bitmap)
    );

    sparrow::array arr{std::move(dict_encoded)};
    arr.set_name(name);
    return arr;
}

template<typename TagType>
sparrow::array arrow_array_from_block(
        TypedBlockData<TagType>& block, std::string_view name, std::optional<sparrow::validity_bitmap>&& maybe_bitmap
) {
    using DataTagType = typename TagType::DataTypeTag;
    using RawType = typename DataTagType::raw_type;
    auto* data_ptr = block.release();
    const auto data_size = block.row_count();
    auto arr = [&]() {
        if constexpr (is_time_type(TagType::DataTypeTag::data_type)) {
            auto timestamp_array = create_timestamp_array<RawType>(data_ptr, data_size, std::move(maybe_bitmap));
            return sparrow::array{std::move(timestamp_array)};
        } else {
            auto primitive_array = create_primitive_array<RawType>(data_ptr, data_size, std::move(maybe_bitmap));
            return sparrow::array{std::move(primitive_array)};
        }
    }();
    arr.set_name(name);
    return arr;
}

sparrow::array empty_arrow_array_from_type(const TypeDescriptor& type, std::string_view name) {
    auto res = details::visit_scalar(type, [](auto&& tdt) {
        using TagType = std::decay_t<decltype(tdt)>;
        using DataTagType = typename TagType::DataTypeTag;
        using RawType = typename DataTagType::raw_type;
        std::optional<sparrow::validity_bitmap> validity_bitmap;
        if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
            sparrow::u8_buffer<int32_t> dict_keys_buffer{nullptr, 0};
            auto dict_values_array = minimal_strings_dict();
            return sparrow::array{create_dict_array<int32_t>(
                    sparrow::array{std::move(dict_values_array)},
                    std::move(dict_keys_buffer),
                    std::move(validity_bitmap)
            )};
        } else if constexpr (is_time_type(TagType::DataTypeTag::data_type)) {
            return sparrow::array{create_timestamp_array<RawType>(nullptr, 0, std::move(validity_bitmap))};
        } else {
            return sparrow::array{create_primitive_array<RawType>(nullptr, 0, std::move(validity_bitmap))};
        }
    });
    res.set_name(name);
    return res;
}

std::vector<sparrow::array> arrow_arrays_from_column(const Column& column, std::string_view name) {
    std::vector<sparrow::array> vec;
    auto column_data = column.data();
    vec.reserve(column.num_blocks());
    details::visit_scalar(column.type(), [&vec, &column_data, &column, name](auto&& impl) {
        using TagType = std::decay_t<decltype(impl)>;
        if (column_data.num_blocks() == 0) {
            // For empty columns we want to return one empty array instead of no arrays.
            vec.emplace_back(empty_arrow_array_from_type(column.type(), name));
        }
        while (auto block = column_data.next<TagType>()) {
            auto bitmap = create_validity_bitmap(block->offset(), column, block->row_count());
            if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
                vec.emplace_back(string_dict_from_block<TagType>(*block, column, name, std::move(bitmap)));
            } else {
                vec.emplace_back(arrow_array_from_block<TagType>(*block, name, std::move(bitmap)));
            }
        }
    });
    return vec;
}

std::shared_ptr<std::vector<sparrow::record_batch>> segment_to_arrow_data(SegmentInMemory& segment) {
    const auto total_blocks = segment.num_blocks();
    const auto num_columns = segment.num_columns();
    const auto column_blocks = segment.column(0).num_blocks();
    util::check(total_blocks == column_blocks * num_columns, "Expected regular block size");

    // column_blocks == 0 is a special case where we are returning a zero-row structure (e.g. if date_range is
    // provided outside of the time range covered by the symbol)
    auto output = std::make_shared<std::vector<sparrow::record_batch>>(
            column_blocks == 0 ? 1 : column_blocks, sparrow::record_batch{}
    );
    for (auto i = 0UL; i < num_columns; ++i) {
        auto& column = segment.column(static_cast<position_t>(i));
        util::check(
                column.num_blocks() == column_blocks,
                "Non-standard column block number: {} != {}",
                column.num_blocks(),
                column_blocks
        );

        auto column_arrays = arrow_arrays_from_column(column, segment.field(i).name());
        util::check(
                column_arrays.size() == output->size(),
                "Unexpected number of arrow arrays returned: {} != {}",
                column_arrays.size(),
                output->size()
        );

        for (auto block_idx = 0UL; block_idx < column_arrays.size(); ++block_idx) {
            util::check(block_idx < output->size(), "Block index overflow {} > {}", block_idx, output->size());
            (*output)[block_idx].add_column(
                    static_cast<std::string>(segment.field(i).name()), std::move(column_arrays[block_idx])
            );
        }
    }
    return output;
}

} // namespace arcticdb
