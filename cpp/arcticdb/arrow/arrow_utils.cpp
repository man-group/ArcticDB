/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/allocator.hpp>
#include <sparrow/layout/primitive_data_access.hpp>
#include <sparrow/record_batch.hpp>

namespace arcticdb {

std::optional<sparrow::validity_bitmap> create_validity_bitmap(
        size_t offset, const Column& column, size_t bitmap_size
) {
    if (column.has_extra_buffer(offset, ExtraBufferType::BITMAP)) {
        auto& bitmap_buffer = column.get_extra_buffer(offset, ExtraBufferType::BITMAP);
        util::check(
                bitmap_buffer.blocks().size() == 1,
                "Expected a single block bitmap extra buffer but got {} blocks",
                bitmap_buffer.blocks().size()
        );
        return sparrow::validity_bitmap{
                reinterpret_cast<uint8_t*>(bitmap_buffer.block(0)->release()), bitmap_size, get_detachable_allocator()
        };
    } else {
        return std::nullopt;
    }
}

template<typename T>
sparrow::primitive_array<T> create_primitive_array(
        T* data_ptr, size_t data_size, std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    sparrow::u8_buffer<T> buffer(data_ptr, data_size, get_detachable_allocator());
    if (validity_bitmap) {
        return sparrow::primitive_array<T>{std::move(buffer), data_size, std::move(*validity_bitmap)};
    } else {
        return sparrow::primitive_array<T>{std::move(buffer), data_size};
    }
}

template<>
sparrow::primitive_array<bool> create_primitive_array(
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
            reinterpret_cast<sparrow::zoned_time_without_timezone_nanoseconds*>(data_ptr),
            data_size,
            get_detachable_allocator()
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

sparrow::u8_buffer<char> minimal_string_buffer() {
    return sparrow::u8_buffer<char>(nullptr, 0, get_detachable_allocator());
}

// TODO: This is super hacky. If our string column return type is dictionary encoded, and this should be
//  consistent when there are zero rows or the validity bitmap is all zeros. But sparrow (or possibly Arrow) requires at
//  least one key-value pair in the dictionary, even if there are zero rows
sparrow::big_string_array minimal_strings_for_dict() {
    return sparrow::big_string_array(std::vector<std::string>{"a"});
}

template<typename OffsetType>
sparrow::array minimal_strings_array() {
    return sparrow::array(sparrow::string_array_impl<OffsetType>(std::vector<std::string>()));
}

template<typename TagType>
requires(is_sequence_type(TagType::DataTypeTag::data_type))
sparrow::array string_array_from_block(
        TypedBlockData<TagType>& block, const Column& column, std::string_view name,
        std::optional<sparrow::validity_bitmap>&& maybe_bitmap
) {
    using DataTagType = typename TagType::DataTypeTag;
    using RawType = typename DataTagType::raw_type;
    // Arrow spec expects signed integers as offsets even though all of them are unsigned
    using SignedType = std::make_signed_t<RawType>;
    auto offset = block.offset();
    auto block_size = block.row_count();
    util::check(
            block.mem_block()->bytes() == (block_size + 1) * sizeof(SignedType),
            "Expected memory block for variable length strings to have size {} due to extra bytes but got a memory "
            "block with size {}",
            (block_size + 1) * sizeof(SignedType),
            block.mem_block()->bytes()
    );
    sparrow::u8_buffer<SignedType> offset_buffer(
            reinterpret_cast<SignedType*>(block.release()), block_size + 1, get_detachable_allocator()
    );
    const bool has_string_buffer = column.has_extra_buffer(offset, ExtraBufferType::STRING);
    auto strings_buffer = [&]() {
        if (!has_string_buffer) {
            return minimal_string_buffer();
        }
        auto& strings = column.get_extra_buffer(offset, ExtraBufferType::STRING);
        const auto strings_buffer_size = strings.block(0)->bytes();
        return sparrow::u8_buffer<char>(
                reinterpret_cast<char*>(strings.block(0)->release()), strings_buffer_size, get_detachable_allocator()
        );
    }();
    auto arr = [&]() {
        if (maybe_bitmap.has_value()) {
            return sparrow::array(sparrow::string_array_impl<SignedType>(
                    std::move(strings_buffer), std::move(offset_buffer), std::move(maybe_bitmap.value())
            ));
        } else {
            return sparrow::array(
                    sparrow::string_array_impl<SignedType>(std::move(strings_buffer), std::move(offset_buffer))
            );
        }
    }();
    arr.set_name(name);
    return arr;
}

template<typename TagType>
requires(is_sequence_type(TagType::DataTypeTag::data_type))
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
    sparrow::u8_buffer<int32_t> dict_keys_buffer{
            reinterpret_cast<int32_t*>(block.release()), block_size, get_detachable_allocator()
    };

    const bool has_offset_buffer = column.has_extra_buffer(offset, ExtraBufferType::OFFSET);
    const bool has_string_buffer = column.has_extra_buffer(offset, ExtraBufferType::STRING);
    auto dict_values_array = [&]() -> sparrow::big_string_array {
        if (has_offset_buffer && has_string_buffer) {
            auto& string_offsets = column.get_extra_buffer(offset, ExtraBufferType::OFFSET);
            const auto offset_buffer_value_count = string_offsets.block(0)->bytes() / sizeof(int64_t);
            sparrow::u8_buffer<int64_t> offsets_buffer(
                    reinterpret_cast<int64_t*>(string_offsets.block(0)->release()),
                    offset_buffer_value_count,
                    get_detachable_allocator()
            );
            auto& strings = column.get_extra_buffer(offset, ExtraBufferType::STRING);
            const auto strings_buffer_size = strings.block(0)->bytes();
            sparrow::u8_buffer<char> strings_buffer(
                    reinterpret_cast<char*>(strings.block(0)->release()),
                    strings_buffer_size,
                    get_detachable_allocator()
            );
            return {std::move(strings_buffer), std::move(offsets_buffer)};
        } else if (!has_offset_buffer && !has_string_buffer) {
            return minimal_strings_for_dict();
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

sparrow::array empty_arrow_array_for_column(const Column& column, std::string_view name) {
    auto res = details::visit_scalar(column.type(), [&](auto&& tdt) {
        using TagType = std::decay_t<decltype(tdt)>;
        using DataTagType = typename TagType::DataTypeTag;
        using RawType = typename DataTagType::raw_type;
        std::optional<sparrow::validity_bitmap> validity_bitmap;
        if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
            using SignedType = std::make_signed_t<RawType>;
            if (column.data().buffer().has_extra_bytes_per_block()) {
                return minimal_strings_array<SignedType>();
            } else {
                sparrow::u8_buffer<int32_t> dict_keys_buffer{nullptr, 0, get_detachable_allocator()};
                auto dict_values_array = minimal_strings_for_dict();
                return sparrow::array{create_dict_array<int32_t>(
                        sparrow::array{std::move(dict_values_array)},
                        std::move(dict_keys_buffer),
                        std::move(validity_bitmap)
                )};
            }
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
            vec.emplace_back(empty_arrow_array_for_column(column, name));
        }
        while (auto block = column_data.next<TagType>()) {
            auto bitmap = create_validity_bitmap(block->offset(), column, block->row_count());
            if constexpr (is_sequence_type(TagType::DataTypeTag::data_type)) {
                if (column_data.buffer().has_extra_bytes_per_block()) {
                    vec.emplace_back(string_array_from_block<TagType>(*block, column, name, std::move(bitmap)));
                } else {
                    vec.emplace_back(string_dict_from_block<TagType>(*block, column, name, std::move(bitmap)));
                }
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
    if (num_columns == 0) {
        // We can't construct a record batch with no columns, so in this case we return an empty list of record batches,
        // which needs special handling in python.
        return std::make_shared<std::vector<sparrow::record_batch>>();
    }
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

DataType arcticdb_type_from_arrow_array(const sparrow::array& array) {
    schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
            !array.dictionary().has_value(), "Dictionary-encoded Arrow data unsupported"
    );
    switch (array.data_type()) {
    case sparrow::data_type::BOOL:
        return DataType::BOOL8;
    case sparrow::data_type::UINT8:
        return DataType::UINT8;
    case sparrow::data_type::UINT16:
        return DataType::UINT16;
    case sparrow::data_type::UINT32:
        return DataType::UINT32;
    case sparrow::data_type::UINT64:
        return DataType::UINT64;
    case sparrow::data_type::INT8:
        return DataType::INT8;
    case sparrow::data_type::INT16:
        return DataType::INT16;
    case sparrow::data_type::INT32:
        return DataType::INT32;
    case sparrow::data_type::INT64:
        return DataType::INT64;
    case sparrow::data_type::FLOAT:
        return DataType::FLOAT32;
    case sparrow::data_type::DOUBLE:
        return DataType::FLOAT64;
    case sparrow::data_type::TIMESTAMP_NANOSECONDS:
        return DataType::NANOSECONDS_UTC64;
    case sparrow::data_type::STRING:
        return DataType::UTF_DYNAMIC32;
    case sparrow::data_type::LARGE_STRING:
        return DataType::UTF_DYNAMIC64;
    default:
        schema::raise<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                "Unsupported Arrow data type provided `{}`", sparrow::data_type_to_format(array.data_type())
        );
        return DataType::UNKNOWN; // Prevent "control reaches end of non-void function"
    }
}

std::pair<std::vector<DataType>, std::optional<size_t>> find_data_types_and_index_position(
        const sparrow::record_batch& record_batch, const std::optional<std::string>& index_name
) {
    std::vector<DataType> data_types;
    data_types.reserve(record_batch.nb_columns());
    std::optional<size_t> index_column_position;
    for (size_t idx = 0; idx < record_batch.nb_columns(); ++idx) {
        if (index_name.has_value() && record_batch.get_column_name(idx) == *index_name) {
            index_column_position = idx;
        }
        data_types.emplace_back(arcticdb_type_from_arrow_array(record_batch.get_column(idx)));
    }
    if (index_name.has_value()) {
        schema::check<ErrorCode::E_COLUMN_DOESNT_EXIST>(
                index_column_position.has_value(), "Specified index column named '{}' not present in data", *index_name
        );
    }
    return {std::move(data_types), index_column_position};
}

std::pair<SegmentInMemory, std::optional<size_t>> arrow_data_to_segment(
        const std::vector<sparrow::record_batch>& record_batches, const std::optional<std::string>& index_name
) {
    SegmentInMemory seg;
    if (record_batches.empty()) {
        return {seg, std::nullopt};
    }
    auto record_batch = record_batches.cbegin();
    auto [data_types, index_column_position] = find_data_types_and_index_position(*record_batch, index_name);
    uint64_t total_rows = std::accumulate(
            record_batches.cbegin(),
            record_batches.cend(),
            uint64_t(0),
            [](const uint64_t& accum, const sparrow::record_batch& record_batch) {
                return accum + record_batch.nb_rows();
            }
    );
    auto column_names = record_batches.front().names();
    std::vector<Column> columns;
    columns.reserve(data_types.size());
    for (auto data_type : data_types) {
        // The Arrow data may be semantically sparse, but this buffer is still dense, hence Sparsity::NOT_PERMITTED
        columns.emplace_back(make_scalar_type(data_type), Sparsity::NOT_PERMITTED, ChunkedBuffer());
    }
    uint64_t start_row{0};
    for (; record_batch != record_batches.cend(); ++record_batch) {
        for (size_t idx = 0; idx < record_batch->nb_columns(); ++idx) {
            auto& column = columns[idx];
            const auto& data_type = data_types[idx];
            const auto& array = record_batch->get_column(idx);
            auto [arrow_array, arrow_schema] = sparrow::get_arrow_structures(array);
            schema::check<ErrorCode::E_UNSUPPORTED_COLUMN_TYPE>(
                    array.null_count() == 0,
                    "Column '{}' contains null values, which are not currently supported",
                    record_batch->names()[idx]
            );
            auto arrow_array_buffers = sparrow::get_arrow_array_buffers(*arrow_array, *arrow_schema);
            // arrow_array_buffers[0] seems to be the validity bitmap, and may be NULL if there are no null values
            // need to handle it being non-NULL and all 1s though
            const auto* data = arrow_array_buffers[1].data<uint8_t>();
            if (is_bool_type(data_type)) {
                // Arrow bool columns are packed bitsets
                // This is the limiting factor when writing a lot of bool data as it is serial. This should be moved to
                // WriteToSegmentTask
                if (record_batch == record_batches.cbegin()) {
                    column.buffer() = ChunkedBuffer::presized(total_rows);
                }
                packed_bits_to_buffer(
                        data, array.size(), array.offset(), column.buffer().bytes_at(start_row, array.size())
                );
            } else { // Numeric and string types
                data += array.offset() * get_type_size(data_type);
                // For string columns, we deliberately omit the last value from the offsets buffer to keep our indexing
                // into the column's ChunkedBuffer accurate. See corresponding comment in
                // WriteToSegmentTask::slice_column
                const auto bytes = array.size() * get_type_size(data_type);
                column.buffer().add_external_block(data, bytes);
                if (is_sequence_type(data_type)) {
                    // arrow_array_buffers[2] is the buffer that contains the actual strings. The data pointer
                    // represents offsets into this buffer
                    ChunkedBuffer strings_buffer;
                    const auto string_bytes = arrow_array_buffers[2].size();
                    strings_buffer.add_external_block(arrow_array_buffers[2].data<uint8_t>(), string_bytes);
                    column.set_extra_buffer(
                            start_row * get_type_size(data_type), ExtraBufferType::STRING, std::move(strings_buffer)
                    );
                }
            }
        }
        start_row += record_batch->nb_rows();
    }
    if (index_column_position.has_value()) {
        seg.add_column(
                column_names[*index_column_position],
                std::make_shared<Column>(std::move(columns[*index_column_position]))
        );
    }
    for (size_t idx = 0; idx < column_names.size(); ++idx) {
        if (!index_column_position.has_value() || idx != *index_column_position) {
            seg.add_column(column_names[idx], std::make_shared<Column>(std::move(columns[idx])));
        }
    }
    seg.set_row_data(static_cast<ssize_t>(total_rows) - 1);
    return {seg, index_column_position};
}

} // namespace arcticdb
