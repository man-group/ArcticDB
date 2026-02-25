/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
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

sparrow::u8_buffer<char> minimal_string_buffer() { return sparrow::u8_buffer<char>(nullptr, 0); }

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

sparrow::u8_buffer<char> strings_buffer_at_offset(const Column& column, size_t offset) {
    const bool has_string_buffer = column.has_extra_buffer(offset, ExtraBufferType::STRING);
    if (!has_string_buffer) {
        return minimal_string_buffer();
    }
    auto& strings = column.get_extra_buffer(offset, ExtraBufferType::STRING);
    if (strings.blocks().size() == 0) {
        return minimal_string_buffer();
    }
    util::check(
            strings.blocks().size() == 1,
            "Expected a single block string extra buffer but got {} blocks",
            strings.blocks().size()
    );
    const auto strings_buffer_size = strings.block(0)->bytes();
    return sparrow::u8_buffer<char>(reinterpret_cast<char*>(strings.block(0)->release()), strings_buffer_size);
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
    sparrow::u8_buffer<SignedType> offset_buffer(reinterpret_cast<SignedType*>(block.release()), block_size + 1);
    auto strings_buffer = strings_buffer_at_offset(column, offset);
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
            auto strings_buffer = strings_buffer_at_offset(column, offset);
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
                sparrow::u8_buffer<int32_t> dict_keys_buffer{nullptr, 0};
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

namespace {

// Private data for the merged ArrowArray/ArrowSchema release callbacks.
// Owns all child arrays/schemas and the pointer arrays that parent.children points into.
struct MergedPrivateData {
    std::vector<ArrowArray> child_arrays;
    std::vector<ArrowSchema> child_schemas;
    // Pointer arrays that parent.children / parent.schema.children point into
    std::vector<ArrowArray*> child_array_ptrs;
    std::vector<ArrowSchema*> child_schema_ptrs;
    // Duplicated format string for the struct type
    std::string format;
    // Struct validity bitmap buffer (single null pointer = all-valid)
    const void* null_bitmap = nullptr;
};

void merged_array_release(ArrowArray* array) {
    if (array->release == nullptr) {
        return; // Already released
    }
    auto* data = static_cast<MergedPrivateData*>(array->private_data);
    // Release each child array that still has a release callback
    for (auto& child : data->child_arrays) {
        if (child.release != nullptr) {
            child.release(&child);
        }
    }
    delete data;
    array->release = nullptr;
}

void merged_schema_release(ArrowSchema* schema) {
    if (schema->release == nullptr) {
        return; // Already released
    }
    auto* data = static_cast<MergedPrivateData*>(schema->private_data);
    // Release each child schema that still has a release callback
    for (auto& child : data->child_schemas) {
        if (child.release != nullptr) {
            child.release(&child);
        }
    }
    // Free the format string
    delete data;
    schema->release = nullptr;
}

} // anonymous namespace

RecordBatchData horizontal_merge_arrow_batches(RecordBatchData&& batch_a, RecordBatchData&& batch_b) {
    auto& arr_a = batch_a.array_;
    auto& sch_a = batch_a.schema_;
    auto& arr_b = batch_b.array_;
    auto& sch_b = batch_b.schema_;

    // Collect column names from batch A for deduplication
    std::unordered_set<std::string> seen_names;
    for (int64_t i = 0; i < sch_a.n_children; ++i) {
        if (sch_a.children[i]->name) {
            seen_names.insert(sch_a.children[i]->name);
        }
    }

    // Create private data for the merged array
    auto* arr_data = new MergedPrivateData();
    auto* sch_data = new MergedPrivateData();

    // Reserve space: all of A + non-duplicate from B
    auto total_max = static_cast<size_t>(sch_a.n_children + sch_b.n_children);
    arr_data->child_arrays.reserve(total_max);
    sch_data->child_schemas.reserve(total_max);

    // Transfer all children from A
    for (int64_t i = 0; i < arr_a.n_children; ++i) {
        // Move child array: copy struct value, then nullify source release to prevent double-free
        arr_data->child_arrays.push_back(*arr_a.children[i]);
        arr_a.children[i]->release = nullptr;

        sch_data->child_schemas.push_back(*sch_a.children[i]);
        sch_a.children[i]->release = nullptr;
    }

    // Transfer non-duplicate children from B
    for (int64_t i = 0; i < arr_b.n_children; ++i) {
        std::string name = sch_b.children[i]->name ? sch_b.children[i]->name : "";
        if (seen_names.count(name)) {
            continue; // Skip duplicate (index column)
        }
        arr_data->child_arrays.push_back(*arr_b.children[i]);
        arr_b.children[i]->release = nullptr;

        sch_data->child_schemas.push_back(*sch_b.children[i]);
        sch_b.children[i]->release = nullptr;
    }

    auto n_merged = static_cast<int64_t>(arr_data->child_arrays.size());

    // Build pointer arrays
    arr_data->child_array_ptrs.resize(static_cast<size_t>(n_merged));
    sch_data->child_schema_ptrs.resize(static_cast<size_t>(n_merged));
    for (size_t i = 0; i < static_cast<size_t>(n_merged); ++i) {
        arr_data->child_array_ptrs[i] = &arr_data->child_arrays[i];
        sch_data->child_schema_ptrs[i] = &sch_data->child_schemas[i];
    }

    // Duplicate the struct format string
    sch_data->format = "+s";

    // Release the original parent structs (but children are already nullified)
    if (arr_a.release) {
        arr_a.release(&arr_a);
    }
    if (sch_a.release) {
        sch_a.release(&sch_a);
    }
    if (arr_b.release) {
        arr_b.release(&arr_b);
    }
    if (sch_b.release) {
        sch_b.release(&sch_b);
    }

    // Build merged parent ArrowArray
    ArrowArray merged_array;
    std::memset(&merged_array, 0, sizeof(merged_array));
    merged_array.length = arr_data->child_arrays.empty() ? 0 : arr_data->child_arrays[0].length;
    merged_array.null_count = 0;
    merged_array.offset = 0;
    merged_array.n_buffers = 1; // Struct arrays have 1 (null) validity buffer
    merged_array.buffers = &arr_data->null_bitmap;
    merged_array.n_children = n_merged;
    merged_array.children = arr_data->child_array_ptrs.data();
    merged_array.dictionary = nullptr;
    merged_array.release = merged_array_release;
    merged_array.private_data = arr_data;

    // Build merged parent ArrowSchema
    ArrowSchema merged_schema;
    std::memset(&merged_schema, 0, sizeof(merged_schema));
    merged_schema.format = sch_data->format.c_str();
    merged_schema.name = nullptr;
    merged_schema.metadata = nullptr;
    merged_schema.flags = 0;
    merged_schema.n_children = n_merged;
    merged_schema.children = sch_data->child_schema_ptrs.data();
    merged_schema.dictionary = nullptr;
    merged_schema.release = merged_schema_release;
    merged_schema.private_data = sch_data;

    return RecordBatchData(merged_array, merged_schema);
}

std::string default_arrow_format_for_type(DataType data_type) {
    switch (data_type) {
    case DataType::INT8:
        return "c";
    case DataType::INT16:
        return "s";
    case DataType::INT32:
        return "i";
    case DataType::INT64:
        return "l";
    case DataType::UINT8:
        return "C";
    case DataType::UINT16:
        return "S";
    case DataType::UINT32:
        return "I";
    case DataType::UINT64:
        return "L";
    case DataType::FLOAT32:
        return "f";
    case DataType::FLOAT64:
        return "g";
    case DataType::BOOL8:
        return "b";
    case DataType::NANOSECONDS_UTC64:
        return "tsn:";
    case DataType::UTF_DYNAMIC32:
        return "u"; // small_string (32-bit offsets, used with SMALL_STRING/CATEGORICAL output)
    case DataType::ASCII_DYNAMIC64:
    case DataType::UTF_DYNAMIC64:
    case DataType::ASCII_FIXED64:
    case DataType::UTF_FIXED64:
        return "U"; // large_string (64-bit offsets)
    default:
        return "U"; // Fallback to large_string for unknown types
    }
}

void resolve_target_fields_from_batch(std::vector<TargetField>& target_fields, const ArrowSchema& batch_schema) {
    // Build lookup from name -> schema child index
    std::unordered_map<std::string, int64_t> batch_col_idx;
    for (int64_t i = 0; i < batch_schema.n_children; ++i) {
        if (batch_schema.children[i]->name) {
            batch_col_idx[batch_schema.children[i]->name] = i;
        }
    }

    for (auto& field : target_fields) {
        if (field.format_resolved) {
            continue;
        }
        auto it = batch_col_idx.find(field.name);
        if (it != batch_col_idx.end()) {
            auto* child_schema = batch_schema.children[it->second];
            field.arrow_format = child_schema->format ? child_schema->format : "";
            field.is_dictionary = (child_schema->dictionary != nullptr);
            field.format_resolved = true;
        }
    }
}

namespace {

// Owns the buffers for a null-filled Arrow column.
// Validity bitmap is all zeros (all null), data buffer is zeros.
struct NullColumnOwner {
    std::string name;
    std::string format;
    std::vector<uint8_t> validity_bitmap; // All zeros = all null
    std::vector<uint8_t> data_buffer;     // Zeros
    const void* buffers[3] = {nullptr, nullptr, nullptr};

    // For dictionary-encoded columns:
    struct DictValues {
        std::string format = "U"; // large_string
        // Minimal dictionary with 1 entry (sparrow/Arrow require at least 1)
        std::vector<int64_t> offsets = {0, 1};
        std::vector<char> strings = {'a'};
        uint8_t validity_byte = 0xFF; // 1 valid entry
        const void* buffers[3] = {nullptr, nullptr, nullptr};
        ArrowArray array;
        ArrowSchema schema;
    };
    std::unique_ptr<DictValues> dict;

    ArrowArray array;
    ArrowSchema schema;
};

void null_column_array_release(ArrowArray* arr) {
    if (!arr->release)
        return;
    arr->release = nullptr;
}

void null_column_schema_release(ArrowSchema* sch) {
    if (!sch->release)
        return;
    sch->release = nullptr;
}

void null_dict_array_release(ArrowArray* arr) {
    if (!arr->release)
        return;
    arr->release = nullptr;
}

void null_dict_schema_release(ArrowSchema* sch) {
    if (!sch->release)
        return;
    sch->release = nullptr;
}

// Create a null-filled ArrowArray + ArrowSchema pair for a single column.
// Returns a NullColumnOwner that must be kept alive while the arrays are in use.
NullColumnOwner* create_null_column(
        const std::string& name, const std::string& format, bool is_dictionary, int64_t num_rows
) {
    auto* owner = new NullColumnOwner();
    owner->name = name;
    owner->format = format;

    // Validity bitmap: ceil(num_rows / 8) bytes, all zeros = all null
    auto validity_bytes = static_cast<size_t>((num_rows + 7) / 8);
    owner->validity_bitmap.resize(validity_bytes, 0);

    if (is_dictionary) {
        // Dictionary-encoded null column: int32 keys (all zeros) + minimal dictionary
        auto data_bytes = static_cast<size_t>(num_rows) * sizeof(int32_t);
        owner->data_buffer.resize(data_bytes, 0);

        // Set up dictionary values (minimal large_string with 1 entry)
        owner->dict = std::make_unique<NullColumnOwner::DictValues>();
        auto& dv = *owner->dict;

        // Dict values ArrowArray (large_string with 1 entry ["a"])
        std::memset(&dv.array, 0, sizeof(dv.array));
        dv.buffers[0] = &dv.validity_byte; // 1 valid bit
        dv.buffers[1] = dv.offsets.data(); // [0, 1]
        dv.buffers[2] = dv.strings.data(); // "a"
        dv.array.length = 1;
        dv.array.null_count = 0;
        dv.array.n_buffers = 3;
        dv.array.buffers = dv.buffers;
        dv.array.release = null_dict_array_release;
        dv.array.private_data = owner;

        // Dict values ArrowSchema
        std::memset(&dv.schema, 0, sizeof(dv.schema));
        dv.schema.format = dv.format.c_str();
        dv.schema.release = null_dict_schema_release;

        // Main column ArrowArray (dictionary keys)
        owner->buffers[0] = owner->validity_bitmap.data();
        owner->buffers[1] = owner->data_buffer.data();

        std::memset(&owner->array, 0, sizeof(owner->array));
        owner->array.length = num_rows;
        owner->array.null_count = num_rows;
        owner->array.n_buffers = 2;
        owner->array.buffers = owner->buffers;
        owner->array.dictionary = &dv.array;
        owner->array.release = null_column_array_release;
        owner->array.private_data = owner;

        // Main column ArrowSchema (dictionary keys, format = "i" for int32)
        std::memset(&owner->schema, 0, sizeof(owner->schema));
        owner->schema.format = owner->format.c_str();
        owner->schema.name = owner->name.c_str();
        owner->schema.flags = 2 /* ARROW_FLAG_NULLABLE */;
        owner->schema.dictionary = &dv.schema;
        owner->schema.release = null_column_schema_release;
    } else {
        // Non-dictionary null column
        size_t type_size = 1; // Default for bool ("b")
        if (format == "c" || format == "C")
            type_size = 1;
        else if (format == "s" || format == "S")
            type_size = 2;
        else if (format == "i" || format == "I" || format == "f")
            type_size = 4;
        else if (format == "l" || format == "L" || format == "g" || format.rfind("ts", 0) == 0)
            type_size = 8;

        if (format == "U" || format == "u") {
            // Large/small string: n_buffers=3 (validity, offsets, data)
            auto offset_size = (format == "U") ? sizeof(int64_t) : sizeof(int32_t);
            auto offsets_bytes = static_cast<size_t>(num_rows + 1) * offset_size;
            owner->data_buffer.resize(offsets_bytes, 0);

            owner->buffers[0] = owner->validity_bitmap.data();
            owner->buffers[1] = owner->data_buffer.data(); // offsets (all zeros)
            owner->buffers[2] = nullptr;                   // empty string data

            std::memset(&owner->array, 0, sizeof(owner->array));
            owner->array.length = num_rows;
            owner->array.null_count = num_rows;
            owner->array.n_buffers = 3;
            owner->array.buffers = owner->buffers;
            owner->array.release = null_column_array_release;
            owner->array.private_data = owner;

            std::memset(&owner->schema, 0, sizeof(owner->schema));
            owner->schema.format = owner->format.c_str();
            owner->schema.name = owner->name.c_str();
            owner->schema.flags = 2 /* ARROW_FLAG_NULLABLE */;
            owner->schema.release = null_column_schema_release;
        } else {
            // Numeric, timestamp, or bool
            auto data_bytes = static_cast<size_t>(num_rows) * type_size;
            owner->data_buffer.resize(data_bytes, 0);

            owner->buffers[0] = owner->validity_bitmap.data();
            owner->buffers[1] = owner->data_buffer.data();

            std::memset(&owner->array, 0, sizeof(owner->array));
            owner->array.length = num_rows;
            owner->array.null_count = num_rows;
            owner->array.n_buffers = 2;
            owner->array.buffers = owner->buffers;
            owner->array.release = null_column_array_release;
            owner->array.private_data = owner;

            std::memset(&owner->schema, 0, sizeof(owner->schema));
            owner->schema.format = owner->format.c_str();
            owner->schema.name = owner->name.c_str();
            owner->schema.flags = 2 /* ARROW_FLAG_NULLABLE */;
            owner->schema.release = null_column_schema_release;
        }
    }

    return owner;
}

// Private data for a padded batch. Owns the child pointer arrays and any
// null columns that were created for padding.
struct PaddedBatchData {
    std::vector<ArrowArray> child_arrays;
    std::vector<ArrowSchema> child_schemas;
    std::vector<ArrowArray*> child_array_ptrs;
    std::vector<ArrowSchema*> child_schema_ptrs;
    std::string format = "+s";
    const void* null_bitmap = nullptr;
    std::vector<std::unique_ptr<NullColumnOwner>> null_column_owners;
};

void padded_array_release(ArrowArray* array) {
    if (!array->release)
        return;
    auto* data = static_cast<PaddedBatchData*>(array->private_data);
    for (auto& child : data->child_arrays) {
        if (child.release) {
            child.release(&child);
        }
    }
    delete data;
    array->release = nullptr;
}

void padded_schema_release(ArrowSchema* schema) {
    if (!schema->release)
        return;
    auto* data = static_cast<PaddedBatchData*>(schema->private_data);
    for (auto& child : data->child_schemas) {
        if (child.release) {
            child.release(&child);
        }
    }
    delete data;
    schema->release = nullptr;
}

} // anonymous namespace

RecordBatchData pad_batch_to_schema(RecordBatchData&& batch, const std::vector<TargetField>& target_fields) {
    auto& arr = batch.array_;
    auto& sch = batch.schema_;

    // Fast path: check if batch already matches target schema exactly
    if (static_cast<size_t>(sch.n_children) == target_fields.size()) {
        bool matches = true;
        for (size_t i = 0; i < target_fields.size(); ++i) {
            const char* child_name = sch.children[i]->name;
            if (!child_name || target_fields[i].name != child_name) {
                matches = false;
                break;
            }
        }
        if (matches) {
            return std::move(batch); // Already matches, zero overhead
        }
    }

    // Build lookup: batch column name -> child index
    std::unordered_map<std::string, int64_t> batch_col_idx;
    for (int64_t i = 0; i < sch.n_children; ++i) {
        if (sch.children[i]->name) {
            batch_col_idx[sch.children[i]->name] = i;
        }
    }

    auto num_rows = arr.length;
    auto n_target = target_fields.size();

    auto* arr_data = new PaddedBatchData();
    auto* sch_data = new PaddedBatchData();
    arr_data->child_arrays.reserve(n_target);
    sch_data->child_schemas.reserve(n_target);

    for (const auto& field : target_fields) {
        auto it = batch_col_idx.find(field.name);
        if (it != batch_col_idx.end()) {
            // Column exists in batch - transfer ownership
            auto idx = it->second;
            arr_data->child_arrays.push_back(*arr.children[idx]);
            arr.children[idx]->release = nullptr; // Nullify source

            sch_data->child_schemas.push_back(*sch.children[idx]);
            sch.children[idx]->release = nullptr;
        } else {
            // Column missing - create null column
            std::unique_ptr<NullColumnOwner> null_col(
                    create_null_column(field.name, field.arrow_format, field.is_dictionary, num_rows)
            );
            arr_data->child_arrays.push_back(null_col->array);
            null_col->array.release = nullptr; // Transfer to padded batch

            sch_data->child_schemas.push_back(null_col->schema);
            null_col->schema.release = nullptr;

            arr_data->null_column_owners.push_back(std::move(null_col));
        }
    }

    auto n_children = static_cast<int64_t>(arr_data->child_arrays.size());

    // Build pointer arrays
    arr_data->child_array_ptrs.resize(static_cast<size_t>(n_children));
    sch_data->child_schema_ptrs.resize(static_cast<size_t>(n_children));
    for (size_t i = 0; i < static_cast<size_t>(n_children); ++i) {
        arr_data->child_array_ptrs[i] = &arr_data->child_arrays[i];
        sch_data->child_schema_ptrs[i] = &sch_data->child_schemas[i];
    }

    // Release original parent structs (children already nullified)
    if (arr.release) {
        arr.release(&arr);
    }
    if (sch.release) {
        sch.release(&sch);
    }

    // Build padded parent ArrowArray
    ArrowArray padded_array;
    std::memset(&padded_array, 0, sizeof(padded_array));
    padded_array.length = num_rows;
    padded_array.null_count = 0;
    padded_array.n_buffers = 1;
    padded_array.buffers = &arr_data->null_bitmap;
    padded_array.n_children = n_children;
    padded_array.children = arr_data->child_array_ptrs.data();
    padded_array.release = padded_array_release;
    padded_array.private_data = arr_data;

    // Build padded parent ArrowSchema
    ArrowSchema padded_schema;
    std::memset(&padded_schema, 0, sizeof(padded_schema));
    padded_schema.format = sch_data->format.c_str();
    padded_schema.flags = 0;
    padded_schema.n_children = n_children;
    padded_schema.children = sch_data->child_schema_ptrs.data();
    padded_schema.release = padded_schema_release;
    padded_schema.private_data = sch_data;

    return RecordBatchData(padded_array, padded_schema);
}

} // namespace arcticdb
