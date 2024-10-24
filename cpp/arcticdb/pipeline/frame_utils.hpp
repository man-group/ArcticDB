/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/column_store/chunked_buffer.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <util/flatten_utils.hpp>
#include <util/gil_safe_py_none.hpp>

namespace arcticdb {

inline size_t get_first_string_size(const pipelines::PipelineContextRow& context_row, ChunkedBuffer &src, std::size_t first_row_in_frame) {
    auto offset = first_context_row(context_row.slice_and_key(), first_row_in_frame);
    auto num_rows = context_row.slice_and_key().slice_.row_range.diff();
    util::check(context_row.has_string_pool(), "String pool not found for context row {}", context_row.index());
    return get_first_string_size(num_rows, src, offset, context_row.string_pool());
}

inline size_t get_max_string_size(const pipelines::PipelineContextRow& context_row, ChunkedBuffer &src, std::size_t first_row_in_frame) {
    auto offset = first_context_row(context_row.slice_and_key(), first_row_in_frame);
    auto num_rows = context_row.slice_and_key().slice_.row_range.diff();
    size_t max_length{0u};

    for(auto row = 0u; row < num_rows; ++row) {
        auto offset_val = get_offset_string_at(offset + row, src);
        if (offset_val == nan_placeholder() || offset_val == not_a_string())
            continue;

        max_length = std::max(max_length, get_string_from_pool(offset_val, context_row.string_pool()).size());
    }
    return max_length;
}

TimeseriesDescriptor make_timeseries_descriptor(
    size_t total_rows,
    const StreamDescriptor& desc,
    arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta,
    std::optional<arcticdb::proto::descriptors::UserDefinedMetadata>&& um,
    std::optional<AtomKey>&& prev_key,
    std::optional<AtomKey>&& next_key,
    bool bucketize_dynamic
    );

TimeseriesDescriptor timseries_descriptor_from_index_segment(
    size_t total_rows,
    pipelines::index::IndexSegmentReader&& index_segment_reader,
    std::optional<AtomKey>&& prev_key,
    bool bucketize_dynamic
    );

TimeseriesDescriptor timeseries_descriptor_from_pipeline_context(
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context,
    std::optional<AtomKey>&& prev_key,
    bool bucketize_dynamic);


TimeseriesDescriptor index_descriptor_from_frame(
    const std::shared_ptr<pipelines::InputTensorFrame>& frame,
    size_t existing_rows,
    std::optional<entity::AtomKey>&& prev_key = {});

template <typename RawType>
RawType* flatten_tensor(
        std::optional<ChunkedBuffer>& flattened_buffer,
        size_t rows_to_write,
        const NativeTensor& tensor,
        size_t slice_num,
        size_t regular_slice_size
        ) {
    flattened_buffer = ChunkedBuffer::presized(rows_to_write * sizeof(RawType), entity::AllocationType::PRESIZED);
    TypedTensor<RawType> t(tensor, slice_num, regular_slice_size, rows_to_write);
    util::FlattenHelper flattener{t};
    auto dst = reinterpret_cast<RawType*>(flattened_buffer->data());
    flattener.flatten(dst, reinterpret_cast<RawType const*>(t.data()));
    return reinterpret_cast<RawType*>(flattened_buffer->data());
}


template<typename Aggregator>
std::optional<convert::StringEncodingError> aggregator_set_data(
    const TypeDescriptor& type_desc,
    const entity::NativeTensor& tensor,
    Aggregator& agg,
    size_t col,
    size_t rows_to_write,
    size_t row,
    size_t slice_num,
    size_t regular_slice_size,
    bool sparsify_floats
) {
    return type_desc.visit_tag([&](auto tag) {
        using RawType = typename std::decay_t<decltype(tag)>::DataTypeTag::raw_type;
        constexpr auto dt = std::decay_t<decltype(tag)>::DataTypeTag::data_type;

        util::check(type_desc.data_type() == tensor.data_type(), "Type desc {} != {} tensor type", type_desc.data_type(),
                    tensor.data_type());
        util::check(type_desc.data_type() == dt, "Type desc {} != {} static type", type_desc.data_type(), dt);
        const auto c_style = util::is_cstyle_array<RawType>(tensor);
        std::optional<ChunkedBuffer> flattened_buffer;
        if constexpr (is_sequence_type(dt)) {
            normalization::check<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(tag.dimension() == Dimension::Dim0,
                "Multidimensional string types are not supported.");
            ARCTICDB_SUBSAMPLE_AGG(SetDataString)
            if (is_fixed_string_type(dt)) {
                // deduplicate the strings
                auto str_stride = tensor.strides(0);
                auto data = const_cast<void *>(tensor.data());
                auto char_data = reinterpret_cast<char *>(data) + row * str_stride;
                auto str_len = tensor.elsize();

                for (size_t s = 0; s < rows_to_write; ++s, char_data += str_stride) {
                    agg.set_string_at(col, s, char_data, str_len);
                }
            } else {
                auto data = const_cast<void *>(tensor.data());
                auto ptr_data = reinterpret_cast<PyObject **>(data);
                ptr_data += row;
                if (!c_style)
                    ptr_data = flatten_tensor<PyObject*>(flattened_buffer, rows_to_write, tensor, slice_num, regular_slice_size);

                auto none = GilSafePyNone::instance();
                std::variant<convert::StringEncodingError, convert::PyStringWrapper> wrapper_or_error;
                // GIL will be acquired if there is a string that is not pure ASCII/UTF-8
                // In this case a PyObject will be allocated by convert::py_unicode_to_buffer
                // If such a string is encountered in a column, then the GIL will be held until that whole column has
                // been processed, on the assumption that if a column has one such string it will probably have many.
                std::optional<ScopedGILLock> scoped_gil_lock;
                auto& column = agg.segment().column(col);
                column.allocate_data(rows_to_write * sizeof(entity::position_t));
                auto out_ptr = reinterpret_cast<entity::position_t*>(column.buffer().data());
                auto& string_pool = agg.segment().string_pool();
                for (size_t s = 0; s < rows_to_write; ++s, ++ptr_data) {
                    if (*ptr_data == none->ptr()) {
                        *out_ptr++ = not_a_string();
                    } else if(is_py_nan(*ptr_data)){
                        *out_ptr++ = nan_placeholder();
                    } else {
                        if constexpr (is_utf_type(slice_value_type(dt))) {
                            wrapper_or_error = convert::py_unicode_to_buffer(*ptr_data, scoped_gil_lock);
                        } else {
                            wrapper_or_error = convert::pystring_to_buffer(*ptr_data, false);
                        }
                        // Cannot use util::variant_match as only one of the branches would have a return type
                        if (std::holds_alternative<convert::PyStringWrapper>(wrapper_or_error)) {
                            convert::PyStringWrapper wrapper(std::move(std::get<convert::PyStringWrapper>(wrapper_or_error)));
                            const auto offset = string_pool.get(wrapper.buffer_, wrapper.length_);
                            *out_ptr++ = offset.offset();
                        } else if (std::holds_alternative<convert::StringEncodingError>(wrapper_or_error)) {
                            auto error = std::get<convert::StringEncodingError>(wrapper_or_error);
                            error.row_index_in_slice_ = s;
                            return std::optional<convert::StringEncodingError>(error);
                        } else {
                            internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unexpected variant alternative");
                        }
                    }
                }
            }
        } else if constexpr ((is_numeric_type(dt) || is_bool_type(dt)) && tag.dimension() == Dimension::Dim0) {
            auto ptr = tensor.template ptr_cast<RawType>(row);
            if (sparsify_floats) {
                if constexpr (is_floating_point_type(dt)) {
                    agg.set_sparse_block(col, ptr, rows_to_write);
                } else {
                    util::raise_rte("sparse currently supported for floating point columns only.");
                }
            } else {
                if (c_style) {
                    ARCTICDB_SUBSAMPLE_AGG(SetDataZeroCopy)
                    agg.set_external_block(col, ptr, rows_to_write);
                } else {
                    ARCTICDB_SUBSAMPLE_AGG(SetDataFlatten)
                    ARCTICDB_DEBUG(log::version(),
                            "Data contains non-contiguous columns, writing will be inefficient, consider coercing to c_style ndarray (shape={}, data_size={})",
                            tensor.strides(0),
                            sizeof(RawType));

                    TypedTensor <RawType> t(tensor, slice_num, regular_slice_size, rows_to_write);
                    agg.set_array(col, t);
                }
            }
        } else if constexpr(is_bool_object_type(dt)) {
            normalization::check<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(tag.dimension() == Dimension::Dim0,
                "Multidimensional nullable booleans are not supported");
            auto data = const_cast<void *>(tensor.data());
            auto ptr_data = reinterpret_cast<PyObject **>(data);
            ptr_data += row;

            if (!c_style)
                ptr_data = flatten_tensor<PyObject*>(flattened_buffer, rows_to_write, tensor, slice_num, regular_slice_size);

            util::BitSet bitset = util::scan_object_type_to_sparse(ptr_data, rows_to_write);

            const auto num_values = bitset.count();
            auto bool_buffer = ChunkedBuffer::presized(num_values * sizeof(uint8_t), entity::AllocationType::PRESIZED);
            auto bool_ptr = bool_buffer.ptr_cast<uint8_t>(0u, num_values);
            for (auto it = bitset.first(); it < bitset.end(); ++it) {
                *bool_ptr = static_cast<uint8_t>(PyObject_IsTrue(ptr_data[*it]));
                ++bool_ptr;
            }
            if(bitset.count() > 0)
                agg.set_sparse_block(col, std::move(bool_buffer), std::move(bitset));

        } else if constexpr(is_array_type(TypeDescriptor(tag))) {
            auto data = const_cast<void*>(tensor.data());
            const auto ptr_data = reinterpret_cast<PyObject**>(data) + row;

            util::BitSet values_bitset = util::scan_object_type_to_sparse(ptr_data, rows_to_write);
            util::check(!values_bitset.empty(),
                "Empty bit set means empty colum and should be processed by the empty column code path.");
            if constexpr (is_empty_type(dt)) {
                // If we have a column of type {EMPTYVAL, Dim1} and all values of the bitset are set to 1 this means
                // that we have a column full of empty arrays. In this case there is no need to proceed further and
                // store anything on disc. Empty arrays can be reconstructed given the type descriptor. However, if
                // there is at least one "missing" value this means that we're mixing empty arrays and None values.
                // In that case we need to save the bitset so that we can distinguish empty array from None during the
                // read.
                if(values_bitset.size() == values_bitset.count()) {
                    Column arr_col{TypeDescriptor{DataType::EMPTYVAL, Dimension::Dim2}, Sparsity::PERMITTED};
                    agg.set_sparse_block(col, arr_col.release_buffer(), arr_col.release_shapes(), std::move(values_bitset));
                    return std::optional<convert::StringEncodingError>();
                }
            }

            ssize_t last_logical_row{0};
            const auto column_type_descriptor = TypeDescriptor{tensor.data_type(), Dimension::Dim2};
            TypeDescriptor secondary_type = type_desc;
            Column arr_col{column_type_descriptor, Sparsity::PERMITTED};
            for (auto en = values_bitset.first(); en < values_bitset.end(); ++en) {
                const auto arr_pos = *en;
                const auto row_tensor = convert::obj_to_tensor(ptr_data[arr_pos], false);
                const auto row_type_descriptor = TypeDescriptor{row_tensor.data_type(), Dimension::Dim1};
                const std::optional<TypeDescriptor>& common_type = has_valid_common_type(row_type_descriptor, secondary_type);
                normalization::check<ErrorCode::E_COLUMN_SECONDARY_TYPE_MISMATCH>(
                    common_type.has_value(),
                    "Numpy arrays in the same column must be of compatible types {} {}",
                    datatype_to_str(secondary_type.data_type()),
                    datatype_to_str(row_type_descriptor.data_type()));
                secondary_type = *common_type;
                // TODO: If the input array contains unexpected elements such as None, NaN, string the type
                //  descriptor will have data_type == BYTES_DYNAMIC64. TypeDescriptor::visit_tag does not have a
                //  case for it and it will throw exception which is not meaningful. Adding BYTES_DYNAMIC64 in
                //  TypeDescriptor::visit_tag leads to a bunch of compilation errors spread all over the code.
                normalization::check<ErrorCode::E_UNIMPLEMENTED_COLUMN_SECONDARY_TYPE>(
                    is_numeric_type(row_type_descriptor.data_type()) || is_empty_type(row_type_descriptor.data_type()),
                    "Numpy array type {} is not implemented. Only dense int and float arrays are supported.",
                    datatype_to_str(row_type_descriptor.data_type())
                );
                row_type_descriptor.visit_tag([&arr_col, &row_tensor, &last_logical_row] (auto tdt) {
                    using ArrayDataTypeTag = typename decltype(tdt)::DataTypeTag;
                    using ArrayType = typename ArrayDataTypeTag::raw_type;
                    if constexpr(is_empty_type(ArrayDataTypeTag::data_type)) {
                        arr_col.set_empty_array(last_logical_row, row_tensor.ndim());
                    } else if constexpr(is_numeric_type(ArrayDataTypeTag::data_type)) {
                        if(row_tensor.nbytes()) {
                            TypedTensor<ArrayType> typed_tensor{row_tensor};
                            arr_col.set_array(last_logical_row, typed_tensor);
                        } else {
                            arr_col.set_empty_array(last_logical_row, row_tensor.ndim());
                        }
                    } else {
                        normalization::raise<ErrorCode::E_UNIMPLEMENTED_COLUMN_SECONDARY_TYPE>(
                            "Numpy array type is not implemented. Only dense int and float arrays are supported.");
                    }
                });
                last_logical_row++;
            }
            arr_col.set_type(TypeDescriptor{secondary_type.data_type(), column_type_descriptor.dimension()});
            agg.set_sparse_block(col, arr_col.release_buffer(), arr_col.release_shapes(), std::move(values_bitset));
        } else if constexpr(tag.dimension() == Dimension::Dim2) {
            normalization::raise<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(
                "Trying to add matrix of base type {}. Matrix types are not supported.",
                datatype_to_str(tag.data_type()));
        } else if constexpr(!is_empty_type(dt)) {
            static_assert(!sizeof(dt), "Unknown data type");
        }
        return std::optional<convert::StringEncodingError>();
    });
}

namespace pipelines {
struct SliceAndKey;
struct PipelineContext;
}

size_t adjust_slice_rowcounts(
    std::vector<pipelines::SliceAndKey> & slice_and_keys, const std::optional<size_t>& first_row = std::nullopt);

void adjust_slice_rowcounts(
    const std::shared_ptr<pipelines::PipelineContext>& pipeline_context);

size_t get_slice_rowcounts(
    std::vector<pipelines::SliceAndKey>& slice_and_keys);

std::pair<size_t, size_t> offset_and_row_count(
    const std::shared_ptr<pipelines::PipelineContext>& context);

bool index_is_not_timeseries_or_is_sorted_ascending(const pipelines::InputTensorFrame& frame);

} //namespace arcticdb
