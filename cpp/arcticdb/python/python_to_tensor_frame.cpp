/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/stream/index.hpp>
#include <pybind11/numpy.h>
#include <sparrow/record_batch.hpp>

namespace arcticdb::convert {
constexpr const char none_char[8] = {'\300', '\000', '\000', '\000', '\000', '\000', '\000', '\000'};

using namespace arcticdb::pipelines;
using namespace arcticdb::stream;

[[nodiscard]] static inline bool is_unicode(PyObject* obj) { return PyUnicode_Check(obj); }

[[nodiscard]] static inline bool is_py_boolean(PyObject* obj) { return PyBool_Check(obj); }

std::variant<StringEncodingError, PyStringWrapper> pystring_to_buffer(PyObject* obj, bool is_owned) {
    if (is_unicode(obj)) {
        return StringEncodingError(
                fmt::format("Unexpected unicode in Python object with type {}", obj->ob_type->tp_name)
        );
    }
    char* buffer;
    ssize_t length;
    if (PYBIND11_BYTES_AS_STRING_AND_SIZE(obj, &buffer, &length)) {
        return StringEncodingError(
                fmt::format("Unable to extract string contents from Python object with type {}", obj->ob_type->tp_name)
        );
    }
    return PyStringWrapper(buffer, length, is_owned ? obj : nullptr);
}

[[nodiscard]] static inline bool is_py_array(PyObject* obj) {
    const auto& api = pybind11::detail::npy_api::get();
    return api.PyArray_Check_(obj);
}

[[nodiscard]] static std::tuple<ValueType, uint8_t, ssize_t> determine_python_object_type(PyObject* obj) {
    if (is_py_boolean(obj)) {
        normalization::raise<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>("Nullable booleans are not supported at the moment"
        );
        return {ValueType::BOOL_OBJECT, 1, 1};
    }

    return {ValueType::BYTES, 8, 1};
}

// Parse an array descriptor into type and elsize
static std::tuple<char, int> parse_array_descriptor(PyObject* obj) {
    if (pybind11::detail::npy_api::get().PyArray_RUNTIME_VERSION_ < 0x12) {
        ARCTICDB_DEBUG(log::version(), "Using numpy 1 API to get array descriptor");
        auto descr = pybind11::detail::array_descriptor1_proxy(obj);
        return {descr->kind, descr->elsize};
    } else {
        ARCTICDB_DEBUG(log::version(), "Using numpy 2 API to get array descriptor");
        auto descr = pybind11::detail::array_descriptor2_proxy(obj);
        return {descr->kind, descr->elsize};
    }
}

/// @brief Determine the type for column composed of arrays
/// In case column is composed of arrays all arrays must have the same element type. This iterates until if finds the
/// first non-empty array and returns its type.
/// @todo We will iterate over all arrays in a column in aggregator_set_data anyways, so this is redundant, however
///     the type is determined at the point when obj_to_tensor is called. We need to make it possible to change the
///     the column type in aggregator_set_data in order not to iterate all arrays twice.
[[nodiscard]] static std::tuple<ValueType, uint8_t, ssize_t> determine_python_array_type(
        PyObject** begin, PyObject** end
) {
    while (begin != end) {
        begin = std::find_if(begin, end, is_py_none);
        if (begin == end) {
            break;
        }
        const auto arr = pybind11::detail::array_proxy(*begin);
        normalization::check<ErrorCode::E_UNIMPLEMENTED_COLUMN_SECONDARY_TYPE>(
                arr->nd == 1, "Only one dimensional arrays are supported in columns."
        );
        const ssize_t element_count = arr->dimensions[0];
        if (element_count != 0) {
            const auto [kind, val_bytes] = parse_array_descriptor(arr->descr);
            return {get_value_type(kind), static_cast<uint8_t>(val_bytes), 2};
        }
        begin++;
    }
    return {ValueType::EMPTY, 8, 2};
}

std::variant<StringEncodingError, PyStringWrapper> py_unicode_to_buffer(
        PyObject* obj, std::optional<ScopedGILLock>& scoped_gil_lock
) {
    util::check(obj != nullptr, "Got null pointer in py_unicode_to_buffer");
    if (!is_unicode(obj)) {
        return StringEncodingError(
                fmt::format("Unexpected non-unicode in Python object with type {}", obj->ob_type->tp_name)
        );
    }
    if (PyUnicode_IS_COMPACT_ASCII(obj)) {
        return PyStringWrapper(reinterpret_cast<char*>(PyUnicode_DATA(obj)), PyUnicode_GET_LENGTH(obj));
        // Later versions of cpython expose macros in unicodeobject.h to perform this check, and to get the utf8_length,
        // but for 3.6 we have to hand-roll it
    } else if (reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8) {
        return PyStringWrapper(
                reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8,
                reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8_length
        );
    } else {
        if (PyUnicode_READY(obj) != 0) {
            return StringEncodingError(
                    fmt::format("PyUnicode_READY failed on Python object with type", obj->ob_type->tp_name)
            );
        }

        if (!scoped_gil_lock.has_value()) {
            scoped_gil_lock.emplace();
        }
        PyObject* utf8_obj = PyUnicode_AsUTF8String(obj);
        if (!utf8_obj) {
            return StringEncodingError(fmt::format(
                    "Unable to extract string contents from Python object with type {}", obj->ob_type->tp_name
            ));
        }
        return pystring_to_buffer(utf8_obj, true);
    }
}

NativeTensor obj_to_tensor(PyObject* ptr, bool empty_types) {
    auto& api = pybind11::detail::npy_api::get();
    util::check(api.PyArray_Check_(ptr), "Expected Python array");
    const auto arr = pybind11::detail::array_proxy(ptr);
    const auto [kind, elsize] = parse_array_descriptor(arr->descr);
    auto ndim = arr->nd;
    const ssize_t size = ndim == 1 ? arr->dimensions[0] : arr->dimensions[0] * arr->dimensions[1];
    // In Pandas < 2, empty series dtype is `"float"`, but as of Pandas 2.0, empty series dtype is `"object"`
    // The Normalizer in Python cast empty `"float"` series to `"object"` so `EMPTY` is used here.
    // See: https://github.com/man-group/ArcticDB/pull/1049
    auto val_type = size == 0 && empty_types ? ValueType::EMPTY : get_value_type(kind);
    auto val_bytes = static_cast<uint8_t>(elsize);
    const int64_t element_count =
            ndim == 1 ? int64_t(arr->dimensions[0]) : int64_t(arr->dimensions[0]) * int64_t(arr->dimensions[1]);
    const auto c_style = arr->strides[0] == val_bytes;

    if (is_empty_type(val_type)) {
        val_bytes = 8;
        val_type = ValueType::EMPTY;
    } else if (is_sequence_type(val_type)) {
        // wide type always is 64bits
        val_bytes = 8;

        if (!is_fixed_string_type(val_type) && element_count > 0) {
            auto obj = reinterpret_cast<PyObject**>(arr->data);
            bool empty_string_placeholder = false;
            PyObject* sample = *obj;
            PyObject** current_object = obj;
            // Arctic allows both None and NaN to represent a string with no value. We have 3 options:
            // * In case all values are None we can mark this column segment as EmptyType and avoid allocating storage
            //      memory for it
            // * In case all values are NaN we can't sample the value to check if it's UTF or ASCII (since NaN is not
            //      UTF nor ASCII). In that case we choose to save it as UTF
            // * In case there is at least one actual string we can sample it and decide the type of the column segment
            //      based on it
            // Note: ValueType::ASCII_DYNAMIC was used when Python 2 was supported. It is no longer supported, and
            // we're not expected to enter that branch.
            if (is_py_none(sample) || is_py_nan(sample)) {
                empty_string_placeholder = true;
                util::check(c_style, "Non contiguous columns with first element as None not supported yet.");
                const auto* end = obj + size;
                while (current_object < end) {
                    if (!(is_py_nan(*current_object) || is_py_none(*current_object))) {
                        empty_string_placeholder = false;
                        break;
                    }
                    ++current_object;
                }
                if (current_object != end)
                    sample = *current_object;
            }
            // Column full of NaN values is interpreted differently based on the kind. If kind is object "O" the column
            // is assigned a string type if kind is float "f" the column is assigned a float type. This is done in
            // order to preserve a legacy behavior of ArcticDB allowing to use both NaN and None as a placeholder for
            // missing string values.
            if (empty_string_placeholder && kind == 'O') {
                val_type = empty_types ? ValueType::EMPTY : ValueType::UTF_DYNAMIC;
            } else if (is_unicode(sample)) {
                val_type = ValueType::UTF_DYNAMIC;
            } else if (PYBIND11_BYTES_CHECK(sample)) {
                val_type = ValueType::ASCII_DYNAMIC;
            } else if (is_py_array(sample)) {
                normalization::raise<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(
                        "Array types are not supported at the moment"
                );
                std::tie(val_type, val_bytes, ndim) =
                        determine_python_array_type(current_object, current_object + element_count);
            } else {
                std::tie(val_type, val_bytes, ndim) = determine_python_object_type(sample);
            }
        }
    }

    // When processing empty collections, the size bits have to be `SizeBits::S64`,
    // and we can't use `val_bytes` to get this information since some dtype have another `elsize` than 8.
    const SizeBits size_bits = is_empty_type(val_type) ? SizeBits::S64 : get_size_bits(val_bytes);
    const auto dt = combine_data_type(val_type, size_bits);
    const int64_t nbytes = element_count * elsize;
    const void* data = nbytes ? arr->data : nullptr;
    const std::array<stride_t, 2> strides = {arr->strides[0], arr->nd > 1 ? arr->strides[1] : 0};
    const std::array<shape_t, 2> shapes = {arr->dimensions[0], arr->nd > 1 ? arr->dimensions[1] : 0};
    return {nbytes, arr->nd, strides.data(), shapes.data(), dt, elsize, data, ndim};
}

void tensors_to_frame(const py::tuple& tuple, const bool empty_types, InputFrame& frame) {
    frame.num_rows = 0u;
    // Fill index
    auto idx_names = tuple[0].cast<std::vector<std::string>>();
    auto idx_vals = tuple[2].cast<std::vector<py::object>>();
    util::check(
            idx_names.size() == idx_vals.size(),
            "Number idx names {} and values {} do not match",
            idx_names.size(),
            idx_vals.size()
    );

    StreamDescriptor desc;
    std::vector<entity::NativeTensor> field_tensors;
    std::optional<entity::NativeTensor> opt_index_tensor;
    if (!idx_names.empty()) {
        util::check(idx_names.size() == 1, "Multi-indexed dataframes not handled");
        auto index_tensor = obj_to_tensor(idx_vals[0].ptr(), empty_types);
        util::check(index_tensor.ndim() == 1, "Multi-dimensional indexes not handled");
        util::check(index_tensor.shape() != nullptr, "Index tensor expected to contain shapes");
        std::string index_column_name = !idx_names.empty() ? idx_names[0] : "index";
        frame.num_rows = static_cast<size_t>(index_tensor.shape(0));
        // TODO handle string indexes
        if (index_tensor.data_type() == DataType::NANOSECONDS_UTC64) {
            desc.set_index_field_count(1);
            desc.set_index_type(IndexDescriptor::Type::TIMESTAMP);

            desc.add_scalar_field(index_tensor.dt_, index_column_name);
            frame.index = stream::TimeseriesIndex(index_column_name);
            opt_index_tensor = std::move(index_tensor);
        } else {
            frame.index = stream::RowCountIndex();
            desc.set_index_type(IndexDescriptor::Type::ROWCOUNT);
            desc.add_scalar_field(index_tensor.dt_, index_column_name);
            field_tensors.push_back(std::move(index_tensor));
        }
    }

    // Fill tensors
    auto col_names = tuple[1].cast<std::vector<std::string>>();
    auto col_vals = tuple[3].cast<std::vector<py::object>>();
    auto sorted = tuple[4].cast<SortedValue>();

    for (auto i = 0u; i < col_vals.size(); ++i) {
        auto tensor = obj_to_tensor(col_vals[i].ptr(), empty_types);
        frame.num_rows = std::max(frame.num_rows, static_cast<size_t>(tensor.shape(0)));
        if (tensor.expanded_dim() == 1) {
            desc.add_field(scalar_field(tensor.data_type(), col_names[i]));
        } else if (tensor.expanded_dim() == 2) {
            desc.add_field(FieldRef{TypeDescriptor{tensor.data_type(), Dimension::Dim1}, col_names[i]});
        }
        field_tensors.push_back(std::move(tensor));
    }

    // idx_names are passed by the python layer. They are empty in case row count index is used see:
    // https://github.com/man-group/ArcticDB/blob/4184a467d9eee90600ddcbf34d896c763e76f78f/python/arcticdb/version_store/_normalization.py#L291
    // Currently the python layers assign RowRange index to both empty dataframes and dataframes which do not specify
    // index explicitly. Thus we handle this case after all columns are read so that we know how many rows are there.
    if (idx_names.empty()) {
        frame.index = stream::RowCountIndex();
        desc.set_index_type(IndexDescriptor::Type::ROWCOUNT);
    }

    if (empty_types && frame.num_rows == 0) {
        frame.index = stream::EmptyIndex();
        desc.set_index_type(IndexDescriptor::Type::EMPTY);
    }
    desc.set_sorted(sorted);
    frame.set_from_tensors(std::move(desc), std::move(field_tensors), std::move(opt_index_tensor));
}

void record_batches_to_frame(const std::vector<RecordBatchData>& record_batches, InputFrame& frame) {
    util::check(
            frame.norm_meta.has_experimental_arrow(), "Unexpected non-Arrow norm metadata provided with Arrow data"
    );
    const auto& arrow_norm_metadata = frame.norm_meta.experimental_arrow();
    std::vector<sparrow::record_batch> sparrow_record_batches(record_batches.size(), sparrow::record_batch{});
    std::ranges::transform(record_batches, sparrow_record_batches.begin(), [](const RecordBatchData& record_batch) {
        return sparrow::record_batch{&record_batch.array_, &record_batch.schema_};
    });
    auto [seg, index_column_position] = arrow_data_to_segment(
            sparrow_record_batches,
            arrow_norm_metadata.has_index() ? arrow_norm_metadata.index_column_name() : std::optional<std::string>()
    );
    if (index_column_position.has_value()) {
        frame.norm_meta.mutable_experimental_arrow()->set_index_column_position(*index_column_position);
    }
    frame.set_segment(std::move(seg));
}

std::shared_ptr<InputFrame> py_ndf_to_frame(
        const StreamId& stream_name, const InputItem& item, const py::object& norm_meta, const py::object& user_meta,
        bool empty_types
) {
    ARCTICDB_SUBSAMPLE_DEFAULT(NormalizeFrame)
    auto res = std::make_shared<InputFrame>();
    python_util::pb_from_python(norm_meta, res->norm_meta);
    if (!user_meta.is_none()) {
        python_util::pb_from_python(user_meta, res->user_meta);
    }

    if (std::holds_alternative<py::tuple>(item)) {
        tensors_to_frame(std::get<py::tuple>(item), empty_types, *res);
    } else {
        record_batches_to_frame(std::get<std::vector<RecordBatchData>>(item), *res);
    }
    res->set_index_range();
    res->desc().set_id(stream_name);
    ARCTICDB_DEBUG(log::version(), "Received frame with descriptor {}", res->desc());
    return res;
}

std::shared_ptr<InputFrame> py_none_to_frame() {
    ARCTICDB_SUBSAMPLE_DEFAULT(NormalizeNoneFrame)
    auto res = std::make_shared<InputFrame>();
    res->num_rows = 0u;

    arcticdb::proto::descriptors::NormalizationMetadata::MsgPackFrame msg;
    msg.set_size_bytes(1);
    msg.set_version(1);
    res->norm_meta.mutable_msg_pack_frame()->CopyFrom(msg);

    // Fill index
    res->index = stream::RowCountIndex();
    StreamDescriptor desc;
    desc.set_index_type(IndexDescriptorImpl::Type::ROWCOUNT);
    desc.set_sorted(SortedValue::UNKNOWN);

    // Fill tensors
    auto col_name = "bytes";

    const stride_t strides = 8;
    const shape_t shapes = 1;
    constexpr int ndim = 1;
    auto tensor = NativeTensor{8, ndim, &strides, &shapes, DataType::UINT64, 8, none_char, ndim};
    res->num_rows = std::max(res->num_rows, static_cast<size_t>(tensor.shape(0)));
    desc.add_field(scalar_field(tensor.data_type(), col_name));

    res->set_from_tensors(std::move(desc), {tensor}, std::nullopt);

    ARCTICDB_DEBUG(log::version(), "Received frame with descriptor {}", res->desc());
    res->set_index_range();
    return res;
}

} // namespace arcticdb::convert