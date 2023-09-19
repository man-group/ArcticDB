/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */


#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/entity/native_tensor.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_types.hpp>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>

namespace arcticdb::convert {
const char none_char[8] = {'\300', '\000', '\000', '\000', '\000', '\000', '\000', '\000'};

using namespace arcticdb::pipelines;

bool is_unicode(PyObject *obj) {
    return PyUnicode_Check(obj);
}

PyStringWrapper pystring_to_buffer(PyObject *obj, bool is_owned) {
    char *buffer;
    ssize_t length;
    util::check(!is_unicode(obj), "Unexpected unicode object");
    if (PYBIND11_BYTES_AS_STRING_AND_SIZE(obj, &buffer, &length))
        util::raise_rte("Unable to extract string contents! (invalid type)");

    return {buffer, length, is_owned ? obj : nullptr};
}

PyStringWrapper py_unicode_to_buffer(PyObject *obj, std::optional<ScopedGILLock>& scoped_gil_lock) {
    if (is_unicode(obj)) {
        if (PyUnicode_IS_COMPACT_ASCII(obj)) {
            return {reinterpret_cast<char *>(PyUnicode_DATA(obj)), PyUnicode_GET_LENGTH(obj)};
        // Later versions of cpython expose macros in unicodeobject.h to perform this check, and to get the utf8_length,
        // but for 3.6 we have to hand-roll it
        } else if (reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8) {
            return {reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8, reinterpret_cast<PyCompactUnicodeObject*>(obj)->utf8_length};
        } else {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(PyUnicode_READY(obj) == 0, "PyUnicode_READY failed");
            if (!scoped_gil_lock.has_value()) {
                scoped_gil_lock.emplace();
            }
            PyObject* utf8_obj = PyUnicode_AsUTF8String(obj);
            if (!utf8_obj)
                util::raise_rte("Unable to extract string contents! (encoding issue)");
            return pystring_to_buffer(utf8_obj, true);
        }
    } else {
        util::raise_rte("Expected unicode");
    }
}

NativeTensor obj_to_tensor(PyObject *ptr) {
//    ARCTICDB_SAMPLE(ObjToTensor, RMTSF_Aggregate)
    auto& api = pybind11::detail::npy_api::get();
    util::check(api.PyArray_Check_(ptr), "Expected Python array");
    auto arr = pybind11::detail::array_proxy(ptr);
    auto descr = pybind11::detail::array_descriptor_proxy(arr->descr);
    auto ndim = arr->nd;
    ssize_t size = ndim == 1 ? arr->dimensions[0] : arr->dimensions[0] * arr->dimensions[1];
    auto val_type = size > 0 ? get_value_type(descr->kind) : ValueType::EMPTY;
    auto val_bytes = static_cast<uint8_t>(descr->elsize);
    auto c_style = arr->strides[0] == val_bytes;

    if (is_sequence_type(val_type)) {
        // wide type always is 64bits
        val_bytes = 8;

        // If Numpy has type 'O' then get_value_type above will return type 'BYTES'
        // If there is no value and we can't deduce a type then leave it that way,
        // otherwise try to work out whether it was a bytes (string) type or unicode
        if (!is_fixed_string_type(val_type) && size > 0) {
            auto none = py::none{};
            auto obj = reinterpret_cast<PyObject **>(arr->data);
            bool empty = false;
            bool all_nans = false;
            PyObject *sample = *obj;
            // Arctic allows both None and NaN to represent a string with no value. We have 3 options:
            // * In case all values are None we can mark this column segment as EmptyType and avoid allocating storage
            //      memory for it
            // * In case all values are NaN we can't sample the value to check if it's UTF or ASCII (since NaN is not
            //      UTF nor ASCII). In that case we choose to save it as UTF
            // * In case there is at least one actual string we can sample it and decide the type of the column segment
            //      based on it
            // Note: ValueType::ASCII_DYNAMIC was used when Python 2 was supported. It is no longer supported, and
            //  we're not expected to enter that branch.
            if (sample == none.ptr() || is_py_nan(sample)) {
                empty = true;
                all_nans = true;
                util::check(c_style, "Non contiguous columns with first element as None not supported yet.");
                PyObject** current_object = obj;
                while(current_object < obj + size) {
                    if(*current_object == none.ptr()) {
                        all_nans = false;
                    } else if(is_py_nan(*current_object)) {
                        empty = false;
                    } else {
                        all_nans = false;
                        empty = false;
                        break;
                    }
                    ++current_object;
                }
                sample = *current_object;
            }
            if (empty) {
                val_type = ValueType::EMPTY;
            } else if(all_nans || is_unicode(sample)){
                val_type = ValueType::UTF_DYNAMIC;
            } else if (PYBIND11_BYTES_CHECK(sample)) {
                val_type = ValueType::ASCII_DYNAMIC;
            }
        }
    }

    // When processing empty collections, the size bits have to be `SizeBits::S64`,
    // and we can't use `val_bytes` to get this information since some dtype have another `elsize` than 8.
    SizeBits size_bits = val_type == ValueType::EMPTY ? SizeBits::S64 : get_size_bits(val_bytes);
    auto dt = combine_data_type(val_type, size_bits);
    ssize_t nbytes = size * descr->elsize;
    return {nbytes, ndim, arr->strides, arr->dimensions, dt, descr->elsize, arr->data};
}

InputTensorFrame py_ndf_to_frame(
    const StreamId& stream_name,
    const py::tuple &item,
    const py::object &norm_meta,
    const py::object &user_meta) {
    ARCTICDB_SUBSAMPLE_DEFAULT(NormalizeFrame)
    InputTensorFrame res;
    res.desc.set_id(stream_name);
    res.num_rows = 0u;
    python_util::pb_from_python(norm_meta, res.norm_meta);
    if (!user_meta.is_none())
        python_util::pb_from_python(user_meta, res.user_meta);

    // Fill index
    auto idx_names = item[0].cast<std::vector<std::string>>();
    auto idx_vals = item[2].cast<std::vector<py::object>>();
    util::check(idx_names.size() == idx_vals.size(),
                "Number idx names {} and values {} do not match",
                idx_names.size(), idx_vals.size());

    if (idx_names.empty()) {
        res.index = stream::RowCountIndex();
        res.desc.set_index_type(IndexDescriptor::ROWCOUNT);
    } else {
        util::check(idx_names.size() == 1, "Multi-indexed dataframes not handled");
        auto index_tensor = obj_to_tensor(idx_vals[0].ptr());
        util::check(index_tensor.ndim() == 1, "Multi-dimensional indexes not handled");
        util::check(index_tensor.shape() != nullptr, "Index tensor expected to contain shapes");
        std::string index_column_name = !idx_names.empty() ? idx_names[0] : "index";
        res.num_rows = index_tensor.shape(0);
        //TODO handle string indexes
        if (index_tensor.data_type() == DataType::NANOSECONDS_UTC64) {

            res.desc.set_index_field_count(1);
            res.desc.set_index_type(IndexDescriptor::TIMESTAMP);

            res.desc.add_scalar_field(index_tensor.dt_, index_column_name);
            res.index = stream::TimeseriesIndex(index_column_name);
            res.index_tensor = std::move(index_tensor);
        } else {
            // Just treat this field as a simple field, since we can't really handle it
            res.index = stream::RowCountIndex();
            res.desc.set_index_type(IndexDescriptor::ROWCOUNT);
            res.desc.add_scalar_field(index_tensor.dt_, index_column_name);
            res.field_tensors.push_back(std::move(index_tensor));
        }
    }

    // Fill tensors
    auto col_names = item[1].cast<std::vector<std::string>>();
    auto col_vals = item[3].cast<std::vector<py::object>>();
    auto sorted = item[4].cast<SortedValue>();

    res.set_sorted(sorted);

    for (auto i = 0u; i < col_vals.size(); ++i) {
        auto tensor = obj_to_tensor(col_vals[i].ptr());
        res.num_rows = std::max(res.num_rows, tensor.shape(0));
        res.desc.add_field(scalar_field(tensor.data_type(), col_names[i]));
        res.field_tensors.push_back(std::move(tensor));
    }

    ARCTICDB_DEBUG(log::version(), "Received frame with descriptor {}", res.desc);
    res.set_index_range();
    return res;
}

InputTensorFrame py_none_to_frame() {
    ARCTICDB_SUBSAMPLE_DEFAULT(NormalizeNoneFrame)
    InputTensorFrame res;
    res.num_rows = 0u;

    arcticdb::proto::descriptors::NormalizationMetadata::MsgPackFrame msg;
    msg.set_size_bytes(1);
    msg.set_version(1);
    res.norm_meta.mutable_msg_pack_frame()->CopyFrom(msg);

    // Fill index
    res.index = stream::RowCountIndex();
    res.desc.set_index_type(IndexDescriptor::ROWCOUNT);

    // Fill tensors
    auto col_name = "bytes";
    auto sorted = SortedValue::UNKNOWN;

    res.set_sorted(sorted);

    ssize_t strides = 8;
    ssize_t shapes = 1;

    auto tensor = NativeTensor{8, 1, &strides, &shapes, DataType::UINT64, 8, none_char};
    res.num_rows = std::max(res.num_rows, tensor.shape(0));
    res.desc.add_field(scalar_field(tensor.data_type(), col_name));
    res.field_tensors.push_back(std::move(tensor));

    ARCTICDB_DEBUG(log::version(), "Received frame with descriptor {}", res.desc);
    res.set_index_range();
    return res;
}

} // namespace arcticdb::convert
