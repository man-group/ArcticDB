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

using namespace arcticdb::pipelines;

bool is_unicode(PyObject* obj)
{
    return PyUnicode_Check(obj);
}

PyStringWrapper pystring_to_buffer(PyObject* obj, py::handle handle)
{
    char* buffer;
    ssize_t length;
    util::check(!is_unicode(obj), "Unexpected unicode object");
    if (PYBIND11_BYTES_AS_STRING_AND_SIZE(obj, &buffer, &length))
        util::raise_rte("Unable to extract string contents! (invalid type)");

    return {buffer, length, handle};
}

PyStringWrapper py_unicode_to_buffer(PyObject* obj)
{
    if (is_unicode(obj)) {
        py::handle handle{PyUnicode_AsUTF8String(obj)};
        if (!handle)
            util::raise_rte("Unable to extract string contents! (encoding issue)");
        return pystring_to_buffer(handle.ptr(), handle);
    } else {
        util::raise_rte("Expected unicode");
    }
}

NativeTensor obj_to_tensor(PyObject* ptr)
{
    //    ARCTICDB_SAMPLE(ObjToTensor, RMTSF_Aggregate)
    auto& api = pybind11::detail::npy_api::get();
    util::check(api.PyArray_Check_(ptr), "Expected Python array");
    auto arr = pybind11::detail::array_proxy(ptr);
    auto descr = pybind11::detail::array_descriptor_proxy(arr->descr);
    auto ndim = arr->nd;
    auto val_type = get_value_type(descr->kind);
    auto val_bytes = static_cast<uint8_t>(descr->elsize);
    ssize_t size = ndim == 1 ? arr->dimensions[0] : arr->dimensions[0] * arr->dimensions[1];
    auto c_style = arr->strides[0] == val_bytes;

    if (is_sequence_type(val_type)) {
        // wide type always is 64bits
        val_bytes = 8;

        // If Numpy has type 'O' then get_value_type above will return type 'BYTES'
        // If there is no value and we can't deduce a type then leave it that way,
        // otherwise try to work out whether it was a bytes (string) type or unicode
        if (!is_fixed_string_type(val_type) && size > 0) {
            auto none = py::none{};
            auto obj = reinterpret_cast<PyObject**>(arr->data);
            bool empty = false;
            PyObject* sample = *obj;
            if (sample == none.ptr() || is_py_nan(sample)) {
                // Iterate till we find the first non null element, the frontend ensures there is at least one.
                util::check(c_style, "Non contiguous columns with first element as None not supported yet.");
                auto casted_col =
                    std::find_if(obj, obj + size, [&none](auto val) { return val != none.ptr() && !is_py_nan(val); });
                empty = casted_col == obj + size;
                sample = *casted_col;
            }
            if (empty || is_unicode(sample)) {
                val_type = ValueType::UTF_DYNAMIC;
            } else if (PYBIND11_BYTES_CHECK(sample))
                val_type = ValueType::ASCII_DYNAMIC;
        }
    }

    auto dt = combine_data_type(val_type, get_size_bits(val_bytes));
    ssize_t nbytes = size * descr->elsize;
    return {nbytes, ndim, arr->strides, arr->dimensions, dt, descr->elsize, arr->data};
}

InputTensorFrame py_ndf_to_frame(const StreamId& stream_name,
    const py::tuple& item,
    const py::object& norm_meta,
    const py::object& user_meta)
{
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
        idx_names.size(),
        idx_vals.size());

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
        if (index_tensor.data_type() == DataType::MICROS_UTC64) {

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
    std::vector<std::pair<std::string_view, NativeTensor>> tensors;
    tensors.reserve(idx_names.size() + col_names.size());

    for (auto i = 0u; i < col_vals.size(); ++i) {
        auto tensor = obj_to_tensor(col_vals[i].ptr());
        res.num_rows = std::max(res.num_rows, tensor.shape(0));
        res.desc.add_field(scalar_field_proto(tensor.data_type(), col_names[i]));
        res.field_tensors.push_back(std::move(tensor));
    }

    ARCTICDB_DEBUG(log::version(), "Received frame with descriptor {}", res.desc);
    res.set_index_range();
    return res;
}

} // namespace arcticdb::convert