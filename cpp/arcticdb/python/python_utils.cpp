#include <arcticdb/column_store/column_utils.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_c_interface.hpp>

#include <pybind11/stl.h>

namespace arcticdb::python_util {

// Per-column payload for a pandas frame: either a numpy array (numeric/index/object columns) or a list of
// arrow arrays (one per row-slice, for arrow-backed string columns).
using PandasColumn = std::variant<py::array, std::vector<std::shared_ptr<RecordBatchData>>>;

void prefill_with_none(
        PyObject** ptr_dest, size_t num_rows, size_t sparse_count, PythonHandlerData& python_handler_data,
        IncrementRefCount inc_ref_count
) {
    std::fill_n(ptr_dest, num_rows, Py_None);

    if (inc_ref_count == IncrementRefCount::ON) {
        const auto none_count = num_rows - sparse_count;
        python_handler_data.increment_none_refcount(none_count);
    }
}

PyObject** fill_with_none(PyObject** ptr_dest, size_t count, PythonHandlerData& handler_data) {
    std::fill_n(ptr_dest, count, Py_None);
    handler_data.increment_none_refcount(count);
    return ptr_dest + count;
}

py::tuple extract_numpy_arrays(PandasOutputFrame& pandas_output_frame) {
    auto frame = pandas_output_frame.release_frame();
    const size_t field_count = frame.fields().size();
    const size_t index_field_count = frame.descriptor().index().field_count();
    std::vector<PandasColumn> arrays;
    std::vector<std::string> index_column_names;
    std::vector<std::string> column_names;
    arrays.reserve(field_count);
    index_column_names.reserve(index_field_count);
    column_names.reserve(field_count - index_field_count);
    for (std::size_t c = 0; c < field_count; ++c) {
        if (frame.column(c).buffer().has_extra_bytes_per_block()) { // Checks if this column decoded as arrow string
            arrays.emplace_back(column_to_arrow_arrays(frame.column(c), frame.field(c).name()));
        } else {
            arrays.emplace_back(arcticdb::detail::array_at(frame, c));
        }
        if (c < index_field_count) {
            index_column_names.emplace_back(frame.field(c).name());
        } else {
            column_names.emplace_back(frame.field(c).name());
        }
    }
    return py::make_tuple(
            std::move(arrays), std::move(column_names), std::move(index_column_names), frame.row_count(), frame.offset()
    );
}

} // namespace arcticdb::python_util
