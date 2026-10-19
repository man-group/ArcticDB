#include <arcticdb/column_store/column_utils.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/util/collection_utils.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/arrow/arrow_c_interface.hpp>

#include <pybind11/stl.h>
#include <string>

namespace arcticdb::python_util {

// Per-column payload for a pandas frame: either a numpy array (numeric/index/object columns) or a list of
// arrow arrays (one per row-slice, for arrow-backed string columns).
using PandasColumn = std::variant<py::array, std::vector<std::shared_ptr<RecordBatchData>>>;

PyObject** fill_with_none_or_nan(
        PyObject** ptr_dest, size_t num_rows, PythonHandlerData& handler_data, PandasMissingValue value,
        IncrementRefCount inc_ref_count, size_t sparse_count
) {
    std::fill_n(ptr_dest, num_rows, handler_data.get_missing_value(value));
    if (inc_ref_count == IncrementRefCount::ON) {
        handler_data.increment_missing_refcount(value, num_rows - sparse_count);
    }
    return ptr_dest + num_rows;
}

py::tuple extract_pandas_columns(PandasOutputFrame& pandas_output_frame) {
    auto frame = pandas_output_frame.release_frame();
    const size_t field_count = frame.fields().size();
    const size_t index_field_count = frame.descriptor().index().field_count();
    auto arrays = util::reserve_vector<PandasColumn>(field_count);
    auto index_column_names = util::reserve_vector<std::string>(index_field_count);
    auto column_names = util::reserve_vector<std::string>(field_count - index_field_count);
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
