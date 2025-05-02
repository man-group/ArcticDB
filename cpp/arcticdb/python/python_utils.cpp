#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb::python_util {

void prefill_with_none(
    PyObject** ptr_dest,
    size_t num_rows,
    size_t sparse_count,
    PythonHandlerData& python_handler_data,
    IncrementRefCount inc_ref_count) {
    std::fill_n(ptr_dest, num_rows, Py_None);

    if(inc_ref_count == IncrementRefCount::ON) {
        const auto none_count = num_rows - sparse_count;
        python_handler_data.increment_none_refcount(none_count);
    }
}

PyObject** fill_with_none(PyObject** ptr_dest, size_t count, PythonHandlerData& handler_data) {
    std::fill_n(ptr_dest, count, Py_None);
    handler_data.increment_none_refcount(count);
    return ptr_dest + count;
}

}