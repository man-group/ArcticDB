#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/python/python_handler_data.hpp>

namespace arcticdb::python_util {
void increment_none_refcount(size_t amount) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(PyGILState_Check(), "The thread incrementing None refcount must hold the GIL");
    for(size_t i = 0; i < amount; ++i) {
        Py_INCREF(Py_None);
    }
}

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
    std::fill_n(ptr_dest, count, nullptr);
    handler_data.increment_none_refcount(count);
    return ptr_dest + count;
}

}