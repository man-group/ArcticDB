#pragma once

#include <arcticdb/util/spinlock.hpp>
#include <pybind11/pybind11.h>

#include <memory>

namespace arcticdb {

struct PythonHandlerData {
    PythonHandlerData() :
        py_nan_(std::shared_ptr<PyObject>(create_py_nan(spin_lock_), [spinlock=spin_lock_](PyObject *py_obj) {
            spinlock->lock();
            Py_DECREF(py_obj);
            spinlock->unlock();})) {
    }

    SpinLock& spin_lock() {
        return *spin_lock_;
    }

    std::shared_ptr<SpinLock> spin_lock_ = std::make_shared<SpinLock>();
    std::shared_ptr<PyObject> py_nan_;
};
}