#pragma once

#include <arcticdb/util/spinlock.hpp>
#include <pybind11/pybind11.h>

#include <memory>

namespace arcticdb {

namespace py = pybind11;

inline py::handle* create_py_nan() {
    util::check(PyGILState_Check() != 0, "Expected GIL to be held when allocating Python nan");
    auto ptr = PyFloat_FromDouble(std::numeric_limits<double>::quiet_NaN());
    util::check(ptr != nullptr, "Got null nan ptr");
    return new py::handle(ptr);
}

struct PythonHandlerData {
    PythonHandlerData() :
        py_nan_(std::shared_ptr<py::handle>(create_py_nan(), [](py::handle* py_obj) {
                util::check(PyGILState_Check() != 0, "Expected GIL to be held when deallocating Python nan");
                py_obj->dec_ref();
        })) {
    }

    SpinLock& spin_lock() {
        util::check(spin_lock_, "Spinlock not set on python handler data");
        return *spin_lock_;
    }

    std::shared_ptr<SpinLock> spin_lock_ = std::make_shared<SpinLock>();
    std::shared_ptr<py::handle> py_nan_;
};

struct PythonHandlerDataFactory  : public TypeHandlerDataFactory {
    std::any get_data() const override {
        return {PythonHandlerData{}};
    }
};

}