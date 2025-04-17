#pragma once

#include <pybind11/pybind11.h>
#include <folly/ThreadCachedInt.h>
#include <arcticdb/python/python_utils.hpp>

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

    void increment_none_refcount(size_t increment) {
        none_refcount_->increment(increment);
    }

    void increment_nan_refcount(size_t increment) {
        nan_refcount_->increment(increment);
    }

    bool is_nan_initialized() const {
        return static_cast<bool>(py_nan_);
    }

    PyObject* non_owning_nan_handle() const {
        return py_nan_->ptr();
    }

    /// The GIL must be acquired when this is called as it changes the refcount of the global static None variable which
    /// can be used by other Python threads
    void apply_none_refcount() {
        const size_t cnt = none_refcount_->readFullAndReset();
        python_util::increment_none_refcount(cnt);
    }

    /// There is no need to hold the GIL for this operation as this python object was created by the
    /// PythonHandlerData object on a read/read_batch/etc... operation and not handled to python yet.
    void apply_nan_refcount() {
        const size_t count = nan_refcount_->readFullAndReset();
        for (size_t i = 0; i < count; ++i) {
            Py_INCREF(py_nan_->ptr());
        }
    }

    ~PythonHandlerData() {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(none_refcount_, "None refcount must not be null");
        const size_t none_count = none_refcount_->readFull();
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(none_count == 0, "None refcount not applied. {} more to be applied", none_count);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(nan_refcount_, "None refcount must not be null");
        const size_t nan_count = none_refcount_->readFull();
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(nan_count == 0, "NaN refcount not applied. {} more to be applied", nan_count);
    }
private:
    std::shared_ptr<folly::ThreadCachedInt<size_t>> none_refcount_ = std::make_shared<folly::ThreadCachedInt<size_t>>();
    std::shared_ptr<folly::ThreadCachedInt<size_t>> nan_refcount_ = std::make_shared<folly::ThreadCachedInt<size_t>>();
    std::shared_ptr<py::handle> py_nan_;
};

inline void apply_global_refcounts(std::any& handler_data, OutputFormat output_format) {
    if (output_format == OutputFormat::PANDAS) {
        PythonHandlerData& python_handler_data = std::any_cast<PythonHandlerData&>(handler_data);
        python_handler_data.apply_nan_refcount();
        python_handler_data.apply_none_refcount();
    }
}

struct PythonHandlerDataFactory  : public TypeHandlerDataFactory {
    std::any get_data() const override {
        return std::any{PythonHandlerData()};
    }
};

}
