#pragma once

#include <pybind11/pybind11.h>
#include <arcticdb/pipeline/output_config.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/type_handler.hpp>

#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

// Python 3.12 (PEP 683) made Py_None immortal: Py_INCREF on it is a no-op, so we no longer
// need to track how many times it was used and replay the INCREFs at end-of-read.
#define ARCTICDB_PY_NONE_IMMORTAL_HEX 0x030C0000

namespace arcticdb {

namespace py = pybind11;

// Keep a single process-wide NaN owned for the lifetime of the interpreter so workers can borrow
// its PyObject* and drop PythonHandlerData without ever acquiring the GIL (safe during interpreter
// exit, early error returns, and after fork).
inline PyObject* shared_py_nan() {
    util::check(PyGILState_Check() != 0, "Expected GIL to be held when accessing Python nan");
    PYBIND11_CONSTINIT static py::gil_safe_call_once_and_store<py::object> storage;
    return storage
            .call_once_and_store_result([]() {
                auto* ptr = PyFloat_FromDouble(std::numeric_limits<double>::quiet_NaN());
                util::check(ptr != nullptr, "Got null nan ptr");
                return py::reinterpret_steal<py::object>(ptr);
            })
            .get_stored()
            .ptr();
}

struct PythonHandlerData {
    PythonHandlerData() : py_nan_(shared_py_nan()) {}

    void increment_missing_refcount(PandasMissingValue value, size_t increment) {
        if (value == PandasMissingValue::NaN) {
            nan_refcount_->fetch_add(increment, std::memory_order_relaxed);
        } else {
#if PY_VERSION_HEX < ARCTICDB_PY_NONE_IMMORTAL_HEX
            none_refcount_->fetch_add(increment, std::memory_order_relaxed);
#else
            (void)increment;
#endif
        }
    }

    bool is_nan_initialized() const { return py_nan_ != nullptr; }

    PyObject* non_owning_nan_handle() const { return py_nan_; }

    PyObject* get_missing_value(PandasMissingValue value) const {
        return value == PandasMissingValue::NaN ? non_owning_nan_handle() : Py_None;
    }

    /// The GIL must be acquired when this is called as it changes the refcount of the global static None variable which
    /// can be used by other Python threads
    void apply_none_refcount() {
#if PY_VERSION_HEX < ARCTICDB_PY_NONE_IMMORTAL_HEX
        const size_t cnt = none_refcount_->exchange(0, std::memory_order_acq_rel);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                PyGILState_Check(), "The thread incrementing None refcount must hold the GIL"
        );
        for (size_t i = 0; i < cnt; ++i) {
            Py_INCREF(Py_None);
        }
#endif
    }

    /// The GIL must be acquired when this is called as the shared NaN can be referenced by other Python threads.
    void apply_nan_refcount() {
        const size_t count = nan_refcount_->exchange(0, std::memory_order_acq_rel);
        if (count == 0) {
            return;
        }
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                PyGILState_Check(), "The thread incrementing NaN refcount must hold the GIL"
        );
#if !defined(Py_GIL_DISABLED) && !defined(Py_DEBUG)
        // Fast O(1) bulk refcount update under the GIL. Stay below PEP 683 immortal thresholds
        // (1 << 30 on 32-bit, 1 << 31 on 64-bit) and fall back to Py_INCREF otherwise.
        constexpr size_t kFastRefcntLimit = size_t{1} << 29;
        const Py_ssize_t current = Py_REFCNT(py_nan_);
        if (current > 0 && static_cast<size_t>(current) < kFastRefcntLimit &&
            count < kFastRefcntLimit - static_cast<size_t>(current)) {
            Py_SET_REFCNT(py_nan_, current + static_cast<Py_ssize_t>(count));
            return;
        }
#endif
        for (size_t i = 0; i < count; ++i) {
            Py_INCREF(py_nan_);
        }
    }

  private:
#if PY_VERSION_HEX < ARCTICDB_PY_NONE_IMMORTAL_HEX
    std::shared_ptr<std::atomic<uint64_t>> none_refcount_ = std::make_shared<std::atomic<uint64_t>>(0);
#endif
    std::shared_ptr<std::atomic<uint64_t>> nan_refcount_ = std::make_shared<std::atomic<uint64_t>>(0);
    PyObject* py_nan_;
};

inline void apply_global_refcounts(std::any& handler_data, OutputFormat output_format) {
    if (output_format == OutputFormat::PANDAS) {
        PythonHandlerData& python_handler_data = std::any_cast<PythonHandlerData&>(handler_data);
        python_handler_data.apply_nan_refcount();
        python_handler_data.apply_none_refcount();
    }
}

struct PythonHandlerDataFactory : public TypeHandlerDataFactory {
    std::any get_data() const override { return {PythonHandlerData{}}; }
};

} // namespace arcticdb
