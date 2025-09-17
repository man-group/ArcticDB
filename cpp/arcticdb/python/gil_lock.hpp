#pragma once
#include <pybind11/pybind11.h>
namespace arcticdb {

struct GILLock {
    PyGILState_STATE gstate;

    void lock() { gstate = PyGILState_Ensure(); }

    void unlock() { PyGILState_Release(gstate); }
};

class ScopedGILLock {
  public:
    ScopedGILLock() : acquired_gil_(false) { acquire(); }

    ~ScopedGILLock() { release(); }

    void acquire() {
        if (!acquired_gil_) {
            state_ = PyGILState_Ensure();
            acquired_gil_ = true;
        }
    }

    void release() {
        if (acquired_gil_) {
            PyGILState_Release(state_);
            acquired_gil_ = false;
        }
    }

  private:
    bool acquired_gil_;
    PyGILState_STATE state_ = PyGILState_UNLOCKED;
};

} // namespace arcticdb