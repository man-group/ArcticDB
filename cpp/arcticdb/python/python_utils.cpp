#include <arcticdb/python/python_utils.hpp>

namespace arcticdb::python_util {
void increment_none_refcount(size_t amount, SpinLock& lock) {
    std::lock_guard guard(lock);
    for(size_t i = 0; i < amount; ++i) {
        Py_INCREF(Py_None);
    }
}
}