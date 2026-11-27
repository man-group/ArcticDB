/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/python/python_types.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <vector>

#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

TEST(PythonHandlerData, GetMissingValue) {
    using namespace arcticdb;

    ScopedGILLock gil;
    PythonHandlerData handler_data;
    EXPECT_EQ(handler_data.get_missing_value(PandasMissingValue::None), Py_None);
    EXPECT_EQ(handler_data.get_missing_value(PandasMissingValue::NaN), handler_data.non_owning_nan_handle());
    EXPECT_TRUE(is_py_nan(handler_data.non_owning_nan_handle()));
}

TEST(PythonHandlerData, DestructionDoesNotWaitForGil) {
    using namespace arcticdb;

    ScopedGILLock gil;
    std::promise<void> release_worker;
    std::future<void> worker;
    {
        auto handler_data = std::make_shared<PythonHandlerData>();
        worker = std::async(
                std::launch::async,
                [data = std::move(handler_data), ready = release_worker.get_future()]() mutable {
                    ready.wait();
                    data.reset();
                }
        );
    }

    release_worker.set_value();
    const auto status = worker.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(status, std::future_status::ready) << "Worker must release handler data while the caller holds the GIL";
    if (status != std::future_status::ready) {
        py::gil_scoped_release release;
        worker.wait();
    }
    worker.get();
}

TEST(PythonHandlerData, UnusedHandlerDataReusesNanWithoutLeakingRefcount) {
    using namespace arcticdb;

    ScopedGILLock gil;
    PyObject* shared_nan = nullptr;
    Py_ssize_t baseline_refcnt = 0;
    {
        PythonHandlerData initial;
        shared_nan = initial.non_owning_nan_handle();
        ASSERT_NE(shared_nan, nullptr);
        baseline_refcnt = Py_REFCNT(shared_nan);
    }

    for (const bool apply_zero : {false, true}) {
        PythonHandlerData handler_data;
        EXPECT_EQ(handler_data.non_owning_nan_handle(), shared_nan);
        if (apply_zero) {
            handler_data.apply_nan_refcount();
        }
    }
    EXPECT_EQ(Py_REFCNT(shared_nan), baseline_refcnt);
}

TEST(PythonHandlerData, ApplyNanRefcountAccumulatesAcrossReads) {
    using namespace arcticdb;

    ScopedGILLock gil;
    PythonHandlerData first_read;
    PythonHandlerData second_read;
    PyObject* const nan = first_read.non_owning_nan_handle();
    ASSERT_EQ(second_read.non_owning_nan_handle(), nan);
    const Py_ssize_t baseline_refcnt = Py_REFCNT(nan);

    first_read.increment_missing_refcount(PandasMissingValue::NaN, 2);
    first_read.apply_nan_refcount();
    EXPECT_EQ(Py_REFCNT(nan), baseline_refcnt + 2);

    second_read.increment_missing_refcount(PandasMissingValue::NaN, 3);
    second_read.apply_nan_refcount();
    EXPECT_EQ(Py_REFCNT(nan), baseline_refcnt + 5);

    // Calling apply_nan_refcount again after draining is a no-op.
    second_read.apply_nan_refcount();
    EXPECT_EQ(Py_REFCNT(nan), baseline_refcnt + 5);

    {
        std::vector<py::object> slots;
        slots.reserve(5);
        for (int i = 0; i < 5; ++i) {
            slots.emplace_back(py::reinterpret_steal<py::object>(nan));
        }
    }
    EXPECT_EQ(Py_REFCNT(nan), baseline_refcnt);
}

#ifndef _WIN32
TEST(PythonHandlerData, ForkChildPreservesSharedNan) {
    using namespace arcticdb;

    ScopedGILLock gil;
    PythonHandlerData parent_data;
    PyObject* const parent_nan = parent_data.non_owning_nan_handle();
    ASSERT_TRUE(is_py_nan(parent_nan));
    const Py_ssize_t baseline_refcnt = Py_REFCNT(parent_nan);

    PyOS_BeforeFork();
    const pid_t child = fork();
    if (child == 0) {
        alarm(10);
        PyOS_AfterFork_Child();
        PythonHandlerData child_data;
        PyObject* const child_nan = child_data.non_owning_nan_handle();
        bool ok = child_nan == parent_nan && is_py_nan(child_nan) && Py_REFCNT(child_nan) == baseline_refcnt;
        child_data.increment_missing_refcount(PandasMissingValue::NaN, 2);
        child_data.apply_nan_refcount();
        ok = ok && Py_REFCNT(child_nan) == baseline_refcnt + 2;
        {
            py::object slot1 = py::reinterpret_steal<py::object>(child_nan);
            py::object slot2 = py::reinterpret_steal<py::object>(child_nan);
            (void)slot1;
            (void)slot2;
        }
        ok = ok && Py_REFCNT(child_nan) == baseline_refcnt;
        std::_Exit(ok ? EXIT_SUCCESS : EXIT_FAILURE);
    }
    PyOS_AfterFork_Parent();
    int status = 0;
    ASSERT_GT(child, 0);
    ASSERT_EQ(waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), EXIT_SUCCESS);
    EXPECT_EQ(Py_REFCNT(parent_nan), baseline_refcnt);
}
#endif
