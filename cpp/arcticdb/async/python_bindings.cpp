/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/async/python_bindings.hpp>
#include <arcticdb/async/task_scheduler.hpp>

namespace py = pybind11;

namespace arcticdb::async {

void register_bindings(py::module &m) {
    auto async = m.def_submodule("cpp_async", "Asynchronous processing");

    py::class_<TaskScheduler, std::shared_ptr<TaskScheduler>>(async, "TaskScheduler")
        .def(py::init<>([](py::kwargs &conf) {
            auto thread_count = conf.attr("get")("thread_count", 1).cast<std::size_t>();
            return std::make_shared<TaskScheduler>(thread_count);
        }), "Number of threads used to execute tasks");

    async.def("print_scheduler_stats", &print_scheduler_stats);

    async.def("reinit_task_scheduler", &arcticdb::async::TaskScheduler::reattach_instance);
    async.def("cpu_thread_count", []() {
        return arcticdb::async::TaskScheduler::instance()->cpu_thread_count();
    });
    async.def("io_thread_count", []() {
        return arcticdb::async::TaskScheduler::instance()->io_thread_count();
    });
}

} // namespace arcticdb::async


