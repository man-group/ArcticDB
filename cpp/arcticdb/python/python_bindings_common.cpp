/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <pybind11/pybind11.h>
#include <arcticdb/python/python_bindings_common.hpp>
#include <arcticdb/python/python_utils.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <logger.pb.h>

namespace py = pybind11;

void register_metrics(py::module&& m, arcticdb::BindingScope scope) {
    bool local_bindings = (scope == arcticdb::BindingScope::LOCAL);
    auto prometheus = m.def_submodule("prometheus");
    py::class_<arcticdb::PrometheusInstance, std::shared_ptr<arcticdb::PrometheusInstance>>(
            prometheus, "Instance", py::module_local(local_bindings)
    );

    py::class_<arcticdb::MetricsConfig, std::shared_ptr<arcticdb::MetricsConfig>>(
            prometheus, "MetricsConfig", py::module_local(local_bindings)
    )
            .def(py::init<
                    const std::string&,
                    const std::string&,
                    const std::string&,
                    const std::string&,
                    const std::string&,
                    const arcticdb::MetricsConfig::Model>())
            .def_property_readonly("host", [](const arcticdb::MetricsConfig& config) { return config.host; })
            .def_property_readonly("port", [](const arcticdb::MetricsConfig& config) { return config.port; })
            .def_property_readonly("job_name", [](const arcticdb::MetricsConfig& config) { return config.job_name; })
            .def_property_readonly("instance", [](const arcticdb::MetricsConfig& config) { return config.instance; })
            .def_property_readonly(
                    "prometheus_env", [](const arcticdb::MetricsConfig& config) { return config.prometheus_env; }
            )
            .def_property_readonly("model", [](const arcticdb::MetricsConfig& config) { return config.model_; });

    py::enum_<arcticdb::MetricsConfig::Model>(prometheus, "MetricsConfigModel", py::module_local(local_bindings))
            .value("NO_INIT", arcticdb::MetricsConfig::Model::NO_INIT)
            .value("PUSH", arcticdb::MetricsConfig::Model::PUSH)
            .value("PULL", arcticdb::MetricsConfig::Model::PULL)
            .export_values();
}

void register_log(py::module&& log, arcticdb::BindingScope scope) {
    using arcticdb::LoggerId;
    bool local_bindings = (scope == arcticdb::BindingScope::LOCAL);

    log.def(
            "configure",
            [](const py::object& py_log_conf, bool force = false) {
                arcticdb::proto::logger::LoggersConfig config;
                arcticdb::python_util::pb_from_python(py_log_conf, config);
                return arcticdb::log::Loggers::instance().configure(config, force);
            },
            py::arg("py_log_conf"),
            py::arg("force") = false
    );

    py::enum_<spdlog::level::level_enum>(log, "LogLevel", py::module_local(local_bindings))
            .value("DEBUG", spdlog::level::level_enum::debug)
            .value("INFO", spdlog::level::level_enum::info)
            .value("WARN", spdlog::level::level_enum::warn)
            .value("ERROR", spdlog::level::level_enum::err)
            .export_values();

    py::enum_<LoggerId>(log, "LoggerId", py::module_local(local_bindings))
            .value("ROOT", LoggerId::ROOT)
            .value("STORAGE", LoggerId::STORAGE)
            .value("IN_MEM", LoggerId::IN_MEM)
            .value("CODEC", LoggerId::CODEC)
            .value("VERSION", LoggerId::VERSION)
            .value("MEMORY", LoggerId::MEMORY)
            .value("TIMINGS", LoggerId::TIMINGS)
            .value("LOCK", LoggerId::LOCK)
            .value("SCHEDULE", LoggerId::SCHEDULE)
            .value("SYMBOL", LoggerId::SYMBOL)
            .value("SNAPSHOT", LoggerId::SNAPSHOT)
            .export_values();

    auto choose_logger = [](LoggerId log_id) -> spdlog::logger& {
        switch (log_id) {
        case LoggerId::STORAGE:
            return arcticdb::log::storage();
        case LoggerId::IN_MEM:
            return arcticdb::log::inmem();
        case LoggerId::CODEC:
            return arcticdb::log::codec();
        case LoggerId::MEMORY:
            return arcticdb::log::memory();
        case LoggerId::VERSION:
            return arcticdb::log::version();
        case LoggerId::ROOT:
            return arcticdb::log::root();
        case LoggerId::TIMINGS:
            return arcticdb::log::timings();
        case LoggerId::LOCK:
            return arcticdb::log::lock();
        case LoggerId::SCHEDULE:
            return arcticdb::log::schedule();
        case LoggerId::SYMBOL:
            return arcticdb::log::symbol();
        case LoggerId::SNAPSHOT:
            return arcticdb::log::snapshot();
        default:
            arcticdb::util::raise_rte("Unsupported logger id");
        }
    };

    log.def("log", [choose_logger](LoggerId log_id, spdlog::level::level_enum level, const std::string& msg) {
        py::gil_scoped_release gil_release;
        auto& logger = choose_logger(log_id);
        switch (level) {
        case spdlog::level::level_enum::debug:
            logger.debug(msg);
            break;
        case spdlog::level::level_enum::info:
            logger.info(msg);
            break;
        case spdlog::level::level_enum::warn:
            logger.warn(msg);
            break;
        case spdlog::level::level_enum::err:
            logger.error(msg);
            break;
        default:
            arcticdb::util::raise_rte("Unsupported log level", spdlog::level::to_string_view(level));
        }
    });

    log.def("is_active", [choose_logger](LoggerId log_id, spdlog::level::level_enum level) {
        return choose_logger(log_id).should_log(level);
    });

    log.def("flush_all", []() {
        py::gil_scoped_release gil_release;
        arcticdb::log::Loggers::instance().flush_all();
    });
}
