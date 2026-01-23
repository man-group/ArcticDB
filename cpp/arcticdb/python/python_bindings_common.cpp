/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <pybind11/pybind11.h>
#include <arcticdb/python/python_bindings_common.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/util/preconditions.hpp>

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
