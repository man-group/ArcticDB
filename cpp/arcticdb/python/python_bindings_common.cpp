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
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/python/python_utils.hpp>

namespace py = pybind11;

arcticdb::proto::utils::PrometheusConfig_PrometheusModel model_to_proto(arcticdb::MetricsConfig::Model model) {
    switch (model) {
    case arcticdb::MetricsConfig::Model::NO_INIT:
        return arcticdb::proto::utils::PrometheusConfig_PrometheusModel_NO_INIT;
    case arcticdb::MetricsConfig::Model::PUSH:
        return arcticdb::proto::utils::PrometheusConfig_PrometheusModel_PUSH;
    case arcticdb::MetricsConfig::Model::PULL:
        return arcticdb::proto::utils::PrometheusConfig_PrometheusModel_WEB;
    default:
        return arcticdb::proto::utils::PrometheusConfig_PrometheusModel_NO_INIT;
    }
}

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

    prometheus.def("configure", [](const py::object& py_config) {
        arcticdb::proto::utils::PrometheusConfig proto_config;
        arcticdb::python_util::pb_from_python(py_config, proto_config);
        arcticdb::configure_prometheus_from_proto(proto_config);
    });

    prometheus.def("get_config", []() {
        arcticdb::proto::utils::PrometheusConfig proto_config;
        const auto& cfg = arcticdb::PrometheusInstance::instance()->cfg_;
        proto_config.set_host(cfg.host);
        proto_config.set_port(cfg.port);
        proto_config.set_job_name(cfg.job_name);
        proto_config.set_instance(cfg.instance);
        proto_config.set_prometheus_env(cfg.prometheus_env);
        proto_config.set_prometheus_model(model_to_proto(cfg.model_));
        return arcticdb::python_util::pb_to_python(proto_config);
    });
}
