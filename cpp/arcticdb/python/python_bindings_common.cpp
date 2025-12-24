#include <arcticdb/python/python_bindings_common.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace py = pybind11;

namespace arcticdb::apy {

py::tuple to_tuple(const MetricsConfig& config) {
    return py::make_tuple(
            config.host, config.port, config.job_name, config.instance, config.prometheus_env, config.model_
    );
}

MetricsConfig metrics_config_from_tuple(const py::tuple& t) {
    util::check(t.size() == 6, "Invalid MetricsConfig pickle object, expected 6 attributes but was {}", t.size());
    return MetricsConfig{
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::HOST)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::PORT)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::JOB_NAME)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::INSTANCE)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::PROMETHEUS_ENV)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::MODEL)].cast<MetricsConfig::Model>()
    };
}

} // namespace arcticdb::apy

void register_metrics(py::module&& m, bool local_bindings) {

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
            .def(py::pickle(
                    [](const arcticdb::MetricsConfig& config) { return arcticdb::apy::to_tuple(config); },
                    [](py::tuple t) { return arcticdb::apy::metrics_config_from_tuple(t); }
            ));

    py::enum_<arcticdb::MetricsConfig::Model>(prometheus, "MetricsConfigModel", py::module_local(local_bindings))
            .value("NO_INIT", arcticdb::MetricsConfig::Model::NO_INIT)
            .value("PUSH", arcticdb::MetricsConfig::Model::PUSH)
            .value("PULL", arcticdb::MetricsConfig::Model::PULL)
            .export_values();

    prometheus.def("metrics_config_from_tuple", &arcticdb::apy::metrics_config_from_tuple);
}
