#include <pybind11/pybind11.h>
#include <arcticdb/python/python_bindings_common.hpp>
#include <arcticdb/entity/metrics.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace py = pybind11;

namespace arcticdb {

enum class MetricsConfigPickleOrder : uint32_t {
    HOST = 0,
    PORT = 1,
    JOB_NAME = 2,
    INSTANCE = 3,
    PROMETHEUS_ENV = 4,
    MODEL = 5
};

py::tuple to_tuple(const MetricsConfig& config) {
    return py::make_tuple(
            config.host, config.port, config.job_name, config.instance, config.prometheus_env, config.model_
    );
}

MetricsConfig metrics_config_from_tuple(const py::tuple& t) {
    static size_t py_object_size = 6;
    util::check(
            t.size() >= py_object_size,
            "Invalid MetricsConfig pickle object, expected at least {} attributes but was {}",
            py_object_size,
            t.size()
    );
    util::warn(
            t.size() > py_object_size,
            "MetricsConfig py tuple expects {} attributes but has {}. Will continue by ignoring extra attributes.",
            py_object_size,
            t.size()
    );
    return MetricsConfig{
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::HOST)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::PORT)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::JOB_NAME)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::INSTANCE)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::PROMETHEUS_ENV)].cast<std::string>(),
            t[static_cast<uint32_t>(MetricsConfigPickleOrder::MODEL)].cast<MetricsConfig::Model>()
    };
}

} // namespace arcticdb

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
            .def(py::pickle(
                    [](const arcticdb::MetricsConfig& config) { return arcticdb::to_tuple(config); },
                    [](py::tuple t) { return arcticdb::metrics_config_from_tuple(t); }
            ));

    py::enum_<arcticdb::MetricsConfig::Model>(prometheus, "MetricsConfigModel", py::module_local(local_bindings))
            .value("NO_INIT", arcticdb::MetricsConfig::Model::NO_INIT)
            .value("PUSH", arcticdb::MetricsConfig::Model::PUSH)
            .value("PULL", arcticdb::MetricsConfig::Model::PULL)
            .export_values();

    prometheus.def("metrics_config_from_tuple", &arcticdb::metrics_config_from_tuple);
}
