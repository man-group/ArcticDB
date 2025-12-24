#pragma once
#include <pybind11/pybind11.h>
#include <arcticdb/entity/metrics.hpp>

namespace arcticdb::apy {

enum class MetricsConfigPickleOrder : uint32_t {
    HOST = 0,
    PORT = 1,
    JOB_NAME = 2,
    INSTANCE = 3,
    PROMETHEUS_ENV = 4,
    MODEL = 5
};

pybind11::tuple to_tuple(const MetricsConfig& config);
MetricsConfig metrics_config_from_tuple(const pybind11::tuple& t);

} // namespace arcticdb::apy

void register_metrics(pybind11::module&& m, bool local_bindings);
