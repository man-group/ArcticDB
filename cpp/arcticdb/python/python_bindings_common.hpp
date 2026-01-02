#pragma once
#include <pybind11/pybind11.h>
#include <arcticdb/entity/metrics.hpp>

namespace arcticdb {
enum class BindingScope : uint32_t {
    LOCAL = 0,
    GLOBAL = 1
};

pybind11::tuple to_tuple(const MetricsConfig& config);
MetricsConfig metrics_config_from_tuple(const pybind11::tuple& t);

} // namespace arcticdb

void register_metrics(pybind11::module&& m, arcticdb::BindingScope scope);
