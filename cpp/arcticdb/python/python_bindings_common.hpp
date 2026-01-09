#pragma once
#include <arcticdb/entity/metrics.hpp>

namespace pybind11 {
class module_;
using module = module_;
class tuple;
} // namespace pybind11

namespace arcticdb {
enum class BindingScope : uint32_t { LOCAL = 0, GLOBAL = 1 };

pybind11::tuple to_tuple(const MetricsConfig& config);
MetricsConfig metrics_config_from_tuple(const pybind11::tuple& t);

} // namespace arcticdb

void register_metrics(pybind11::module&& m, arcticdb::BindingScope scope);
