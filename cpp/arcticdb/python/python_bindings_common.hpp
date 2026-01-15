/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

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
