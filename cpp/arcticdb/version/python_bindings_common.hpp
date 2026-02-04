/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>
#include <arcticdb/python/python_bindings_common.hpp>

namespace arcticdb::version_store {

void register_version_store_common_bindings(pybind11::module& m, BindingScope scope);

} // namespace arcticdb::version_store
