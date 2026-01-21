/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/python/python_bindings_common.hpp>

namespace arcticdb::entity::apy {

void register_common_entity_bindings(pybind11::module& m, BindingScope scope);

} // namespace arcticdb::entity::apy
