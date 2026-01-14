/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <typeinfo>

namespace arcticdb {

std::string get_type_name(const std::type_info& type_info);

#ifdef ARCTICDB_COUNT_ALLOCATIONS
std::string unwind_stack(int max_depth);
#endif
} // namespace arcticdb
