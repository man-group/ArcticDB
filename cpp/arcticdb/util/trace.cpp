/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/trace.hpp>

#ifndef _WIN32
#include <cxxabi.h>
#endif

namespace arcticdb {

std::string get_type_name(const std::type_info& ti) {
#ifndef _WIN32
  char* demangled = abi::__cxa_demangle(ti.name(), nullptr, nullptr, nullptr);
  std::string ret = demangled;
  free(demangled);
  return ret;
#else
  return ti.name();
#endif
}
} // namespace arcticdb
