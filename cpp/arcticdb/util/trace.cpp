/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/trace.hpp>

#ifndef _WIN32
#include <cxxabi.h>
#endif

#ifdef ARCTICDB_COUNT_ALLOCATIONS
#include <cpptrace/cpptrace.hpp>
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

#ifdef ARCTICDB_COUNT_ALLOCATIONS

std::string get_trace() { return cpptrace::generate_trace(5, 10).to_string(); }

std::string_view removePrefix(std::string_view input, std::string_view prefix) {
    auto pos = input.rfind(prefix);
    if (pos != std::string_view::npos) {
        return {input.data() + prefix.size(), input.size() - prefix.size()};
    }
    return input;
}

std::string unwind_stack(int) { return get_trace(); }

std::string unwind_stack(int max_depth) {
    void* buffer[max_depth];
    int num_frames = backtrace(buffer, max_depth);
    char** symbols = backtrace_symbols(buffer, num_frames);

    thread_local std::ostringstream oss;
    for (int i = 0; i < num_frames; ++i) {
        auto filtered = removePrefix(
                symbols[i], "/opt/arcticdb/arcticdb_link/python/arcticdb_ext.cpython-38-x86_64-linux-gnu.so"
        );
        oss << filtered << " ";
    }

    free(symbols);
    return oss.str();
}

#endif

} // namespace arcticdb