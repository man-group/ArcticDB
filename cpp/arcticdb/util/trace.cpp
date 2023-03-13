/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/trace.hpp>

#ifndef  _WIN32
#include <cxxabi.h>
#include <execinfo.h>
#endif

#include <iostream>
#include <sstream>

namespace arcticdb {

    std::string get_stack() {
#ifndef _WIN32
        char sep = '\n';
        void *trace[256];
        int trace_size = ::backtrace(trace, 256);
        char **buffer = ::backtrace_symbols(trace, trace_size);

        std::string stack;
        for (int i = 1; buffer && i < trace_size - 1; i++) {
            stack += "[#" + std::to_string((trace_size - 1) - i) + "] ";
            std::string s = buffer[i];
            std::string mangle = s.substr(s.find('(') + 1, s.find('+') - s.find('(') - 1);

            if (char *demangled = abi::__cxa_demangle(mangle.c_str(), nullptr, nullptr, nullptr)) {
                std::string dm = demangled;
                free(demangled);
                stack += dm;
            } else stack += mangle;

            stack += sep;
        }

        if (buffer)
            free(buffer);

        return stack;
#else
        //TODO: Implement get_stack for windows
        return std::string("");
#endif
    }

    std::string get_type_name(const std::type_info & ti){
#ifndef _WIN32
        char* demangled = abi::__cxa_demangle(ti.name(), nullptr, nullptr, nullptr);
        std::string ret = demangled;
        free(demangled);
        return ret;
#else
        //TODO: Implement get_name_type for windows
        return std::string("");
#endif
    }

    void do_assert(const char *assertion, const char *message, const char *file, int line) {
        throw std::runtime_error(fmt::format("Assertion failed: {}\n{}\n at {}: {}\n{}",
                                             assertion,
                                             message,
                                             file,
                                             line,
                                             get_stack()));
    }

}