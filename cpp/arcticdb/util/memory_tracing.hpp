/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <array>
#include <fstream>
#include <arcticdb/util/preprocess.hpp>
#ifdef WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace arcticdb::util {

struct MemBytes {
    static auto suffixes() {
        static const char *ret[] = {" bytes", "Kb", "Mb", "Gb", "Tb", "Pb", "Eb"};
        return ret;
    }

    static const int num_suffixes_ = 7;
    uint64_t value_;
};

constexpr int page_size = 4096;

inline MemBytes pages(uint64_t num_pages) {
    return {num_pages * page_size};
}

} //namespace arcticdb::util

namespace fmt {
template<>
struct formatter<arcticdb::util::MemBytes> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::util::MemBytes bytes, FormatContext &ctx) const {
        using namespace arcticdb::util;

        uint8_t s = 0;
        double count = static_cast<double>(bytes.value_);
        while (count >= 1024 && s < MemBytes::num_suffixes_) {
            s++;
            count /= 1024;
        }

        auto suffixes = MemBytes::suffixes();
        if (count - floor(count) == 0.0)
            return format_to(ctx.out(), "{:d}{:s}", (int) count, suffixes[s]);
        else
            return format_to(ctx.out(), "{:.1f}{:s}", count, suffixes[s]);
    }
};

} //namespace fmt

namespace arcticdb::util {

inline void print_total_mem_usage(const char *file ARCTICDB_UNUSED, int line ARCTICDB_UNUSED, const char *function ARCTICDB_UNUSED) {
    auto pid = getpid();
    auto file_name = fmt::format("/proc/{:d}/statm", pid);
    std::array<int, 7> mem_stat{};

    // Don't try to do this with fstream, it doesn't work
    FILE *statm_file;
    statm_file = fopen(file_name.c_str(), "r");
    if(statm_file == nullptr) {
        ARCTICDB_DEBUG(log::memory(), "Unable to read {}", file_name);
        return;
    }

    for(auto i = 0u; i < 7u; ++i)
        fscanf(statm_file, "%d", &mem_stat[i]);

    fclose(statm_file);
    ARCTICDB_DEBUG(log::memory(), "{} ({}:{}) size {} resident {} share {} text {} data/stack {}",
                       file,
                       function,
                       line,
                       pages(mem_stat[0]),
                       pages(mem_stat[1]),
                       pages(mem_stat[2]),
                       pages(mem_stat[3]),
                       pages(mem_stat[5]));
}
}  //namespace arcticdb::util
