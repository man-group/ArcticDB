/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <folly/Likely.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <arcticdb/log/log.hpp>
#include <signal.h>

#include <exception>
#include <cstdint>
#include <type_traits>
#include <arcticdb/util/preprocess.hpp>

namespace arcticdb::util {

template<typename...Args>
void check_range(size_t idx, size_t first_incl, size_t last_excl, const char *msg) {
    if (UNLIKELY(!(first_incl <= idx && idx < last_excl)))
        throw std::out_of_range(fmt::format(
            "{} expected first <= idx< last, actual first={}, idx={}, actual={}",
            msg, first_incl, idx, last_excl));
}

template<typename...Args>
void check_range(size_t idx, size_t size, const char *msg) {
    if (UNLIKELY(idx >= size))
        throw std::out_of_range(fmt::format("{} expected  0 <= idx < size, actual idx={}, size={}",
                                            msg, idx, size));
}

template<class Exception=std::invalid_argument, typename...Args>
void check(bool cond, fmt::format_string<Args...> format, Args &&...args) {
    if (UNLIKELY(!cond)) {
        std::string err = fmt::vformat(format, fmt::make_format_args(std::forward<Args>(args)...));
        log::root().error("ASSERTION FAILURE: {}", err);
        throw Exception(err);
    }
}

template<typename...Args>
void check_arg(bool cond, const char *format, const Args &...args) {
    check<std::invalid_argument>(cond, format, args...);
}

template<typename...Args>
void debug_check(bool cond ARCTICDB_UNUSED, const char *format ARCTICDB_UNUSED, const Args &...args ARCTICDB_UNUSED) {
#ifdef DEBUG_BUILD
    if (UNLIKELY(!cond)) {
        std::string err = fmt::vformat(format,  fmt::make_format_args(args...));
        log::root().error("ASSERTION FAILURE: {}", err);
        throw std::invalid_argument(err);
    }
#endif
}

template<typename...Args>
void check_rte(bool cond, const char *format, const Args &...args) {
    check<std::runtime_error>(cond, format, args...);
}

template<typename...Args>
void warn(bool cond, const char *format, const Args &...args) {
    if (UNLIKELY(!cond)) {
        std::string err = fmt::vformat(format,  fmt::make_format_args(args...));
        log::root().warn("ASSERTION WARNING: {}", err);
    }
}

[[ noreturn ]] inline void raise_rte(const char *err){
    ARCTICDB_DEBUG(log::root(), "Explicity throw: {}", err);
    throw std::runtime_error(err);
}

template<class ...Args>
[[ noreturn ]] void raise_rte(const char *format, const Args &...args){
    std::string err = fmt::vformat(format,  fmt::make_format_args(args...));
    ARCTICDB_DEBUG(log::root(), "Explicity throw: {}", err);
    throw std::runtime_error(err);
}

} // arcticdb::util

// useful to enable deferred formatting using lambda
namespace fmt {
template <typename A>
struct formatter<A, std::enable_if_t<std::is_invocable_v<A>,char>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const A &a, FormatContext &ctx) const {
        return format_to(ctx.out(), "~({})", a());
    }
};
}
