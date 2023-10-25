/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
*/

#pragma once

#include <arcticdb/log/log.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/preprocess.hpp>

#include <fmt/compile.h>

namespace arcticdb {

namespace util::detail {

template<ErrorCode code, ErrorCategory error_category>
struct Raise {
    static_assert(get_error_category(code) == error_category);

    template<typename...Args>
    [[noreturn]] void operator()(fmt::format_string<Args...> format, Args&&...args) const {
        std::string combo_format = fmt::format(FMT_COMPILE("{} {}"), error_code_data<code>.name_, format);
        std::string msg = fmt::format(combo_format, std::forward<Args>(args)...);
        if constexpr(error_category == ErrorCategory::INTERNAL)
            log::root().error(msg);
        throw_error<code>(msg);
    }

    template<typename FormatString, typename...Args>
    [[noreturn]] void operator()(FormatString format, Args&&...args) const {
        std::string combo_format = fmt::format(FMT_COMPILE("{} {}"), error_code_data<code>.name_, format);
        std::string msg = fmt::format(combo_format, std::forward<Args>(args)...);
        if constexpr(error_category == ErrorCategory::INTERNAL)
            log::root().error(msg);
        throw_error<code>(msg);
    }
};

template<ErrorCode code, ErrorCategory error_category>
struct Check {
    static constexpr Raise<code, error_category> raise{};

    template<typename...Args>
    void operator()(bool cond, fmt::format_string<Args...> format, Args&&...args) const {
        if (ARCTICDB_UNLIKELY(!cond)) {
            raise(format, std::forward<Args>(args)...);
        }
    }

    template<typename FormatString, typename...Args>
    void operator()(bool cond, FormatString format, Args&&...args) const {
        if (ARCTICDB_UNLIKELY(!cond)) {
            raise(format, std::forward<Args>(args)...);
        }
    }
};
} // namespace util::detail

namespace internal {
    template<ErrorCode code>
    constexpr auto check = util::detail::Check<code, ErrorCategory::INTERNAL>{};

    template<ErrorCode code>
    constexpr auto raise = check<code>.raise;
}

namespace normalization {
template<ErrorCode code>
constexpr auto check = util::detail::Check<code, ErrorCategory::NORMALIZATION>{};

template<ErrorCode code>
constexpr auto raise = check<code>.raise;
}

namespace missing_data {

template<ErrorCode code>
constexpr auto check = util::detail::Check<code, ErrorCategory::MISSING_DATA>{};

template<ErrorCode code>
constexpr auto raise = check<code>.raise;
}

namespace schema {
    template<ErrorCode code>
    constexpr auto check = util::detail::Check<code, ErrorCategory::SCHEMA>{};

    template<ErrorCode code>
    constexpr auto raise = check<code>.raise;
}

namespace storage {

template<ErrorCode code>
constexpr auto check = util::detail::Check<code, ErrorCategory::STORAGE>{};

template<ErrorCode code>
constexpr auto raise = check<code>.raise;
}

namespace sorting {
    template<ErrorCode code>
    constexpr auto check = util::detail::Check<code, ErrorCategory::SORTING>{};

    template<ErrorCode code>
    constexpr auto raise = check<code>.raise;
}

namespace user_input {
    template<ErrorCode code>
    constexpr auto check = util::detail::Check<code, ErrorCategory::USER_INPUT>{};

    template<ErrorCode code>
    constexpr auto raise = check<code>.raise;
}

namespace compatibility {
    template<ErrorCode code>
    constexpr auto check = util::detail::Check<code, ErrorCategory::COMPATIBILITY>{};

    template<ErrorCode code>
    constexpr auto raise = check<code>.raise;
}



// TODO Change legacy codes to internal::
namespace util {

    constexpr auto check = util::detail::Check<ErrorCode::E_ASSERTION_FAILURE, ErrorCategory::INTERNAL>{};

    constexpr auto check_range_impl =  util::detail::Check<ErrorCode::E_INVALID_RANGE, ErrorCategory::INTERNAL>{};

template<typename...Args>
void check_range(size_t idx, size_t size, const char *msg) {
    check_range_impl(idx < size, "{} expected  0 <= idx < size, actual idx={}, size={}", msg, idx, size);
}

constexpr auto check_arg =  util::detail::Check<ErrorCode::E_INVALID_ARGUMENT, ErrorCategory::INTERNAL>{};

// TODO Replace occurrences with specific error code
constexpr auto check_rte = util::detail::Check<ErrorCode::E_RUNTIME_ERROR, ErrorCategory::INTERNAL>{};

// TODO Replace occurrences with specific error code
constexpr auto raise_rte = check.raise;

template<typename...Args>
void warn(bool cond, const char *format, const Args &...args) {
    if (ARCTICDB_UNLIKELY(!cond)) {
        std::string err = fmt::vformat(format,  fmt::make_format_args(args...));
        log::root().warn("ASSERTION WARNING: {}", err);
    }
}

struct WarnOnce {
    bool warned_ = false;

    template<typename...Args>
    void check(bool cond, const char *format, const Args &...args) {
        if (!warned_) {
            warn(cond, format, std::forward<const Args&>(args)...);
            warned_ = true;
        }
    }
};

} // namespace util

} // namespace arcticdb

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
