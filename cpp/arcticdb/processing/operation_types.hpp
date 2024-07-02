/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <unordered_set>
#include <optional>

#include <arcticdb/processing/signed_unsigned_comparison.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <ankerl/unordered_dense.h>

namespace arcticdb {
// If reordering this enum, is_binary_operation may also need to be changed
enum class OperationType : uint8_t {
    // Unary
    // Operator
    ABS,
    NEG,
    // Comparison
    ISNULL,
    NOTNULL,
    // Boolean
    IDENTITY,
    NOT,
    // Binary
    // Operator
    ADD,
    SUB,
    MUL,
    DIV,
    // Comparison
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE,
    ISIN,
    ISNOTIN,
    // Boolean
    AND,
    OR,
    XOR
};

constexpr bool is_binary_operation(OperationType o) {
    return uint8_t(o) >= uint8_t(OperationType::ADD);
}

struct AbsOperator;
struct NegOperator;
struct PlusOperator;
struct MinusOperator;
struct TimesOperator;
struct DivideOperator;
struct MembershipOperator;

namespace arithmetic_promoted_type::details {
    template <class VAL>
    struct width {
        static constexpr size_t value = sizeof(VAL);
    };
    template <class LHS, class RHS>
    struct max_width {
        static constexpr size_t value = std::max(sizeof(LHS), sizeof(RHS));
    };
    // Has member type naming an unsigned integer of WIDTH 1, 2, 4, or 8 bytes (the default)
    template<size_t WIDTH>
    struct unsigned_width {
        using type = typename
            std::conditional_t<WIDTH == 1,
                uint8_t,
                std::conditional_t<WIDTH == 2,
                    uint16_t,
                    std::conditional_t<WIDTH == 4,
                        uint32_t,
                        uint64_t
                    >
                >
            >;
    };
    // Has member type naming a signed integer of WIDTH 1, 2, 4, or 8 bytes (the default)
    template<size_t WIDTH>
    struct signed_width {
        using type = typename
            std::conditional_t<WIDTH == 1,
                int8_t,
                std::conditional_t<WIDTH == 2,
                    int16_t,
                    std::conditional_t<WIDTH == 4,
                        int32_t,
                        int64_t
                    >
                >
            >;
    };

    template <class VAL>
    inline constexpr size_t width_v = width<VAL>::value;
    template <class LHS, class RHS>
    inline constexpr size_t max_width_v = max_width<LHS, RHS>::value;
    template <size_t WIDTH>
    using unsigned_width_t = typename unsigned_width<WIDTH>::type;
    template <size_t WIDTH>
    using signed_width_t = typename signed_width<WIDTH>::type;
}

template <class VAL, class Func>
struct unary_arithmetic_promoted_type {
    static constexpr size_t val_width = arithmetic_promoted_type::details::width_v<VAL>;
    using type = typename
        /* All types promote to themselves with the AbsOperator
         * Floating point and signed integer types promote to themselves with the NegOperator as well */
        std::conditional_t<std::is_same_v<Func, AbsOperator> || std::is_signed_v<VAL>,
            VAL,
            // Unsigned integer types promote to a signed type of double the width with the NegOperator
            typename arithmetic_promoted_type::details::signed_width_t<2 * val_width>
        >;
};

template <class LHS, class RHS, class Func>
struct type_arithmetic_promoted_type {
    static constexpr size_t max_width = arithmetic_promoted_type::details::max_width_v<LHS, RHS>;
    using type = typename
        std::conditional_t<std::is_floating_point_v<LHS> || std::is_floating_point_v<RHS>,
            // At least one of the types is floating point
            std::conditional_t<std::is_floating_point_v<LHS> && std::is_floating_point_v<RHS>,
                // If both types are floating point, promote to the type of the widest one
                std::conditional_t<max_width == 8,
                    double,
                    float
                >,
                // Otherwise, if only one type is floating point, promote to this type
                std::conditional_t<std::is_floating_point_v<LHS>,
                    LHS,
                    RHS
                >
            >,
            // Otherwise, both types are integers
            std::conditional_t<std::is_unsigned_v<LHS> && std::is_unsigned_v<RHS>,
                // Both types are unsigned
                std::conditional_t<std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, TimesOperator>,
                    /* Plus and Times operators can overflow if using max_width, so promote to a wider unsigned type
                     * e.g. 255*255 (both uint8_t's) = 65025, requiring uint16_t to hold the result */
                    typename arithmetic_promoted_type::details::unsigned_width_t<2 * max_width>,
                    std::conditional_t<std::is_same_v<Func, MinusOperator>,
                        /* The result of Minus with two unsigned types can be negative
                         * Can also underflow if using max_width, so promote to a wider signed type
                         * e.g. 0 - 255 (both uint8_t's) = -255, requiring int16_t to hold the result */
                        typename arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                        /* The result of integer division with two unsigned types COULD always be represented by an
                         * unsigned type of the width of the numerator (as the result is <= the numerator)
                         * However, this would require extra logic in the DivideOperator::apply method, which we would
                         * like to avoid.
                         * e.g. uint8_t(200) / uint16_t(300) = 0, representable by uint8_t
                         * e.g. uint8_t(200) / uint16_t(2) = 100, representable by uint8_t
                         * We cannot safely static_cast the uint16_t to uint8_t without a runtime check if it is >255
                         * We could also static_cast to the larger type, perform the division, and then static_cast back
                         * To avoid all this complexity, we just promote to the type of the widest input */
                        typename arithmetic_promoted_type::details::unsigned_width_t<max_width>
                    >
                >,
                std::conditional_t<std::is_signed_v<LHS> && std::is_signed_v<RHS>,
                    // Both types are signed integers (as we are in the "else" of the floating point checks)
                    std::conditional_t<std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, MinusOperator> || std::is_same_v<Func, TimesOperator>,
                        /* Plus, Minus, and Times operators can overflow if using max_width, so promote to a wider signed type
                        * e.g. -100*100 (both int8_t's) = -10000, requiring int16_t to hold the result */
                        typename arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                        // See above comment on division of two unsigned types
                        typename arithmetic_promoted_type::details::signed_width_t<max_width>
                    >,
                    // We have one signed and one unsigned type
                    std::conditional_t<std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, MinusOperator> || std::is_same_v<Func, TimesOperator>,
                        // Plus, Minus, and Times operators can overflow if using max_width, so promote to a wider signed type
                        typename arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                        // Divide/IsIn/IsNotIn Operator
                        std::conditional_t<(std::is_signed_v<LHS> && sizeof(LHS) > sizeof(RHS)) || (std::is_signed_v<RHS> && sizeof(RHS) > sizeof(LHS)),
                            // If the signed type is strictly larger than the unsigned type, then promote to the signed type
                            typename arithmetic_promoted_type::details::signed_width_t<max_width>,
                            // Otherwise, check if the unsigned one is the widest type we support
                            std::conditional_t<std::is_same_v<LHS, uint64_t> || std::is_same_v<RHS, uint64_t>,
                                // If so, there's no common type that can completely hold both arguments. We trigger operation-specific handling
                                std::conditional_t<std::is_base_of_v<MembershipOperator, Func>,
                                    RHS, // Retains ValueSetBaseType in binary_membership()
                                    int64_t>, // Retain the broken behaviour for Divide for now (https://github.com/man-group/ArcticDB/issues/594)
                                // There should be a signed type wider than the unsigned type, so both can be exactly represented
                                typename arithmetic_promoted_type::details::signed_width_t<2 * max_width>
                            >
                        >
                    >
                >
            >
        >;
};

struct AbsOperator {
template<typename T, typename V = typename unary_arithmetic_promoted_type<T, AbsOperator>::type>
V apply(T t) {
    if constexpr(std::is_unsigned_v<T>)
        return t;
    else
        return std::abs(static_cast<V>(t));
}
};

struct NegOperator {
template<typename T, typename V = typename unary_arithmetic_promoted_type<T, NegOperator>::type>
V apply(T t) {
    return -static_cast<V>(t);
}
};

// Needed for null and not null operators as INT64, NANOSECONDS_UTC64, and all string columns hold int64_t values
struct TimeTypeTag{};
struct StringTypeTag{};

struct IsNullOperator {
template<typename tag, std::enable_if_t<std::is_same_v<tag, TimeTypeTag> || std::is_same_v<tag, StringTypeTag>, bool> = true>
bool apply(int64_t t) {
    if constexpr (std::is_same_v<tag, TimeTypeTag>) {
        return t == NaT;
    } else if constexpr (std::is_same_v<tag, StringTypeTag>) {
        // Relies on string_nan == string_none - 1
        return t >=  string_nan;
    }
}
template<typename T, std::enable_if_t<std::is_floating_point_v<T>, bool> = true>
bool apply(T t) {
    return std::isnan(t);
}
};

struct NotNullOperator {
template<typename tag, std::enable_if_t<std::is_same_v<tag, TimeTypeTag> || std::is_same_v<tag, StringTypeTag>, bool> = true>
bool apply(int64_t t) {
    if constexpr (std::is_same_v<tag, TimeTypeTag>) {
        return t != NaT;
    } else if constexpr (std::is_same_v<tag, StringTypeTag>) {
        // Relies on string_nan == string_none - 1
        return t < string_nan;
    }
}
template<typename T, std::enable_if_t<std::is_floating_point_v<T>, bool> = true>
bool apply(T t) {
    return !std::isnan(t);
}
};

struct PlusOperator {
template<typename T, typename U, typename V = typename type_arithmetic_promoted_type<T, U, PlusOperator>::type>
V apply(T t, U u) {
    return static_cast<V>(t) + static_cast<V>(u);
}
};

struct MinusOperator {
template<typename T, typename U, typename V = typename type_arithmetic_promoted_type<T, U, MinusOperator>::type>
V apply(T t, U u) {
    return static_cast<V>(t) - static_cast<V>(u);
}
};

struct TimesOperator {
template<typename T, typename U, typename V = typename type_arithmetic_promoted_type<T, U, TimesOperator>::type>
V apply(T t, U u) {
    return static_cast<V>(t) * static_cast<V>(u);
}
};

struct DivideOperator {
template<typename T, typename U, typename V = typename type_arithmetic_promoted_type<T, U, DivideOperator>::type>
V apply(T t, U u) {
    return static_cast<V>(t) / static_cast<V>(u);
}
};

struct EqualsOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t == T(u);
}
template<typename T>
bool operator()(T t, std::optional<T> u) const {
    if (u.has_value())
        return t == *u;
    else
        return false;
}
template<typename T>
bool operator()(std::optional<T> t, T u) const {
    if (t.has_value())
        return *t == u;
    else
        return false;
}
template<typename T>
bool operator()(std::optional<T> t, std::optional<T> u) const {
    if (t.has_value() && u.has_value())
        return *t == *u;
    else
        return false;
}
};

struct NotEqualsOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t != T(u);
}
template<typename T>
bool operator()(T t, std::optional<T> u) const {
    if (u.has_value())
        return t != *u;
    else
        return true;
}
template<typename T>
bool operator()(std::optional<T> t, T u) const {
    if (t.has_value())
        return *t != u;
    else
        return true;
}
template<typename T>
bool operator()(std::optional<T> t, std::optional<T> u) const {
    if (t.has_value() && u.has_value())
        return *t != *u;
    else
        return true;
}
};

struct LessThanOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t < T(u);
}
template<typename T>
bool operator()([[maybe_unused]] std::optional<T> t, [[maybe_unused]] T u) const {
    util::raise_rte("Less than operator not supported with strings");
}
template<typename T>
bool operator()([[maybe_unused]] T t, [[maybe_unused]] std::optional<T> u) const {
    util::raise_rte("Less than operator not supported with strings");
}
bool operator()(uint64_t t, int64_t u) const {
    return comparison::less_than(t, u);
}
bool operator()(int64_t t, uint64_t u) const {
    return comparison::less_than(t, u);
}
};

struct LessThanEqualsOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t <= T(u);
}
template<typename T>
bool operator()([[maybe_unused]] std::optional<T> t, [[maybe_unused]] T u) const {
    util::raise_rte("Less than equals operator not supported with strings");
}
template<typename T>
bool operator()([[maybe_unused]] T t, [[maybe_unused]] std::optional<T> u) const {
    util::raise_rte("Less than equals operator not supported with strings");
}
bool operator()(uint64_t t, int64_t u) const {
    return comparison::less_than_equals(t, u);
}
bool operator()(int64_t t, uint64_t u) const {
    return comparison::less_than_equals(t, u);
}
};

struct GreaterThanOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t > T(u);
}
template<typename T>
bool operator()([[maybe_unused]] std::optional<T> t, [[maybe_unused]] T u) const {
    util::raise_rte("Greater than operator not supported with strings");
}
template<typename T>
bool operator()([[maybe_unused]] T t, [[maybe_unused]] std::optional<T> u) const {
    util::raise_rte("Greater than operator not supported with strings");
}
bool operator()(uint64_t t, int64_t u) const {
    return comparison::greater_than(t, u);
}
bool operator()(int64_t t, uint64_t u) const {
    return comparison::greater_than(t, u);
}
};

struct GreaterThanEqualsOperator {
template<typename T, typename U>
bool operator()(T t, U u) const {
    return t >= T(u);
}
template<typename T>
bool operator()([[maybe_unused]] std::optional<T> t, [[maybe_unused]] T u) const {
    util::raise_rte("Greater than equals operator not supported with strings");
}
template<typename T>
bool operator()([[maybe_unused]] T t, [[maybe_unused]] std::optional<T> u) const {
    util::raise_rte("Greater than equals operator not supported with strings");
}
bool operator()(uint64_t t, int64_t u) const {
    return comparison::greater_than_equals(t, u);
}
bool operator()(int64_t t, uint64_t u) const {
    return comparison::greater_than_equals(t, u);
}
};

struct MembershipOperator {
protected:
    template<typename U>
    static constexpr bool is_signed_int = std::is_integral_v<U> && std::is_signed_v<U>;

public:
    /** This is tighter than the signatures of the special handling operator()s below to reject argument types smaller
     * than uint64 going down the special handling via type promotion. */
    template<typename ColumnType, typename ValueSetBaseType>
    static constexpr bool needs_uint64_special_handling =
        (std::is_same_v<ColumnType, uint64_t> && is_signed_int<ValueSetBaseType>) ||
        (std::is_same_v<ValueSetBaseType, uint64_t> && is_signed_int<ColumnType>);
};

/** Used as a dummy parameter to ensure we don't pick the non-special handling overloads by mistake. */
struct UInt64SpecialHandlingTag {};

struct IsInOperator: MembershipOperator {
template<typename T, typename U>
bool operator()(T t, const std::unordered_set<U>& u) const {
    return u.count(t) > 0;
}

template<typename U, typename=std::enable_if_t<is_signed_int<U>>>
bool operator()(uint64_t t, const std::unordered_set<U>& u, UInt64SpecialHandlingTag = {}) const {
    if (t > static_cast<uint64_t>(std::numeric_limits<U>::max()))
        return false;
    else
        return u.count(t) > 0;
}
bool operator()(int64_t t, const std::unordered_set<uint64_t>& u, UInt64SpecialHandlingTag = {}) const {
    if (t < 0)
        return false;
    else
        return u.count(t) > 0;
}

#ifdef _WIN32
// MSVC has bugs with template expansion when they are using `using`-declaration,
// as used by `ankerl::unordered_dense`.
// Hence we explicitly define the concrete implementations here.
template<typename T>
bool operator()(T t, const ankerl::unordered_dense::set<uint64_t>& u) const {
    return u.contains(t);
}

template<typename T>
bool operator()(T t, const ankerl::unordered_dense::set<int64_t>& u) const {
    return u.contains(t);
}
#else
template<typename T, typename U>
bool operator()(T t, const ankerl::unordered_dense::set<U>& u) const {
    return u.contains(t);
}
#endif
};

struct IsNotInOperator: MembershipOperator {
template<typename T, typename U>
bool operator()(T t, const std::unordered_set<U>& u) const {
    return u.count(t) == 0;
}

template<typename U, typename = std::enable_if_t<is_signed_int<U>>>
bool operator()(uint64_t t, const std::unordered_set<U>& u, UInt64SpecialHandlingTag = {}) const {
    if (t > static_cast<uint64_t>(std::numeric_limits<U>::max()))
        return true;
    else
        return u.count(t) == 0;
}
bool operator()(int64_t t, const std::unordered_set<uint64_t>& u, UInt64SpecialHandlingTag = {}) const {
    if (t < 0)
        return true;
    else
        return u.count(t) == 0;
}

#ifdef _WIN32
// MSVC has bugs with template expansion when they are using `using`-declaration,
// as used by `ankerl::unordered_dense`.
// Hence we explicitly define the concrete implementations here.
template<typename T>
bool operator()(T t, const ankerl::unordered_dense::set<uint64_t>& u) const {
    return !u.contains(t);
}

template<typename T>
bool operator()(T t, const ankerl::unordered_dense::set<int64_t>& u) const {
    return !u.contains(t);
}
#else
template<typename T, typename U>
bool operator()(T t, const ankerl::unordered_dense::set<U>& u) const {
    return !u.contains(t);
}
#endif
};

} //namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::AbsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::AbsOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "ABS");
    }
};

template<>
struct formatter<arcticdb::NegOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NegOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "-");
    }
};

template<>
struct formatter<arcticdb::IsNullOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsNullOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "ISNULL");
    }
};

template<>
struct formatter<arcticdb::NotNullOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NotNullOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "NOTNULL");
    }
};

template<>
struct formatter<arcticdb::PlusOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::PlusOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "+");
    }
};

template<>
struct formatter<arcticdb::MinusOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::MinusOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "-");
    }
};

template<>
struct formatter<arcticdb::TimesOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::TimesOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "*");
    }
};

template<>
struct formatter<arcticdb::DivideOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::DivideOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "/");
    }
};

template<>
struct formatter<arcticdb::EqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::EqualsOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "==");
    }
};

template<>
struct formatter<arcticdb::NotEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NotEqualsOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "!=");
    }
};

template<>
struct formatter<arcticdb::LessThanOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::LessThanOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "<");
    }
};

template<>
struct formatter<arcticdb::LessThanEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::LessThanEqualsOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "<=");
    }
};

template<>
struct formatter<arcticdb::GreaterThanOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::GreaterThanOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), ">");
    }
};

template<>
struct formatter<arcticdb::GreaterThanEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::GreaterThanEqualsOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), ">=");
    }
};

template<>
struct formatter<arcticdb::IsInOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsInOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "IS IN");
    }
};

template<>
struct formatter<arcticdb::IsNotInOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsNotInOperator, FormatContext &ctx) const {
        return fmt::format_to(ctx.out(), "NOT IN");
    }
};

} // namespace fmt
