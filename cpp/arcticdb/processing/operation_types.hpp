/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <unordered_set>
#include <optional>
#include <cmath>

#include <arcticdb/processing/signed_unsigned_comparison.hpp>
#include <arcticdb/util/constants.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/type_traits.hpp>
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
    REGEX_MATCH,
    // Boolean
    AND,
    OR,
    XOR,
    // Operator (kept here to preserve existing enum values)
    MOD,
    // Ternary
    TERNARY
};

inline std::string_view operation_type_to_str(const OperationType ot) {
    switch (ot) {
#define TO_STR(ARG)                                                                                                    \
    case OperationType::ARG:                                                                                           \
        return std::string_view(#ARG);
        TO_STR(ABS)
        TO_STR(NEG)
        TO_STR(ISNULL)
        TO_STR(NOTNULL)
        TO_STR(IDENTITY)
        TO_STR(NOT)
        TO_STR(ADD)
        TO_STR(SUB)
        TO_STR(MUL)
        TO_STR(DIV)
        TO_STR(MOD)
        TO_STR(EQ)
        TO_STR(NE)
        TO_STR(LT)
        TO_STR(LE)
        TO_STR(GT)
        TO_STR(GE)
        TO_STR(ISIN)
        TO_STR(ISNOTIN)
        TO_STR(REGEX_MATCH)
        TO_STR(AND)
        TO_STR(OR)
        TO_STR(XOR)
        TO_STR(TERNARY)
#undef TO_STR
    default:
        return std::string_view("UNKNOWN");
    }
}

constexpr bool is_unary_operation(OperationType o) { return uint8_t(o) <= uint8_t(OperationType::NOT); }

constexpr bool is_binary_operation(OperationType o) {
    return uint8_t(o) >= uint8_t(OperationType::ADD) && uint8_t(o) < uint8_t(OperationType::TERNARY);
}

constexpr bool is_ternary_operation(OperationType o) { return uint8_t(o) >= uint8_t(OperationType::TERNARY); }

struct AbsOperator;
struct NegOperator;
struct PlusOperator;
struct MinusOperator;
struct TimesOperator;
struct DivideOperator;
struct MembershipOperator;

namespace arithmetic_promoted_type::details {
template<class VAL>
struct width {
    static constexpr size_t value = sizeof(VAL);
};
template<class LHS, class RHS>
struct max_width {
    static constexpr size_t value = std::max(sizeof(LHS), sizeof(RHS));
};
// Has member type naming an unsigned integer of WIDTH 1, 2, 4, or 8 bytes (the default)
template<size_t WIDTH>
struct unsigned_width {
    using type = typename std::conditional_t<
            WIDTH == 1, uint8_t,
            std::conditional_t<WIDTH == 2, uint16_t, std::conditional_t<WIDTH == 4, uint32_t, uint64_t>>>;
};
// Has member type naming a signed integer of WIDTH 1, 2, 4, or 8 bytes (the default)
template<size_t WIDTH>
struct signed_width {
    using type = typename std::conditional_t<
            WIDTH == 1, int8_t,
            std::conditional_t<WIDTH == 2, int16_t, std::conditional_t<WIDTH == 4, int32_t, int64_t>>>;
};

template<class VAL>
inline constexpr size_t width_v = width<VAL>::value;
template<class LHS, class RHS>
inline constexpr size_t max_width_v = max_width<LHS, RHS>::value;
template<size_t WIDTH>
using unsigned_width_t = typename unsigned_width<WIDTH>::type;
template<size_t WIDTH>
using signed_width_t = typename signed_width<WIDTH>::type;
} // namespace arithmetic_promoted_type::details

template<class VAL, class Func>
struct unary_operation_promoted_type {
    static constexpr size_t val_width = arithmetic_promoted_type::details::width_v<VAL>;
    using type = typename
            /* Unsigned ints promote to themselves for the abs operator, and to a signed int of double the width with
             * the neg operator Floating point types promote to themselves with both operators Signed ints promote to a
             * signed int of double the width for both operators, as their range is not symmetric about zero */
            std::conditional_t<
                    std::is_floating_point_v<VAL> || (std::is_same_v<Func, AbsOperator> && std::is_unsigned_v<VAL>),
                    VAL, typename arithmetic_promoted_type::details::signed_width_t<2 * val_width>>;
};

template<class LHS, class RHS, class Func>
struct binary_operation_promoted_type {
    static constexpr size_t max_width = arithmetic_promoted_type::details::max_width_v<LHS, RHS>;
    using type = typename std::conditional_t<
            std::is_same_v<Func, DivideOperator>,
            // Always use doubles for division operations
            double,
            std::conditional_t<
                    std::is_floating_point_v<LHS> || std::is_floating_point_v<RHS>,
                    // At least one of the types is floating point
                    std::conditional_t<
                            std::is_floating_point_v<LHS> && std::is_floating_point_v<RHS>,
                            // If both types are floating point, promote to the type of the widest one
                            std::conditional_t<max_width == 8, double, float>,
                            // Otherwise, if only one type is floating point, always promote to double
                            // For example when combining int32 and float32 the result can only fit in float64 without
                            // loss of precision Special cases like int16 and float32 can fit in float32, but we always
                            // promote up to float64 (as does Pandas)
                            double>,
                    // Otherwise, both types are integers
                    std::conditional_t<
                            std::is_unsigned_v<LHS> && std::is_unsigned_v<RHS>,
                            // Both types are unsigned
                            std::conditional_t<
                                    std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, TimesOperator>,
                                    /* Plus and Times operators can overflow if using max_width, so promote to a wider
                                     * unsigned type e.g. 255*255 (both uint8_t's) = 65025, requiring uint16_t to hold
                                     * the result */
                                    arithmetic_promoted_type::details::unsigned_width_t<2 * max_width>,
                                    std::conditional_t<
                                            std::is_same_v<Func, MinusOperator>,
                                            /* The result of Minus with two unsigned types can be negative
                                             * Can also underflow if using max_width, so promote to a wider signed type
                                             * e.g. 0 - 255 (both uint8_t's) = -255, requiring int16_t to hold the
                                             * result */
                                            arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                                            // IsIn/IsNotIn operators, just use the type of the widest input
                                            arithmetic_promoted_type::details::unsigned_width_t<max_width>>>,
                            std::conditional_t<
                                    std::is_signed_v<LHS> && std::is_signed_v<RHS>,
                                    // Both types are signed integers (as we are in the "else" of the floating point
                                    // checks)
                                    std::conditional_t<
                                            std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, MinusOperator> ||
                                                    std::is_same_v<Func, TimesOperator>,
                                            /* Plus, Minus, and Times operators can overflow if using max_width, so
                                             * promote to a wider signed type e.g. -100*100 (both int8_t's) = -10000,
                                             * requiring int16_t to hold the result */
                                            arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                                            // IsIn/IsNotIn operators, just use the type of the widest input
                                            arithmetic_promoted_type::details::signed_width_t<max_width>>,
                                    // We have one signed and one unsigned type
                                    std::conditional_t<
                                            std::is_same_v<Func, PlusOperator> || std::is_same_v<Func, MinusOperator> ||
                                                    std::is_same_v<Func, TimesOperator>,
                                            // Plus, Minus, and Times operators can overflow if using max_width, so
                                            // promote to a wider signed type
                                            arithmetic_promoted_type::details::signed_width_t<2 * max_width>,
                                            // IsIn/IsNotIn Operator
                                            std::conditional_t<
                                                    (std::is_signed_v<LHS> && sizeof(LHS) > sizeof(RHS)) ||
                                                            (std::is_signed_v<RHS> && sizeof(RHS) > sizeof(LHS)),
                                                    // If the signed type is strictly larger than the unsigned type,
                                                    // then promote to the signed type
                                                    arithmetic_promoted_type::details::signed_width_t<max_width>,
                                                    // Otherwise, check if the unsigned one is the widest type we
                                                    // support
                                                    std::conditional_t<
                                                            std::is_same_v<LHS, uint64_t> ||
                                                                    std::is_same_v<RHS, uint64_t>,
                                                            // Retains ValueSetBaseType in binary_membership(), which
                                                            // handles mixed int64/uint64 operations gracefully
                                                            RHS,
                                                            // There should be a signed type wider than the unsigned
                                                            // type, so both can be exactly represented
                                                            arithmetic_promoted_type::details::signed_width_t<
                                                                    2 * max_width>>>>>>>>;
};

template<class LHS, class RHS>
struct ternary_operation_promoted_type {
    static constexpr size_t max_width = arithmetic_promoted_type::details::max_width_v<LHS, RHS>;
    using type = typename std::conditional_t<
            std::is_same_v<LHS, bool> && std::is_same_v<RHS, bool>,
            // Both types are bool, return bool
            bool,
            std::conditional_t<
                    std::is_floating_point_v<LHS> || std::is_floating_point_v<RHS>,
                    // At least one of the types is floating point
                    std::conditional_t<
                            std::is_floating_point_v<LHS> && std::is_floating_point_v<RHS>,
                            // If both types are floating point, promote to the type of the widest one
                            std::conditional_t<max_width == 8, double, float>,
                            // Otherwise, if only one type is floating point, promote to double to avoid data loss when
                            // the integer cannot be represented by float32
                            double>,
                    // Otherwise, both types are integers
                    std::conditional_t<
                            std::is_unsigned_v<LHS> && std::is_unsigned_v<RHS>,
                            // Both types are unsigned, promote to the type of the widest one
                            typename arithmetic_promoted_type::details::unsigned_width_t<max_width>,
                            std::conditional_t<
                                    std::is_signed_v<LHS> && std::is_signed_v<RHS>,
                                    // Both types are signed integers (as we are in the "else" of the floating point
                                    // checks), promote to the type of the widest one
                                    typename arithmetic_promoted_type::details::signed_width_t<max_width>,
                                    // We have one signed and one unsigned type
                                    std::conditional_t<
                                            (std::is_signed_v<LHS> && sizeof(LHS) > sizeof(RHS)) ||
                                                    (std::is_signed_v<RHS> && sizeof(RHS) > sizeof(LHS)),
                                            // If the signed type is strictly larger than the unsigned type, then
                                            // promote to the signed type
                                            typename arithmetic_promoted_type::details::signed_width_t<max_width>,
                                            // Otherwise, check if the unsigned one is the widest type we support
                                            std::conditional_t<
                                                    std::is_same_v<LHS, uint64_t> || std::is_same_v<RHS, uint64_t>,
                                                    // Unsigned type is as wide as we go, so no integer type can exactly
                                                    // represent both input types So promote to float64
                                                    double,
                                                    // There should be a signed type wider than the unsigned type, so
                                                    // both can be exactly represented
                                                    typename arithmetic_promoted_type::details::signed_width_t<
                                                            2 * max_width>>>>>>>;
};

struct AbsOperator {
    template<typename T, typename V = typename unary_operation_promoted_type<T, AbsOperator>::type>
    V apply(T t) {
        if constexpr (std::is_unsigned_v<T>)
            return t;
        else
            return std::abs(static_cast<V>(t));
    }
};

struct NegOperator {
    template<typename T, typename V = typename unary_operation_promoted_type<T, NegOperator>::type>
    V apply(T t) {
        return -static_cast<V>(t);
    }
};

// Needed for null and not null operators as INT64, NANOSECONDS_UTC64, and all string columns hold int64_t values
struct TimeTypeTag {};
struct StringTypeTag {};

struct IsNullOperator {
    template<typename tag>
    requires util::any_of<tag, TimeTypeTag, StringTypeTag>
    bool apply(int64_t t) {
        if constexpr (std::is_same_v<tag, TimeTypeTag>) {
            return t == NaT;
        } else if constexpr (std::is_same_v<tag, StringTypeTag>) {
            // Relies on string_nan == string_none - 1
            return t >= string_nan;
        }
    }
    bool apply(std::floating_point auto t) { return std::isnan(t); }
};

struct NotNullOperator {
    template<typename tag>
    requires util::any_of<tag, TimeTypeTag, StringTypeTag>
    bool apply(int64_t t) {
        if constexpr (std::is_same_v<tag, TimeTypeTag>) {
            return t != NaT;
        } else if constexpr (std::is_same_v<tag, StringTypeTag>) {
            // Relies on string_nan == string_none - 1
            return t < string_nan;
        }
    }
    template<std::floating_point T>
    bool apply(T t) {
        return !std::isnan(t);
    }
};

struct PlusOperator {
    template<typename T, typename U, typename V = typename binary_operation_promoted_type<T, U, PlusOperator>::type>
    V apply(T t, U u) {
        return static_cast<V>(t) + static_cast<V>(u);
    }
};

struct MinusOperator {
    template<typename T, typename U, typename V = typename binary_operation_promoted_type<T, U, MinusOperator>::type>
    V apply(T t, U u) {
        return static_cast<V>(t) - static_cast<V>(u);
    }
};

struct TimesOperator {
    template<typename T, typename U, typename V = typename binary_operation_promoted_type<T, U, TimesOperator>::type>
    V apply(T t, U u) {
        return static_cast<V>(t) * static_cast<V>(u);
    }
};

struct DivideOperator {
    template<typename T, typename U, typename V = typename binary_operation_promoted_type<T, U, DivideOperator>::type>
    V apply(T t, U u) {
        return static_cast<V>(t) / static_cast<V>(u);
    }
};

struct ModOperator {
    template<typename T, typename U, typename V = typename binary_operation_promoted_type<T, U, ModOperator>::type>
    V apply(T t, U u) {
        if constexpr (std::is_floating_point_v<V>) {
            return std::fmod(static_cast<V>(t), static_cast<V>(u));
        } else {
            return static_cast<V>(t) % static_cast<V>(u);
        }
    }
};

struct EqualsOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t == u;
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
    bool operator()(uint64_t t, int64_t u) const { return comparison::equals(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::equals(t, u); }
};

struct NotEqualsOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t != u;
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
    bool operator()(uint64_t t, int64_t u) const { return comparison::not_equals(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::not_equals(t, u); }
};

struct LessThanOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t < u;
    }
    template<typename T>
    bool operator()(std::optional<T>, T) const {
        util::raise_rte("Less than operator not supported with strings");
    }
    template<typename T>
    bool operator()(T, std::optional<T>) const {
        util::raise_rte("Less than operator not supported with strings");
    }
    bool operator()(uint64_t t, int64_t u) const { return comparison::less_than(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::less_than(t, u); }
};

struct LessThanEqualsOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t <= u;
    }
    template<typename T>
    bool operator()(std::optional<T>, T) const {
        util::raise_rte("Less than equals operator not supported with strings");
    }
    template<typename T>
    bool operator()(T, std::optional<T>) const {
        util::raise_rte("Less than equals operator not supported with strings");
    }
    bool operator()(uint64_t t, int64_t u) const { return comparison::less_than_equals(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::less_than_equals(t, u); }
};

struct GreaterThanOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t > u;
    }
    template<typename T>
    bool operator()(std::optional<T>, T) const {
        util::raise_rte("Greater than operator not supported with strings");
    }
    template<typename T>
    bool operator()(T, std::optional<T>) const {
        util::raise_rte("Greater than operator not supported with strings");
    }
    bool operator()(uint64_t t, int64_t u) const { return comparison::greater_than(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::greater_than(t, u); }
};

struct GreaterThanEqualsOperator {
    template<typename T, typename U>
    bool operator()(T t, U u) const {
        return t >= u;
    }
    template<typename T>
    bool operator()(std::optional<T>, T) const {
        util::raise_rte("Greater than equals operator not supported with strings");
    }
    template<typename T>
    bool operator()(T, std::optional<T>) const {
        util::raise_rte("Greater than equals operator not supported with strings");
    }
    bool operator()(uint64_t t, int64_t u) const { return comparison::greater_than_equals(t, u); }
    bool operator()(int64_t t, uint64_t u) const { return comparison::greater_than_equals(t, u); }
};

struct RegexMatchOperator {
    template<typename T, typename U>
    bool operator()(T, U) const {
        util::raise_rte("RegexMatchOperator does not support {} and {}", typeid(T).name(), typeid(U).name());
    }
    bool operator()(entity::position_t offset, const ankerl::unordered_dense::set<position_t>& offset_set) const {
        return offset_set.contains(offset);
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

struct IsInOperator : MembershipOperator {
    template<typename T, typename U>
    bool operator()(T t, const std::unordered_set<U>& u) const {
        return u.contains(t);
    }

    template<typename U>
    requires is_signed_int<U>
    bool operator()(uint64_t t, const std::unordered_set<U>& u, UInt64SpecialHandlingTag = {}) const {
        if (t > static_cast<uint64_t>(std::numeric_limits<U>::max()))
            return false;
        else
            return u.contains(t);
    }
    bool operator()(int64_t t, const std::unordered_set<uint64_t>& u, UInt64SpecialHandlingTag = {}) const {
        if (t < 0)
            return false;
        else
            return u.contains(t);
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

struct IsNotInOperator : MembershipOperator {
    template<typename T, typename U>
    bool operator()(T t, const std::unordered_set<U>& u) const {
        return !u.contains(t);
    }

    template<typename U>
    requires is_signed_int<U>
    bool operator()(uint64_t t, const std::unordered_set<U>& u, UInt64SpecialHandlingTag = {}) const {
        if (t > static_cast<uint64_t>(std::numeric_limits<U>::max()))
            return true;
        else
            return !u.contains(t);
    }
    bool operator()(int64_t t, const std::unordered_set<uint64_t>& u, UInt64SpecialHandlingTag = {}) const {
        if (t < 0)
            return true;
        else
            return !u.contains(t);
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

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::OperationType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::OperationType ot, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", operation_type_to_str(ot));
    }
};

template<>
struct formatter<arcticdb::AbsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::AbsOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "ABS");
    }
};

template<>
struct formatter<arcticdb::NegOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NegOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "-");
    }
};

template<>
struct formatter<arcticdb::IsNullOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsNullOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "ISNULL");
    }
};

template<>
struct formatter<arcticdb::NotNullOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NotNullOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "NOTNULL");
    }
};

template<>
struct formatter<arcticdb::PlusOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::PlusOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "+");
    }
};

template<>
struct formatter<arcticdb::MinusOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::MinusOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "-");
    }
};

template<>
struct formatter<arcticdb::TimesOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::TimesOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "*");
    }
};

template<>
struct formatter<arcticdb::DivideOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::DivideOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "/");
    }
};

template<>
struct formatter<arcticdb::ModOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::ModOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "%");
    }
};

template<>
struct formatter<arcticdb::EqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::EqualsOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "==");
    }
};

template<>
struct formatter<arcticdb::NotEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::NotEqualsOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "!=");
    }
};

template<>
struct formatter<arcticdb::LessThanOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::LessThanOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "<");
    }
};

template<>
struct formatter<arcticdb::LessThanEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::LessThanEqualsOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "<=");
    }
};

template<>
struct formatter<arcticdb::GreaterThanOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::GreaterThanOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), ">");
    }
};

template<>
struct formatter<arcticdb::GreaterThanEqualsOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::GreaterThanEqualsOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), ">=");
    }
};

template<>
struct formatter<arcticdb::IsInOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsInOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "IS IN");
    }
};

template<>
struct formatter<arcticdb::IsNotInOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::IsNotInOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "NOT IN");
    }
};

template<>
struct formatter<arcticdb::RegexMatchOperator> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    constexpr auto format(arcticdb::RegexMatchOperator, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "REGEX MATCH");
    }
};

} // namespace fmt
