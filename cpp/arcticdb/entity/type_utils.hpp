/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <optional>
#include <fmt/format.h>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

/// Defines which static casts from int to float are permitted in is_valid_type_promotion_to_target
enum class IntToFloatConversion {
    /// Avoids lossy casting from int to float and tries to make sure that the integer can be represented exactly using
    /// the specified float, (u)int8, (u)int16 can be represented exactly using float 32, (u)int32 can be represented
    /// exactly via float64. Note this still allows casting (u)int64 to float64 even though it's a lossy cast.
    STRICT,
    /// Allow all type casts from int to float regardless of the byte size of the int and float type
    PERMISSIVE
};

/// Two types are trivially compatible if their byte representation is exactly the same i.e. you can memcpy
/// n elements of left type from one buffer to n elements of type right in another buffer and get the same result
[[nodiscard]] bool trivially_compatible_types(const entity::TypeDescriptor& left, const entity::TypeDescriptor& right);

[[nodiscard]] bool is_valid_type_promotion_to_target(
        const entity::TypeDescriptor& source, const entity::TypeDescriptor& target,
        const IntToFloatConversion int_to_to_float_conversion = IntToFloatConversion::STRICT
);

[[nodiscard]] std::optional<entity::TypeDescriptor> has_valid_common_type(
        const entity::TypeDescriptor& left, const entity::TypeDescriptor& right,
        IntToFloatConversion int_to_to_float_conversion = IntToFloatConversion::STRICT
);

[[nodiscard]] std::optional<entity::TypeDescriptor> promotable_type(
        const entity::TypeDescriptor& left, const entity::TypeDescriptor& right
);

inline std::string get_user_friendly_type_string(const entity::TypeDescriptor& type) {
    return is_sequence_type(type.data_type()) ? fmt::format("TD<type=STRING, dim={}>", type.dimension_)
                                              : fmt::format("{}", type);
}

} // namespace arcticdb

template<>
struct fmt::formatter<arcticdb::IntToFloatConversion> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::IntToFloatConversion conversion, FormatContext& ctx) const {
        switch (conversion) {
        case arcticdb::IntToFloatConversion::PERMISSIVE:
            return fmt::format_to(ctx.out(), "PERMISSIVE");
        case arcticdb::IntToFloatConversion::STRICT:
            return fmt::format_to(ctx.out(), "STRICT");
        default:
            arcticdb::util::raise_rte("Unrecognized int to float conversion type {}", int(conversion));
        }
    }
};