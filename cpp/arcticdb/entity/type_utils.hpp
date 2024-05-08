/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include<optional>
#include <fmt/format.h>
#include<arcticdb/entity/descriptors.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {

namespace entity {
    struct TypeDescriptor;
}

/// Two types are trivially compatible if their byte representation is exactly the same i.e. you can memcpy
/// n elements of left type from one buffer to n elements of type right in another buffer and get the same result
[[nodiscard]] bool trivially_compatible_types(const entity::TypeDescriptor& left, const entity::TypeDescriptor& right);

[[nodiscard]] std::optional<entity::TypeDescriptor> has_valid_type_promotion(
    const entity::TypeDescriptor& source,
    const entity::TypeDescriptor& target
);

[[nodiscard]] std::optional<entity::TypeDescriptor> has_valid_type_promotion(
    const proto::descriptors::TypeDescriptor& source,
    const proto::descriptors::TypeDescriptor& target
);

[[nodiscard]] std::optional<entity::TypeDescriptor> has_valid_common_type(
    const entity::TypeDescriptor& left,
    const entity::TypeDescriptor& right
);

[[nodiscard]] std::optional<entity::TypeDescriptor> has_valid_common_type(
    const proto::descriptors::TypeDescriptor& left,
    const proto::descriptors::TypeDescriptor& right
);

inline std::string get_user_friendly_type_string(const entity::TypeDescriptor& type) {
    return is_sequence_type(type.data_type()) ? "TD<type=STRING, dim=0>" : fmt::format("{}", type);
}

}
