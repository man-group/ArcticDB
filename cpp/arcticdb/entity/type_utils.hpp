/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>

namespace arcticdb {

inline bool trivially_compatible_types(entity::TypeDescriptor left, entity::TypeDescriptor right) {
    if(left == right)
        return true;

    if(is_sequence_type(left.data_type()) && is_sequence_type(right.data_type())) {
        //TODO coercion of utf strings is not always safe, should allow safe conversion and reinstate the
        //stronger requirement for trivial conversion below.
//        if(!is_utf_type(slice_value_type(left.data_type)) && !is_utf_type(slice_value_type(right.data_type)))
//            return true;

        return is_utf_type(slice_value_type(left.data_type()))  == is_utf_type(slice_value_type(right.data_type()));
    }

    return false;
}

inline std::optional<entity::TypeDescriptor> has_valid_type_promotion(entity::TypeDescriptor source, entity::TypeDescriptor target) {
    if(source.dimension() != target.dimension())
        return std::nullopt;

    if (source == target)
        return target;

    // Empty type is coercible to any type
    if(is_empty_type(source.data_type())) {
        return target;
    }

    // Nothing is coercible to the empty type.
    if(is_empty_type(target.data_type())) {
        return std::nullopt;
    }

    auto source_type = source.data_type();
    auto target_type = target.data_type();
    auto source_size = slice_bit_size(source_type);
    auto target_size = slice_bit_size(target_type);

    if (is_time_type(source_type)) {
        if (!is_time_type(target_type))
            return std::nullopt;
    } else if (is_unsigned_type(source_type)) {
        if (is_unsigned_type(target_type)) {
            // UINT->UINT, target_size must be >= source_size
            if (source_size > target_size)
                return std::nullopt;
        } else if (is_signed_type(target_type)) {
            // UINT->INT, target_size must be > source_size
            if (source_size >= target_size)
                return std::nullopt;
        } else if (is_floating_point_type(target_type)) {
            // UINT->FLOAT, no restrictions on relative sizes
        } else {
            // Non-numeric target type
            return std::nullopt;
        }
    } else if (is_signed_type(source_type)) {
        if (is_unsigned_type(target_type)) {
            // INT->UINT never promotable
            return std::nullopt;
        } else if (is_signed_type(target_type)) {
            // INT->INT, target_size must be >= source_size
            if (source_size > target_size)
                return std::nullopt;
        } else if (is_floating_point_type(target_type)) {
            // INT->FLOAT, no restrictions on relative sizes
        } else {
            // Non-numeric target type
            return std::nullopt;
        }
    } else if (is_floating_point_type(source_type)) {
        if (is_unsigned_type(target_type) || is_signed_type(target_type)) {
            // FLOAT->U/INT never promotable
            return std::nullopt;
        } else if (is_floating_point_type(target_type)) {
            // FLOAT->FLOAT, target_size must be >= source_size
            if (source_size > target_size)
                return std::nullopt;
        } else {
            // Non-numeric target type
            return std::nullopt;
        }
    } else if (is_sequence_type(source_type) && is_sequence_type(target_type)) {
        // Only allow promotion with UTF strings, and only to dynamic (never to fixed width)
        if (!is_utf_type(source_type) || !is_utf_type(target_type) || !is_dynamic_string_type(target_type))
            return std::nullopt;
    } else {
        // Non-numeric source type
        return std::nullopt;
    }

    return entity::TypeDescriptor{combine_data_type(slice_value_type(target_type), target_size), target.dimension()};
}

inline std::optional<entity::TypeDescriptor> has_valid_type_promotion(
    const proto::descriptors::TypeDescriptor& source,
    const proto::descriptors::TypeDescriptor& target) {
    return has_valid_type_promotion(entity::type_desc_from_proto(source), entity::type_desc_from_proto(target));
}

inline std::optional<entity::TypeDescriptor> has_valid_common_type(entity::TypeDescriptor left, entity::TypeDescriptor right) {
    auto maybe_common_type = has_valid_type_promotion(left, right);
    if (!maybe_common_type) {
        maybe_common_type = has_valid_type_promotion(right, left);
    }
    return maybe_common_type;
}

inline std::optional<entity::TypeDescriptor> has_valid_common_type(
    const proto::descriptors::TypeDescriptor& left,
    const proto::descriptors::TypeDescriptor& right) {
    return has_valid_common_type(entity::type_desc_from_proto(left), entity::type_desc_from_proto(right));
}

}
