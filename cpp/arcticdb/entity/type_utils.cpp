/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/types_proto.hpp>

namespace arcticdb {
    bool trivially_compatible_types(const entity::TypeDescriptor& left, const entity::TypeDescriptor& right) {
        if (left == right)
            return true;

        // Multidimensional types are pointers
        if (left.dimension() >= entity::Dimension::Dim1 && right.dimension() >= entity::Dimension::Dim1)
            return true;

        // Multidimensional types are pointers the empty type is pointer as well
        if (left.dimension() >= entity::Dimension::Dim1 && is_empty_type(right.data_type()))
            return true;

        // Multidimensional types are pointers the empty type is pointer as well
        if (right.dimension() >= entity::Dimension::Dim1 && is_empty_type(left.data_type()))
            return true;

        if (is_sequence_type(left.data_type()) && is_sequence_type(right.data_type())) {
            // TODO coercion of utf strings is not always safe, should allow safe conversion and reinstate the
            // stronger requirement for trivial conversion below.
            //        if(!is_utf_type(slice_value_type(left.data_type)) &&
            //        !is_utf_type(slice_value_type(right.data_type)))
            //            return true;

            return is_utf_type(slice_value_type(left.data_type())) == is_utf_type(slice_value_type(right.data_type()));
        }

        return false;
    }

    std::optional<entity::TypeDescriptor> has_valid_type_promotion(
        const entity::TypeDescriptor& source,
        const entity::TypeDescriptor& target
    ) {

        if (source.dimension() != target.dimension()) {
            // Empty of dimension 0 means lack of any given type and can be promoted to anything (even if the dimensions
            // don't match), e.g. empty type can become int or array of ints. Empty type of higher dimension is used to
            // specify an empty array or an empty matrix, thus it cannot become any other type unless the dimensionality
            // matches
            if (is_empty_type(source.data_type()) && source.dimension() == entity::Dimension::Dim0)
                return target;
            return std::nullopt;
        }

        if (source == target)
            return target;

        // Empty type is coercible to any type
        if (is_empty_type(source.data_type())) {
            return target;
        }

        // Nothing is coercible to the empty type.
        if (is_empty_type(target.data_type())) {
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
        } else if (is_bool_object_type(source_type)) {
            return std::nullopt;
        } else {
            // Non-numeric source type
            return std::nullopt;
        }

        return entity::TypeDescriptor{
            combine_data_type(slice_value_type(target_type), target_size),
            target.dimension()};
    }

    std::optional<entity::TypeDescriptor> has_valid_type_promotion(
        const proto::descriptors::TypeDescriptor& source,
        const proto::descriptors::TypeDescriptor& target
    ) {
        return has_valid_type_promotion(entity::type_desc_from_proto(source), entity::type_desc_from_proto(target));
    }

    std::optional<entity::TypeDescriptor> has_valid_common_type(
        const entity::TypeDescriptor& left,
        const entity::TypeDescriptor& right
    ) {
        auto maybe_common_type = has_valid_type_promotion(left, right);
        if (!maybe_common_type) {
            maybe_common_type = has_valid_type_promotion(right, left);
        }
        return maybe_common_type;
    }

    std::optional<entity::TypeDescriptor> has_valid_common_type(
        const proto::descriptors::TypeDescriptor& left,
        const proto::descriptors::TypeDescriptor& right
    ) {
        return has_valid_common_type(entity::type_desc_from_proto(left), entity::type_desc_from_proto(right));
    }

}