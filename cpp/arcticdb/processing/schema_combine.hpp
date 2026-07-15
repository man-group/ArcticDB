/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/stream_descriptor.hpp>

namespace arcticdb {

// How a column present in one schema but missing from the other is treated.
//   Raise - the two schemas must carry the same non-index columns (static append/update).
//   Drop  - keep only the columns present in both (concat inner join).
//   Keep  - keep the union of columns (dynamic append/update, concat outer join).
enum class MissingColumnPolicy { Raise, Drop, Keep };

// How the type of a column present in both schemas is reconciled.
//   Static  - promote empty->concrete and fixed->dynamic string, otherwise the types must be
//             trivially compatible or the combine raises.
//   Dynamic - promote to any common type via has_valid_common_type, raising only if none exists.
enum class TypePromotionPolicy { Static, Dynamic };

// How a mismatch in the names of the required (index / Series value) fields is treated.
//   Raise           - the required field names must match (append/update).
//   ReconcileToFake - reconcile mismatched names to the unnamed-multiindex naming scheme and record
//                     the position in the output multi-index fake_field_pos (concat).
enum class RequiredNameMismatchPolicy { Raise, ReconcileToFake };

struct SchemaCombineOptions {
    MissingColumnPolicy missing_column;
    TypePromotionPolicy type_promotion;
    RequiredNameMismatchPolicy name_mismatch;
};

inline SchemaCombineOptions append_static_schema_options() {
    return {MissingColumnPolicy::Raise, TypePromotionPolicy::Static, RequiredNameMismatchPolicy::Raise};
}

inline SchemaCombineOptions append_dynamic_schema_options() {
    return {MissingColumnPolicy::Keep, TypePromotionPolicy::Dynamic, RequiredNameMismatchPolicy::Raise};
}

inline SchemaCombineOptions concat_outer_options() {
    return {MissingColumnPolicy::Keep, TypePromotionPolicy::Dynamic, RequiredNameMismatchPolicy::ReconcileToFake};
}

inline SchemaCombineOptions concat_inner_options() {
    return {MissingColumnPolicy::Drop, TypePromotionPolicy::Dynamic, RequiredNameMismatchPolicy::ReconcileToFake};
}

// Combine two schemas into one, applying the checks and merges that append/update/concat previously
// scattered across schema_checks, normalization_utils, index_utils and clause_utils. Raises on
// incompatibility. Concat folds over its inputs: res = combine_schema(res, next, concat_*_options()).
// Row counts, offsets and operational guards (pickled, sortedness, timeseries-only for update, index
// contiguity) live outside this function, in the append/update/concat wrappers.
entity::OutputSchema combine_schema(
        const entity::OutputSchema& base, const entity::OutputSchema& other, const SchemaCombineOptions& options
);

} // namespace arcticdb
