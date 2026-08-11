/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>

#include <span>
#include <string_view>

namespace arcticdb {

namespace pipelines {
struct InputFrame;
}

// How a column present in some schemas but missing from others is treated.
//   STRICT - every schema must carry the same non-index columns in the same order (static append/update).
//   DROP   - keep only the columns present in all of them (concat inner join).
//   KEEP   - keep the union of columns (dynamic append/update, concat outer join).
enum class MissingColumnPolicy { STRICT, DROP, KEEP };

// How the type of a column present in more than one schema is reconciled.
//   STATIC          - Static schema: promote empty->concrete and fixed->dynamic string
//   DYNAMIC         - Dynamic schema: promote via (has_valid_common_type)
//   MOST_PERMISSIVE - as DYNAMIC, but promotes to float64 when no exact common type exists
//                     for integral types. E.g. int32 + uint64 -> float64. Used for concat.
enum class TypePromotionPolicy { STATIC, DYNAMIC, MOST_PERMISSIVE };

// How a mismatch in the names of the required (index / Series) fields is treated.
//   RAISE                - the required field names must match (append/update).
//   RECONCILE_TO_UNNAMED - reconcile mismatched names to unnamed (concat).
enum class RequiredNameMismatchPolicy { RAISE, RECONCILE_TO_UNNAMED };

enum NormalizationOperation : uint8_t {
    APPEND,
    UPDATE,
    CONCAT,
};

// The operation named as the user would recognise it, so that a failed append does not report itself as a failed join.
constexpr std::string_view operation_name(NormalizationOperation operation) {
    switch (operation) {
    case APPEND:
        return "append";
    case UPDATE:
        return "update";
    case CONCAT:
        return "concat";
    }
    return "combine";
}

struct SchemaCombineOptions {
    MissingColumnPolicy missing_column;
    TypePromotionPolicy type_promotion;
    RequiredNameMismatchPolicy name_mismatch;
    // operation and stream_id only used for precise error messages
    NormalizationOperation operation;
    std::optional<StreamId> stream_id{};

    [[nodiscard]] std::string name() const {
        const auto operation_str = operation_name(operation);
        if (stream_id.has_value()) {
            return fmt::format("{} (symbol '{}')", operation_str, *stream_id);
        }
        return std::string{operation_str};
    }
};

inline SchemaCombineOptions append_or_update_options(
        bool dynamic_schema, NormalizationOperation operation, std::optional<StreamId> stream_id = std::nullopt
) {
    const auto missing_column = dynamic_schema ? MissingColumnPolicy::KEEP : MissingColumnPolicy::STRICT;
    const auto type_promotion = dynamic_schema ? TypePromotionPolicy::DYNAMIC : TypePromotionPolicy::STATIC;
    return {missing_column, type_promotion, RequiredNameMismatchPolicy::RAISE, operation, std::move(stream_id)};
}

// Only the symbol-less forms the tests need; every production caller knows its symbol and passes it.
inline SchemaCombineOptions append_options(bool dynamic_schema) {
    return append_or_update_options(dynamic_schema, APPEND);
}

inline SchemaCombineOptions update_options(bool dynamic_schema) {
    return append_or_update_options(dynamic_schema, UPDATE);
}

// Multi-symbol join utilities
enum class JoinType : uint8_t { OUTER, INNER };

inline SchemaCombineOptions concat_options(JoinType join_type) {
    const auto missing_column = join_type == JoinType::OUTER ? MissingColumnPolicy::KEEP : MissingColumnPolicy::DROP;
    return {missing_column,
            TypePromotionPolicy::MOST_PERMISSIVE,
            RequiredNameMismatchPolicy::RECONCILE_TO_UNNAMED,
            NormalizationOperation::CONCAT,
            std::nullopt};
}

// Combine schemas into one. Resolves differences according to SchemaCombineOptions. The first schema is the
// base: its column order leads the output, and for append/update it is the existing symbol's schema.
entity::OutputSchema combine_schema(std::span<const entity::OutputSchema> schemas, const SchemaCombineOptions& options);

// Raises unless two index types can combine.
void check_index_types_combinable(
        entity::IndexDescriptorImpl::Type accumulated, entity::IndexDescriptorImpl::Type other,
        const SchemaCombineOptions& options
);

SortedValue deduce_sorted(SortedValue existing_frame, SortedValue input_frame);

// Extracting a schema out of an existing tsd or an input frame.
entity::OutputSchema schema_from_tsd(const TimeseriesDescriptor& tsd);
entity::OutputSchema schema_from_input_frame(const pipelines::InputFrame& frame);

// Assemble the timeseries descriptor for an index key from a combined schema plus the row count
TimeseriesDescriptor tsd_from_schema(entity::OutputSchema&& schema, size_t total_rows, pipelines::InputFrame& frame);

} // namespace arcticdb
