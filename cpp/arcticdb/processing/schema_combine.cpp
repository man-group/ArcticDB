/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/schema_combine.hpp>

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/types_proto.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/pipeline/frame_utils.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/python/normalization_utils.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/util/collection_utils.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/variant.hpp>

#include <iterator>
#include <optional>
#include <ranges>
#include <set>
#include <span>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace arcticdb {

using namespace proto::descriptors;
using entity::DataType;
using entity::Field;
using entity::IndexDescriptorImpl;
using entity::OutputSchema;
using entity::StreamDescriptor;
using entity::TypeDescriptor;
using ArrowColumnMeta = NormalizationMetadata_ExperimentalArrow_ColumnMeta;
using Pandas = NormalizationMetadata_Pandas;
using PandasIndex = NormalizationMetadata_PandasIndex;
using PandasMultiIndex = NormalizationMetadata_PandasMultiIndex;

std::string SchemaCombineOptions::name() const {
    const auto operation_str = operation_name(operation);
    if (stream_id.has_value()) {
        return fmt::format("{} (symbol '{}')", operation_str, *stream_id);
    }
    return std::string{operation_str};
}

SchemaCombineOptions append_or_update_options(
        bool dynamic_schema, NormalizationOperation operation, std::optional<StreamId> stream_id
) {
    const auto missing_column = dynamic_schema ? MissingColumnPolicy::KEEP : MissingColumnPolicy::STRICT;
    const auto type_promotion = dynamic_schema ? TypePromotionPolicy::DYNAMIC : TypePromotionPolicy::STATIC;
    return {missing_column, type_promotion, RequiredNameMismatchPolicy::RAISE, operation, std::move(stream_id)};
}

SchemaCombineOptions append_options(bool dynamic_schema) { return append_or_update_options(dynamic_schema, APPEND); }

SchemaCombineOptions update_options(bool dynamic_schema) { return append_or_update_options(dynamic_schema, UPDATE); }

SchemaCombineOptions concat_options(JoinType join_type) {
    const auto missing_column = join_type == JoinType::OUTER ? MissingColumnPolicy::KEEP : MissingColumnPolicy::DROP;
    return {missing_column,
            TypePromotionPolicy::MOST_PERMISSIVE,
            RequiredNameMismatchPolicy::RECONCILE_TO_UNNAMED,
            NormalizationOperation::CONCAT,
            std::nullopt};
}

namespace {
// Whether a schema carries the empty index a zero-row write produces with empty_types enabled. Taken from the
// descriptor rather than the normalization metadata, which cannot distinguish it from a pre-Nov-2020 RangeIndex - both
// have no step.
bool has_empty_index(const OutputSchema& schema) {
    return schema.stream_descriptor().index().type() == IndexDescriptorImpl::Type::EMPTY;
}

using pipelines::index::required_fields_info;
using pipelines::index::RequiredFieldInfo;

// A user facing reason something could not be combined, so that a signature says which of its strings is one.
using Error = std::string;

// The two sides of a name disagreement, quoted so that an unnamed field is visible as ''.
Error names_differ(std::string_view accumulated, std::string_view other) {
    return fmt::format("'{}' against '{}'", accumulated, other);
}

// What the normalization metadata records about the name of an index, a multi-index level or a Series value column.
// Built by the named constructors below rather than read off the metadata directly, because each of the three spells
// "this has no name" differently.
struct PandasName {
    bool has_name{false};
    bool is_int{false};
    std::string_view name{};

    // A faked name is a placeholder for "this index has no name".
    static PandasName of_index(const PandasIndex& index) {
        return {.has_name = !index.fake_name(), .is_int = index.is_int(), .name = index.name()};
    }

    // A multi-index records its unnamed levels in fake_field_pos rather than with a fake_name flag. Only level 0 has
    // a name and an is_int flag of its own; the rest are named by their descriptor field alone.
    static PandasName of_multi_index_level_0(const PandasMultiIndex& multi_index) {
        const auto& unnamed = multi_index.fake_field_pos();
        return {.has_name = std::ranges::find(unnamed, 0u) == unnamed.end(),
                .is_int = multi_index.is_int(),
                .name = multi_index.name()};
    }

    // A Series name is never an integer. A client older than the has_name field wrote the name without setting it and
    // denormalization still honours such a name, so a non-empty name counts as one whether the flag was set or not.
    static PandasName of_series(const Pandas& common) {
        return {.has_name = common.has_name() || !common.name().empty(), .is_int = false, .name = common.name()};
    }
};

// Compares pandas names (index, multindex or series) and returns a user friendly error message if they don't match
std::optional<Error> compare_pandas_names(const PandasName& left, const PandasName& right) {
    if (left.has_name != right.has_name) {
        return fmt::format("'{}' is named on one side only", left.has_name ? left.name : right.name);
    }
    // Neither side has a name, so both store a placeholder.
    if (!left.has_name) {
        return std::nullopt;
    }
    if (left.name != right.name) {
        return names_differ(left.name, right.name);
    }
    if (left.is_int != right.is_int) {
        return fmt::format("'{}' is an integer on one side only", left.name);
    }
    return std::nullopt;
}

// What disagreed about the names of the required fields, accumulated across every schema being combined.
class RequiredNameMismatches {
  public:
    explicit RequiredNameMismatches(const SchemaCombineOptions& options) : options_(options) {}

    // A name disagreement is a descriptor mismatch wherever it occurs - index level, Series value column or data
    // column. Only a disagreement about the *shape* of the required fields (their count, or Series versus DataFrame)
    // is an index incompatibility. The multi_index flag only picks the wording.
    void add_index(size_t position, bool multi_index, std::string_view detail) {
        if (raises()) {
            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    "Cannot {}: {} names must match, {}",
                    options_.name(),
                    multi_index ? "multi-index level" : "index",
                    detail
            );
        }
        index_positions_.emplace(position);
    }

    void add_series_name(std::string_view detail) {
        if (raises()) {
            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    "Cannot {}: Series names must match, {}", options_.name(), detail
            );
        }
        series_name_ = true;
    }

    [[nodiscard]] bool index_at(size_t position) const { return index_positions_.contains(position); }

    [[nodiscard]] const std::unordered_set<size_t>& index_positions() const { return index_positions_; }

    [[nodiscard]] bool series_name() const { return series_name_; }

    [[nodiscard]] bool any() const { return series_name_ || !index_positions_.empty(); }

  private:
    [[nodiscard]] bool raises() const { return options_.name_mismatch == RequiredNameMismatchPolicy::RAISE; }

    const SchemaCombineOptions& options_;
    std::unordered_set<size_t> index_positions_{};
    bool series_name_{false};
};

// ---------------------------------------------------------------------------------------------------------------------
// Field types
// ---------------------------------------------------------------------------------------------------------------------

// Reconcile the type of a field according to the type promotion policy. Nullopt if the two have no common type.
std::optional<TypeDescriptor> promote_field_type(
        const TypeDescriptor& base, const TypeDescriptor& other, TypePromotionPolicy policy
) {
    if (base == other) {
        return base;
    }
    switch (policy) {
    case TypePromotionPolicy::DYNAMIC:
        return has_valid_common_type(base, other);
    case TypePromotionPolicy::MOST_PERMISSIVE:
        return promotable_type(base, other);
    case TypePromotionPolicy::STATIC:
        // Only empty->concrete and fixed->dynamic string promotions are allowed; anything else has to be
        // byte-compatible already.
        if (is_empty_type(base.data_type())) {
            return other;
        }
        if (is_empty_type(other.data_type())) {
            return base;
        }
        if (is_sequence_type(base.data_type()) && is_sequence_type(other.data_type()) &&
            is_dynamic_string_type(base.data_type()) != is_dynamic_string_type(other.data_type())) {
            return is_dynamic_string_type(base.data_type()) ? base : other;
        }
        return trivially_compatible_types(base, other) ? std::optional{base} : std::nullopt;
    }
    return std::nullopt;
}

// The combined type of a field, or why the two could not be combined. Carrying the reason lets a caller that only
// finds out later whether the column matters report the types that clashed rather than just the column name.
using CombinedFieldType = std::variant<TypeDescriptor, Error>;

CombinedFieldType try_combine_field_type(
        const TypeDescriptor& base, const TypeDescriptor& other, const SchemaCombineOptions& options,
        std::string_view name
) {
    if (const auto promoted = promote_field_type(base, other, options.type_promotion)) {
        return *promoted;
    }
    return fmt::format("Cannot {}: no common type for column '{}', {} and {}", options.name(), name, base, other);
}

TypeDescriptor combine_field_type(
        const TypeDescriptor& base, const TypeDescriptor& other, const SchemaCombineOptions& options,
        std::string_view name
) {
    return util::variant_match(
            try_combine_field_type(base, other, options, name),
            [](const TypeDescriptor& combined) { return combined; },
            [](const Error& error) -> TypeDescriptor { schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(error); }
    );
}

// ---------------------------------------------------------------------------------------------------------------------
// Stream descriptor
// ---------------------------------------------------------------------------------------------------------------------

// The index type and field count must match across every schema (except empty index)
IndexDescriptorImpl combine_index_descriptors(
        std::span<const OutputSchema> schemas, const SchemaCombineOptions& options
) {
    const auto is_empty = [](const IndexDescriptorImpl& index) {
        return index.type() == IndexDescriptorImpl::Type::EMPTY;
    };
    auto result = schemas.front().stream_descriptor().index();
    for (const auto& schema : schemas.subspan(1)) {
        const auto& other = schema.stream_descriptor().index();
        check_index_types_combinable(result.type(), other.type(), options);
        if (is_empty(result)) {
            result = other;
            continue;
        }
        if (is_empty(other)) {
            continue;
        }
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                result.field_count() == other.field_count(),
                "Cannot {}: mismatching index field count, {} and {}",
                options.name(),
                result.field_count(),
                other.field_count()
        );
    }
    return result;
}

// Merge the required fields - the index levels, plus the value column for a Series - which are always the
// leading fields of the descriptor.
// Works with IndexType::EMPTY as well (num_required_columns_for(EMPTY) will skip the missing index columns)
void add_required_fields(
        StreamDescriptor& out, std::span<const OutputSchema> schemas, const RequiredFieldInfo& info,
        RequiredNameMismatches& mismatches, const SchemaCombineOptions& options
) {
    const auto required_fields = info.num_physical_required_columns();
    // Required fields can't use the MOST_PERMISSIVE type promotion policy,
    // because MOST_PERMISSIVE can break equality which is unacceptable for an index column.
    auto required_field_options = options;
    if (required_field_options.type_promotion == TypePromotionPolicy::MOST_PERMISSIVE) {
        required_field_options.type_promotion = TypePromotionPolicy::DYNAMIC;
    }
    // A position stays unset until a schema that has that field is reached
    // So e.g. if we're combining `Empty, Empty, Multiindex{"A", "B"}`. The fields for A and B will be set
    // on the 3rd iteration.
    std::vector<std::optional<FieldRef>> fields(required_fields);
    for (const auto& schema : schemas) {
        const auto& desc = schema.stream_descriptor();
        const auto required_for_schema = info.num_required_columns_for(desc.index().type());
        const auto skipped_levels = required_fields - required_for_schema;
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                desc.field_count() >= required_for_schema,
                "Cannot {}: expected at least {} required fields, but received {}",
                options.name(),
                required_for_schema,
                desc.field_count()
        );
        for (size_t idx = 0; idx < required_for_schema; ++idx) {
            const auto& field = desc.field(idx);
            auto& combined = fields[idx + skipped_levels];
            if (!combined.has_value()) {
                combined = field.ref();
                continue;
            }
            if (combined->name() != field.name()) {
                const auto detail = names_differ(combined->name(), field.name());
                if (idx + skipped_levels < info.num_physical_indices) {
                    mismatches.add_index(idx + skipped_levels, info.has_multi_index, detail);
                } else {
                    mismatches.add_series_name(detail);
                }
            }
            combined->type_ = combine_field_type(combined->type(), field.type(), required_field_options, field.name());
        }
    }
    for (size_t idx = 0; idx < required_fields; ++idx) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                fields[idx].has_value(), "No schema described required field {} of {}", idx, required_fields
        );
        const auto& field = *fields[idx];
        const bool is_index_level = idx < info.num_physical_indices;
        const bool unnamed = is_index_level ? mismatches.index_at(idx) : mismatches.series_name();
        if (!unnamed) {
            out.add_scalar_field(field.type().data_type(), field.name());
        } else if (is_index_level) {
            // Use the same naming scheme as _normalization.py does for unnamed multiindex levels, so that
            // later processing which looks for columns of that form keeps working.
            out.fields().add_field(field.type(), idx == 0 ? "index" : fmt::format("__fkidx__{}", idx));
        } else {
            // An unnamed Series is written with its value column named "0", the name Series.to_frame() gives it.
            out.fields().add_field(field.type(), "0");
        }
    }
}

// The non-index columns of a schema, in descriptor order. A view rather than a copy.
auto data_columns(const StreamDescriptor& desc, const RequiredFieldInfo& info) {
    return desc.fields() | std::views::drop(info.num_required_columns_for(desc.index().type()));
}

size_t data_column_count(const StreamDescriptor& desc, const RequiredFieldInfo& info) {
    return desc.field_count() - info.num_required_columns_for(desc.index().type());
}

// The non-index columns as an associative list, so that a union of two of them stays in descriptor order.
using DataColumns = std::vector<std::pair<std::string_view, TypeDescriptor>>;

DataColumns data_columns_of(const StreamDescriptor& desc, const RequiredFieldInfo& info) {
    auto columns = util::reserve_vector<DataColumns::value_type>(data_column_count(desc, info));
    for (const auto& field : data_columns(desc, info)) {
        columns.emplace_back(field.name(), field.type());
    }
    return columns;
}

// More detailed error message in case static schema recieves different number of columns.
// Lists the missing or new unexpected columns.
std::string data_column_differences(
        const StreamDescriptor& base, const StreamDescriptor& other, const RequiredFieldInfo& info
) {
    // A wide symbol can disagree about hundreds of columns, and a message that long is unreadable and unloggable.
    constexpr size_t max_listed = 5;
    const auto names_of = [&info](const StreamDescriptor& desc) {
        std::set<std::string_view> names;
        for (const auto& field : data_columns(desc, info)) {
            names.emplace(field.name());
        }
        return names;
    };
    const auto base_names = names_of(base);
    const auto other_names = names_of(other);
    std::vector<std::string_view> missing;
    std::vector<std::string_view> unexpected;
    std::ranges::set_difference(base_names, other_names, std::back_inserter(missing));
    std::ranges::set_difference(other_names, base_names, std::back_inserter(unexpected));
    // The count goes in the message so that a truncated list still says how much work the disagreement is, rather
    // than leaving the reader to discover the rest five at a time.
    const auto listed = [max_listed](const std::vector<std::string_view>& names) {
        const auto shown = std::min(names.size(), max_listed);
        return fmt::format(
                "{} [{}{}]",
                names.size(),
                fmt::join(names.begin(), names.begin() + shown, ", "),
                names.size() > shown ? ", etc." : ""
        );
    };
    return fmt::format("missing {}, unexpected {}", listed(missing), listed(unexpected));
}

// Every schema must carry the same non-index columns, in the same order.
void add_data_columns_static(
        StreamDescriptor& out, std::span<const OutputSchema> schemas, const RequiredFieldInfo& info,
        const SchemaCombineOptions& options
) {
    const auto& base = schemas.front().stream_descriptor();
    const auto column_count = data_column_count(base, info);
    const auto num_required_for_base = info.num_required_columns_for(base.index().type());
    auto types = util::reserve_vector<TypeDescriptor>(column_count);
    for (const auto& field : data_columns(base, info)) {
        types.emplace_back(field.type());
    }
    for (const auto& schema : schemas.subspan(1)) {
        const auto& desc = schema.stream_descriptor();
        if (data_column_count(desc, info) != column_count) {
            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    "Cannot {}: mismatching column count, {} against {}: {}",
                    options.name(),
                    column_count,
                    data_column_count(desc, info),
                    data_column_differences(base, desc, info)
            );
        }
        const auto num_required_for_schema = info.num_required_columns_for(desc.index().type());
        for (size_t idx = 0; idx < column_count; ++idx) {
            const auto name = base.field(idx + num_required_for_base).name();
            const auto& field = desc.field(idx + num_required_for_schema);
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    name == field.name(),
                    "Cannot {}: mismatching column name at position {}, {}",
                    options.name(),
                    idx,
                    names_differ(name, field.name())
            );
            types[idx] = combine_field_type(types[idx], field.type(), options, name);
        }
    }
    for (size_t idx = 0; idx < column_count; ++idx) {
        out.add_scalar_field(types[idx].data_type(), base.field(idx + num_required_for_base).name());
    }
}

// Keep only the columns present in every schema.
void add_data_columns_intersection(
        StreamDescriptor& out, std::span<const OutputSchema> schemas, const RequiredFieldInfo& info,
        const SchemaCombineOptions& options
) {
    const auto& base = schemas.front().stream_descriptor();
    // What every schema seen so far says about a column: the combined type, why the schemas disagreed about it, or
    // monostate once a schema turns up that does not have it at all. A column dropped for being absent stays dropped
    // even if the schemas disagreed about its type, because a type clash on a column not in the output is irrelevant.
    using Combined = std::variant<TypeDescriptor, Error, std::monostate>;
    ankerl::unordered_dense::map<std::string_view, Combined> columns_to_keep;
    for (const auto& field : data_columns(base, info)) {
        columns_to_keep.emplace(field.name(), field.type());
    }
    for (const auto& schema : schemas.subspan(1)) {
        util::for_each_key_union(
                data_columns_of(schema.stream_descriptor(), info),
                columns_to_keep,
                [&](std::string_view name, const TypeDescriptor* current, Combined* to_keep) {
                    // An intersection only ever shrinks, so a column the accumulated set never had is not of interest.
                    if (to_keep == nullptr) {
                        return;
                    }
                    if (current == nullptr) {
                        *to_keep = std::monostate{};
                        return;
                    }
                    util::variant_match(
                            *to_keep,
                            [](const std::monostate&) {},
                            [](const Error&) {},
                            [&](const TypeDescriptor& kept) {
                                *to_keep = util::variant_match(
                                        try_combine_field_type(kept, *current, options, name),
                                        [](const auto& combined) { return Combined{combined}; }
                                );
                            }
                    );
                }
        );
    }
    // Everything retained was present in every schema, so emit it in the base schema's order.
    for (const auto& field : data_columns(base, info)) {
        const auto it = columns_to_keep.find(field.name());
        if (it == columns_to_keep.end()) {
            continue;
        }
        util::variant_match(
                it->second,
                [&](const TypeDescriptor& combined) { out.add_scalar_field(combined.data_type(), field.name()); },
                [](const Error& error) { schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(error); },
                [](const std::monostate&) {}
        );
    }
}

// Keep the union of the columns, in the order they are first seen.
void add_data_columns_union(
        StreamDescriptor& out, std::span<const OutputSchema> schemas, const RequiredFieldInfo& info,
        const SchemaCombineOptions& options
) {
    ankerl::unordered_dense::map<std::string_view, TypeDescriptor> columns_to_keep;
    // Maintain the order in which the columns first appeared across the schemas.
    std::vector<std::string_view> column_names_to_keep;
    for (const auto& schema : schemas) {
        for (const auto& field : data_columns(schema.stream_descriptor(), info)) {
            if (const auto [it, inserted] = columns_to_keep.try_emplace(field.name(), field.type()); inserted) {
                column_names_to_keep.emplace_back(field.name());
            } else {
                it->second = combine_field_type(it->second, field.type(), options, field.name());
            }
        }
    }
    for (const auto name : column_names_to_keep) {
        out.add_scalar_field(columns_to_keep.at(name).data_type(), name);
    }
}

void add_data_columns(
        StreamDescriptor& out, std::span<const OutputSchema> schemas, const RequiredFieldInfo& info,
        const SchemaCombineOptions& options
) {
    // The three share only the per-schema column extraction, already factored into data_columns. Their control
    // flow differs enough - positional equality, erase-on-absent, insert-on-new - that merging them behind
    // flags reads worse than leaving them apart.
    switch (options.missing_column) {
    case MissingColumnPolicy::STRICT:
        add_data_columns_static(out, schemas, info, options);
        return;
    case MissingColumnPolicy::DROP:
        add_data_columns_intersection(out, schemas, info, options);
        return;
    case MissingColumnPolicy::KEEP:
        add_data_columns_union(out, schemas, info, options);
        return;
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// Normalization metadata
// ---------------------------------------------------------------------------------------------------------------------

bool operator==(
        const NormalizationMetadata_Pandas_ColumnName& lhs, const NormalizationMetadata_Pandas_ColumnName& rhs
) {
    return lhs.is_empty() == rhs.is_empty() && lhs.is_int() == rhs.is_int() && lhs.is_none() == rhs.is_none() &&
           lhs.original_name() == rhs.original_name();
}

void accumulate_norm_metadata_column_names(
        NormalizationMetadata_PandasDataFrame& accumulated, const NormalizationMetadata_PandasDataFrame& new_entry
) {
    accumulated.set_has_synthetic_columns(accumulated.has_synthetic_columns() && new_entry.has_synthetic_columns());
    auto* accumulated_col_names = accumulated.mutable_common()->mutable_col_names();
    for (auto& [col_name, col_name_info] : new_entry.common().col_names()) {
        if (const auto it = accumulated_col_names->find(col_name); it != accumulated_col_names->end()) {
            normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
                    it->second == col_name_info,
                    "Merging column name normalization for column: \"{}\" does not allow different ColumnName "
                    "settings for columns named the same way.",
                    col_name
            );
        }
    }
    accumulated_col_names->insert(new_entry.common().col_names().begin(), new_entry.common().col_names().end());
}

void accumulate_norm_metadata_column_names(NormalizationMetadata& accumulated, const NormalizationMetadata& new_entry) {
    if (accumulated.has_df()) {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                new_entry.has_df(),
                "Mismatching normalization metadata types in accumulation"
        );
        accumulate_norm_metadata_column_names(*accumulated.mutable_df(), new_entry.df());
    } else if (accumulated.has_series()) {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                new_entry.has_series(),
                "Mismatching normalization metadata types in accumulation"
        );
        accumulate_norm_metadata_column_names(*accumulated.mutable_series(), new_entry.series());
    }
}

bool has_arrow_or_pandas(const NormalizationMetadata& norm) {
    return norm.has_experimental_arrow() || pandas_common(norm) != nullptr;
}

// The kind of object named as the user knows it, rather than by the protobuf field the normalization metadata
// happens to store it under - "ndarray", not "np".
std::string_view input_type_name(const NormalizationMetadata& norm) {
    switch (norm.input_type_case()) {
    case NormalizationMetadata::kDf:
        return "DataFrame";
    case NormalizationMetadata::kSeries:
        return "Series";
    case NormalizationMetadata::kTs:
        return "TimeFrame";
    case NormalizationMetadata::kMsgPackFrame:
        return "pickled object";
    case NormalizationMetadata::kNp:
        return "ndarray";
    case NormalizationMetadata::kExperimentalArrow:
        return "arrow table";
    case NormalizationMetadata::INPUT_TYPE_NOT_SET:
        break;
    }
    return "unset";
}

void check_same_input_type(
        const NormalizationMetadata& lhs, const NormalizationMetadata& rhs, const SchemaCombineOptions& options
) {
    if (lhs.input_type_case() == rhs.input_type_case()) {
        return;
    }
    // A Series beside a DataFrame is much the commonest of these, and worth naming as the user would.
    if ((lhs.has_series() && rhs.has_df()) || (lhs.has_df() && rhs.has_series())) {
        normalization::raise<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
                "Cannot {}: a Series cannot be combined with a DataFrame", options.name()
        );
    }
    normalization::raise<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
            "Cannot {}: differing normalization input types, {} and {}",
            options.name(),
            input_type_name(lhs),
            input_type_name(rhs)
    );
}

// How a timezone reads in an error message. A timezone that was never set is not the same as one set to the empty
// string, so the two have to look different.
std::string timezone_name(std::optional<std::string_view> timezone) {
    return timezone.has_value() ? fmt::format("'{}'", *timezone) : std::string{"unset"};
}

// Disagreeing timezones are not allowed for static schema.
// Otherwise are reconciled by treating the result as timezone naive.
void on_timezone_mismatch(
        const SchemaCombineOptions& options, std::string_view column, std::optional<std::string_view> accumulated,
        std::optional<std::string_view> other
) {
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            options.type_promotion != TypePromotionPolicy::STATIC,
            "Cannot {}: timezones for column '{}' must match under static schema, {} against {}",
            options.name(),
            column,
            timezone_name(accumulated),
            timezone_name(other)
    );
}

// Only one side carries Arrow metadata for this column. Not allowed for static schema, otherwise whatever the one
// side says about the column is kept.
void on_column_metadata_one_sided(const SchemaCombineOptions& options, std::string_view column) {
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            options.type_promotion != TypePromotionPolicy::STATIC,
            "Cannot {}: only one side has arrow metadata for column '{}', which must match under static schema",
            options.name(),
            column
    );
}

// The leading dimension of an ndarray is the row count, thus has to be combined when combining norm metadata.
NormalizationMetadata combine_ndarray_metadata(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        const SchemaCombineOptions& options
) {
    normalization::check<ErrorCode::E_UPDATE_NOT_SUPPORTED>(
            options.operation != UPDATE, "current normalization scheme doesn't allow update of ndarray"
    );
    const auto& accumulated_shape = accumulated.np().shape();
    const auto& other_shape = other.np().shape();
    normalization::check<ErrorCode::E_WRONG_SHAPE>(
            !accumulated_shape.empty() && !other_shape.empty(),
            "Cannot {}: numpy array normalization metadata has an empty shape",
            options.name()
    );
    normalization::check<ErrorCode::E_WRONG_SHAPE>(
            std::equal(
                    accumulated_shape.begin() + 1, accumulated_shape.end(), other_shape.begin() + 1, other_shape.end()
            ),
            "The appending NDArray must have the same shape as the existing (excl. the first dimension)"
    );
    auto res = accumulated;
    (*res.mutable_np()->mutable_shape())[0] = accumulated_shape[0] + other_shape[0];
    return res;
}

NormalizationMetadata accumulate_arrow_and_arrow_norm(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        const SchemaCombineOptions& options
) {
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            accumulated.experimental_arrow().has_index() == other.experimental_arrow().has_index(),
            "Cannot {}: cannot combine indexed arrow data with unindexed arrow data",
            options.name()
    );
    // Per-column metadata is merged rather than taking only the base schema's, so that a column only a later
    // schema has keeps what that schema says about it. Presence is checked per column rather than by comparing
    // map sizes, so that a metadata field a future client adds does not make existing data un-appendable.
    auto res = accumulated;
    auto& res_columns = *res.mutable_experimental_arrow()->mutable_columns();
    util::for_each_key_union(
            accumulated.experimental_arrow().columns(),
            other.experimental_arrow().columns(),
            [&](const std::string& column_name,
                const ArrowColumnMeta* accumulated_column,
                const ArrowColumnMeta* other_column) {
                if (accumulated_column == nullptr) {
                    on_column_metadata_one_sided(options, column_name);
                    res_columns[column_name] = *other_column;
                    return;
                }
                if (other_column == nullptr) {
                    on_column_metadata_one_sided(options, column_name);
                    return;
                }
                // An absent timezone is not the same as an empty one, so compare presence as well as value.
                const auto timezone_of = [](const ArrowColumnMeta& column) {
                    return column.has_timezone() ? std::optional{std::string_view{column.timezone()}} : std::nullopt;
                };
                if (timezone_of(*accumulated_column) != timezone_of(*other_column)) {
                    on_timezone_mismatch(
                            options, column_name, timezone_of(*accumulated_column), timezone_of(*other_column)
                    );
                    // Present in both, so drop anything they disagree on, such as the timezone.
                    res_columns[column_name].clear_timezone();
                }
            }
    );
    return res;
}

// TODO (monday ref 11325694339): To be changed when working on arrow with pandas interop
// One arrow, one pandas: pandas is preferred as it carries more detail. Compatible when
// arrow.has_index() == pandas.index().is_physically_stored().
NormalizationMetadata accumulate_arrow_and_pandas_norm(
        const NormalizationMetadata& arrow_meta, const NormalizationMetadata& pandas_meta,
        const SchemaCombineOptions& options
) {
    const auto& common = *pandas_common(pandas_meta);
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            common.has_index(),
            "Cannot {}: cannot combine arrow-written data with multi-indexed pandas data",
            options.name()
    );
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            arrow_meta.experimental_arrow().has_index() == common.index().is_physically_stored(),
            "Cannot {}: cannot combine unindexed data with indexed data",
            options.name()
    );
    return pandas_meta;
}

void accumulate_multi_index(
        PandasMultiIndex* res_index, const PandasMultiIndex& other_index, RequiredNameMismatches& mismatches,
        const SchemaCombineOptions& options
) {
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            res_index->field_count() == other_index.field_count(),
            "Cannot {}: schemas have different index level counts, {} and {}",
            options.name(),
            res_index->field_count() + 1,
            other_index.field_count() + 1
    );
    const std::set<uint32_t> res_unnamed{res_index->fake_field_pos().begin(), res_index->fake_field_pos().end()};
    const std::set<uint32_t> other_unnamed{other_index.fake_field_pos().begin(), other_index.fake_field_pos().end()};
    if (const auto error = compare_pandas_names(
                PandasName::of_multi_index_level_0(*res_index), PandasName::of_multi_index_level_0(other_index)
        )) {
        mismatches.add_index(0, true, *error);
    }
    if (other_index.tz() != res_index->tz()) {
        on_timezone_mismatch(options, "Top level index", res_index->tz(), other_index.tz());
        res_index->clear_tz();
    }
    for (const auto& [idx, idx_timezone] : other_index.timezone()) {
        auto& res_timezone = (*res_index->mutable_timezone())[idx];
        if (res_timezone != idx_timezone) {
            on_timezone_mismatch(options, fmt::format("Index level {}", idx), res_timezone, idx_timezone);
            res_timezone = "";
        }
    }
    // A level both sides already record as unnamed agrees, and stays unnamed by virtue of already being in
    // the accumulated positions. A level only one side records as unnamed is a disagreement, so it is the
    // symmetric difference that matters.
    std::vector<uint32_t> disagreed;
    std::ranges::set_symmetric_difference(res_unnamed, other_unnamed, std::back_inserter(disagreed));
    for (const auto position : disagreed) {
        mismatches.add_index(position, true, fmt::format("level {} is unnamed on one side only", position));
    }
}

void accumulate_index(
        PandasIndex* res_index, const PandasIndex& other_index, RequiredNameMismatches& mismatches,
        const SchemaCombineOptions& options
) {
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            res_index->is_physically_stored() == other_index.is_physically_stored(),
            "Cannot {}: one index is a DatetimeIndex and the other is a RangeIndex",
            options.name()
    );
    if (const auto error = compare_pandas_names(PandasName::of_index(*res_index), PandasName::of_index(other_index))) {
        mismatches.add_index(0, false, *error);
    }
    if (other_index.tz() != res_index->tz()) {
        on_timezone_mismatch(options, "Index", res_index->tz(), other_index.tz());
        res_index->clear_tz();
    }
    if (other_index.step() != res_index->step()) {
        // This case can only be reached for concat.
        // Update doesn't support range indices and append has special handling for range indices to make sure they
        // are aligned.
        util::check(
                options.operation == NormalizationOperation::CONCAT,
                "Encountered mismatched range indices for operation {} which should have been handled earlier.",
                options.name()
        );
        log::version().warn("Mismatching RangeIndexes being combined, setting to start=0, step=1");
        res_index->set_start(0);
        res_index->set_step(1);
    }
}

NormalizationMetadata accumulate_pandas_and_pandas_norm(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        RequiredNameMismatches& mismatches, const SchemaCombineOptions& options
) {
    // Pandas + Pandas. A TimeFrame and a DataFrame describe their index alike but denormalize differently, and a
    // Series carries a value column a DataFrame does not, so the kind of object has to agree.
    check_same_input_type(accumulated, other, options);
    auto res = accumulated;
    auto* res_common = mutable_pandas_common(res);
    const auto& other_common = *pandas_common(other);
    // First check pandas + pandas shapes are compatible
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            res_common->has_multi_index() == other_common.has_multi_index(),
            "Cannot {}: cannot combine multi-indexed data with non-multi-indexed data",
            options.name()
    );

    if (res_common->has_multi_index()) {
        accumulate_multi_index(res_common->mutable_multi_index(), other_common.multi_index(), mismatches, options);
    } else {
        accumulate_index(res_common->mutable_index(), other_common.index(), mismatches, options);
    }

    // Last of the required fields, so that a disagreement about the index - which every schema has - is reported
    // ahead of one about the value column, which only a Series has.
    if (res.has_series()) {
        if (const auto error =
                    compare_pandas_names(PandasName::of_series(*res_common), PandasName::of_series(other_common))) {
            mismatches.add_series_name(*error);
        }
    }
    accumulate_norm_metadata_column_names(res, other);
    return res;
}

// Pairwise merge of two normalization metadata objects: timezones, RangeIndex start/step, multi-index fields
// and per-column Arrow metadata. Required field name disagreements are added to mismatches.
NormalizationMetadata accumulate_norm_metadata(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        RequiredNameMismatches& mismatches, const SchemaCombineOptions& options
) {
    const auto operation = options.name();
    normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
            !accumulated.has_msg_pack_frame() && !other.has_msg_pack_frame(),
            "Cannot {}: pickled data cannot be combined",
            operation
    );
    if (!has_arrow_or_pandas(accumulated) || !has_arrow_or_pandas(other)) {
        check_same_input_type(accumulated, other, options);
        if (accumulated.has_np()) {
            return combine_ndarray_metadata(accumulated, other, options);
        }
        // arrow_or_pandas are handled below and we raise for MsgPackFrame. This leaves only unset input types
        // used in C++ tests
        util::check(
                accumulated.input_type_case() == NormalizationMetadata::INPUT_TYPE_NOT_SET,
                "All cases apart from NdArray and unset should have been covered but got {}",
                input_type_name(accumulated)
        );
        return other;
    }

    if (accumulated.has_experimental_arrow() && other.has_experimental_arrow()) {
        return accumulate_arrow_and_arrow_norm(accumulated, other, options);
    }

    if (accumulated.has_experimental_arrow() || other.has_experimental_arrow()) {
        const auto& arrow_meta = accumulated.has_experimental_arrow() ? accumulated : other;
        const auto& pandas_meta = accumulated.has_experimental_arrow() ? other : accumulated;
        return accumulate_arrow_and_pandas_norm(arrow_meta, pandas_meta, options);
    }

    return accumulate_pandas_and_pandas_norm(accumulated, other, mismatches, options);
}

// Apply the recorded name disagreements to the normalization metadata.
// Should be done once with mismatches from both normalization metadatas and from descriptors.
void apply_required_name_mismatches(
        NormalizationMetadata& norm, const RequiredFieldInfo& info, const RequiredNameMismatches& mismatches
) {
    if (!mismatches.any()) {
        return;
    }
    auto* common = mutable_pandas_common(norm);
    if (common == nullptr) {
        return;
    }
    if (info.has_multi_index && !mismatches.index_positions().empty()) {
        auto* multi_index = common->mutable_multi_index();
        // The accumulated positions are the levels every schema agrees are unnamed; the recorded ones are the
        // levels they disagree about. Both end up unnamed, so the output is the union.
        std::set<uint32_t> unnamed{multi_index->fake_field_pos().begin(), multi_index->fake_field_pos().end()};
        for (const auto position : mismatches.index_positions()) {
            unnamed.insert(static_cast<uint32_t>(position));
        }
        multi_index->clear_fake_field_pos();
        for (const auto position : unnamed) {
            multi_index->add_fake_field_pos(position);
        }
        if (unnamed.contains(0)) {
            multi_index->set_name("index");
        }
    } else if (mismatches.index_at(0)) {
        auto* index = common->mutable_index();
        index->set_name("index");
        index->set_is_int(false);
        index->set_fake_name(true);
    }
    if (mismatches.series_name()) {
        common->set_name("");
        common->set_has_name(false);
    }
}

// Fold the normalization metadata over every schema.
NormalizationMetadata combine_norm_metadata(
        std::span<const OutputSchema> schemas, RequiredNameMismatches& mismatches, const SchemaCombineOptions& options
) {
    auto result = schemas.front().norm_metadata_;
    for (const auto& schema : schemas.subspan(1)) {
        result = accumulate_norm_metadata(result, schema.norm_metadata_, mismatches, options);
    }
    return result;
}

// What is known about the sort order of every schema's data laid end to end.
SortedValue combine_sorted(std::span<const OutputSchema> schemas) {
    auto result = schemas.front().stream_descriptor().sorted();
    for (const auto& schema : schemas.subspan(1)) {
        result = deduce_sorted(result, schema.stream_descriptor().sorted());
    }
    return result;
}
} // namespace

void check_index_types_combinable(
        IndexDescriptorImpl::Type accumulated, IndexDescriptorImpl::Type other, const SchemaCombineOptions& options
) {
    if (accumulated == IndexDescriptorImpl::Type::EMPTY || other == IndexDescriptorImpl::Type::EMPTY) {
        return;
    }
    normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
            accumulated == other,
            "Cannot {}: cannot combine a {} index with a {} index",
            options.name(),
            index_type_to_str(other),
            index_type_to_str(accumulated)
    );
}

SortedValue deduce_sorted(SortedValue existing_frame, SortedValue input_frame) {
    constexpr auto UNKNOWN = SortedValue::UNKNOWN;
    constexpr auto ASCENDING = SortedValue::ASCENDING;
    constexpr auto DESCENDING = SortedValue::DESCENDING;
    constexpr auto UNSORTED = SortedValue::UNSORTED;

    SortedValue final_state;
    switch (existing_frame) {
    case UNKNOWN:
        final_state = input_frame == UNSORTED ? UNSORTED : UNKNOWN;
        break;
    case ASCENDING:
        if (input_frame == UNKNOWN) {
            final_state = UNKNOWN;
        } else if (input_frame != ASCENDING) {
            final_state = UNSORTED;
        } else {
            final_state = ASCENDING;
        }
        break;
    case DESCENDING:
        if (input_frame == UNKNOWN) {
            final_state = UNKNOWN;
        } else if (input_frame != DESCENDING) {
            final_state = UNSORTED;
        } else {
            final_state = DESCENDING;
        }
        break;
    default:
        final_state = UNSORTED;
        break;
    }
    return final_state;
}

OutputSchema combine_schema(std::span<const OutputSchema> schemas, const SchemaCombineOptions& options) {
    util::check(!schemas.empty(), "Cannot combine an empty list of schemas");
    // An empty index must be combinable with any other type of index. All non index related metadata and descriptor
    // fields should be combined as usual. combine_norm_metadata doesn't handle this currently.
    // All of append, update, concat filter out empty symbols (hence empty indices), so the logic to merge empty
    // index with non-empty one is not currently needed.
    // TODO (monday ref 12911136883): support combining an empty index with a non-empty one.
    const auto empty_indexes = std::ranges::count_if(schemas, has_empty_index);
    internal::check<ErrorCode::E_NOT_IMPLEMENTED>(
            empty_indexes == 0 || empty_indexes == std::ssize(schemas),
            "Cannot {}: combining an empty index with a non-empty one is not implemented yet, {} of {} schemas have an "
            "empty index",
            options.name(),
            empty_indexes,
            schemas.size()
    );
    // The normalization metadata goes first because it is what decides which shapes may combine at all
    RequiredNameMismatches mismatches{options};
    auto norm = combine_norm_metadata(schemas, mismatches, options);

    // The RequiredFieldsInfo is constructed from the combined norm.
    // This allows combining arrow with pandas multindex (which would otherwise have different required fields counts).
    const auto info = required_fields_info(norm);

    StreamDescriptor out{options.stream_id.value_or(StreamId{}), combine_index_descriptors(schemas, options)};
    out.set_sorted(combine_sorted(schemas));
    add_required_fields(out, schemas, info, mismatches, options);
    add_data_columns(out, schemas, info, options);
    // Whatever only the normalization metadata reveals - a RangeIndex name, a Series name - is applied here, in
    // one place, so that the descriptor field names and the metadata cannot end up disagreeing.
    apply_required_name_mismatches(norm, info, mismatches);
    return OutputSchema{std::move(out), std::move(norm)};
}

OutputSchema schema_from_tsd(const TimeseriesDescriptor& tsd) {
    return {tsd.as_stream_descriptor(), tsd.normalization()};
}

OutputSchema schema_from_input_frame(const pipelines::InputFrame& frame) {
    // compute_desc_for_tsd rather than desc() because the result describes what will be stored, and an Arrow string
    // column holds 32 bit offsets in the frame but always 64 bit on disk.
    return {frame.compute_desc_for_tsd(), frame.norm_meta};
}

TimeseriesDescriptor tsd_from_schema(OutputSchema&& schema, size_t total_rows, pipelines::InputFrame& frame) {
    auto [descriptor, norm_meta, _] = schema.release();
    descriptor.set_id(frame.desc().id());
    return make_timeseries_descriptor(
            total_rows,
            std::move(descriptor),
            std::move(norm_meta),
            std::move(frame.user_meta),
            std::nullopt,
            frame.bucketize_dynamic
    );
}

} // namespace arcticdb
