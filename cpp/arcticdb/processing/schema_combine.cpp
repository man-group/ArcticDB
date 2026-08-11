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
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/type_traits.hpp>

#include <iterator>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace arcticdb {

using namespace proto::descriptors;
using entity::DataType;
using entity::Field;
using entity::IndexDescriptorImpl;
using entity::OutputSchema;
using entity::StreamDescriptor;
using entity::TypeDescriptor;

namespace {
// Whether the metadata describes the empty index a zero-row write produces with empty_types enabled.
bool has_empty_index(const NormalizationMetadata& norm) {
    const auto* common = pandas_common(norm);
    return common != nullptr && !common->has_multi_index() && !common->index().is_physically_stored() &&
           common->index().step() == 0;
}

using pipelines::index::required_fields_info;
using pipelines::index::RequiredFieldInfo;

// The two sides of a name disagreement, quoted so that an unnamed field is visible as ''.
std::string names_differ(std::string_view accumulated, std::string_view other) {
    return fmt::format("'{}' against '{}'", accumulated, other);
}

// What disagreed about the names of the required fields, accumulated across every schema being combined.
class RequiredNameMismatches {
  public:
    explicit RequiredNameMismatches(const SchemaCombineOptions& options) : options_(options) {}

    // A multi-index's level names are reported as an index incompatibility: they are what keeps the normalization
    // metadata in step with the data, which is why they must match even under dynamic schema. A single index's
    // name is a descriptor field, so a disagreement about it is a descriptor mismatch.
    void add_index(size_t position, bool multi_index, const std::string& detail) {
        if (raises()) {
            if (multi_index) {
                normalization::raise<ErrorCode::E_INCOMPATIBLE_INDEX>(
                        "Cannot {}: multi-index level names must match, {}", options_.name(), detail
                );
            }
            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    "Cannot {}: index names must match, {}", options_.name(), detail
            );
        }
        index_positions_.emplace(position);
    }

    void add_series_name(const std::string& detail) {
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

// Reconcile the type of a field according to the type promotion policy.
std::optional<TypeDescriptor> try_combine_field_type(
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

TypeDescriptor combine_field_type(
        const TypeDescriptor& base, const TypeDescriptor& other, const SchemaCombineOptions& options,
        std::string_view name
) {
    const auto combined = try_combine_field_type(base, other, options.type_promotion);
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            combined.has_value(),
            "Cannot {}: no common type for column '{}', {} and {}",
            options.name(),
            name,
            base,
            other
    );
    return *combined;
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
        const bool unnamed = idx < info.num_physical_indices ? mismatches.index_at(idx) : mismatches.series_name();
        if (unnamed) {
            // Use the same naming scheme as _normalization.py does for unnamed multiindex levels, so that
            // later processing which looks for columns of that form keeps working.
            out.fields().add_field(field.type(), idx == 0 ? "index" : fmt::format("__fkidx__{}", idx));
        } else {
            out.add_scalar_field(field.type().data_type(), field.name());
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
    const auto listed = [max_listed](const std::vector<std::string_view>& names) {
        const auto shown = std::min(names.size(), max_listed);
        return fmt::format(
                "[{}{}]", fmt::join(names.begin(), names.begin() + shown, ", "), names.size() > shown ? ", etc." : ""
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
    std::vector<TypeDescriptor> types;
    types.reserve(column_count);
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
    // The combined type is optional because two schemas may disagree irreconcilably about a column that a
    // third schema does not have at all, in which case the column is dropped and the clash is irrelevant.
    // Cannot use ankerl::unordered_dense as its iterators are not stable across erase.
    std::unordered_map<std::string_view, std::optional<TypeDescriptor>> columns_to_keep;
    for (const auto& field : data_columns(base, info)) {
        columns_to_keep.emplace(field.name(), field.type());
    }
    for (const auto& schema : schemas.subspan(1)) {
        ankerl::unordered_dense::map<std::string_view, TypeDescriptor> other_columns;
        for (const auto& field : data_columns(schema.stream_descriptor(), info)) {
            other_columns.emplace(field.name(), field.type());
        }
        for (auto it = columns_to_keep.begin(); it != columns_to_keep.end();) {
            const auto other_it = other_columns.find(it->first);
            if (other_it == other_columns.end()) {
                it = columns_to_keep.erase(it);
                continue;
            }
            if (it->second.has_value()) {
                it->second = try_combine_field_type(*it->second, other_it->second, options.type_promotion);
            }
            ++it;
        }
    }
    // Everything retained was present in every schema, so emit it in the base schema's order.
    for (const auto& field : data_columns(base, info)) {
        if (const auto it = columns_to_keep.find(field.name()); it != columns_to_keep.end()) {
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    it->second.has_value(), "Cannot {}: no common type for column '{}'", options.name(), field.name()
            );
            out.add_scalar_field(it->second->data_type(), field.name());
        }
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

template<typename ColumnNameMapParent>
requires util::any_of<
        ColumnNameMapParent, NormalizationMetadata_NormalisedTimeSeries, NormalizationMetadata_PandasDataFrame>
void accumulate_norm_metadata_column_names(ColumnNameMapParent& accumulated, const ColumnNameMapParent& new_entry) {
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

// The kind of object as the normalization metadata names it - "df", "series", "np", "msg_pack_frame" - which is what
// a user comparing two error messages needs, rather than the oneof's tag number.
std::string input_type_name(const NormalizationMetadata& norm) {
    const auto* field = NormalizationMetadata::descriptor()->FindFieldByNumber(norm.input_type_case());
    // FieldDescriptor::name() returns a string_view on newer protobuf and a const string& on older, so neither arm
    // of the conditional can be left to deduce the common type.
    return field != nullptr ? std::string{field->name()} : std::string{"unset"};
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

// Disagreeing timezones are not allowed for static schema.
// Otherwise are reconciled by treating the result as timezone naive.
void on_timezone_mismatch(const SchemaCombineOptions& options, std::string_view column) {
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            options.type_promotion != TypePromotionPolicy::STATIC,
            "Cannot {}: timezones for column '{}' must match under static schema",
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
        // Preserve append behavior to overwrite with the new schema.
        return other;
    }

    // Allow combining an empty index with any other index
    if (has_empty_index(accumulated)) {
        return other;
    }
    if (has_empty_index(other)) {
        return accumulated;
    }

    // Arrow + arrow
    if (accumulated.has_experimental_arrow() && other.has_experimental_arrow()) {
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                accumulated.experimental_arrow().has_index() == other.experimental_arrow().has_index(),
                "Cannot {}: cannot combine indexed arrow data with unindexed arrow data",
                operation
        );
        // Per-column metadata is merged rather than taking only the base schema's, so that a column only a later
        // schema has keeps what that schema says about it.
        auto res = accumulated;
        auto& res_columns = *res.mutable_experimental_arrow()->mutable_columns();
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                options.type_promotion != TypePromotionPolicy::STATIC ||
                        res_columns.size() == other.experimental_arrow().columns().size(),
                "Cannot {}: number of columns with metadata does not match under static schema, {} columns carry it "
                "against {}",
                options.name(),
                res_columns.size(),
                other.experimental_arrow().columns().size()
        );
        for (const auto& [column_name, other_column] : other.experimental_arrow().columns()) {
            const auto it = res_columns.find(column_name);
            const bool matches = it != res_columns.end() && other_column.timezone() == it->second.timezone();
            if (!matches) {
                on_timezone_mismatch(options, column_name);
                if (it == res_columns.end()) {
                    res_columns[column_name] = other_column;
                } else {
                    // Present in both, so drop anything they disagree on, such as the timezone.
                    it->second.clear_timezone();
                }
            }
        }
        return res;
    }

    // TODO (monday ref 11325694339): To be changed when working on arrow with pandas interop
    // One arrow, one pandas: pandas is preferred as it carries more detail. Compatible when
    // arrow.has_index() == pandas.index().is_physically_stored().
    if (accumulated.has_experimental_arrow() || other.has_experimental_arrow()) {
        const auto& arrow_meta = accumulated.has_experimental_arrow() ? accumulated : other;
        const auto& pandas_meta = accumulated.has_experimental_arrow() ? other : accumulated;
        const auto& common = *pandas_common(pandas_meta);
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                common.has_index(),
                "Cannot {}: cannot combine arrow-written data with multi-indexed pandas data",
                operation
        );
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                arrow_meta.experimental_arrow().has_index() == common.index().is_physically_stored(),
                "Cannot {}: cannot combine unindexed data with indexed data",
                operation
        );
        return pandas_meta;
    }

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
            operation
    );
    if (res_common->has_multi_index()) {
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                res_common->multi_index().field_count() == other_common.multi_index().field_count(),
                "Cannot {}: schemas have different index level counts, {} and {}",
                operation,
                res_common->multi_index().field_count() + 1,
                other_common.multi_index().field_count() + 1
        );
    } else {
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                res_common->index().is_physically_stored() == other_common.index().is_physically_stored(),
                "Cannot {}: one index is physically stored and the other is not",
                operation
        );
    }

    if (res_common->has_multi_index()) {
        auto* res_index = res_common->mutable_multi_index();
        const auto& other_index = other_common.multi_index();
        const std::set<uint32_t> res_unnamed{res_index->fake_field_pos().begin(), res_index->fake_field_pos().end()};
        const std::set<uint32_t> other_unnamed{
                other_index.fake_field_pos().begin(), other_index.fake_field_pos().end()
        };
        // As for a single index, the level 0 name is a placeholder when both sides record it as unnamed, and which
        // placeholder gets stored differs between client versions.
        const bool both_faked = res_unnamed.contains(0) && other_unnamed.contains(0);
        if (other_index.is_int() != res_index->is_int() || (!both_faked && other_index.name() != res_index->name())) {
            mismatches.add_index(0, true, names_differ(res_index->name(), other_index.name()));
        }
        if (other_index.tz() != res_index->tz()) {
            on_timezone_mismatch(options, "Top level index");
            res_index->clear_tz();
        }
        for (const auto& [idx, idx_timezone] : other_index.timezone()) {
            if ((*res_index->mutable_timezone())[idx] != idx_timezone) {
                on_timezone_mismatch(options, fmt::format("Index level {}", idx));
                (*res_index->mutable_timezone())[idx] = "";
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
    } else {
        auto* res_index = res_common->mutable_index();
        const auto& other_index = other_common.index();
        // A faked name is a placeholder for "this index has no name", and which placeholder gets stored has changed
        // between client versions - 1.6.2 left it empty where we write "index" - so only a real name can be compared.
        const bool both_faked = res_index->fake_name() && other_index.fake_name();
        if (other_index.fake_name() != res_index->fake_name() || other_index.is_int() != res_index->is_int() ||
            (!both_faked && other_index.name() != res_index->name())) {
            mismatches.add_index(0, false, names_differ(res_index->name(), other_index.name()));
        }
        if (other_index.tz() != res_index->tz()) {
            on_timezone_mismatch(options, "Index");
            res_index->clear_tz();
        }
        if (other_index.step() != res_index->step()) {
            log::version().warn("Mismatching RangeIndexes being combined, setting to start=0, step=1");
            res_index->set_start(0);
            res_index->set_step(1);
        }
    }
    // Last of the required fields, so that a disagreement about the index - which every schema has - is reported
    // ahead of one about the value column, which only a Series has.
    if (res.has_series() &&
        (res_common->has_name() != other_common.has_name() || res_common->name() != other_common.name())) {
        mismatches.add_series_name(names_differ(res_common->name(), other_common.name()));
    }
    accumulate_norm_metadata_column_names(res, other);
    return res;
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
