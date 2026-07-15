/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/schema_combine.hpp>

#include <google/protobuf/util/message_differencer.h>

#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/type_traits.hpp>

#include <string>
#include <unordered_map>
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

// ---------------------------------------------------------------------------------------------------------------------
// Moved here from index_utils.cpp / normalization_utils.cpp so that all schema merge/verification logic lives under
// combine_schema. The originals are deleted once the append/update/concat call sites are rewired.
// ---------------------------------------------------------------------------------------------------------------------

// The number of leading fields that make up the index (plus the value column for a Series). Copied from
// index_utils.cpp::required_fields_count.
uint32_t required_fields_count(const StreamDescriptor& stream_desc, const NormalizationMetadata& norm_meta) {
    if (norm_meta.has_df() && norm_meta.df().common().has_multi_index()) {
        // The field count in the norm metadata is one less than the actual number of levels in the multiindex.
        return norm_meta.df().common().multi_index().field_count() + 1;
    } else if (norm_meta.has_series() && norm_meta.series().common().has_multi_index()) {
        return norm_meta.series().common().multi_index().field_count() + 2;
    } else {
        return stream_desc.index().field_count() + (norm_meta.has_series() ? 1 : 0);
    }
}

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

// Pairwise merge of two normalization metadata objects, reconciling series/index names, timezones, RangeIndex
// start/step and multi-index fields. Copied from normalization_utils.cpp::accumulate_norm_metadata.
NormalizationMetadata accumulate_norm_metadata(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        std::unordered_set<size_t>& fake_field_pos_acc
) {
    // Arrow + Arrow: compatible as long as both have the same has_index; keep the accumulated metadata.
    if (accumulated.has_experimental_arrow() && other.has_experimental_arrow()) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                accumulated.experimental_arrow().has_index() == other.experimental_arrow().has_index(),
                "Cannot join indexed arrow data with unindexed arrow data"
        );
        return accumulated;
    }

    // One arrow, one pandas: pandas is preferred as it carries more detail. Compatible when
    // arrow.has_index() == pandas.index().is_physically_stored().
    if (accumulated.has_experimental_arrow() || other.has_experimental_arrow()) {
        const auto& arrow_meta = accumulated.has_experimental_arrow() ? accumulated : other;
        const auto& pandas_meta = accumulated.has_experimental_arrow() ? other : accumulated;
        const auto& common = pandas_meta.has_series() ? pandas_meta.series().common() : pandas_meta.df().common();
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                common.has_index(), "Cannot join arrow-written data with multi-indexed pandas data"
        );
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                arrow_meta.experimental_arrow().has_index() == common.index().is_physically_stored(),
                "Cannot join unindexed with indexed data"
        );
        return pandas_meta;
    }

    // Pandas + Pandas.
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            accumulated.has_series() == other.has_series(), "Multi-symbol joins cannot join a Series to a DataFrame"
    );

    auto res = accumulated;
    auto* res_common = res.has_series() ? res.mutable_series()->mutable_common() : res.mutable_df()->mutable_common();
    const auto& other_common = other.has_series() ? other.series().common() : other.df().common();

    if (res.has_series()) {
        if (res_common->name() != other_common.name() || !other_common.has_name() || !res_common->has_name()) {
            res_common->set_name("");
            res_common->set_has_name(false);
        }
    }

    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            other_common.has_multi_index() == res_common->has_multi_index(), "Mismatching norm metadata in schema join"
    );

    if (res_common->has_multi_index()) {
        auto* res_index = res_common->mutable_multi_index();
        const auto& other_index = other_common.multi_index();
        if (other_index.name() != res_index->name() || other_index.is_int() != res_index->is_int()) {
            res_index->clear_name();
            res_index->set_is_int(false);
        }
        if (other_index.tz() != res_index->tz()) {
            res_index->clear_tz();
        }
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                other_index.field_count() == res_index->field_count(), "Mismatching norm metadata in schema join"
        );
        for (const auto& [idx, idx_timezone] : other_index.timezone()) {
            if ((*res_index->mutable_timezone())[idx] != idx_timezone) {
                (*res_index->mutable_timezone())[idx] = "";
            }
        }
        fake_field_pos_acc.insert(other_index.fake_field_pos().begin(), other_index.fake_field_pos().end());
    } else {
        auto* res_index = res_common->mutable_index();
        const auto& other_index = other_common.index();
        if (other_index.name() != res_index->name() || other_index.is_int() != res_index->is_int() ||
            other_index.fake_name() || res_index->fake_name()) {
            res_index->set_name("index");
            res_index->set_is_int(false);
            res_index->set_fake_name(true);
        }
        if (other_index.tz() != res_index->tz()) {
            res_index->clear_tz();
        }
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                other_index.is_physically_stored() == res_index->is_physically_stored(),
                "Mismatching norm metadata in schema join"
        );
        if (other_index.step() != res_index->step()) {
            log::version().warn("Mismatching RangeIndexes being combined, setting to start=0, step=1");
            res_index->set_start(0);
            res_index->set_step(1);
        }
    }
    accumulate_norm_metadata_column_names(res, other);
    return res;
}

// Reconcile the type of a field present in both schemas.
TypeDescriptor combine_field_type(
        const TypeDescriptor& base, const TypeDescriptor& other, TypePromotionPolicy policy, std::string_view name
) {
    if (base == other) {
        return base;
    }
    if (policy == TypePromotionPolicy::Dynamic) {
        if (auto common = has_valid_common_type(base, other); common.has_value()) {
            return *common;
        }
        schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                "No valid common type between {} and {} for column {}", base, other, name
        );
    }
    // Static: only empty->concrete and fixed->dynamic string promotions, mirroring get_merged_tsd. Anything else
    // must be trivially compatible or the combine raises, mirroring columns_match.
    if (is_empty_type(base.data_type())) {
        return other;
    }
    if (is_empty_type(other.data_type())) {
        return base;
    }
    if (is_sequence_type(base.data_type()) && is_sequence_type(other.data_type())) {
        const bool base_dynamic = is_dynamic_string_type(base.data_type());
        const bool other_dynamic = is_dynamic_string_type(other.data_type());
        if (base_dynamic != other_dynamic) {
            return base_dynamic ? base : other;
        }
    }
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            trivially_compatible_types(base, other),
            "Incompatible types {} and {} for column {} under static schema",
            base,
            other,
            name
    );
    return base;
}

// Reconcile the combined index descriptor. Append/update let an empty index defer to the concrete one; concat
// requires the type and field count to match exactly (see generate_index_descriptor).
IndexDescriptorImpl combine_index_descriptor(
        const IndexDescriptorImpl& base, const IndexDescriptorImpl& other, bool allow_empty_defer
) {
    if (allow_empty_defer) {
        if (base.type() == IndexDescriptorImpl::Type::EMPTY) {
            return other;
        }
        if (other.type() == IndexDescriptorImpl::Type::EMPTY) {
            return base;
        }
    }
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            base.type() == other.type(),
            "Mismatching index type when combining schemas: {} and {}",
            static_cast<int>(base.type()),
            static_cast<int>(other.type())
    );
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            base.field_count() == other.field_count(),
            "Mismatching index field count when combining schemas: {} and {}",
            base.field_count(),
            other.field_count()
    );
    return base;
}

// Merge the required (index / Series value) fields, positions [0, required_fields_count). Returns the set of
// positions where the field names differed, which drives the output multi-index fake_field_pos for concat.
std::unordered_set<size_t> add_required_fields(
        StreamDescriptor& out, const StreamDescriptor& base, const StreamDescriptor& other, uint32_t required_fields,
        const SchemaCombineOptions& options
) {
    std::unordered_set<size_t> non_matching_name_indices;
    schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
            other.field_count() >= required_fields,
            "Expected at least {} fields for index, but received {}",
            required_fields,
            other.field_count()
    );
    for (uint32_t idx = 0; idx < required_fields; ++idx) {
        const auto& base_field = base.field(idx);
        const auto& other_field = other.field(idx);
        const auto type =
                combine_field_type(base_field.type(), other_field.type(), options.type_promotion, base_field.name());
        if (base_field.name() == other_field.name()) {
            out.add_scalar_field(type.data_type(), base_field.name());
            continue;
        }
        if (options.name_mismatch == RequiredNameMismatchPolicy::Raise) {
            schema::raise<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    "Index/required field names must match when combining schemas: {} and {}",
                    base_field.name(),
                    other_field.name()
            );
        }
        // ReconcileToFake: use the unnamed-multiindex naming scheme from _normalization.py so downstream
        // processing that looks for these columns keeps working.
        non_matching_name_indices.emplace(idx);
        out.fields().add_field(type, idx == 0 ? "index" : fmt::format("__fkidx__{}", idx));
    }
    return non_matching_name_indices;
}

// Non-index columns of a schema, in descriptor order, as (name, type) pairs.
std::vector<std::pair<std::string, TypeDescriptor>> data_columns(const StreamDescriptor& desc, uint32_t skip) {
    std::vector<std::pair<std::string, TypeDescriptor>> out;
    out.reserve(desc.field_count() - skip);
    for (uint32_t idx = skip; idx < desc.field_count(); ++idx) {
        const auto& field = desc.field(idx);
        out.emplace_back(std::string(field.name()), field.type());
    }
    return out;
}

void add_data_columns(
        StreamDescriptor& out, const StreamDescriptor& base, const StreamDescriptor& other, uint32_t required_fields,
        const SchemaCombineOptions& options
) {
    const auto base_cols = data_columns(base, required_fields);
    const auto other_cols = data_columns(other, required_fields);
    std::unordered_map<std::string, TypeDescriptor> other_by_name;
    for (const auto& [name, type] : other_cols) {
        other_by_name.emplace(name, type);
    }

    if (options.missing_column == MissingColumnPolicy::Raise) {
        // Static append/update: the two schemas must carry the same non-index columns in the same order.
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                base_cols.size() == other_cols.size(),
                "Mismatching column count when combining schemas under static schema: {} and {}",
                base_cols.size(),
                other_cols.size()
        );
        for (size_t idx = 0; idx < base_cols.size(); ++idx) {
            const auto& [base_name, base_type] = base_cols[idx];
            const auto& [other_name, other_type] = other_cols[idx];
            schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                    base_name == other_name,
                    "Mismatching column name when combining schemas: {} and {}",
                    base_name,
                    other_name
            );
            out.add_scalar_field(
                    combine_field_type(base_type, other_type, options.type_promotion, base_name).data_type(), base_name
            );
        }
        return;
    }

    // Keep (union) or Drop (intersection). Emit base columns in order first.
    for (const auto& [name, base_type] : base_cols) {
        auto it = other_by_name.find(name);
        if (it != other_by_name.end()) {
            out.add_scalar_field(
                    combine_field_type(base_type, it->second, options.type_promotion, name).data_type(), name
            );
        } else if (options.missing_column == MissingColumnPolicy::Keep) {
            out.add_scalar_field(base_type.data_type(), name);
        }
        // Drop: column missing from other, omit it.
    }
    if (options.missing_column == MissingColumnPolicy::Keep) {
        // Then append columns unique to other, in other's order.
        std::unordered_set<std::string> base_names;
        for (const auto& [name, type] : base_cols) {
            base_names.insert(name);
        }
        for (const auto& [name, type] : other_cols) {
            if (!base_names.contains(name)) {
                out.add_scalar_field(type.data_type(), name);
            }
        }
    }
}

template<typename CommonNormalization>
requires util::any_of<
        CommonNormalization, proto::descriptors::NormalizationMetadata_Pandas,
        proto::descriptors::NormalizationMetadata_NormalisedTimeSeries>
void take_new_index(CommonNormalization& dest_common, const CommonNormalization& source_common) {
    // Last-write-wins for the RangeIndex (Monday 9797097831), matching merge_rowrange_index.
    if (dest_common.has_index()) {
        *dest_common.mutable_index() = source_common.index();
    } else if (dest_common.has_multi_index()) {
        *dest_common.mutable_multi_index() = source_common.multi_index();
    }
}

template<typename CommonNormalization>
requires util::any_of<
        CommonNormalization, proto::descriptors::NormalizationMetadata_Pandas,
        proto::descriptors::NormalizationMetadata_NormalisedTimeSeries>
void overwrite_index_tz(CommonNormalization& dest_common, const CommonNormalization& source_common) {
    // The new frame's timezone overwrites the existing one (Monday 12029540807), matching merge_timeseries_index.
    if (dest_common.has_multi_index()) {
        dest_common.mutable_multi_index()->set_tz(source_common.multi_index().tz());
    } else {
        dest_common.mutable_index()->set_tz(source_common.index().tz());
    }
}

// Append/update normalization merge. Column names accumulate; the RangeIndex name is last-write-wins (Monday
// 9797097831) and the timeseries timezone is overwritten by the new frame (Monday 12029540807); arrow per-column
// metadata is reconciled. The numpy shape[0] bump stays in the caller's TSD assembly.
NormalizationMetadata append_merge_norm_metadata(
        const NormalizationMetadata& base, const NormalizationMetadata& other, IndexDescriptorImpl::Type index_type
) {
    if (index_type == IndexDescriptorImpl::Type::EMPTY) {
        return other;
    }
    NormalizationMetadata result = base;
    accumulate_norm_metadata_column_names(result, other);
    if (result.has_experimental_arrow()) {
        result = other;
        auto& new_col_meta_map = *result.mutable_experimental_arrow()->mutable_columns();
        const auto& old_col_meta_map = base.experimental_arrow().columns();
        for (const auto& [col_name, old_col_meta] : old_col_meta_map) {
            if (auto it = new_col_meta_map.find(col_name); it != new_col_meta_map.end()) {
                if (!google::protobuf::util::MessageDifferencer::Equals(old_col_meta, it->second)) {
                    it->second.clear_timezone();
                }
            } else {
                new_col_meta_map[col_name] = old_col_meta;
            }
        }
    }
    if (index_type == IndexDescriptorImpl::Type::ROWCOUNT) {
        if (result.has_series()) {
            take_new_index(*result.mutable_series()->mutable_common(), other.series().common());
        } else if (result.has_df()) {
            take_new_index(*result.mutable_df()->mutable_common(), other.df().common());
        }
    } else if (index_type == IndexDescriptorImpl::Type::TIMESTAMP) {
        if (result.has_series()) {
            overwrite_index_tz(*result.mutable_series()->mutable_common(), other.series().common());
        } else if (result.has_df()) {
            overwrite_index_tz(*result.mutable_df()->mutable_common(), other.df().common());
        }
    }
    return result;
}

NormalizationMetadata reconcile_merge_norm_metadata(
        const NormalizationMetadata& base, const NormalizationMetadata& other,
        std::unordered_set<size_t>& non_matching_name_indices
) {
    auto result = accumulate_norm_metadata(base, other, non_matching_name_indices);
    // Apply accumulated fake positions to the output multi-index (see generate_norm_meta).
    auto* common = result.has_series() ? result.mutable_series()->mutable_common()
                   : result.has_df()   ? result.mutable_df()->mutable_common()
                                       : nullptr;
    if (common && common->has_multi_index()) {
        auto* index = common->mutable_multi_index();
        for (auto pos : index->fake_field_pos()) {
            non_matching_name_indices.insert(pos);
        }
        index->clear_fake_field_pos();
        for (auto idx : non_matching_name_indices) {
            index->add_fake_field_pos(idx);
        }
        if (non_matching_name_indices.contains(0)) {
            index->set_name("index");
        }
    }
    return result;
}

} // namespace

OutputSchema combine_schema(const OutputSchema& base, const OutputSchema& other, const SchemaCombineOptions& options) {
    const auto& base_desc = base.stream_descriptor();
    const auto& other_desc = other.stream_descriptor();
    const bool allow_empty_defer = options.name_mismatch == RequiredNameMismatchPolicy::Raise;
    const auto index_desc = combine_index_descriptor(base_desc.index(), other_desc.index(), allow_empty_defer);

    StreamDescriptor out{StreamId{}, index_desc};
    const auto required_fields = required_fields_count(base_desc, base.norm_metadata_);
    auto non_matching_name_indices = add_required_fields(out, base_desc, other_desc, required_fields, options);
    add_data_columns(out, base_desc, other_desc, required_fields, options);

    NormalizationMetadata norm;
    if (options.name_mismatch == RequiredNameMismatchPolicy::ReconcileToFake) {
        norm = reconcile_merge_norm_metadata(base.norm_metadata_, other.norm_metadata_, non_matching_name_indices);
    } else {
        norm = append_merge_norm_metadata(base.norm_metadata_, other.norm_metadata_, index_desc.type());
    }
    return OutputSchema{std::move(out), std::move(norm)};
}

} // namespace arcticdb
