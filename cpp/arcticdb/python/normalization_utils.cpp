/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <arcticdb/python/normalization_utils.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/pb_util.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#undef GetMessage // defined as GetMessageA on Windows

namespace arcticdb {
using namespace proto::descriptors;

template<typename InnerFunction, typename FieldType = google::protobuf::FieldDescriptor*>
auto get_pandas_common_via_reflection(
        proto::descriptors::NormalizationMetadata norm_meta, InnerFunction&& inner_function
) -> decltype(inner_function(norm_meta, std::declval<FieldType>(), std::declval<FieldType>())) {
    try {
        if (norm_meta.input_type_case() != proto::descriptors::NormalizationMetadata::INPUT_TYPE_NOT_SET) {
            if (auto one_of =
                        proto::descriptors::NormalizationMetadata::descriptor()->field(norm_meta.input_type_case());
                one_of) {
                log::storage().info(
                        "Inefficient NormalizationMetadata.input_type.{} access via reflection", one_of->name()
                );
                if (auto msg_type = one_of->message_type(); msg_type) {
                    if (auto common_field = msg_type->FindFieldByName("common"); common_field) {
                        normalization::check<ErrorCode::E_UNIMPLEMENTED_INPUT_TYPE>(
                                common_field->message_type() ==
                                        proto::descriptors::NormalizationMetadata::Pandas::descriptor(),
                                "{}.common must be Pandas",
                                one_of->name()
                        );
                        return inner_function(norm_meta, one_of, common_field);
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        log::storage().info("get_common_pandas() reflection exception: {}", e.what());
    }
    log::storage().warn("New NormalizationMetadata.input_type access failure. Cannot check.");
    return std::nullopt;
}

std::optional<std::decay_t<std::reference_wrapper<const arcticdb::proto::descriptors::NormalizationMetadata_Pandas>>>
get_common_pandas(const proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = const arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.df().common()));
    case proto::descriptors::NormalizationMetadata::kSeries:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.series().common()));
    case proto::descriptors::NormalizationMetadata::kTs:
        return std::make_optional(std::reference_wrapper<Pandas>(norm_meta.ts().common()));
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp:
    case proto::descriptors::NormalizationMetadata::kExperimentalArrow:
        return std::nullopt;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return std::make_optional(std::reference_wrapper<Pandas>(
                    *reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg))
            ));
        });
    }
}

std::optional<std::decay_t<std::reference_wrapper<arcticdb::proto::descriptors::NormalizationMetadata_Pandas>>>
get_common_pandas(proto::descriptors::NormalizationMetadata& norm_meta) {
    using Pandas = arcticdb::proto::descriptors::NormalizationMetadata_Pandas;
    switch (norm_meta.input_type_case()) {
    case proto::descriptors::NormalizationMetadata::kDf:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_df()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kSeries:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_series()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kTs:
        return std::make_optional(std::reference_wrapper<Pandas>(*norm_meta.mutable_ts()->mutable_common()));
    case proto::descriptors::NormalizationMetadata::kMsgPackFrame:
    case proto::descriptors::NormalizationMetadata::kNp:
    case proto::descriptors::NormalizationMetadata::kExperimentalArrow:
        return std::nullopt;
    default:
        return get_pandas_common_via_reflection(norm_meta, [](auto& norm_meta, auto one_of, auto common_field) {
            auto& one_of_msg = norm_meta.GetReflection()->GetMessage(norm_meta, one_of);
            auto& common_msg = one_of_msg.GetReflection()->GetMessage(one_of_msg, common_field);
            return std::make_optional(std::reference_wrapper<Pandas>(
                    *reinterpret_cast<Pandas*>(const_cast<::google::protobuf::Message*>(&common_msg))
            ));
        });
    }
}

/// In case both indexes are row-ranged sanity checks will be performed:
/// * Both indexes must have the same step
/// * The new index must start at the point where the old one ends
/// If the checks above pass update the new normalization index so that it spans the whole index (old + new)
/// @throws In case the row-ranged indexes are incompatible
void update_rowcount_normalization_data(
        const proto::descriptors::NormalizationMetadata& old_norm, proto::descriptors::NormalizationMetadata& new_norm,
        size_t old_length
) {
    const auto old_pandas = get_common_pandas(old_norm);
    const auto new_pandas = get_common_pandas(new_norm);
    const auto* old_index = old_pandas->get().has_index() ? &old_pandas->get().index() : nullptr;
    const auto* new_index = new_pandas->get().has_index() ? &new_pandas->get().index() : nullptr;
    if (old_index) {
        constexpr auto error_suffix =
                " the existing version. Please convert both to use Int64Index if you need this to work.";

        util::check(new_index != nullptr, "New index is null in normalization checks");
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                old_index->is_physically_stored() == new_index->is_physically_stored(),
                "The argument uses a {} index which is incompatible with {}",
                new_index->is_physically_stored() ? "non-range" : "range-style",
                error_suffix
        );

        if (!old_index->is_physically_stored()) {
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                    old_index->step() == new_index->step(),
                    "The new argument has a different RangeIndex step from {}",
                    error_suffix
            );

            size_t new_start = new_index->start();
            auto stop = old_index->start() + old_length * old_index->step();
            normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                    new_start == stop || (new_start == 0 && new_index->step() == 1),
                    "The appending data has a RangeIndex.start={} that is not contiguous with the "
                    "stop ({}) of {}",
                    new_start,
                    stop,
                    error_suffix
            );

            new_pandas->get().mutable_index()->set_start(old_index->start());
        }
    }
}

bool check_pandas_like(
        const proto::descriptors::NormalizationMetadata& old_norm, proto::descriptors::NormalizationMetadata& new_norm
) {
    auto old_pandas = get_common_pandas(old_norm);
    auto new_pandas = get_common_pandas(new_norm);
    if (old_pandas || new_pandas) {
        normalization::check<ErrorCode::E_UPDATE_NOT_SUPPORTED>(
                old_pandas && new_pandas,
                "Currently only supports modifying existing Pandas data with Pandas.\nexisting={}\nargument={}",
                util::newlines_to_spaces(old_norm),
                util::newlines_to_spaces(new_norm)
        );

        const auto* old_index = old_pandas->get().has_index() ? &old_pandas->get().index() : nullptr;
        const auto* new_index = new_pandas->get().has_index() ? &new_pandas->get().index() : nullptr;
        normalization::check<ErrorCode::E_INCOMPATIBLE_INDEX>(
                static_cast<bool>(old_index) == static_cast<bool>(new_index),
                "The argument has an index type incompatible with the existing version:\nexisting={}\nargument={}",
                util::newlines_to_spaces(old_norm),
                util::newlines_to_spaces(new_norm)
        );
        // FUTURE: check PandasMultiIndex and many other descriptor types. Might be more efficiently implemented using
        // some structural comparison lib or do it via Python
        return true;
    }
    return false;
}

template<class NormalizationMetadata>
bool check_ndarray_append(const NormalizationMetadata& old_norm, NormalizationMetadata& new_norm) {
    if (old_norm.has_np() || new_norm.has_np()) {
        normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
                old_norm.has_np() && new_norm.has_np(), "Currently, can only append numpy.ndarray to each other."
        );
        const auto& old_shape = old_norm.np().shape();
        const auto& new_shape = new_norm.mutable_np()->shape();
        normalization::check<ErrorCode::E_WRONG_SHAPE>(
                !new_shape.empty(), "Append input has invalid normalization metadata (empty shape)"
        );
        normalization::check<ErrorCode::E_WRONG_SHAPE>(
                std::equal(old_shape.begin() + 1, old_shape.end(), new_shape.begin() + 1, new_shape.end()),
                "The appending NDArray must have the same shape as the existing (excl. the first dimension)"
        );
        return true;
    }
    return false;
}

void check_arrow_column_metadata(
        const proto::descriptors::NormalizationMetadata::ExperimentalArrow& old_norm,
        proto::descriptors::NormalizationMetadata::ExperimentalArrow& new_norm, bool dynamic_schema
) {
    if (!dynamic_schema) {
        const auto& old_col_meta_map = old_norm.columns();
        auto& new_col_meta_map = *new_norm.mutable_columns();
        // Column metadata must match exactly
        bool should_raise{old_col_meta_map.size() != new_col_meta_map.size()};
        if (!should_raise) {
            for (const auto& [col_name, old_col_meta] : old_col_meta_map) {
                if (auto it = new_col_meta_map.find(col_name); it == new_col_meta_map.end()) {
                    should_raise = true;
                    break;
                } else {
                    if (!google::protobuf::util::MessageDifferencer::Equals(old_col_meta, it->second)) {
                        should_raise = true;
                        break;
                    }
                }
            }
        }
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                !should_raise, "Column metadata does not match between existing data and input frame"
        );
    }
}

void fix_normalization_or_throw(
        bool is_append, const pipelines::index::IndexSegmentReader& existing_isr,
        const pipelines::InputFrame& new_frame, bool dynamic_schema
) {
    auto& old_norm = existing_isr.tsd().proto().normalization();
    auto& new_norm = new_frame.norm_meta;
    normalization::check<ErrorCode::E_INCOMPATIBLE_OBJECTS>(
            old_norm.input_type_case() == new_frame.norm_meta.input_type_case(),
            "{} can be performed only on objects of the same type. Existing type is {} new type is {}.",
            is_append ? "Append" : "Update",
            old_norm.input_type_case(),
            new_frame.norm_meta.input_type_case()
    );
    if (check_pandas_like(old_norm, new_norm)) {
        const IndexDescriptor::Type old_index_type = existing_isr.tsd().index().type();
        const IndexDescriptor::Type new_index_type = new_frame.desc().index().type();
        if (old_index_type == new_index_type && old_index_type == IndexDescriptor::Type::ROWCOUNT) {
            update_rowcount_normalization_data(old_norm, new_norm, existing_isr.tsd().total_rows());
        }
        return;
    }
    if (is_append) {
        if (check_ndarray_append(old_norm, new_norm))
            return;
    } else {
        // ndarray normalizes to a ROWCOUNT frame and we don't support update on those
        normalization::check<ErrorCode::E_UPDATE_NOT_SUPPORTED>(
                !old_norm.has_np() && !new_norm.has_np(), "current normalization scheme doesn't allow update of ndarray"
        );
    }
    if (old_norm.has_experimental_arrow() && new_norm.has_experimental_arrow()) {
        check_arrow_column_metadata(
                old_norm.experimental_arrow(), *new_norm.mutable_experimental_arrow(), dynamic_schema
        );
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

NormalizationMetadata accumulate_norm_metadata(
        const NormalizationMetadata& accumulated, const NormalizationMetadata& other,
        std::unordered_set<size_t>& fake_field_pos_acc
) {
    // Arrow + Arrow
    // Arrow schemas are compatible as long as both have the same `has_index`.
    // We return the `accumulated` metadata if compatible.
    if (accumulated.has_experimental_arrow() && other.has_experimental_arrow()) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                accumulated.experimental_arrow().has_index() == other.experimental_arrow().has_index(),
                "Cannot join indexed arrow data with unindexed arrow data"
        );
        return accumulated;
    }

    // One arrow, one pandas
    // Pandas norm metadata is preferred over arrow when mixing, because it carries more detail
    // (index name, timezone, range index params, etc.). Arrow and pandas are compatible when:
    //   arrow.has_index() == pandas.index().is_physically_stored()
    if (accumulated.has_experimental_arrow() || other.has_experimental_arrow()) {
        const auto& arrow_meta = accumulated.has_experimental_arrow() ? accumulated : other;
        const auto& pandas_meta = accumulated.has_experimental_arrow() ? other : accumulated;
        const auto& common = pandas_meta.has_series() ? pandas_meta.series().common() : pandas_meta.df().common();
        // TODO: When arrow normalization metadata is finalized we can consider allowing
        // concat(arrow,pandas_with_multiindex)
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                common.has_index(), "Cannot join arrow-written data with multi-indexed pandas data"
        );
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                arrow_meta.experimental_arrow().has_index() == common.index().is_physically_stored(),
                "Cannot join unindexed with indexed data"
        );
        return pandas_meta;
    }

    // Pandas + Pandas
    // Ensure:
    // Both are Series or both are DataFrames
    // Both have PandasIndex or both have PandasMultiIndex
    // If PandasIndex:
    //  - name/is_int/fake_name - if all the same maintain, otherwise "index"/false/true
    //  - tz - if all the same maintain, otherwise empty string
    //  - is_physically stored must all be the same
    //  - RangeIndex
    //    - start==0/step==1 - maintain
    //    - All steps the same, use start from first schema and maintain step
    //    - Otherwise, log warning, set start==0/step==1
    // If PandasMultiIndex:
    //  - name/is_int - if all the same maintain, otherwise "index"/false
    //  - field_count must all be same
    //  - tz - if all the same maintain, otherwise empty string
    //  - fake_field_pos - added to fake_field_pos_acc
    //      fake_field_pos.contains(0) serves same purpose as fake_name flag on PandasIndex
    //  - timezone - Like tz, for a given key all values must be same, otherwise set value to empty string
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

NormalizationMetadata generate_norm_meta(
        const std::vector<OutputSchema>& input_schemas, std::unordered_set<size_t>&& non_matching_name_indices
) {
    util::check(!input_schemas.empty(), "Cannot join empty list of schemas");
    for (const auto& schema : input_schemas) {
        schema::check<ErrorCode::E_DESCRIPTOR_MISMATCH>(
                schema.norm_metadata_.has_experimental_arrow() || schema.norm_metadata_.has_series() ||
                        schema.norm_metadata_.has_df(),
                "Multi-symbol joins only supported with Arrow, Series, and DataFrames"
        );
    }
    auto res = input_schemas.front().norm_metadata_;
    for (auto it = std::next(input_schemas.cbegin()); it != input_schemas.cend(); ++it) {
        res = accumulate_norm_metadata(res, it->norm_metadata_, non_matching_name_indices);
    }
    // Apply accumulated non_matching_name_indices to the result multi-index.
    // non_matching_name_indices contains values from add_index_fields (index fields with mismatched names)
    // plus fake_field_pos accumulated from all schemas during pairwise merges above.
    auto* res_common = res.has_series() ? res.mutable_series()->mutable_common()
                       : res.has_df()   ? res.mutable_df()->mutable_common()
                                        : nullptr;
    if (res_common && res_common->has_multi_index()) {
        auto* index = res_common->mutable_multi_index();
        // Make sure the result fake_field_pos are also included. E.g. if input_schemas.size() == 1
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
    return res;
}
} // namespace arcticdb
