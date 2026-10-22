/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <arcticdb/arrow/arrow_schema_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/entity/arrow_pandas_norm.hpp>
#include <arcticdb/entity/normalization_utils.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace arcticdb {

constexpr std::string_view multiindex_prefix{"__idx__"};

using StringSet = ankerl::unordered_dense::set<std::string, util::TransparentStringHash, std::equal_to<>>;
using StringMap = ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>;

namespace {

StringSet generate_original_index_names(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const pipelines::index::RequiredFieldInfo& index_info
) {
    StringSet original_index_names;
    if (index_info.num_physical_indices == 1 && !index_info.has_multi_index) {
        const auto& index_meta = pandas_common.index();
        if (!index_meta.fake_name()) {
            original_index_names.insert(index_meta.name());
        }
    } else if (index_info.has_multi_index) {
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<int> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < index_info.num_physical_indices; ++index_col_idx) {
            if (!fake_field_pos.contains(static_cast<int32_t>(index_col_idx))) {
                if (index_col_idx == 0) {
                    original_index_names.insert(multi_index_meta.name());
                } else {
                    original_index_names.insert(fields.at(index_col_idx).name().substr(multiindex_prefix.size()));
                }
            }
        }
    }
    return original_index_names;
}

StringSet generate_original_column_names(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const size_t index_count, const bool unnamed_series

) {
    StringSet original_column_names;
    if (unnamed_series) {
        original_column_names.insert("");
    } else {
        auto field = fields.begin();
        std::advance(field, index_count);
        for (; field != fields.end(); ++field) {
            if (auto it = pandas_common.col_names().find(field->name()); it != pandas_common.col_names().end()) {
                original_column_names.insert(it->second.original_name());
            } else {
                original_column_names.insert(field->name());
            }
        }
    }
    return original_column_names;
}

std::string ensure_unique_name(std::string_view candidate_name, const StringSet& existing_names) {
    std::string res{candidate_name};
    while (existing_names.contains(res)) {
        res = fmt::format("_{}_", res);
    }
    return res;
}

void insert_if_non_matching(StringMap& map, std::string_view key, std::string_view value) {
    if (key != value) {
        map[key] = value;
    }
}

void append_index_output_names(
        const size_t index_count, StringSet& taken_column_names,
        const proto::descriptors::NormalizationMetadata_Pandas& pandas_common, const FieldCollection& fields,
        const std::optional<std::vector<std::string>>& index_columns, const StringSet& original_column_names,
        std::vector<std::string>& output_names
) {
    if (pandas_common.has_index()) {
        const auto& index_meta = pandas_common.index();
        // Use this condition over is_physically_stored() as this is incorrect for zero-row dataframes
        if (index_count == 1) {
            std::string new_name = [&]() {
                if (index_columns.has_value()) {
                    return index_columns->front();
                } else if (index_meta.fake_name()) {
                    return ensure_unique_name("__index__", original_column_names);
                } else {
                    return index_meta.name().empty() ? "__empty__" : index_meta.name();
                }
            }();
            taken_column_names.insert(new_name);
            output_names.emplace_back(std::move(new_name));
        }
    } else { // multiindex
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<size_t> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < index_count; ++index_col_idx) {
            std::string new_name = [&]() {
                if (index_columns.has_value()) {
                    return index_columns->at(index_col_idx);
                } else if (fake_field_pos.contains(index_col_idx)) {
                    // Here, we are creating a new column name out of thin-air, so we compare to the original column
                    // names to make sure there is no clash with an actual name given by the user.
                    return ensure_unique_name(fmt::format("__index_level_{}__", index_col_idx), original_column_names);
                } else if (index_col_idx == 0) {
                    return multi_index_meta.name().empty() ? "__empty__" : multi_index_meta.name();
                } else {
                    // We now compare to taken_column_names (which was empty on entry to this function) to handle cases
                    // like a multiindex where all index levels had the same name, so that ["a", "a"] -> ["a", "_a_"]
                    const auto level_name = fields.at(index_col_idx).name().substr(multiindex_prefix.size());
                    return ensure_unique_name(level_name.empty() ? "__empty__" : level_name, taken_column_names);
                }
            }();
            taken_column_names.insert(new_name);
            output_names.emplace_back(std::move(new_name));
        }
    }
}

// Output names are returned by position, instead of keyed by on-disk name because we can have duplicate on-disk names:
// E.g. an input dataframe with multiindex ["5", "lvl2"] and column names [5] (note int not string) will get written
// with on disk names: ["5", "__idx__lvl2", "5"]
std::vector<std::string> generate_output_names(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const std::optional<std::vector<std::string>>& index_columns, const size_t index_count,
        const bool unnamed_series, const StringSet& original_column_names
) {
    auto output_names = util::reserve_vector<std::string>(fields.size());
    StringSet taken_column_names;
    append_index_output_names(
            index_count, taken_column_names, pandas_common, fields, index_columns, original_column_names, output_names
    );
    auto field = fields.begin();
    std::advance(field, index_count);
    for (; field != fields.end(); ++field) {
        auto new_name = [&]() -> std::string {
            if (unnamed_series || pandas_common.col_names().contains(field->name())) {
                if (unnamed_series || pandas_common.col_names().at(field->name()).is_none()) {
                    return "None";
                } else if (pandas_common.col_names().at(field->name()).is_empty()) {
                    return "__empty__";
                } else {
                    return pandas_common.col_names().at(field->name()).original_name();
                }
            } else {
                return std::string(field->name());
            }
        }();
        new_name = ensure_unique_name(new_name, taken_column_names);
        taken_column_names.insert(new_name);
        output_names.emplace_back(std::move(new_name));
    }
    return output_names;
}

FieldCollection generate_output_fields(const FieldCollection& input_fields, const StringMap& column_renames) {
    FieldCollection output_fields;
    StringSet final_col_names;
    for (auto input_field = input_fields.begin(); input_field != input_fields.end(); ++input_field) {
        const auto final_col_name = [&]() -> std::string {
            if (auto it = column_renames.find(input_field->name()); it != column_renames.end()) {
                return it->second;
            } else {
                return std::string{input_field->name()};
            }
        }();
        internal::check<ErrorCode::E_NOT_SUPPORTED>(
                final_col_names.insert(final_col_name).second,
                "Column name {} appears multiple times in the input descriptor",
                final_col_name
        );
        output_fields.add_field(input_field->type(), final_col_name);
    }
    return output_fields;
}

proto::descriptors::NormalizationMetadata generate_output_norm(
        const FieldCollection& output_fields, const proto::descriptors::NormalizationMetadata& input_norm,
        const size_t index_count, const bool range_index
) {
    proto::descriptors::NormalizationMetadata output_norm = input_norm;
    auto& output_pandas_common = *mutable_pandas_common(output_norm);
    output_pandas_common.clear_col_names();
    if (output_norm.has_series()) {
        auto& series_meta = *output_norm.mutable_series();
        series_meta.set_has_synthetic_columns(false);
        output_pandas_common.set_has_name(true);
        output_pandas_common.set_name(std::string(output_fields.at(output_fields.size() - 1).name()));
    } else if (output_norm.has_df()) {
        auto& df_meta = *output_norm.mutable_df();
        df_meta.set_has_synthetic_columns(false);
    } else { // ts
        auto& ts_meta = *output_norm.mutable_ts();
        ts_meta.set_has_synthetic_columns(false);
    }
    if (output_pandas_common.has_index()) {
        auto& index_meta = *output_pandas_common.mutable_index();
        if (index_count == 1) {
            index_meta.set_name(std::string(output_fields.begin()->name()));
            index_meta.set_fake_name(false);
            index_meta.set_is_int(false);
        }
    } else {
        auto& multi_index_meta = *output_pandas_common.mutable_multi_index();
        multi_index_meta.set_name(std::string(output_fields.begin()->name()));
        multi_index_meta.clear_fake_field_pos();
        multi_index_meta.set_is_int(false);
    }
    auto field = output_fields.begin();
    std::advance(field, range_index ? 0 : 1);
    for (; field != output_fields.end(); ++field) {
        (*output_pandas_common.mutable_col_names())[field->name()].set_original_name(std::string(field->name()));
    }
    return output_norm;
}

proto::descriptors::NormalizationMetadata_Pandas_ColumnName pandas_label(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const size_t index_count, const bool unnamed_series, const size_t pos
) {
    proto::descriptors::NormalizationMetadata_Pandas_ColumnName label;
    const auto set_name = [&label](std::string_view name) {
        if (name.empty()) {
            label.set_is_empty(true);
        } else {
            label.set_original_name(std::string{name});
        }
    };
    const auto stored_name = fields.at(pos).name();
    if (pos >= index_count) {
        if (unnamed_series) {
            label.set_is_none(true);
        } else if (auto it = pandas_common.col_names().find(stored_name); it != pandas_common.col_names().end()) {
            label = it->second;
        } else {
            label.set_original_name(std::string{stored_name});
        }
    } else if (pandas_common.has_index()) {
        if (pandas_common.index().fake_name()) {
            label.set_is_none(true);
        } else {
            set_name(pandas_common.index().name());
        }
    } else {
        const auto& multi_index_meta = pandas_common.multi_index();
        if (std::ranges::find(multi_index_meta.fake_field_pos(), pos) != multi_index_meta.fake_field_pos().end()) {
            label.set_is_none(true);
        } else if (pos == 0) {
            set_name(multi_index_meta.name());
        } else {
            set_name(stored_name.substr(multiindex_prefix.size()));
        }
    }
    return label;
}

} // namespace

void prepare_for_arrow_output(StreamDescriptor& desc, proto::descriptors::NormalizationMetadata& norm) {
    if (is_pandas_convertible_to_arrow(norm)) {
        norm = arrow_norm_from_pandas(norm, desc);
    }
    if (!norm.has_experimental_arrow()) {
        return;
    }
    auto& arrow = *norm.mutable_experimental_arrow();
    auto* common = mutable_embedded_pandas_common(arrow);
    if (common == nullptr) {
        return;
    }
    const auto& fields = desc.fields();
    const bool unnamed_series =
            arrow.pandas_input_type_case() == proto::descriptors::NormalizationMetadata_ExperimentalArrow::kSeries &&
            !common->has_name() && common->name().empty();
    const auto index_info = pipelines::index::required_fields_info(desc, norm);
    const auto index_count = index_info.num_physical_indices;
    auto original_column_names = generate_original_column_names(fields, *common, index_count, unnamed_series);
    for (auto&& name : generate_original_index_names(fields, *common, index_info)) {
        original_column_names.insert(std::move(name));
    }
    const auto output_names =
            generate_output_names(fields, *common, std::nullopt, index_count, unnamed_series, original_column_names);
    auto output_fields = std::make_shared<FieldCollection>(fields.size(), fields.buffer().bytes());
    google::protobuf::Map<std::string, proto::descriptors::NormalizationMetadata_Pandas_ColumnName> col_names;
    google::protobuf::Map<std::string, proto::descriptors::NormalizationMetadata_ExperimentalArrow_ColumnMeta> columns;
    for (size_t pos = 0; pos < fields.size(); ++pos) {
        const auto& output_name = output_names[pos];
        output_fields->add_field(fields.at(pos).type(), output_name);
        auto label = pandas_label(fields, *common, index_count, unnamed_series, pos);
        if (label.is_none() || label.is_empty() || label.is_int() || label.original_name() != output_name) {
            col_names[output_name] = std::move(label);
        }
        if (auto it = arrow.columns().find(fields.at(pos).name()); it != arrow.columns().end()) {
            columns[output_name] = it->second;
        }
    }
    common->mutable_col_names()->swap(col_names);
    arrow.mutable_columns()->swap(columns);
    desc.set_fields(std::move(output_fields));
}

void prepare_for_arrow_output(SegmentInMemory& segment, proto::descriptors::NormalizationMetadata& norm) {
    // A copy, as other segments may share the descriptor
    auto desc = std::make_shared<StreamDescriptor>(segment.descriptor());
    prepare_for_arrow_output(*desc, norm);
    segment.attach_descriptor(std::move(desc));
}

std::optional<ArrowTransformedSchema> make_schema_arrow_compatible(
        const OutputSchema& input_schema, const std::optional<std::vector<std::string>>& index_columns
) {
    const auto& norm = input_schema.norm_metadata_;
    if (norm.has_experimental_arrow()) {
        return std::nullopt;
    }
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_PICKLED_DATA>(
            !norm.has_msg_pack_frame(), "rename_columns_arrow_compat not supported with pickled data"
    );
    schema::check<ErrorCode::E_OPERATION_NOT_SUPPORTED_WITH_NUMPY_ARRAY>(
            !norm.has_np(), "rename_columns_arrow_compat not supported with numpy arrays"
    );
    const auto& desc = input_schema.stream_descriptor();
    const auto& input_pandas_common = *pandas_common(norm);
    const bool unnamed_series =
            norm.has_series() && (!input_pandas_common.has_name() && input_pandas_common.name().empty());
    const auto index_info = pipelines::index::required_fields_info(desc, norm);
    const auto index_count = index_info.num_physical_indices;
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !index_columns.has_value() || index_columns->size() == index_count,
            "Symbol has {} index levels, but {} index names were provided",
            index_count,
            index_columns.has_value() ? index_columns->size() : 0
    );
    const bool range_index{index_count == 0};
    auto original_index_names = generate_original_index_names(desc.fields(), input_pandas_common, index_info);
    auto original_column_names =
            generate_original_column_names(desc.fields(), input_pandas_common, index_count, unnamed_series);
    if (index_columns.has_value()) {
        for (const auto& index_column : *index_columns) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    !original_column_names.contains(index_column),
                    "Provided index column name {} already exists in the data columns",
                    index_column
            );
        }
        if (norm.has_ts()) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    index_columns->front() == "times",
                    "Cannot rename NormalisedTimeSeries index to {}, must always be \"times\"",
                    index_columns->front()
            );
        }
    } else {
        original_column_names.insert(
                std::make_move_iterator(original_index_names.begin()),
                std::make_move_iterator(original_index_names.end())
        );
    }
    const auto output_names = generate_output_names(
            desc.fields(), input_pandas_common, index_columns, index_count, unnamed_series, original_column_names
    );
    StringMap column_renames;
    for (size_t pos = 0; pos < output_names.size(); ++pos) {
        const bool is_multiindex_level = pos > 0 && pos < index_count;
        insert_if_non_matching(
                column_renames,
                desc.field(pos).name(),
                is_multiindex_level ? fmt::format("{}{}", multiindex_prefix, output_names[pos]) : output_names[pos]
        );
    }
    auto output_fields = generate_output_fields(desc.fields(), column_renames);
    StreamDescriptor output_desc{
            desc.data_ptr(), std::make_shared<FieldCollection>(std::move(output_fields)), desc.stream_id_
    };
    auto output_norm = generate_output_norm(output_desc.fields(), norm, index_count, range_index);
    const bool changed =
            !column_renames.empty() || !google::protobuf::util::MessageDifferencer::Equals(output_norm, norm);
    if (changed) {
        return ArrowTransformedSchema{{std::move(output_desc), std::move(output_norm)}, column_renames};
    } else {
        return std::nullopt;
    }
}

} // namespace arcticdb