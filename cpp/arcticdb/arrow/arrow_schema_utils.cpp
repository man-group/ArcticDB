/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <google/protobuf/util/message_differencer.h>

#include <arcticdb/arrow/arrow_schema_utils.hpp>

namespace arcticdb {

constexpr std::string_view multiindex_prefix{"__idx__"};

using StringSet = ankerl::unordered_dense::set<std::string, util::TransparentStringHash, std::equal_to<>>;
using StringMap = ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>;

size_t count_indexes(const proto::descriptors::NormalizationMetadata_Pandas& pandas_common) {
    if (pandas_common.has_index()) {
        return (pandas_common.index().is_physically_stored() || pandas_common.index().step() == 0) ? 1 : 0;
    } else { // multiindex
        return pandas_common.multi_index().field_count() + 1;
    }
}

StringSet generate_original_index_names(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const size_t index_count
) {
    StringSet original_index_names;
    if (index_count == 1) {
        const auto& index_meta = pandas_common.index();
        if (!index_meta.fake_name()) {
            original_index_names.insert(index_meta.name());
        }
    } else if (index_count > 1) {
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<int> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < index_count; ++index_col_idx) {
            if (!fake_field_pos.contains(index_col_idx)) {
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
                const auto& col_data = it->second;
                if (col_data.original_name() != field->name()) {
                    original_column_names.insert(col_data.original_name());
                } else {
                    original_column_names.insert(field->name());
                }
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

StringMap generate_index_renames(
        const size_t index_count, StringSet& taken_column_names,
        const proto::descriptors::NormalizationMetadata_Pandas& pandas_common, const FieldCollection& fields,
        const std::optional<std::vector<std::string>>& index_columns, const StringSet& original_column_names
) {
    StringMap index_renames;
    if (pandas_common.has_index()) {
        const auto& index_meta = pandas_common.index();
        // Use this condition over is_physically_stored() as this is incorrect for zero-row dataframes
        if (index_count == 1) {
            const std::string new_name = [&]() {
                if (index_columns.has_value()) {
                    return index_columns->front();
                } else if (index_meta.fake_name()) {
                    return ensure_unique_name("__index__", original_column_names);
                } else {
                    return index_meta.name().empty() ? "__empty__" : index_meta.name();
                }
            }();
            taken_column_names.insert(new_name);
            insert_if_non_matching(index_renames, fields.at(0).name(), new_name);
        }
    } else { // multiindex
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<int> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < index_count; ++index_col_idx) {
            const std::string new_name = [&]() {
                if (index_columns.has_value()) {
                    return index_columns->at(index_col_idx);
                } else if (fake_field_pos.contains(index_col_idx)) {
                    return ensure_unique_name(fmt::format("__index_level_{}__", index_col_idx), original_column_names);
                } else if (index_col_idx == 0) {
                    return multi_index_meta.name();
                } else {
                    return ensure_unique_name(
                            fields.at(index_col_idx).name().substr(multiindex_prefix.size()), taken_column_names
                    );
                }
            }();
            taken_column_names.insert(new_name);
            insert_if_non_matching(
                    index_renames,
                    fields.at(index_col_idx).name(),
                    index_col_idx == 0 ? new_name : fmt::format("{}{}", multiindex_prefix, new_name)
            );
        }
    }
    return index_renames;
}

StringMap generate_column_renames(
        const FieldCollection& fields, const proto::descriptors::NormalizationMetadata_Pandas& pandas_common,
        const std::optional<std::vector<std::string>>& index_columns, const size_t index_count,
        const bool unnamed_series, const StringSet& original_column_names
) {
    StringSet taken_column_names;
    StringMap res = generate_index_renames(
            index_count, taken_column_names, pandas_common, fields, index_columns, original_column_names
    );
    auto field = fields.begin();
    std::advance(field, index_count);
    for (; field != fields.end(); ++field) {
        if (unnamed_series || pandas_common.col_names().contains(field->name())) {
            auto new_name = [&]() -> std::string {
                if (unnamed_series) {
                    return "None";
                } else if (pandas_common.col_names().at(field->name()).is_none()) {
                    return "None";
                } else if (pandas_common.col_names().at(field->name()).is_empty()) {
                    return "__empty__";
                } else if (pandas_common.col_names().at(field->name()).is_int()) {
                    return pandas_common.col_names().at(field->name()).original_name();
                } else if (pandas_common.col_names().at(field->name()).original_name() != field->name()) {
                    return pandas_common.col_names().at(field->name()).original_name();
                } else {
                    return std::string(field->name());
                }
            }();
            new_name = ensure_unique_name(new_name, taken_column_names);
            taken_column_names.insert(new_name);
            insert_if_non_matching(res, field->name(), new_name);
        }
    }
    return res;
}

FieldCollection generate_output_fields(const FieldCollection& input_fields, const StringMap& column_renames) {
    FieldCollection output_fields;
    StringSet final_col_names;
    for (auto input_field = input_fields.begin(); input_field != input_fields.end(); ++input_field) {
        const auto final_col_name = [&]() -> std::string {
            if (auto it = column_renames.find(std::string(input_field->name())); it != column_renames.end()) {
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
    auto& output_pandas_common =
            *(output_norm.has_df() ? output_norm.mutable_df()->mutable_common()
                                   : output_norm.mutable_series()->mutable_common());
    output_pandas_common.clear_col_names();
    if (output_norm.has_series()) {
        auto& series_meta = *output_norm.mutable_series();
        series_meta.set_has_synthetic_columns(false);
        output_pandas_common.set_has_name(true);
        output_pandas_common.set_name(std::string(output_fields.at(output_fields.size() - 1).name()));
    } else {
        auto& df_meta = *output_norm.mutable_df();
        df_meta.set_has_synthetic_columns(false);
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
    const auto& pandas_common = norm.has_df() ? norm.df().common() : norm.series().common();
    const bool unnamed_series = norm.has_series() && (!pandas_common.has_name() && pandas_common.name().empty());
    const auto index_count = count_indexes(pandas_common);
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !index_columns.has_value() || index_columns->size() == index_count,
            "Symbol has {} index levels, but {} index names were provided",
            index_count,
            index_columns->size()
    );
    const bool range_index{index_count == 0};
    auto original_index_names = generate_original_index_names(desc.fields(), pandas_common, index_count);
    auto original_column_names =
            generate_original_column_names(desc.fields(), pandas_common, index_count, unnamed_series);
    if (index_columns.has_value()) {
        for (const auto& index_column : *index_columns) {
            user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    !original_column_names.contains(index_column),
                    "Provided index colum name {} already exists in the data columns",
                    index_column
            );
        }
    } else {
        original_column_names.insert(
                std::make_move_iterator(original_index_names.begin()),
                std::make_move_iterator(original_index_names.end())
        );
    }
    auto column_renames = generate_column_renames(
            desc.fields(), pandas_common, index_columns, index_count, unnamed_series, original_column_names
    );
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