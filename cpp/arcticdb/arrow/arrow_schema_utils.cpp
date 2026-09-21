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

ArrowTransformedSchema make_schema_arrow_compatible(
        const OutputSchema& input_schema, ARCTICDB_UNUSED const std::optional<std::vector<std::string>>& index_columns
) {
    const auto& desc = input_schema.stream_descriptor();
    const auto& norm = input_schema.norm_metadata_;
    const auto& pandas_common = norm.has_df() ? norm.df().common() : norm.series().common();
    const bool unnamed_series = norm.has_series() && (!pandas_common.has_name() and pandas_common.name().empty());
    const auto pandas_indexes = [&pandas_common]() -> size_t {
        if (pandas_common.has_index()) {
            // TODO: Handle len(item) == 0 case from ArrowTableNormalizer.denormalize
            return pandas_common.index().is_physically_stored() ? 1 : 0;
        } else { // multiindex
            return pandas_common.multi_index().field_count() + 1;
        }
    }();
    ankerl::unordered_dense::set<std::string> original_column_names;
    if (pandas_indexes == 1) {
        const auto& index_meta = pandas_common.index();
        if (!index_meta.fake_name()) {
            original_column_names.insert(index_meta.name());
        }
    } else if (pandas_indexes > 1) {
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<int> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < pandas_indexes; ++index_col_idx) {
            if (!fake_field_pos.contains(index_col_idx)) {
                if (index_col_idx == 0) {
                    original_column_names.insert(multi_index_meta.name());
                } else {
                    auto raw_field_name = desc.field(index_col_idx).name();
                    // Strip "__idx__" prefix
                    std::string stripped_field_name{raw_field_name.substr(7)};
                    original_column_names.insert(stripped_field_name);
                }
            }
        }
    }
    auto field = desc.fields().begin();
    std::advance(field, pandas_indexes);
    for (; field != desc.fields().end(); ++field) {
        if (unnamed_series) {
            original_column_names.insert("");
        } else if (auto it = pandas_common.col_names().find(field->name()); it != pandas_common.col_names().end()) {
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

    ankerl::unordered_dense::map<std::string, std::string> column_renames;
    ankerl::unordered_dense::set<std::string> taken_column_names;
    if (pandas_common.has_index()) {
        const auto& index_meta = pandas_common.index();
        if (index_meta.fake_name()) {
            std::string new_name{"__index__"};
            while (original_column_names.contains(new_name)) {
                new_name = fmt::format("_{}_", new_name);
            }
            taken_column_names.insert(new_name);
            column_renames[std::string(desc.field(0).name())] = new_name;
        } else {
            // Is this really necessary?
            column_renames[std::string(desc.field(0).name())] = index_meta.name();
            taken_column_names.insert(index_meta.name());
        }
    } else { // multiindex
        const auto& multi_index_meta = pandas_common.multi_index();
        ankerl::unordered_dense::set<int> fake_field_pos(
                multi_index_meta.fake_field_pos().cbegin(), multi_index_meta.fake_field_pos().cend()
        );
        for (size_t index_col_idx = 0; index_col_idx < pandas_indexes; ++index_col_idx) {
            if (fake_field_pos.contains(index_col_idx)) {
                auto new_name = fmt::format("__index_level_{}__", index_col_idx);
                while (original_column_names.contains(new_name)) {
                    new_name = fmt::format("_{}_", new_name);
                }
                taken_column_names.insert(new_name);
                column_renames[std::string(desc.field(index_col_idx).name())] = new_name;
            } else if (index_col_idx == 0) {
                column_renames[std::string(desc.field(index_col_idx).name())] = multi_index_meta.name();
                taken_column_names.insert(multi_index_meta.name());
            } else {
                std::string new_name{desc.field(index_col_idx).name().substr(7)};
                while (taken_column_names.contains(new_name)) {
                    new_name = fmt::format("_{}_", new_name);
                }
                taken_column_names.insert(new_name);
                column_renames[std::string(desc.field(index_col_idx).name())] = new_name;
            }
        }
    }

    field = desc.fields().begin();
    std::advance(field, pandas_indexes);
    for (; field != desc.fields().end(); ++field) {
        if (unnamed_series || pandas_common.col_names().contains(field->name())) {
            std::string new_name;
            if (unnamed_series) {
                new_name = "";
            } else if (pandas_common.col_names().at(field->name()).is_none()) {
                new_name = "None";
            } else if (pandas_common.col_names().at(field->name()).is_empty()) {
                // TODO: Use __empty__ here and in ArrowTableNormalizer.denormalize
                new_name = "";
            } else if (pandas_common.col_names().at(field->name()).is_int()) {
                new_name = pandas_common.col_names().at(field->name()).original_name();
            } else if (pandas_common.col_names().at(field->name()).original_name() != field->name()) {
                new_name = pandas_common.col_names().at(field->name()).original_name();
            } else {
                new_name = field->name();
            }
            while (taken_column_names.contains(new_name)) {
                new_name = fmt::format("_{}_", new_name);
            }
            taken_column_names.insert(new_name);
            column_renames[std::string(field->name())] = new_name;
        }
    }
    // TODO: Don't even add these in in the first place
    for (auto it = column_renames.begin(); it != column_renames.end();) {
        if (it->first == it->second) {
            it = column_renames.erase(it);
        } else {
            ++it;
        }
    }

    StreamDescriptor output_desc{desc.data_ptr(), std::make_shared<FieldCollection>(), desc.stream_id_};
    proto::descriptors::NormalizationMetadata output_norm = norm;
    auto& common =
            *(output_norm.has_df() ? output_norm.mutable_df()->mutable_common()
                                   : output_norm.mutable_series()->mutable_common());
    auto& new_fields = output_desc.fields();
    // TODO: Is calling add_field repeatedly super inefficient?
    for (auto old_field = desc.begin(); old_field != desc.end(); ++old_field) {
        if (auto it = column_renames.find(std::string(old_field->name())); it != column_renames.end()) {
            new_fields.add_field(old_field->type(), it->second);
            common.mutable_col_names()->erase(std::string(old_field->name()));
            (*common.mutable_col_names())[it->second].set_original_name(it->second);
        } else {
            new_fields.add_field(old_field->type(), old_field->name());
            auto& col_data = (*common.mutable_col_names())[old_field->name()];
            col_data.set_is_none(false);
            col_data.set_is_empty(false);
            col_data.set_original_name(std::string(old_field->name()));
            col_data.set_is_int(false);
        }
    }

    if (output_norm.has_series()) {
        auto& series_meta = *output_norm.mutable_series();
        if (series_meta.has_synthetic_columns()) {
            series_meta.set_has_synthetic_columns(false);
        }
        if (!common.has_name()) {
            common.set_has_name(true);
            common.set_name(std::string(std::prev(new_fields.end())->name()));
        }
    } else {
        auto& df_meta = *output_norm.mutable_df();
        if (df_meta.has_synthetic_columns()) {
            df_meta.set_has_synthetic_columns(false);
        }
    }
    if (common.has_index()) {
        auto& index_meta = *common.mutable_index();
        if (index_meta.is_physically_stored() && (index_meta.fake_name() || index_meta.is_int())) {
            index_meta.set_fake_name(false);
            index_meta.set_is_int(false);
            index_meta.set_name(std::string(new_fields.begin()->name()));
        }
    } else {
        auto& multi_index_meta = *common.mutable_multi_index();
        if (multi_index_meta.is_int() || !multi_index_meta.fake_field_pos().empty()) {
            multi_index_meta.set_is_int(false);
            multi_index_meta.clear_fake_field_pos();
            multi_index_meta.set_name(std::string(new_fields.begin()->name()));
        }
    }
    const bool changed =
            !column_renames.empty() || !google::protobuf::util::MessageDifferencer::Equals(output_norm, norm);
    return ArrowTransformedSchema{changed, {std::move(output_desc), std::move(output_norm)}, column_renames};
}

} // namespace arcticdb