/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/merge_descriptors.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {
StreamDescriptor merge_descriptors(
    const StreamDescriptor &original,
    const std::vector<std::shared_ptr<FieldCollection>> &entries,
    const std::unordered_set<std::string_view> &filtered_set,
    const std::optional<IndexDescriptor>& default_index) {
    using namespace arcticdb::stream;
    std::vector<std::string_view> merged_fields;
    std::unordered_map<std::string_view, TypeDescriptor> merged_fields_map;

    for (const auto &field : original.fields()) {
        merged_fields.push_back(field.name());
        merged_fields_map.try_emplace(field.name(), field.type());
    }

    auto index = empty_index();
    if(original.index().uninitialized()) {
        if(default_index) {
            auto temp_idx = default_index_type_from_descriptor(*default_index);
            util::variant_match(temp_idx, [&merged_fields, &merged_fields_map] (const auto& idx) {
                using IndexType = std::decay_t<decltype(idx)>;
                merged_fields.emplace_back(idx.name());
                merged_fields_map.try_emplace(idx.name(), static_cast<TypeDescriptor>(typename IndexType::TypeDescTag{}));
            });
        }
        else
            util::raise_rte("Descriptor has uninitialized index and no default supplied");
    } else {
        index = index_type_from_descriptor(original);
    }

    const bool has_index = !std::holds_alternative<RowCountIndex>(index);

    // Merge all the fields for all slices, apart from the index which we already have from the first descriptor.
    // Note that we preserve the ordering as we see columns, especially the index which needs to be column 0.
    for (const auto &fields : entries) {
        if(has_index)
            util::variant_match(index, [&fields] (const auto& idx) { idx.check(*fields); });

        for (size_t idx = has_index ? 1u : 0u; idx < static_cast<size_t>(fields->size()); ++idx) {
            const auto& field = fields->at(idx);
            const auto& type_desc = field.type();
            if (filtered_set.empty() || (filtered_set.find(field.name()) != filtered_set.end())) {
                if(auto existing = merged_fields_map.find(field.name()); existing != merged_fields_map.end()) {
                    auto existing_type_desc = existing->second;
                    if(existing_type_desc != type_desc) {
                        log::version().info(
                                "Merging different type descriptors for column: {}\n"
                                "Existing type descriptor                : {}\n"
                                "New type descriptor                     : {}",
                                field.name(), existing_type_desc, type_desc
                        );
                        auto new_descriptor = has_valid_common_type(existing_type_desc, type_desc);
                        if(new_descriptor) {
                            merged_fields_map[field.name()] = *new_descriptor;
                        } else {
                            util::raise_rte("No valid common type between {} and {} for column {}", existing_type_desc, type_desc, field.name());
                        }
                    }
                } else {
                    merged_fields.push_back(field.name());
                    merged_fields_map.try_emplace(field.name(), type_desc);
                }
            }
        }
    }
    auto new_fields = std::make_shared<FieldCollection>();
    for(const auto& field_name : merged_fields) {
        new_fields->add_field(merged_fields_map[field_name], field_name);
    }
    return arcticdb::entity::StreamDescriptor{original.id(), get_descriptor_from_index(index), std::move(new_fields)};
}

StreamDescriptor merge_descriptors(
    const StreamDescriptor &original,
    const std::vector<std::shared_ptr<FieldCollection>> &entries,
    const std::vector<std::string> &filtered_columns,
    const std::optional<IndexDescriptor>& default_index) {
    std::unordered_set<std::string_view> filtered_set(filtered_columns.begin(), filtered_columns.end());
    return merge_descriptors(original, entries, filtered_set, default_index);
}

StreamDescriptor merge_descriptors(
    const StreamDescriptor &original,
    const std::vector<pipelines::SliceAndKey> &entries,
    const std::vector<std::string> &filtered_columns,
    const std::optional<IndexDescriptor>& default_index) {
    std::vector<std::shared_ptr<FieldCollection>> fields;
    for (const auto &entry : entries) {
        fields.push_back(std::make_shared<FieldCollection>(entry.slice_.desc()->fields().clone()));
    }
    return merge_descriptors(original, fields, filtered_columns, default_index);
}

StreamDescriptor merge_descriptors(
    const std::shared_ptr<Store>& store,
    const StreamDescriptor &original,
    const std::vector<pipelines::SliceAndKey> &entries,
    const std::unordered_set<std::string_view> &filtered_set,
    const std::optional<IndexDescriptor>& default_index) {
    std::vector<std::shared_ptr<FieldCollection>> fields;
    for (const auto &entry : entries) {
        fields.push_back(std::make_shared<FieldCollection>(entry.segment(store).descriptor().fields().clone()));
    }
    return merge_descriptors(original, fields, filtered_set, default_index);
}
}