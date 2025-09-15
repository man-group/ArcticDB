/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <span>

namespace arcticdb {
StreamDescriptor merge_descriptors(
        const StreamDescriptor& original, std::span<const std::shared_ptr<FieldCollection>> entries,
        const std::unordered_set<std::string_view>& filtered_set,
        const std::optional<IndexDescriptorImpl>& default_index, bool convert_int_to_float = false
);

entity::StreamDescriptor merge_descriptors(
        const entity::StreamDescriptor& original, const std::vector<std::shared_ptr<FieldCollection>>& entries,
        const std::optional<std::vector<std::string>>& filtered_columns,
        const std::optional<entity::IndexDescriptorImpl>& default_index = std::nullopt,
        bool convert_int_to_float = false
);

entity::StreamDescriptor merge_descriptors(
        const entity::StreamDescriptor& original, std::span<const std::shared_ptr<FieldCollection>> entries,
        const std::optional<std::vector<std::string>>& filtered_columns,
        const std::optional<entity::IndexDescriptorImpl>& default_index = std::nullopt,
        bool convert_int_to_float = false
);

entity::StreamDescriptor merge_descriptors(
        const entity::StreamDescriptor& original, const std::vector<pipelines::SliceAndKey>& entries,
        const std::optional<std::vector<std::string>>& filtered_columns,
        const std::optional<entity::IndexDescriptorImpl>& default_index = std::nullopt,
        bool convert_int_to_float = false
);

entity::StreamDescriptor merge_descriptors(
        const std::shared_ptr<Store>& store, const entity::StreamDescriptor& original,
        const std::vector<pipelines::SliceAndKey>& entries, const std::unordered_set<std::string_view>& filtered_set,
        const std::optional<entity::IndexDescriptorImpl>& default_index = std::nullopt,
        bool convert_int_to_float = false
);
} // namespace arcticdb