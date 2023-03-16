#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>

namespace arcticdb {
StreamDescriptor merge_descriptors(
    const StreamDescriptor &original,
    const std::vector<std::shared_ptr<FieldCollection>> &entries,
    const std::unordered_set<std::string_view> &filtered_set,
    const std::optional<IndexDescriptor>& default_index);

entity::StreamDescriptor merge_descriptors(
    const entity::StreamDescriptor &original,
    const std::vector<std::shared_ptr<FieldCollection>> &entries,
    const std::vector<std::string> &filtered_columns,
    const std::optional<entity::IndexDescriptor>& default_index = std::nullopt);

entity::StreamDescriptor merge_descriptors(
    const entity::StreamDescriptor &original,
    const std::vector<pipelines::SliceAndKey> &entries,
    const std::vector<std::string> &filtered_columns,
    const std::optional<entity::IndexDescriptor>& default_index = std::nullopt);

entity::StreamDescriptor merge_descriptors(
    const std::shared_ptr<Store>& store,
    const entity::StreamDescriptor &original,
    const std::vector<pipelines::SliceAndKey> &entries,
    const std::unordered_set<std::string_view> &filtered_set,
    const std::optional<entity::IndexDescriptor>& default_index = std::nullopt);
}