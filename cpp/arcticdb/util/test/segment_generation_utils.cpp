#include <arcticdb/util/test/segment_generation_utils.hpp>

namespace arcticdb {
std::vector<StreamDescriptor> split_descriptor(const StreamDescriptor& descriptor, const size_t cols_per_segment) {
    if (descriptor.fields().size() <= cols_per_segment) {
        return std::vector{descriptor};
    }
    const size_t num_segments = (descriptor.fields().size() + cols_per_segment - 1) / cols_per_segment;
    std::vector<StreamDescriptor> res;
    res.reserve(num_segments);

    const unsigned field_count = descriptor.field_count();
    for (size_t i = 0, source_field = descriptor.index().field_count(); i < num_segments; ++i) {
        StreamDescriptor partial(descriptor.id());
        if (descriptor.index().field_count() > 0) {
            partial.set_index(descriptor.index());
            for (unsigned index_field = 0; index_field < descriptor.index().field_count(); ++index_field) {
                partial.add_field(descriptor.field(index_field));
            }
        }
        for (size_t field = 0; field < cols_per_segment && source_field < field_count; ++field) {
            partial.add_field(descriptor.field(source_field++));
        }
        res.push_back(std::move(partial));
    }
    return res;
}

} // namespace arcticdb
