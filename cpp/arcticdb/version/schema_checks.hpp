#pragma once

#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/python/normalization_checks.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {

enum NormalizationOperation : uint8_t {
    APPEND,
    UPDATE,
};

inline std::string_view normalization_operation_str(NormalizationOperation operation)
{
    switch (operation) {
    case APPEND:
        return "APPEND";
    case UPDATE:
        return "UPDATE";
    default:
        util::raise_rte("Unknown operation type {}", static_cast<uint8_t>(operation));
    }
}

struct StreamDescriptorMismatch : ArcticSpecificException<ErrorCode::E_DESCRIPTOR_MISMATCH> {
    StreamDescriptorMismatch(const char* preamble,
        const StreamDescriptor& existing,
        const StreamDescriptor& new_val,
        NormalizationOperation operation)
        : ArcticSpecificException(fmt::format("{}: {} \nexisting={}\n new_val={}",
              preamble,
              normalization_operation_str(operation),
              fmt::join(existing.fields(), ", "),
              fmt::join(new_val.fields(), ", ")))
    {
    }
};

inline void check_normalization_index_match(NormalizationOperation operation,
    const StreamDescriptor::Proto& old_descriptor,
    const pipelines::InputTensorFrame& frame)
{
    auto old_idx_kind = old_descriptor.index().kind();
    bool new_is_timeseries = std::holds_alternative<TimeseriesIndex>(frame.index);

    if (operation == UPDATE) {
        util::check_rte(old_idx_kind == IndexDescriptor::TIMESTAMP && new_is_timeseries,
            "Update will not work as expected with a non-timeseries index");
    } else {
        // TODO: AN-722
        if (new_is_timeseries) {
            if (old_idx_kind != IndexDescriptor::TIMESTAMP) {
                log::version().warn("Appending a timeseries to a non-timeseries-indexed symbol may create a "
                                    "confusing index and cause problems later");
            }
        } else {
            if (old_idx_kind != IndexDescriptor::ROWCOUNT) {
                // Backwards compatibility
                log::version().warn("Appending a non-timeseries-indexed data to a timeseries symbol is highly "
                                    "likely to cause corruption/unexpected behaviour.");
            }
        }
    }
}

inline bool columns_match(const StreamDescriptor::Proto& left, const StreamDescriptor::Proto& right)
{
    if (left.fields_size() != right.fields_size())
        return false;

    for (auto i = 0; i < int(left.fields_size()); ++i) {
        if (left.fields(i).name() != right.fields(i).name())
            return false;

        if (!trivially_compatible_types(left.fields(i).type_desc(), right.fields(i).type_desc()))
            return false;
    }
    return true;
}

inline void fix_descriptor_mismatch_or_throw(NormalizationOperation operation,
    bool dynamic_schema,
    const pipelines::index::IndexSegmentReader& existing_isr,
    const pipelines::InputTensorFrame& new_frame)
{
    const auto& old_sd = existing_isr.tsd().stream_descriptor();
    check_normalization_index_match(operation, old_sd, new_frame);

    if (dynamic_schema)
        return; // TODO: dynamic schema may need some of the checks as below

    fix_normalization_or_throw(operation == APPEND, existing_isr, new_frame);

    if (!columns_match(old_sd, new_frame.desc.proto())) {
        throw StreamDescriptorMismatch(
            "The columns (names and types) in the argument are not identical to that of the existing version",
            StreamDescriptor{old_sd},
            new_frame.desc,
            operation);
    }
}
} // namespace arcticdb