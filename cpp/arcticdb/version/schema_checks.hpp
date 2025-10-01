#pragma once

#include <arcticdb/pipeline/input_tensor_frame.hpp>

namespace arcticdb {

namespace pipelines::index {
struct IndexSegmentReader;
}

enum NormalizationOperation : uint8_t { APPEND, UPDATE, MERGE };

std::string_view normalization_operation_str(NormalizationOperation operation);

struct StreamDescriptorMismatch : ArcticSpecificException<ErrorCode::E_DESCRIPTOR_MISMATCH> {
    StreamDescriptorMismatch(
            const char* preamble, const StreamId& stream_id, const StreamDescriptor& existing,
            const StreamDescriptor& new_val, NormalizationOperation operation
    );
};

IndexDescriptor::Type get_common_index_type(const IndexDescriptor::Type& left, const IndexDescriptor::Type& right);

void check_normalization_index_match(
        NormalizationOperation operation, const StreamDescriptor& old_descriptor,
        const pipelines::InputTensorFrame& frame, bool empty_types
);

bool index_names_match(const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor);

bool columns_match(
        const StreamDescriptor& df_in_store_descriptor, const StreamDescriptor& new_df_descriptor,
        const bool convert_int_to_float = false
);

void fix_descriptor_mismatch_or_throw(
        NormalizationOperation operation, bool dynamic_schema, const pipelines::index::IndexSegmentReader& existing_isr,
        const pipelines::InputTensorFrame& new_frame, bool empty_types
);
} // namespace arcticdb

template<>
struct fmt::formatter<arcticdb::NormalizationOperation> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::NormalizationOperation op, FormatContext& ctx) const {
        using namespace arcticdb::entity;
        switch (op) {
        case arcticdb::NormalizationOperation::APPEND:
            return fmt::format_to(ctx.out(), "APPEND");
        case arcticdb::NormalizationOperation::UPDATE:
            return fmt::format_to(ctx.out(), "UPDATE");
        case arcticdb::NormalizationOperation::MERGE:
            return fmt::format_to(ctx.out(), "MERGE");
        }
    }
};