#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline//error.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/python/normalization_checks.hpp>
#include <arcticdb/entity/type_utils.hpp>

namespace arcticdb {

using CheckOutcome = std::variant<Error, std::monostate>;
using StaticSchemaCompactionChecks = folly::Function<CheckOutcome(const StreamDescriptor&, const StreamDescriptor&)>;
using CompactionWrittenKeys = std::vector<VariantKey>;
using CompactionResult = std::variant<CompactionWrittenKeys, Error>;

enum NormalizationOperation : uint8_t {
    APPEND,
    UPDATE,
};

std::string_view normalization_operation_str(NormalizationOperation operation);

struct StreamDescriptorMismatch : ArcticSpecificException<ErrorCode::E_DESCRIPTOR_MISMATCH>  {
    StreamDescriptorMismatch(const char* preamble, const StreamId& stream_id, const StreamDescriptor& existing, const StreamDescriptor& new_val, NormalizationOperation operation) :
    ArcticSpecificException(fmt::format("{}: {}; stream_id=\"{}\"; existing=\"{}\"; new_val=\"{}\"",
                                        preamble,
                                        normalization_operation_str(operation),
                                        stream_id,
                                        existing.fields(),
                                        new_val.fields())) {}
};

IndexDescriptor::Type get_common_index_type(const IndexDescriptor::Type& left, const IndexDescriptor::Type& right) ;

void check_normalization_index_match(
    NormalizationOperation operation,
    const StreamDescriptor& old_descriptor,
    const pipelines::InputTensorFrame& frame,
    bool empty_types
);

bool index_names_match(
    const StreamDescriptor& df_in_store_descriptor,
    const StreamDescriptor& new_df_descriptor
);

bool columns_match(
    const StreamDescriptor& df_in_store_descriptor,
    const StreamDescriptor& new_df_descriptor
);

void fix_descriptor_mismatch_or_throw(
    NormalizationOperation operation,
    bool dynamic_schema,
    const pipelines::index::IndexSegmentReader &existing_isr,
    const pipelines::InputTensorFrame &new_frame,
    bool empty_types
);
} // namespace arcticdb
