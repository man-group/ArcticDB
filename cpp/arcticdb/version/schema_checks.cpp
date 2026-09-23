#include <arcticdb/version/schema_checks.hpp>
#include <arcticdb/pipeline/index_segment_reader.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/processing/schema_combine.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace {
using namespace arcticdb;

// A RangeIndex has to continue where the existing one stopped, which is the only part of the merge that needs the
// existing row count and so the only part combine_schema cannot do. Rewrites the incoming metadata's start so that it
// spans both, and so has to run before the schemas are combined.
void align_rowrange_norm_for_append(
        const entity::OutputSchema& existing, size_t existing_total_rows, entity::OutputSchema& incoming
) {
    if (existing.inferred_from_empty_frame() || incoming.inferred_from_empty_frame()) {
        // A RangeIndex normalized from an empty frame needs no alignment. It will be skipped by `combine_schema`
        return;
    }
    if (existing.stream_descriptor().index().type() != IndexDescriptor::Type::ROWCOUNT ||
        incoming.stream_descriptor().index().type() != IndexDescriptor::Type::ROWCOUNT) {
        return;
    }
    // We need to update only for pandas rowrange.
    const auto* existing_pandas = pandas_common(existing.norm_metadata_);
    const auto* incoming_pandas = pandas_common(incoming.norm_metadata_);
    if (existing_pandas == nullptr || incoming_pandas == nullptr || !existing_pandas->has_index() ||
        !incoming_pandas->has_index()) {
        return;
    }
    update_rowrange_norm_for_append(existing.norm_metadata_, incoming.norm_metadata_, existing_total_rows);
}

} // namespace

namespace arcticdb {

entity::OutputSchema combine_existing_tsd_with_frame(
        NormalizationOperation operation, bool dynamic_schema, const TimeseriesDescriptor& existing_tsd,
        pipelines::InputFrame& new_frame
) {
    const auto options = within_symbol_combine_options(dynamic_schema, operation, new_frame.desc().id());
    auto existing = schema_from_tsd(existing_tsd);
    // Before the schema is taken from the frame, and so before the frame writes its data keys, so that the names those
    // keys record are the ones the index key will
    align_multi_index_names(existing, new_frame.desc());
    auto incoming = schema_from_input_frame(new_frame);
    if (operation == NormalizationOperation::APPEND) {
        align_rowrange_norm_for_append(existing, existing_tsd.total_rows(), incoming);
    }
    const std::array schemas{std::move(existing), std::move(incoming)};
    return combine_schema(schemas, options);
}

IncompleteSchemas combine_incomplete_schemas(
        const std::optional<entity::OutputSchema>& existing, size_t existing_total_rows,
        std::span<const AppendMapEntry> incompletes, const ReadIncompletesFlags& flags, const StreamId& stream_id
) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !incompletes.empty(), "combine_incomplete_schemas requires at least one incomplete segment"
    );
    const auto options =
            within_symbol_combine_options(flags.dynamic_schema, NormalizationOperation::INCOMPLETE, stream_id);

    const auto schema_of = [&](const AppendMapEntry& entry) {
        StreamDescriptor descriptor = entry.descriptor().clone();
        if (flags.convert_int_to_float) {
            stream::convert_descriptor_types(descriptor);
        }
        // Staged data is deliberately not aligned to an existing multi-index. Its segments are already written, and the
        // dynamic read maps their columns by name, so aligning only the schema would leave the data behind and the
        // combination is better refused. Doing it properly means renaming each segment's descriptor as compaction
        // reads it.
        auto norm = entry.norm_meta_;
        // A segment staged by the tick collector carries no normalization metadata of its own.
        ensure_timeseries_norm_meta(norm, stream_id);
        if (flags.sparsify) {
            // Reaching a timezone decision through the sparsify flag is a bug. Monday ref 11198274752.
            label_index_utc_if_unlabelled(norm);
        }
        return entity::OutputSchema{std::move(descriptor), std::move(norm), entry.empty()};
    };

    auto staged_schemas = util::reserve_vector<entity::OutputSchema>(incompletes.size());
    for (const auto& entry : incompletes) {
        staged_schemas.emplace_back(schema_of(entry));
    }
    auto staged = staged_schemas.size() == 1 ? std::move(staged_schemas.front())
                                             : combine_schema(std::span{staged_schemas}, options);

    if (!existing.has_value()) {
        return {staged, staged};
    }
    align_rowrange_norm_for_append(*existing, existing_total_rows, staged);
    const std::array schemas{*existing, staged};
    return {std::move(staged), combine_schema(schemas, options)};
}
} // namespace arcticdb
