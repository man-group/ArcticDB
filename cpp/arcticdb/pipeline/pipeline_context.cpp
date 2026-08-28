/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/pipeline/index_utils.hpp>
#include <arcticdb/column_store/column_map.hpp>

namespace arcticdb::pipelines {

PipelineContext::PipelineContext(SegmentInMemory& frame, const AtomKey& key) : on_disk_descriptor_(frame.descriptor()) {
    SliceAndKey sk{FrameSlice{frame}, key};
    slice_and_keys_.emplace_back(std::move(sk));
    util::BitSet bitset(1);
    bitset.flip();
    fetch_index_ = std::move(bitset);
    ensure_vectors();

    generate_filtered_field_descriptors({});
    string_pools_[0] = frame.string_pool_ptr();
    auto map = std::make_shared<ColumnMap>(frame.descriptor().field_count());
    map->set_from_descriptor(frame.descriptor());

    auto descriptor = std::make_shared<StreamDescriptor>(frame.descriptor());
    segment_descriptors_[0] = std::move(descriptor);
}

void PipelineContext::set_selected_columns(const std::optional<std::vector<std::string>>& columns) {
    util::check(static_cast<bool>(on_disk_descriptor_), "Descriptor not set in set_selected_columns");
    selected_columns_ = requested_column_bitset_including_index(*on_disk_descriptor_, columns);
}

void PipelineContext::generate_string_coerced_descriptor(const ReadOptions& read_options) {
    const bool force_strings_to_object = opt_false(read_options.force_strings_to_object());
    const bool force_strings_to_fixed = opt_false(read_options.force_strings_to_fixed());
    if (!force_strings_to_object && !force_strings_to_fixed)
        return;

    StreamDescriptor desc = *on_disk_descriptor_;
    if (force_strings_to_object) {
        auto& fields = desc.fields();
        for (Field& field_desc : fields) {
            if (field_desc.type().data_type() == DataType::ASCII_FIXED64)
                set_data_type(DataType::ASCII_DYNAMIC64, field_desc.mutable_type());

            if (field_desc.type().data_type() == DataType::UTF_FIXED64)
                set_data_type(DataType::UTF_DYNAMIC64, field_desc.mutable_type());
        }
    } else if (force_strings_to_fixed) {
        auto& fields = desc.fields();
        for (Field& field_desc : fields) {
            if (field_desc.type().data_type() == DataType::ASCII_DYNAMIC64)
                set_data_type(DataType::ASCII_FIXED64, field_desc.mutable_type());

            if (field_desc.type().data_type() == DataType::UTF_DYNAMIC64)
                set_data_type(DataType::UTF_FIXED64, field_desc.mutable_type());
        }
    }
    util::check(
            desc.field_count() == on_disk_descriptor_->field_count(),
            "Coerced descriptor field count {} does not match on-disk descriptor field count {}",
            desc.field_count(),
            on_disk_descriptor_->field_count()
    );
    string_coerced_descriptor_ = std::move(desc);
}

void PipelineContext::generate_filtered_field_descriptors(const std::optional<std::vector<std::string>>& columns) {
    if (columns.has_value()) {
        const ankerl::unordered_dense::set<std::string_view> column_set{std::begin(*columns), std::end(*columns)};

        filter_columns_ = std::make_shared<FieldCollection>();
        ARCTICDB_DEBUG(log::version(), "Context descriptor: {}", output_descriptor());
        for (const auto& field : output_descriptor().fields()) {
            if (column_set.find(field.name()) != column_set.end())
                filter_columns_->add_field(field.type(), field.name());
        }

        filter_columns_set_ = std::unordered_set<std::string_view>{};
        for (const auto& field : *filter_columns_)
            filter_columns_set_->insert(field.name());
    }
}

bool PipelineContext::only_index_columns_selected() const {
    if (!overall_column_bitset_)
        return false;
    if (overall_column_bitset_->count() == 0)
        return true;
    // For RangeIndex, field_count() == 0, so bit 0 is a data column, not an index column.
    if (on_disk_descriptor_ && on_disk_descriptor_->index().field_count() == 0)
        return false;
    return overall_column_bitset_->count() == 1 && (*overall_column_bitset_)[0];
}

std::optional<proto::descriptors::UserDefinedMetadata> PipelineContext::release_opt_user_defined_metadata() {
    if (tsd_.has_value()) {
        return std::move(*tsd_->mutable_proto().mutable_user_meta());
    } else {
        return std::nullopt;
    }
}

const std::optional<util::BitSet>& PipelineContextRow::get_selected_columns() const {
    return parent_->selected_columns_;
}

void PipelineContext::set_tsd(TimeseriesDescriptor&& tsd) { tsd_.emplace(std::move(tsd)); }

const TimeseriesDescriptor& PipelineContext::tsd() const {
    util::check(tsd_.has_value(), "No TSD defined");
    return *tsd_;
}

bool PipelineContext::has_normalization() const { return tsd_.has_value(); }

const arcticdb::proto::descriptors::NormalizationMetadata& PipelineContext::normalization() const {
    util::check(tsd_.has_value(), "No normalization metadata defined");
    return tsd_->proto().normalization();
}

arcticdb::proto::descriptors::NormalizationMetadata& PipelineContext::mutable_normalization() {
    if (!tsd_.has_value()) {
        tsd_.emplace();
    }
    return *tsd_->mutable_proto().mutable_normalization();
}

void PipelineContext::set_normalization(arcticdb::proto::descriptors::NormalizationMetadata&& norm_meta) {
    if (!tsd_.has_value()) {
        tsd_.emplace();
    }
    *tsd_->mutable_proto().mutable_normalization() = std::move(norm_meta);
}

arcticdb::proto::descriptors::NormalizationMetadata PipelineContext::release_normalization() {
    util::check(tsd_.has_value(), "No normalization metadata defined");
    return std::move(*tsd_->mutable_proto().mutable_normalization());
}

const StreamDescriptor& PipelineContext::on_disk_descriptor() const {
    util::check(
            on_disk_descriptor_, "Trying to read on disk descriptor from the processing pipeline but it is not set"
    );
    return *on_disk_descriptor_;
}
StreamDescriptor& PipelineContext::on_disk_descriptor() {
    return const_cast<StreamDescriptor&>(static_cast<const PipelineContext&>(*this).on_disk_descriptor());
}
[[nodiscard]] bool PipelineContext::has_on_disk_descriptor() const { return on_disk_descriptor_.has_value(); }

void PipelineContext::set_on_disk_descriptor(StreamDescriptor&& desc) { on_disk_descriptor_ = std::move(desc); }

void PipelineContext::set_on_disk_descriptor(const StreamDescriptor& desc) { on_disk_descriptor_ = desc; }

[[nodiscard]] bool PipelineContext::are_string_fields_coerced() const { return string_coerced_descriptor_.has_value(); }

void PipelineContext::set_output_schema(OutputSchema&& output_schema) { output_schema_ = std::move(output_schema); }

const StreamDescriptor& PipelineContext::output_descriptor() const {
    if (output_schema_) {
        return output_schema_->stream_descriptor();
    }
    if (string_coerced_descriptor_) {
        return *string_coerced_descriptor_;
    }
    return on_disk_descriptor();
}

const proto::descriptors::NormalizationMetadata& PipelineContext::output_normalization() const {
    return output_schema_ ? output_schema_->norm_metadata_ : normalization();
}

index::RequiredFieldInfo PipelineContext::output_required_fields_info() const {
    if (output_schema_ || tsd_) {
        return index::required_fields_info(output_descriptor(), output_normalization());
    }
    return index::required_fields_info(output_descriptor());
}

const ankerl::unordered_dense::map<std::string, Value>& PipelineContext::output_default_values() const {
    if (output_schema_) {
        return output_schema_->default_values();
    }
    static const ankerl::unordered_dense::map<std::string, Value> empty_default_values;
    return empty_default_values;
}

const StringPool& PipelineContextRow::string_pool() const { return *parent_->string_pools_[index_]; }

StringPool& PipelineContextRow::string_pool() { return *parent_->string_pools_[index_]; }

const std::shared_ptr<StringPool>& PipelineContextRow::string_pool_ptr() { return parent_->string_pools_[index_]; }

void PipelineContextRow::allocate_string_pool() { parent_->string_pools_[index_] = std::make_shared<StringPool>(); }

void PipelineContextRow::set_string_pool(const std::shared_ptr<StringPool>& pool) {
    parent_->string_pools_[index_] = pool;
}

const SliceAndKey& PipelineContextRow::slice_and_key() const { return parent_->slice_and_keys_[index_]; }

SliceAndKey& PipelineContextRow::slice_and_key() { return parent_->slice_and_keys_[index_]; }

bool PipelineContextRow::fetch_index() const { return parent_->fetch_index_[index_]; }

size_t PipelineContextRow::index() const { return index_; }

bool PipelineContextRow::has_string_pool() const { return static_cast<bool>(parent_->string_pools_[index_]); }
const StreamDescriptor& PipelineContextRow::descriptor() const {
    util::check(index_ < parent_->segment_descriptors_.size(), "Descriptor out of bounds for index {}", index_);
    util::check(static_cast<bool>(parent_->segment_descriptors_[index_]), "Null descriptor at index {}", index_);
    return *parent_->segment_descriptors_[index_];
}

void PipelineContextRow::set_descriptor(std::shared_ptr<StreamDescriptor>&& desc) {
    parent_->segment_descriptors_[index_] = std::move(desc);
}

void PipelineContextRow::set_descriptor(const StreamDescriptor& desc) {
    parent_->segment_descriptors_[index_] = std::make_shared<StreamDescriptor>(desc);
}

void PipelineContextRow::set_descriptor(const std::shared_ptr<StreamDescriptor>& desc) {
    parent_->segment_descriptors_[index_] = desc;
}

void PipelineContextRow::set_compacted(bool val) { parent_->compacted_[index_] = val; }

bool PipelineContextRow::compacted() const { return parent_->compacted_[index_]; }

void PipelineContextRow::set_descriptor(StreamDescriptor&& desc) {
    auto shared_desc = std::make_shared<StreamDescriptor>(std::move(desc));
    set_descriptor(std::move(shared_desc));
}

} // namespace arcticdb::pipelines