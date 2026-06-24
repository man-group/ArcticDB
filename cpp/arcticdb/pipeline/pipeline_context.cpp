/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/pipeline/pipeline_context.hpp>
#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/column_store/column_map.hpp>

namespace arcticdb::pipelines {

PipelineContext::PipelineContext(SegmentInMemory& frame, const AtomKey& key) : desc_(frame.descriptor()) {
    SliceAndKey sk{FrameSlice{frame}, key};
    slice_and_keys_.emplace_back(std::move(sk));
    util::BitSet bitset(1);
    bitset.flip();
    fetch_index_ = std::move(bitset);
    ensure_vectors();

    generate_filtered_field_descriptors(*this, {});
    string_pools_[0] = frame.string_pool_ptr();
    auto map = std::make_shared<ColumnMap>(frame.descriptor().field_count());
    map->set_from_descriptor(frame.descriptor());

    auto descriptor = std::make_shared<StreamDescriptor>(frame.descriptor());
    segment_descriptors_[0] = std::move(descriptor);
}

void PipelineContext::set_selected_columns(const std::optional<std::vector<std::string>>& columns) {
    util::check(static_cast<bool>(desc_), "Descriptor not set in set_selected_columns");
    selected_columns_ = requested_column_bitset_including_index(*desc_, columns);
}

bool PipelineContext::only_index_columns_selected() const {
    if (!overall_column_bitset_)
        return false;
    if (overall_column_bitset_->count() == 0)
        return true;
    // For RangeIndex, field_count() == 0, so bit 0 is a data column, not an index column.
    if (desc_ && desc_->index().field_count() == 0)
        return false;
    return overall_column_bitset_->count() == 1 && (*overall_column_bitset_)[0];
}

std::optional<proto::descriptors::UserDefinedMetadata> PipelineContext::release_user_defined_metadata() {
    if (index_segment_reader_.has_value()) {
        return std::move(*index_segment_reader_->mutable_tsd().mutable_proto().mutable_user_meta());
    } else {
        return std::nullopt;
    }
}

bool PipelineContext::has_norm_metadata() const { return !std::holds_alternative<std::monostate>(norm_meta_); }

const proto::descriptors::NormalizationMetadata& PipelineContext::norm_metadata() const {
    if (std::holds_alternative<std::monostate>(norm_meta_)) {
        util::raise_rte("PipelineContext::norm_metadata: No normalization metadata defined");
    } else if (std::holds_alternative<proto::descriptors::NormalizationMetadata*>(norm_meta_)) {
        return *std::get<proto::descriptors::NormalizationMetadata*>(norm_meta_);
    } else { // std::holds_alternative<proto::descriptors::NormalizationMetadata>(norm_meta_)
        return std::get<proto::descriptors::NormalizationMetadata>(norm_meta_);
    }
}

proto::descriptors::NormalizationMetadata& PipelineContext::mutable_norm_metadata() {
    if (std::holds_alternative<std::monostate>(norm_meta_)) {
        util::raise_rte("PipelineContext::mutable_norm_metadata: No normalization metadata defined");
    } else if (std::holds_alternative<proto::descriptors::NormalizationMetadata*>(norm_meta_)) {
        norm_meta_ = std::move(*std::get<proto::descriptors::NormalizationMetadata*>(norm_meta_));
    }
    return std::get<proto::descriptors::NormalizationMetadata>(norm_meta_);
}

void PipelineContext::set_norm_metadata(proto::descriptors::NormalizationMetadata norm_metadata) {
    norm_meta_ = std::move(norm_metadata);
}

void PipelineContext::set_norm_metadata(proto::descriptors::NormalizationMetadata* norm_metadata) {
    norm_meta_ = norm_metadata;
}

proto::descriptors::NormalizationMetadata PipelineContext::release_norm_metadata() {
    return util::variant_match(
            norm_meta_,
            [this](proto::descriptors::NormalizationMetadata* norm_meta) {
                auto res = std::move(*norm_meta);
                norm_meta_ = std::monostate();
                return res;
            },
            [this](proto::descriptors::NormalizationMetadata& norm_meta) {
                auto res = std::move(norm_meta);
                norm_meta_ = std::monostate();
                return res;
            },
            [](auto&&) -> proto::descriptors::NormalizationMetadata {
                util::raise_rte("PipelineContext::release_norm_metadata: No normalization metadata defined");
            }
    );
}

const std::optional<util::BitSet>& PipelineContextRow::get_selected_columns() const {
    return parent_->selected_columns_;
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