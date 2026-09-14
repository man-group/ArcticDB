/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/processing/clause.hpp>

#include <cstdint>
#include <set>

#include <arcticdb/column_store/column_reslicer.hpp>
#include <arcticdb/column_store/segment_reslicer.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/pipeline/write_frame.hpp>
#include <arcticdb/processing/clause_utils.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace arcticdb {

RenameColumnsClause::RenameColumnsClause(ankerl::unordered_dense::map<std::string, std::string>&& column_renames) :
    column_renames_(std::move(column_renames)) {
    clause_info_.input_structure_ = ProcessingStructure::ALL;
    clause_info_.output_structure_ = ProcessingStructure::ALL;
    clause_info_.can_combine_with_column_selection_ = false;
}

std::vector<std::vector<size_t>> RenameColumnsClause::structure_for_processing(
        std::vector<RangesAndKey>& ranges_and_keys
) {
    log::version().debug("RenameColumnsClause structuring {} data keys for processing", ranges_and_keys.size());
    // TODO: Factor out into clause_utils.hpp as structure_any
    auto res = util::reserve_vector<std::vector<size_t>>(ranges_and_keys.size());
    for (size_t idx = 0; idx < ranges_and_keys.size(); ++idx) {
        res.emplace_back(std::vector<size_t>(1, idx));
    }
    return res;
}

std::vector<std::vector<EntityId>> RenameColumnsClause::structure_for_processing(std::vector<std::vector<EntityId>>&&) {
    internal::raise<ErrorCode::E_ASSERTION_FAILURE>("RenameColumns clause should be the first clause in the pipeline");
}

std::vector<EntityId> RenameColumnsClause::process(std::vector<EntityId>&& entity_ids) const {
    auto input_segment_count = entity_ids.size();
    util::check(
            input_segment_count == 1,
            "Unexpected number of segments {} in RenameColumnsClause::process",
            input_segment_count
    );
    auto proc = gather_entities<std::shared_ptr<SegmentInMemory>, std::shared_ptr<RowRange>, std::shared_ptr<ColRange>>(
            *component_manager_, entity_ids
    );
    const auto& input_desc = proc.segments_->front()->descriptor();
    auto output_desc = std::make_shared<StreamDescriptor>(
            input_desc.data_ptr(), std::make_shared<FieldCollection>(), input_desc.stream_id_
    );
    auto& new_fields = output_desc->fields();
    for (const auto& field : input_desc) {
        if (auto it = column_renames_.find(std::string(field.name())); it != column_renames_.end()) {
            new_fields.add_field(field.type(), it->second);
        } else {
            new_fields.add_field(field.type(), field.name());
        }
    }
    proc.segments_->front()->attach_descriptor(output_desc);
    return push_entities(*component_manager_, std::move(proc));
}

const ClauseInfo& RenameColumnsClause::clause_info() const { return clause_info_; }

void RenameColumnsClause::set_processing_config(const ProcessingConfig&) {}

void RenameColumnsClause::set_component_manager(std::shared_ptr<ComponentManager> component_manager) {
    component_manager_ = std::move(component_manager);
}

OutputSchema RenameColumnsClause::modify_schema(OutputSchema&& output_schema) const { return output_schema; }

OutputSchema RenameColumnsClause::join_schemas(std::vector<OutputSchema>&&) const {
    util::raise_rte("RenameColumnsClause::join_schemas should never be called");
}

std::string RenameColumnsClause::to_string() const {
    return fmt::format("RenameColumnsClause(column_renames={})", column_renames_);
}

} // namespace arcticdb
