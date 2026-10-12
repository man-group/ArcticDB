/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/util/collection_utils.hpp>

namespace arcticdb {

RenameColumnsClause::RenameColumnsClause(
        const ankerl::unordered_dense::map<std::string, std::string, util::TransparentStringHash, std::equal_to<>>&
                column_renames
) :
    column_renames_(column_renames) {
    clause_info_.input_structure_ = ProcessingStructure::ALL;
    clause_info_.output_structure_ = ProcessingStructure::ALL;
    clause_info_.can_combine_with_column_selection_ = false;
}

std::vector<std::vector<size_t>> RenameColumnsClause::structure_for_processing(
        std::vector<RangesAndKey>& ranges_and_keys
) {
    log::version().debug("RenameColumnsClause structuring {} data keys for processing", ranges_and_keys.size());
    std::ranges::sort(ranges_and_keys, [](const RangesAndKey& l, const RangesAndKey& r) {
        return std::tie(l.col_range().first, l.row_range().first) < std::tie(r.col_range().first, r.row_range().first);
    });
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
    auto [segments, row_ranges, col_ranges, atom_keys] = component_manager_->get_components_and_decrement_refcount<
            std::shared_ptr<Segment>,
            std::shared_ptr<RowRange>,
            std::shared_ptr<ColRange>,
            std::shared_ptr<AtomKey>>(entity_ids);
    auto& segment = segments.at(0);
    // Renaming columns without decoding with V2 is much more complicated than with V1, and no data is written with this
    // format yet. Implement when V2 encoding fully supported.
    internal::check<ErrorCode::E_NOT_IMPLEMENTED>(
            segment->header().encoding_version() == EncodingVersion::V1,
            "Column renaming not supported with V2 encoding"
    );
    auto& key = atom_keys.at(0);
    auto input_fields = segment->fields_ptr();
    // The number of fields in new_fields will be exact here. The buffer size will not be, but will be close enough
    // to avoid a realloc+memcpy on most add_field calls
    FieldCollection new_fields{input_fields->size(), input_fields->buffer().bytes()};
    bool changed{false};
    for (const auto& field : *input_fields) {
        if (auto it = column_renames_.find(field.name()); it != column_renames_.end()) {
            new_fields.add_field(field.type(), it->second);
            changed = true;
        } else {
            new_fields.add_field(field.type(), field.name());
        }
    }
    if (changed) {
        segment->set_fields(std::move(new_fields));
        key->set_content_hash(get_segment_hash(*segment));
        return component_manager_->add_entities(segments, row_ranges, col_ranges, atom_keys);
    } else {
        return {};
    }
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
