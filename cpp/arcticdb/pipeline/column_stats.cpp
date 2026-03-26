#include <pipeline/column_stats.hpp>

#include "index_fields.hpp"

#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <semimap/semimap.h>

namespace arcticdb {

SegmentInMemory merge_column_stats_segments(const std::vector<SegmentInMemory>& segments) {
    SegmentInMemory merged;
    merged.init_column_map();
    merged.descriptor().set_index(IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});

    // Maintain the order of the columns in the input segments
    ankerl::unordered_dense::map<std::string, size_t> field_name_to_index;
    std::vector<TypeDescriptor> type_descriptors;
    std::vector<std::string> field_names;
    std::vector<arcticc::pb2::descriptors_pb2::ColumnStatsType> stat_types;
    std::vector<uint32_t> data_col_offsets;
    for (auto& segment : segments) {
        arcticc::pb2::descriptors_pb2::ColumnStatsHeader header;
        auto metadata = segment.metadata();
        bool unpacked = metadata->UnpackTo(&header);
        util::check(unpacked, "Could not unpack column stats metadata?");
        for (const auto& [idx, field] : folly::enumerate(segment.descriptor().fields())) {
            auto new_type = field.type();

            if (auto it = field_name_to_index.find(std::string{field.name()}); it != field_name_to_index.end()) {
                auto& merged_type = type_descriptors.at(field_name_to_index.at(std::string{field.name()}));
                auto opt_common_type = has_valid_common_type(merged_type, new_type);
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        opt_common_type.has_value(),
                        "No valid common type between {} and {} in {}",
                        merged_type,
                        new_type,
                        __FUNCTION__
                );
                merged_type = *opt_common_type;
            } else {
                type_descriptors.emplace_back(new_type);
                field_name_to_index.emplace(field.name(), type_descriptors.size() - 1);
                field_names.emplace_back(field.name());
                auto end_index_offset = static_cast<size_t>(index::Fields::end_index);
                if (idx > end_index_offset) {
                    // Skip start_index and end_index which are statistics
                    auto& header_info = header.stats().at(idx - end_index_offset - 1);
                    stat_types.emplace_back(header_info.type());
                    data_col_offsets.emplace_back(header_info.data_col_offset());
                }
            }
        }
    }

    arcticc::pb2::descriptors_pb2::ColumnStatsHeader merged_header;
    for (const auto& [idx, type_descriptor] : folly::enumerate(type_descriptors)) {
        merged.add_column(
                FieldRef{type_descriptor, field_names.at(idx)}, 0, AllocationType::DYNAMIC
        );
        auto* new_stats = merged_header.add_stats();
        new_stats->set_data_col_offset(data_col_offsets.at(idx));
        new_stats->set_stats_seg_offset(idx);
        new_stats->set_type(stat_types.at(idx));
    }
    for (auto& segment : segments) {
        merged.append(segment);
    }
    merged.set_compacted(true);
    merged.sort(start_index_column_name);

    google::protobuf::Any any;
    bool packed = any.PackFrom(merged_header);
    util::check(packed, "Failed to pack merged_header in to Any?");
    merged.set_metadata(std::move(any));
    return merged;
}


std::string type_to_operator_string(ColumnStatTypeInternal type) {
    struct Tag {};
    using TypeToOperatorStringMap = semi::static_map<ColumnStatTypeInternal, std::string, Tag>;
    TypeToOperatorStringMap::get(ColumnStatTypeInternal::MIN) = "MIN";
    TypeToOperatorStringMap::get(ColumnStatTypeInternal::MAX) = "MAX";
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            TypeToOperatorStringMap::contains(type), "Unknown column stat type requested"
    );
    return TypeToOperatorStringMap::get(type);
}

std::string type_to_name(ColumnStatType type) {
    struct Tag {};
    using TypeToNameMap = semi::static_map<ColumnStatType, std::string, Tag>;
    TypeToNameMap::get(ColumnStatType::MINMAX) = "MINMAX";
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            TypeToNameMap::contains(type), "Unknown column stat type requested"
    );
    return TypeToNameMap::get(type);
}

std::optional<ColumnStatType> name_to_type(const std::string& name) {
    // Cannot use static_map here as keys come from user input
    semi::map<std::string, ColumnStatType> name_to_type_map;
    name_to_type_map.get("MINMAX") = ColumnStatType::MINMAX;
    return name_to_type_map.contains(name) ? std::make_optional<ColumnStatType>(name_to_type_map.get(name))
                                           : std::nullopt;
}

std::string to_segment_column_name(const std::string& column, ColumnStatTypeInternal type) {
    return fmt::format("{}({})", type_to_operator_string(type), column);
}

ColumnStats::ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats) {
    for (const auto& [column, column_stat_names] : column_stats) {
        if (!column_stat_names.empty()) {
            column_stats_[column] = {};
            for (const auto& column_stat_name : column_stat_names) {
                auto opt_index_type = name_to_type(column_stat_name);
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        opt_index_type.has_value(), "Unknown column stat type provided: {}", column_stat_name
                );
                column_stats_[column].emplace(*opt_index_type);
            }
        }
        // we leave offsets_ empty in this case - there is no segment to index in to
    }
}

ColumnStats::ColumnStats(
        const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const FieldCollection& data_fields
) {
    using namespace arcticc::pb2::descriptors_pb2;
    for (const auto& mapping : header.stats()) {
        auto column_name = std::string{data_fields.at(mapping.data_col_offset()).name()};
        ColumnStatType external_type;
        switch (mapping.type()) {
        case COLUMN_STATS_MIN:
        case COLUMN_STATS_MAX:
            external_type = ColumnStatType::MINMAX;
            break;
        default:
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                    "Unrecognised column stats type in header: {}", static_cast<int>(mapping.type())
            );
        }
        if (auto it = column_stats_.find(column_name); it == column_stats_.end()) {
            column_stats_[column_name] = {external_type};
            offsets_[column_name] = {{external_type, {mapping.stats_seg_offset()}}};
        } else {
            it->second.emplace(external_type);
            offsets_.at(column_name).at(external_type).push_back(mapping.stats_seg_offset());
        }
    }
}

std::vector<size_t> ColumnStats::drop(const ColumnStats& to_drop, bool warn_if_missing) {
    std::vector<size_t> dropped_offsets;
    for (const auto& [column, column_stat_types] : to_drop.column_stats_) {
        if (auto it = column_stats_.find(column); it == column_stats_.end()) {
            if (warn_if_missing) {
                log::version().warn(
                        "Requested column stats drop but column '{}' does not have any column stats", column
                );
            }
        } else {
            for (const auto& column_stat_type : column_stat_types) {
                bool none_erased = it->second.erase(column_stat_type) == 0;
                if (none_erased) {
                    if (warn_if_missing) {
                        log::version().warn(
                                "Requested column stats drop but column '{}' does not have the specified column stat '{}'",
                                column,
                                type_to_name(column_stat_type)
                        );
                    }
                } else {
                    auto offsets_col_it = offsets_.find(column);
                    util::check(offsets_col_it != offsets_.end(), "offsets_ in invalid state");
                    auto& offsets_map = offsets_col_it->second;

                    auto offsets_type_it = offsets_map.find(column_stat_type);
                    util::check(offsets_type_it != offsets_map.end(), "offsets_ in invalid state");
                    dropped_offsets.insert(dropped_offsets.end(), offsets_type_it->second.begin(), offsets_type_it->second.end());

                    offsets_map.erase(offsets_type_it);
                    if (offsets_map.empty()) {
                        offsets_.erase(offsets_col_it);
                    }
                }
            }
        }
    }
    for (auto it = column_stats_.begin(); it != column_stats_.end();) {
        if (it->second.empty()) {
            it = column_stats_.erase(it);
        } else {
            ++it;
        }
    }
    return dropped_offsets;
}

ankerl::unordered_dense::set<std::string> ColumnStats::segment_column_names() const {
    struct Tag {};
    using ExternalToInternalColumnStatType =
            semi::static_map<ColumnStatType, std::unordered_set<ColumnStatTypeInternal>, Tag>;
    ExternalToInternalColumnStatType::get(ColumnStatType::MINMAX) =
            std::unordered_set<ColumnStatTypeInternal>{ColumnStatTypeInternal::MIN, ColumnStatTypeInternal::MAX};
    ankerl::unordered_dense::set<std::string> res;
    for (const auto& [column, column_stat_types] : column_stats_) {
        for (const auto& column_stat_type : column_stat_types) {
            for (const auto& column_stat_type_internal : ExternalToInternalColumnStatType::get(column_stat_type)) {
                res.emplace(to_segment_column_name(column, column_stat_type_internal));
            }
        }
    }
    return res;
}

std::unordered_map<std::string, std::unordered_set<std::string>> ColumnStats::to_map() const {
    std::unordered_map<std::string, std::unordered_set<std::string>> res;
    for (const auto& [column, types] : column_stats_) {
        res[column] = {};
        for (const auto& type : types) {
            res[column].emplace(type_to_name(type));
        }
    }
    return res;
}

std::optional<Clause> ColumnStats::clause(const FieldCollection& all_fields) const {
    if (column_stats_.empty()) {
        return std::nullopt;
    }
    std::unordered_set<std::string> input_columns;
    auto index_generation_aggregators = std::make_shared<std::vector<ColumnStatsAggregator>>();
    std::shared_ptr<FieldCollection> all_fields_ptr = std::make_shared<FieldCollection>(all_fields.clone());
    for (const auto& [column, column_stat_types] : column_stats_) {
        input_columns.emplace(column);

        for (const auto& column_stat_type : column_stat_types) {
            switch (column_stat_type) {
            case ColumnStatType::MINMAX:
                index_generation_aggregators->emplace_back(MinMaxAggregator(
                        ColumnName(column),
                        ColumnName(to_segment_column_name(column, ColumnStatTypeInternal::MIN)),
                        ColumnName(to_segment_column_name(column, ColumnStatTypeInternal::MAX)),
                        all_fields_ptr
                ));
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised ColumnStatType");
            }
        }
    }
    return ColumnStatsGenerationClause(std::move(input_columns), index_generation_aggregators);
}

bool ColumnStats::empty() const {
    return column_stats_.empty();
}

bool ColumnStats::operator==(const ColumnStats& right) const { return column_stats_ == right.column_stats_; }

} // namespace arcticdb