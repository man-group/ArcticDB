#include <pipeline/column_stats.hpp>

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
    for (auto& segment : segments) {
        for (const auto& field : segment.descriptor().fields()) {
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
            }
        }
    }
    for (const auto& type_descriptor : folly::enumerate(type_descriptors)) {
        merged.add_column(
                FieldRef{*type_descriptor, field_names.at(type_descriptor.index)}, 0, AllocationType::DYNAMIC
        );
    }
    for (auto& segment : segments) {
        merged.append(segment);
    }
    merged.set_compacted(true);
    merged.sort(start_index_column_name);
    return merged;
}

// Needed as MINMAX maps to 2 columns in the column stats object
enum class ColumnStatTypeInternal { MIN, MAX };

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

std::string to_segment_column_name(const std::string& column, ColumnStatTypeInternal type) {
    return fmt::format("{}({})", type_to_operator_string(type), column);
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
    }
}

ColumnStats::ColumnStats(
        const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const StreamDescriptor& data_descriptor
) {
    using namespace arcticc::pb2::descriptors_pb2;
    for (const auto& mapping : header.stats()) {
        auto column_name = std::string{data_descriptor.field(mapping.data_col_offset()).name()};
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
        } else {
            it->second.emplace(external_type);
        }
    }
}

void ColumnStats::drop(const ColumnStats& to_drop, bool warn_if_missing) {
    for (const auto& [column, column_stat_types] : to_drop.column_stats_) {
        if (auto it = column_stats_.find(column); it == column_stats_.end()) {
            if (warn_if_missing) {
                log::version().warn(
                        "Requested column stats drop but column '{}' does not have any column stats", column
                );
            }
        } else {
            for (const auto& column_stat_type : column_stat_types) {
                if (it->second.erase(column_stat_type) == 0 && warn_if_missing) {
                    log::version().warn(
                            "Requested column stats drop but column '{}' does not have the specified column stat '{}'",
                            column,
                            type_to_name(column_stat_type)
                    );
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

std::optional<Clause> ColumnStats::clause() const {
    if (column_stats_.empty()) {
        return std::nullopt;
    }
    std::unordered_set<std::string> input_columns;
    auto index_generation_aggregators = std::make_shared<std::vector<ColumnStatsAggregator>>();
    for (const auto& [column, column_stat_types] : column_stats_) {
        input_columns.emplace(column);

        for (const auto& column_stat_type : column_stat_types) {
            switch (column_stat_type) {
            case ColumnStatType::MINMAX:
                index_generation_aggregators->emplace_back(MinMaxAggregator(
                        ColumnName(column),
                        ColumnName(to_segment_column_name(column, ColumnStatTypeInternal::MIN)),
                        ColumnName(to_segment_column_name(column, ColumnStatTypeInternal::MAX))
                ));
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised ColumnStatType");
            }
        }
    }
    return ColumnStatsGenerationClause(std::move(input_columns), index_generation_aggregators);
}

bool ColumnStats::operator==(const ColumnStats& right) const { return column_stats_ == right.column_stats_; }

void ColumnStats::merge(const ColumnStats& other) {
    for (const auto& [column, stat_types] : other.column_stats_) {
        column_stats_[column].insert(stat_types.begin(), stat_types.end());
    }
}

arcticc::pb2::descriptors_pb2::ColumnStatsHeader ColumnStats::build_header(
        const StreamDescriptor& data_descriptor, const StreamDescriptor& stats_descriptor
) const {
    using namespace arcticc::pb2::descriptors_pb2;
    ColumnStatsHeader header;
    header.set_major_version(1);
    header.set_minor_version(0);

    // Maps external ColumnStatType to internal types with their proto enum values
    const std::map<ColumnStatType, std::vector<std::pair<ColumnStatTypeInternal, ColumnStatsType>>>
            external_to_internal = {
                    {ColumnStatType::MINMAX,
                     {{ColumnStatTypeInternal::MIN, COLUMN_STATS_MIN},
                      {ColumnStatTypeInternal::MAX, COLUMN_STATS_MAX}}},
    };

    for (const auto& [source_column, stat_types] : column_stats_) {
        auto data_col_offset = data_descriptor.find_field(source_column);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                data_col_offset.has_value(),
                "Column stats source column '{}' not found in data descriptor",
                source_column
        );

        for (const auto& stat_type : stat_types) {
            auto it = external_to_internal.find(stat_type);
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    it != external_to_internal.end(), "Unrecognised ColumnStatType"
            );
            for (const auto& [internal_type, proto_type] : it->second) {
                auto stats_col_name = to_segment_column_name(source_column, internal_type);
                auto stats_seg_offset = stats_descriptor.find_field(stats_col_name);
                internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                        stats_seg_offset.has_value(),
                        "Column stats field '{}' not found in stats segment descriptor",
                        stats_col_name
                );
                auto* mapping = header.add_stats();
                mapping->set_stats_seg_offset(static_cast<uint32_t>(*stats_seg_offset));
                mapping->set_data_col_offset(static_cast<uint32_t>(*data_col_offset));
                mapping->set_type(proto_type);
            }
        }
    }
    return header;
}

} // namespace arcticdb
