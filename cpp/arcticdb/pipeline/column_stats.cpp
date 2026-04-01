#include <pipeline/column_stats.hpp>

#include <pipeline/index_fields.hpp>

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
    std::vector<size_t> data_col_offsets;
    for (auto& segment : segments) {
        arcticc::pb2::descriptors_pb2::ColumnStatsHeader header;
        auto metadata = segment.metadata();
        util::check(metadata != nullptr, "Column stats segment has no metadata");
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
                    // Skip start_index and end_index which are not statistics
                    auto& header_info = header.stats().at(idx - end_index_offset - 1);
                    stat_types.emplace_back(header_info.type());
                    data_col_offsets.emplace_back(header_info.data_col_offset());
                }
            }
        }
    }

    arcticc::pb2::descriptors_pb2::ColumnStatsHeader merged_header;
    merged_header.set_version(1); // see descriptors.proto for explanation of the versioning scheme
    auto end_index_offset = static_cast<size_t>(index::Fields::end_index);
    size_t stat_idx = 0;
    for (const auto& [idx, type_descriptor] : folly::enumerate(type_descriptors)) {
        merged.add_column(FieldRef{type_descriptor, field_names.at(idx)}, 0, AllocationType::DYNAMIC);
        if (idx > end_index_offset) {
            auto* new_stats = merged_header.add_stats();
            new_stats->set_data_col_offset(data_col_offsets.at(stat_idx));
            new_stats->set_stats_seg_offset(idx);
            new_stats->set_type(stat_types.at(stat_idx));
            ++stat_idx;
        }
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
    TypeToOperatorStringMap::get(ColumnStatTypeInternal::COLUMN_STATS_MIN_V1) = "v1_MIN";
    TypeToOperatorStringMap::get(ColumnStatTypeInternal::COLUMN_STATS_MAX_V1) = "v1_MAX";
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
            user_specified_column_stats_[column] = {};
            for (const auto& column_stat_name : column_stat_names) {
                auto opt_index_type = name_to_type(column_stat_name);
                user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        opt_index_type.has_value(), "Unknown column stat type provided: {}", column_stat_name
                );
                user_specified_column_stats_[column].emplace(*opt_index_type);
            }
        }
    }
}

ColumnStats::ColumnStats(
        const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
) {
    using namespace arcticc::pb2::descriptors_pb2;
    auto version = header.version();
    compatibility::check<ErrorCode::E_UNRECOGNISED_COLUMN_STATS_VERSION>(
            version == 1,
            "This client only understands column stats version 1 but has encountered version={}. Upgrade your ArcticDB "
            "installation.",
            version
    );

    for (const auto& mapping : header.stats()) {
        ColumnStatType external_type;
        switch (mapping.type()) {
        case COLUMN_STATS_MIN_V1:
        case COLUMN_STATS_MAX_V1:
            external_type = ColumnStatType::MINMAX;
            break;
        case COLUMN_STATS_UNKNOWN:
        default:
            // TODO when we use column stats at read time, just ignore any missing.
            // For maintenance operations, we should raise.
            compatibility::raise<ErrorCode::E_UNRECOGNISED_COLUMN_STATS_VERSION>(
                    "Encountered unknown column stat. Upgrade your ArcticDB installation."
            );
        }
        if (auto it = offset_to_stat_info_.find(mapping.data_col_offset()); it != offset_to_stat_info_.end()) {
            it->second.column_stats.insert(external_type);
        } else {
            std::string name{tsd.fields().at(mapping.data_col_offset()).name()};
            offset_to_stat_info_.insert({mapping.data_col_offset(), {name, {external_type}}});
        }
    }
    offset_to_stat_info_set_ = true;
}

void ColumnStats::calculate_offsets(const TimeseriesDescriptor& tsd, utils::MissingColumnsBehavior missing_columns) {
    util::check(!offset_to_stat_info_set_, "Should not calculate offsets twice");
    std::unordered_set<std::string> unmangled_names;
    for (const auto& k : user_specified_column_stats_ | ranges::views::keys) {
        unmangled_names.insert(k);
    }
    auto offsets_and_mangled_names =
            utils::find_offset_and_mangled_name(unmangled_names, tsd.as_stream_descriptor(), missing_columns);
    util::check(
            missing_columns == utils::MissingColumnsBehavior::IGNORE_MISSING ||
                    offsets_and_mangled_names.size() == user_specified_column_stats_.size(),
            "Expect calculated offsets_and_mangled_names to match column_stats_"
    );
    for (auto& offset_and_mangled_name : offsets_and_mangled_names) {
        offset_to_stat_info_.insert(
                {offset_and_mangled_name.offset,
                 {offset_and_mangled_name.mangled_name,
                  user_specified_column_stats_.at(offset_and_mangled_name.unmangled_name)}}
        );
    }
    offset_to_stat_info_set_ = true;
}

namespace {
std::unordered_set<ColumnStatTypeInternal> external_to_internal(ColumnStatType type) {
    struct Tag {};
    using Map = semi::static_map<ColumnStatType, std::unordered_set<ColumnStatTypeInternal>, Tag>;
    Map::get(ColumnStatType::MINMAX
    ) = std::unordered_set{ColumnStatTypeInternal::COLUMN_STATS_MIN_V1, ColumnStatTypeInternal::COLUMN_STATS_MAX_V1};
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(Map::contains(type), "Unknown column stat type");
    return Map::get(type);
}
} // namespace

std::vector<std::string> ColumnStats::drop(const ColumnStats& to_drop, bool warn_if_missing) {
    util::check(offset_to_stat_info_set_, "Expect this->offset to stat info to be set");
    util::check(to_drop.offset_to_stat_info_set_, "Expect to_drop.offset to stat info to be set");
    std::vector<std::string> dropped_names;
    for (const auto& [offset, name_and_stat_types] : to_drop.offset_to_stat_info_) {
        if (auto it = offset_to_stat_info_.find(offset); it == offset_to_stat_info_.end()) {
            if (warn_if_missing) {
                log::version().warn(
                        "Requested column stats drop but column '{}' does not have any column stats",
                        name_and_stat_types.mangled_name
                );
            }
        } else {
            for (const auto& column_stat_type : name_and_stat_types.column_stats) {
                bool none_erased = it->second.column_stats.erase(column_stat_type) == 0;
                if (none_erased) {
                    if (warn_if_missing) {
                        log::version().warn(
                                "Requested column stats drop but column '{}' does not have the specified column stat "
                                "'{}'",
                                name_and_stat_types.mangled_name,
                                type_to_name(column_stat_type)
                        );
                    }
                } else {
                    for (const auto& internal_type : external_to_internal(column_stat_type)) {
                        dropped_names.emplace_back(
                                to_segment_column_name(name_and_stat_types.mangled_name, internal_type)
                        );
                    }
                }
            }
        }
    }
    for (auto it = offset_to_stat_info_.begin(); it != offset_to_stat_info_.end();) {
        if (it->second.column_stats.empty()) {
            it = offset_to_stat_info_.erase(it);
        } else {
            ++it;
        }
    }
    user_specified_column_stats_ = {};
    return dropped_names;
}

std::unordered_map<std::string, std::unordered_set<std::string>> ColumnStats::to_map() const {
    util::check(offset_to_stat_info_set_, "Expect offset_to_stat_info to be set in to_map");
    std::unordered_map<std::string, std::unordered_set<std::string>> res;
    for (const auto& [offset, name_and_stat_types] : offset_to_stat_info_) {
        auto& entry = res[name_and_stat_types.mangled_name];
        for (const auto& type : name_and_stat_types.column_stats) {
            entry.emplace(type_to_name(type));
        }
    }
    return res;
}

std::optional<Clause> ColumnStats::clause() const {
    if (empty()) {
        return std::nullopt;
    }
    util::check(offset_to_stat_info_set_, "Expect offset_to_stat_info to be set");
    std::unordered_set<std::string> input_columns;
    auto index_generation_aggregators = std::make_shared<std::vector<ColumnStatsAggregator>>();
    for (const auto& [offset, name_and_stat_types] : offset_to_stat_info_) {
        input_columns.emplace(name_and_stat_types.mangled_name);

        for (const auto& column_stat_type : name_and_stat_types.column_stats) {
            switch (column_stat_type) {
            case ColumnStatType::MINMAX:
                index_generation_aggregators->emplace_back(MinMaxAggregator(
                        ColumnName(name_and_stat_types.mangled_name),
                        offset,
                        ColumnName(to_segment_column_name(
                                name_and_stat_types.mangled_name, ColumnStatTypeInternal::COLUMN_STATS_MIN_V1
                        )),
                        ColumnName(to_segment_column_name(
                                name_and_stat_types.mangled_name, ColumnStatTypeInternal::COLUMN_STATS_MAX_V1
                        ))
                ));
                break;
            default:
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised ColumnStatType");
            }
        }
    }
    return ColumnStatsGenerationClause(std::move(input_columns), index_generation_aggregators);
}

bool ColumnStats::empty() const { return user_specified_column_stats_.empty() && offset_to_stat_info_.empty(); }

bool ColumnStats::operator==(const ColumnStats& right) const {
    if (offset_to_stat_info_set_) {
        return offset_to_stat_info_ == right.offset_to_stat_info_;
    } else {
        return user_specified_column_stats_ == right.user_specified_column_stats_;
    }
}

} // namespace arcticdb
