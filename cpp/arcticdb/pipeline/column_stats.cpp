#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

SegmentInMemory merge_column_stats_segments(const std::vector<SegmentInMemory>& segments) {
    SegmentInMemory merged(Sparsity::PERMITTED);
    merged.init_column_map();
    merged.descriptor().set_index(IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});

    // Maintain the order of the columns in the input segments
    ankerl::unordered_dense::map<std::string, size_t> field_name_to_index;
    std::vector<TypeDescriptor> type_descriptors;
    std::vector<std::string> field_names;
    std::vector<arcticc::pb2::column_stats_pb2::ColumnStatsType> stat_types;
    std::vector<size_t> data_col_offsets;
    for (auto& segment : segments) {
        arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
        auto metadata = segment.metadata();
        util::check(metadata != nullptr, "Column stats segment has no metadata");
        bool unpacked = metadata->UnpackTo(&header);
        util::check(unpacked, "Could not unpack column stats metadata?");

        // Build reverse lookup: stats_seg_offset -> (data_col_offset, type)
        ankerl::unordered_dense::map<uint32_t, std::pair<uint32_t, arcticc::pb2::column_stats_pb2::ColumnStatsType>>
                offset_lookup;
        for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
            for (const auto& entry : entry_list.entries()) {
                offset_lookup[entry.stats_seg_offset()] = {data_col_offset, entry.type()};
            }
        }

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
                    auto& [dcol, stype] = offset_lookup.at(idx);
                    stat_types.emplace_back(stype);
                    data_col_offsets.emplace_back(dcol);
                }
            }
        }
    }

    arcticc::pb2::column_stats_pb2::ColumnStatsHeader merged_header;
    merged_header.set_version(1); // see column_stats.proto for explanation of the versioning scheme
    auto end_index_offset = static_cast<size_t>(index::Fields::end_index);
    size_t stat_idx = 0;
    for (const auto& [idx, type_descriptor] : folly::enumerate(type_descriptors)) {
        merged.add_column(FieldRef{type_descriptor, field_names.at(idx)}, 0, AllocationType::DYNAMIC);
        if (idx > end_index_offset) {
            auto& entry_list = (*merged_header.mutable_stats_by_column())[data_col_offsets.at(stat_idx)];
            auto* new_entry = entry_list.add_entries();
            new_entry->set_stats_seg_offset(idx);
            new_entry->set_type(stat_types.at(stat_idx));
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
    switch (type) {
    case ColumnStatTypeInternal::MIN_V1:
        return "v1_MIN";
    case ColumnStatTypeInternal::MAX_V1:
        return "v1_MAX";
    case ColumnStatTypeInternal::NAN_COUNT_V1:
        return "v1_NAN_COUNT";
    case ColumnStatTypeInternal::NULL_COUNT_V1:
        return "v1_NULL_COUNT";
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown column stat type requested");
    }
}

std::string type_to_name(ColumnStatType type) {
    switch (type) {
    case ColumnStatType::MINMAX:
        return "MINMAX";
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown column stat type requested");
    }
}

std::optional<ColumnStatType> name_to_type(const std::string& name) {
    if (name == "MINMAX") {
        return ColumnStatType::MINMAX;
    }
    return std::nullopt;
}

std::string to_segment_column_name(const std::string& column, ColumnStatTypeInternal type) {
    return fmt::format("{}({})", type_to_operator_string(type), column);
}

void validate_column_stats_header_version(const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header) {
    auto version = header.version();
    if (version > 1) {
        log::version().warn(
                "This client only understands column stats version 1 but has encountered version={}. Upgrade your "
                "ArcticDB "
                "installation.",
                version
        );
    }
}

ColumnStats::ColumnStats(
        const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
) {
    using namespace arcticc::pb2::column_stats_pb2;
    validate_column_stats_header_version(header);

    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        for (const auto& entry : entry_list.entries()) {
            ColumnStatType external_type;
            switch (entry.type()) {
            case MIN_V1:
            case MAX_V1:
            case NAN_COUNT_V1:
            case NULL_COUNT_V1:
                external_type = ColumnStatType::MINMAX; // null and nan are calculated inline with minmax
                break;
            case UNKNOWN:
            default:
                log::version().warn(
                        "Unrecognised column stats type in header. Upgrade your ArcticDB installation. Skipping stat."
                );
                continue;
            }
            if (auto it = offset_to_stat_info_.find(data_col_offset); it != offset_to_stat_info_.end()) {
                it->second.column_stats.insert(external_type);
            } else {
                std::string name{tsd.fields().at(data_col_offset).name()};
                offset_to_stat_info_.emplace(data_col_offset, NameAndStatTypes{name, {external_type}});
            }
        }
    }
    offset_to_stat_info_set_ = true;
}

namespace {
bool is_col_eligible_for_stats(DataType col_data_type) {
    return is_numeric_type(col_data_type) || is_bool_type(col_data_type);
}

// Type-disambiguated key for duplicate detection, so that e.g. an integer column labelled 2
// and a string column labelled "2" are not treated as duplicates.
std::string to_user_facing_name_key(
        std::string_view field_name, const arcticdb::proto::descriptors::NormalizationMetadata::Pandas& common
) {
    auto it = common.col_names().find(std::string{field_name});
    if (it == common.col_names().end())
        return "str:" + std::string{field_name};

    const auto& info = it->second;
    if (info.is_none())
        return "none:";
    if (info.is_empty())
        return "empty:";
    if (info.is_int())
        return "int:" + info.original_name();
    if (!info.original_name().empty())
        return "str:" + info.original_name();

    return "str:" + std::string{field_name};
}

// The denormalized column name as the user sees it, for error messages.
std::string to_user_facing_display_name(
        std::string_view field_name, const arcticdb::proto::descriptors::NormalizationMetadata::Pandas& common
) {
    auto it = common.col_names().find(std::string{field_name});
    if (it == common.col_names().end())
        return std::string{field_name};

    const auto& info = it->second;
    if (info.is_none())
        return "None";
    if (info.is_empty())
        return "";
    if (info.is_int() || !info.original_name().empty())
        return info.original_name();

    return std::string{field_name};
}

} // namespace

// Build MINMAX stats for every eligible column, computed directly from the TSD.
// The timeseries index is skipped.
// Rejects symbols with duplicated data-column names.
ColumnStats::ColumnStats(const TimeseriesDescriptor& tsd) {
    const auto& fields = tsd.fields();
    const auto& norm = tsd.normalization();

    const bool has_timeseries_index = tsd.index().field_count() > 0;
    const size_t start_field_index = has_timeseries_index ? 1 : 0;

    std::unordered_set<std::string> seen_user_names;

    for (const auto& [field_index, field] : folly::enumerate(fields)) {
        if (field_index < start_field_index) {
            continue;
        }
        if (!is_col_eligible_for_stats(field.type().data_type())) {
            continue;
        }

        std::string field_name{field.name()};

        if (norm.has_df()) {
            const auto& common = norm.df().common();
            if (!seen_user_names.insert(to_user_facing_name_key(field.name(), common)).second) {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                        "Cannot create column stats: symbol has duplicated data column name [{}]",
                        to_user_facing_display_name(field.name(), common)
                );
            }
        }

        offset_to_stat_info_.emplace(field_index, NameAndStatTypes{std::move(field_name), {ColumnStatType::MINMAX}});
    }
    offset_to_stat_info_set_ = true;
}

namespace {
std::unordered_set<ColumnStatTypeInternal> external_to_internal(ColumnStatType type) {
    switch (type) {
    case ColumnStatType::MINMAX:
        return {ColumnStatTypeInternal::MIN_V1,
                ColumnStatTypeInternal::MAX_V1,
                ColumnStatTypeInternal::NAN_COUNT_V1,
                ColumnStatTypeInternal::NULL_COUNT_V1};
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown column stat type");
    }
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
                        ColumnName(
                                to_segment_column_name(name_and_stat_types.mangled_name, ColumnStatTypeInternal::MIN_V1)
                        ),
                        ColumnName(
                                to_segment_column_name(name_and_stat_types.mangled_name, ColumnStatTypeInternal::MAX_V1)
                        ),
                        ColumnName(to_segment_column_name(
                                name_and_stat_types.mangled_name, ColumnStatTypeInternal::NAN_COUNT_V1
                        )),
                        ColumnName(to_segment_column_name(
                                name_and_stat_types.mangled_name, ColumnStatTypeInternal::NULL_COUNT_V1
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

bool ColumnStats::empty() const { return offset_to_stat_info_.empty(); }

bool ColumnStats::operator==(const ColumnStats& right) const {
    return offset_to_stat_info_ == right.offset_to_stat_info_;
}

} // namespace arcticdb
