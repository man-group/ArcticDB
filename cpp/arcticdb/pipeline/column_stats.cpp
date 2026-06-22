#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {

ankerl::unordered_dense::map<std::string, size_t> map_symbol_fields_to_offsets(const TimeseriesDescriptor& tsd) {
    ankerl::unordered_dense::map<std::string, size_t> field_to_offset;
    for (const auto& [index, field] : folly::enumerate(tsd.fields())) {
        field_to_offset.emplace(std::string{field.name()}, index);
    }
    return field_to_offset;
}

namespace merge_internal {

struct MergedSchema {
    std::vector<std::string> field_names;
    std::vector<TypeDescriptor> type_descriptors;
    ankerl::unordered_dense::map<std::string, size_t> name_to_index;
};

MergedSchema compute_merged_schema(const std::vector<SegmentInMemory>& col_stats_segments) {
    MergedSchema schema;
    for (auto& segment : col_stats_segments) {
        for (const auto& field : segment.descriptor().fields()) {
            const auto new_type = field.type();
            const std::string name{field.name()};
            if (auto it = schema.name_to_index.find(name); it != schema.name_to_index.end()) {
                auto& merged_type = schema.type_descriptors.at(it->second);
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
                schema.name_to_index.emplace(name, schema.type_descriptors.size());
                schema.type_descriptors.emplace_back(new_type);
                schema.field_names.emplace_back(name);
            }
        }
    }
    return schema;
}

arcticc::pb2::column_stats_pb2::ColumnStatsHeader build_column_stats_header(
        const MergedSchema& schema, const ankerl::unordered_dense::map<std::string, size_t>& symbol_fields_to_offsets
) {
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    header.set_version(1); // see column_stats.proto for explanation of the versioning scheme
    const auto end_index_offset = static_cast<size_t>(index::Fields::end_index);
    for (const auto& [idx, field_name] : folly::enumerate(schema.field_names)) {
        if (idx <= end_index_offset) {
            // start_index and end_index are not statistics
            continue;
        }
        auto parsed = parse_segment_column_name(field_name);
        auto offset_it = symbol_fields_to_offsets.find(parsed.column_name);
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                offset_it != symbol_fields_to_offsets.end(),
                "Column stats refer to column '{}' which is not present in the data's TimeseriesDescriptor",
                parsed.column_name
        );
        auto& entry_list = (*header.mutable_stats_by_column())[static_cast<uint32_t>(offset_it->second)];
        auto* new_entry = entry_list.add_entries();
        new_entry->set_stats_seg_offset(static_cast<uint32_t>(idx));
        new_entry->set_type(parsed.stat_type);
    }
    return header;
}

} // namespace merge_internal

SegmentInMemory merge_column_stats_segments(
        const std::vector<SegmentInMemory>& col_stats_segments,
        const ankerl::unordered_dense::map<std::string, size_t>& symbol_fields_to_offsets
) {
    using namespace merge_internal;

    auto schema = compute_merged_schema(col_stats_segments);

    SegmentInMemory merged(Sparsity::PERMITTED);
    merged.init_column_map();
    merged.descriptor().set_index(IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});
    for (const auto& [idx, type_descriptor] : folly::enumerate(schema.type_descriptors)) {
        merged.add_column(FieldRef{type_descriptor, schema.field_names.at(idx)}, 0, AllocationType::DYNAMIC);
    }
    for (auto& segment : col_stats_segments) {
        merged.append(segment);
    }
    merged.set_compacted(true);
    merged.sort(start_index_column_name);

    auto header = build_column_stats_header(schema, symbol_fields_to_offsets);
    google::protobuf::Any any;
    bool packed = any.PackFrom(header);
    util::check(packed, "Failed to pack merged column stats header into Any");
    merged.set_metadata(std::move(any));
    return merged;
}

std::string stat_to_operator_string(ColumnStatTypeInternal stat) {
    switch (stat) {
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

std::string stat_to_name(ColumnStatType stat) {
    switch (stat) {
    case ColumnStatType::MINMAX:
        return "MINMAX";
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown column stat type requested");
    }
}

std::optional<ColumnStatType> stat_name_to_stat(const std::string& name) {
    if (name != "MINMAX") {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>("Unknown column stat type provided: {}", name);
    }

    return ColumnStatType::MINMAX;
}

std::string column_and_stat_to_segment_name(const std::string& column, ColumnStatTypeInternal stat) {
    return fmt::format("{}({})", stat_to_operator_string(stat), column);
}

ParsedSegmentColumnName parse_segment_column_name(std::string_view segment_column_name) {
    // Expected format: "{prefix}({column_name})". The prefix identifies the stat
    // type; the column_name is the original (mangled) data-column name.
    auto open_paren = segment_column_name.find('(');
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            open_paren != std::string_view::npos && !segment_column_name.empty() && segment_column_name.back() == ')',
            "Malformed column stats segment field name: '{}'",
            segment_column_name
    );
    auto prefix = segment_column_name.substr(0, open_paren);
    auto column_name = segment_column_name.substr(open_paren + 1, segment_column_name.size() - open_paren - 2);

    ColumnStatTypeInternal stat_type;
    if (prefix == "v1_MIN") {
        stat_type = ColumnStatTypeInternal::MIN_V1;
    } else if (prefix == "v1_MAX") {
        stat_type = ColumnStatTypeInternal::MAX_V1;
    } else if (prefix == "v1_NAN_COUNT") {
        stat_type = ColumnStatTypeInternal::NAN_COUNT_V1;
    } else if (prefix == "v1_NULL_COUNT") {
        stat_type = ColumnStatTypeInternal::NULL_COUNT_V1;
    } else {
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Unknown column stats prefix '{}' in segment field name '{}'", prefix, segment_column_name
        );
    }
    return {std::string{column_name}, stat_type};
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

            auto it = offset_to_input_column_and_stats_.find(data_col_offset);

            if (it != offset_to_input_column_and_stats_.end()) {
                it->second.column_stats.insert(external_type);
            } else {
                std::string name{tsd.fields().at(data_col_offset).name()};
                offset_to_input_column_and_stats_.emplace(data_col_offset, NameAndStats{name, {external_type}});
            }
        }
    }
    offset_to_input_column_and_stats_calculated_ = true;
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

        offset_to_input_column_and_stats.emplace(
                field_index, NameAndStatTypes{std::move(field_name), {ColumnStatType::MINMAX}}
        );
    }
    offset_to_stat_info_set_ = true;
}

std::unordered_set<std::string> get_unmangled_column_names(
        const std::map<std::string, std::set<ColumnStatType>>& column_name_to_stats
) {
    std::unordered_set<std::string> unmangled_names;
    for (const auto& k : column_name_to_stats | ranges::views::keys) {
        unmangled_names.insert(k);
    }
    return unmangled_names;
}

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

MinMaxAggregator create_minmax_aggregator(const std::string& input_column_name, size_t input_column_offset) {
    return MinMaxAggregator(
            ColumnName(input_column_name),
            input_column_offset,
            ColumnName(column_and_stat_to_segment_name(input_column_name, ColumnStatTypeInternal::MIN_V1)),
            ColumnName(column_and_stat_to_segment_name(input_column_name, ColumnStatTypeInternal::MAX_V1)),
            ColumnName(column_and_stat_to_segment_name(input_column_name, ColumnStatTypeInternal::NAN_COUNT_V1)),
            ColumnName(column_and_stat_to_segment_name(input_column_name, ColumnStatTypeInternal::NULL_COUNT_V1))
    );
}

ColumnStatsAggregator create_aggregator(
        const std::string& input_column_name, size_t input_column_offset, ColumnStatType stat
) {
    switch (stat) {
    case ColumnStatType::MINMAX:
        return create_minmax_aggregator(input_column_name, input_column_offset);
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unrecognised ColumnStatType");
    }
}
} // namespace

std::vector<std::string> ColumnStats::drop(const ColumnStats& to_drop, bool warn_if_missing) {
    util::check(
            offset_to_input_column_and_stats_calculated_, "Expect this->offset to input column and stats to be set"
    );
    util::check(
            to_drop.offset_to_input_column_and_stats_calculated_,
            "Expect to_drop.offset to input column and stats to be set"
    );
    std::vector<std::string> dropped_names;
    for (const auto& [offset, input_column_and_stats] : to_drop.offset_to_input_column_and_stats_) {
        if (auto it = offset_to_input_column_and_stats_.find(offset); it == offset_to_input_column_and_stats_.end()) {
            if (warn_if_missing) {
                log::version().warn(
                        "Requested column stats drop but column '{}' does not have any column stats",
                        input_column_and_stats.mangled_name
                );
            }
        } else {
            for (const auto& column_stat : input_column_and_stats.column_stats) {
                bool none_erased = it->second.column_stats.erase(column_stat) == 0;
                if (none_erased) {
                    if (warn_if_missing) {
                        log::version().warn(
                                "Requested column stats drop but column '{}' does not have the specified column stat "
                                "'{}'",
                                input_column_and_stats.mangled_name,
                                stat_to_name(column_stat)
                        );
                    }
                } else {
                    for (const auto& internal_type : external_to_internal(column_stat)) {
                        dropped_names.emplace_back(
                                column_and_stat_to_segment_name(input_column_and_stats.mangled_name, internal_type)
                        );
                    }
                }
            }
        }
    }
    for (auto it = offset_to_input_column_and_stats_.begin(); it != offset_to_input_column_and_stats_.end();) {
        if (it->second.column_stats.empty()) {
            it = offset_to_input_column_and_stats_.erase(it);
        } else {
            ++it;
        }
    }
    return dropped_names;
}

std::unordered_map<std::string, std::unordered_set<std::string>> ColumnStats::to_map() const {
    util::check(
            offset_to_input_column_and_stats_calculated_, "Expect offset_to_input_column_and_stats to be set in to_map"
    );
    std::unordered_map<std::string, std::unordered_set<std::string>> res;
    for (const auto& [offset, input_column_and_stats] : offset_to_input_column_and_stats_) {
        auto& entry = res[input_column_and_stats.mangled_name];
        for (const auto& stat : input_column_and_stats.column_stats) {
            entry.emplace(stat_to_name(stat));
        }
    }
    return res;
}

std::optional<Clause> ColumnStats::clause() const {
    if (empty()) {
        return std::nullopt;
    }

    util::check(offset_to_input_column_and_stats_calculated_, "Expect offset_to_input_column_and_stats to be set");
    std::unordered_set<std::string> input_columns;
    auto aggregators = std::make_shared<std::vector<ColumnStatsAggregator>>();

    for (const auto& [input_column_offset, input_column_and_stats] : offset_to_input_column_and_stats_) {
        input_columns.emplace(input_column_and_stats.mangled_name);

        for (const auto& column_stat : input_column_and_stats.column_stats) {
            aggregators->emplace_back(
                    create_aggregator(input_column_and_stats.mangled_name, input_column_offset, column_stat)
            );
        }
    }

    return ColumnStatsGenerationClause(std::move(input_columns), aggregators);
}

bool ColumnStats::empty() const { return offset_to_input_column_and_stats_.empty(); }

bool ColumnStats::operator==(const ColumnStats& right) const {
        return offset_to_input_column_and_stats_ == right.offset_to_input_column_and_stats_;
}

} // namespace arcticdb
