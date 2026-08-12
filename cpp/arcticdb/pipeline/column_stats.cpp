#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <algorithm>
#include <tuple>

namespace arcticdb {

namespace {
struct StatColumn {
    std::string name;
    ColumnStatTypeInternal type;
    size_t data_col_offset;
    TypeDescriptor type_descriptor;
};

// Distinct stat columns in first-appearance order, each widened to a type that can hold every
// component's value for it. Dynamic schema can give a different type per row slice.
std::vector<StatColumn> collect_stat_columns(
        const std::vector<ColumnStatsComponent>& components,
        ankerl::unordered_dense::map<std::string, size_t>& name_to_index
) {
    std::vector<StatColumn> stat_columns;
    for (const auto& component : components) {
        for (const auto& stat : component.stats) {
            auto new_type = make_scalar_type(stat.value.data_type());
            if (auto it = name_to_index.find(stat.segment_column_name); it != name_to_index.end()) {
                auto& merged_type = stat_columns.at(it->second).type_descriptor;
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
                name_to_index.emplace(stat.segment_column_name, stat_columns.size());
                stat_columns.emplace_back(
                        StatColumn{stat.segment_column_name, stat.type, stat.data_col_offset, new_type}
                );
            }
        }
    }
    return stat_columns;
}

// has_valid_common_type guarantees the target represents the source exactly, so a static_cast is
// faithful. The source type is needed to interpret Value's raw bytes, hence the nested visit.
void set_stat_value(Column& column, size_t row, const Value& value) {
    details::visit_type(column.type().data_type(), [&column, row, &value](auto target_tag) {
        using TargetRaw = typename ScalarTypeInfo<decltype(target_tag)>::RawType;
        details::visit_type(value.data_type(), [&column, row, &value](auto source_tag) {
            using SourceRaw = typename ScalarTypeInfo<decltype(source_tag)>::RawType;
            column.set_scalar<TargetRaw>(static_cast<ssize_t>(row), static_cast<TargetRaw>(value.get<SourceRaw>()));
        });
    });
}
} // namespace

SegmentInMemory build_column_stats_segment(std::vector<ColumnStatsComponent>&& components) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !components.empty(), "build_column_stats_segment requires at least one component"
    );
    std::sort(components.begin(), components.end(), [](const auto& left, const auto& right) {
        return std::tie(left.start_row, left.end_row) < std::tie(right.start_row, right.end_row);
    });

    ankerl::unordered_dense::map<std::string, size_t> name_to_index;
    const auto stat_columns = collect_stat_columns(components, name_to_index);

    const auto last_row = static_cast<ssize_t>(components.size()) - 1;
    SegmentInMemory seg(Sparsity::PERMITTED);
    seg.init_column_map();
    seg.descriptor().set_index(IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});

    auto start_row_col = std::make_shared<Column>(make_scalar_type(DataType::UINT64), Sparsity::NOT_PERMITTED);
    auto end_row_col = std::make_shared<Column>(make_scalar_type(DataType::UINT64), Sparsity::NOT_PERMITTED);
    for (const auto& component : components) {
        start_row_col->push_back<uint64_t>(component.start_row);
        end_row_col->push_back<uint64_t>(component.end_row);
    }
    start_row_col->set_row_data(last_row);
    end_row_col->set_row_data(last_row);
    seg.add_column(scalar_field(DataType::UINT64, start_row_column_name), start_row_col);
    seg.add_column(scalar_field(DataType::UINT64, end_row_column_name), end_row_col);

    std::vector<std::shared_ptr<Column>> columns;
    columns.reserve(stat_columns.size());
    for (const auto& stat_column : stat_columns) {
        columns.emplace_back(std::make_shared<Column>(stat_column.type_descriptor, Sparsity::PERMITTED));
    }
    // set_scalar creates and backfills the sparse map where a stat is missing from a row slice
    for (const auto& [row, component] : folly::enumerate(components)) {
        for (const auto& stat : component.stats) {
            set_stat_value(*columns.at(name_to_index.at(stat.segment_column_name)), row, stat.value);
        }
    }

    const auto stats_offset_base = seg.descriptor().field_count();
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    header.set_version(1); // see column_stats.proto for explanation of the versioning scheme
    for (const auto& [idx, stat_column] : folly::enumerate(stat_columns)) {
        columns.at(idx)->set_row_data(last_row);
        seg.add_column(FieldRef{stat_column.type_descriptor, stat_column.name}, columns.at(idx));
        auto* new_entry = (*header.mutable_stats_by_column())[stat_column.data_col_offset].add_entries();
        new_entry->set_stats_seg_offset(stats_offset_base + idx);
        new_entry->set_type(stat_column.type);
    }

    seg.set_row_id(last_row);
    seg.set_compacted(true);

    google::protobuf::Any any;
    bool packed = any.PackFrom(header);
    util::check(packed, "Failed to pack header in to Any?");
    seg.set_metadata(std::move(any));
    return seg;
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
