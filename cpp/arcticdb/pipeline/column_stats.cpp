#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/timeseries_descriptor.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/column_store/column_algorithms.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <algorithm>
#include <tuple>
#include <unordered_set>

namespace arcticdb {

namespace {
struct StatColumn {
    std::string name;
    ColumnStatTypeInternal type;
    size_t data_col_offset;
    TypeDescriptor type_descriptor;
};

// Identifies a stat column
struct StatKey {
    ColumnStatTypeInternal type;
    size_t data_col_offset;

    friend bool operator==(const StatKey& left, const StatKey& right) {
        return left.type == right.type && left.data_col_offset == right.data_col_offset;
    }
};

struct StatKeyHash {
    size_t operator()(const StatKey& key) const noexcept {
        return folly::hash::hash_combine(key.type, key.data_col_offset);
    }
};

bool is_count_stat(ColumnStatTypeInternal type) { return type == ColumnStatTypeInternal::ISNULL_COUNT_V1; }

std::vector<StatColumn> collect_stat_columns(
        const std::vector<ColumnStatsRow>& column_stats_rows, const StreamDescriptor& descriptor,
        ankerl::unordered_dense::map<StatKey, size_t, StatKeyHash>& stat_key_to_index
) {
    std::vector<StatColumn> stat_columns;
    for (const auto& column_stats_row : column_stats_rows) {
        for (const auto& stat : column_stats_row.stats) {
            const StatKey stat_key{stat.type, stat.data_col_offset};
            if (stat_key_to_index.contains(stat_key)) {
                continue;
            }
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    stat.data_col_offset < descriptor.field_count(),
                    "Column stats data_col_offset {} is out of range for a descriptor with {} fields. Use "
                    "drop_column_stats_experimental and recreate the column stats.",
                    stat.data_col_offset,
                    descriptor.field_count()
            );
            const auto resolved_type = is_count_stat(stat.type) ? make_scalar_type(DataType::UINT64)
                                                                : descriptor.field(stat.data_col_offset).type();
            auto name = to_segment_column_name(descriptor.field(stat.data_col_offset).name(), stat.type);
            stat_key_to_index.emplace(stat_key, stat_columns.size());
            stat_columns.emplace_back(StatColumn{std::move(name), stat.type, stat.data_col_offset, resolved_type});
        }
    }
    return stat_columns;
}

// The target is the descriptor's field type, which merge_descriptors resolved as a common type over
// every slice's type for that column, so the static_cast is OK. The source type is needed to
// interpret Value's raw bytes, hence the nested visit.
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

SegmentInMemory build_column_stats_segment(
        std::vector<ColumnStatsRow>&& column_stats_rows, const StreamDescriptor& descriptor
) {
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !column_stats_rows.empty(), "build_column_stats_segment requires at least one component"
    );
    std::sort(column_stats_rows.begin(), column_stats_rows.end(), [](const auto& left, const auto& right) {
        return left.row_range.start() < right.row_range.start();
    });

    auto start_row_col = std::make_shared<Column>(
            make_scalar_type(DataType::UINT64),
            column_stats_rows.size(),
            AllocationType::PRESIZED,
            Sparsity::NOT_PERMITTED
    );
    auto end_row_col = std::make_shared<Column>(
            make_scalar_type(DataType::UINT64),
            column_stats_rows.size(),
            AllocationType::PRESIZED,
            Sparsity::NOT_PERMITTED
    );
    using RowTDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
    auto start_row_data = start_row_col->data();
    auto end_row_data = end_row_col->data();
    auto start_it = start_row_data.begin<RowTDT>();
    auto end_it = end_row_data.begin<RowTDT>();
    for (size_t i = 0; i < column_stats_rows.size(); ++i, ++start_it, ++end_it) {
        internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                column_stats_rows.at(i).row_range.end() > column_stats_rows.at(i).row_range.start(),
                "Column stats component has empty row range [{}, {})",
                column_stats_rows.at(i).row_range.start(),
                column_stats_rows.at(i).row_range.end()
        );
        if (i > 0) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    column_stats_rows.at(i).row_range.start() > column_stats_rows.at(i - 1).row_range.start(),
                    "Column stats components must have strictly increasing start_row, got [{}, {}) after [{}, {})",
                    column_stats_rows.at(i).row_range.start(),
                    column_stats_rows.at(i).row_range.end(),
                    column_stats_rows.at(i - 1).row_range.start(),
                    column_stats_rows.at(i - 1).row_range.end()
            );
        }
        *start_it = column_stats_rows.at(i).row_range.start();
        *end_it = column_stats_rows.at(i).row_range.end();
    }

    ankerl::unordered_dense::map<StatKey, size_t, StatKeyHash> stat_key_to_index;
    const auto stat_columns = collect_stat_columns(column_stats_rows, descriptor, stat_key_to_index);

    const auto last_row = static_cast<ssize_t>(column_stats_rows.size()) - 1;
    SegmentInMemory seg(Sparsity::PERMITTED);
    seg.init_column_map();
    seg.descriptor().set_index(IndexDescriptorImpl{IndexDescriptor::Type::ROWCOUNT, 0});

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
    for (const auto& [row, column_stats_row] : folly::enumerate(column_stats_rows)) {
        for (const auto& stat : column_stats_row.stats) {
            const StatKey stat_key{stat.type, stat.data_col_offset};
            set_stat_value(*columns.at(stat_key_to_index.at(stat_key)), row, stat.value);
        }
    }

    const auto stats_offset_base = seg.descriptor().field_count();
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    header.set_version(CURRENT_COLUMN_STATS_HEADER_VERSION);
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
    case ColumnStatTypeInternal::ISNULL_COUNT_V1:
        return "v1_ISNULL_COUNT";
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>("Unknown column stat type requested");
    }
}

std::vector<ColumnStatsRow> decode_column_stats_segment(const SegmentInMemory& segment) {
    if (segment.row_count() == 0) {
        return {};
    }

    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            segment.metadata(), "Column stats segment is missing its header metadata"
    );
    arcticc::pb2::column_stats_pb2::ColumnStatsHeader header;
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            segment.metadata()->UnpackTo(&header), "Failed to unpack column stats header from segment metadata"
    );
    // decode_column_stats_segment is only used for creating/extending column stats, so it is OK to fatally
    // error on an unrecognised header version here
    validate_column_stats_header_version(header, ColumnStatsHeaderVersionMismatchAction::Raise);

    const auto& fields = segment.fields();
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            fields.size() >= 2 && fields.at(start_row_column_offset).name() == start_row_column_name &&
                    fields.at(end_row_column_offset).name() == end_row_column_name,
            "Column stats segment does not have start_row/end_row columns at the expected offsets"
    );

    const auto& start_row_col = segment.column(start_row_column_offset);
    const auto& end_row_col = segment.column(end_row_column_offset);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            !start_row_col.is_sparse() && !end_row_col.is_sparse() &&
                    static_cast<size_t>(start_row_col.row_count()) == segment.row_count() &&
                    static_cast<size_t>(end_row_col.row_count()) == segment.row_count(),
            "Column stats start_row/end_row columns must be dense and cover every row of the segment"
    );

    std::vector<ColumnStatsRow> column_stats_rows(segment.row_count());
    for (size_t row = 0; row < segment.row_count(); ++row) {
        column_stats_rows.at(row).row_range = pipelines::RowRange{
                *start_row_col.scalar_at<uint64_t>(static_cast<position_t>(row)),
                *end_row_col.scalar_at<uint64_t>(static_cast<position_t>(row))
        };
    }

    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        for (const auto& entry : entry_list.entries()) {
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    entry.type() != ColumnStatTypeInternal::UNKNOWN,
                    "Column stats header entry for data column {} has an unrecognised stat type - you need to upgrade "
                    "your ArcticDB client",
                    data_col_offset
            );
            const auto stats_seg_offset = entry.stats_seg_offset();
            internal::check<ErrorCode::E_ASSERTION_FAILURE>(
                    stats_seg_offset > end_row_column_offset && stats_seg_offset < fields.size(),
                    "Column stats header entry stats_seg_offset {} is out of range for a segment with {} fields",
                    stats_seg_offset,
                    fields.size()
            );

            const auto& column = segment.column(stats_seg_offset);
            details::visit_type(column.type().data_type(), [&]([[maybe_unused]] auto tag) {
                using type_info = ScalarTypeInfo<decltype(tag)>;
                for_each_enumerated<typename type_info::TDT>(column, [&](const auto& it) {
                    column_stats_rows.at(static_cast<size_t>(it.idx()))
                            .stats.emplace_back(ColumnStatValue{
                                    entry.type(), data_col_offset, Value{it.value(), type_info::data_type}
                            });
                });
            });
        }
    }
    return column_stats_rows;
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

std::string to_segment_column_name(std::string_view column, ColumnStatTypeInternal type) {
    return fmt::format("{}({})", type_to_operator_string(type), column);
}

void validate_column_stats_header_version(
        const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, ColumnStatsHeaderVersionMismatchAction action
) {
    const auto version = header.version();
    if (version > CURRENT_COLUMN_STATS_HEADER_VERSION) {
        const auto message = fmt::format(
                "This client only understands column stats version {} but has encountered version={}. Upgrade your "
                "ArcticDB installation.",
                CURRENT_COLUMN_STATS_HEADER_VERSION,
                version
        );
        if (action == ColumnStatsHeaderVersionMismatchAction::Raise) {
            internal::raise<ErrorCode::E_ASSERTION_FAILURE>(message);
        } else {
            log::version().warn(message);
        }
    }
}

ColumnStats::ColumnStats(
        const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
) {
    using namespace arcticc::pb2::column_stats_pb2;
    validate_column_stats_header_version(header, ColumnStatsHeaderVersionMismatchAction::Warn);

    for (const auto& [data_col_offset, entry_list] : header.stats_by_column()) {
        for (const auto& entry : entry_list.entries()) {
            ColumnStatType external_type;
            switch (entry.type()) {
            case MIN_V1:
            case MAX_V1:
            case ISNULL_COUNT_V1:
                external_type = ColumnStatType::MINMAX; // isnull count is calculated inline with minmax
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
} // namespace

// Build MINMAX stats for every eligible column, computed directly from the TSD.
ColumnStats::ColumnStats(const TimeseriesDescriptor& tsd) {
    const auto& fields = tsd.fields();

    for (const auto& [field_index, field] : folly::enumerate(fields)) {
        if (!is_col_eligible_for_stats(field.type().data_type())) {
            continue;
        }

        std::string field_name{field.name()};
        offset_to_stat_info_.emplace(field_index, NameAndStatTypes{std::move(field_name), {ColumnStatType::MINMAX}});
    }
    offset_to_stat_info_set_ = true;
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
                index_generation_aggregators->emplace_back(
                        MinMaxAggregator(ColumnName(name_and_stat_types.mangled_name), offset)
                );
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
