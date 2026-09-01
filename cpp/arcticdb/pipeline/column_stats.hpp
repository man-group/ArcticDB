#pragma once

#include <arcticdb/entity/stream_descriptor.hpp>
#include <arcticdb/pipeline/column_name_resolution.hpp>
#include <arcticdb/processing/clause.hpp>
#include <arcticdb/pipeline/column_stats_types.hpp>
#include <arcticdb/pipeline/index_fields.hpp>
#include <column_stats.pb.h>
#include <ankerl/unordered_dense.h>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace arcticdb {

SegmentInMemory build_column_stats_segment(
        std::vector<ColumnStatsRow>&& column_stats_rows, const StreamDescriptor& descriptor
);

std::vector<ColumnStatsRow> decode_column_stats_segment(const SegmentInMemory& segment);

// User facing types - eg users are only allowed to create min and max together, not one or the other
enum class ColumnStatType { MINMAX };

static const char* const start_row_column_name = "start_row";
static constexpr size_t start_row_column_offset = 0;
static const char* const end_row_column_name = "end_row";
static constexpr size_t end_row_column_offset = 1;

struct NameAndStatTypes {
    std::string mangled_name;
    std::set<ColumnStatType> column_stats;

    bool operator==(const NameAndStatTypes& right) const {
        return mangled_name == right.mangled_name && column_stats == right.column_stats;
    }
};

// The version of the ColumnStatsHeader written by this build. See column_stats.proto for an
// explanation of the header versioning scheme.
static constexpr uint32_t CURRENT_COLUMN_STATS_HEADER_VERSION = 1;

enum class ColumnStatsHeaderVersionMismatchAction { Warn, Raise };

void validate_column_stats_header_version(
        const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, ColumnStatsHeaderVersionMismatchAction action
);

std::string to_segment_column_name(std::string_view column, ColumnStatTypeInternal type);

class ColumnStats {
  public:
    explicit ColumnStats(const TimeseriesDescriptor& tsd);
    explicit ColumnStats(
            const arcticc::pb2::column_stats_pb2::ColumnStatsHeader& header, const TimeseriesDescriptor& tsd
    );

    std::unordered_map<std::string, std::unordered_set<std::string>> to_map() const;
    std::optional<Clause> clause() const;
    bool empty() const;

    bool operator==(const ColumnStats& right) const;

  private:
    std::unordered_map<size_t, NameAndStatTypes> offset_to_stat_info_;
    bool offset_to_stat_info_set_{false};
};

} // namespace arcticdb
