#pragma once

#include <arcticdb/processing/clause.hpp>
#include <descriptors.pb.h>
#include <ankerl/unordered_dense.h>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace arcticdb {

SegmentInMemory merge_column_stats_segments(const std::vector<SegmentInMemory>& segments);

enum class ColumnStatType { MINMAX };

static const char* const start_index_column_name = "start_index";
static const char* const end_index_column_name = "end_index";

class ColumnStats {
  public:
    explicit ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats);
    explicit ColumnStats(
            const arcticc::pb2::descriptors_pb2::ColumnStatsHeader& header, const StreamDescriptor& data_descriptor
    );

    void drop(const ColumnStats& to_drop, bool warn_if_missing = true);
    ankerl::unordered_dense::set<std::string> segment_column_names() const;

    std::unordered_map<std::string, std::unordered_set<std::string>> to_map() const;
    std::optional<Clause> clause() const;

    bool operator==(const ColumnStats& right) const;

  private:
    // Use ordered map/set here for consistent ordering in the resulting stats objects
    std::map<std::string, std::set<ColumnStatType>> column_stats_;
};

arcticc::pb2::descriptors_pb2::ColumnStatsHeader build_column_stats_header(
        const SegmentInMemory& stats_segment, const StreamDescriptor& data_descriptor
);

} // namespace arcticdb
