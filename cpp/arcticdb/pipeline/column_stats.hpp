#pragma once

#include <arcticdb/processing/clause.hpp>
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
static constexpr size_t start_index_column_offset = 0;
static const char* const end_index_column_name = "end_index";
static constexpr size_t end_index_column_offset = 1;

enum class ColumnStatElement { MIN, MAX };

/**
 * Parse a column stats segment column name and extract the stat type (MIN or MAX) and original column name.
 * Expected format: "vX.Y_MIN(column)" or "vX.Y_MAX(column)"
 *
 * @param segment_column_name The column name from the stats segment
 */
std::pair<std::string, ColumnStatElement> from_segment_column_name_to_internal(std::string_view segment_column_name);

std::pair<std::string, ColumnStatType> from_segment_column_name_to_external(std::string_view segment_column_name);

class ColumnStats {
  public:
    explicit ColumnStats(const std::unordered_map<std::string, std::unordered_set<std::string>>& column_stats);
    explicit ColumnStats(const FieldCollection& column_stats_fields);

    void drop(const ColumnStats& to_drop, bool warn_if_missing = true);
    ankerl::unordered_dense::set<std::string> segment_column_names() const;

    std::unordered_map<std::string, std::unordered_set<std::string>> to_map() const;
    std::optional<Clause> clause() const;

    bool operator==(const ColumnStats& right) const;

  private:
    // Use ordered map/set here for consistent ordering in the resulting stats objects
    std::map<std::string, std::set<ColumnStatType>> column_stats_;
    // If the fields ctor is used, store the major and minor version numbers as a pair so we can reconstruct the column
    // names
    std::optional<std::pair<uint64_t, uint64_t>> version_{std::nullopt};

    void parse_version(std::string_view version_string);
};

} // namespace arcticdb