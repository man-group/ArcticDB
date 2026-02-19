#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/processing/aggregation_interface.hpp>
#include <arcticdb/processing/unsorted_aggregation.hpp>
#include <arcticdb/entity/type_utils.hpp>
#include <arcticdb/util/preconditions.hpp>

#include <semimap/semimap.h>

#include <charconv>

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

std::string type_to_operator_string(ColumnStatElement element) {
    struct Tag {};
    using TypeToOperatorStringMap = semi::static_map<ColumnStatElement, std::string, Tag>;
    TypeToOperatorStringMap::get(ColumnStatElement::MIN) = "MIN";
    TypeToOperatorStringMap::get(ColumnStatElement::MAX) = "MAX";
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            TypeToOperatorStringMap::contains(element), "Unknown column stat type requested"
    );
    return TypeToOperatorStringMap::get(element);
}

std::string to_segment_column_name_v1(
        const std::string& column, ColumnStatElement column_stat_element,
        std::optional<uint64_t> minor_version = std::nullopt
) {
    // Increment when modifying
    const uint64_t latest_minor_version = 0;
    return fmt::format(
            "v1.{}_{}({})",
            minor_version.value_or(latest_minor_version),
            type_to_operator_string(column_stat_element),
            column
    );
}

std::string to_segment_column_name(
        const std::string& column, ColumnStatElement column_stat_element,
        std::optional<std::pair<uint64_t, uint64_t>> version = std::nullopt
) {
    if (!version.has_value()) {
        // Use latest version
        return to_segment_column_name_v1(column, column_stat_element);
    } else {
        // Use version specified
        switch (version->first) {
        case 1:
            return to_segment_column_name_v1(column, column_stat_element, version->second);
        default:
            compatibility::raise<ErrorCode::E_UNRECOGNISED_COLUMN_STATS_VERSION>(
                    "Unrecognised major version number in column stats column name: {}", version->first
            );
        }
    }
}

// Expected to be of the form "<operation>(<column name>)"
// Note: pattern is taken by reference so the caller sees the updated view with the operator prefix stripped.
ColumnStatElement from_segment_column_name_v1_internal(std::string_view& pattern) {
    const semi::map<std::string, ColumnStatType> name_to_type_map;
    const ankerl::unordered_dense::map<std::string, ColumnStatElement> operator_string_to_type{
            {"MIN", ColumnStatElement::MIN}, {"MAX", ColumnStatElement::MAX}
    };
    std::optional<ColumnStatElement> type;
    for (const auto& [name, type_candidate] : operator_string_to_type) {
        if (pattern.find(name) == 0) {
            pattern = pattern.substr(name.size());
            type = type_candidate;
            break;
        }
    }
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            type.has_value(), "Unexpected column stat column prefix {}", pattern
    );
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            pattern.find('(') == 0 && pattern.rfind(')') == pattern.size() - 1,
            "Unexpected column stat column format: {}",
            pattern
    );
    return *type;
}

ColumnStatType convert_to_external_type(ColumnStatElement internal_type) {
    struct Tag {};
    using InternalToExternalColumnStatType = semi::static_map<ColumnStatElement, ColumnStatType, Tag>;
    InternalToExternalColumnStatType::get(ColumnStatElement::MIN) = ColumnStatType::MINMAX;
    InternalToExternalColumnStatType::get(ColumnStatElement::MAX) = ColumnStatType::MINMAX;
    return InternalToExternalColumnStatType::get(internal_type);
}

std::pair<std::string, ColumnStatElement> from_segment_column_name_v1(std::string_view pattern) {
    auto type = from_segment_column_name_v1_internal(pattern);
    return std::make_pair(std::string(pattern.substr(1, pattern.size() - 2)), type);
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

ColumnStats::ColumnStats(const FieldCollection& column_stats_fields) {
    for (const auto& field : column_stats_fields) {
        if (field.name() != start_index_column_name && field.name() != end_index_column_name) {
            auto [column_name, index_type] = from_segment_column_name_to_external(field.name());
            if (auto it = column_stats_.find(column_name); it == column_stats_.end()) {
                column_stats_[column_name] = {index_type};
            } else {
                it->second.emplace(index_type);
            }
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
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            version_.has_value(), "Cannot construct column stat column names without specified versions"
    );
    struct Tag {};
    using ExternalToInternalColumnStatType =
            semi::static_map<ColumnStatType, std::unordered_set<ColumnStatElement>, Tag>;
    ExternalToInternalColumnStatType::get(ColumnStatType::MINMAX) =
            std::unordered_set<ColumnStatElement>{ColumnStatElement::MIN, ColumnStatElement::MAX};
    ankerl::unordered_dense::set<std::string> res;
    for (const auto& [column, column_stat_types] : column_stats_) {
        for (const auto& column_stat_type : column_stat_types) {
            for (const auto& column_stat_type_internal : ExternalToInternalColumnStatType::get(column_stat_type)) {
                res.emplace(to_segment_column_name(column, column_stat_type_internal, version_));
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
                        ColumnName(to_segment_column_name(column, ColumnStatElement::MIN)),
                        ColumnName(to_segment_column_name(column, ColumnStatElement::MAX))
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

// Expected to be of the form "vX.Y"
std::pair<uint64_t, uint64_t> parse_version_internal(std::string_view version_string) {
    auto dot_position = version_string.find('.');
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            dot_position != std::string::npos,
            "Unexpected version string in column stats column name (expected vX.Y): {}",
            version_string
    );
    auto candidate = version_string.substr(1, dot_position - 1);
    uint64_t major_version = 0;
    auto result = std::from_chars(candidate.data(), candidate.data() + candidate.size(), major_version);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            result.ec != std::errc::invalid_argument,
            "Expected positive integer in version string, but got: {}",
            candidate
    );

    candidate = version_string.substr(dot_position + 1, std::string::npos);
    uint64_t minor_version = 0;
    result = std::from_chars(candidate.data(), candidate.data() + candidate.size(), minor_version);
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            result.ec != std::errc::invalid_argument,
            "Expected positive integer in version string, but got: {}",
            candidate
    );

    return std::make_pair(major_version, minor_version);
}

// Expected to be of the form "vX.Y"
void ColumnStats::parse_version(std::string_view version_string) { version_ = parse_version_internal(version_string); }

// Expected to be of the form "vX.Y_<version specific pattern>"
std::pair<std::string, ColumnStatType> from_segment_column_name_to_external(std::string_view segment_column_name) {
    auto res = from_segment_column_name_to_internal(segment_column_name);
    return {res.first, convert_to_external_type(res.second)};
}

std::pair<std::string, ColumnStatElement> from_segment_column_name_to_internal(std::string_view segment_column_name) {
    auto underscore_position = segment_column_name.find('_');
    internal::check<ErrorCode::E_ASSERTION_FAILURE>(
            underscore_position != std::string::npos,
            "Unexpected column stats column name (expected vX.Y_<version specific pattern>): {}",
            segment_column_name
    );
    auto version = parse_version_internal(segment_column_name.substr(0, underscore_position));
    auto version_specific_pattern = segment_column_name.substr(underscore_position + 1);
    switch (version.first) {
    case 1:
        return from_segment_column_name_v1(version_specific_pattern);
    default:
        internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                "Unsupported major version {} when parsing column stats column name", version.first
        );
    }
}

} // namespace arcticdb