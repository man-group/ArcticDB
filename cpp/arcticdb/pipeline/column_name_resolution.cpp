/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/pipeline/column_name_resolution.hpp>

#include <vector>

#include <arcticdb/stream/merge.hpp>

#include <arcticdb/pipeline/column_stats.hpp>
#include <arcticdb/pipeline/query.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/storage/store.hpp>

#include <boost/regex.hpp>

namespace arcticdb::utils {

namespace {
const boost::regex mangled_col_name_pattern(R"(__col_(.+)__(\d+))");
const boost::regex mangled_index_name_pattern(R"(__idx__(.+))");

std::optional<std::string_view> get_index_name(const StreamDescriptor& descriptor) {
    return descriptor.index().field_count() ? std::optional(descriptor.field(0).name()) : std::nullopt;
}

bool can_name_be_repeated(std::string_view column_name, const std::optional<std::string_view>& index_name) {
    return index_name ? column_name == *index_name : false;
}
} // namespace

std::vector<OffsetAndMangledName> find_offset_and_mangled_name(
        std::unordered_set<std::string> column_names, const StreamDescriptor& descriptor,
        MissingColumnsBehavior missing_columns
) {
    const std::optional<std::string_view> index_name = get_index_name(descriptor);
    // If the DatetimeIndex is the same as the column name we allow one repetition so that if there's a column named
    // "index" and the DatetimeIndex is not named, it'll work. This'll also work if the index has the same name as the
    // column.
    bool index_name_repeated{false};
    std::unordered_set<std::string> found_column_names;
    std::set<std::string> duplicated_column_names; // set for stable ordering in error message
    std::vector<OffsetAndMangledName> res;
    res.reserve(column_names.size());
    for (size_t i = descriptor.index().field_count(); i < descriptor.field_count(); ++i) {
        const std::string field_name(descriptor.field(i).name());
        if (auto it = column_names.find(field_name); it != column_names.end()) {
            if (auto [_, inserted] = found_column_names.insert(field_name); !inserted) {
                duplicated_column_names.insert(field_name);
            }
            res.emplace_back(i, field_name, field_name);
            column_names.erase(it);
        } else if (found_column_names.contains(field_name)) {
            // field_name was already matched and erased from column_names — duplicate in the descriptor
            duplicated_column_names.insert(field_name);
        } else {
            boost::cmatch match;
            if (boost::regex_match(
                        field_name.data(), field_name.data() + field_name.size(), match, mangled_col_name_pattern
                ) ||
                boost::regex_match(
                        field_name.data(), field_name.data() + field_name.size(), match, mangled_index_name_pattern
                )) {
                const std::string matched_name(match[1].first, match[1].length());
                if (auto mangled_name_it = column_names.find(matched_name); mangled_name_it != column_names.end()) {
                    if (auto [_, inserted] = found_column_names.insert(matched_name); !inserted) {
                        if (can_name_be_repeated(matched_name, index_name) && !index_name_repeated) {
                            index_name_repeated = true;
                        } else {
                            duplicated_column_names.insert(field_name);
                        }
                    }
                    res.emplace_back(i, field_name, matched_name);
                    column_names.erase(mangled_name_it);
                } else if (found_column_names.contains(matched_name)) {
                    // matched_name was already matched and erased — duplicate via mangled name
                    if (can_name_be_repeated(matched_name, index_name) && !index_name_repeated) {
                        index_name_repeated = true;
                    } else {
                        duplicated_column_names.insert(field_name);
                    }
                }
            }
        }
    }

    if (!duplicated_column_names.empty()) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Column names \"{}\" are repeated multiple times in the data. This makes their use with "
                "column stats ambiguous for the stream descriptor: {}",
                duplicated_column_names,
                descriptor
        );
    }

    if (!column_names.empty() && missing_columns == MissingColumnsBehavior::RAISE) {
        user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                "Trying to create stats for columns \"{}\" that do not exist in the data {}", column_names, descriptor
        );
    }

    return res;
}

} // namespace arcticdb::utils
