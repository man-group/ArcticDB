#include <arcticdb/pipeline/read_pipeline.hpp>
#include <arcticdb/processing/clause.hpp>

namespace arcticdb::pipelines {

inline std::optional<util::BitSet> clause_column_bitset(
        const StreamDescriptor& desc, const std::vector<std::shared_ptr<Clause>>& clauses
) {
    folly::F14FastSet<std::string_view> column_set;
    for (const auto& clause : clauses) {
        auto opt_columns = clause->clause_info().input_columns_;
        if (opt_columns.has_value()) {
            for (const auto& column : *clause->clause_info().input_columns_) {
                column_set.insert(std::string_view(column));
            }
        }
    }
    if (!column_set.empty())
        return build_column_bitset(desc, column_set);
    else
        return std::nullopt;
}

// Returns std::nullopt if all columns are required, which is the case if requested_columns is std::nullopt
// Otherwise augment the requested_columns bitset with columns that are required by any of the clauses
std::optional<util::BitSet> overall_column_bitset(
        const StreamDescriptor& desc, const std::vector<std::shared_ptr<Clause>>& clauses,
        const std::optional<util::BitSet>& requested_columns
) {
    // std::all_of returns true if the range is empty
    auto clauses_can_combine_with_column_selection = ranges::all_of(clauses, [](const std::shared_ptr<Clause>& clause) {
        return clause->clause_info().can_combine_with_column_selection_;
    });
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            !requested_columns.has_value() || clauses_can_combine_with_column_selection,
            "Cannot combine provided clauses with column selection"
    );

    if (clauses_can_combine_with_column_selection) {
        if (requested_columns.has_value()) {
            auto clause_bitset = clause_column_bitset(desc, clauses);
            return clause_bitset.has_value() ? *requested_columns | *clause_bitset : requested_columns;
        } else {
            return std::nullopt;
        }
    } else {
        // clauses_can_combine_with_column_selection is false implies requested_columns.has_value() is false by the
        // previous check
        return clause_column_bitset(desc, clauses);
    }
}

} // namespace arcticdb::pipelines