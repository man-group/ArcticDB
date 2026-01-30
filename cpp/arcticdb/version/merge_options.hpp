/* Copyright 2025 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once
#include <cstdint>
#include <arcticdb/util/preconditions.hpp>

namespace arcticdb {
enum class MergeAction : uint8_t { DO_NOTHING, UPDATE, INSERT };
struct MergeStrategy {
    MergeAction matched;
    MergeAction not_matched_by_target;
    bool operator==(const MergeStrategy&) const = default;
};

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::MergeAction> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::MergeAction action, FormatContext& ctx) const {
        switch (action) {
        case arcticdb::MergeAction::DO_NOTHING:
            return fmt::format_to(ctx.out(), "do_nothing");
        case arcticdb::MergeAction::UPDATE:
            return fmt::format_to(ctx.out(), "update");
        case arcticdb::MergeAction::INSERT:
            return fmt::format_to(ctx.out(), "insert");
        default:
            arcticdb::internal::raise<arcticdb::ErrorCode::E_ASSERTION_FAILURE>(
                    "Unknown merge action: {}", static_cast<std::underlying_type_t<decltype(action)>>(action)
            );
        }
    }
};
template<>
struct formatter<arcticdb::MergeStrategy> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::MergeStrategy strategy, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "MergeStrategy(matched={}, not_matched_by_target={})",
                strategy.matched,
                strategy.not_matched_by_target
        );
    }
};
} // namespace fmt