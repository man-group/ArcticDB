/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <vector>
#include <ranges>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace arcticdb {
struct StageResult {
    explicit StageResult(std::vector<entity::AtomKey> staged_segments) : staged_segments(std::move(staged_segments)) {}

    std::vector<entity::AtomKey> staged_segments;

    std::string view() const;
};
} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::StageResult> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::StageResult& stage_result, FormatContext& ctx) const {
        auto transform_view =
                stage_result.staged_segments | std::views::transform([](const auto& key) { return key.view_human(); });
        return fmt::format_to(ctx.out(), "StageResult(staged_segments=[{}])", fmt::join(transform_view, ", "));
    }
};
} // namespace fmt
