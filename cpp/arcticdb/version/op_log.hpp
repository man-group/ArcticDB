/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {
using namespace arcticdb::entity;
class OpLog {
  public:
    OpLog() = delete;
    OpLog(AtomKey&& key);
    OpLog(StringId id, VersionId version_id, const std::string& action, timestamp creation_ts);

    ARCTICDB_MOVE_COPY_DEFAULT(OpLog)

    const StringId& id() const;
    VersionId version_id() const;
    const std::string& action() const;
    timestamp creation_ts() const;

    AtomKey extract_key();

  private:
    // Represents the symbol or snapshot name
    StringId id_;
    // Unused for snapshot creation/deletion op logs
    VersionId version_id_;
    std::string action_;
    timestamp creation_ts_;
    std::optional<ContentHash> content_hash_;
};
} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::OpLog> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::OpLog& op_log, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(), "{} {} v{} at {}", op_log.action(), op_log.id(), op_log.version_id(), op_log.creation_ts()
        );
    }
};
} // namespace fmt
