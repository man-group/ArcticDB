/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdlib>
#include <cstdint>
#include <memory>

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/entity/types.hpp>

namespace arcticdb {
using namespace arcticdb::entity;
class Cursor {
  public:
    Cursor() : cursor_(0) {}

    explicit Cursor(position_t cursor) : cursor_(cursor) {}

    ARCTICDB_MOVE_ONLY_DEFAULT(Cursor)

    [[nodiscard]] position_t pos() const { return cursor_; }

    [[nodiscard]] Cursor clone() const { return Cursor{cursor_}; }

    void advance(position_t pos, size_t buffer_size) {
        util::check_arg(
                cursor_ + pos <= position_t(buffer_size),
                "Buffer overflow , cannot advance {} in buffer of size {} with cursor at {}",
                pos,
                buffer_size,
                cursor_
        );

        cursor_ += pos;
    }

    void commit(size_t buffer_size) {
        util::check_arg(
                cursor_ == 0 || cursor_ < position_t(buffer_size),
                "Commit called twice on buffer of size {}",
                buffer_size
        );
        cursor_ = position_t(buffer_size);
    }

    void reset() { cursor_ = 0; }

    friend bool operator==(const Cursor& left, const Cursor& right) { return left.cursor_ == right.cursor_; }

  private:
    position_t cursor_;
};

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::Cursor> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::Cursor& c, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", c.pos());
    }
};

} // namespace fmt
