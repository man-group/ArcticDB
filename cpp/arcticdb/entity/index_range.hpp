/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/variant.hpp>

#include <fmt/format.h>
#include <string_view>
#include <limits>

namespace arcticdb::entity {
using timestamp = int64_t;

using TimestampRange = std::pair<timestamp, timestamp>;

using NumericIndex = timestamp;
using StringIndex = std::string;
using IndexValue = std::variant<NumericIndex, StringIndex>;

inline std::string tokenized_index(const IndexValue& val) {
    return util::variant_match(val,
                               [] (const NumericIndex& num) {
        return fmt::format("{}", *reinterpret_cast<const uint64_t*>(&num));
        },
        [](const StringIndex& str) {
        return str;
    });
}


struct IndexRange {
    IndexValue start_;
    IndexValue end_;
    bool specified_;

    bool start_closed_;
    bool end_closed_;

    IndexRange() : specified_(false), start_closed_(true), end_closed_(true) {}

    IndexRange(IndexValue start, IndexValue end) :
        start_(std::move(start)),
        end_(std::move(end)),
        specified_(true),
        start_closed_(true),
        end_closed_(true) {
    }

    explicit IndexRange(const TimestampRange &rg) :
        start_(rg.first),
        end_(rg.second),
        specified_(true),
        start_closed_(true),
        end_closed_(true) {
    }

    // Indices of non-matching types will always be excluded, might want to assert though
    // as this should never happen
    bool accept(const IndexValue &index) {
        if (!specified_)
            return true;

        if (index >= start_ && index <= end_) {
            ARCTICDB_DEBUG(log::inmem(), "Returning index {} which is in range {}", index, *this);
            return true;
        }

        ARCTICDB_DEBUG(log::inmem(), "Filtered index {} as it was not in range {}", index, *this);
        return false;
    }

    // N.B. Convenience function, variant construction will be too expensive for tight loops
    friend bool intersects(const IndexRange &left, const IndexRange& right) {
        if (!left.specified_ || !right.specified_)
            return true;

        return left.start_ <= right.end_ && left.end_ >= right.start_;
    }

    friend bool intersects(const IndexRange &rg, const IndexValue &start, const IndexValue &end) {
        if (!rg.specified_)
            return true;

        return rg.start_ <= end && rg.end_ >= start;
    }

    friend bool overlaps(const IndexRange &left, const IndexRange& right) {
        if (!left.specified_ || !right.specified_)
            return true;

        return left.start_ == right.start_ && left.end_ == right.end_;
    }

    void adjust_start(const IndexValue &index_value) {
        std::visit(util::overload{
                [that = this](const auto &start, const auto &val) {
                    if constexpr(std::is_same_v<std::decay_t<decltype(start)>, std::decay_t<decltype(val)>>) {
                        that->start_ = std::min(start, val);
                    } else {
                        util::raise_rte("Type mismatch in update");
                    }
                }}, start_, index_value);
    }

    void adjust_end(const IndexValue &index_value) {
        std::visit(util::overload{
                [that = this](const auto &end, const auto &val) {
                    if constexpr(std::is_same_v<std::decay_t<decltype(end)>, std::decay_t<decltype(val)>>) {
                        that->end_ = std::min(end, val);
                    } else {
                        util::raise_rte("Type mismatch in update");
                    }
                }}, end_, index_value);
    }

    void adjust_open_closed_interval() {
        util::variant_match(start_,
                            [that = this](const NumericIndex &start) mutable {
                                if (!that->start_closed_)
                                    that->start_ = IndexValue{start + 1};
                            },
                            [](const auto &) {
                            });

        util::variant_match(end_,
                            [that = this](const NumericIndex &end) {
                                if (!that->end_closed_)
                                    that->end_ = IndexValue{end - 1};
                            },
                            [](const auto &) {
                            }
        );
    }
};

inline IndexRange unspecified_range() { return {}; }

inline IndexRange universal_range(){ return IndexRange{std::numeric_limits<timestamp>::min(), std::numeric_limits<timestamp>::max()} ;}

} //namespace arcticdb::entity

namespace fmt {
using namespace arcticdb::entity;

template<>
struct formatter<TimestampRange> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const TimestampRange &r, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}-{}", r.first, r.second);
    }
};

template<>
struct formatter<IndexRange> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const IndexRange &r, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}-{}", r.start_, r.end_);
    }
};

} //namespace fmt
