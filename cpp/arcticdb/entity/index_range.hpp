/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
using UnsignedIndex = uint64_t;
using IndexValue = std::variant<NumericIndex, StringIndex, UnsignedIndex>;

/** The IndexValue variant holds NumericIndex=timestamp=int64_t but is also used to store sizes up to uint64, so needs
    safe conversion. See also safe_convert_to_numeric_id. */
inline NumericIndex safe_convert_to_numeric_index(uint64_t input, const char* input_name) {
    util::check(
            input <= static_cast<uint64_t>(std::numeric_limits<NumericIndex>::max()),
            "{} greater than 2^63 is not supported.",
            input_name
    );
    return static_cast<NumericIndex>(input);
}

inline std::string tokenized_index(const IndexValue& val) {
    return util::variant_match(
            val,
            [](const NumericIndex& num) { return fmt::format("{}", *reinterpret_cast<const uint64_t*>(&num)); },
            [](const StringIndex& str) { return str; }
    );
}

inline bool intersects(const TimestampRange& left, const TimestampRange& right) {
    return left.first <= right.second && left.second >= right.first;
}

inline bool contains(const TimestampRange& range, timestamp point) {
    return point >= range.first && point <= range.second;
}

struct IndexRange {
    IndexValue start_;
    IndexValue end_;
    bool specified_ = false;
    bool start_closed_ = true;
    bool end_closed_ = true;

    IndexRange() = default;

    IndexRange(IndexValue start, IndexValue end) : start_(std::move(start)), end_(std::move(end)), specified_(true) {}

    explicit IndexRange(const TimestampRange& rg) : start_(rg.first), end_(rg.second), specified_(true) {}

    operator TimestampRange() const {
        util::check(
                std::holds_alternative<NumericIndex>(start_) && std::holds_alternative<NumericIndex>(end_),
                "Can't get timestamp range from non-numeric index"
        );
        return {std::get<timestamp>(start_), std::get<timestamp>(end_)};
    }

    // Indices of non-matching types will always be excluded, might want to assert though
    // as this should never happen
    bool accept(const IndexValue& index);

    // TODO: So many of these functions don't verify they are using the expected values of `start_inclusive` or
    // `end_inclusive`. We should fix them and make `start_` and `end_` private and make them only accessible through
    // functions like `inclusive_end()`.

    // N.B. Convenience function, variant construction will be too expensive for tight loops
    friend bool intersects(const IndexRange& left, const IndexRange& right) {
        if (!left.specified_ || !right.specified_)
            return true;

        return left.start_ <= right.end_ && left.end_ >= right.start_;
    }

    friend bool closed_aware_intersects(const IndexRange& left, const IndexRange& right) {
        return left.inclusive_start() <= right.inclusive_end() && left.inclusive_end() >= right.inclusive_start();
    }

    friend bool intersects(const IndexRange& rg, const IndexValue& start, const IndexValue& end) {
        if (!rg.specified_)
            return true;

        return rg.start_ <= end && rg.end_ >= start;
    }

    friend bool overlaps(const IndexRange& left, const IndexRange& right) {
        if (!left.specified_ || !right.specified_)
            return true;

        return left.start_ == right.start_ && left.end_ == right.end_;
    }

    IndexValue inclusive_end() const {
        if (std::holds_alternative<NumericIndex>(end_) && !end_closed_) {
            return NumericIndex(std::get<NumericIndex>(end_) - 1);
        }
        return end_;
    }

    IndexValue inclusive_start() const {
        if (std::holds_alternative<NumericIndex>(start_) && !start_closed_) {
            return NumericIndex(std::get<NumericIndex>(start_) + 1);
        }
        return start_;
    }

    bool empty() const { return inclusive_start() > inclusive_end(); }
};

// A segment's end_index is stored as its last index value + 1. When the last value is INT64_MAX this wraps to
// INT64_MIN, which is never otherwise a valid end_index (INT64_MIN is NaT), so it means "ends at INT64_MAX inclusive".
constexpr timestamp end_index_from_last_index_value(timestamp last) {
    return static_cast<timestamp>(static_cast<uint64_t>(last) + 1);
}

constexpr bool is_end_index_wrapped(timestamp end_index) { return end_index == std::numeric_limits<timestamp>::min(); }

constexpr timestamp last_index_value_from_end_index(timestamp end_index) {
    return is_end_index_wrapped(end_index) ? std::numeric_limits<timestamp>::max() : end_index - 1;
}

// The end-exclusive range covered by a segment, which is end-inclusive at INT64_MAX if its end_index wrapped
inline IndexRange segment_index_range(IndexValue start_index, IndexValue end_index) {
    IndexRange range{std::move(start_index), std::move(end_index)};
    range.end_closed_ = false;
    if (std::holds_alternative<NumericIndex>(range.end_) && is_end_index_wrapped(std::get<NumericIndex>(range.end_))) {
        range.end_ = std::numeric_limits<timestamp>::max();
        range.end_closed_ = true;
    }
    return range;
}

inline IndexRange unspecified_range() { return {}; }

inline IndexRange universal_range() {
    return IndexRange{std::numeric_limits<timestamp>::min(), std::numeric_limits<timestamp>::max()};
}

} // namespace arcticdb::entity

namespace fmt {

template<>
struct formatter<arcticdb::entity::TimestampRange> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::entity::TimestampRange& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}-{}", r.first, r.second);
    }
};

template<>
struct formatter<arcticdb::entity::IndexRange> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::entity::IndexRange& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}-{}", r.start_, r.end_);
    }
};

} // namespace fmt

namespace arcticdb::entity {

// Note: this needs to be defined after formatters.
inline bool IndexRange::accept(const IndexValue& index) {
    if (!specified_)
        return true;

    if (index >= start_ && index <= end_) {
        ARCTICDB_DEBUG(log::inmem(), "Returning index {} which is in range {}", index, *this);
        return true;
    }

    ARCTICDB_DEBUG(log::inmem(), "Filtered index {} as it was not in range {}", index, *this);
    return false;
}

} // namespace arcticdb::entity
