#pragma once

namespace arcticdb {
static consteval timestamp one_day_in_nanoseconds() {
    return timestamp(24) * 60 * 60 * 1'000'000'000;
}

template<typename T>
requires std::integral<T>
[[nodiscard]] static T python_mod(T a, T b) {
    return (a % b + b) % b;
}

/// @param ts in nanoseconds
[[nodiscard]] static timestamp start_of_day_nanoseconds(timestamp ts) {
    return ts - python_mod(ts, one_day_in_nanoseconds());
}

/// @param ts in nanoseconds
[[nodiscard]] static timestamp end_of_day_nanoseconds(timestamp ts) {
    const timestamp start_of_day = start_of_day_nanoseconds(ts);
    const bool is_midnnight = start_of_day == ts;
    if (is_midnnight) {
        return ts;
    }
    return start_of_day + one_day_in_nanoseconds();
}

[[nodiscard]] static std::pair<timestamp, timestamp> compute_first_last_dates(
    timestamp start,
    timestamp end,
    timestamp rule,
    ResampleBoundary closed_boundary_arg,
    timestamp offset,
    const ResampleOrigin& origin
) {
    // Origin value formula from Pandas:
    // https://github.com/pandas-dev/pandas/blob/68d9dcab5b543adb3bfe5b83563c61a9b8afae77/pandas/core/resample.py#L2564
    auto [origin_ns, origin_adjusted_start] = util::variant_match(
        origin,
        [start](timestamp o) -> std::pair<timestamp, timestamp> {return {o, start}; },
        [&](const std::string& o) -> std::pair<timestamp, timestamp> {
            if (o == "epoch") {
                return { 0, start };
            } else if (o == "start") {
                return { start, start };
            } else if (o == "start_day") {
                return { start_of_day_nanoseconds(start), start };
            } else if (o == "end_day" || o == "end") {
                const timestamp origin_last = o == "end" ? end: end_of_day_nanoseconds(end);
                const timestamp bucket_count = (origin_last - start) / rule + (closed_boundary_arg == ResampleBoundary::LEFT);
                const timestamp local_origin_ns = origin_last - bucket_count * rule;
                return { local_origin_ns, local_origin_ns };
            } else {
                user_input::raise<ErrorCode::E_INVALID_USER_ARGUMENT>(
                    "Invalid origin value {}. Supported values are: \"start\", \"start_day\", \"end\", \"end_day\", \"epoch\" or timestamp in nanoseconds",
                    o);
            }
        }
    );
    origin_ns += offset;

    const timestamp ns_to_prev_offset_start = python_mod(origin_adjusted_start - origin_ns, rule);
    const timestamp ns_to_prev_offset_end = python_mod(end - origin_ns, rule);

    if (closed_boundary_arg == ResampleBoundary::RIGHT) {
        return {
            ns_to_prev_offset_start > 0 ? origin_adjusted_start - ns_to_prev_offset_start : origin_adjusted_start - rule,
            ns_to_prev_offset_end > 0 ? end + (rule - ns_to_prev_offset_end) : end
        };
    } else {
        return {
            ns_to_prev_offset_start > 0 ? origin_adjusted_start - ns_to_prev_offset_start : origin_adjusted_start,
            ns_to_prev_offset_end > 0 ? end + (rule - ns_to_prev_offset_end) : end + rule
        };
    }
}

std::vector<timestamp> generate_buckets(
    timestamp start,
    timestamp end,
    std::string_view rule,
    ResampleBoundary closed_boundary_arg,
    timestamp offset,
    const ResampleOrigin& origin
) {
    const timestamp rule_ns = [](std::string_view rule) {
        py::gil_scoped_acquire acquire_gil;
        return python_util::pd_to_offset(rule);
    }(rule);
    const auto [start_with_offset, end_with_offset] = compute_first_last_dates(start, end, rule_ns, closed_boundary_arg, offset, origin);
    const auto bucket_boundary_count = (end_with_offset - start_with_offset) / rule_ns + 1;
    std::vector<timestamp> res;
    res.reserve(bucket_boundary_count);
    for (auto boundary = start_with_offset; boundary <= end_with_offset; boundary += rule_ns) {
        res.push_back(boundary);
    }
    return res;
}

} // namespace arcticdb