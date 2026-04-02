/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <ranges>
#include <vector>

namespace arcticdb {

namespace util {

template<typename T>
inline std::vector<T> flatten_vectors(std::vector<std::vector<T>>&& vec_of_vecs) {
    size_t res_size = std::accumulate(
            vec_of_vecs.cbegin(),
            vec_of_vecs.cend(),
            size_t(0),
            [](size_t acc, const std::vector<T>& vec) { return acc + vec.size(); }
    );
    std::vector<T> res;
    res.reserve(res_size);
    for (const auto& vec : vec_of_vecs) {
        res.insert(res.end(), std::make_move_iterator(vec.begin()), std::make_move_iterator(vec.end()));
    }
    return res;
}

// These are one-liners in C++23
template<typename T>
std::vector<T> extract_from_pointers(std::vector<std::shared_ptr<T>>&& input) {
    std::vector<T> res;
    res.reserve(input.size());
    std::ranges::transform(input, std::back_inserter(res), [](std::shared_ptr<T>& value) {
        ARCTICDB_DEBUG_CHECK(
                ErrorCode::E_ASSERTION_FAILURE,
                value.use_count() == 1,
                "Shouldn't move from shared_ptr with more than 1 owner"
        );
        return std::move(*value);
    });
    return res;
}

template<typename T>
std::vector<std::shared_ptr<T>> extract_to_pointers(std::vector<T>&& input) {
    std::vector<std::shared_ptr<T>> res;
    res.reserve(input.size());
    std::ranges::transform(input, std::back_inserter(res), [](T& value) {
        return std::make_shared<T>(std::move(value));
    });
    return res;
}

} // namespace util

} // namespace arcticdb
