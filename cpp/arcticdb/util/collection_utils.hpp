/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <ankerl/unordered_dense.h>

#include <concepts>
#include <memory>
#include <ranges>
#include <utility>
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
    for (auto& vec : vec_of_vecs) {
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

template<typename T>
std::vector<T> reserve_vector(size_t size) {
    std::vector<T> res;
    res.reserve(size);
    return res;
}

// Anything we can iterate as key/value pairs and look a key up in: std::map, std::unordered_map,
// ankerl::unordered_dense::map and google::protobuf::Map all qualify.
template<typename M>
concept KeyedLookup = requires(const M& map, const typename M::key_type& key) {
    typename M::key_type;
    typename M::mapped_type;
    { map.begin() };
    { map.end() };
    { map.find(key) };
    { map.find(key)->second } -> std::convertible_to<const typename M::mapped_type&>;
};

// for_each_key_union variants call per_key once for each key in union(left.keys(), right.keys()), with a pointer to
// what each side holds for it, or nullptr where that side lacks the key.

// Map variant. Iterates left, then remaining from right.
template<KeyedLookup Left, KeyedLookup Right, typename Func>
requires std::invocable<
        Func, const typename Left::key_type&, const typename Left::mapped_type*, const typename Right::mapped_type*>
void for_each_key_union(const Left& left, const Right& right, Func per_key) {
    for (const auto& [key, left_value] : left) {
        const auto it = right.find(key);
        per_key(key, &left_value, it != right.end() ? &it->second : nullptr);
    }
    for (const auto& [key, right_value] : right) {
        if (left.find(key) == left.end()) {
            per_key(key, nullptr, &right_value);
        }
    }
}

// Associative list variant. Iterates left in order, then remaining from right.
template<typename Left, KeyedLookup Right, typename Func>
requires(!KeyedLookup<Left>) && std::ranges::input_range<Left> &&
        std::invocable<
                Func, const typename std::ranges::range_value_t<Left>::first_type&,
                const typename std::ranges::range_value_t<Left>::second_type*, typename Right::mapped_type*>
void for_each_key_union(const Left& left, Right& right, Func per_key) {
    ankerl::unordered_dense::set<typename std::ranges::range_value_t<Left>::first_type> left_keys;
    for (const auto& [key, left_value] : left) {
        left_keys.emplace(key);
        const auto it = right.find(key);
        per_key(key, &left_value, it != right.end() ? &it->second : nullptr);
    }
    for (auto& [key, right_value] : right) {
        if (!left_keys.contains(key)) {
            per_key(key, nullptr, &right_value);
        }
    }
}

} // namespace util

} // namespace arcticdb
