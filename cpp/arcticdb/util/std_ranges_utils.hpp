#pragma once

#include <ranges>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <functional>
#include <optional>
#include <stack>
#include <memory>

namespace arcticdb::util {

// Standard library replacement for folly::gen::groupBy
template<typename Container, typename KeyFunc>
auto group_by(Container&& container, KeyFunc&& key_func) {
    // Use decltype to get the correct value type from the range
    using ValueType = std::remove_reference_t<decltype(*std::begin(container))>;
    using KeyType = std::invoke_result_t<KeyFunc, ValueType>;
    
    std::unordered_map<KeyType, std::vector<ValueType>> result;
    
    for (auto&& item : std::forward<Container>(container)) {
        auto key = std::invoke(std::forward<KeyFunc>(key_func), item);
        result[std::move(key)].push_back(std::forward<ValueType>(item));
    }
    
    return result;
}

// Standard library replacement for folly::gen::zip with move semantics
template<typename Container1, typename Container2>
auto zip_move(Container1&& container1, Container2&& container2) {
    using ValueType1 = std::remove_reference_t<decltype(*std::begin(container1))>;
    using ValueType2 = std::remove_reference_t<decltype(*std::begin(container2))>;
    
    std::vector<std::pair<ValueType1, ValueType2>> result;
    result.reserve(std::min(container1.size(), container2.size()));
    
    auto it1 = std::begin(container1);
    auto it2 = std::begin(container2);
    auto end1 = std::end(container1);
    auto end2 = std::end(container2);
    
    while (it1 != end1 && it2 != end2) {
        result.emplace_back(std::move(*it1), std::move(*it2));
        ++it1;
        ++it2;
    }
    
    return result;
}

// Helper for foreach on grouped data
template<typename GroupedContainer, typename Func>
void foreach_group(GroupedContainer&& grouped, Func&& func) {
    for (auto&& [key, values] : std::forward<GroupedContainer>(grouped)) {
        std::invoke(std::forward<Func>(func), key, std::forward<decltype(values)>(values));
    }
}

// Standard library replacement for folly::gen::from with move semantics
template<typename Container>
auto from_move(Container&& container) {
    return std::forward<Container>(container);
}

// Standard library replacement for folly::gen::map
template<typename Container, typename Func>
auto map(Container&& container, Func&& func) {
    using InputType = std::remove_reference_t<decltype(*std::begin(container))>;
    using OutputType = std::invoke_result_t<Func, InputType>;
    
    std::vector<OutputType> result;
    result.reserve(container.size());
    
    for (auto&& item : std::forward<Container>(container)) {
        result.push_back(std::invoke(std::forward<Func>(func), std::forward<InputType>(item)));
    }
    
    return result;
}

// Standard library replacement for folly::gen::filter
template<typename Container, typename Func>
auto filter(Container&& container, Func&& func) {
    using ValueType = std::remove_reference_t<decltype(*std::begin(container))>;
    
    std::vector<ValueType> result;
    result.reserve(container.size()); // Reserve for worst case
    
    for (auto&& item : std::forward<Container>(container)) {
        if (std::invoke(std::forward<Func>(func), item)) {
            result.push_back(std::forward<ValueType>(item));
        }
    }
    
    result.shrink_to_fit();
    return result;
}

// Standard library replacement for folly::gen::as<std::vector>
template<typename Container>
auto as_vector(Container&& container) {
    return std::forward<Container>(container);
}

// Generator-like interface for lazy evaluation
template<typename T>
class Generator {
public:
    virtual ~Generator() = default;
    virtual std::optional<T> next() = 0;
};

// Simple generator implementation
template<typename T, typename Func>
class SimpleGenerator : public Generator<T> {
    Func func_;
    bool done_ = false;
    
public:
    SimpleGenerator(Func func) : func_(std::move(func)) {}
    
    std::optional<T> next() override {
        if (done_) return std::nullopt;
        return func_();
    }
    
    void set_done() { done_ = true; }
};

// Concatenation of generators
template<typename T>
class ConcatGenerator : public Generator<T> {
    std::vector<std::unique_ptr<Generator<T>>> generators_;
    size_t current_ = 0;
    
public:
    void add_generator(std::unique_ptr<Generator<T>> gen) {
        generators_.push_back(std::move(gen));
    }
    
    std::optional<T> next() override {
        while (current_ < generators_.size()) {
            if (auto result = generators_[current_]->next()) {
                return result;
            }
            ++current_;
        }
        return std::nullopt;
    }
};

// Helper to collect generator into vector
template<typename T>
std::vector<T> collect_generator(std::unique_ptr<Generator<T>> gen) {
    std::vector<T> result;
    while (auto item = gen->next()) {
        result.push_back(std::move(*item));
    }
    return result;
}

} // namespace arcticdb::util 