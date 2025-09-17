/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/util/constructors.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/variant.hpp>

#include <ranges>
#include <folly/container/Enumerate.h>
#include <boost/iterator/iterator_facade.hpp>

#include <variant>
#include <vector>
#include <numeric>

namespace arcticdb {

template<typename T>
struct Composite {
    using CompositePtr = std::unique_ptr<Composite<T>>;
    using ValueType = std::variant<T, CompositePtr>;
    using ValueVector = std::vector<ValueType>;
    ValueVector values_;

    using RangeType = std::ranges::subrange<typename ValueVector::iterator>;
    using RangePair = std::pair<RangeType, std::ranges::iterator_t<RangeType>>;

    RangeType get_range() const { return std::ranges::subrange(values_); }

    RangePair range_pair() const {
        auto range = get_range();
        auto pos = range.begin();
        return {std::move(range), std::move(pos)};
    }

    template<class ValueType>
    class CompositeIterator
        : public boost::iterator_facade<Composite<ValueType>, ValueType, boost::forward_traversal_tag> {
        Composite* parent_ = nullptr;
        std::vector<RangePair> ranges_;

      public:
        explicit CompositeIterator(RangePair&& pair) { ranges_.emplace_back(std::move(pair)); }

        CompositeIterator() = default;

        const RangePair& current_pair() { return ranges_.back(); }

        const ValueType& pos() { return *ranges_.back().second; }

        void check_at_end() {
            while (!ranges_.empty() && current_pair().second == current_pair().first.end())
                ranges_.pop_back();
        }

        void find_next_value() {
            check_at_end();

            while (!ranges_.empty() && std::holds_alternative<CompositePtr>(pos())) {
                ranges_.emplace_back(pos()->get_range());
            }
        }

        template<class OtherValue>
        explicit CompositeIterator(const CompositeIterator<OtherValue>& other) :
            parent_(other.parent_),
            ranges_(other.ranges_) {}

        template<class OtherValue>
        bool equal(const CompositeIterator<OtherValue>& other) const {
            return ranges_ = other.ranges_;
        }

        void increment() { find_next_value(); }

        ValueType& dereference() const {
            util::check(!ranges_.empty(), "Derefenence on composite iterator at end");
            return pos();
        }
    };

    using value_type = T;

    Composite() = default;

    explicit Composite(T&& t) { values_.emplace_back(std::move(t)); }

    const ValueType& operator[](size_t idx) const { return values_[idx]; }

    ValueType& operator[](size_t idx) { return values_[idx]; }

    [[nodiscard]] size_t level_1_size() const { return values_.size(); }

    explicit Composite(std::vector<T>&& vec) {
        util::check(!vec.empty(), "Cannot create composite with no values");
        values_.insert(
                std::end(values_), std::make_move_iterator(std::begin(vec)), std::make_move_iterator(std::end(vec))
        );
    }

    ARCTICDB_MOVE_ONLY_DEFAULT(Composite)

    [[nodiscard]] bool is_single() const { return values_.size() == 1 && std::holds_alternative<T>(values_[0]); }

    [[nodiscard]] size_t size() const {
        return std::accumulate(std::begin(values_), std::end(values_), 0, [](size_t n, const ValueType& v) {
            return util::variant_match(
                    v,
                    [n](const T&) { return n + 1; },
                    [n](const std::unique_ptr<Composite<T>>& c) { return n + c->size(); }
            );
        });
    }

    [[nodiscard]] bool empty() const {
        return values_.empty() || std::all_of(values_.begin(), values_.end(), [](const ValueType& v) {
                   return util::variant_match(
                           v,
                           [](const T&) { return false; },
                           [](const std::unique_ptr<Composite<T>>& c) { return c->empty(); }
                   );
               });
    }

    auto as_range() {
        std::vector<T> output;
        broadcast([&output](auto val) { output.emplace_back(std::move(val)); });
        return output;
    }

    void push_back(T&& value) { values_.emplace_back(std::move(value)); }

    void push_back(const T& value) { values_.emplace_back(value); }

    void push_back(Composite<T>&& value) { values_.emplace_back(std::make_unique<Composite<T>>(std::move(value))); }

    template<typename Func>
    void broadcast(const Func& func) {
        for (auto& value : values_) {
            util::variant_match(
                    value,
                    [&func](T& val) { func(val); },
                    [&func](std::unique_ptr<Composite<T>>& comp) { comp->broadcast(func); }
            );
        }
    }

    template<typename Func>
    void broadcast(const Func& func) const {
        for (auto& value : values_) {
            util::variant_match(
                    value,
                    [&func](const T& val) { func(val); },
                    [&func](const std::unique_ptr<Composite<T>>& comp) { comp->broadcast(func); }
            );
        }
    }

    template<typename Func>
    auto transform(const Func& func) {
        using ReturnType = std::decay_t<decltype(func(std::declval<T>()))>;
        Composite<ReturnType> output;
        broadcast([&func, &output](auto&& val) { output.push_back(func(val)); });
        return output;
    }

    template<typename Func>
    auto filter(const Func& func) {
        Composite output;
        broadcast([&func, &output](auto&& v) {
            auto val = std::forward<T>(v);
            if (func(val))
                output.push_back(std::move(val));
        });
        return output;
    }

    template<typename Func, typename U>
    auto fold(const Func& func, U initial) {
        broadcast([&initial, &func](auto& v) { initial = func(initial, v); });
        return initial;
    }
};

/*
 * Joins the roots of the composites via a common parent:
 *
 *    *            *
 *   / \     +    / \
 *  *   *        *   *
 *  =>
 *             *
 *            / \
 *           *   *
 *          / \ / \
 *         *  * *  *
 */
template<typename T>
Composite<T> merge_composites(std::vector<Composite<T>>&& cmp) {
    auto composites = std::move(cmp);
    Composite<T> output;
    for (auto&& composite : composites) {
        output.push_back(std::move(composite));
    }

    return output;
}

/*
 * Joins the roots of the composites:
 *
 *    *            *
 *   / \     +    / \
 *  *   *        *   *
 *  =>
 *             *
 *             |
 *       -------------
 *       |   |   |   |
 *       *   *   *   *
 */
template<typename T>
Composite<T> merge_composites_shallow(std::vector<Composite<T>>&& cmp) {
    std::vector<Composite<T>> composites = std::move(cmp);
    Composite<T> output;
    for (Composite<T>& composite : composites) {
        for (size_t i = 0; i < composite.level_1_size(); i++) {
            util::variant_match(
                    composite[i],
                    [&output](T& val) { output.push_back(std::move(val)); },
                    [&output](std::unique_ptr<Composite<T>>& comp) {
                        auto t_ptr = std::move(comp);
                        output.push_back(std::move(*t_ptr));
                    }
            );
        }
    }

    return output;
}

} // namespace arcticdb

namespace fmt {
template<typename T>
struct formatter<arcticdb::Composite<T>> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::Composite<T>& c, FormatContext& ctx) const {
        auto it = fmt::format_to(ctx.out(), "Composite: ");
        c.broadcast([&it](const auto& v) { it = fmt::format_to(it, "{}, ", v); });
        return it;
    }
};
} // namespace fmt
