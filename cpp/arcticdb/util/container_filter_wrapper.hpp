/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <algorithm>

namespace arcticdb::util {

/**
 * Wraps an expensive-to-copy container and supports filtering on it.
 * Copying only happens the first time a modification happens.
 * The most efficient implementation is obviously a bit-set-based view on the original_ container, but that requires too
 * much code.
 *
 * **NOT THREAD SAFE**
 */
template<typename Container>
class ContainerFilterWrapper {
    using value_type = typename Container::value_type;

    const Container& original_;
    Container filtered_;
    bool use_original_;

  public:
    explicit ContainerFilterWrapper(const Container& original_) :
        original_(original_),
        filtered_(),
        use_original_(true) {};

    /**
     * The filter should take an item and return true to indicate if the item should be removed.
     *
     * If the filter throws, the result might be corrupted.
     */
    template<typename Filter>
    void remove_if(Filter filter) {
        if (use_original_) {
            auto end = original_.cend();
            for (auto itr = original_.cbegin(); itr != end; itr++) {
                bool remove = filter(*itr);
                if (remove) {
                    if (use_original_) {
                        use_original_ = false;
                        filtered_.insert(filtered_.end(), original_.cbegin(), itr);
                    }
                } else if (!use_original_) {
                    filtered_.insert(filtered_.end(), *itr); // insert() for all container types
                }
            }
        } else {
            filtered_.erase(std::remove_if(filtered_.begin(), filtered_.end(), filter), filtered_.end());
        }
    }

    /**
     * Only makes a copy if the argument is not already in the set.
     */
    template<typename C = Container>
    void insert(const std::enable_if_t<std::is_same_v<C, std::unordered_set<value_type>>, value_type>& val) {
        if (use_original_) {
            if (original_.count(val) > 0) {
                return;
            } else {
                use_original_ = false;
                filtered_ = original_;
                // Fall-through:
            }
        }
        filtered_.insert(val);
    }

    /**
     * Only makes a copy if the argument is in the set.
     */
    template<typename C = Container>
    void erase(const std::enable_if_t<std::is_same_v<C, std::unordered_set<value_type>>, value_type>& val) {
        if (use_original_) {
            if (original_.count(val) == 0) {
                return;
            } else {
                use_original_ = false;
                filtered_ = original_;
                // Fall-through:
            }
        }
        filtered_.erase(val);
    }

    void clear() {
        use_original_ = false;
        filtered_.clear();
    }

    // C++20: Use bitset and Ranges to hide/combine items instead of copying
    const Container& get() { return use_original_ ? original_ : filtered_; }

    const Container& operator*() { return get(); }
    const Container* operator->() { return &get(); }
};

} // namespace arcticdb::util