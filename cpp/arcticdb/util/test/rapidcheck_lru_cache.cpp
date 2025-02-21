#include <unordered_map>
#include <list>
#include <optional>
#include <vector>
#include <arcticdb/util/lru_cache.hpp>

#include <gtest/gtest.h>
#include <arcticdb/util/test/rapidcheck.hpp>
#include <rapidcheck.h>

RC_GTEST_PROP(LRUCacheTest, BehavesCorrectly, (const std::vector<int>& keys)) {
    using namespace arcticdb;
    const size_t capacity = 10;
    arcticdb::LRUCache<int, int> cache(capacity);
    auto values = keys;

    RC_PRE(!keys.empty());

    std::map<int, int> reference;

    for (size_t i = 0; i < keys.size(); ++i) {
        int key = keys[i];
        int value = values[i];

        cache.put(key, value);

        if (reference.size() + 1 > capacity) {
            auto oldest = std::min_element(reference.begin(), reference.end());
            reference.erase(oldest);
        }

        reference[key] = value;

        // Check get
        for (const auto& [k, v] : reference) {
            auto result = cache.get(k);
            RC_ASSERT(result.has_value());
            RC_ASSERT(result.value() == v);
        }

        // Check non-existent key
        int non_existent_key = 0;
        while (reference.find(non_existent_key) != reference.end()) {
            non_existent_key++;
        }
        RC_ASSERT(!cache.get(non_existent_key).has_value());

        // Check remove
        if (!reference.empty() && i % 2 == 0) {
            auto it = reference.begin();
            std::advance(it, i % reference.size());
            int key_to_remove = it->first;
            cache.remove(key_to_remove);
            reference.erase(key_to_remove);
            RC_ASSERT(!cache.get(key_to_remove).has_value());
        }
    }

    RC_ASSERT(cache.capacity() == capacity);
}