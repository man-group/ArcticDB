/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

class SegmentMap {
    using ValueType = std::variant<VariantKey, SegmentInMemoryImpl>;
    using ContainerType = std::unordered_map<uint64_t, std::weak_ptr<ValueType>>;
    std::atomic<uint64_t> id_;
    std::shared_ptr<Store> store_;
    std::mutex mutex_;
public:
    using const_iterator = ContainerType::const_iterator;

    SegmentMap(const std::shared_ptr<Store>& store) :
        store_(store) {
    }

    uint64_t insert(std::shared_ptr<SegmentInMemoryImpl>&& seg) {
        const auto id = id_++;
        std::shared_ptr<ValueType> value(
            std::move(seg),
            [this, id](ValueType* v)
                map_.erase(id);
                delete v;
            }
            );
        map_.emplace(id, value);
        return id;
    }

    int size() const {
        return map_.size();
    }

    const_iterator begin() const {
        return map_.begin();
    }

    const_iterator end() const {
        return map_.end();
    }
private:
    container_type map_;
};
}