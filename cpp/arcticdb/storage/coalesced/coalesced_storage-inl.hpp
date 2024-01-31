/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <folly/gen/Base.h>
#ifndef ARCTICDB_COALESCED_STORAGE_H_
#error "This should only be included by coalesced_storage.hpp"
#endif

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/storage_coalesce.hpp>

namespace arcticdb::storage::coalesced {

namespace fg = folly::gen;

template <typename UnderlyingStorage>
inline void CoalescedStorage<UnderlyingStorage>::do_write(Composite<KeySegmentPair>&& kvs) {
    ARCTICDB_SAMPLE(CoalescedStorageWrite, 0)
    base_.write(std::move(kvs));
}

template <typename UnderlyingStorage>
inline void CoalescedStorage<UnderlyingStorage>::do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts) {
    ARCTICDB_SAMPLE(CoalescedStorageUpdate, 0)

    auto fmt_db = [](auto &&k) { return variant_key_type(k.variant_key()); };

    (fg::from(kvs.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &kv : group.values()) {
            util::variant_match(kv.variant_key,
                [this, &kv](const RefKey&) {
                        base_.update(std::move(kv));
                },
                [this, &kv](const AtomKey& atom_key) {
                    if(!last_coalesce_time_ || atom_key.creation_ts() > last_coalesce_time_.value())
                        base_.update(std::move(kv));
                    else
                        base_.write(std::move(kv));
                }
            );
        }
    });
}

template<class UnderlyingStorage>
template<class Visitor>
void CoalescedStorage<UnderlyingStorage>::do_read(Composite<VariantKey>&& ks, Visitor &&visitor, ReadKeyOpts) {
    ARCTICDB_SAMPLE(CoalescedStorageRead, 0)
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (const auto& k : group.values()) {
            util::variant_match(k,
                [this, &k, &visitor](const RefKey&) {
                    return base_.read(k, std::move(visitor));
                },
                [this, &k, &visitor](const AtomKey& atom_key) {
                    if(!last_coalesce_time_ || atom_key.creation_ts() > last_coalesce_time_.value()) {
                        try {
                            return base_.read(k, [&visitor] (auto&& s) { return visitor(s); });
                        } catch (const NoDataFoundException&) {
                            refresh_coalesced_keys_ = true;
                        }
                    }

                    std::shared_lock reader_lock(keys_mutex_);
                    if(auto it = coalesced_keys_.find(k.type())) {
                        //return it.second->
                    }
                }
            );
        }
    });
}

template<class UnderlyingStorage>
inline bool CoalescedStorage<UnderlyingStorage>::do_key_exists(const VariantKey& key) {
    ARCTICDB_SAMPLE(CoalescedStorageKeyExists, 0)

}

template<class UnderlyingStorage>
inline void CoalescedStorage<UnderlyingStorage>::do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) {
    ARCTICDB_SAMPLE(CoalescedStorageRemove, 0)
    auto fmt_db = [](auto &&k) { return variant_key_type(k); };

    (fg::from(ks.as_range()) | fg::move | fg::groupBy(fmt_db)).foreach([&](auto &&group) {
        for (auto &k : group.values()) {

        }
    });
}

template <class UnderlyingStorage>
bool CoalescedStorage<UnderlyingStorage>::do_fast_delete() {
    return base_.do_fast_delete();
}

template<class UnderlyingStorage>
template< class Visitor>
void CoalescedStorage<UnderlyingStorage>::do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &/*prefix*/) {
    ARCTICDB_SAMPLE(CoalescedStorageItType, 0)
        base_.do_iterate_type(key_type, [&visitor] (auto&& key) {
            visitor(std::move(key));
        });
}

template <class UnderlyingStorage>
void CoalescedStorage<UnderlyingStorage> coalesce_storage(

    ) {


}
}
