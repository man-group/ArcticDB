#pragma once

#include <folly/futures/Future.h>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/coalesced/coalesced_key_type.hpp>

#include <folly/Range.h>

namespace arcticdb::storage::coalesced {

template <typename UnderlyingStorage>
class CoalescedStorage: public Storage {
    UnderlyingStorage base_;


    std::optional<timestamp> last_coalesce_time_;
    std::atomic<bool> refresh_coalesced_keys_ = false;
    std::shared_mutex keys_mutex_;
    std::unordered_map<KeyType, std::shared_ptr<CoalescedKeyType>> coalesced_keys_;
    static constexpr std::array<KeyType, 1> updated_types_ = {KeyType::COLUMN_STATS};

    void coalesce_key_type(KeyType key_type, std::optional<timestamp> last_coalesce_time);

public:
        using Config = typename UnderlyingStorage::Config;

    CoalescedStorage(const LibraryPath &lib, OpenMode mode, const Config &conf) :
        base_(lib, mode, conf) {
    }

    protected:
        void do_write(Composite<KeySegmentPair>&& kvs);

        void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts);

        template<class Visitor>
        void do_read(Composite<VariantKey>&& ks, Visitor &&visitor, ReadKeyOpts opts);


        void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts);

        bool do_key_exists(const VariantKey& key);

        bool do_supports_prefix_matching() {
            return false;
        }

        inline bool do_fast_delete();

        template<class Visitor>
        void do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix);
};

} //namespace arcticdb

#define ARCTICDB_COALESCED_STORAGE_H_
#include <arcticdb/storage/coalesced/coalesced_storage-inl.hpp>
#undef ARCTICDB_COALESCED_STORAGE_H_