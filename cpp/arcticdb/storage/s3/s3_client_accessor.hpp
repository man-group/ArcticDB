/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <arcticdb/util/composite.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/entity/key.hpp>

namespace arcticdb::storage {

template<typename StorageType>
class S3TestClientAccessor {
public:
    static auto& get_client(StorageType& storage) { return storage.client(); }

    static auto& get_bucket_name(StorageType& storage) { return storage.bucket_name(); }

    static auto& get_root_folder(StorageType& storage) { return storage.root_folder(); }
};

template<typename StorageType>
class S3TestForwarder {
    StorageType storage_;
public:
    S3TestForwarder(const LibraryPath &lib, OpenMode mode, const typename StorageType::Config &conf) :
    storage_(lib, mode, conf) {
    }

    void do_write(Composite<KeySegmentPair>&& kvs) {
        storage_.do_write(std::move(kvs));
    }

    void do_update(Composite<KeySegmentPair>&& kvs) {
        storage_.do_update(std::move(kvs));
    }

    template<class Visitor>
        void do_read(Composite<VariantKey>&& ks, Visitor &&visitor) {
        storage_.do_read(std::move(ks), std::move(visitor));
    }

    void do_remove(Composite<VariantKey>&& ks) {
        storage_.do_remove(std::move(ks));
    }

    template<class Visitor>
        void do_iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix) {
        storage_.do_iterate_type(key_type, std::move(visitor), prefix);
    }

    bool do_key_exists(const VariantKey& key) {
        return storage_.do_key_exists(key);
    }
};
} //namespace arcticdb::storage