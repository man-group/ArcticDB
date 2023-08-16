/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/util/hash.hpp>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>

#include <folly/Range.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <boost/core/noncopyable.hpp>
#include <filesystem>


#ifdef _WIN32
//Windows #defines DELETE in winnt.h which clashes with OpenMode.DELETE
#undef DELETE
#endif

namespace arcticdb::storage {

class Library {
  public:
    Library(
        LibraryPath path,
        std::shared_ptr<Storages> &&storages,
        LibraryDescriptor::VariantStoreConfig cfg) :
            library_path_(std::move(path)),
            storages_(std::move(storages)),
            config_(std::move(cfg)){
        ARCTICDB_DEBUG(log::storage(), fmt::format("Opened library {}", library_path()));
        util::variant_match(config_,
                            [that = this](const arcticdb::proto::storage::VersionStoreConfig &version_config) {
            that->storage_fallthrough_ = version_config.storage_fallthrough();
            },
            [](std::monostate) {}
            );
    }

    Library(LibraryPath path, std::shared_ptr<Storages> &&storages) :
        Library(std::move(path), std::move(storages), std::monostate{}){}

    Library(const Library&) = delete;
    Library(Library&&) = default;
    Library& operator=(const Library&) = delete;
    Library& operator=(Library&&) = delete;

    /**
     * Tries to get every key of the given type (and prefix if not empty). Please assume this can skip keys sometimes
     * and code defensively.
     * @param visitor Takes one VariantKey which should be moved in but no guarantees
     */
    void iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix=std::string{}) {
        ARCTICDB_SAMPLE(LibraryIterate, 0)
        storages_->iterate_type(key_type, visitor, prefix);
    }

    void write(Composite<KeySegmentPair>&& kvs) {
        ARCTICDB_SAMPLE(LibraryWrite, 0)
        if (open_mode() < OpenMode::WRITE)
            throw PermissionException(library_path_, open_mode(), "write");

        size_t ARCTICDB_UNUSED total_size = kvs.fold([] (size_t s, const KeySegmentPair& seg) { return s + seg.segment().total_segment_size(); }, size_t(0));
        auto kv_count ARCTICDB_UNUSED = kvs.size();
        storages_->write(std::move(kvs));
        ARCTICDB_TRACE(log::storage(), "{} kv written, {} bytes", kv_count, total_size);
    }

    void update(Composite<KeySegmentPair>&& kvs, storage::UpdateOpts opts) {
        ARCTICDB_SAMPLE(LibraryUpdate, 0)
        if (open_mode() < OpenMode::WRITE)
            throw PermissionException(library_path_, open_mode(), "update");

        size_t total_size ARCTICDB_UNUSED = kvs.fold([] (size_t s, const KeySegmentPair& seg) { return s + seg.segment().total_segment_size(); }, size_t(0));
        auto kv_count ARCTICDB_UNUSED = kvs.size();
        storages_->update(std::move(kvs), opts);
        ARCTICDB_TRACE(log::storage(), "{} kv updated, {} bytes", kv_count, total_size);
    }

    void read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
        ARCTICDB_SAMPLE(LibraryRead, 0)
        storages_->read(std::move(ks), visitor, opts, !storage_fallthrough_);
    }

    void remove(Composite<VariantKey>&& ks, storage::RemoveOpts opts) {
        if (open_mode() < arcticdb::storage::OpenMode::DELETE)
            throw PermissionException(library_path_, open_mode(), "delete");

        ARCTICDB_SAMPLE(LibraryRemove, 0)
        storages_->remove(std::move(ks), opts);
    }

    bool fast_delete() {
        return storages_->fast_delete();
    }

    bool key_exists(const VariantKey& key) {
        return storages_->key_exists(key);
    }

    KeySegmentPair read(VariantKey key, ReadKeyOpts opts = ReadKeyOpts{}) {
        KeySegmentPair res{VariantKey{key}};
        util::check(!std::holds_alternative<StringId>(variant_key_id(key)) || !std::get<StringId>(variant_key_id(key)).empty(), "Unexpected empty id");
        const ReadVisitor& visitor = [&res](const VariantKey&, Segment&& value) {
            res.segment() = std::move(value);
        };

        read(Composite<VariantKey>(std::move(key)), visitor, opts);

        return res;
    }

    /** Calls VariantStorage::do_key_path on the primary storage */
    std::string key_path(const VariantKey& key) const {
        return storages_->key_path(key);
    }

    void move_storage(KeyType key_type, timestamp horizon, size_t storage_index = 0) {
        storages_->move_storage(key_type, horizon, storage_index);
    }

    bool supports_prefix_matching() const { return storages_->supports_prefix_matching(); }

    const LibraryPath &library_path() const { return library_path_; }

    OpenMode open_mode() const { return storages_->open_mode(); }

    const auto & config() const { return config_;}

    void set_failure_sim(const arcticdb::proto::storage::VersionStoreConfig::StorageFailureSimulator& cfg) {
       StorageFailureSimulator::instance()->configure(cfg);
    }

    std::string name() {
        return library_path_.to_delim_path();
    }

  private:
    LibraryPath library_path_;
    std::shared_ptr<Storages> storages_;
    LibraryDescriptor::VariantStoreConfig config_;
    bool storage_fallthrough_ = false;
};

inline Library create_library(const LibraryPath& library_path, OpenMode mode, const std::vector<arcticdb::proto::storage::VariantStorage>& storage_configs) {
    return Library{library_path, create_storages(library_path, mode, storage_configs)};
}

}

