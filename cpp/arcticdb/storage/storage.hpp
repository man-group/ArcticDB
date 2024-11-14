#pragma once

#include <functional>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/async_storage.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/composite.hpp>

#include <folly/futures/Future.h>

#include <span>

namespace arcticdb::storage {

class Storage {
public:
    Storage(LibraryPath library_path, OpenMode mode) :
        lib_path_(std::move(library_path)),
        mode_(mode) {}

    virtual ~Storage() = default;

    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;
    Storage(Storage&&) = default;
    Storage& operator=(Storage&&) = delete;

    void write(KeySegmentPair&& key_seg) {
        ARCTICDB_SAMPLE(StorageWrite, 0)
        return do_write(std::move(key_seg));
    }

    void write_if_none(KeySegmentPair&& kv) {
        return do_write_if_none(std::move(kv));
    }

    void update(KeySegmentPair&& key_seg, UpdateOpts opts) {
        ARCTICDB_SAMPLE(StorageUpdate, 0)
        return do_update(std::move(key_seg), opts);
    }

    void read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
        return do_read(std::move(variant_key), visitor, opts);
    }

    KeySegmentPair read(VariantKey&& variant_key, ReadKeyOpts opts) {
        return do_read(std::move(variant_key), opts);
    }

    [[nodiscard]] virtual bool has_async_api() const {
        return false;
    }

    virtual AsyncStorage* async_api() {
        util::raise_rte("Request for async API on non-async storage");
    }

    void remove(VariantKey&& variant_key, RemoveOpts opts) {
        do_remove(std::move(variant_key), opts);
    }

    void remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
        return do_remove(variant_keys, opts);
    }

    [[nodiscard]] bool supports_prefix_matching() const {
        return do_supports_prefix_matching();
    }

    [[nodiscard]] bool supports_atomic_writes() const {
        return do_supports_atomic_writes();
    }

    bool fast_delete() {
        return do_fast_delete();
    }

    virtual void cleanup() { }

    inline bool key_exists(const VariantKey &key) {
        return do_key_exists(key);
    }

    bool scan_for_matching_key(KeyType key_type, const IterateTypePredicate& predicate) {
      return do_iterate_type_until_match(key_type, predicate, std::string());
    }

    void iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix = std::string()) {
        const IterateTypePredicate predicate_visitor = [&visitor](VariantKey&& k) {
          visitor(std::move(k));
          return false; // keep applying the visitor no matter what
        };
      do_iterate_type_until_match(key_type, predicate_visitor, prefix);
    }

    [[nodiscard]] std::string key_path(const VariantKey& key) const {
        return do_key_path(key);
    }

    [[nodiscard]] bool is_path_valid(std::string_view path) const {
        return do_is_path_valid(path);
    }

    [[nodiscard]] const LibraryPath &library_path() const { return lib_path_; }
    [[nodiscard]] OpenMode open_mode() const { return mode_; }

    [[nodiscard]] virtual std::string name() const = 0;

private:
    virtual void do_write(KeySegmentPair&& key_seg) = 0;

    virtual void do_write_if_none(KeySegmentPair&& kv) = 0;

    virtual void do_update(KeySegmentPair&& key_seg, UpdateOpts opts) = 0;

    virtual void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) = 0;

    virtual KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) = 0;

    virtual void do_remove(VariantKey&& variant_key, RemoveOpts opts) = 0;

    virtual void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) = 0;

    virtual bool do_key_exists(const VariantKey& key) = 0;

    virtual bool do_supports_prefix_matching() const = 0;

    virtual bool do_supports_atomic_writes() const = 0;

    virtual bool do_fast_delete() = 0;

    // Stop iteration and return true upon the first key k for which visitor(k) is true, return false if no key matches
    // the predicate.
    virtual bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string & prefix) = 0;

    [[nodiscard]] virtual std::string do_key_path(const VariantKey& key) const = 0;

    [[nodiscard]] virtual bool do_is_path_valid(std::string_view) const { return true; }

    LibraryPath lib_path_;
    OpenMode mode_;
};

}
