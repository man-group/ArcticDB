#pragma once

#include <functional>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/composite.hpp>
#include <util/composite.hpp>

namespace arcticdb::storage {

using ReadVisitor = std::function<void(const VariantKey&, Segment &&)>;

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

    void write(Composite<KeySegmentPair> &&kvs) {
        ARCTICDB_SAMPLE(StorageWrite, 0)
        return do_write(std::move(kvs));
    }

    void write(KeySegmentPair &&kv) {
        return write(Composite<KeySegmentPair>{std::move(kv)});
    }

    void update(Composite<KeySegmentPair> &&kvs, UpdateOpts opts) {
        ARCTICDB_SAMPLE(StorageUpdate, 0)
        return do_update(std::move(kvs), opts);
    }

    void update(KeySegmentPair &&kv, UpdateOpts opts) {
        return update(Composite<KeySegmentPair>{std::move(kv)}, opts);
    }

    void read(Composite<VariantKey> &&ks, const ReadVisitor& visitor, ReadKeyOpts opts) {
        return do_read(std::move(ks), visitor, opts);
    }

    void read(VariantKey&& key, const ReadVisitor& visitor, ReadKeyOpts opts) {
        return read(Composite<VariantKey>{std::move(key)}, visitor, opts);
    }

    template<class KeyType>
    KeySegmentPair read(KeyType&& key, ReadKeyOpts opts) {
        KeySegmentPair key_seg;
        const ReadVisitor& visitor = [&key_seg](const VariantKey & vk, Segment&& value) {
            key_seg.set_key(vk);
            key_seg.segment() = std::move(value);
        };

        read(std::forward<KeyType>(key), visitor, opts);
        return key_seg;
    }

    void remove(Composite<VariantKey> &&ks, RemoveOpts opts) {
        do_remove(std::move(ks), opts);
    }

    void remove(VariantKey&& key, RemoveOpts opts) {
        return remove(Composite<VariantKey>{std::move(key)}, opts);
    }

    [[nodiscard]] bool supports_prefix_matching() const {
        return do_supports_prefix_matching();
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

    [[nodiscard]] bool is_path_valid(const std::string_view path) const {
        return do_is_path_valid(path);
    }

    [[nodiscard]] const LibraryPath &library_path() const { return lib_path_; }
    [[nodiscard]] OpenMode open_mode() const { return mode_; }

    [[nodiscard]] virtual std::string name() const = 0;

private:
    virtual void do_write(Composite<KeySegmentPair>&& kvs) = 0;

    virtual void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) = 0;

    virtual void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) = 0;

    virtual void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) = 0;

    virtual bool do_key_exists(const VariantKey& key) = 0;

    [[nodiscard]] virtual bool do_supports_prefix_matching() const = 0;

    virtual bool do_fast_delete() = 0;

    // Stop iteration and return true upon the first key k for which visitor(k) is true, return false if no key matches
    // the predicate.
    virtual bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string & prefix) = 0;

    [[nodiscard]] virtual std::string do_key_path(const VariantKey& key) const = 0;

    [[nodiscard]] virtual bool do_is_path_valid(const std::string_view) const { return true; }

    LibraryPath lib_path_;
    OpenMode mode_;
};

}
