#pragma once
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/failure_simulation.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/type_traits.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <boost/callable_traits.hpp>
#include <folly/Range.h>
#include <type_traits>
#include <iterator>
#include <array>
#include <string_view>
#include <storage/key_segment_pair.hpp>
#include <util/composite.hpp>
#include <folly/futures/Future.h>

namespace arcticdb::storage {

using ReadVisitor = std::function<void(const VariantKey&, Segment &&)>;

class DuplicateKeyException : public ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY> {
public:
    explicit DuplicateKeyException(VariantKey key) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(std::string(variant_key_view(key))),
        key_(std::move(key)) {}

    [[nodiscard]] const VariantKey &key() const {
        return key_;
    }
private:
    VariantKey key_;
};

class NoDataFoundException : public ArcticCategorizedException<ErrorCategory::MISSING_DATA> {
public:
    explicit NoDataFoundException(VariantId key) :
            ArcticCategorizedException<ErrorCategory::MISSING_DATA>(std::visit([](const auto &key) { return fmt::format("{}", key); }, key)),
            key_(key){
    }

    explicit NoDataFoundException(const std::string& msg) :
            ArcticCategorizedException<ErrorCategory::MISSING_DATA>(msg) {
    }

    explicit NoDataFoundException(const char* msg) :
            ArcticCategorizedException<ErrorCategory::MISSING_DATA>(std::string(msg)) {
    }

    [[nodiscard]] const VariantId &key() const {
        util::check(static_cast<bool>(key_), "Key not found");
        return *key_;
    }
private:
    std::optional<VariantId> key_;
};

class KeyNotFoundException : public ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY> {
public:
    explicit KeyNotFoundException(Composite<VariantKey>&& keys) :
        ArcticSpecificException<ErrorCode::E_DUPLICATE_KEY>(fmt::format("{}", keys)),
        keys_(std::make_shared<Composite<VariantKey>>(std::move(keys))) {
    }

    Composite<VariantKey>& keys() {
        return *keys_;
    }
private:
    std::shared_ptr<Composite<VariantKey>> keys_;
    mutable std::string msg_;
};

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
            key_seg.variant_key() = vk;
            key_seg.segment() = value;
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

    bool supports_prefix_matching() const {
        return do_supports_prefix_matching();
    }

    bool fast_delete() {
        return do_fast_delete();
    }

    inline bool key_exists(const VariantKey &key) {
        return do_key_exists(key);
    }

    void iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string &prefix = std::string()) {
        do_iterate_type(key_type, visitor, prefix);
    }

    std::string key_path(const VariantKey& key) const {
        return do_key_path(key);
    }

    [[nodiscard]] const LibraryPath &library_path() const { return lib_path_; }
    [[nodiscard]] OpenMode open_mode() const { return mode_; }

private:
    virtual void do_write(Composite<KeySegmentPair>&& kvs) = 0;

    virtual void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) = 0;

    virtual void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) = 0;

    virtual void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) = 0;

    virtual bool do_key_exists(const VariantKey& key) = 0;

    virtual bool do_supports_prefix_matching() const = 0;

    virtual bool do_fast_delete() = 0;

    virtual void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string & prefix) = 0;

    virtual std::string do_key_path(const VariantKey& key) const = 0;

    LibraryPath lib_path_;
    OpenMode mode_;
};

}
