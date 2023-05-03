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

struct StorageBase {}; // marker class for type checking

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

template<class Impl>
class Storage : public StorageBase {
public:

    Storage(LibraryPath library_path, OpenMode mode) :
        lib_path_(std::move(library_path)),
        mode_(mode) {}

    void write(Composite<KeySegmentPair> &&kvs) {
        ARCTICDB_SAMPLE(VariantStorageWrite, 0)
        return derived().do_write(std::move(kvs));
    }

    void write(KeySegmentPair &&kv) {
        return write(Composite<KeySegmentPair>{std::move(kv)});
    }

    void update(Composite<KeySegmentPair> &&kvs, UpdateOpts opts) {
        ARCTICDB_SAMPLE(VariantStorageUpdate, 0)
        return derived().do_update(std::move(kvs), opts);
    }

    void update(KeySegmentPair &&kv, UpdateOpts opts) {
        return update(Composite<KeySegmentPair>{std::move(kv)}, opts);
    }

    template<class Visitor>
    void read(Composite<VariantKey> &&ks, Visitor &&visitor, ReadKeyOpts opts) {
        return derived().do_read(std::move(ks), std::forward<Visitor>(visitor), opts);
    }

    template<class Visitor>
    void read(VariantKey&& key, Visitor &&visitor, ReadKeyOpts opts) {
        return read(Composite<VariantKey>{std::move(key)}, std::forward<Visitor>(visitor), opts);
    }

    template<class KeyType>
    KeySegmentPair read(KeyType&& key, ReadKeyOpts opts) {
        KeySegmentPair key_seg;
         read(std::forward<KeyType>(key), [&key_seg](auto && vk, auto &&value) {
             key_seg.variant_key() = std::forward<VariantKey>(vk);
             key_seg.segment() = std::forward<Segment>(value);
        }, opts);
         return key_seg;
    }

    void remove(Composite<VariantKey> &&ks, RemoveOpts opts) {
        derived().do_remove(std::move(ks), opts);
    }

    void remove(VariantKey&& key, RemoveOpts opts) {
        return remove(Composite<VariantKey>{std::move(key)}, opts);
    }

    bool supports_prefix_matching() {
        return derived().do_supports_prefix_matching();
    }

    bool fast_delete() {
        return derived().do_fast_delete();
    }

    inline bool key_exists(const VariantKey &key) {
        return derived().do_key_exists(key);
    }

    template<class Visitor>
    void iterate_type(KeyType key_type, Visitor &&visitor, const std::string &prefix = std::string()) {
        derived().do_iterate_type(key_type, std::forward<Visitor>(visitor), prefix);
    }

    [[nodiscard]] const LibraryPath &library_path() const { return lib_path_; }
    [[nodiscard]] OpenMode open_mode() const { return mode_; }

private:
    LibraryPath lib_path_;
    OpenMode mode_;

    Impl &derived() {
        return *static_cast<Impl *>(this);
    }
};

}
