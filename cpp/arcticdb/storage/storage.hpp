/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

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

#include <folly/futures/Future.h>

namespace arcticdb::storage {

struct StorageBase {}; // marker class for type checking

class DuplicateKeyException : public std::exception {
public:
    explicit DuplicateKeyException(VariantKey key) : key_(std::move(key)) {}
    const char *what() const noexcept override {
        if (msg_.empty()) {
            msg_ = std::visit([](const auto &key) { return fmt::format("duplicate key {}", key); }, key_);
        }
        return msg_.data();
    }
    const VariantKey &key() const {
        return key_;
    }
private:
    VariantKey key_;
    mutable std::string msg_;
};

class NoDataFoundException : public std::exception {
public:
    explicit NoDataFoundException(VariantId key) : key_(std::move(key)) {}

    const char *what() const noexcept override {
        if (msg_.empty()) {
            msg_ = std::visit([](const auto &key) { return fmt::format("{}", key); }, key_);
        }
        return msg_.data();
    }
    const VariantId &key() const {
        return key_;
    }
private:
    VariantId key_;
    mutable std::string msg_;
};

class KeyNotFoundException : public std::runtime_error {
public:
    explicit KeyNotFoundException(Composite<VariantKey>&& keys) :
        std::runtime_error("Key not found"),
        keys_(std::move(keys)) {
    }
    const char *what() const noexcept override {
        if (msg_.empty()) {
            msg_ = fmt::format("{}", keys_);
        }
        return msg_.data();
    }
    Composite<VariantKey> &keys() {
        return keys_;
    }
private:
    Composite<VariantKey> keys_;
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

    void update(Composite<KeySegmentPair> &&kvs) {
        ARCTICDB_SAMPLE(VariantStorageUpdate, 0)
        return derived().do_update(std::move(kvs));
    }

    void update(KeySegmentPair &&kv) {
        return update(Composite<KeySegmentPair>{std::move(kv)});
    }

    template<class Visitor>
    void read(Composite<VariantKey> &&ks, Visitor &&visitor) {
        return derived().do_read(std::move(ks), std::forward<Visitor>(visitor));
    }

    template<class Visitor>
    void read(VariantKey&& key, Visitor &&visitor) {
        return read(Composite<VariantKey>{std::move(key)}, std::forward<Visitor>(visitor));
    }

    template<class KeyType>
    KeySegmentPair read(KeyType&& key) {
        KeySegmentPair key_seg;
         read(std::forward<KeyType>(key), [&key_seg](auto && vk, auto &&value) {
             key_seg.variant_key() = std::forward<VariantKey>(vk);
             key_seg.segment() = std::forward<Segment>(value);
        });
         return key_seg;
    }

    void remove(Composite<VariantKey> &&ks) {
        derived().do_remove(std::move(ks));
    }

    void remove(VariantKey&& key) {
        return remove(Composite<VariantKey>{std::move(key)});
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
    OpenMode open_mode() const { return mode_; }

private:
    LibraryPath lib_path_;
    OpenMode mode_;

    Impl &derived() {
        return *static_cast<Impl *>(this);
    }
};

}