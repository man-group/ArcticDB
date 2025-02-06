#pragma once

#include <functional>
#include <random>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/async_storage.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/codec/codec.hpp>

#include <folly/futures/Future.h>

#include <span>

namespace arcticdb::storage {

enum class SupportsAtomicWrites {
    NO,
    YES,
    // There can be no way to differentiate whether a storage supports atomic writes only from its config.
    // As of Nov 2024 AWS S3 and MinIO support atomic write operations. Unfortunately if we're running on PURE or VAST
    // (which are also S3) atomic writes would not work (either through an error or by silently wrongly succeeding).
    // Thus we need the option to test whether a conditional write succeeds and hence the option NEEDS_TEST.
    NEEDS_TEST
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

    [[nodiscard]] bool supports_atomic_writes() {
        if (supports_atomic_writes_.has_value()) {
            return supports_atomic_writes_.value();
        }
        switch (do_supports_atomic_writes()) {
            case SupportsAtomicWrites::NO:
                supports_atomic_writes_ = false;
                break;
            case SupportsAtomicWrites::YES:
                supports_atomic_writes_ = true;
                break;
            case SupportsAtomicWrites::NEEDS_TEST:
                supports_atomic_writes_ = test_atomic_write_support();
                break;
            default:
                util::raise_rte("Invalid SupportsAtomicWrites");
        }
        return supports_atomic_writes_.value();
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
    // Tests whether a storage supports atomic write_if_none operations. The test is required for some backends (e.g. S3)
    // for which different vendors/versions might or might not support atomic operations and might not indicate they're
    // not supporting them in any meaningful way (e.g. as of 2025-01 Vast will happily override an existing key with an
    // IfNoneMatch header).
    [[nodiscard]] bool test_atomic_write_support() {
        auto atomic_write_works_as_expected = false;

        std::random_device rd;
        std::mt19937_64 e2(rd());
        std::uniform_int_distribution<uint64_t> dist;
        // We use the configs map to get a custom suffix to allow inserting a fail trigger for tests
        auto dummy_key_suffix = ConfigsMap::instance()->get_string("Storage.AtomicSupportTestSuffix", "");
        auto dummy_key = RefKey(fmt::format("ATOMIC_TEST_{}_{}{}", dist(e2), dist(e2), dummy_key_suffix), KeyType::ATOMIC_LOCK);
        auto descriptor = stream_descriptor("test", stream::RowCountIndex(), {});
        auto dummy_segment = Segment::initialize(
                SegmentHeader{},
                std::make_shared<Buffer>(),
                descriptor.data_ptr(),
                descriptor.fields_ptr(),
                descriptor.id());
        try {
            // First write should succeed (as we've chosen a unique random key, previously not written to the storage).
            write_if_none(KeySegmentPair{dummy_key, dummy_segment.clone()});
            try {
                // Second write should fail with an AtomicOperationFailed because the key is already written.
                write_if_none(KeySegmentPair{dummy_key, dummy_segment.clone()});
                // If second write succeeded then storage ignores the IfNoneMatch headers and doesn't support atomic writes. (e.g. Vast)
                atomic_write_works_as_expected = false;
            } catch (AtomicOperationFailedException&) {
                atomic_write_works_as_expected = true;
            }
            remove(dummy_key, RemoveOpts{});
        } catch (NotImplementedException&) {
            // If a write_if_none raises a NotImplementedException it doesn't support atomic writes. (e.g. Pure does this)
            atomic_write_works_as_expected = false;
        }
        return atomic_write_works_as_expected;
    }

    virtual void do_write(KeySegmentPair&& key_seg) = 0;

    virtual void do_write_if_none(KeySegmentPair&& kv) = 0;

    virtual void do_update(KeySegmentPair&& key_seg, UpdateOpts opts) = 0;

    virtual void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) = 0;

    virtual KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts opts) = 0;

    virtual void do_remove(VariantKey&& variant_key, RemoveOpts opts) = 0;

    virtual void do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) = 0;

    virtual bool do_key_exists(const VariantKey& key) = 0;

    virtual bool do_supports_prefix_matching() const = 0;

    virtual SupportsAtomicWrites do_supports_atomic_writes() const = 0;

    virtual bool do_fast_delete() = 0;

    // Stop iteration and return true upon the first key k for which visitor(k) is true, return false if no key matches
    // the predicate.
    virtual bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string & prefix) = 0;

    [[nodiscard]] virtual std::string do_key_path(const VariantKey& key) const = 0;

    [[nodiscard]] virtual bool do_is_path_valid(std::string_view) const { return true; }

    LibraryPath lib_path_;
    OpenMode mode_;
    std::optional<bool> supports_atomic_writes_;
};

}
