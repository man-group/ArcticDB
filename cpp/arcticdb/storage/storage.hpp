#pragma once

#include <functional>
#include <random>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/async_storage.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/key_segment_pair.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/toolbox/query_stats.hpp>
#include <arcticdb/util/composite.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/stream/index.hpp>

#include <boost/container/small_vector.hpp>
#include <chrono>
#include <optional>
#include <set>
#include <span>
#include <string_view>

namespace arcticdb::storage {

using CompressedSize = uint64_t;
using ObjectSizesVisitor = std::function<void(const VariantKey&, CompressedSize)>;

// '*', '<' and '>' are problematic for S3 and are rejected by every backend, so they form the default set of
// characters disallowed in names. Backends extend this set by overriding Storage::do_unsupported_*_chars().
inline const std::set<char> GLOBALLY_UNSUPPORTED_CHARS{'*', '<', '>'};

struct ObjectSizes {
    // scan_duration_ns has no default: every caller that copies an ObjectSizes has to say what it does with the
    // duration, so a new copy site cannot silently drop it and report a scan that apparently took no time.
    ObjectSizes(KeyType key_type, uint64_t count, CompressedSize compressed_size, uint64_t scan_duration_ns) :
        key_type_(key_type),
        count_(count),
        compressed_size_(compressed_size),
        scan_duration_ns_(scan_duration_ns) {}

    explicit ObjectSizes(KeyType key_type) noexcept : ObjectSizes(key_type, 0, 0, 0) {}

    ObjectSizes(const ObjectSizes& other) noexcept :
        key_type_(other.key_type_),
        count_(other.count_.load()),
        compressed_size_(other.compressed_size_.load()),
        scan_duration_ns_(other.scan_duration_ns_.load()) {}

    ObjectSizes& operator=(const ObjectSizes& that) noexcept {
        if (this != &that) {
            key_type_ = that.key_type_;
            count_ = that.count_.load();
            compressed_size_ = that.compressed_size_.load();
            scan_duration_ns_ = that.scan_duration_ns_.load();
        }

        return *this;
    }

    KeyType key_type_;
    std::atomic_uint64_t count_;
    std::atomic_uint64_t compressed_size_;
    // Wall-clock time AsyncStore::get_object_sizes took to scan this key type, from submission to completion.
    // Zero on an ObjectSizes that was not produced by a scan, e.g. one default constructed to accumulate into.
    std::atomic_uint64_t scan_duration_ns_;
};

enum class SupportsAtomicWrites {
    NO,
    YES,
    // There can be no way to differentiate whether a storage supports atomic writes only from its config.
    // As of Nov 2024 AWS S3 and MinIO support atomic write operations. Unfortunately if we're running on PURE or VAST
    // (which are also S3) atomic writes would not work (either through an error or by silently wrongly succeeding).
    // Thus we need the option to test whether a conditional write succeeds and hence the option NEEDS_TEST.
    NEEDS_TEST
};

namespace detail {

class StorageOpGuard {
  public:
    StorageOpGuard(
            query_stats::StorageBackend backend, query_stats::StorageOp op, entity::KeyType key_type,
            std::string_view library, std::string_view symbol
    ) noexcept :
        backend_(backend),
        op_(op),
        key_type_(key_type),
        library_(library),
        symbol_(symbol),
        start_(std::chrono::steady_clock::now()) {}

    ~StorageOpGuard() noexcept {
        if (!completed_) {
            record(query_stats::StorageOutcome::Error,
                   0,
                   use_tracked_ ? tracked_items_ : 1,
                   use_tracked_ ? tracked_subtract_ns_ : 0);
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(StorageOpGuard);

    void enable_iteration_tracking() noexcept {
        use_tracked_ = true;
        tracked_items_ = 0;
        tracked_subtract_ns_ = 0;
    }

    void inc_items() noexcept { ++tracked_items_; }

    uint64_t& subtract_ns_ref() noexcept { return tracked_subtract_ns_; }

    void finish(query_stats::StorageOutcome outcome) noexcept {
        finish(outcome, 0, use_tracked_ ? tracked_items_ : 1, use_tracked_ ? tracked_subtract_ns_ : 0);
    }

    void finish(
            query_stats::StorageOutcome outcome, uint64_t bytes, uint64_t items = 1, uint64_t subtract_ns = 0
    ) noexcept {
        if (!completed_) {
            record(outcome, bytes, items, subtract_ns);
            completed_ = true;
        }
    }

    void finish_tracked(query_stats::StorageOutcome outcome, uint64_t bytes = 0) noexcept {
        finish(outcome, bytes, tracked_items_, tracked_subtract_ns_);
    }

  private:
    void record(query_stats::StorageOutcome outcome, uint64_t bytes, uint64_t items, uint64_t subtract_ns) noexcept {
        const auto total_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count()
        );
        const uint64_t eff_ns = total_ns > subtract_ns ? (total_ns - subtract_ns) : 0;
        query_stats::record_storage_op(backend_, op_, key_type_, outcome, eff_ns, bytes, items, library_, symbol_);
    }

    query_stats::StorageBackend backend_;
    query_stats::StorageOp op_;
    entity::KeyType key_type_;
    std::string_view library_;
    std::string_view symbol_;
    query_stats::TimePoint start_;
    uint64_t tracked_items_{1};
    uint64_t tracked_subtract_ns_{0};
    bool use_tracked_{false};
    bool completed_{false};
};

class BatchDeleteOpGuard {
  public:
    BatchDeleteOpGuard(
            query_stats::StorageBackend backend, std::span<const query_stats::BatchDeleteKeyInfo> keys,
            std::string_view library
    ) noexcept :
        backend_(backend),
        keys_(keys),
        library_(library),
        start_(std::chrono::steady_clock::now()) {}

    ~BatchDeleteOpGuard() noexcept {
        if (!completed_) {
            record(query_stats::StorageOutcome::Error, 0);
        }
    }

    ARCTICDB_NO_MOVE_OR_COPY(BatchDeleteOpGuard);

    void finish(query_stats::StorageOutcome outcome, uint64_t bytes = 0) noexcept {
        if (!completed_) {
            record(outcome, bytes);
            completed_ = true;
        }
    }

  private:
    void record(query_stats::StorageOutcome outcome, uint64_t bytes) noexcept {
        const auto total_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start_).count()
        );
        query_stats::record_storage_batch_delete(backend_, keys_, outcome, total_ns, bytes, library_);
    }

    query_stats::StorageBackend backend_;
    std::span<const query_stats::BatchDeleteKeyInfo> keys_;
    std::string_view library_;
    query_stats::TimePoint start_;
    bool completed_{false};
};

template<typename Guard, typename Fn>
decltype(auto) run_classified_storage_op(Guard& guard, Fn&& fn) {
    try {
        return fn();
    } catch (const KeyNotFoundException&) {
        guard.finish(query_stats::StorageOutcome::NotFound);
        throw;
    } catch (const DuplicateKeyException&) {
        guard.finish(query_stats::StorageOutcome::Conflict);
        throw;
    } catch (const AtomicOperationFailedException&) {
        guard.finish(query_stats::StorageOutcome::Conflict);
        throw;
    } catch (const NotImplementedException&) {
        guard.finish(query_stats::StorageOutcome::Unsupported);
        throw;
    } catch (const UnsupportedAtomicOperationException&) {
        guard.finish(query_stats::StorageOutcome::Unsupported);
        throw;
    }
}

struct VisitorTimeGuard {
    uint64_t& accum_ns;
    query_stats::TimePoint start;

    explicit VisitorTimeGuard(uint64_t& accum) noexcept : accum_ns(accum), start(std::chrono::steady_clock::now()) {}
    ~VisitorTimeGuard() noexcept {
        accum_ns += static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start).count()
        );
    }
};

} // namespace detail

class Storage {
  public:
    Storage(LibraryPath library_path, OpenMode mode) :
        lib_path_(std::move(library_path)),
        library_name_(lib_path_.empty() ? std::string{} : lib_path_.to_delim_path()),
        mode_(mode) {}

    virtual ~Storage() = default;

    Storage(const Storage&) = delete;
    Storage& operator=(const Storage&) = delete;
    Storage(Storage&&) = default;
    Storage& operator=(Storage&&) = delete;

    [[nodiscard]] virtual query_stats::StorageBackend backend_type() const noexcept {
        return query_stats::StorageBackend::Custom;
    }

    [[nodiscard]] std::string_view library_name() const noexcept { return library_name_; }

    template<typename T>
    void write(T&& key_seg) {
        ARCTICDB_SAMPLE(StorageWrite, 0)
        if (!query_stats::is_active()) {
            return do_write(key_seg);
        }
        const auto key_type = key_seg.key_type();
        const auto sym = detail::extract_symbol_from_variant_key(key_seg.variant_key());
        const uint64_t bytes = key_seg.segment_ptr() ? key_seg.segment_ptr()->size_or_calculate() : 0;
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Write, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Write, key_type, library_name_, sym);
        detail::run_classified_storage_op(guard, [&]() { do_write(key_seg); });
        guard.finish(query_stats::StorageOutcome::Ok, bytes, 1);
    }

    template<typename T>
    void write_if_none(T&& kv) {
        if (!query_stats::is_active()) {
            return do_write_if_none(kv);
        }
        const auto key_type = kv.key_type();
        const auto sym = detail::extract_symbol_from_variant_key(kv.variant_key());
        const uint64_t bytes = kv.segment_ptr() ? kv.segment_ptr()->size_or_calculate() : 0;
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::WriteIfNone, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::WriteIfNone, key_type, library_name_, sym);
        detail::run_classified_storage_op(guard, [&]() { do_write_if_none(kv); });
        guard.finish(query_stats::StorageOutcome::Ok, bytes, 1);
    }

    template<typename T>
    void update(T&& key_seg, UpdateOpts opts) {
        ARCTICDB_SAMPLE(StorageUpdate, 0)
        if (!query_stats::is_active()) {
            return do_update(key_seg, opts);
        }
        const auto key_type = key_seg.key_type();
        const auto sym = detail::extract_symbol_from_variant_key(key_seg.variant_key());
        const uint64_t bytes = key_seg.segment_ptr() ? key_seg.segment_ptr()->size_or_calculate() : 0;
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Update, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Update, key_type, library_name_, sym);
        detail::run_classified_storage_op(guard, [&]() { do_update(key_seg, opts); });
        guard.finish(query_stats::StorageOutcome::Ok, bytes, 1);
    }

    void read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) {
        if (!query_stats::is_active()) {
            return do_read(std::move(variant_key), visitor, opts);
        }
        const auto key_type = variant_key_type(variant_key);
        const std::string sym(detail::extract_symbol_from_variant_key(variant_key));
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Read, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Read, key_type, library_name_, sym);
        const ReadVisitor wrapped_visitor = [&](const VariantKey& k, Segment&& seg) {
            const uint64_t bytes = seg.size_or_calculate();
            guard.finish(query_stats::StorageOutcome::Ok, bytes, 1);
            visitor(k, std::move(seg));
        };
        detail::run_classified_storage_op(guard, [&]() { do_read(std::move(variant_key), wrapped_visitor, opts); });
        guard.finish(query_stats::StorageOutcome::Ok, 0, 1);
    }

    KeySegmentPair read(VariantKey&& variant_key, ReadKeyOpts opts) {
        if (!query_stats::is_active()) {
            return do_read(std::move(variant_key), opts);
        }
        const auto key_type = variant_key_type(variant_key);
        const std::string sym(detail::extract_symbol_from_variant_key(variant_key));
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Read, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Read, key_type, library_name_, sym);
        auto res = detail::run_classified_storage_op(guard, [&]() { return do_read(std::move(variant_key), opts); });
        const uint64_t bytes = res.segment_ptr() ? res.segment_ptr()->size_or_calculate() : 0;
        guard.finish(query_stats::StorageOutcome::Ok, bytes, 1);
        return res;
    }

    [[nodiscard]] virtual bool has_async_api() const { return false; }

    virtual AsyncStorage* async_api() { util::raise_rte("Request for async API on non-async storage"); }

    void remove(VariantKey&& variant_key, RemoveOpts opts) {
        if (!query_stats::is_active()) {
            return do_remove(std::move(variant_key), opts);
        }
        const auto key_type = variant_key_type(variant_key);
        const std::string sym(detail::extract_symbol_from_variant_key(variant_key));
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Delete, key_type);
        query_stats::ScopedDeletedBytesTracker deleted_bytes;
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Delete, key_type, library_name_, sym);
        detail::run_classified_storage_op(guard, [&]() { do_remove(std::move(variant_key), opts); });
        guard.finish(query_stats::StorageOutcome::Ok, deleted_bytes.bytes(), 1);
    }

    void remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
        if (!query_stats::is_active() || variant_keys.empty()) {
            return do_remove(variant_keys, opts);
        }
        // Copy symbol strings up front so backends that move out of variant_keys cannot invalidate key_infos views.
        boost::container::small_vector<std::string, 16> owned_syms;
        owned_syms.reserve(variant_keys.size());
        boost::container::small_vector<query_stats::BatchDeleteKeyInfo, 16> key_infos;
        key_infos.reserve(variant_keys.size());
        for (const auto& k : variant_keys) {
            owned_syms.emplace_back(detail::extract_symbol_from_variant_key(k));
            key_infos.push_back(query_stats::BatchDeleteKeyInfo{variant_key_type(k), owned_syms.back()});
        }
        query_stats::ContextTagScope tag(library_name_, {}, query_stats::StorageOp::DeleteBatch, std::nullopt);
        query_stats::ScopedDeletedBytesTracker deleted_bytes;
        detail::BatchDeleteOpGuard guard(backend_type(), key_infos, library_name_);
        detail::run_classified_storage_op(guard, [&]() { do_remove(variant_keys, opts); });
        guard.finish(query_stats::StorageOutcome::Ok, deleted_bytes.bytes());
    }

    [[nodiscard]] bool supports_prefix_matching() const { return do_supports_prefix_matching(); }

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
        if (!query_stats::is_active()) {
            return do_fast_delete();
        }
        query_stats::ContextTagScope tag(library_name_, {}, query_stats::StorageOp::FastDelete, KeyType::UNDEFINED);
        detail::StorageOpGuard guard(
                backend_type(), query_stats::StorageOp::FastDelete, KeyType::UNDEFINED, library_name_, {}
        );
        const bool res = detail::run_classified_storage_op(guard, [&]() { return do_fast_delete(); });
        guard.finish(query_stats::StorageOutcome::Ok, 0, 1);
        return res;
    }

    // Maximum number of keys this storage will accept in a single do_remove(span) call
    // std::nullopt means no limit (e.g. LMDB, memory)
    [[nodiscard]] virtual std::optional<size_t> max_delete_batch_size() const { return std::nullopt; }

    virtual void cleanup() {}

    inline bool key_exists(const VariantKey& key) {
        if (!query_stats::is_active()) {
            return do_key_exists(key);
        }
        const auto key_type = variant_key_type(key);
        const auto sym = detail::extract_symbol_from_variant_key(key);
        query_stats::ContextTagScope tag(library_name_, sym, query_stats::StorageOp::Exists, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Exists, key_type, library_name_, sym);
        const bool exists = detail::run_classified_storage_op(guard, [&]() { return do_key_exists(key); });
        guard.finish(exists ? query_stats::StorageOutcome::Ok : query_stats::StorageOutcome::NotFound, 0, 1);
        return exists;
    }

    void iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string& prefix = std::string()) {
        if (!query_stats::is_active()) {
            const IterateTypePredicate predicate_visitor = [&visitor](VariantKey&& k) {
                visitor(std::move(k));
                return false; // keep applying the visitor no matter what
            };
            do_iterate_type_until_match(key_type, predicate_visitor, prefix);
            return;
        }
        query_stats::ContextTagScope tag(library_name_, {}, query_stats::StorageOp::List, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::List, key_type, library_name_, {});
        guard.enable_iteration_tracking();
        const IterateTypePredicate predicate_visitor = [&](VariantKey&& k) {
            guard.inc_items();
            try {
                detail::VisitorTimeGuard vguard(guard.subtract_ns_ref());
                visitor(std::move(k));
            } catch (...) {
                guard.finish_tracked(query_stats::StorageOutcome::Ok);
                throw;
            }
            return false; // keep applying the visitor no matter what
        };
        detail::run_classified_storage_op(guard, [&]() {
            do_iterate_type_until_match(key_type, predicate_visitor, prefix);
        });
        guard.finish_tracked(query_stats::StorageOutcome::Ok);
    }

    [[nodiscard]] virtual bool supports_object_size_calculation() const {
        // TODO remove this default implementation when more storages have special size calculations implemented
        return false;
    }

    void visit_object_sizes(KeyType key_type, const std::string& prefix, const ObjectSizesVisitor& visitor) {
        util::check(
                supports_object_size_calculation(),
                "get_object_sizes called on storage {} which does not support "
                "object size calculation",
                name()
        );
        if (!query_stats::is_active()) {
            do_visit_object_sizes(key_type, prefix, visitor);
            return;
        }
        query_stats::ContextTagScope tag(library_name_, {}, query_stats::StorageOp::VisitSizes, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::VisitSizes, key_type, library_name_, {});
        guard.enable_iteration_tracking();
        const ObjectSizesVisitor wrapped_visitor = [&](const VariantKey& k, CompressedSize sz) {
            guard.inc_items();
            try {
                detail::VisitorTimeGuard vguard(guard.subtract_ns_ref());
                visitor(k, sz);
            } catch (...) {
                guard.finish_tracked(query_stats::StorageOutcome::Ok);
                throw;
            }
        };
        detail::run_classified_storage_op(guard, [&]() { do_visit_object_sizes(key_type, prefix, wrapped_visitor); });
        guard.finish_tracked(query_stats::StorageOutcome::Ok);
    }

    bool scan_for_matching_key(KeyType key_type, const IterateTypePredicate& predicate) {
        if (!query_stats::is_active()) {
            return do_iterate_type_until_match(key_type, predicate, std::string());
        }
        query_stats::ContextTagScope tag(library_name_, {}, query_stats::StorageOp::Scan, key_type);
        detail::StorageOpGuard guard(backend_type(), query_stats::StorageOp::Scan, key_type, library_name_, {});
        guard.enable_iteration_tracking();
        const IterateTypePredicate wrapped_predicate = [&](VariantKey&& k) {
            guard.inc_items();
            try {
                detail::VisitorTimeGuard vguard(guard.subtract_ns_ref());
                return predicate(std::move(k));
            } catch (...) {
                guard.finish_tracked(query_stats::StorageOutcome::Ok);
                throw;
            }
        };
        const bool found = detail::run_classified_storage_op(guard, [&]() {
            return do_iterate_type_until_match(key_type, wrapped_predicate, std::string());
        });
        guard.finish_tracked(query_stats::StorageOutcome::Ok);
        return found;
    }

    [[nodiscard]] std::string key_path(const VariantKey& key) const { return do_key_path(key); }

    // Characters this backend cannot store faithfully in a symbol key or snapshot name (a superset of
    // GLOBALLY_UNSUPPORTED_CHARS). Fed into verify_name as its unsupported_chars argument.
    [[nodiscard]] const std::set<char>& unsupported_symbol_chars() const { return do_unsupported_symbol_chars(); }
    // Characters disallowed in a library name. Defaults to GLOBALLY_UNSUPPORTED_CHARS; backends override to add their
    // own restrictions (e.g. Mongo database names disallow '/', Azure blob paths disallow '\').
    [[nodiscard]] const std::set<char>& unsupported_library_chars() const { return do_unsupported_library_chars(); }
    // Positional (suffix) validation of a library name, complementing the unsupported-character sets above. Returns
    // the offending trailing character if the name ends in one the backend cannot represent (currently only LMDB's
    // Windows filesystem rule: no trailing '.' or whitespace).
    [[nodiscard]] std::optional<char> verify_library_suffix(std::string_view path) const {
        return do_verify_library_suffix(path);
    }

    [[nodiscard]] const LibraryPath& library_path() const { return lib_path_; }
    [[nodiscard]] OpenMode open_mode() const { return mode_; }

    [[nodiscard]] virtual std::string name() const = 0;

  private:
    // Tests whether a storage supports atomic write_if_none operations. The test is required for some backends (e.g.
    // S3) for which different vendors/versions might or might not support atomic operations and might not indicate
    // they're not supporting them in any meaningful way (e.g. as of 2025-01 Vast will happily override an existing key
    // with an IfNoneMatch header).
    [[nodiscard]] bool test_atomic_write_support() {
        auto atomic_write_works_as_expected = false;

        std::random_device rd;
        std::mt19937_64 e2(rd());
        std::uniform_int_distribution<uint64_t> dist;
        // We use the configs map to get a custom suffix to allow inserting a fail trigger for tests
        auto dummy_key_suffix = ConfigsMap::instance()->get_string("Storage.AtomicSupportTestSuffix", "");
        auto dummy_key =
                RefKey(fmt::format("ATOMIC_TEST_{}_{}{}", dist(e2), dist(e2), dummy_key_suffix), KeyType::ATOMIC_LOCK);
        auto descriptor = stream_descriptor("test", stream::RowCountIndex(), {});
        auto dummy_segment = Segment::initialize(
                SegmentHeader{},
                std::make_shared<Buffer>(),
                descriptor.data_ptr(),
                descriptor.fields_ptr(),
                descriptor.id()
        );
        try {
            // First write should succeed (as we've chosen a unique random key, previously not written to the storage).
            write_if_none(KeySegmentPair{dummy_key, dummy_segment.clone()});
            try {
                // Second write should fail with an AtomicOperationFailed because the key is already written.
                write_if_none(KeySegmentPair{dummy_key, dummy_segment.clone()});
                // If second write succeeded then storage ignores the IfNoneMatch headers and doesn't support atomic
                // writes. (e.g. Vast)
                atomic_write_works_as_expected = false;
            } catch (AtomicOperationFailedException&) {
                atomic_write_works_as_expected = true;
            }
            remove(dummy_key, RemoveOpts{});
        } catch (NotImplementedException&) {
            // If a write_if_none raises a NotImplementedException it doesn't support atomic writes. (e.g. Pure does
            // this)
            atomic_write_works_as_expected = false;
        }
        return atomic_write_works_as_expected;
    }

    virtual void do_write(KeySegmentPair& key_seg) = 0;

    virtual void do_write_if_none(KeySegmentPair& kv) = 0;

    virtual void do_update(KeySegmentPair& key_seg, UpdateOpts opts) = 0;

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
    virtual bool do_iterate_type_until_match(
            KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
    ) = 0;

    virtual void do_visit_object_sizes(
            [[maybe_unused]] KeyType key_type, [[maybe_unused]] const std::string& prefix,
            [[maybe_unused]] const ObjectSizesVisitor& visitor
    ) {
        // Must be overridden if you want to use this
        util::raise_rte(
                "do_visit_object_sizes called on storage {} that does not support object size calculation {}", name()
        );
    }

    [[nodiscard]] virtual std::string do_key_path(const VariantKey& key) const = 0;

    [[nodiscard]] virtual const std::set<char>& do_unsupported_symbol_chars() const {
        return GLOBALLY_UNSUPPORTED_CHARS;
    }
    [[nodiscard]] virtual const std::set<char>& do_unsupported_library_chars() const {
        return GLOBALLY_UNSUPPORTED_CHARS;
    }
    [[nodiscard]] virtual std::optional<char> do_verify_library_suffix(std::string_view) const { return std::nullopt; }

    LibraryPath lib_path_;
    std::string library_name_;
    OpenMode mode_;
    std::optional<bool> supports_atomic_writes_;
};

} // namespace arcticdb::storage

namespace fmt {
using namespace arcticdb::storage;

template<>
struct formatter<ObjectSizes> {

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const ObjectSizes& sizes, FormatContext& ctx) const {
        return fmt::format_to(
                ctx.out(),
                "ObjectSizes key_type[{}] count[{}] compressed_size[{}] scan_duration_ns[{}]",
                sizes.key_type_,
                sizes.count_,
                sizes.compressed_size_,
                sizes.scan_duration_ns_
        );
    }
};
} // namespace fmt