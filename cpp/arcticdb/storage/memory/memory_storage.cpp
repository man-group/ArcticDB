/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/storage/memory/memory_storage.hpp>

#include <arcticdb/entity/performance_tracing.hpp>
#include <arcticdb/storage/storage_options.hpp>
#include <arcticdb/storage/storage_utils.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>

#include <arcticdb/toolbox/query_stats.hpp>

namespace arcticdb::storage::memory {

void add_serialization_fields(KeySegmentPair& kv) {
    auto& segment = *kv.segment_ptr();
    auto& hdr = segment.header();
    (void)segment.calculate_size();
    if (hdr.encoding_version() == EncodingVersion::V2) {
        const auto* src = segment.buffer().data();
        set_body_fields(hdr, src);
    }
}

std::string MemoryStorage::name() const { return "memory_storage-0"; }

void MemoryStorage::do_write(KeySegmentPair& key_seg) {
    ARCTICDB_SAMPLE(MemoryStorageWrite, 0)
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);

    auto& key_vec = data_[variant_key_type(key_seg.variant_key())];

    util::variant_match(
            key_seg.variant_key(),
            [&](const RefKey& key) {
                if (auto it = key_vec.find(key); it != key_vec.end()) {
                    key_vec.erase(it);
                }
                add_serialization_fields(key_seg);
                key_vec.try_emplace(key, key_seg.segment().clone());
                etags_[key_seg.variant_key()] = fmt::format("\"mem-{}\"", ++etag_seq_);
            },
            [&](const AtomKey& key) {
                if (key_vec.find(key) != key_vec.end()) {
                    throw DuplicateKeyException(key);
                }
                add_serialization_fields(key_seg);
                key_vec.try_emplace(key, key_seg.segment().clone());
                etags_[key_seg.variant_key()] = fmt::format("\"mem-{}\"", ++etag_seq_);
            }
    );
}

std::string MemoryStorage::do_write_if_none_with_etag(KeySegmentPair& kv) {
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);
    auto& key_vec = data_[variant_key_type(kv.variant_key())];
    if (key_vec.find(kv.variant_key()) != key_vec.end()) {
        storage::raise<ErrorCode::E_ATOMIC_OPERATION_FAILED>(
                "MemoryStorage write_if_none_with_etag failed: key already exists"
        );
    }
    add_serialization_fields(kv);
    key_vec.try_emplace(kv.variant_key(), kv.segment().clone());
    std::string etag = fmt::format("\"mem-{}\"", ++etag_seq_);
    etags_[kv.variant_key()] = etag;
    return etag;
}

std::string MemoryStorage::do_write_if_match(KeySegmentPair& kv, const std::string& expected_etag) {
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);
    auto& key_vec = data_[variant_key_type(kv.variant_key())];
    auto it = key_vec.find(kv.variant_key());
    auto et_it = etags_.find(kv.variant_key());
    if (it == key_vec.end() || et_it == etags_.end() || et_it->second != expected_etag) {
        storage::raise<ErrorCode::E_ATOMIC_OPERATION_FAILED>(
                "MemoryStorage write_if_match failed: ETag mismatch or key missing"
        );
    }
    key_vec.erase(it);
    add_serialization_fields(kv);
    key_vec.try_emplace(kv.variant_key(), kv.segment().clone());
    std::string etag = fmt::format("\"mem-{}\"", ++etag_seq_);
    etags_[kv.variant_key()] = etag;
    return etag;
}

std::pair<KeySegmentPair, std::string> MemoryStorage::do_read_with_etag(VariantKey&& variant_key, ReadKeyOpts) {
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);
    auto& key_vec = data_[variant_key_type(variant_key)];
    auto it = key_vec.find(variant_key);
    if (it == key_vec.end()) {
        throw KeyNotFoundException(variant_key);
    }
    auto et_it = etags_.find(variant_key);
    std::string etag = (et_it != etags_.end()) ? et_it->second : std::string{"\"mem-0\""};
    auto seg = it->second.clone();
    return {KeySegmentPair{std::move(variant_key), std::move(seg)}, std::move(etag)};
}

void MemoryStorage::do_update(KeySegmentPair& key_seg, UpdateOpts opts) {
    ARCTICDB_SAMPLE(MemoryStorageUpdate, 0)
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);

    auto& key_vec = data_[variant_key_type(key_seg.variant_key())];
    auto it = key_vec.find(key_seg.variant_key());

    if (!opts.upsert_ && it == key_vec.end()) {
        std::string err_message =
                fmt::format("do_update called with upsert=false on non-existent key(s): {}", key_seg.variant_key());
        throw KeyNotFoundException(key_seg.variant_key(), err_message);
    }

    if (it != key_vec.end()) {
        key_vec.erase(it);
    }

    add_serialization_fields(key_seg);
    key_vec.insert(std::make_pair(key_seg.variant_key(), key_seg.segment().clone()));
    etags_[key_seg.variant_key()] = fmt::format("\"mem-{}\"", ++etag_seq_);
}

void MemoryStorage::do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts) {
    auto key_seg = do_read(std::move(variant_key), ReadKeyOpts{});
    visitor(key_seg.variant_key(), std::move(*key_seg.segment_ptr()));
}

KeySegmentPair MemoryStorage::do_read(VariantKey&& variant_key, ReadKeyOpts) {
    ARCTICDB_SAMPLE(MemoryStorageRead, 0)
    auto& key_vec = data_[variant_key_type(variant_key)];
    auto it = key_vec.find(variant_key);

    if (it != key_vec.end()) {
        ARCTICDB_DEBUG(log::storage(), "Read key {}: {}", variant_key_type(variant_key), variant_key_view(variant_key));
        auto seg = it->second.clone();
        return {std::move(variant_key), std::move(seg)};
    } else {
        throw KeyNotFoundException(variant_key);
    }
}

bool MemoryStorage::do_key_exists(const VariantKey& key) {
    ARCTICDB_SAMPLE(MemoryStorageKeyExists, 0)
    const auto& key_vec = data_[variant_key_type(key)];
    auto it = key_vec.find(key);
    return it != key_vec.end();
}

void MemoryStorage::do_remove_single(VariantKey&& variant_key, RemoveOpts opts, bool record_legacy) {
    ARCTICDB_SAMPLE(MemoryStorageRemove, 0)
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);
    const auto key_type = variant_key_type(variant_key);
    auto& key_vec = data_[key_type];

    auto it = key_vec.find(variant_key);

    if (it != key_vec.end()) {
        ARCTICDB_DEBUG(log::storage(), "Removed key {}: {}", key_type, variant_key_view(variant_key));
        if (query_stats::is_active()) {
            const uint64_t sz = it->second.size();
            query_stats::note_deleted_bytes(sz);
            if (record_legacy) {
                query_stats::record_legacy_memory_delete(key_type, sz);
            }
        }
        key_vec.erase(it);
        etags_.erase(variant_key);
    } else if (!opts.ignores_missing_key_) {
        if (record_legacy && query_stats::is_active()) {
            query_stats::record_legacy_memory_delete(key_type, 0);
        }
        throw KeyNotFoundException(variant_key);
    } else if (record_legacy && query_stats::is_active()) {
        query_stats::record_legacy_memory_delete(key_type, 0);
    }
}

void MemoryStorage::do_remove(VariantKey&& variant_key, RemoveOpts opts) {
    do_remove_single(std::move(variant_key), opts, /*record_legacy=*/false);
}

void MemoryStorage::do_remove(std::span<VariantKey> variant_keys, RemoveOpts opts) {
    ARCTICDB_SAMPLE(MemoryStorageRemoveMultiple, 0)
    for (auto&& variant_key : variant_keys) {
        do_remove_single(std::move(variant_key), opts, /*record_legacy=*/true);
    }
}

bool MemoryStorage::do_fast_delete() {
    std::lock_guard<std::mutex> cas_lk(cas_mutex_);
    foreach_key_type([&](KeyType key_type) { data_[key_type].clear(); });
    etags_.clear();
    return true;
}

bool MemoryStorage::do_iterate_type_until_match(
        KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix
) {
    ARCTICDB_SAMPLE(MemoryStorageItType, 0)
    auto& key_vec = data_[key_type];
    auto prefix_matcher = stream_id_prefix_matcher(prefix);

    for (auto& key_value : key_vec) {
        auto key = key_value.first;

        if (prefix_matcher(variant_key_id(key))) {
            if (visitor(std::move(key))) {
                return true;
            }
        }
    }
    return false;
}

MemoryStorage::MemoryStorage(const LibraryPath& library_path, OpenMode mode, const Config&) :
    Storage(library_path, mode) {
    arcticdb::entity::foreach_key_type([this](KeyType&& key_type) { data_[key_type]; });
}

} // namespace arcticdb::storage::memory
