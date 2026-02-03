/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <arcticdb/util/pb_util.hpp>

namespace arcticdb::storage::memory {

class MemoryStorage final : public Storage {
  public:
    using Config = arcticdb::proto::memory_storage::Config;

    MemoryStorage(const LibraryPath& lib, OpenMode mode, const Config& conf);

    std::string name() const final;

  private:
    void do_write(KeySegmentPair& key_seg) final;

    void do_write_if_none(KeySegmentPair& kv [[maybe_unused]]) final {
        storage::raise<ErrorCode::E_UNSUPPORTED_ATOMIC_OPERATION>("Atomic operations are only supported for s3 backend"
        );
    };

    void do_update(KeySegmentPair& key_seg, UpdateOpts opts) final;

    void do_read(VariantKey&& variant_key, const ReadVisitor& visitor, ReadKeyOpts opts) final;

    KeySegmentPair do_read(VariantKey&& variant_key, ReadKeyOpts) final;

    void do_remove(VariantKey&& variant_key, RemoveOpts opts) final;

    void do_remove(std::span<VariantKey> variant_key, RemoveOpts opts) final;

    bool do_key_exists(const VariantKey& key) final;

    bool do_supports_prefix_matching() const final { return false; }

    SupportsAtomicWrites do_supports_atomic_writes() const final { return SupportsAtomicWrites::NO; }

    inline bool do_fast_delete() final;

    bool do_iterate_type_until_match(KeyType key_type, const IterateTypePredicate& visitor, const std::string& prefix)
            final;

    std::string do_key_path(const VariantKey&) const final { return {}; };

    void maybe_write_sleep() const;
    void maybe_read_sleep() const;

    using KeyMap = folly::ConcurrentHashMap<VariantKey, Segment>;
    // This is pre-populated so that concurrent access is fine.
    // An outer folly::ConcurrentHashMap would only return const inner hash maps which is no good.
    using TypeMap = std::unordered_map<KeyType, KeyMap>;

    TypeMap data_;

    bool testing_add_delays_{false};
};

inline arcticdb::proto::storage::VariantStorage pack_config() {
    arcticdb::proto::storage::VariantStorage output;
    arcticdb::proto::memory_storage::Config cfg;
    util::pack_to_any(cfg, *output.mutable_config());
    return output;
}

} // namespace arcticdb::storage::memory
