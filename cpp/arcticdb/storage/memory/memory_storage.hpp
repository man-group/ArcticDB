/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/util/composite.hpp>
#include <folly/Range.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <arcticdb/storage/key_segment_pair.hpp>

namespace arcticdb::storage::memory {

    class MemoryStorage final : public Storage {
    public:
        using Config = arcticdb::proto::memory_storage::Config;

        MemoryStorage(const LibraryPath &lib, OpenMode mode, const Config &conf);

    private:
        void do_write(Composite<KeySegmentPair>&& kvs) final;

        void do_update(Composite<KeySegmentPair>&& kvs, UpdateOpts opts) final;

        void do_read(Composite<VariantKey>&& ks, const ReadVisitor& visitor, ReadKeyOpts opts) final;

        void do_remove(Composite<VariantKey>&& ks, RemoveOpts opts) final;

        bool do_key_exists(const VariantKey& key) final;

        bool do_supports_prefix_matching() const final {
            return false;
        }

        inline bool do_fast_delete() final;

        void do_iterate_type(KeyType key_type, const IterateTypeVisitor& visitor, const std::string & prefix) final;

        std::string do_key_path(const VariantKey&) const final { return {}; };

        using KeyMap = folly::ConcurrentHashMap<VariantKey, Segment>;
        // This is pre-populated so that concurrent access is fine.
        // An outer folly::ConcurrentHashMap would only return const inner hash maps which is no good.
        using TypeMap = std::unordered_map<KeyType, KeyMap>;

        TypeMap data_;
    };

    inline arcticdb::proto::storage::VariantStorage pack_config() {
        arcticdb::proto::storage::VariantStorage output;
        arcticdb::proto::memory_storage::Config cfg;
        util::pack_to_any(cfg, *output.mutable_config());
        return output;
    }

}//namespace arcticdbx::storage
