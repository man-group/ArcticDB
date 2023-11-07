/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/toolbox/library_tool.hpp>

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/util/variant.hpp>

namespace arcticdb::toolbox::apy {

using namespace arcticdb::entity;

Segment LibraryTool::read_to_segment(const VariantKey& key) {
    auto kv = std::visit([lib=lib_](const auto &k) { return lib->read(k); }, key);
    util::check(kv.has_segment(), "Failed to read key: {}", key);
    return kv.segment();
}

void LibraryTool::write(VariantKey key, Segment segment) {
    storage::KeySegmentPair kv{std::move(key), std::move(segment)};
    lib_->write(Composite<storage::KeySegmentPair>{std::move(kv)});
}

void LibraryTool::remove(VariantKey key) {
    lib_->remove(Composite<VariantKey>{std::move(key)}, storage::RemoveOpts{});
}

void LibraryTool::clear_ref_keys() {
    auto store = std::make_shared<arcticdb::async::AsyncStore<util::SysClock>>(lib_, codec::default_lz4_codec(), encoding_version(lib_->config()));
    delete_all_keys_of_type(KeyType::SNAPSHOT_REF, store, false);
}

std::vector<VariantKey> LibraryTool::find_keys(entity::KeyType kt) {
    std::vector<VariantKey> res;

    lib_->iterate_type(kt, [&](VariantKey &&found_key) {
        res.emplace_back(found_key);
    });
    return res;
}

int LibraryTool::count_keys(entity::KeyType kt) {
    int count = 0;

    const IterateTypeVisitor& visitor = [&](VariantKey &&) {
        count++;
    };

    lib_->iterate_type(kt, visitor);
    return count;
}

std::vector<bool> LibraryTool::batch_key_exists(const std::vector<VariantKey>& keys) {
    auto store = std::make_shared<arcticdb::async::AsyncStore<util::SysClock>>(lib_, codec::default_lz4_codec(), encoding_version(lib_->config()));
    auto key_exists_fut = store->batch_key_exists(keys);
    return folly::collect(key_exists_fut).get();
}

std::vector<VariantKey> LibraryTool::find_keys_for_id(entity::KeyType kt, const StreamId &stream_id) {
    util::check(std::holds_alternative<StringId>(stream_id), "keys for id only implemented for string ids");

    std::vector<VariantKey> res;
    const auto &string_id = std::get<StringId>(stream_id);

    const IterateTypeVisitor& visitor = [&](VariantKey &&found_key) {
        // Only S3 handles the prefix in iterate_type, the others just return everything, thus the additional check.
        if (variant_key_id(found_key) == stream_id) {
            res.emplace_back(found_key);
        }
    };

    lib_->iterate_type(kt, visitor, string_id);
    return res;
}

std::string LibraryTool::get_key_path(const VariantKey& key) {
    return lib_->key_path(key);
}

} // namespace arcticdb::toolbox::apy