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
#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <cstdlib>

namespace arcticdb::toolbox::apy {

using namespace arcticdb::entity;

LibraryTool::LibraryTool(std::shared_ptr<storage::Library> lib) {
    store_ = std::make_shared<async::AsyncStore<util::SysClock>>(lib, codec::default_lz4_codec(), encoding_version(lib->config()));
}

ReadResult LibraryTool::read(const VariantKey& key) {
    auto segment = read_to_segment(key);
    auto segment_in_memory = decode_segment(std::move(segment));
    auto frame_and_descriptor = frame_and_descriptor_from_segment(std::move(segment_in_memory));
    return pipelines::make_read_result_from_frame(frame_and_descriptor, to_atom(key));
}

Segment LibraryTool::read_to_segment(const VariantKey& key) {
    auto kv = store_->read_compressed_sync(key, storage::ReadKeyOpts{});
    util::check(kv.has_segment(), "Failed to read key: {}", key);
    kv.segment().force_own_buffer();
    return kv.segment();
}

std::optional<google::protobuf::Any> LibraryTool::read_metadata(const VariantKey& key){
    return store_->read_metadata(key, storage::ReadKeyOpts{}).get().second;
}

StreamDescriptor LibraryTool::read_descriptor(const VariantKey& key){
    auto metadata_and_descriptor = store_->read_metadata_and_descriptor(key, storage::ReadKeyOpts{}).get();
    return std::get<StreamDescriptor>(metadata_and_descriptor);
}

TimeseriesDescriptor LibraryTool::read_timeseries_descriptor(const VariantKey& key){
    return store_->read_timeseries_descriptor(key).get().second;
}

void LibraryTool::write(VariantKey key, Segment segment) {
    storage::KeySegmentPair kv{std::move(key), std::move(segment)};
    store_->write_compressed_sync(std::move(kv));
}

void LibraryTool::remove(VariantKey key) {
    store_->remove_key_sync(std::move(key), storage::RemoveOpts{});
}

void LibraryTool::clear_ref_keys() {
    delete_all_keys_of_type(KeyType::SNAPSHOT_REF, store_, false);
}

std::vector<VariantKey> LibraryTool::find_keys(entity::KeyType kt) {
    std::vector<VariantKey> res;

    store_->iterate_type(kt, [&](VariantKey &&found_key) {
        res.emplace_back(found_key);
    }, "");
    return res;
}

int LibraryTool::count_keys(entity::KeyType kt) {
    int count = 0;

    const IterateTypeVisitor& visitor = [&](VariantKey &&) {
        count++;
    };

    store_->iterate_type(kt, visitor, "");
    return count;
}

std::vector<bool> LibraryTool::batch_key_exists(const std::vector<VariantKey>& keys) {
    auto key_exists_fut = store_->batch_key_exists(keys);
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

    store_->iterate_type(kt, visitor, string_id);
    return res;
}

std::string LibraryTool::get_key_path(const VariantKey& key) {
    return store_->key_path(key);
}

std::optional<std::string> LibraryTool::inspect_env_variable(std::string name){
    auto value = getenv(name.c_str());
    if (value == nullptr) return std::nullopt;
    return std::string(value);
}


} // namespace arcticdb::toolbox::apy