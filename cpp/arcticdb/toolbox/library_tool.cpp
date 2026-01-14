/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/toolbox/library_tool.hpp>

#include <arcticdb/async/async_store.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/pipeline/pipeline_utils.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/util/clock.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/stream/incompletes.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>
#include <cstdlib>

namespace arcticdb::toolbox::apy {

using namespace arcticdb::entity;

LibraryTool::LibraryTool(std::shared_ptr<storage::Library> lib) : engine_(lib, util::SysClock()) {}

std::shared_ptr<Store> LibraryTool::store() { return engine_._test_get_store(); }

async::AsyncStore<>& LibraryTool::async_store() { return dynamic_cast<async::AsyncStore<>&>(*store()); }

ReadResult LibraryTool::segment_in_memory_to_read_result(
        arcticdb::SegmentInMemory& segment, std::any& handler_data, OutputFormat output_format
) {
    std::pair<std::any&, OutputFormat> handler{handler_data, output_format};

    // This is a dummy atom key needed to construct the read result, otherwise not important
    const auto& atom_key = AtomKeyBuilder().build<KeyType::VERSION_REF>(segment.descriptor().id());
    auto frame_and_descriptor = frame_and_descriptor_from_segment(std::move(segment));

    return pipelines::read_result_from_single_frame(frame_and_descriptor, atom_key, handler_data, output_format);
}

Segment LibraryTool::read_to_segment(const VariantKey& key) {
    auto kv = store()->read_compressed_sync(key);
    util::check(kv.has_segment(), "Failed to read key: {}", key);
    return kv.segment().clone();
}

std::optional<google::protobuf::Any> LibraryTool::read_metadata(const VariantKey& key) {
    return store()->read_metadata(key, storage::ReadKeyOpts{}).get().second;
}

StreamDescriptor LibraryTool::read_descriptor(const VariantKey& key) {
    auto metadata_and_descriptor = store()->read_metadata_and_descriptor(key, storage::ReadKeyOpts{}).get();
    return std::get<StreamDescriptor>(metadata_and_descriptor);
}

TimeseriesDescriptor LibraryTool::read_timeseries_descriptor(const VariantKey& key) {
    return store()->read_timeseries_descriptor(key).get().second;
}

void LibraryTool::write(VariantKey key, const Segment& segment) {
    storage::KeySegmentPair kv{std::move(key), segment.clone()};
    store()->write_compressed_sync(kv);
}

void LibraryTool::overwrite_segment_in_memory(VariantKey key, const SegmentInMemory& segment_in_memory) {
    auto segment = encode_dispatch(segment_in_memory.clone(), *(async_store().codec_), async_store().encoding_version_);
    storage::UpdateOpts opts;
    opts.upsert_ = true;
    storage::KeySegmentPair kv{std::move(key), std::move(segment)};
    store()->update_compressed_sync(kv, opts);
}

SegmentInMemory LibraryTool::item_to_segment_in_memory(
        const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
        std::optional<AtomKey> next_key
) {
    auto frame =
            convert::py_ndf_to_frame(stream_id, item, norm, user_meta, engine_.cfg().write_options().empty_types());
    auto segment_in_memory = incomplete_segment_from_tensor_frame(
            frame, 0, std::move(next_key), engine_.cfg().write_options().allow_sparse()
    );
    return segment_in_memory;
}

SegmentInMemory LibraryTool::overwrite_append_data(
        VariantKey key, const py::tuple& item, const py::object& norm, const py::object& user_meta
) {
    user_input::check<ErrorCode::E_INVALID_USER_ARGUMENT>(
            std::holds_alternative<AtomKey>(key) && std::get<AtomKey>(key).type() == KeyType::APPEND_DATA,
            "Can only override APPEND_DATA keys. Received: {}",
            key
    );
    auto old_segment = read_to_segment(key);
    auto old_segment_in_memory = decode_segment(old_segment);
    const auto& tsd = old_segment_in_memory.index_descriptor();
    std::optional<AtomKey> next_key = std::nullopt;
    if (tsd.proto().has_next_key()) {
        next_key = key_from_proto(tsd.proto().next_key());
    }

    auto stream_id = util::variant_match(key, [](const auto& key) { return key.id(); });
    auto segment_in_memory = item_to_segment_in_memory(stream_id, item, norm, user_meta, next_key);
    overwrite_segment_in_memory(key, segment_in_memory);
    return old_segment_in_memory;
}

bool LibraryTool::key_exists(const VariantKey& key) { return store()->key_exists_sync(key); }

void LibraryTool::remove(VariantKey key) { store()->remove_key_sync(std::move(key), storage::RemoveOpts{}); }

void LibraryTool::clear_ref_keys() { delete_all_keys_of_type(KeyType::SNAPSHOT_REF, store(), false); }

std::vector<VariantKey> LibraryTool::find_keys(entity::KeyType kt) {
    std::vector<VariantKey> res;

    store()->iterate_type(kt, [&](VariantKey&& found_key) { res.emplace_back(found_key); }, "");
    return res;
}

int LibraryTool::count_keys(entity::KeyType kt) {
    int count = 0;

    const IterateTypeVisitor& visitor = [&](VariantKey&&) { count++; };

    store()->iterate_type(kt, visitor, "");
    return count;
}

std::vector<bool> LibraryTool::batch_key_exists(const std::vector<VariantKey>& keys) {
    auto key_exists_fut = store()->batch_key_exists(keys);
    return folly::collect(key_exists_fut).get();
}

std::vector<VariantKey> LibraryTool::find_keys_for_id(entity::KeyType kt, const StreamId& stream_id) {
    util::check(std::holds_alternative<StringId>(stream_id), "keys for id only implemented for string ids");

    std::vector<VariantKey> res;
    const auto& string_id = std::get<StringId>(stream_id);

    const IterateTypeVisitor& visitor = [&](VariantKey&& found_key) {
        // Only S3 handles the prefix in iterate_type, the others just return everything, thus the additional check.
        if (variant_key_id(found_key) == stream_id) {
            res.emplace_back(found_key);
        }
    };

    store()->iterate_type(kt, visitor, string_id);
    return res;
}

std::string LibraryTool::get_key_path(const VariantKey& key) { return async_store().key_path(key); }

std::optional<std::string> LibraryTool::inspect_env_variable(std::string name) {
    auto value = getenv(name.c_str());
    if (value == nullptr)
        return std::nullopt;
    return std::string(value);
}

py::object LibraryTool::read_unaltered_lib_cfg(const storage::LibraryManager& lib_manager, std::string lib_name) {
    return lib_manager.get_unaltered_library_config(storage::LibraryPath{lib_name, '.'});
}

} // namespace arcticdb::toolbox::apy