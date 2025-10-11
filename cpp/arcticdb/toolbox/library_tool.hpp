/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/entity/read_result.hpp>
#include <arcticdb/version/local_versioned_engine.hpp>
#include <memory>

namespace arcticdb::toolbox::apy {

namespace py = pybind11;

class LibraryTool {

  public:
    explicit LibraryTool(std::shared_ptr<storage::Library> lib);

    ReadResult read(const VariantKey& key, std::any& handler_data, OutputFormat output_format);

    ReadResult segment_in_memory_to_read_result(
            arcticdb::SegmentInMemory& segment, std::any& handler_data, OutputFormat output_format
    );

    Segment read_to_segment(const VariantKey& key);

    [[nodiscard]] std::optional<google::protobuf::Any> read_metadata(const VariantKey& key);

    [[nodiscard]] StreamDescriptor read_descriptor(const VariantKey& key);

    [[nodiscard]] TimeseriesDescriptor read_timeseries_descriptor(const VariantKey& key);

    void write(VariantKey key, const Segment& segment);

    void update(VariantKey key, const Segment& segment);

    void overwrite_segment_in_memory(VariantKey key, const SegmentInMemory& segment_in_memory);

    SegmentInMemory item_to_segment_in_memory(
            const StreamId& stream_id, const py::tuple& item, const py::object& norm, const py::object& user_meta,
            std::optional<AtomKey> next_key = std::nullopt
    );

    SegmentInMemory overwrite_append_data(
            VariantKey key, const py::tuple& item, const py::object& norm, const py::object& user_meta
    );

    void remove(VariantKey key);

    std::vector<VariantKey> find_keys(entity::KeyType);

    bool key_exists(const VariantKey& key);

    std::vector<bool> batch_key_exists(const std::vector<VariantKey>& keys);

    std::string get_key_path(const VariantKey& key);

    std::vector<VariantKey> find_keys_for_id(entity::KeyType kt, const StreamId& stream_id);

    int count_keys(entity::KeyType kt);

    void clear_ref_keys();

    std::optional<std::string> inspect_env_variable(std::string name);

    static py::object read_unaltered_lib_cfg(const storage::LibraryManager& lib_manager, std::string lib_name);

  private:
    std::shared_ptr<Store> store();
    async::AsyncStore<>& async_store();
    version_store::LocalVersionedEngine engine_;
};

} // namespace arcticdb::toolbox::apy
