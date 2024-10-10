/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <pybind11/pybind11.h>

#include <arcticdb/entity/key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/library_manager.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/entity/read_result.hpp>

#include <memory>

namespace arcticdb::storage {
class Library;

class LibraryIndex;
}

namespace arcticdb::toolbox::apy {

namespace py = pybind11;

class LibraryTool {

public:
    explicit LibraryTool(std::shared_ptr<storage::Library> lib);

    ReadResult read(const VariantKey& key);

    Segment read_to_segment(const VariantKey& key);

    [[nodiscard]] std::optional<google::protobuf::Any> read_metadata(const VariantKey& key);

    [[nodiscard]] StreamDescriptor read_descriptor(const VariantKey& key);

    [[nodiscard]] TimeseriesDescriptor read_timeseries_descriptor(const VariantKey& key);

    void write(VariantKey key, Segment& segment);

    void remove(VariantKey key);

    std::vector<VariantKey> find_keys(arcticdb::entity::KeyType);

    bool key_exists(const VariantKey& key);

    std::vector<bool> batch_key_exists(const std::vector<VariantKey>& keys);

    std::string get_key_path(const VariantKey& key);

    std::vector<VariantKey> find_keys_for_id(entity::KeyType kt, const StreamId &stream_id);

    int count_keys(entity::KeyType kt);

    void clear_ref_keys();

    std::optional<std::string> inspect_env_variable(std::string name);

    static py::object read_unaltered_lib_cfg(const storage::LibraryManager& lib_manager, std::string lib_name);

private:
    // TODO: Remove the shared_ptr and just keep the store.
    // The only reason we use a shared_ptr for the store is to be able to pass it to delete_all_keys_of_type.
    // We can remove the shared_ptr when delete_all_keys_of_type takes a const ref instead of a shared pointer.
    std::shared_ptr<arcticdb::async::AsyncStore<util::SysClock>> store_;
};

} //namespace arcticdb::toolbox::apy
