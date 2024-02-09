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
    explicit LibraryTool(std::shared_ptr<storage::Library> lib) : lib_(std::move(lib)) {}

    ReadResult read(const VariantKey& key);

    Segment read_to_segment(const VariantKey& key);

    void write(VariantKey key, Segment segment);

    void remove(VariantKey key);

    std::vector<VariantKey> find_keys(arcticdb::entity::KeyType);

    std::vector<bool> batch_key_exists(const std::vector<VariantKey>& keys);

    std::string get_key_path(const VariantKey& key);

    std::vector<VariantKey> find_keys_for_id(entity::KeyType kt, const StreamId &stream_id);

    int count_keys(entity::KeyType kt);

    void clear_ref_keys();

private:
    std::shared_ptr<storage::Library> lib_;
};

} //namespace arcticdb::toolbox::apy
