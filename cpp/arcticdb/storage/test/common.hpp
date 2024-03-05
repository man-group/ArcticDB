/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb::storage {

inline VariantKey get_test_key(std::string name) {
    auto builder = arcticdb::atom_key_builder();
    return builder.build<arcticdb::entity::KeyType::TABLE_DATA>(name);
}

inline Segment get_test_segment() {
    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    return encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
}

inline void write_in_store(Storage &store, std::string symbol) {
    auto variant_key = get_test_key(symbol);
    store.write(KeySegmentPair(std::move(variant_key), get_test_segment()));
}

inline void update_in_store(Storage &store, std::string symbol) {
    auto variant_key = get_test_key(symbol);
    store.update(KeySegmentPair(std::move(variant_key), get_test_segment()), arcticdb::storage::StorageUpdateOptions{});
}

inline bool exists_in_store(Storage &store, std::string symbol) {
    auto variant_key = get_test_key(symbol);
    return store.key_exists(variant_key);
}

inline std::string read_in_store(Storage &store, std::string symbol) {
    auto variant_key = get_test_key(symbol);
    auto opts = ReadKeyOpts{};
    auto result = store.read(std::move(variant_key), opts);
    return std::get<StringId>(result.atom_key().id());
}

inline void remove_in_store(Storage &store, std::vector<std::string> symbols) {
    auto to_remove = std::vector<VariantKey>();
    for (auto &symbol: symbols) {
        to_remove.emplace_back(get_test_key(symbol));
    }
    auto opts = RemoveOpts();
    store.remove(Composite(std::move(to_remove)), opts);
}

inline std::set<std::string> list_in_store(Storage &store) {
    auto keys = std::set<std::string>();
    store.iterate_type(KeyType::TABLE_DATA, [&keys](VariantKey &&key) {
        auto atom_key = std::get<AtomKey>(key);
        keys.emplace(std::get<StringId>(atom_key.id()));
    });
    return keys;
}

}
