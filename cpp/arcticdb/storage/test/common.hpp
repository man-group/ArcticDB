/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

namespace arcticdb::storage {

inline VariantKey get_test_key(const std::string& name, entity::KeyType key_type = entity::KeyType::TABLE_DATA) {
    auto builder = arcticdb::atom_key_builder();
    return builder.build(name, key_type);
}

inline Segment get_test_segment() {
    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    return encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
}

inline void write_in_store(
        Storage& store, const std::string& symbol, entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto variant_key = get_test_key(symbol, key_type);
    store.write(KeySegmentPair(std::move(variant_key), get_test_segment()));
}

inline void update_in_store(
        Storage& store, const std::string& symbol, entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto variant_key = get_test_key(symbol, key_type);
    store.update(KeySegmentPair(std::move(variant_key), get_test_segment()), arcticdb::storage::UpdateOpts{});
}

inline bool exists_in_store(
        Storage& store, const std::string& symbol, entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto variant_key = get_test_key(symbol, key_type);
    return store.key_exists(variant_key);
}

inline std::string read_in_store(
        Storage& store, const std::string& symbol, entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto variant_key = get_test_key(symbol, key_type);
    auto opts = ReadKeyOpts{};
    auto result = store.read(std::move(variant_key), opts);
    return std::get<StringId>(result.atom_key().id());
}

inline void remove_in_store(
        Storage& store, const std::vector<std::string>& symbols, entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto to_remove = std::vector<VariantKey>();
    for (auto& symbol : symbols) {
        to_remove.emplace_back(get_test_key(symbol, key_type));
    }
    auto opts = RemoveOpts();
    store.remove(std::span(to_remove), opts);
}

inline std::set<std::string> list_in_store(
        Storage& store, entity::KeyType key_type = entity::KeyType::TABLE_DATA,
        const std::string& prefix = std::string()
) {
    auto keys = std::set<std::string>();
    store.iterate_type(
            key_type,
            [&keys](VariantKey&& key) {
                auto atom_key = std::get<AtomKey>(key);
                keys.emplace(std::get<StringId>(atom_key.id()));
            },
            prefix
    );
    return keys;
}

inline std::set<std::string> populate_store(
        Storage& store, std::string_view symbol_prefix, int start, int end,
        entity::KeyType key_type = entity::KeyType::TABLE_DATA
) {
    auto symbols = std::set<std::string>();
    for (int i = start; i < end; ++i) {
        auto symbol = fmt::format("{}_{}", symbol_prefix, i);
        write_in_store(store, symbol, key_type);
        symbols.emplace(symbol);
    }
    return symbols;
}

} // namespace arcticdb::storage
