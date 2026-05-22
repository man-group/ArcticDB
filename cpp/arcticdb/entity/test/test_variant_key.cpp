/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/ref_key.hpp>
#include <arcticdb/entity/variant_key.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;

namespace {
AtomKey make_atom(KeyType kt, int64_t id) {
    return atom_key_builder()
            .version_id(0)
            .creation_ts(0)
            .content_hash(0)
            .start_index(NumericId{0})
            .end_index(NumericId{0})
            .build(NumericId{id}, kt);
}
} // namespace

TEST(VariantKey, KeyTypesEmpty) {
    std::vector<VariantKey> keys;
    ASSERT_TRUE(key_types(keys).empty());
}

TEST(VariantKey, KeyTypesSingleType) {
    std::vector<VariantKey> keys{make_atom(KeyType::TABLE_DATA, 1), make_atom(KeyType::TABLE_DATA, 2)};
    auto result = key_types(keys);
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result.at(0), KeyType::TABLE_DATA);
}

TEST(VariantKey, KeyTypesMultipleTypesDeduped) {
    std::vector<VariantKey> keys{
            make_atom(KeyType::TABLE_DATA, 1),
            make_atom(KeyType::TABLE_INDEX, 2),
            make_atom(KeyType::TABLE_DATA, 3),
            make_atom(KeyType::VERSION, 4),
            make_atom(KeyType::TABLE_INDEX, 5),
    };
    auto result = key_types(keys);
    ASSERT_EQ(result.size(), 3u);
    // Results are returned in KeyType enum order.
    ASSERT_EQ(result.at(0), KeyType::TABLE_DATA);
    ASSERT_EQ(result.at(1), KeyType::TABLE_INDEX);
    ASSERT_EQ(result.at(2), KeyType::VERSION);
}

TEST(VariantKey, KeyTypesAtomAndRefMix) {
    std::vector<VariantKey> keys{
            make_atom(KeyType::TABLE_DATA, 1),
            RefKey{"sym", KeyType::VERSION_REF},
    };
    auto result = key_types(keys);
    ASSERT_EQ(result.size(), 2u);
    ASSERT_EQ(result.at(0), KeyType::TABLE_DATA);
    ASSERT_EQ(result.at(1), KeyType::VERSION_REF);
}
