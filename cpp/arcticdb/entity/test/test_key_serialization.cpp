/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/serialized_key.hpp>

namespace arcticdb {
std::string old_style_key(const AtomKey& key) {
    return fmt::format(
            "{}|{}|{}|{}|{}|{}|{}",
            key.id(),
            key.version_id(),
            key.creation_ts(),
            key.content_hash(),
            int(stream::get_index_value_type(key)),
            key.start_index(),
            key.end_index()
    );
}
} // namespace arcticdb

TEST(KeySerialize, RoundtripStringidNumericIndex) {
    using namespace arcticdb;
    using namespace arcticdb::entity;

    auto stream_id = StreamId("happy");
    auto key_type = KeyType::TABLE_INDEX;
    auto version_id = VersionId(26);
    auto content_hash = 0xBADF00D;
    auto start_index = IndexValue(NumericIndex{1234});
    auto end_index = IndexValue(NumericIndex{4321});

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .creation_ts(version_id)
                       .content_hash(content_hash)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(stream_id, key_type);

    std::string serialized = to_serialized_key(key);
    auto data = reinterpret_cast<const uint8_t*>(serialized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key = from_serialized_atom_key(data, key_type);
    ASSERT_EQ(key, test_key);
    auto test_key2 = atom_key_from_bytes(data, serialized.size(), key_type);
    std::string tokenized = to_tokenized_key(key);
    data = reinterpret_cast<const uint8_t*>(tokenized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key3 = from_tokenized_atom_key(data, tokenized.size(), key_type);
    ASSERT_EQ(key, test_key);
    auto test_key4 = atom_key_from_bytes(data, tokenized.size(), key_type);
    std::string old_style = old_style_key(key);
    data = reinterpret_cast<const uint8_t*>(old_style.data());
    auto test_key5 = atom_key_from_bytes(data, old_style.size(), key_type);
}

TEST(KeySerialize, RoundtripNumericIdNumericIndex) {
    using namespace arcticdb;
    using namespace arcticdb::entity;

    auto stream_id = StreamId(NumericId{753});
    auto key_type = KeyType::TABLE_DATA;
    auto version_id = VersionId(26);
    auto content_hash = 0xBADF00D;
    auto start_index = IndexValue(NumericIndex{1234});
    auto end_index = IndexValue(NumericIndex{4321});

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .creation_ts(version_id)
                       .content_hash(content_hash)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(stream_id, key_type);

    std::string serialized = to_serialized_key(key);
    auto data = reinterpret_cast<const uint8_t*>(serialized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key = from_serialized_atom_key(data, key_type);
    ASSERT_EQ(key, test_key);
    auto test_key2 = atom_key_from_bytes(data, serialized.size(), key_type);
    std::string tokenized = to_tokenized_key(key);
    data = reinterpret_cast<const uint8_t*>(tokenized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key3 = from_tokenized_atom_key(data, tokenized.size(), key_type);
    ASSERT_EQ(key, test_key);
    auto test_key4 = atom_key_from_bytes(data, tokenized.size(), key_type);
    std::string old_style = old_style_key(key);
    data = reinterpret_cast<const uint8_t*>(old_style.data());
    auto test_key5 = atom_key_from_bytes(data, old_style.size(), key_type);
}

TEST(KeySerialize, RoundtripStringidStringIndex) {
    using namespace arcticdb;
    using namespace arcticdb::entity;

    auto stream_id = StreamId("happy");
    auto key_type = KeyType::TABLE_DATA;
    auto version_id = VersionId(26);
    auto content_hash = 0xBADF00D;
    auto start_index = IndexValue("aaaaa");
    auto end_index = IndexValue("zzzzzz");

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .creation_ts(version_id)
                       .content_hash(content_hash)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(stream_id, key_type);

    std::string serialized = to_serialized_key(key);
    auto data = reinterpret_cast<const uint8_t*>(serialized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key = from_serialized_atom_key(data, key_type);
    ASSERT_EQ(key, test_key);
    auto test_key2 = atom_key_from_bytes(data, serialized.size(), key_type);
    std::string tokenized = to_tokenized_key(key);
    data = reinterpret_cast<const uint8_t*>(tokenized.data());
    ASSERT_TRUE(is_serialized_key(data));
    auto test_key3 = from_tokenized_atom_key(data, tokenized.size(), key_type);
    ASSERT_EQ(key, test_key);
    auto test_key4 = atom_key_from_bytes(data, tokenized.size(), key_type);
    std::string old_style = old_style_key(key);
    data = reinterpret_cast<const uint8_t*>(old_style.data());
    auto test_key5 = atom_key_from_bytes(data, old_style.size(), key_type);
}

TEST(KeySerialize, RefKeyTokenized) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    auto ref_key = RefKey{StreamId{"thing"}, KeyType::SNAPSHOT_REF};
    auto str = to_tokenized_key(ref_key);
    auto out_key = from_tokenized_ref_key((const uint8_t*)str.data(), str.size(), KeyType::SNAPSHOT_REF);
    ASSERT_EQ(ref_key, out_key);
}

TEST(KeySerialize, RefKeySerialized) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    auto ref_key = RefKey{StreamId{"thing"}, KeyType::SNAPSHOT_REF};
    auto str = to_serialized_key(ref_key);
    auto out_key = from_serialized_ref_key((const uint8_t*)str.data(), KeyType::SNAPSHOT_REF);
    ASSERT_EQ(ref_key, out_key);
}

TEST(SerializeNumber, SignedNumbers) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    auto stream_id = StreamId("happy");
    auto key_type = KeyType::TABLE_DATA;
    auto version_id = VersionId(26);
    auto content_hash = 0xBADF00D;
    auto start_index = IndexValue(NumericIndex{-123456789});
    auto end_index = IndexValue(NumericIndex{-987654321});

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .creation_ts(version_id)
                       .content_hash(content_hash)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(stream_id, key_type);

    auto tokenized = to_tokenized_key(key);
    auto new_key = from_tokenized_atom_key((const uint8_t*)tokenized.data(), tokenized.size(), key_type);
    ASSERT_EQ(key, new_key);

    auto serialized = to_serialized_key(key);
    new_key = from_serialized_atom_key((const uint8_t*)serialized.data(), key_type);
    ASSERT_EQ(key, new_key);
}

TEST(KeySerialize, RoundtripStringidSpecialCharacter) {
    using namespace arcticdb;
    using namespace arcticdb::entity;

    auto stream_id = StreamId("happy*.uk");
    auto key_type = KeyType::TABLE_INDEX;
    auto version_id = VersionId(26);
    auto content_hash = 0xBADF00D;
    auto start_index = IndexValue(NumericIndex{234});
    auto end_index = IndexValue(NumericIndex{4321});

    auto key = atom_key_builder()
                       .version_id(version_id)
                       .creation_ts(version_id)
                       .content_hash(content_hash)
                       .start_index(start_index)
                       .end_index(end_index)
                       .build(stream_id, key_type);

    std::string tokenized = to_tokenized_key(key);
    auto data = reinterpret_cast<const uint8_t*>(tokenized.data());
    auto test_key = from_tokenized_atom_key(data, tokenized.size(), key_type);
    ASSERT_EQ(key, test_key);
}
