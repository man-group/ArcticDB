/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/library_path.hpp>

#include <arcticdb/entity/serialized_key.hpp>
#include <arcticdb/entity/protobuf_mappings.hpp>

using namespace arcticdb;
using namespace arcticdb::entity;

TEST(Key, Basic) {
    using namespace arcticdb;
    using namespace arcticdb::entity;
    using namespace arcticdb::storage;

    NumericId numeric_id(127);
    VersionId version_id(384);
    timestamp creation_ts(34534);
    ContentHash content_hash(2734);
    KeyType numeric_data_key_type(KeyType::TABLE_DATA);
    IndexValue timestamp_start(NumericId{33});
    IndexValue timestamp_end(NumericId{57});
    AtomKey construct_numeric_key
        (numeric_id, version_id, creation_ts, content_hash, timestamp_start, timestamp_end, numeric_data_key_type);

    auto build_numeric_key =
        atom_key_builder().version_id(version_id).creation_ts(creation_ts).content_hash(content_hash).start_index(
            timestamp_start).end_index(timestamp_end).build(numeric_id, numeric_data_key_type);

    ASSERT_EQ(construct_numeric_key, build_numeric_key);

    std::string numeric_key_string(to_tokenized_key(build_numeric_key));
    auto numeric_key_from_bytes =
        atom_key_from_bytes(reinterpret_cast<uint8_t *>(numeric_key_string.data()),
                            numeric_key_string.size(),
                            numeric_data_key_type);
    ASSERT_EQ(build_numeric_key, numeric_key_from_bytes);

    StringId string_id("Geebles");
    KeyType string_data_key_type(KeyType::TABLE_DATA);
    AtomKey construct_string_key
        (string_id, version_id, creation_ts, content_hash, timestamp_start, timestamp_end, string_data_key_type);
    std::string string_key_string(to_serialized_key(construct_string_key));
    auto string_key_from_bytes =
        atom_key_from_bytes(reinterpret_cast<uint8_t *>(string_key_string.data()),
                            string_key_string.size(),
                            string_data_key_type);

    ASSERT_EQ(construct_string_key, string_key_from_bytes);

    IndexValue string_start("Foffbot");
    IndexValue string_end("Xoffbot");
    auto build_numeric_key_string_index =
        atom_key_builder().version_id(version_id).creation_ts(creation_ts).content_hash(content_hash).start_index(
            string_start).end_index(string_end).build(numeric_id, numeric_data_key_type);

    std::string string_index_key_string(to_tokenized_key(build_numeric_key_string_index));
    auto string_index_from_bytes =
        atom_key_from_bytes(reinterpret_cast<uint8_t *>(string_index_key_string.data()),
                            string_index_key_string.size(),
                            numeric_data_key_type);

    ASSERT_EQ(build_numeric_key_string_index, string_index_from_bytes);
}

TEST(Key, StringViewable) {
    using namespace arcticdb::storage;

    DefaultStringViewable sv{"toto"};
    DefaultStringViewable sv2{"toto"};

    ASSERT_EQ(sv.hash(), sv2.hash());
    ASSERT_EQ(sv, sv2);

}

TEST(Key, Library) {
    using namespace arcticdb::storage;

    LibraryPath lib{"a", "b"};
    LibraryPath lib2(std::views::all(std::vector<std::string>{"a", "b"}));

    ASSERT_EQ(lib.hash(), lib2.hash());
    ASSERT_EQ(lib, lib2);

    ASSERT_EQ(lib, LibraryPath(lib));

    auto s = std::string_view("a");
    std::string_view s2 = *lib.as_range().begin();
    ASSERT_EQ(s, s2);

    ASSERT_EQ(lib.to_delim_path(), "a.b");
    ASSERT_EQ(LibraryPath::from_delim_path("a.b"), lib);
}

struct AlternativeFormat {
    static constexpr char format[] = "t={},id={},g={:d},h=0x{:x},c={:d},s={},e={}";
};

TEST(Key, Formatting) {

    AtomKey k{
        arcticdb::StreamId{NumericId{999}},
        VersionId(123),
        timestamp(123000000LL),
        0x789456321ULL,
        NumericIndex(122000000ULL),
        NumericIndex(122000999ULL),
        KeyType::TABLE_DATA};

    AtomKey k2 = k;

    ASSERT_EQ(k2, k);

    auto k3 = atom_key_builder().gen_id(123).creation_ts(123000000)
        .start_index(timestamp(122000000)).end_index(timestamp(122000999))
        .content_hash(0x789456321).build<KeyType::TABLE_DATA>(NumericId{999});

    ASSERT_EQ(k3, k2);

    auto def_s0 = fmt::format("{}", k);
    ASSERT_EQ("d:999:123:0x789456321@123000000[122000000,122000999]", def_s0);

    //Default formatting using formattable ref:
    FormattableRef fk(k);
    auto def_s1 = fmt::format("{}", fk);
    auto def_s2 = fmt::format("{}", formattable(k));

    ASSERT_EQ(def_s0, def_s1);
    ASSERT_EQ(def_s1, def_s2);

    // Overload key formatting with a tag to avoid strings all over the place
    auto alt = fmt::format("{}", formattable<AtomKey, AlternativeFormat>(k));
    ASSERT_EQ("t=d,id=999,g=123,h=0x789456321,c=123000000,s=122000000,e=122000999", alt);

}


TEST(AtomKey, ProtobufRoundtrip) {
    auto key = atom_key_builder().version_id(0).content_hash(1).creation_ts(2).start_index(3)
    .end_index(4).build(StreamId{"Natbag"}, KeyType::TABLE_INDEX);

    auto pb_key = arcticdb::key_to_proto(key);
    auto decoded_key = arcticdb::key_from_proto(pb_key);
    ASSERT_EQ(key, decoded_key);
}
