#include <gtest/gtest.h>
#include "arcticdb/storage/coalesced/multisegment_header.hpp"
#include "arcticdb/storage/coalesced/multisegment.hpp"

TEST(MultiSegment, AddToHeader) {
    using namespace arcticdb;

    auto key_1 = atom_key_builder().start_index(1).end_index(2).version_id(3).content_hash(4).creation_ts(5).build(StreamId("symbol_1"), KeyType::TABLE_DATA);
    auto key_2 = atom_key_builder().start_index(3).end_index(4).version_id(5).content_hash(6).creation_ts(7).build(StreamId("symbol_1"), KeyType::TABLE_DATA);
    storage::MultiSegmentHeader header("test_multi");
    header.add_key(key_1, 0, 5);
    header.add_key(key_2, 5, 8);
    auto offset_and_size = header.get_offset_for_key(key_2);
    ASSERT_EQ(offset_and_size->first, 5);
    ASSERT_EQ(offset_and_size->second, 8);
    auto key_3 = atom_key_builder().start_index(5).end_index(6).version_id(7).content_hash(8).creation_ts(9).build(StreamId("symbol_1"), KeyType::TABLE_DATA);
    auto not_there = header.get_offset_for_key(key_3);
    ASSERT_EQ(not_there, std::nullopt);

}
