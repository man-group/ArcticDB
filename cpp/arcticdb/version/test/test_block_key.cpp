/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/version/block_key.hpp>

using namespace arcticdb;
using namespace arcticdb::version_store;

TEST(BlockKey, BasicRoundtrip) {
    auto store = test_store("BlockKey.BasicRoundtrip")->_test_get_store();
    StreamId block_id{"block_key"};

    BlockKey block_key = BlockKey{KeyType::BLOCK_VERSION_REF, block_id};

    std::vector<StreamId> streams{"symbol_1", "symbol_2"};
    for (size_t i = 0; i < streams.size(); i++) {
        AtomKey k = AtomKeyBuilder()
            .start_index(10 + i)
            .end_index(100 + i)
            .creation_ts(123 + i)
            .version_id(1 + i)
            .content_hash(2 + i)
            .build<KeyType::VERSION>(streams[i]);

        block_key.upsert(std::move(k));
    }

    write_block_key(store.get(), std::move(block_key));
    block_key = read_block_key(store.get(), KeyType::BLOCK_VERSION_REF, block_id);

    ASSERT_EQ(KeyType::BLOCK_VERSION_REF, block_key.key_type());
    ASSERT_EQ(block_id, block_key.id());
    for (int64_t i = 0; i < static_cast<int64_t>(streams.size()); i++) {
        std::optional<AtomKey> version_key = block_key.read(streams[i]);
        ASSERT_TRUE(version_key.has_value());
        ASSERT_EQ(version_key->start_index(), IndexValue{10 + i});
        ASSERT_EQ(version_key->end_index(), IndexValue{100 + i});
        ASSERT_EQ(version_key->creation_ts(), 123 + i);
        ASSERT_EQ(version_key->version_id(), 1 + i);
        ASSERT_EQ(version_key->content_hash(), 2 + i);
        ASSERT_EQ(version_key->type(), KeyType::VERSION);
    }
}

TEST(BlockKey, RemoveKey) {
    auto store = test_store("BlockKey.BasicRoundtrip")->_test_get_store();
    StreamId block_id{"block_key"};

    BlockKey block_key = BlockKey{KeyType::BLOCK_VERSION_REF, block_id};

    std::vector<StreamId> streams{"symbol_1", "symbol_2"};
    for (size_t i = 0; i < streams.size(); i++) {
        AtomKey k = AtomKeyBuilder()
            .start_index(10 + i)
            .end_index(100 + i)
            .creation_ts(123 + i)
            .version_id(1 + i)
            .content_hash(2 + i)
            .build<KeyType::VERSION>(streams[i]);

        block_key.upsert(std::move(k));
    }

    block_key.remove(streams[0]);

    write_block_key(store.get(), std::move(block_key));
    block_key = read_block_key(store.get(), KeyType::BLOCK_VERSION_REF, block_id);

    ASSERT_FALSE(block_key.read(streams[0]).has_value());
    ASSERT_TRUE(block_key.read(streams[1]).has_value());
}

TEST(BlockKey, ErrorIfReleasedTwice) {
    auto store = test_store("BlockKey.BasicRoundtrip")->_test_get_store();
    StreamId block_id{"block_key"};
    BlockKey block_key = BlockKey{KeyType::BLOCK_VERSION_REF, block_id};

    std::vector<StreamId> streams{"symbol_1", "symbol_2"};
    for (size_t i = 0; i < streams.size(); i++) {
        AtomKey k = AtomKeyBuilder()
            .start_index(10 + i)
            .end_index(100 + i)
            .creation_ts(123 + i)
            .version_id(1 + i)
            .content_hash(2 + i)
            .build<KeyType::VERSION>(streams[i]);

        block_key.upsert(std::move(k));
    }

    write_block_key(store.get(), std::move(block_key));
    ASSERT_THROW(write_block_key(store.get(), std::move(block_key)), InternalException);
}

TEST(BlockKey, CopySegmentToNewBlock) {
    auto store = test_store("BlockKey.BasicRoundtrip")->_test_get_store();
    StreamId block_id{"block_key"};

    BlockKey block_key = BlockKey{KeyType::BLOCK_VERSION_REF, block_id};

    std::vector<StreamId> streams{"symbol_1", "symbol_2"};
    for (size_t i = 0; i < streams.size(); i++) {
        AtomKey k = AtomKeyBuilder()
            .start_index(10 + i)
            .end_index(100 + i)
            .creation_ts(123 + i)
            .version_id(1 + i)
            .content_hash(2 + i)
            .build<KeyType::VERSION>(streams[i]);

        block_key.upsert(std::move(k));
    }

    StreamId new_block_id{"new_block_key"};
    BlockKey block_key_copy = block_key.block_with_same_keys(new_block_id);
    block_key_copy.remove(streams[0]);

    ASSERT_FALSE(block_key_copy.read(streams[0]).has_value());
    ASSERT_TRUE(block_key_copy.read(streams[1]).has_value());
    ASSERT_TRUE(block_key.read(streams[0]).has_value());
    ASSERT_TRUE(block_key.read(streams[1]).has_value());
}
