/* Copyright 2024 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/storage/store.hpp>
#include <arcticdb/entity/key.hpp>

namespace arcticdb::version_store {

/**
 * A key whose segment stores many atom keys (all of the same type).
 */
class KeyBlock {
  public:
    /**
     * Loaded from an existing key block.
     */
    KeyBlock(KeyType key_type, StreamId id, SegmentInMemory&& segment);

    /**
     * A new key block.
     */
    KeyBlock(KeyType key_type, StreamId id);

    KeyBlock(KeyType key_type, StreamId id, std::unordered_map<StreamId, AtomKey> keys);

    KeyBlock block_with_same_id(StreamId new_id);

    void upsert(AtomKey&& key);

    /**
     * Returns true iff the id was removed. False indicates that the id was not present.
     */
    bool remove(const StreamId& id);

    /**
     * nullopt indicates that the id was not present in this block
     */
    std::optional<AtomKey> read(const StreamId& id) const;

    SegmentInMemory release_segment_in_memory();

    KeyType key_type() const;

    StreamId id() const;

  private:
    static KeyType expected_key_type_of_contents(const KeyType& key_type);
    std::unordered_map<StreamId, AtomKey> map_from_segment(SegmentInMemory&& segment);

    std::unordered_map<StreamId, AtomKey> keys_;
    bool valid_{true};
    KeyType key_block_type_;
    StreamId id_;
    KeyType expected_key_type_;
};

/**
 * Write the key to storage. Invalidates the in-memory key.
 */
void write_key_block(Store* store, KeyBlock&& key);

/**
 * Read the key block from storage. If the key does not exist in the storage, returns an empty KeyBlock.
 */
KeyBlock read_key_block(Store* store, KeyType key_type, const StreamId& id);

} // namespace arcticdb::version_store
