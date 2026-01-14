/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#include <arcticdb/version/key_block.hpp>
#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/index_aggregator.hpp>
#include <arcticdb/stream/stream_utils.hpp>
#include <utility>

namespace arcticdb::version_store {

KeyBlock::KeyBlock(KeyType key_type, StreamId id, SegmentInMemory&& segment) {
    util::check(is_block_ref_key_class(key_type), "Expected block ref key but type was {}", key_type);
    expected_key_type_ = expected_key_type_of_contents(key_type);
    key_block_type_ = key_type;
    id_ = std::move(id);
    keys_ = map_from_segment(std::move(segment));
}

KeyBlock::KeyBlock(KeyType key_type, StreamId id) : KeyBlock(key_type, std::move(id), SegmentInMemory()) {}

KeyBlock::KeyBlock(KeyType key_type, StreamId id, std::unordered_map<StreamId, AtomKey> keys) :
    keys_(std::move(keys)),
    key_block_type_(key_type),
    id_(std::move(id)) {
    expected_key_type_ = expected_key_type_of_contents(key_type);
}

KeyBlock KeyBlock::block_with_same_id(StreamId new_id) { return {key_type(), std::move(new_id), keys_}; }

void KeyBlock::upsert(AtomKey&& key) {
    util::check(valid_, "Attempt to use KeyBlock after release_segment_in_memory");
    util::check(
            key.type() == expected_key_type_, "Unexpected key_type, was {} expected {}", key.type(), expected_key_type_
    );
    keys_[key.id()] = std::move(key);
}

bool KeyBlock::remove(const StreamId& id) {
    util::check(valid_, "Attempt to use KeyBlock after release_segment_in_memory");
    return keys_.erase(id) == 1;
}

std::optional<AtomKey> KeyBlock::read(const StreamId& id) const {
    util::check(valid_, "Attempt to use KeyBlock after release_segment_in_memory");
    auto it = keys_.find(id);
    if (it == keys_.end()) {
        return std::nullopt;
    } else {
        return it->second;
    }
}

SegmentInMemory KeyBlock::release_segment_in_memory() {
    util::check(valid_, "Attempt to release_segment_in_memory on a KeyBlock twice");
    valid_ = false;
    SegmentInMemory result;
    stream::IndexAggregator<stream::RowCountIndex> agg(id_, [&result](SegmentInMemory&& segment) {
        result = std::move(segment);
    });

    for (auto&& [_, k] : keys_) {
        agg.add_key(k);
    }

    agg.finalize();
    keys_.clear();
    return result;
}

KeyType KeyBlock::key_type() const { return key_block_type_; }

StreamId KeyBlock::id() const { return id_; }

std::unordered_map<StreamId, AtomKey> KeyBlock::map_from_segment(SegmentInMemory&& segment) {
    std::unordered_map<StreamId, AtomKey> result;
    for (size_t idx = 0; idx < segment.row_count(); idx++) {
        auto id = stream::stream_id_from_segment<pipelines::index::Fields>(segment, idx);
        auto row_key =
                stream::read_key_row_into_builder<pipelines::index::Fields>(segment, idx).build(id, expected_key_type_);
        result.insert({id, row_key});
    }
    return result;
}

KeyType KeyBlock::expected_key_type_of_contents(const KeyType& key_type) {
    switch (key_type) {
    case KeyType::BLOCK_VERSION_REF:
        return KeyType::VERSION;
    default:
        util::raise_rte("Unsupported key type {}", key_type);
    }
}

void write_key_block(Store* store, KeyBlock&& key) {
    store->write_sync(key.key_type(), key.id(), key.release_segment_in_memory());
}

KeyBlock read_key_block(Store* store, const KeyType key_type, const StreamId& id) {
    util::check(is_block_ref_key_class(key_type), "Expected block ref key but type was {}", key_type);
    auto opts = storage::ReadKeyOpts{};
    opts.dont_warn_about_missing_key = true;
    try {
        SegmentInMemory segment = store->read_sync(RefKey{id, key_type}, opts).second;
        return KeyBlock{key_type, id, std::move(segment)};
    } catch (storage::KeyNotFoundException&) {
        return KeyBlock{key_type, id};
    }
}

} // namespace arcticdb::version_store