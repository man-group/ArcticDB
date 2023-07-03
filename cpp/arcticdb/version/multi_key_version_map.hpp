// First segment oontains all most recent keys. This is the only one that is sorted primarily on symbol
// Then index of remaining versions sorted by version then id - we already know the previous version from the most recent version collection above
// Drop previous versions history in compacted storage? (need to load snapshots)

// Indexes are stored by creation timestamp (maybe?)
// Or maybe density function. Perhaps same for data segments
// Want to keep older files immutable

// Partition vs internal sort?

// Initial latest versions segment - sort by symbol then write in that order, that way can scan for offset

// How to do invalidation? Check ref key

#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/storage/store.hpp>
#include "arcticdb/storage/coalesced/coalesced_storage_common.hpp"
#include "arcticdb/storage/coalesced/multi_segment_utils.hpp"

namespace arcticdb {

static constexpr const char* LatestVersionsSymbol = "__versions__";

enum class LatestVersionFields : uint32_t {
    index,
    stream_id,
    version_id,
    start_index,
    end_index,
    creation_ts,
    content_hash,
    index_type,
    id_type,
    key_type,
    end,
};

enum class VersionKeyFields : uint32_t {
    stream_id = int(LatestVersionFields::end),
    version_id,
    start_index,
    end_index,
    creation_ts,
    content_hash,
    index_type,
    id_type,
    key_type,
    end,
};

position_t as_pos(LatestVersionFields id_type) {
    return static_cast<position_t>(id_type);
}
StreamDescriptor sorted_keys_descriptor(StreamId stream_id) {
    return stream_descriptor(stream_id, stream::RowCountIndex(), {
        scalar_field(DataType::UINT64, "index"),
        scalar_field(DataType::UINT64, "stream_id"),
        scalar_field(DataType::UINT64, "version_id"),
        scalar_field(DataType::UINT64, "start_index"),
        scalar_field(DataType::UINT64, "end_index"),
        scalar_field(DataType::UINT64, "creation_ts"),
        scalar_field(DataType::UINT64, "content_hash"),
        scalar_field(DataType::UINT8, "index_type"),
        scalar_field(DataType::UINT8, "id_type"),
        scalar_field(DataType::UINT8, "key_type"),
        scalar_field(DataType::UINT64, "version_version_id"),
        scalar_field(DataType::UINT64, "version_start_index"),
        scalar_field(DataType::UINT64, "version_end_index"),
        scalar_field(DataType::UINT64, "version_creation_ts"),
        scalar_field(DataType::UINT64, "version_content_hash"),
        scalar_field(DataType::UINT8, "version_index_type"),
        scalar_field(DataType::UINT8, "version_id_type"),
        scalar_field(DataType::UINT8, "version_key_type"),
    });
}

SegmentInMemory write_latest_versions_segment(std::vector<AtomKey>&& ks, std::vector<AtomKey>&& pk) {
    auto keys = std::move(ks);
    auto previous_keys = std::move(pk);
    util::check(keys.size() == previous_keys.size(), "Size mismatch in latest_version_segment: {} != {}", keys.size(), previous_keys.size());
    std::map<uint64_t, std::pair<AtomKey, std::optional<AtomKey>>> sorted_keys;

    for(auto&& key : folly::enumerate(keys)) {
        auto prefix = get_symbol_prefix<uint64_t>(key->id());
        sorted_keys[prefix] = std::make_pair(std::move(*key), previous_keys[key.index]);
    }

    SegmentInMemory segment(sorted_keys_descriptor(LatestVersionsSymbol), sorted_keys.size(), false, true);
    for(const auto& [prefix, index_and_version] : sorted_keys) {
        segment.set_scalar(as_pos(LatestVersionFields::index), prefix);
        set_key<LatestVersionFields>(index_and_version.first, segment);
        if(index_and_version.second)
            set_key<VersionKeyFields>(index_and_version.second.value(), segment);

        segment.end_row();
    }

    return segment;
}

std::unordered_map<StreamId, IndexVersionPair> read_latest_versions_segment(SegmentInMemory&& s) {
    auto segment = std::move(s);
    std::unordered_map<StreamId, IndexVersionPair> output;

    const auto row_count = segment.row_count();
    for(auto i = 0u; i < row_count; ++i) {
        IndexVersionPair index_and_version{
            get_key<LatestVersionFields>(i, segment),
            get_key<VersionKeyFields>(i, segment)
        };

        output.try_emplace(index_and_version.index_.id(), std::move(index_and_version));
    }
    
    return output;
}

} //namespace arcticdb