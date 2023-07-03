#pragma once

#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/storage/coalesced/coalesced_storage_common.hpp>
#include <arcticdb/version/version_tasks.hpp>
#include <arcticdb/storage/coalesced/multi_segment_aggregator.hpp>

namespace arcticdb {

static constexpr const char* LastCoalesceTime = "__last_coalesce__";

inline StreamDescriptor last_coalesce_stream_descriptor(const StreamId &stream_id) {
    return stream_descriptor(
        stream_id,
        stream::RowCountIndex(),
        {scalar_field(DataType::MICROS_UTC64, "time")});
}

static RefKey ref_key() {
    return RefKey{LastCoalesceTime, KeyType::COALESCE};
}

SegmentInMemory last_coalesce_segment(uint64_t timestamp) {
    SegmentInMemory output{last_coalesce_stream_descriptor(LastCoalesceTime)};
    output.set_scalar(0, timestamp);
    output.end_row();
    return output;
}

std::vector<StreamId> get_uncoalesced_symbols(
    const std::shared_ptr<Store>& store
    ) {

    std::vector<StreamId> output;
    store->iterate_type(KeyType::VERSION_REF, [&output](auto &&vk) {
        output.emplace_back(variant_key_id(std::forward<VariantKey&&>(vk)));
    });
    return output;
}

std::vector<std::shared_ptr<VersionMapEntry>> get_uncoalesced_versions(
    const std::vector<StreamId>& uncoalesced_ids,
    std::optional<timestamp> last_coalesce_time,
    const std::shared_ptr<Store>& store,
    const std::shared_ptr<VersionMap>& version_map
    ) {
    auto load_param = last_coalesce_time ? LoadParameter{LoadType::LOAD_FROM_TIME, last_coalesce_time.value()} : LoadParameter{LoadType::LOAD_ALL};
    auto coalesce_window_size = ConfigsMap::instance()->get_int("Coalesce.SymbolWindowSize", 100);
    auto fut_entries = folly::window(uncoalesced_ids, [store, version_map, load_param] (const auto& id) {
        return async::submit_io_task(CheckReloadTask{store, version_map, id, load_param, false});
    }, coalesce_window_size);

    return folly::collect(fut_entries).get();
}

void produce_latest_versions_segment() {
    // Called at the end, updates existing latest versions segment
    // with any latest ones added
}

template <typename Storage>
std::pair<VariantKey, SegmentInMemory> storage_read(const VariantKey& key, Storage& storage) {
    SegmentInMemory output;
    storage.do_read(key, [&output] (auto&& segment) {
       output = decode_segment(std::forward<Segment>(segment));
    });
    return {key, std::move(output)};
}

template <typename Storage>
std::optional<timestamp> get_last_coalesce_time(
    const Storage& storage
) {
    try {
        auto [key, seg] = storage_read(ref_key(), storage);
        return seg.template scalar_at<timestamp>(0, 0).value();
    } catch (const std::invalid_argument &) {
        return std::nullopt;
    } catch (const storage::KeyNotFoundException &) {
        return std::nullopt;
    }
}

std::map<StreamId, std::shared_ptr<VersionMapEntry>> get_versions_by_id(
    const std::vector<std::shared_ptr<VersionMapEntry>>& versions) {
    std::map<StreamId, std::shared_ptr<VersionMapEntry>> output;
    for(const auto& entry : versions) {
        util::check(!entry->empty(), "Empty version entry in version_by_id");
        util::check(static_cast<bool>(entry->head_), "Empty head in version entry in version_by_id");
        output.try_emplace(entry->head_->id(), entry);
    }

    return output;
}


std::map<TimeSymbol::IndexDataType, std::shared_ptr<VersionMapEntry>> get_versions_by_time(
    const std::map<StreamId, std::shared_ptr<VersionMapEntry>>& version_by_id) {
    std::map<TimeSymbol::IndexDataType, std::shared_ptr<VersionMapEntry>> output;
    for(const auto& [id, entry] : version_by_id) {
        output.try_emplace(time_symbol_from_key(*entry->head_).data(), entry);
    }
    return output;
}

std::vector<AtomKey> get_uncoalesced_data_keys(const std::shared_ptr<VersionMapEntry>& entry) {
    std::vector<AtomKey> output;
    std::unordered_set<AtomKey> unique_keys;

    std::move(std::begin(unique_keys), std::end(unique_keys), std::back_inserter(output));
    return output;
}

template <typename Storage>
auto get_write_function(
    Storage& storage,
    const std::shared_ptr<arcticdb::proto::encoding::VariantCodec>& codec_meta,
    EncodingVersion encoding_version) {
    return [&storage, codec_meta, encoding_version] (VariantKey&& key, SegmentInMemory&& segment) {
        auto seg = encode_dispatch(std::move(segment), *codec_meta, encoding_version);
        storage.do_write(key, std::move(seg));  //TODO write_raw?
    };
}

template <typename Storage>
auto get_read_function(
    Storage& storage) {
    return [&storage] (VariantKey&& key) {
        SegmentInMemory segment;
        storage.do_read(key, [&segment] (auto&& seg) {
            segment = decode_segment(std::forward<Segment>(seg));
        });
        return segment; //TODO read_raw?
    };
}

template <typename Storage>
void coalesce_keys_of_type(
    KeyType key_type,
    std::vector<AtomKey>& keys,
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
    EncodingVersion encoding_version,
    size_t multi_segment_bytes,
    Storage& storage) {
    storage::coalesced::MultiSegmentIndexer indexer{
        key_type,
        storage::coalesced::BytesSizingPolicy{static_cast<uint64_t>(multi_segment_bytes)},
        get_write_function(storage, codec_meta, encoding_version),
        get_read_function(storage),
        codec_meta,
        encoding_version
    };

    for(const auto& key : keys)
        indexer.add(key);

    indexer.commit();

    for(const auto& key : keys)
        storage.do_remove(key);
}

std::string rendezvous_hash(const std::string &key, const std::unordered_map<std::string, uint64_t> &nodes) {
    std::string max_node;
    uint64_t max_hash = 0;
    for (const auto &node : nodes) {
        uint64_t node_hash = hash(node.first + key);
        if (node_hash > max_hash) {
            max_node = node.first;
            max_hash = node_hash;
        }
    }
    return max_node;
}


template <typename Storage>
void coalesce_keys_of_type_hashed(
    KeyType key_type,
    std::vector<AtomKey>& keys,
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
    EncodingVersion encoding_version,
    size_t multi_segment_bytes,
    Storage& storage) {
    using namespace storage::coalesced;

    std::vector<std::unique_ptr<MultiSegmentIndexer>> indexers;
    storage::coalesced::MultiSegmentIndexer indexer{
        key_type,
        storage::coalesced::BytesSizingPolicy{static_cast<uint64_t>(multi_segment_bytes)},
        get_write_function(storage, codec_meta, encoding_version),
        get_read_function(storage),
        codec_meta,
        encoding_version
    };

    for(const auto& key : keys)
        indexer.add(key);

    indexer.commit();

    for(const auto& key : keys)
        storage.do_remove(key);
}

template <typename Storage>
void storage_coalesce(
    Storage& storage,
    std::unordered_map<StreamId, IndexVersionPair> existing_versions,
    std::shared_ptr<arcticdb::proto::encoding::VariantCodec> codec_meta,
    EncodingVersion encoding_version
    ) {
    auto version_map = std::make_shared<VersionMap>();
    auto last_coalesce_time = get_last_coalesce_time(storage);
    auto uncoalesced_symbols = get_uncoalesced_symbols(storage);
    auto uncoalesced_versions = get_uncoalesced_versions(uncoalesced_symbols, last_coalesce_time, storage, version_map);
    auto versions_by_id = get_versions_by_id(uncoalesced_versions);
    auto versions_by_time = get_versions_by_time(versions_by_id);

    /*
     * For each new entry in time/symbol order, see if it's in the list of existing most recent
     * symbols. If it is, the last  version map entry should be a version key equal to the target
     * version key in the existing_versions list (i.e. it is not a new symbol since the last
     * coalescence). If it's a new then the last key should be an index key (i.e. there is no prior version
     * segment), and the new version can be added without a predecessor.
     *
     * Declare three multi-segment aggregators, one for version-type information, one for indexes
     * and one for data.
     *
     * Form a compacted version segment, add it to the version multi-segment aggregator. Read the indexes
     * in it, scan them for data chunks that have not been coalesced (because their version is equal to
     * or greater than the last coalesced version and add them these to the data multisegment aggregator).
     * Write the indexes to the index aggregator. Delete the keys from the primary storage.
    */

    auto multi_segment_bytes = ConfigsMap::instance()->get_int("Coalesce.ChunkSizeBytes", 1000000000);

    std::vector<AtomKey> index_keys;
    std::vector<AtomKey> data_keys;

    for(const auto& [time_sym, entry] : versions_by_time) {
        const auto& last_key = *entry->keys_.rbegin();

        std::deque<AtomKey> compacted_keys;
        const auto& old_keys = entry->keys_;
        std::copy_if(std::begin(old_keys), std::end(old_keys), std::back_inserter(compacted_keys),
                     [](const auto& k){return is_index_or_tombstone(k);});

        storage::coalesced::MultiSegmentIndexer version_indexer{
            KeyType::VERSION,
            storage::coalesced::BytesSizingPolicy{static_cast<uint64_t>(multi_segment_bytes)},
            get_write_function(storage, codec_meta, encoding_version),
            get_read_function(storage),
            codec_meta,
            encoding_version
        };

        const auto& stream_id = entry->head_->id();
        if(auto it = existing_versions.find(stream_id); it != std::end(existing_versions)) {
            util::check(it->second.version_ == last_key, "Key mismatch in coalesced version: {} != {}",
                        it->second.version_, last_key);

            std::copy(std::begin(compacted_keys), std::end(compacted_keys), std::back_inserter(index_keys));
            for(const auto& index_key : compacted_keys) {
                data_keys.insert(get_uncoalesced_data_keys(entry));
            }

            compacted_keys.push_back(last_key);
            auto version_key = entry->head_.value();
            SegmentInMemory new_entry_segment;
            aggregate_entry(stream_id, entry, [&new_entry_segment] (auto&& s) {
                new_entry_segment = std::forward<SegmentInMemory&&>(s);
            });

            version_indexer.add(
                entry->head_.value(),
                std::move(new_entry_segment),
                std::make_optional<AtomKey>(entry->head_.value()));
        }
    }

    coalesce_keys_of_type(KeyType::TABLE_INDEX,
        index_keys,
        codec_meta,
        encoding_version,
        multi_segment_bytes,
        storage);


    coalesce_keys_of_type_hashed(KeyType::TABLE_DATA,
                          data_keys,
                          codec_meta,
                          encoding_version,
                          multi_segment_bytes,
                          storage);
}

}
