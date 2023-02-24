/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/version/version_utils.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/entity/variant_key.hpp>
#include <arcticdb/storage/store.hpp>

namespace arcticdb {

using namespace arcticdb::storage;
using namespace arcticdb::entity;
using namespace arcticdb::stream;


std::unordered_map<entity::StreamId, size_t> get_num_version_entries(const std::shared_ptr<Store>& store, size_t batch_size)  {
    std::unordered_map<entity::StreamId, size_t> output;
    size_t max_blocks = ConfigsMap::instance()->get_int("VersionMap.MaxVersionBlocks", 5);
    store->iterate_type(entity::KeyType::VERSION, [&output, batch_size, max_blocks] (const VariantKey& key) {
        ++output[variant_key_id(key)];
        if (output.size() >= batch_size) {
            // remove half of them which are under max_blocks
            // otherwise memory would blow up for big libraries
            auto iter = output.begin();
            while(iter != output.end()) {
                auto copy = iter;
                iter++;
                if (copy->second < max_blocks) {
                    output.erase(copy);
                }
                if (output.size() <= batch_size / 2) {
                    break;
                }
            }
        }
    });
    return output;
}

void fix_stream_ids_of_index_keys(
    const std::shared_ptr<Store> &store,
    const StreamId &stream_id,
    const std::shared_ptr<VersionMapEntry> &entry) {
    for (auto &key : entry->keys_) {
        if (is_index_key_type(key.type()) && key.id() != stream_id) {
            log::version().warn("Mismatch in stream_id - {} != {} in version: {}. Rewriting", key.id(), stream_id,
                                key.version_id());
            try {
                auto data_kvs = store->batch_read_compressed({key}, BatchReadArgs{}, false);
                util::check(data_kvs.size() == 1, "Unexpected size of data_kvs {} in fix_stream_ids_of_index_keys",
                            data_kvs.size());
                data_kvs[0].atom_key() = atom_key_builder()
                    .version_id(key.version_id())
                    .creation_ts(key.creation_ts())
                    .start_index(key.start_index())
                    .end_index(key.end_index())
                    .content_hash(key.content_hash())
                    .build(stream_id, key.type());
                AtomKey new_key = data_kvs[0].atom_key();
                store->batch_write_compressed(std::move(data_kvs)).get();
                key = new_key;
                // We don't delete the old index key - it could be referred in a snapshot (TODO: fix that as well)
            } catch (const std::exception &ex) {
                log::version().info("Could not fix stream_id for {} due to {}", key.version_id(), ex.what());
            }
        }
    }
}
}