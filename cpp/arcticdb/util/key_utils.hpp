/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <arcticdb/column_store/key_segment.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/stream/stream_utils.hpp>

namespace arcticdb {

template<class Predicate>
inline void delete_keys_of_type_if(const std::shared_ptr<Store>& store, Predicate&& predicate, KeyType key_type, const std::string& prefix = std::string(), bool continue_on_error = false) {
    static const size_t delete_object_limit = ConfigsMap::instance()->get_int("Storage.DeleteBatchSize", 1000);
    std::vector<VariantKey> keys{};
    try {
        store->iterate_type(key_type, [predicate=std::forward<Predicate>(predicate), store=store, &keys](VariantKey &&key) {
            if(predicate(key))
                keys.emplace_back(std::move(key));

            if(keys.size() == delete_object_limit) {
                store->remove_keys(keys).get();
                keys.clear();
            }
        }, prefix);

        if(!keys.empty())
            store->remove_keys(keys).get();
    }
    catch(const std::exception& ex) {
        if(continue_on_error)
            log::storage().warn("Caught exception {} trying to delete key, continuing", ex.what());
        else
            throw;
    }
}

template<class Predicate>
inline void delete_keys_of_type_if_sync(const std::shared_ptr<Store>& store, Predicate&& predicate, KeyType key_type, const std::string& prefix = std::string(), bool continue_on_error = false) {
    try {
        store->iterate_type(key_type, [predicate=std::forward<Predicate>(predicate), store=store](VariantKey &&key) {
            if(predicate(key))
                store->remove_key_sync(key);
        }, prefix);
    }
    catch(const std::exception& ex) {
        if(continue_on_error)
            log::storage().warn("Caught exception {} trying to delete key, continuing", ex.what());
        else
            throw;
    }
}

inline void delete_keys_of_type_for_stream(const std::shared_ptr<Store>& store, const StreamId& stream_id, KeyType key_type, bool continue_on_error = false) {
    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
    auto match_stream_id =  [&stream_id](const VariantKey & k){ return variant_key_id(k) == stream_id; };
    delete_keys_of_type_if(store, std::move(match_stream_id), key_type, prefix, continue_on_error);
}

inline void delete_keys_of_type_for_stream_sync(const std::shared_ptr<Store>& store, const StreamId& stream_id, KeyType key_type, bool continue_on_error = false) {
    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
    auto match_stream_id =  [&stream_id](const VariantKey & k){ return variant_key_id(k) == stream_id; };
    delete_keys_of_type_if_sync(store, std::move(match_stream_id), key_type, prefix, continue_on_error);
}

inline void delete_all_keys_of_type(KeyType key_type, const std::shared_ptr<Store>& store, bool continue_on_error) {
    auto match_stream_id = [](const VariantKey &){ return true; };
    delete_keys_of_type_if(store, std::move(match_stream_id), key_type, std::string{}, continue_on_error);
}

inline void delete_all_for_stream(const std::shared_ptr<Store>& store, const StreamId& stream_id, bool continue_on_error = false) {
    foreach_key_type([&store, &stream_id, continue_on_error] (KeyType key_type) { delete_keys_of_type_for_stream(store, stream_id, key_type, continue_on_error); });
}

inline void delete_all(const std::shared_ptr<Store>& store, bool continue_on_error) {
    foreach_key_type([&store, continue_on_error] (KeyType key_type) {
        ARCTICDB_DEBUG(log::version(), "Deleting keys of type {}", key_type);
        delete_all_keys_of_type(key_type, store, continue_on_error);
    });
}

template<typename KeyContainer, typename = std::enable_if<std::is_base_of_v<AtomKey, typename KeyContainer::value_type>>>
inline std::vector<AtomKey> get_data_keys(
    const std::shared_ptr<stream::StreamSource>& store,
    const KeyContainer& keys,
    storage::ReadKeyOpts opts) {
    using KeySupplier = folly::Function<KeyContainer()>;
    using StreamReader = arcticdb::stream::StreamReader<AtomKey, KeySupplier, SegmentInMemory::Row>;
    auto gen = [&keys]() { return keys; };
    StreamReader stream_reader(std::move(gen), store, opts);
    return stream_reader.generate_data_keys() | folly::gen::as<std::vector>();
}

inline std::vector<AtomKey> get_data_keys(
    const std::shared_ptr<stream::StreamSource>& store,
    const AtomKey& key,
    storage::ReadKeyOpts opts) {
    const std::vector<AtomKey> keys{key};
    return get_data_keys(store, keys, opts);
}

ankerl::unordered_dense::set<AtomKey> recurse_segment(const std::shared_ptr<stream::StreamSource>& store,
                                            SegmentInMemory segment,
                                            const std::optional<VersionId>& version_id);

/* Given a [multi-]index key, returns a set containing the top level [multi-]index key itself, and all the
 * multi-index, index, and data keys referenced by this [multi-]index key.
 * If the version_id argument is provided, the returned set will only contain keys matching that version_id.
 * Note that this differs from recurse_index_keys, which does not include the passed in keys in the returned set. */
inline ankerl::unordered_dense::set<AtomKey> recurse_index_key(
    const std::shared_ptr<stream::StreamSource>& store,
    const IndexTypeKey& index_key,
    const std::optional<VersionId>& version_id=std::nullopt) {
    auto segment = store->read_sync(index_key).second;
    auto res = recurse_segment(store, segment, version_id);
    res.emplace(index_key);
    return res;
}

inline ankerl::unordered_dense::set<AtomKey> recurse_segment(
        const std::shared_ptr<stream::StreamSource>& store,
        SegmentInMemory segment,
        const std::optional<VersionId>& version_id) {
    ankerl::unordered_dense::set<AtomKey> res;
    for (size_t idx = 0; idx < segment.row_count(); idx++) {
        auto key = stream::read_key_row(segment, idx);
        if (!version_id || key.version_id() == *version_id) {
            switch (key.type()) {
                case KeyType::TABLE_DATA:
                    res.emplace(std::move(key));
                    break;
                case KeyType::TABLE_INDEX:
                case KeyType::MULTI_KEY: {
                    auto sub_keys = recurse_index_key(store, key, version_id);
                    for (auto&& sub_key: sub_keys) {
                        res.emplace(std::move(sub_key));
                    }
                    break;
                }
                default:
                    break;
            }
        }
    }
    return res;
}

/* Given a container of [multi-]index keys, returns a set containing all the multi-index, index, and data keys
 * referenced by these [multi-]index keys.
 * Note that this differs from recurse_index_key, which includes the passed in key in the returned set. */
template<typename KeyContainer>
requires std::is_base_of_v<AtomKey, typename KeyContainer::value_type>
inline ankerl::unordered_dense::set<AtomKey> recurse_index_keys(
        const std::shared_ptr<stream::StreamSource>& store,
        const KeyContainer& keys,
        storage::ReadKeyOpts opts) {
    if (keys.empty()) {
        return {};
    }
    // Having one set for AtomKeys and one for AtomKeyPacked is intentional. This handles the case of pruning data for symbol.
    // In that case all keys will be for the same symbol and we can use the less expensive to hash AtomKeyPacked struct as
    // rehashing when the set grows is expensive for AtomKeys. In case the keys are for different symbols (e.g. when
    // deleting a snapshot) AtomKey must be used as we need the symbol_id per key.
    ankerl::unordered_dense::set<AtomKey> res;
    ankerl::unordered_dense::set<AtomKeyPacked> res_packed;
    const StreamId& first_stream_id = keys.begin()->id();
    bool same_stream_id = true;
    for (const auto& index_key: keys) {
        same_stream_id = first_stream_id == index_key.id();
        try {
            if (index_key.type() == KeyType::MULTI_KEY) {
                // recurse_index_key includes the input key in the returned set, remove this here
                auto sub_keys = recurse_index_key(store, index_key);
                sub_keys.erase(index_key);
                for (auto &&key : sub_keys) {
                    res.emplace(std::move(key));
                }
            } else if (index_key.type() == KeyType::TABLE_INDEX) {
                KeySegment key_segment(store->read_sync(index_key, opts).second, SymbolStructure::SAME);
                auto data_keys = key_segment.materialise();
                util::variant_match(
                    data_keys,
                    [&]<typename KeyType>(std::vector<KeyType>&atom_keys) {
                        for (KeyType& key : atom_keys) {
                            if constexpr (std::is_same_v<KeyType, AtomKey>) {
                                res.emplace(std::move(key));
                            } else if constexpr (std::is_same_v<KeyType, AtomKeyPacked>) {
                                if (same_stream_id) {
                                    res_packed.emplace(std::move(key));
                                } else {
                                    res.emplace(key.to_atom_key(index_key.id()));
                                }
                            }
                        }
                    }
                );
            } else {
                internal::raise<ErrorCode::E_ASSERTION_FAILURE>(
                    "recurse_index_keys: expected index or multi-index key, received {}",
                    index_key.type()
                );
            }
        } catch (storage::KeyNotFoundException& e) {
            if (opts.ignores_missing_key_) {
                log::version().info("Missing key while recursing index key {}", e.keys());
            } else {
                throw;
            }
        }
    }
    if (!res_packed.empty()) {
        res.reserve(res_packed.size() + res.size());
        for (const auto& key : res_packed) {
            res.emplace(key.to_atom_key(first_stream_id));
        }
    }
    return res;
}

inline VersionId get_next_version_from_key(const AtomKey& prev) {
    auto version = prev.version_id();
    return ++version;
}

inline VersionId get_next_version_from_key(const std::optional<AtomKey>& maybe_prev) {
    VersionId version = 0;
    if (maybe_prev) {
       version = get_next_version_from_key(*maybe_prev);
    }

    return version;
}

inline AtomKey in_memory_key(KeyType key_type, const StreamId& stream_id, VersionId version_id) {
    return atom_key_builder().version_id(version_id).build(stream_id, key_type);
}

template<class Predicate, class Function>
inline void iterate_keys_of_type_if(const std::shared_ptr<Store>& store, Predicate&& predicate, KeyType key_type, const std::string& prefix, Function&& function) {
    std::vector<folly::Future<entity::VariantKey>> fut_vec;
    store->iterate_type(key_type, [predicate=std::forward<Predicate>(predicate), function=std::forward<Function>(function)](const VariantKey &&key) {
        if(predicate(key)) {
           function(key);
        }
    }, prefix);
}

template <class Function>
inline void iterate_keys_of_type_for_stream(
    std::shared_ptr<Store> store, KeyType key_type, const StreamId& stream_id, Function&& function
    ) {
    auto prefix = std::holds_alternative<StringId>(stream_id) ? std::get<StringId>(stream_id) : std::string();
    auto match_stream_id =  [&stream_id](const VariantKey & k){ return variant_key_id(k) == stream_id; };
    iterate_keys_of_type_if(store, match_stream_id, key_type, prefix, std::forward<Function>(function));
}

} //namespace arcticdb