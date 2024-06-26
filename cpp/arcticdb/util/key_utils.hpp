/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <memory>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/util/variant.hpp>
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

/* Given a [multi-]index key, returns a set containing the top level [multi-]index key itself, and all of the
 * multi-index, index, and data keys referenced by this [multi-]index key.
 * If the version_id argument is provided, the returned set will only contain keys matching that version_id. */
inline ankerl::unordered_dense::set<AtomKey> recurse_index_key(const std::shared_ptr<stream::StreamSource>& store,
                                                     const IndexTypeKey& index_key,
                                                     const std::optional<VersionId>& version_id=std::nullopt) {
    auto segment = store->read_sync(index_key).second;
    auto res = recurse_segment(store, segment, version_id);
    res.emplace(index_key);
    return res;
}

inline ankerl::unordered_dense::set<AtomKey> recurse_segment(const std::shared_ptr<stream::StreamSource>& store,
                                                   SegmentInMemory segment,
                                                   const std::optional<VersionId>& version_id) {
    ankerl::unordered_dense::set<AtomKey> res;
    ankerl::unordered_dense::set<AtomKey> tmp;
    for (size_t idx = 0; idx < segment.row_count(); idx++) {
        auto key = stream::read_key_row(segment, idx);
        if ((version_id && key.version_id() == *version_id) || !version_id) {
            switch (key.type()) {
                case KeyType::TABLE_DATA:
                    res.emplace(std::move(key));
                    break;
                case KeyType::TABLE_INDEX:
                case KeyType::MULTI_KEY:
                    tmp = recurse_index_key(store, key, version_id);
                    for (auto&& tmp_key: tmp) {
                        res.emplace(std::move(tmp_key));
                    }
//                    res.merge(recurse_index_key(store, key, version_id));
                    break;
                default:
                    break;
            }
        }
    }
    return res;
}

struct AtomKeyNoId {
    VersionId version_id_ = 0;
    timestamp creation_ts_ = 0;
    ContentHash content_hash_ = 0;
    KeyType key_type_ = KeyType::UNDEFINED;
    // TODO: String indexes
    timestamp index_start_;
    timestamp index_end_;

    using is_avalanching = void;

    friend bool operator==(const AtomKeyNoId &l, const AtomKeyNoId &r) {
        return l.version_id_ == r.version_id_
               && l.creation_ts_ == r.creation_ts_
               && l.content_hash_ == r.content_hash_
               && l.key_type_ == r.key_type_
               && l.index_start_ == r.index_start_
               && l.index_end_ == r.index_end_;
    }
};

// TODO: Better name, in multi-index keys the returned set can contain both table and multi index keys
template<typename KeyContainer, typename = std::enable_if<std::is_base_of_v<AtomKey, typename KeyContainer::value_type>>>
inline ankerl::unordered_dense::set<AtomKey> get_data_keys_set(
        const std::shared_ptr<stream::StreamSource>& store,
        const KeyContainer& keys,
        storage::ReadKeyOpts opts) {
    auto start = std::chrono::steady_clock::now();
    ankerl::unordered_dense::set<AtomKeyNoId> res;
    for (const auto& index_key: keys) {
        // TODO: Async and in parallel?
        // TODO: Handle multi-index
        auto segment = store->read_sync(index_key, opts).second;
        const auto& version_col = segment.column(*segment.column_index("version_id"));
        const auto& creation_ts_col = segment.column(*segment.column_index("creation_ts"));
        const auto& content_hash_col = segment.column(*segment.column_index("content_hash"));
        const auto& key_type_col = segment.column(*segment.column_index("key_type"));
        const auto& index_start_col = segment.column(*segment.column_index("start_index"));
        const auto& index_end_col = segment.column(*segment.column_index("start_index"));

        auto version_data = version_col.data();
        auto creation_ts_data = creation_ts_col.data();
        auto content_hash_data = content_hash_col.data();
        auto key_type_data = key_type_col.data();
        auto index_start_data = index_start_col.data();
        auto index_end_data = index_end_col.data();

        using version_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
        using creation_ts_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;
        using content_hash_TDT = ScalarTagType<DataTypeTag<DataType::UINT64>>;
        using key_type_TDT = ScalarTagType<DataTypeTag<DataType::UINT8>>;
        using index_start_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;
        using index_end_TDT = ScalarTagType<DataTypeTag<DataType::INT64>>;

        auto version_it = version_data.template cbegin<version_TDT>();
        auto creation_ts_it = creation_ts_data.template cbegin<creation_ts_TDT>();
        auto content_hash_it = content_hash_data.template cbegin<content_hash_TDT>();
        auto key_type_it = key_type_data.template cbegin<key_type_TDT>();
        auto index_start_it = index_start_data.template cbegin<index_start_TDT>();
        auto index_end_it = index_end_data.template cbegin<index_end_TDT>();

        for (
                size_t row_idx = 0;
                row_idx < segment.row_count();
                ++row_idx, ++version_it, ++creation_ts_it, ++content_hash_it, ++key_type_it, ++index_start_it, ++index_end_it) {
            AtomKeyNoId tmp{*version_it, *creation_ts_it, *content_hash_it, KeyType(*key_type_it), *index_start_it, *index_end_it};
            res.emplace(std::move(tmp));
        }
    }
    auto end = std::chrono::steady_clock::now();
    log::version().warn("Spent {}ms in get_data_keys_set AtomKeyNoId", std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    // TODO: Return vector for data_keys_to_be_deleted, set for data_keys_not_to_be_deleted
    ankerl::unordered_dense::set<AtomKey> res_2;
    auto id = keys.begin()->id();
    res_2.reserve(res.size());
    for (const auto& k: res) {
        AtomKey tmp{id, k.version_id_, k.creation_ts_, k.content_hash_, k.index_start_, k.index_end_, k.key_type_};
        res_2.emplace(std::move(tmp));
    }
    auto end_2 = std::chrono::steady_clock::now();
    log::version().warn("Spent {}ms in get_data_keys_set AtomKeyNoId->AtomKey", std::chrono::duration_cast<std::chrono::milliseconds>(end_2 - end).count());
    return res_2;

//    auto vec = get_data_keys(store, keys, opts);
//    return {vec.begin(), vec.end()};
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

namespace std {
    template<>
    struct hash<arcticdb::AtomKeyNoId> {
        inline arcticdb::HashedValue operator()(const arcticdb::AtomKeyNoId& key) const noexcept {
            return folly::hash::hash_combine(key.version_id_, key.creation_ts_, key.content_hash_, key.key_type_, key.index_start_,
                                             key.index_end_);
        }
    };
}