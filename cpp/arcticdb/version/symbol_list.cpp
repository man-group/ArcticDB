/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/symbol_list.hpp>

namespace arcticdb {

using namespace arcticdb::stream;

static const StreamId compaction_id {CompactionId};

struct SymbolList::LoadResult {
    mutable KeyVector symbol_list_keys;
    /** The last CompactionId key in symbol_list_keys, if any. */
    std::optional<KeyVectorItr> maybe_last_compaction;
    mutable CollectionType symbols;
    arcticdb::timestamp timestamp;

    KeyVector&& detach_symbol_list_keys() const { return std::move(symbol_list_keys); }
    CollectionType&& detach_symbols() const { return std::move(symbols); }
};

SymbolList::LoadResult SymbolList::attempt_load(const std::shared_ptr<Store>& store) {
    SYMBOL_LIST_RUNTIME_LOG("Symbol list load attempt");
    LoadResult load_result;
    load_result.symbol_list_keys = get_all_symbol_list_keys(store);
    load_result.maybe_last_compaction = last_compaction(load_result.symbol_list_keys);

    if (load_result.maybe_last_compaction) {
        load_result.timestamp = load_result.symbol_list_keys.rbegin()->creation_ts();
        load_result.symbols = load_from_symbol_list_keys(store,
                {*load_result.maybe_last_compaction, load_result.symbol_list_keys.cend()});
    } else {
        load_result.timestamp = store->current_timestamp();
        load_result.symbols = load_from_version_keys(store);
    }
    return load_result;
}

SymbolList::CollectionType SymbolList::load(const std::shared_ptr<Store>& store, bool no_compaction) {
    // TODO: tighten the exception that triggers retry. https://github.com/man-group/ArcticDB/issues/447
    const LoadResult load_result = ExponentialBackoff<std::runtime_error>(100, 2000)
            .go([this, &store]() { return attempt_load(store); });

    if (!no_compaction && (load_result.symbol_list_keys.size() > max_delta_ || !load_result.maybe_last_compaction)) {
        SYMBOL_LIST_RUNTIME_LOG("Compaction necessary. Obtaining lock...");
        try {
            if (StorageLock lock{CompactionLockName}; lock.try_lock(store)) {
                OnExit x([&lock, &store] { lock.unlock(store); });

                SYMBOL_LIST_RUNTIME_LOG("Checking whether we still need to compact under lock");
                if (can_update_symbol_list(store, load_result.maybe_last_compaction)) {
                    auto written = write_symbols(store, load_result.symbols, compaction_id, load_result.timestamp).get();
                    delete_keys(store, load_result.detach_symbol_list_keys(), std::get<AtomKey>(written)).get();
                }
            } else {
                SYMBOL_LIST_RUNTIME_LOG("Not compacting the symbol list due to lock contention");
            }
        } catch (const storage::PermissionException& ex) {
            // Note: this only reflects AN's permission check and is not thrown by the Storage
            SYMBOL_LIST_RUNTIME_LOG("Not compacting the symbol list due to lack of permission");
        } catch (const std::exception& ex) {
            log::version().warn("Ignoring the error while trying to compact the symbol list: {}", ex.what());
        }
    }

    return load_result.detach_symbols();
}

bool SymbolList::can_update_symbol_list(const std::shared_ptr<Store>& store,
        const std::optional<KeyVectorItr>& maybe_last_compaction) {
    bool found_last = false;
    bool has_newer = false;

    if (maybe_last_compaction.has_value()) {
        // Symbol list keys source
        store->iterate_type(KeyType::SYMBOL_LIST,
                [&found_last, &has_newer, &last_compaction_key = *maybe_last_compaction.value()](auto&& key) {
                    auto atom = to_atom(key);
                    if (atom == last_compaction_key) found_last = true;
                    if (atom.creation_ts() > last_compaction_key.creation_ts()) has_newer = true;
            }, std::get<std::string>(compaction_id));
    } else {
        // Version keys source
        store->iterate_type(KeyType::SYMBOL_LIST, [&has_newer](auto&&) {
                has_newer = true;
            }, std::get<std::string>(compaction_id));
    }

    SYMBOL_LIST_RUNTIME_LOG_VAL("found_last={}", found_last);
    SYMBOL_LIST_RUNTIME_LOG_VAL("has_newer={}", has_newer);
    return (!maybe_last_compaction || found_last) && !has_newer;
}

    void SymbolList::write_journal(const std::shared_ptr<Store>& store, const StreamId& symbol, std::string action) {
        SegmentInMemory seg{journal_stream_descriptor(action, symbol)};
        util::variant_match(symbol,
            [&seg] (const StringId& id) {
                seg.set_string(0, id);
            },
            [&seg] (const NumericId& id) {
                seg.set_scalar<uint64_t>(0, id);
            });

        seg.end_row();
        try {
            store->write_sync(KeyType::SYMBOL_LIST, 0, StreamId{ action }, IndexValue{ symbol }, IndexValue{ symbol },
                std::move(seg));
        } catch ([[maybe_unused]] const arcticdb::storage::DuplicateKeyException& e)  {
            // Both version and content hash are fixed, so collision is possible
            ARCTICDB_DEBUG(log::storage(), "Symbol list DuplicateKeyException: {}", e.what());
        }
    }

    SymbolList::CollectionType SymbolList::load_from_version_keys(const std::shared_ptr<Store>& store) {
        SYMBOL_LIST_RUNTIME_LOG("Loading symbols from version keys");
        std::vector<StreamId> stream_ids;
        store->iterate_type(KeyType::VERSION_REF, [&] (const auto& key) {
            auto id = variant_key_id(key);
            stream_ids.push_back(id);

            if (stream_ids.size() == this->max_delta_ * 2 && !warned_expected_slowdown_) {
                log::version().warn(
                        "No compacted symbol list cache found. "
                        "`list_symbols` may take longer than expected. \n\n"
                        "See here for more information: https://docs.arcticdb.io/technical/on_disk_storage/#symbol-list-caching\n\n"
                        "To resolve, run `list_symbols` through to completion to compact the symbol list cache.\n"
                        "Note: This warning will only appear once.\n");

                warned_expected_slowdown_ = true;
            }
        });
        auto res = batch_get_latest_version(store, version_map_, stream_ids, false);

        CollectionType symbols{};
        for(const auto& map_pair: *res) {
            symbols.insert(map_pair.first);
        }

        version_map_->flush();
        return symbols;
    }

    folly::Future<VariantKey> SymbolList::write_symbols(
        const std::shared_ptr<Store>& store,
        const CollectionType& symbols,
        const StreamId& stream_id,
        timestamp creation_ts)  {

        SYMBOL_LIST_RUNTIME_LOG_VAL("Writing following number of symbols to symbol list cache: {}", symbols.size());
        SegmentInMemory list_segment{symbol_stream_descriptor(stream_id)};

        for(const auto& symbol : symbols) {
            ARCTICDB_DEBUG(log::version(), "Writing symbol '{}' to list", symbol);
            util::variant_match(type_holder_,
                [&](const StringId&) {
                    util::check(std::holds_alternative<StringId>(symbol), "Cannot write string symbol name, existing symbols are numeric");
                    list_segment.set_string(0, std::get<StringId>(symbol));
                },
                [&](const NumericId&) {
                    util::check(std::holds_alternative<NumericId>(symbol), "Cannot write numeric symbol name, existing symbols are strings");
                    list_segment.set_scalar(0, std::get<NumericId>(symbol));
                }
            );
            list_segment.end_row();
        }
        if(symbols.empty()) {
            google::protobuf::Any any = {};
            arcticdb::proto::descriptors::SymbolListDescriptor metadata;
            metadata.set_enabled(true);
            any.PackFrom(metadata);
            list_segment.set_metadata(std::move(any));
        }
        return store->write(KeyType::SYMBOL_LIST, 0, stream_id, creation_ts, 0, 0, std::move(list_segment));
    }

    SymbolList::CollectionType SymbolList::load_from_symbol_list_keys(
            const std::shared_ptr<StreamSource>& store,
            const folly::Range<KeyVectorItr>& keys) {
        SYMBOL_LIST_RUNTIME_LOG("Loading symbols from symbol list keys");
        bool read_compaction = false;
        CollectionType symbols{};
        for(const auto& key : keys) {
            if(key.id() == compaction_id) {
                read_list_from_storage(store, key, symbols);
                read_compaction = true;
            }
            else {
                const auto& action = key.id();
                const auto& symbol = key.start_index();
                if(action == StreamId{DeleteSymbol}) {
                    ARCTICDB_DEBUG(log::version(), "Got delete action for symbol '{}'", symbol);
                    symbols.erase(symbol);
                }
                else {
                    ARCTICDB_DEBUG(log::version(), "Got insert action for symbol '{}'", symbol);
                    symbols.insert(symbol);
                }
            }
        }
        SYMBOL_LIST_RUNTIME_LOG_VAL("Post load, got following number of symbols: {}", symbols.size());
        if(!read_compaction) {
            SYMBOL_LIST_RUNTIME_LOG_VAL("Read no compacted segment from symbol list of size: {}", keys.size());
        }
        return symbols;
    }

    void SymbolList::read_list_from_storage(
        const std::shared_ptr<StreamSource>& store,
        const AtomKey& key,
        CollectionType& symbols) {
        ARCTICDB_DEBUG(log::version(), "Reading list from storage with key {}", key);
        auto key_seg = store->read(key).get().second;
        missing_data::check<ErrorCode::E_UNREADABLE_SYMBOL_LIST>( key_seg.descriptor().field_count() > 0,
            "Expected at least one column in symbol list with key {}", key);

        const auto& field_desc = key_seg.descriptor().field(0);
        if (key_seg.row_count() > 0) {
            auto data_type =  field_desc.type().data_type();
            if (data_type == DataType::UINT64) {
                for (auto row : key_seg) {
                    auto num_id = key_seg.scalar_at<uint64_t>(row.row_id_, 0).value();
                    ARCTICDB_DEBUG(log::version(), "Reading numeric symbol {}", num_id);
                    symbols.emplace(safe_convert_to_numeric_id(num_id, "Numeric symbol"));
                }
            } else if (data_type == DataType::ASCII_DYNAMIC64) {
                for (auto row : key_seg) {
                    auto sym = key_seg.string_at(row.row_id_, 0).value();
                    ARCTICDB_DEBUG(log::version(), "Reading string symbol '{}'", sym);
                    symbols.emplace(std::string{sym});
                }
            } else {
                missing_data::raise<ErrorCode::E_UNREADABLE_SYMBOL_LIST>(
                        "The symbol list contains unsupported symbol type: {}", data_type);
            }
        }
    }

    SymbolList::KeyVector SymbolList::get_all_symbol_list_keys(const std::shared_ptr<StreamSource>& store) {
        std::vector<AtomKey> output;
        uint64_t uncompacted_keys_found = 0;
        store->iterate_type(KeyType::SYMBOL_LIST, [&] (auto&& key) -> void {
            auto atom_key = to_atom(key);
            if(atom_key.id() != compaction_id) {
                uncompacted_keys_found++;
            }
            if (uncompacted_keys_found == this->max_delta_ * 2 && !warned_expected_slowdown_) {
                log::version().warn(
                        "`list_symbols` may take longer than expected as there have been many modifications since `list_symbols` was last called. \n\n"
                        "See here for more information: https://docs.arcticdb.io/technical/on_disk_storage/#symbol-list-caching\n\n"
                        "To resolve, run `list_symbols` through to completion frequently.\n"
                        "Note: This warning will only appear once.\n");

                warned_expected_slowdown_ = true;
            }

            output.push_back(atom_key);
        });

        std::sort(output.begin(), output.end(), [] (const AtomKey& left, const AtomKey& right) {
            return left.creation_ts() < right.creation_ts();
        });
        return output;
    }

    folly::Future<std::vector<Store::RemoveKeyResultType>>
    SymbolList::delete_keys(const std::shared_ptr<Store>& store, KeyVector&& to_remove, const AtomKey& exclude) {
        std::vector<VariantKey> variant_keys;
        variant_keys.reserve(to_remove.size());
        for(auto& atom_key: to_remove) {
            // Corner case: if the newly written Compaction key (exclude) has the same timestamp as an existing one
            // (e.g. when a previous compaction round failed in the deletion step), we don't want to delete the former
            if (atom_key != exclude) {
                variant_keys.emplace_back(std::move(atom_key));
            }
        }

        return store->remove_keys(variant_keys);
    }

std::optional<SymbolList::KeyVectorItr> SymbolList::last_compaction(const KeyVector& keys) {
        auto pos = std::find_if(keys.rbegin(), keys.rend(), [] (const auto& key) {
            return key.id() == compaction_id;
        }) ;

        if (pos == keys.rend()) {
            return std::nullopt;
        } else {
            return { (pos + 1).base() }; // reverse_iterator -> forward itr has an offset of 1 per docs
        }
    }

} //namespace arcticdb
