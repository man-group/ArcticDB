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

    SymbolList::CollectionType SymbolList::load_symbols(const std::shared_ptr<Store>& store, bool no_compaction) {
        auto all_keys = get_all_symbol_list_keys(store);
        auto maybe_last_compaction = last_compaction(all_keys);
        if(!maybe_last_compaction && !no_compaction) {
            SYMBOL_LIST_RUNTIME_LOG("Failed to find most recent compaction, reloading all");
            StorageLock lock{CompactionLockName};
            timestamp creation_ts = store->current_timestamp();
            // We choose the creation_ts for the compacted key just before we do the version key iteration
            auto symbols = load_from_version_keys(store);
            try {
                if (lock.try_lock(store)) {
                    SYMBOL_LIST_RUNTIME_LOG("Got lock");
                    OnExit on_exit([&, store = store]() { lock.unlock(store); });
                    write_symbols(store, symbols, compaction_id, creation_ts).get();
                    delete_keys(store, all_keys);
                } else {
                    SYMBOL_LIST_RUNTIME_LOG("Not writing symbols as another write is in progress");
                }
            } catch (const storage::PermissionException& ex) {
                SYMBOL_LIST_RUNTIME_LOG("Can't compact symbol list in read-only mode");
            } catch (const std::system_error& ex) {
                SYMBOL_LIST_RUNTIME_LOG("Caught error in symbol list write: {}", ex.what());
            }
            // Acceptable to swallow the exceptions above and return symbols because it is loaded from the source
            // (version keys) without involving the (inaccessible) symbol list cache:
            return symbols;
        }
        else {
            if(all_keys.size() > max_delta_ && !no_compaction) {
                SYMBOL_LIST_RUNTIME_LOG("Symbol chain too long, doing compaction");
                return compact(store, all_keys);
            }
            else {
                auto compaction_and_deltas = KeyVector{all_keys.begin() + maybe_last_compaction.value_or(0), all_keys.end()};
                return load_from_storage(store, compaction_and_deltas);
            }
        }
    }

    SymbolList::CollectionType SymbolList::compact(std::shared_ptr<Store> store, const std::vector<AtomKey>& symbol_keys) {
        try {
            StorageLock lock{CompactionLockName};
            SYMBOL_LIST_RUNTIME_LOG("Doing symbol list compaction with {} keys", symbol_keys.size());
            if(lock.try_lock(store)) {
                SYMBOL_LIST_RUNTIME_LOG("Got lock");
                OnExit on_exit([&, store=store] () { lock.unlock(store); });
                // Rescan keys inside the lock just in case another compaction
                // is just finishing (N.B. unlikely)
                auto all_symbols = get_all_symbol_list_keys(store);
                auto symbols = load_from_storage(store, all_symbols);
                write_symbols(store, symbols, compaction_id, symbol_keys.rbegin()->creation_ts()).get();


                delete_keys(store, all_symbols);
                return symbols;
            } else {
                SYMBOL_LIST_RUNTIME_LOG("Didn't get lock, not compacting");
            }
        } catch(const storage::PermissionException& ex) {
            SYMBOL_LIST_RUNTIME_LOG("Can't compact symbol list in read-only mode, loading from storage");
        } catch (const std::system_error& ex) {
            SYMBOL_LIST_RUNTIME_LOG("Caught error in symbol list write: {}", ex.what());
        }

        SYMBOL_LIST_RUNTIME_LOG("Fallback load_from_storage");
        auto all_symbols = get_all_symbol_list_keys(store);
        return load_from_storage(store, all_symbols);
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
        store->write_sync(KeyType::SYMBOL_LIST, 0, StreamId{action}, IndexValue{symbol}, IndexValue{symbol},
                std::move(seg));
    }

    SymbolList::CollectionType SymbolList::load_from_version_keys(const std::shared_ptr<Store>& store) {
        std::vector<StreamId> stream_ids;
        store->iterate_type(KeyType::VERSION_REF, [&] (const auto& key) {
            auto id = variant_key_id(key);
            stream_ids.push_back(id);
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

        SYMBOL_LIST_RUNTIME_LOG("Writing {} symbols to symbol list cache", symbols.size());
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

    SymbolList::CollectionType SymbolList::load_from_storage(const std::shared_ptr<StreamSource>& store, const std::vector<AtomKey>& keys) {
        SYMBOL_LIST_RUNTIME_LOG("Loading from storage");
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
        SYMBOL_LIST_RUNTIME_LOG("Post load, got {} symbols", symbols.size());
        if(!read_compaction) {
            SYMBOL_LIST_RUNTIME_LOG("Read no compacted segment from symbol list of size {}", keys.size());
        }
        return symbols;
    }

    void SymbolList::read_list_from_storage(const std::shared_ptr<StreamSource>& store, const AtomKey& key,
            CollectionType& symbols) {
        auto key_seg = store->read(key).get();
        ARCTICDB_DEBUG(log::version(), "Reading list from storage with key {}", key);
        if(key_seg.second.descriptor().fields().empty())
            util::raise_rte("Expected at least one column in symbol list with key {}", key);

        const auto& field_desc = key_seg.second.descriptor().fields().at(0);
        const auto data_type = field_desc.type().data_type();
        if (key_seg.second.row_count() > 0) {
            for (auto row : key_seg.second) {
                if (data_type == DataType::UINT64) {
                    row.visit_scalar_type(0, uint64_t{}, [&](auto val) {
                        ARCTICDB_DEBUG(log::version(), "Reading numeric symbol {}", val.value());
                        symbols.insert(StreamId{safe_convert_to_numeric_id(val.value(), "Numeric symbol")});
                    });
                } else if (data_type == DataType::ASCII_DYNAMIC64) {
                    row.visit_string(0, [&](const auto &val) {
                        ARCTICDB_DEBUG(log::version(), "Reading string symbol '{}'", val.value());
                        symbols.insert(StreamId{std::string{val.value()}});
                    });
                } else {
                    util::raise_rte("Encountered unknown key type");
                }
            }
        }
    }

    SymbolList::KeyVector SymbolList::get_all_symbol_list_keys(const std::shared_ptr<StreamSource>& store) const {
        std::vector<AtomKey> output;
        store->iterate_type(KeyType::SYMBOL_LIST, [&] (auto&& key) -> void {
            output.push_back(to_atom(key));
        });

        std::sort(output.begin(), output.end(), [] (const AtomKey& left, const AtomKey& right) {
            return left.creation_ts() < right.creation_ts();
        });
        return output;
    }

    void SymbolList::delete_keys(
            std::shared_ptr<Store> store, const KeyVector& lists) {
        std::vector<VariantKey> vars;
        vars.reserve(lists.size());
        for(const auto& atom_key: lists)
            vars.emplace_back(VariantKey{atom_key});
        store->remove_keys(vars).get();
    }

    std::optional<SymbolList::KeyVector::difference_type> SymbolList::last_compaction(const KeyVector& keys) {
        auto pos = std::find_if(keys.rbegin(), keys.rend(), [] (const auto& key) {
            return key.id() == compaction_id;
        }) ;

        if(pos == keys.rend())
            return std::nullopt;

        auto forward_pos = std::distance(std::begin(keys), pos.base()) - 1;
        util::check(keys[forward_pos].id() == compaction_id,
                "Compaction point {} is incorrect in vector of size {}", forward_pos, keys.size());
        return std::make_optional(forward_pos);
    }

} //namespace arcticdb
