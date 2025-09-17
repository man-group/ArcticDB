#include <arcticdb/entity/types.hpp>
#include <arcticdb/version/symbol_list.hpp>

/*
 * This file represents the way that the symbol list used to work, so that we can test that new- and old-style symbol
 * list functionalirt can co-exist
 */

namespace arcticdb {

using BackwardsCompatCollectionType = std::set<StreamId>;

void backwards_compat_read_list_from_storage(
        const std::shared_ptr<StreamSource>& store, const AtomKey& key, BackwardsCompatCollectionType& symbols
) {
    ARCTICDB_DEBUG(log::version(), "Reading list from storage with key {}", key);
    auto key_seg = store->read(key).get().second;
    missing_data::check<ErrorCode::E_UNREADABLE_SYMBOL_LIST>(
            key_seg.descriptor().field_count() > 0, "Expected at least one column in symbol list with key {}", key
    );

    const auto& field_desc = key_seg.descriptor().field(0);
    if (key_seg.row_count() > 0) {
        auto data_type = field_desc.type().data_type();
        if (data_type == DataType::UINT64) {
            for (auto row : key_seg) {
                auto num_id = key_seg.scalar_at<uint64_t>(row.row_id_, 0).value();
                ARCTICDB_DEBUG(log::version(), "Reading numeric symbol {}", num_id);
                symbols.emplace(safe_convert_to_numeric_id(num_id));
            }
        } else if (data_type == DataType::ASCII_DYNAMIC64) {
            for (auto row : key_seg) {
                auto sym = key_seg.string_at(row.row_id_, 0).value();
                ARCTICDB_DEBUG(log::version(), "Reading string symbol '{}'", sym);
                symbols.emplace(std::string{sym});
            }
        } else {
            missing_data::raise<ErrorCode::E_UNREADABLE_SYMBOL_LIST>(
                    "The symbol list contains unsupported symbol type: {}", data_type
            );
        }
    }
}

std::vector<AtomKey> backwards_compat_get_all_symbol_list_keys(const std::shared_ptr<StreamSource>& store) {
    std::vector<AtomKey> output;
    uint64_t uncompacted_keys_found = 0;
    store->iterate_type(KeyType::SYMBOL_LIST, [&](auto&& key) -> void {
        auto atom_key = to_atom(key);
        if (atom_key.id() != StreamId{std::string{CompactionId}}) {
            uncompacted_keys_found++;
        }

        output.push_back(atom_key);
    });

    std::sort(output.begin(), output.end(), [](const AtomKey& left, const AtomKey& right) {
        return left.creation_ts() < right.creation_ts();
    });
    return output;
}

BackwardsCompatCollectionType backwards_compat_load(
        const std::shared_ptr<StreamSource>& store, const std::vector<AtomKey>& keys
) {
    BackwardsCompatCollectionType symbols{};
    for (const auto& key : keys) {
        if (key.id() == StreamId{std::string{CompactionId}}) {
            backwards_compat_read_list_from_storage(store, key, symbols);
        } else {
            const auto& action = key.id();
            const auto& symbol = key.start_index();
            if (action == StreamId{std::string{DeleteSymbol}}) {
                ARCTICDB_DEBUG(log::version(), "Got delete action for symbol '{}'", symbol);
                symbols.erase(symbol);
            } else {
                ARCTICDB_DEBUG(log::version(), "Got insert action for symbol '{}'", symbol);
                symbols.insert(symbol);
            }
        }
    }
    return symbols;
}

BackwardsCompatCollectionType backwards_compat_get_symbols(const std::shared_ptr<Store>& store) {
    auto keys = backwards_compat_get_all_symbol_list_keys(store);
    return backwards_compat_load(store, keys);
}

inline StreamDescriptor backwards_compat_symbol_stream_descriptor(
        const StreamId& stream_id, const StreamId& type_holder
) {
    auto data_type = std::holds_alternative<StringId>(type_holder) ? DataType::ASCII_DYNAMIC64 : DataType::UINT64;
    return StreamDescriptor{stream_descriptor(stream_id, RowCountIndex(), {scalar_field(data_type, "symbol")})};
}

inline StreamDescriptor backward_compat_journal_stream_descriptor(const StreamId& action, const StreamId& id) {
    return util::variant_match(
            id,
            [&action](const NumericId&) {
                return StreamDescriptor{
                        stream_descriptor(action, RowCountIndex(), {scalar_field(DataType::UINT64, "symbol")})
                };
            },
            [&action](const StringId&) {
                return StreamDescriptor{
                        stream_descriptor(action, RowCountIndex(), {scalar_field(DataType::UTF_DYNAMIC64, "symbol")})
                };
            }
    );
}

folly::Future<VariantKey> backwards_compat_write(
        const std::shared_ptr<Store>& store, const BackwardsCompatCollectionType& symbols, const StreamId& stream_id,
        timestamp creation_ts, const StreamId& type_holder
) {

    SegmentInMemory list_segment{backwards_compat_symbol_stream_descriptor(stream_id, type_holder)};

    for (const auto& symbol : symbols) {
        ARCTICDB_DEBUG(log::version(), "Writing symbol '{}' to list", symbol);
        util::variant_match(
                type_holder,
                [&](const StringId&) {
                    util::check(
                            std::holds_alternative<StringId>(symbol),
                            "Cannot write string symbol name, existing symbols are numeric"
                    );
                    list_segment.set_string(0, std::get<StringId>(symbol));
                },
                [&](const NumericId&) {
                    util::check(
                            std::holds_alternative<NumericId>(symbol),
                            "Cannot write numeric symbol name, existing symbols are strings"
                    );
                    list_segment.set_scalar(0, std::get<NumericId>(symbol));
                }
        );
        list_segment.end_row();
    }
    if (symbols.empty()) {
        google::protobuf::Any any = {};
        arcticdb::proto::descriptors::SymbolListDescriptor metadata;
        metadata.set_enabled(true);
        any.PackFrom(metadata);
        list_segment.set_metadata(std::move(any));
    }
    return store->write(
            KeyType::SYMBOL_LIST, 0, stream_id, creation_ts, NumericIndex{0}, NumericIndex{0}, std::move(list_segment)
    );
}

// Very old internal ArcticDB clients (2021) wrote non-zero version IDs in symbol list entries. This API supports that.
void extremely_backwards_compat_write_journal(
        const std::shared_ptr<Store>& store, const StreamId& symbol, const std::string& action, VersionId version_id
) {
    SegmentInMemory seg{backward_compat_journal_stream_descriptor(action, symbol)};
    util::variant_match(
            symbol,
            [&seg](const StringId& id) { seg.set_string(0, id); },
            [&seg](const NumericId& id) { seg.set_scalar<uint64_t>(0, id); }
    );

    seg.end_row();
    try {
        store->write_sync(
                KeyType::SYMBOL_LIST,
                version_id,
                StreamId{action},
                IndexValue{symbol},
                IndexValue{symbol},
                std::move(seg)
        );
    } catch ([[maybe_unused]] const arcticdb::storage::DuplicateKeyException& e) {
        // Both version and content hash are fixed, so collision is possible
        ARCTICDB_DEBUG(log::storage(), "Symbol list DuplicateKeyException: {}", e.what());
    }
}

// Internal ArcticDB clients (2021) write symbol list entries with an obsolete schema, and always with version ID 0.
void backwards_compat_write_journal(
        const std::shared_ptr<Store>& store, const StreamId& symbol, const std::string& action
) {
    extremely_backwards_compat_write_journal(store, symbol, action, 0);
}

void backwards_compat_compact(
        const std::shared_ptr<Store>& store, std::vector<AtomKey>&& old_keys,
        const BackwardsCompatCollectionType& symbols
) {
    auto compacted_key =
            backwards_compat_write(store, symbols, StreamId{std::string{CompactionId}}, timestamp(12), StringId{})
                    .get();
    delete_keys(store, std::move(old_keys), to_atom(compacted_key));
}

} // namespace arcticdb