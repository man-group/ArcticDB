/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/version/version_map_batch_methods.hpp>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/util/configs_map.hpp>
#include <arcticdb/util/storage_lock.hpp>
#include <arcticdb/util/key_utils.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/log/log.hpp>
#include <arcticdb/storage/store.hpp>
#include <arcticdb/stream/merge_utils.hpp>

namespace arcticdb {

using namespace arcticdb::stream;

static const StreamId compaction_id{StringId{CompactionId}};

constexpr std::string_view version_string = "_v2_";
constexpr NumericIndex version_identifier = std::numeric_limits<NumericIndex>::max();

SymbolListData::SymbolListData(std::shared_ptr<VersionMap> version_map, StreamId type_indicator, uint32_t seed) :
    type_holder_(std::move(type_indicator)),
    seed_(seed),
    version_map_(std::move(version_map)) {}

auto warning_threshold() {
    return 2 * static_cast<size_t>(
                       ConfigsMap::instance()
                               ->get_int("SymbolList.MaxDelta")
                               .value_or(ConfigsMap::instance()->get_int("SymbolList.MaxCompactionThreshold", 700))
               );
}

bool is_new_style_key(const AtomKey& key) {
    return util::variant_match(
            key.end_index(),
            [](std::string_view str) { return str == version_string; },
            [](NumericIndex n) { return n == version_identifier; }
    );
}

std::vector<SymbolListEntry> load_previous_from_version_keys(
        const std::shared_ptr<Store>& store, SymbolListData& data, WillAttemptCompaction will_attempt_compaction
) {
    std::vector<StreamId> stream_ids;
    store->iterate_type(KeyType::VERSION_REF, [&data, &stream_ids, will_attempt_compaction](const auto& key) {
        auto id = variant_key_id(key);
        stream_ids.push_back(id);

        if (stream_ids.size() == warning_threshold() && !data.warned_expected_slowdown_) {
            log::symbol().warn(
                    "No compacted symbol list cache found. "
                    "`list_symbols` may take longer than expected. \n\n"
                    "See here for more information: "
                    "https://docs.arcticdb.io/latest/technical/on_disk_storage/#symbol-list-caching\n\n"
                    "To resolve, run `list_symbols` through to completion to compact the symbol list cache. "
                    "Note: write access to storage is required for compaction. "
                    "{}.\n"
                    "Note: This warning will only appear once.\n",
                    will_attempt_compaction
            );

            data.warned_expected_slowdown_ = true;
        }
    });
    auto res =
            folly::collect(batch_get_latest_undeleted_and_latest_versions_async(store, data.version_map_, stream_ids))
                    .get();

    std::vector<SymbolListEntry> symbols;
    for (auto&& [idx, opt_key_pair] : folly::enumerate(res)) {
        const auto& [maybe_undeleted, _] = opt_key_pair;
        if (maybe_undeleted) {
            const auto version_id = maybe_undeleted->version_id();
            const auto timestamp = maybe_undeleted->creation_ts();
            symbols.emplace_back(stream_ids[idx], version_id, timestamp, ActionType::ADD);
        }
    }

    data.version_map_->flush();
    return symbols;
}

// The below string_at and scalar_at functions should be used for symbol list cache segments instead of the ones
// provided in SegmentInMemory, because the symbol list structure is the only place where columns can have more entries
// than the segment has rows. Hence, we need to bypass the checks inside SegmentInMemory's function and directly call
// the Column's string_at and scalar_at.
std::string_view string_at(const SegmentInMemory& seg, position_t row, position_t col) {
    auto offset = seg.column(col).scalar_at<position_t>(row);
    util::check(offset.has_value(), "Symbol list trying to call string_at for missing row {}, column {}", row, col);
    return seg.string_pool_ptr()->get_view(offset.value());
}

template<typename T>
T scalar_at(const SegmentInMemory& seg, position_t row, position_t col) {
    auto scalar = seg.column(col).scalar_at<T>(row);
    util::check(scalar.has_value(), "Symbol list trying to call scalar_at for missing row {}, column {}", row, col);
    return scalar.value();
}

StreamId stream_id_from_segment(DataType data_type, const SegmentInMemory& seg, position_t row_id, position_t column) {
    if (data_type == DataType::UINT64) {
        auto num_id = scalar_at<uint64_t>(seg, row_id, column);
        ARCTICDB_DEBUG(log::symbol(), "Reading numeric symbol {}", num_id);
        return safe_convert_to_numeric_id(num_id);
    } else {
        auto sym = string_at(seg, row_id, column);
        ARCTICDB_DEBUG(log::symbol(), "Reading string symbol '{}'", sym);
        return StringId{sym};
    }
}

DataType get_symbol_data_type(const SegmentInMemory& seg) {
    const auto& field_desc = seg.descriptor().field(0);
    auto data_type = field_desc.type().data_type();

    missing_data::check<ErrorCode::E_UNREADABLE_SYMBOL_LIST>(
            data_type == DataType::UINT64 || data_type == DataType::ASCII_DYNAMIC64,
            "The symbol list contains unsupported symbol type: {}",
            data_type
    );

    return data_type;
}

std::vector<SymbolListEntry> read_old_style_list_from_storage(const SegmentInMemory& seg) {
    std::vector<SymbolListEntry> output;
    if (seg.empty())
        return output;

    const auto data_type = get_symbol_data_type(seg);

    for (auto row : seg)
        output.emplace_back(stream_id_from_segment(data_type, seg, row.row_id_, 0), 0, 0, ActionType::ADD);

    return output;
}

std::vector<SymbolListEntry> read_new_style_list_from_storage(const SegmentInMemory& seg) {
    std::vector<SymbolListEntry> output;
    if (seg.empty())
        return output;

    const auto data_type = get_symbol_data_type(seg);

    // Because we need to be backwards compatible with the old style symbol list, the additions and deletions are
    // in separate columns. The first three columns are the symbol, version and timestamp for the additions, and the
    // next three are the same for the deletions. Old-style symbol lists will ignore everything but the first column
    // which will mean that they can't do any conflict resolution but will get the correct data.
    util::check(
            seg.column(0).row_count() == seg.column(1).row_count() &&
                    seg.column(0).row_count() == seg.column(2).row_count(),
            "Column mismatch in symbol segment additions: {} {} {}",
            seg.column(0).row_count(),
            seg.column(1).row_count(),
            seg.column(2).row_count()
    );

    for (auto i = 0L; i < seg.column(0).row_count(); ++i) {
        auto stream_id = stream_id_from_segment(data_type, seg, i, 0);
        auto reference_id = VersionId{scalar_at<uint64_t>(seg, i, 1)};
        auto reference_time = timestamp{scalar_at<int64_t>(seg, i, 2)};
        ARCTICDB_RUNTIME_DEBUG(
                log::symbol(), "Reading added symbol {}: {}@{}", stream_id, reference_id, reference_time
        );
        output.emplace_back(stream_id, reference_id, reference_time, ActionType::ADD);
    }

    if (seg.descriptor().field_count() == 6) {
        util::check(
                seg.column(3).row_count() == seg.column(4).row_count() &&
                        seg.column(3).row_count() == seg.column(5).row_count(),
                "Column mismatch in symbol segment deletions: {} {} {}",
                seg.column(3).row_count(),
                seg.column(4).row_count(),
                seg.column(5).row_count()
        );

        for (auto i = 0L; i < seg.column(3).row_count(); ++i) {
            auto stream_id = stream_id_from_segment(data_type, seg, i, 3);
            auto reference_id = VersionId{scalar_at<uint64_t>(seg, i, 4)};
            auto reference_time = timestamp{scalar_at<int64_t>(seg, i, 5)};
            ARCTICDB_RUNTIME_DEBUG(
                    log::symbol(), "Reading deleted symbol {}: {}@{}", stream_id, reference_id, reference_time
            );
            output.emplace_back(stream_id, reference_id, reference_time, ActionType::DELETE);
        }
    }

    return output;
}

std::vector<SymbolListEntry> read_from_storage(const std::shared_ptr<StreamSource>& store, const AtomKey& key) {
    ARCTICDB_DEBUG(log::symbol(), "Reading list from storage with key {}", key);
    auto [_, seg] = store->read_sync(key);
    if (seg.row_count() == 0)
        return {};

    missing_data::check<ErrorCode::E_UNREADABLE_SYMBOL_LIST>(
            seg.descriptor().field_count() > 0, "Expected at least one column in symbol list with key {}", key
    );

    if (seg.descriptor().field_count() == 1)
        return read_old_style_list_from_storage(seg);
    else
        return read_new_style_list_from_storage(seg);
}

JournalEntryData journal_entry_from_atom(const AtomKey& key) {
    return {key.version_id(),
            key.creation_ts(),
            key.content_hash(),
            std::get<StringId>(key.id()) == DeleteSymbol ? ActionType::DELETE : ActionType::ADD,
            is_new_style_key(key)};
}

// Reconstructs the original AtomKey from a JournalEntryData + the owning symbol (map key).
// For new-style keys the end_index encodes the version marker; for old-style it equals the symbol.
AtomKey atom_key_from_journal_entry(const StreamId& symbol, const JournalEntryData& ck) {
    IndexValue end_index;
    if (ck.is_new_style) {
        end_index = std::holds_alternative<StringId>(symbol) ? IndexValue{StringIndex{std::string{version_string}}}
                                                             : IndexValue{NumericIndex{version_identifier}};
    } else {
        end_index = IndexValue{symbol};
    }
    return atom_key_builder()
            .version_id(ck.key_version_id)
            .creation_ts(ck.creation_ts)
            .content_hash(ck.content_hash)
            .start_index(IndexValue{symbol})
            .end_index(end_index)
            .build(action_id(ck.action), KeyType::SYMBOL_LIST);
}

SymbolEntryData to_symbol_entry_data(const JournalEntryData& ck) {
    const auto reference_id = ck.is_new_style ? ck.key_version_id : unknown_version_id;
    return {reference_id, ck.creation_ts, ck.action};
}

void add_journal_entry(JournalMapType& update_map, const AtomKey& key) {
    update_map[key.start_index()].emplace_back(journal_entry_from_atom(key));
}

void sort_journal_map(JournalMapType& update_map) {
    for (auto& [symbol, keys] : update_map) {
        std::sort(keys.begin(), keys.end(), [](const JournalEntryData& a, const JournalEntryData& b) {
            auto a_ver = a.is_new_style ? a.key_version_id : VersionId{0};
            auto b_ver = b.is_new_style ? b.key_version_id : VersionId{0};
            return std::tie(a_ver, a.creation_ts) < std::tie(b_ver, b.creation_ts);
        });
    }
}

/// Single-pass iteration over SYMBOL_LIST keys: builds the update map and locates the latest
/// compaction key. Always uses JournalEntryData (32B/entry) for the update map.
JournalResult load_journal_streaming(
        const std::shared_ptr<Store>& store, SymbolListData& data, WillAttemptCompaction will_attempt_compaction
) {
    JournalResult result;
    size_t uncompacted_keys_found = 0;

    store->iterate_type(KeyType::SYMBOL_LIST, [&](auto&& key) {
        auto atom_key = to_atom(std::forward<decltype(key)>(key));
        result.total_key_count++;

        if (atom_key.id() == compaction_id) {
            if (!result.compaction_key || atom_key.creation_ts() > result.compaction_key->creation_ts())
                result.compaction_key = atom_key;
            result.compaction_keys.emplace_back(std::move(atom_key));
        } else {
            ++uncompacted_keys_found;
            if (uncompacted_keys_found == warning_threshold() && !data.warned_expected_slowdown_) {
                log::symbol().warn(
                        "`list_symbols` may take longer than expected as there have been many modifications "
                        "since `list_symbols` was last called. \n\n"
                        "See here for more information: "
                        "https://docs.arcticdb.io/latest/technical/on_disk_storage/#symbol-list-caching\n\n"
                        "To resolve, run `list_symbols` through to completion frequently. "
                        "Note: write access to storage is required for compaction. "
                        "{}.\n"
                        "Note: This warning will only appear once.\n",
                        will_attempt_compaction
                );
                data.warned_expected_slowdown_ = true;
            }
            add_journal_entry(result.update_map, atom_key);
        }
    });

    sort_journal_map(result.update_map);
    return result;
}

auto tail_range(const std::vector<SymbolEntryData>& updated) {
    auto it = std::crbegin(updated);
    const auto reference_id = it->reference_id_;
    auto action = it->action_;
    bool all_same_action = true;
    ++it;

    while (it != std::crend(updated) && it->reference_id_ == reference_id) {
        if (it->action_ != action)
            all_same_action = false;

        ++it;
    }

    return std::make_pair(it, all_same_action);
}

std::optional<SymbolEntryData> timestamps_too_close(
        const std::vector<SymbolEntryData>::const_reverse_iterator& first, const std::vector<SymbolEntryData>& updated,
        timestamp min_allowed_interval, bool all_same_action
) {
    if (first == std::crend(updated))
        return std::nullopt;

    const auto& latest = *updated.rbegin();
    const bool same_as_updates = all_same_action && latest.action_ == first->action_;
    const auto diff = latest.timestamp_ - first->timestamp_;

    if (same_as_updates || diff >= min_allowed_interval)
        return std::nullopt;

    return latest;
}

bool has_unknown_reference_id(const SymbolEntryData& data) { return data.reference_id_ == unknown_version_id; }

bool contains_unknown_reference_ids(const std::vector<SymbolEntryData>& updated) {
    return std::any_of(std::begin(updated), std::end(updated), [](const auto& data) {
        return has_unknown_reference_id(data);
    });
}

SymbolVectorResult cannot_validate_symbol_vector() { return {ProblematicResult{true}}; }

SymbolVectorResult vector_has_problem(const SymbolEntryData& data) { return {ProblematicResult{data}}; }

SymbolVectorResult vector_okay(bool all_same_version, bool all_same_action, size_t latest_id_count) {
    return {ProblematicResult{false}, all_same_version, all_same_action, latest_id_count};
}

SymbolVectorResult is_problematic_vector(const std::vector<SymbolEntryData>& updated, timestamp min_allowed_interval) {
    if (contains_unknown_reference_ids(updated))
        return cannot_validate_symbol_vector();

    const auto [start, all_same_action] = tail_range(updated);
    const auto latest_id_count = std::distance(std::crbegin(updated), start);
    const auto all_same_version = start == std::crend(updated);

    if (auto timestamp_problem = timestamps_too_close(start, updated, min_allowed_interval, all_same_action);
        timestamp_problem)
        return vector_has_problem(*timestamp_problem);

    if (latest_id_count <= 2 || all_same_action)
        return vector_okay(all_same_version, all_same_action, latest_id_count);

    return vector_has_problem(*std::crbegin(updated));
}

ProblematicResult is_problematic(const std::vector<SymbolEntryData>& updated, timestamp min_allowed_interval) {
    return is_problematic_vector(updated, min_allowed_interval).problematic_result_;
}

ProblematicResult is_problematic(
        const SymbolListEntry& existing, const std::vector<SymbolEntryData>& updated, timestamp min_allowed_interval
) {
    ARCTICDB_DEBUG(
            log::symbol(), "{} {} {}", existing.stream_id_, static_cast<const SymbolEntryData&>(existing), updated
    );

    const auto& latest = *std::crbegin(updated);
    if (existing.reference_id_ > latest.reference_id_)
        return ProblematicResult{existing};

    auto [problematic_result, vector_all_same_version, vector_all_same_action, last_id_count] =
            is_problematic_vector(updated, min_allowed_interval);
    if (problematic_result)
        return problematic_result;

    if (problematic_result.contains_unknown_reference_ids_ || has_unknown_reference_id(existing))
        return cannot_determine_validity();

    const bool all_same_action = vector_all_same_action && existing.action_ == latest.action_;

    if (latest.timestamp_ - existing.timestamp_ < min_allowed_interval && !all_same_action)
        return ProblematicResult{latest.reference_id_ > existing.reference_id_ ? latest : existing};

    if (existing.reference_id_ < latest.reference_id_)
        return not_a_problem();

    if (all_same_action)
        return not_a_problem();

    if (last_id_count == 1)
        return not_a_problem();

    return ProblematicResult{latest};
}

void resolve_problematic_symbols(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store,
        std::map<StreamId, std::pair<VersionId, timestamp>>& problematic_symbols, CollectionType& symbols
) {
    if (problematic_symbols.empty())
        return;

    auto symbol_versions = std::make_shared<std::vector<StreamId>>();
    for (const auto& [symbol, reference_pair] : problematic_symbols)
        symbol_versions->emplace_back(symbol);

    auto versions = batch_check_latest_id_and_status(store, version_map, symbol_versions);

    for (const auto& [symbol, reference_pair] : problematic_symbols) {
        auto reference_id = reference_pair.first;

        if (auto version = versions->find(symbol); version != versions->end()) {
            const auto& symbol_state = version->second;
            if (symbol_state.exists_) {
                ARCTICDB_DEBUG(
                        log::symbol(),
                        "Problematic symbol/version pair: {}@{}: exists at id {}",
                        symbol,
                        reference_id,
                        symbol_state.version_id_
                );
                symbols.emplace_back(symbol, symbol_state.version_id_, symbol_state.timestamp_, ActionType::ADD);
            } else {
                symbols.emplace_back(symbol, symbol_state.version_id_, symbol_state.timestamp_, ActionType::DELETE);
                ARCTICDB_DEBUG(
                        log::symbol(),
                        "Problematic symbol/version pair: {}@{}: deleted at id {}",
                        symbol,
                        reference_id,
                        symbol_state.version_id_
                );
            }
        } else {
            ARCTICDB_DEBUG(
                    log::symbol(), "Problematic symbol/version pair: {}@{}: cannot be found", symbol, reference_id
            );
            symbols.emplace_back(symbol, reference_id, reference_pair.second, ActionType::DELETE);
        }
    }
    std::sort(std::begin(symbols), std::end(symbols), [](const auto& l, const auto& r) {
        return l.stream_id_ < r.stream_id_;
    });
}

/// Merges journal entries (JournalEntryData map) with existing compacted or version-key symbols.
/// The map is read-only so it can be moved into LoadResult for later batch deletion.
CollectionType merge_existing_with_journal_map(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store,
        const JournalMapType& update_map, std::vector<SymbolListEntry>&& existing
) {
    auto existing_keys = std::move(existing);

    CollectionType symbols;
    std::map<StreamId, std::pair<VersionId, timestamp>> problematic_symbols;
    std::unordered_set<StreamId> seen_in_existing;
    const auto min_allowed_interval = ConfigsMap::instance()->get_int("SymbolList.MinIntervalNs", 100'000'000LL);

    for (auto& previous_entry : existing_keys) {
        const auto& stream_id = previous_entry.stream_id_;
        auto updated = update_map.find(stream_id);
        if (updated == std::end(update_map)) {
            if (previous_entry.action_ == ActionType::ADD)
                symbols.emplace_back(std::move(previous_entry));
            else
                util::check(
                        previous_entry.action_ == ActionType::DELETE,
                        "Unknown action type {} in symbol list",
                        static_cast<uint8_t>(previous_entry.action_)
                );
        } else {
            util::check(!updated->second.empty(), "Unexpected empty entry for symbol {}", updated->first);
            seen_in_existing.insert(stream_id);
            std::vector<SymbolEntryData> entries;
            entries.reserve(updated->second.size());
            for (const auto& ck : updated->second)
                entries.push_back(to_symbol_entry_data(ck));
            if (auto problematic_entry = is_problematic(previous_entry, entries, min_allowed_interval);
                problematic_entry) {
                problematic_symbols.try_emplace(
                        stream_id, std::make_pair(problematic_entry.reference_id(), problematic_entry.time())
                );
            } else {
                const auto last = to_symbol_entry_data(updated->second.back());
                symbols.emplace_back(updated->first, last.reference_id_, last.timestamp_, last.action_);
            }
        }
    }

    for (const auto& [symbol, ck_entries] : update_map) {
        if (seen_in_existing.count(symbol) > 0)
            continue;
        std::vector<SymbolEntryData> entries;
        entries.reserve(ck_entries.size());
        for (const auto& ck : ck_entries)
            entries.push_back(to_symbol_entry_data(ck));
        if (auto problematic_entry = is_problematic(entries, min_allowed_interval); problematic_entry) {
            problematic_symbols.try_emplace(symbol, problematic_entry.reference_id(), problematic_entry.time());
        } else {
            const auto last = to_symbol_entry_data(ck_entries.back());
            symbols.emplace_back(symbol, last.reference_id_, last.timestamp_, last.action_);
        }
    }

    resolve_problematic_symbols(version_map, store, problematic_symbols, symbols);
    return symbols;
}

LoadResult attempt_load(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store, SymbolListData& data,
        WillAttemptCompaction will_attempt_compaction
) {
    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Symbol list load attempt");
    const bool will_compact = will_attempt_compaction == WillAttemptCompaction::YES;
    auto journal = load_journal_streaming(store, data, will_attempt_compaction);

    LoadResult load_result;
    load_result.compaction_key_ = journal.compaction_key;
    load_result.total_key_count_ = journal.total_key_count;

    if (journal.compaction_key) {
        ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Loading symbols from symbol list keys");
        auto existing = read_from_storage(store, *journal.compaction_key);
        if (journal.update_map.empty()) {
            load_result.symbols_ =
                    CollectionType(std::make_move_iterator(existing.begin()), std::make_move_iterator(existing.end()));
        } else {
            load_result.symbols_ =
                    merge_existing_with_journal_map(version_map, store, journal.update_map, std::move(existing));
        }
    } else {
        ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Loading symbols from version keys");
        auto previous_entries = load_previous_from_version_keys(store, data, will_attempt_compaction);
        load_result.symbols_ =
                merge_existing_with_journal_map(version_map, store, journal.update_map, std::move(previous_entries));
        if (will_compact) {
            // Verify every journal symbol we'd delete corresponds to a symbol in the merged output.
            // Guards against silent data loss from merge bugs.
            std::unordered_set<StreamId> symbols_in_merge;
            for (const auto& entry : load_result.symbols_)
                symbols_in_merge.emplace(entry.stream_id_);
            for (const auto& [symbol, _] : journal.update_map)
                util::check(symbols_in_merge.count(symbol) > 0, "Would delete unseen symbol {}", symbol);
        }
    }

    if (will_compact) {
        load_result.old_compaction_keys_ = std::move(journal.compaction_keys);
        load_result.update_map_ = std::move(journal.update_map);
    }

    return load_result;
}

inline StreamDescriptor journal_stream_descriptor(ActionType action, const StreamId& id) {
    return util::variant_match(
            id,
            [action](const NumericId&) {
                return StreamDescriptor{stream_descriptor(
                        action_id(action), RowCountIndex(), {scalar_field(DataType::UINT64, "symbol")}
                )};
            },
            [action](const StringId&) {
                return StreamDescriptor{stream_descriptor(
                        action_id(action), RowCountIndex(), {scalar_field(DataType::UTF_DYNAMIC64, "symbol")}
                )};
            }
    );
}

void write_journal(
        const std::shared_ptr<Store>& store, const StreamId& symbol, ActionType action, VersionId reference_id
) {
    SegmentInMemory seg{journal_stream_descriptor(action, symbol)};

    IndexValue version_indicator;
    util::variant_match(
            symbol,
            [&seg, &version_indicator](const StringId& id) {
                seg.set_string(0, id);
                version_indicator = StringIndex{version_string};
            },
            [&seg, &version_indicator](const NumericId& id) {
                seg.set_scalar<uint64_t>(0, id);
                version_indicator = version_identifier;
            }
    );

    seg.end_row();
    store->write_sync(
            KeyType::SYMBOL_LIST, reference_id, action_id(action), IndexValue{symbol}, version_indicator, std::move(seg)
    );
}

void write_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol, VersionId reference_id) {
    write_journal(store, symbol, ActionType::ADD, reference_id);
}

void delete_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol, VersionId reference_id) {
    write_journal(store, symbol, ActionType::DELETE, reference_id);
}

void SymbolList::add_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol, VersionId reference_id) {
    write_symbol(store, symbol, reference_id);
}

void SymbolList::remove_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol, VersionId reference_id) {
    delete_symbol(store, symbol, reference_id);
}

void SymbolList::clear(const std::shared_ptr<Store>& store) {
    delete_all_keys_of_type(KeyType::SYMBOL_LIST, store, true);
}

StreamDescriptor add_symbol_stream_descriptor(const StreamId& stream_id, const StreamId& type_holder) {
    auto data_type = std::holds_alternative<StringId>(type_holder) ? DataType::ASCII_DYNAMIC64 : DataType::UINT64;
    return stream_descriptor(
            stream_id,
            RowCountIndex(),
            {scalar_field(data_type, "added_symbol"),
             scalar_field(DataType::UINT64, "added_reference_id"),
             scalar_field(DataType::NANOSECONDS_UTC64, "added_timestamp")}
    );
}

StreamDescriptor delete_symbol_stream_descriptor(const StreamId& stream_id, const StreamId& type_holder) {
    auto data_type = std::holds_alternative<StringId>(type_holder) ? DataType::ASCII_DYNAMIC64 : DataType::UINT64;
    return stream_descriptor(
            stream_id,
            RowCountIndex(),
            {scalar_field(data_type, "deleted_symbol"),
             scalar_field(DataType::UINT64, "deleted_reference_id"),
             scalar_field(DataType::NANOSECONDS_UTC64, "deleted_timestamp")}
    );
}

bool SymbolList::needs_compaction(const LoadResult& load_result) const {
    if (!load_result.compaction_key_) {
        log::version().debug("Symbol list: needs_compaction=[true] as no previous compaction");
        return true;
    }

    auto n_keys = static_cast<int64_t>(load_result.total_key_count_);
    if (auto fixed = ConfigsMap::instance()->get_int("SymbolList.MaxDelta")) {
        auto result = n_keys > *fixed;
        log::version().debug(
                "Symbol list: Fixed draw for compaction. needs_compaction=[{}] n_keys=[{}], MaxDelta=[{}]",
                result,
                n_keys,
                *fixed
        );
        return result;
    }

    int64_t min = ConfigsMap::instance()->get_int("SymbolList.MinCompactionThreshold", 300);
    int64_t max = ConfigsMap::instance()->get_int("SymbolList.MaxCompactionThreshold", 700);
    util::check(
            max >= min, "Bad configuration, min compaction threshold=[{}] > max compaction threshold=[{}]", min, max
    );

    uint32_t seed;
    if (data_.seed_ == 0) {
        seed = std::random_device{}();
    } else {
        seed = data_.seed_;
    }

    std::mt19937 gen = std::mt19937{seed};
    std::uniform_int_distribution<int64_t> distrib(min, max);
    auto draw = distrib(gen);
    auto result = n_keys > draw;
    log::version().debug(
            "Symbol list: Random draw for compaction. needs_compaction=[{}] n_keys=[{}], draw=[{}]",
            result,
            n_keys,
            draw
    );
    return result;
}

void write_symbol_at(
        const StreamId& type_holder, SegmentInMemory& list_segment, const SymbolListEntry& entry, position_t column
) {
    util::variant_match(
            type_holder,
            [&entry, &list_segment, column](const StringId&) {
                util::check(
                        std::holds_alternative<StringId>(entry.stream_id_),
                        "Cannot write string symbol name, existing symbols are numeric"
                );
                list_segment.set_string(column, std::get<StringId>(entry.stream_id_));
            },
            [&entry, &list_segment, column](const NumericId&) {
                util::check(
                        std::holds_alternative<NumericId>(entry.stream_id_),
                        "Cannot write numeric symbol name, existing symbols are strings"
                );
                list_segment.set_scalar(column, std::get<NumericId>(entry.stream_id_));
            }
    );
}

void write_entry(const StreamId& type_holder, SegmentInMemory& segment, const SymbolListEntry& entry) {
    write_symbol_at(type_holder, segment, entry, 0);
    segment.set_scalar(1, entry.reference_id_);
    segment.set_scalar(2, entry.timestamp_);
    segment.end_row();
}

SegmentInMemory write_entries_to_symbol_segment(
        const StreamId& stream_id, const StreamId& type_holder, const CollectionType& symbols
) {
    SegmentInMemory added_segment{add_symbol_stream_descriptor(stream_id, type_holder)};
    SegmentInMemory deleted_segment{delete_symbol_stream_descriptor(stream_id, type_holder)};

    for (const auto& entry : symbols) {
        if (entry.action_ == ActionType::ADD)
            write_entry(type_holder, added_segment, entry);
        else
            write_entry(type_holder, deleted_segment, entry);
    }

    if (!deleted_segment.empty()) {
        for (auto col = 0UL; col < deleted_segment.descriptor().fields().size(); ++col) {
            const auto& field = deleted_segment.descriptor().fields(col);
            added_segment.add_column(
                    FieldRef{field.type(), field.name()}, deleted_segment.column_ptr(static_cast<position_t>(col))
            );
        }
        util::check(
                added_segment.descriptor().field_count() == 6,
                "Unexpected number of compacted symbol fields: {}",
                added_segment.descriptor().field_count()
        );

        auto& src = added_segment.column(static_cast<position_t>(3)).data().buffer();
        CursoredBuffer<ChunkedBuffer> cursor{src.bytes(), AllocationType::DYNAMIC};
        merge_string_column(src, deleted_segment.string_pool_ptr(), added_segment.string_pool_ptr(), cursor, false);
        std::swap(src, cursor.buffer());
    }

    util::check(
            added_segment.row_count() == static_cast<size_t>(added_segment.column(0).row_count()),
            "Segment row_count should match initial column row_count {} != {}",
            added_segment.row_count(),
            added_segment.column(0).row_count()
    );

    return added_segment;
}

SegmentInMemory create_empty_segment(const StreamId& stream_id) {
    SegmentInMemory output{StreamDescriptor{stream_id}};
    return output;
}

VariantKey write_symbols(
        const std::shared_ptr<Store>& store, CollectionType symbols, const StreamId& stream_id,
        const StreamId& type_holder
) {
    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Writing {} symbols to symbol list cache", symbols.size());

    SegmentInMemory segment;
    if (std::none_of(std::begin(symbols), std::end(symbols), [](const auto& entry) {
            return entry.action_ == ActionType::ADD;
        })) {
        segment = create_empty_segment(stream_id);
    } else {
        segment = write_entries_to_symbol_segment(stream_id, type_holder, symbols);
    }

    ARCTICDB_RUNTIME_DEBUG(
            log::symbol(), "Writing symbol segment with stream id {} and {} rows", stream_id, segment.row_count()
    );
    return store->write_sync(KeyType::SYMBOL_LIST, 0, stream_id, NumericIndex{0}, NumericIndex{0}, std::move(segment));
}

void delete_keys(const std::shared_ptr<Store>& store, std::vector<AtomKey>&& remove, const AtomKey& exclude) {
    auto to_remove = std::move(remove);
    std::vector<VariantKey> variant_keys;
    variant_keys.reserve(to_remove.size());
    for (auto& atom_key : to_remove) {
        // Corner case: if the newly written Compaction key (exclude) has the same timestamp as an existing one
        // (e.g. when a previous compaction round failed in the deletion step), we don't want to delete the former
        if (atom_key != exclude)
            to_remove.emplace_back(std::move(atom_key));
    }

    store->remove_keys_sync(variant_keys);
}

bool has_recent_compaction(const std::shared_ptr<Store>& store, const std::optional<AtomKey>& compaction_key) {
    bool found_last = false;
    bool has_newer = false;

    if (compaction_key) {
        // We found a compaction key during load. Re-scan to check two things:
        // 1. The key we saw still exists (another process may have already replaced it).
        // 2. A newer compaction key has appeared since we loaded (another process compacted concurrently).
        store->iterate_type(
                KeyType::SYMBOL_LIST,
                [&found_last, &has_newer, &last_compaction_key = *compaction_key](const VariantKey& key) {
                    const auto& atom = to_atom(key);
                    if (atom == last_compaction_key)
                        found_last = true;
                    if (atom.creation_ts() > last_compaction_key.creation_ts())
                        has_newer = true;
                },
                std::get<std::string>(compaction_id)
        );
    } else {
        // No compaction key was present during load. Check whether one has appeared since
        // (another process may have compacted while we were loading).
        store->iterate_type(
                KeyType::SYMBOL_LIST,
                [&has_newer](const VariantKey&) { has_newer = true; },
                std::get<std::string>(compaction_id)
        );
    }

    // Abort our compaction if our key was replaced (!found_last) or a newer one exists (has_newer).
    return (compaction_key && !found_last) || has_newer;
}

size_t SymbolList::compact(const std::shared_ptr<Store>& store) {
    auto version_map = data_.version_map_;
    LoadResult load_result = ExponentialBackoff<StorageException>(100, 2000).go([this, &version_map, &store]() {
        return attempt_load(version_map, store, data_, WillAttemptCompaction::YES);
    });
    auto num_symbol_list_keys = load_result.total_key_count_;

    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Forcing compaction. Obtaining lock...");
    StorageLock lock{StringId{CompactionLockName}};
    lock.lock_timeout(store, 10000);
    OnExit x([&lock, &store] { lock.unlock(store); });

    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Running compaction under lock");
    compact_internal(store, load_result);
    return num_symbol_list_keys;
}

void SymbolList::compact_internal(const std::shared_ptr<Store>& store, LoadResult& load_result) const {
    if (has_recent_compaction(store, load_result.compaction_key_)) {
        // legacy arcticc symbol list entries don't get correctly listed when doing `iterate_type`, so can mess
        // up racing symbol list compaction detection.
        ARCTICDB_RUNTIME_DEBUG(
                log::symbol(),
                "Symbol list compaction will be skipped: either a concurrent compaction was detected "
                "or there are legacy arcticc symbol list entries that cannot be verified."
        );
        return;
    }

    auto written = write_symbols(store, std::move(load_result.symbols_), compaction_id, data_.type_holder_);
    const auto& written_key = std::get<AtomKey>(written);

    // Delete old compaction keys (typically 0–1 entries; exclude the newly written one).
    auto& old_ck = load_result.old_compaction_keys_;
    old_ck.erase(
            std::remove_if(
                    old_ck.begin(),
                    old_ck.end(),
                    [&written_key](const VariantKey& vk) { return to_atom(vk) == written_key; }
            ),
            old_ck.end()
    );
    if (!old_ck.empty())
        store->remove_keys_sync(std::move(old_ck));

    // Reconstruct and delete journal keys in batches from the JournalMapType, freeing each symbol's
    // entries as they are processed. Journal keys can never equal written_key (different id field).
    static constexpr size_t kBatchSize = 10'000;
    std::vector<VariantKey> batch;
    batch.reserve(kBatchSize);
    for (auto it = load_result.update_map_.begin(); it != load_result.update_map_.end();) {
        const auto& [symbol, ck_entries] = *it;
        for (const auto& ck : ck_entries) {
            batch.emplace_back(atom_key_from_journal_entry(symbol, ck));
            if (batch.size() == kBatchSize) {
                store->remove_keys_sync(std::move(batch));
                batch.clear();
                batch.reserve(kBatchSize);
            }
        }
        it = load_result.update_map_.erase(it);
    }
    if (!batch.empty())
        store->remove_keys_sync(std::move(batch));
}

} // namespace arcticdb

namespace std {
template<>
struct hash<arcticdb::ActionType> {
    size_t operator()(arcticdb::ActionType at) const { return std::hash<int>{}(static_cast<int>(at)); }
};
} // namespace std
