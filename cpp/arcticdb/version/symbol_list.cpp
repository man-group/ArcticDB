/* Copyright 2023 Man Group Operations Limited
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

using MapType = std::unordered_map<StreamId, std::vector<SymbolEntryData>>;
using Compaction = std::vector<AtomKey>::const_iterator;
using MaybeCompaction = std::optional<Compaction>;
using CollectionType = std::vector<SymbolListEntry>;

constexpr std::string_view version_string = "_v2_";
constexpr NumericIndex version_identifier = std::numeric_limits<NumericIndex>::max();

SymbolListData::SymbolListData(std::shared_ptr<VersionMap> version_map, StreamId type_indicator, uint32_t seed) :
    type_holder_(std::move(type_indicator)),
    seed_(seed),
    version_map_(std::move(version_map)) {}

struct LoadResult {
    std::vector<AtomKey> symbol_list_keys_;
    MaybeCompaction maybe_previous_compaction;
    CollectionType symbols_;
    timestamp timestamp_ = 0L;

    std::vector<AtomKey>&& detach_symbol_list_keys() { return std::move(symbol_list_keys_); }
};

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
        const std::shared_ptr<Store>& store, SymbolListData& data
) {
    std::vector<StreamId> stream_ids;
    store->iterate_type(KeyType::VERSION_REF, [&data, &stream_ids](const auto& key) {
        auto id = variant_key_id(key);
        stream_ids.push_back(id);

        if (stream_ids.size() == warning_threshold() && !data.warned_expected_slowdown_) {
            log::symbol().warn(
                    "No compacted symbol list cache found. "
                    "`list_symbols` may take longer than expected. \n\n"
                    "See here for more information: "
                    "https://docs.arcticdb.io/latest/technical/on_disk_storage/#symbol-list-caching\n\n"
                    "To resolve, run `list_symbols` through to completion to compact the symbol list cache.\n"
                    "Note: This warning will only appear once.\n"
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

std::vector<AtomKey> get_all_symbol_list_keys(const std::shared_ptr<StreamSource>& store, SymbolListData& data) {
    std::vector<AtomKey> output;
    uint64_t uncompacted_keys_found = 0;
    store->iterate_type(KeyType::SYMBOL_LIST, [&data, &output, &uncompacted_keys_found](auto&& key) {
        auto atom_key = to_atom(std::forward<decltype(key)>(key));
        if (atom_key.id() != compaction_id) {
            uncompacted_keys_found++;
        }
        if (uncompacted_keys_found == warning_threshold() && !data.warned_expected_slowdown_) {
            log::symbol().warn("`list_symbols` may take longer than expected as there have been many modifications "
                               "since `list_symbols` was last called. \n\n"
                               "See here for more information: "
                               "https://docs.arcticdb.io/latest/technical/on_disk_storage/#symbol-list-caching\n\n"
                               "To resolve, run `list_symbols` through to completion frequently.\n"
                               "Note: This warning will only appear once.\n");

            data.warned_expected_slowdown_ = true;
        }

        output.push_back(atom_key);
    });

    std::sort(output.begin(), output.end(), [](const AtomKey& left, const AtomKey& right) {
        // Some very old symbol list keys have a non-zero version number, but with different semantics to the new style,
        // so ignore it. See arcticdb-man#116. Most old style symbol list keys have version ID 0 anyway.
        auto left_version = is_new_style_key(left) ? left.version_id() : 0;
        auto right_version = is_new_style_key(right) ? right.version_id() : 0;
        return std::tie(left.start_index(), left_version, left.creation_ts()) <
               std::tie(right.start_index(), right_version, right.creation_ts());
    });
    return output;
}

MaybeCompaction last_compaction(const std::vector<AtomKey>& keys) {
    auto pos = std::find_if(keys.rbegin(), keys.rend(), [](const auto& key) { return key.id() == compaction_id; });

    if (pos == keys.rend()) {
        return std::nullopt;
    } else {
        return {(pos + 1).base()}; // reverse_iterator -> forward itr has an offset of 1 per docs
    }
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

MapType load_journal_keys(const std::vector<AtomKey>& keys) {
    MapType map;
    for (const auto& key : keys) {
        const auto& action_id = key.id();
        if (action_id == compaction_id)
            continue;

        const auto& symbol = key.start_index();
        const auto version_id = is_new_style_key(key) ? key.version_id() : unknown_version_id;
        const auto timestamp = key.creation_ts();
        ActionType action = std::get<StringId>(action_id) == DeleteSymbol ? ActionType::DELETE : ActionType::ADD;
        map[symbol].emplace_back(version_id, timestamp, action);
    }
    return map;
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

CollectionType merge_existing_with_journal_keys(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store,
        const std::vector<AtomKey>& keys, std::vector<SymbolListEntry>&& existing
) {
    auto existing_keys = std::move(existing);
    auto update_map = load_journal_keys(keys);

    CollectionType symbols;
    std::map<StreamId, std::pair<VersionId, timestamp>> problematic_symbols;
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
            if (auto problematic_entry = is_problematic(previous_entry, updated->second, min_allowed_interval);
                problematic_entry) {
                problematic_symbols.try_emplace(
                        stream_id, std::make_pair(problematic_entry.reference_id(), problematic_entry.time())
                );
            } else {
                const auto& last_entry = updated->second.rbegin();
                symbols.emplace_back(
                        updated->first, last_entry->reference_id_, last_entry->timestamp_, last_entry->action_
                );
            }
            update_map.erase(updated);
        }
    }

    for (const auto& [symbol, entries] : update_map) {
        ARCTICDB_DEBUG(log::symbol(), "{} {}", symbol, entries);
        if (auto problematic_entry = is_problematic(entries, min_allowed_interval); problematic_entry) {
            problematic_symbols.try_emplace(symbol, problematic_entry.reference_id(), problematic_entry.time());
        } else {
            const auto& last_entry = entries.rbegin();
            symbols.emplace_back(symbol, last_entry->reference_id_, last_entry->timestamp_, last_entry->action_);
        }
    }

    if (!problematic_symbols.empty()) {
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

    return symbols;
}

CollectionType load_from_symbol_list_keys(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store,
        const std::vector<AtomKey>& keys, const Compaction& compaction
) {
    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Loading symbols from symbol list keys");

    auto previous_compaction = read_from_storage(store, *compaction);
    return merge_existing_with_journal_keys(version_map, store, keys, std::move(previous_compaction));
}

CollectionType load_from_version_keys(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store,
        const std::vector<AtomKey>& keys, SymbolListData& data
) {
    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Loading symbols from version keys");
    auto previous_entries = load_previous_from_version_keys(store, data);
    return merge_existing_with_journal_keys(version_map, store, keys, std::move(previous_entries));
}

LoadResult attempt_load(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store, SymbolListData& data
) {
    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Symbol list load attempt");
    LoadResult load_result;
    load_result.symbol_list_keys_ = get_all_symbol_list_keys(store, data);
    load_result.maybe_previous_compaction = last_compaction(load_result.symbol_list_keys_);

    if (load_result.maybe_previous_compaction)
        load_result.symbols_ = load_from_symbol_list_keys(
                version_map, store, load_result.symbol_list_keys_, *load_result.maybe_previous_compaction
        );
    else {
        load_result.symbols_ = load_from_version_keys(version_map, store, load_result.symbol_list_keys_, data);
        std::unordered_set<StreamId> keys_in_versions;
        for (const auto& entry : load_result.symbols_)
            keys_in_versions.emplace(entry.stream_id_);

        for (const auto& key : load_result.symbol_list_keys_)
            util::check(
                    keys_in_versions.find(StreamId{std::get<StringIndex>(key.start_index())}) != keys_in_versions.end(),
                    "Would delete unseen key {}",
                    key
            );
    }

    load_result.timestamp_ = store->current_timestamp();
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
    if (!load_result.maybe_previous_compaction) {
        log::version().debug("Symbol list: needs_compaction=[true] as no previous compaction");
        return true;
    }

    auto n_keys = static_cast<int64_t>(load_result.symbol_list_keys_.size());
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
        const std::shared_ptr<Store>& store, const CollectionType& symbols, const StreamId& stream_id,
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

std::vector<Store::RemoveKeyResultType> delete_keys(
        const std::shared_ptr<Store>& store, std::vector<AtomKey>&& remove, const AtomKey& exclude
) {
    auto to_remove = std::move(remove);
    std::vector<VariantKey> variant_keys;
    variant_keys.reserve(to_remove.size());
    for (auto& atom_key : to_remove) {
        // Corner case: if the newly written Compaction key (exclude) has the same timestamp as an existing one
        // (e.g. when a previous compaction round failed in the deletion step), we don't want to delete the former
        if (atom_key != exclude)
            variant_keys.emplace_back(atom_key);
    }

    return store->remove_keys_sync(variant_keys);
}

bool has_recent_compaction(
        const std::shared_ptr<Store>& store,
        const std::optional<std::vector<AtomKey>::const_iterator>& maybe_previous_compaction
) {
    bool found_last = false;
    bool has_newer = false;

    if (maybe_previous_compaction.has_value()) {
        // Symbol list keys source
        store->iterate_type(
                KeyType::SYMBOL_LIST,
                [&found_last,
                 &has_newer,
                 &last_compaction_key = *maybe_previous_compaction.value()](const VariantKey& key) {
                    const auto& atom = to_atom(key);
                    if (atom == last_compaction_key)
                        found_last = true;
                    if (atom.creation_ts() > last_compaction_key.creation_ts())
                        has_newer = true;
                },
                std::get<std::string>(compaction_id)
        );
    } else {
        // Version keys source
        store->iterate_type(
                KeyType::SYMBOL_LIST,
                [&has_newer](const VariantKey&) { has_newer = true; },
                std::get<std::string>(compaction_id)
        );
    }

    return (maybe_previous_compaction && !found_last) || has_newer;
}

std::set<StreamId> SymbolList::load(
        const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store, bool no_compaction
) {
    LoadResult load_result = ExponentialBackoff<StorageException>(100, 2000).go([this, &version_map, &store]() {
        return attempt_load(version_map, store, data_);
    });

    if (!no_compaction && needs_compaction(load_result)) {
        ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Compaction necessary. Obtaining lock...");
        try {
            if (StorageLock lock{StringId{CompactionLockName}}; lock.try_lock(store)) {
                OnExit x([&lock, &store] { lock.unlock(store); });

                ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Checking whether we still need to compact under lock");
                compact_internal(store, load_result);
            } else {
                ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Not compacting the symbol list due to lock contention");
            }
        } catch (const storage::LibraryPermissionException& ex) {
            // Note: this only reflects AN's permission check and is not thrown by the Storage
            ARCTICDB_RUNTIME_DEBUG(
                    log::symbol(), "Not compacting the symbol list due to lack of permission", ex.what()
            );
        } catch (const std::exception& ex) {
            log::symbol().warn("Ignoring error while trying to compact the symbol list: {}", ex.what());
        }
    }

    std::set<StreamId> output;
    for (const auto& entry : load_result.symbols_) {
        if (entry.action_ == ActionType::ADD)
            output.insert(entry.stream_id_);
    }

    return output;
}

size_t SymbolList::compact(const std::shared_ptr<Store>& store) {
    auto version_map = data_.version_map_;
    LoadResult load_result = ExponentialBackoff<StorageException>(100, 2000).go([this, &version_map, &store]() {
        return attempt_load(version_map, store, data_);
    });
    auto num_symbol_list_keys = load_result.symbol_list_keys_.size();

    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Forcing compaction. Obtaining lock...");
    StorageLock lock{StringId{CompactionLockName}};
    lock.lock_timeout(store, 10000);
    OnExit x([&lock, &store] { lock.unlock(store); });

    ARCTICDB_RUNTIME_DEBUG(log::symbol(), "Checking whether we still need to compact under lock");
    compact_internal(store, load_result);
    return num_symbol_list_keys;
}

void SymbolList::compact_internal(const std::shared_ptr<Store>& store, LoadResult& load_result) const {
    if (!has_recent_compaction(store, load_result.maybe_previous_compaction)) {
        auto written = write_symbols(store, load_result.symbols_, compaction_id, data_.type_holder_);
        delete_keys(store, load_result.detach_symbol_list_keys(), std::get<AtomKey>(written));
    }
}

} // namespace arcticdb

namespace std {
template<>
struct hash<arcticdb::ActionType> {
    size_t operator()(arcticdb::ActionType at) const { return std::hash<int>{}(static_cast<int>(at)); }
};
} // namespace std
