/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/entity/types.hpp>
#include <arcticdb/async/base_task.hpp>
#include <arcticdb/version/version_map.hpp>
#include <folly/futures/Future.h>
#include <set>

namespace arcticdb {

struct LoadResult;

struct SymbolListData {
    StreamId type_holder_;
    uint32_t seed_;
    std::shared_ptr<VersionMap> version_map_;
    std::atomic<bool> warned_expected_slowdown_ = false;

    explicit SymbolListData(
            std::shared_ptr<VersionMap> version_map, StreamId type_indicator = StringId(), uint32_t seed = 0
    );
};

constexpr std::string_view CompactionId = "__symbols__";
constexpr std::string_view CompactionLockName = "SymbolListCompactionLock";
constexpr std::string_view AddSymbol = "__add__";
constexpr std::string_view DeleteSymbol = "__delete__";

constexpr VersionId unknown_version_id = std::numeric_limits<VersionId>::max();

enum class ActionType : uint8_t { ADD, DELETE };

inline StreamId action_id(ActionType action) {
    switch (action) {
    case ActionType::ADD:
        return StringId{AddSymbol};
    case ActionType::DELETE:
        return StringId{DeleteSymbol};
    default:
        util::raise_rte("Unknown action type {}", static_cast<uint8_t>(action));
    }
}

struct SymbolEntryData {
    util::MagicNum<'S', 'd', 'a', 't'> magic_;
    entity::VersionId reference_id_;
    timestamp timestamp_;
    ActionType action_;

    SymbolEntryData(entity::VersionId reference_id, timestamp time, ActionType action) :
        reference_id_(reference_id),
        timestamp_(time),
        action_(action) {}

    void verify() const { magic_.check(); }
};

inline bool operator==(const SymbolEntryData& l, const SymbolEntryData& r) {
    return l.reference_id_ == r.reference_id_ && l.action_ == r.action_;
}

struct SymbolListEntry : public SymbolEntryData {
    StreamId stream_id_;

    SymbolListEntry(StreamId stream_id, entity::VersionId reference_id, timestamp reference_time, ActionType action) :
        SymbolEntryData(reference_id, reference_time, action),
        stream_id_(std::move(stream_id)) {}
};

struct ProblematicResult {
    std::optional<SymbolEntryData> problem_;
    bool contains_unknown_reference_ids_ = false;

    explicit ProblematicResult(const SymbolEntryData& data) : problem_(data) {}

    explicit ProblematicResult(bool old_style_refs) : contains_unknown_reference_ids_(old_style_refs) {}

    [[nodiscard]] VersionId reference_id() const { return problem_->reference_id_; }

    [[nodiscard]] timestamp time() const { return problem_->timestamp_; }

    [[nodiscard]] ActionType action() const { return problem_->action_; }

    explicit operator bool() const { return static_cast<bool>(problem_); }
};

inline ProblematicResult cannot_determine_validity() { return ProblematicResult{true}; }

inline ProblematicResult not_a_problem() { return ProblematicResult{false}; }

struct SymbolVectorResult {
    ProblematicResult problematic_result_;
    bool all_same_version_ = false;
    bool all_same_action_ = false;
    size_t last_id_count_ = 0;
};

ProblematicResult is_problematic(
        const SymbolListEntry& existing, const std::vector<SymbolEntryData>& updated, timestamp min_allowed_interval
);

ProblematicResult is_problematic(const std::vector<SymbolEntryData>& updated, timestamp min_allowed_interval);

class SymbolList {
    SymbolListData data_;

  public:
    explicit SymbolList(
            std::shared_ptr<VersionMap> version_map, StreamId type_indicator = StringId(), uint32_t seed = 0
    ) :
        data_(std::move(version_map), std::move(type_indicator), seed) {}

    std::set<StreamId> load(
            const std::shared_ptr<VersionMap>& version_map, const std::shared_ptr<Store>& store, bool no_compaction
    );

    std::vector<StreamId> get_symbols(const std::shared_ptr<Store>& store, bool no_compaction = false) {
        auto symbols = load(data_.version_map_, store, no_compaction);
        return {std::make_move_iterator(symbols.begin()), std::make_move_iterator(symbols.end())};
    }

    std::set<StreamId> get_symbol_set(const std::shared_ptr<Store>& store) {
        return load(data_.version_map_, store, false);
    }

    size_t compact(const std::shared_ptr<Store>& store);

    static void add_symbol(const std::shared_ptr<Store>& store, const StreamId& symbol, entity::VersionId reference_id);

    static void remove_symbol(
            const std::shared_ptr<Store>& store, const StreamId& symbol, entity::VersionId reference_id
    );

    static void clear(const std::shared_ptr<Store>& store);

  private:
    void compact_internal(const std::shared_ptr<Store>& store, LoadResult& load_result) const;

    [[nodiscard]] bool needs_compaction(const LoadResult& load_result) const;
};

std::vector<Store::RemoveKeyResultType> delete_keys(
        const std::shared_ptr<Store>& store, std::vector<AtomKey>&& remove, const AtomKey& exclude
);

struct WriteSymbolTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    std::shared_ptr<SymbolList> symbol_list_;
    const StreamId stream_id_;
    const entity::VersionId reference_id_;

    WriteSymbolTask(
            std::shared_ptr<Store> store, std::shared_ptr<SymbolList> symbol_list, StreamId stream_id,
            entity::VersionId reference_id
    ) :
        store_(std::move(store)),
        symbol_list_(std::move(symbol_list)),
        stream_id_(std::move(stream_id)),
        reference_id_(reference_id) {}

    folly::Future<folly::Unit> operator()() {
        SymbolList::add_symbol(store_, stream_id_, reference_id_);
        return folly::Unit{};
    }
};

struct DeleteSymbolTask : async::BaseTask {
    const std::shared_ptr<Store> store_;
    const std::shared_ptr<SymbolList> symbol_list_;
    const StreamId& stream_id_;
    const entity::VersionId reference_id_;

    DeleteSymbolTask(
            std::shared_ptr<Store> store, std::shared_ptr<SymbolList> symbol_list, const StreamId& stream_id,
            entity::VersionId reference_id
    ) :
        store_(std::move(store)),
        symbol_list_(std::move(symbol_list)),
        stream_id_(stream_id),
        reference_id_(reference_id) {}

    folly::Future<folly::Unit> operator()() {
        SymbolList::remove_symbol(store_, stream_id_, reference_id_);
        return folly::Unit{};
    }
};

} // namespace arcticdb

namespace fmt {

template<>
struct formatter<arcticdb::ActionType> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::ActionType a, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", a == arcticdb::ActionType::ADD ? "ADD" : "DELETE");
    }
};

template<>
struct formatter<arcticdb::SymbolEntryData> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::SymbolEntryData& s, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "[{},{}@{}]", s.reference_id_, s.action_, s.timestamp_);
    }
};

} // namespace fmt
