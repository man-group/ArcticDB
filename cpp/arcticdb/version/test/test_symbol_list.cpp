/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h> // Included in gtest 1.10

#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>
#include <arcticdb/version/test/symbol_list_backwards_compat.hpp>

#include <shared_mutex>

#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <arcticdb/async/task_scheduler.hpp>

using namespace arcticdb;
using namespace folly;
using namespace arcticdb::entity;
using namespace arcticdb::action_factories;
using namespace ::testing;

MAKE_GTEST_FMT(FailureType, "{}")

static const StorageFailureSimulator::ParamActionSequence RAISE_ONCE = {fault(), no_op};
static const StorageFailureSimulator::ParamActionSequence RAISE_ON_2ND_CALL = {no_op, fault(), no_op};

namespace aa = arcticdb::async;
namespace as = arcticdb::storage;

/* === Tests overview ===
 * Test each below for both the symbol list source and the version map source:
 * 1. load_internal_with_retry (SymbolListWithReadFailures suite)
 * 2. no_compaction (after we ported the permissions)
 * 3. Step 2 (Need to update the stored symbol list?) condition (implicit in most tests)
 * 4. Step 3 (Get the compaction lock) failures (SymbolListWith*Failures where the sequence affected locks)
 * 5. Step 4 Compaction keys after lock (SymbolListRace)
 * 6. Step 5 (SymbolListWithWriteFailures)
 * 7. Return value (checked in most tests)
 */

struct SymbolListSuite : Test {
    static inline spdlog::level::level_enum saved = log::version().level();

    static void SetUpTestSuite() {
        Test::SetUpTestSuite(); // Fail compilation when the name changes in a later version

        saved = log::version().level();
        log::version().set_level(spdlog::level::debug);
    }

    static void TearDownTestSuite() {
        Test::TearDownTestSuite();
        log::version().set_level(saved);
        StorageFailureSimulator::reset();
    }

    void SetUp() override {
        if (StorageFailureSimulator::instance()->configured()) {
            StorageFailureSimulator::reset();
        }
    }

    void TearDown() override { ConfigsMap::instance()->unset_int("SymbolList.MaxDelta"); }

    const StreamId symbol_1{"aaa"};
    const StreamId symbol_2{"bbb"};
    const StreamId symbol_3{"ccc"};

    std::shared_ptr<InMemoryStore> store_ = std::make_shared<InMemoryStore>();
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::unique_ptr<SymbolList> symbol_list_ = std::make_unique<SymbolList>(version_map_);

    // Need at least one compaction key to avoid using the version keys as source
    void write_initial_compaction_key() const {
        log::version().set_level(spdlog::level::warn);
        symbol_list_->load(version_map_, store_, false);
        log::version().set_level(spdlog::level::debug);
    }

    // The max_delta_ can be set at construction time only, so to change requires a new instance:
    void override_max_delta(int64_t size) {
        ConfigsMap::instance()->set_int("SymbolList.MaxDelta", size);
        symbol_list_ = std::make_unique<SymbolList>(version_map_);
    }

    [[nodiscard]] auto get_symbol_list_keys(const std::string& prefix = "") const {
        std::vector<VariantKey> keys;
        store_->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](const VariantKey& k) { keys.push_back(k); }, prefix);
        return keys;
    }
};

using FailSimParam = StorageFailureSimulator::Params;

/** Adds storage failure cases to some tests. */
struct SymbolListWithReadFailures : SymbolListSuite, testing::WithParamInterface<FailSimParam> {
    static void setup_failure_sim_if_any() { StorageFailureSimulator::instance()->configure(GetParam()); }
};

TEST_P(SymbolListWithReadFailures, FromSymbolListSource) {
    write_initial_compaction_key();
    setup_failure_sim_if_any();

    SymbolList::add_symbol(store_, symbol_1, 0);
    SymbolList::add_symbol(store_, symbol_2, 1);

    std::vector<StreamId> copy = symbol_list_->get_symbols(store_, true);

    ASSERT_EQ(copy.size(), 2) << fmt::format("got {}", copy);
    ASSERT_EQ(copy[0], StreamId{"aaa"});
    ASSERT_EQ(copy[1], StreamId{"bbb"});
}

TEST_F(SymbolListSuite, Persistence) {
    write_initial_compaction_key();

    SymbolList::add_symbol(store_, symbol_1, 0);
    SymbolList::add_symbol(store_, symbol_2, 1);

    SymbolList output_list{version_map_};
    std::vector<StreamId> symbols = output_list.get_symbols(store_, true);

    ASSERT_EQ(symbols.size(), 2) << fmt::format("got {}", symbols);
    ASSERT_EQ(symbols[0], StreamId{symbol_1});
    ASSERT_EQ(symbols[1], StreamId{symbol_2});
}

TEST_P(SymbolListWithReadFailures, VersionMapSource) {

    auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(4).end_index(5).build(
            symbol_3, KeyType::TABLE_INDEX
    );

    auto key2 = atom_key_builder().version_id(2).creation_ts(3).content_hash(4).start_index(5).end_index(6).build(
            symbol_1, KeyType::TABLE_INDEX
    );

    auto key3 = atom_key_builder().version_id(3).creation_ts(4).content_hash(5).start_index(6).end_index(7).build(
            symbol_2, KeyType::TABLE_INDEX
    );

    version_map_->write_version(store_, key1, std::nullopt);
    version_map_->write_version(store_, key2, key1);
    version_map_->write_version(store_, key3, key2);

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list_->get_symbols(store_);
    ASSERT_THAT(symbols, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));
}

INSTANTIATE_TEST_SUITE_P(
        , SymbolListWithReadFailures,
        Values(FailSimParam{}, // No failure
               FailSimParam{{FailureType::ITERATE, RAISE_ONCE}}, FailSimParam{{FailureType::READ, RAISE_ONCE}},
               FailSimParam{{FailureType::READ, {fault(), fault(), fault(), fault(), no_op}}},
               FailSimParam{{FailureType::READ, {fault(0.4), no_op}}}, // 40% chance of exception
               FailSimParam{{FailureType::ITERATE, RAISE_ONCE}, {FailureType::READ, {no_op, fault(), no_op}}})
);

TEST_F(SymbolListSuite, MultipleWrites) {
    write_initial_compaction_key();

    SymbolList::add_symbol(store_, symbol_1, 0);
    SymbolList::add_symbol(store_, symbol_2, 0);

    SymbolList another_instance{version_map_};
    SymbolList::add_symbol(store_, symbol_3, 0);

    std::vector<StreamId> copy = symbol_list_->get_symbols(store_, true);
    ASSERT_THAT(copy, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));

    std::vector<StreamId> symbols = another_instance.get_symbols(store_, true);
    ASSERT_THAT(symbols, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));
}

enum class CompactOutcome : uint8_t { NOT_WRITTEN = 0, WRITTEN, NOT_CLEANED_UP, UNKNOWN };

using WriteFailuresParams = std::tuple<FailSimParam, CompactOutcome>;

struct SymbolListWithWriteFailures : SymbolListSuite, testing::WithParamInterface<WriteFailuresParams> {
    CompactOutcome expected_outcome = CompactOutcome::UNKNOWN;

    void setup_failure_sim_if_any() {
        StorageFailureSimulator::instance()->configure(std::get<0>(GetParam()));
        expected_outcome = std::get<1>(GetParam());
    }

    void check_num_symbol_list_keys_match_expectation(const std::map<CompactOutcome, size_t>& size_by_outcome) const {
        auto keys = get_symbol_list_keys();
        ASSERT_THAT(keys, SizeIs(size_by_outcome.at(expected_outcome)));
    }
};

TEST_P(SymbolListWithWriteFailures, SubsequentCompaction) {
    override_max_delta(500);
    write_initial_compaction_key();

    std::vector<StreamId> expected;
    for (size_t i = 0; i < 500; ++i) {
        auto symbol = fmt::format("sym{}", i);
        SymbolList::add_symbol(store_, symbol, 0);
        expected.emplace_back(symbol);
    }

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list_->get_symbols(store_, false);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

    // Extra 1 in 501 is the initial compaction key
    // NOT_CLEANED_UP case checks that deletion happens after the compaction key is written
    check_num_symbol_list_keys_match_expectation(
            {{{CompactOutcome::NOT_WRITTEN, 501}, {CompactOutcome::WRITTEN, 1}, {CompactOutcome::NOT_CLEANED_UP, 502}}}
    );

    // Retry:
    if (expected_outcome != CompactOutcome::WRITTEN) {
        symbols = symbol_list_->get_symbols(store_, false);
        ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

        check_num_symbol_list_keys_match_expectation(
                {{{CompactOutcome::NOT_WRITTEN, 1}, {CompactOutcome::NOT_CLEANED_UP, 1}}}
        );
    }
}

TEST_P(SymbolListWithWriteFailures, InitialCompact) {
    int64_t n = 20;
    override_max_delta(n);

    std::vector<StreamId> expected;
    for (int64_t i = 0; i < n + 1; ++i) {
        auto symbol = fmt::format("sym{}", i);
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(4).end_index(5).build(
                symbol, KeyType::TABLE_INDEX
        );
        version_map_->write_version(store_, key1, std::nullopt);
        expected.emplace_back(symbol);
    }

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list_->get_symbols(store_);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

    check_num_symbol_list_keys_match_expectation(
            {{{CompactOutcome::NOT_WRITTEN, 0}, {CompactOutcome::WRITTEN, 1}, {CompactOutcome::NOT_CLEANED_UP, 1}}}
    );
}

// If this test is timing out, it's likely that a deadlock is occurring
// A call is blocking on the CPU thread pool, which has only one thread in this test
TEST_F(SymbolListSuite, InitialCompactConcurent) {
    ConfigsMap::instance()->set_int("VersionStore.NumCPUThreads", 1);
    ConfigsMap::instance()->set_int("VersionStore.NumIOThreads", 1);
    async::TaskScheduler::reattach_instance();

    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();
    int64_t n = 20;

    override_max_delta(n);

    std::vector<StreamId> expected;
    for (int64_t i = 0; i < n; ++i) {
        auto symbol = fmt::format("sym{}", i);
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(4).end_index(5).build(
                symbol, KeyType::TABLE_INDEX
        );
        version_map_->write_version(store, key1, std::nullopt);
        expected.emplace_back(symbol);
    }

    // Go through the path without previous compaction
    folly::via(&async::cpu_executor(), [this, &store] { return symbol_list_->get_symbols(store); }).get();

    // Go through the path with previous compaction
    auto res = folly::via(&async::cpu_executor(), [this, &store] { return symbol_list_->get_symbols(store); }).get();

    const std::vector<StreamId>& symbols = res;
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));
}

INSTANTIATE_TEST_SUITE_P(
        , SymbolListWithWriteFailures,
        Values(WriteFailuresParams{FailSimParam{}, CompactOutcome::WRITTEN}, // No failures
               WriteFailuresParams{
                       {{FailureType::WRITE, RAISE_ONCE}},
                       CompactOutcome::NOT_WRITTEN
               }, // Interferes with locking
               WriteFailuresParams{{{FailureType::WRITE, RAISE_ON_2ND_CALL}}, CompactOutcome::NOT_WRITTEN},
               WriteFailuresParams{{{FailureType::DELETE, RAISE_ONCE}}, CompactOutcome::NOT_CLEANED_UP})
);

template<typename T, typename U>
std::optional<T> random_choice(const std::set<T>& set, U& gen) {
    if (set.empty()) {
        return std::nullopt;
    }
    std::uniform_int_distribution dis(0, static_cast<int>(std::min(size_t{0}, set.size() - 1)));
    size_t index = dis(gen);
    auto it = set.begin();
    std::advance(it, index);
    return *it;
}

class SymbolListState {
  public:
    explicit SymbolListState(std::shared_ptr<Store> store, std::shared_ptr<VersionMap> version_map) :
        store_(std::move(store)),
        version_map_(std::move(version_map)) {}

    void do_action(const std::shared_ptr<SymbolList>& symbol_list) {
        std::scoped_lock<std::mutex> lock{mutex_};
        std::uniform_int_distribution dis(0, 10);
        switch (auto action = dis(gen_); action) {
        case 0:
            do_delete();
            break;
        case 1:
            do_readd();
            break;
        case 2:
            do_list_symbols(symbol_list);
            break;
        default:
            do_add();
            break;
        }
        assert_invariants(symbol_list);
    };
    void do_list_symbols(const std::shared_ptr<SymbolList>& symbol_list) const {
        ASSERT_EQ(symbol_list->get_symbol_set(store_), live_symbols_);
    };

  private:
    void do_add() {
        std::uniform_int_distribution dis(0);
        int id = dis(gen_);
        auto symbol = fmt::format("sym-{}", id);
        if (live_symbols_.count(symbol) == 0 && deleted_symbols_.count(symbol) == 0) {
            live_symbols_.insert(symbol);
            SymbolList::add_symbol(store_, symbol, 0);
            versions_.try_emplace(symbol, 0);
            version_map_->write_version(
                    store_, atom_key_builder().version_id(0).build(symbol, KeyType::TABLE_INDEX), std::nullopt
            );
            ARCTICDB_DEBUG(log::version(), "Adding {}", symbol);
        }
    };
    void do_delete() {
        auto symbol = random_choice(live_symbols_, gen_);
        if (symbol.has_value()) {
            live_symbols_.erase(*symbol);
            deleted_symbols_.insert(*symbol);
            version_map_->delete_all_versions(store_, *symbol);
            SymbolList::remove_symbol(store_, *symbol, versions_[*symbol]);
            ARCTICDB_DEBUG(log::version(), "Removing {}", *symbol);
        }
    };
    void do_readd() {
        auto symbol = random_choice(deleted_symbols_, gen_);
        if (symbol.has_value()) {
            deleted_symbols_.erase(*symbol);
            live_symbols_.insert(*symbol);
            ++versions_[*symbol];
            SymbolList::add_symbol(store_, *symbol, versions_[*symbol]);
            version_map_->write_version(
                    store_,
                    atom_key_builder().version_id(versions_[*symbol]).build(*symbol, KeyType::TABLE_INDEX),
                    std::nullopt
            );
            ARCTICDB_DEBUG(log::version(), "Re-adding {}@{}", *symbol, versions_[*symbol]);
        }
    };
    void assert_invariants(const std::shared_ptr<SymbolList>& symbol_list) const {
        for (const auto& symbol : live_symbols_) {
            ASSERT_EQ(deleted_symbols_.count(symbol), 0);
        }
        for (const auto& symbol : deleted_symbols_) {
            ASSERT_EQ(live_symbols_.count(symbol), 0);
        }
        auto symbol_list_symbols_vec = symbol_list->get_symbols(store_, true);
        std::set<StreamId> symbol_list_symbols_set{symbol_list_symbols_vec.begin(), symbol_list_symbols_vec.end()};
        ASSERT_EQ(symbol_list_symbols_set, live_symbols_);
    }

    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_;
    std::mt19937 gen_ = std::mt19937{42};
    std::set<StreamId> live_symbols_;
    std::set<StreamId> deleted_symbols_;
    std::unordered_map<StreamId, size_t> versions_;
    std::mutex mutex_;
};

TEST_F(SymbolListSuite, AddDeleteReadd) {
    auto symbol_list = std::make_shared<SymbolList>(version_map_);
    write_initial_compaction_key();

    SymbolListState state(store_, version_map_);
    for (size_t i = 0; i < 10; ++i) {
        state.do_action(symbol_list);
    }
    state.do_list_symbols(symbol_list);
}

constexpr timestamp operator"" _s(unsigned long long t) { return static_cast<timestamp>(t) * 1000'000'000; }

bool result_equals(const ProblematicResult& got, const SymbolEntryData& expected) {
    return got.reference_id() == expected.reference_id_ && got.action() == expected.action_ &&
           got.time() == expected.timestamp_;
}

TEST(SymbolList, IsProblematic) {
    const timestamp min_interval = 100000;

    // No conflict - all adds
    std::vector<SymbolEntryData> vec1{{0, 0, ActionType::ADD}, {1, 1_s, ActionType::ADD}, {2, 2_s, ActionType::ADD}};
    auto result = is_problematic(vec1, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // No conflict with delete
    std::vector<SymbolEntryData> vec2{{0, 0, ActionType::ADD}, {1, 1_s, ActionType::ADD}, {1, 2_s, ActionType::DELETE}};
    result = is_problematic(vec2, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Version conflict not in the most recent is okay
    std::vector<SymbolEntryData> vec3{
            {0, 0, ActionType::ADD}, {0, 1_s, ActionType::DELETE}, {0, 2_s, ActionType::ADD}, {1, 3_s, ActionType::ADD}
    };
    result = is_problematic(vec3, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Version conflict with same action also fine
    std::vector<SymbolEntryData> vec4{
            {0, 0, ActionType::ADD},
            {0, 1_s, ActionType::DELETE},
            {1, 2_s, ActionType::ADD},
            {1, 3_s, ActionType::ADD},
            {1, 4_s, ActionType::ADD}
    };
    result = is_problematic(vec4, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Version conflict at end returns latest
    std::vector<SymbolEntryData> vec5{
            {0, 0, ActionType::ADD},
            {1, 1_s, ActionType::DELETE},
            {1, 2_s, ActionType::DELETE},
            {1, 3_s, ActionType::ADD}
    };
    result = is_problematic(vec5, min_interval);
    SymbolEntryData expected1{1, 3_s, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected1), true);

    // As above but with the first version
    std::vector<SymbolEntryData> vec6{
            {0, 1_s, ActionType::DELETE}, {0, 2_s, ActionType::DELETE}, {0, 3_s, ActionType::ADD}
    };
    result = is_problematic(vec6, min_interval);
    SymbolEntryData expected2{0, 3_s, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected2), true);

    // Timestamps too close but not more recent is okay
    std::vector<SymbolEntryData> vec7{{0, 0, ActionType::ADD}, {1, 100, ActionType::DELETE}, {2, 2_s, ActionType::ADD}};
    result = is_problematic(vec7, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Timestamp clash in most recent returns latest entry
    std::vector<SymbolEntryData> vec8{
            {0, 0, ActionType::ADD}, {0, 1_s, ActionType::DELETE}, {0, 1_s + 100, ActionType::ADD}
    };
    result = is_problematic(vec8, min_interval);
    SymbolEntryData expected3{0, 1_s + 100, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected3), true);

    // Contains unknown reference ids
    std::vector<SymbolEntryData> vec9{
            {0, 0, ActionType::ADD}, {unknown_version_id, 1_s, ActionType::DELETE}, {2, 2_s, ActionType::ADD}
    };
    result = is_problematic(vec9, min_interval);
    ASSERT_EQ(result.contains_unknown_reference_ids_, true);
}

TEST(SymbolList, IsProblematicWithStored) {
    const timestamp min_interval = 100000;

    // No conflict
    SymbolListEntry entry1{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec1{{1, 1_s, ActionType::ADD}, {2, 2_s, ActionType::ADD}, {3, 3_s, ActionType::ADD}};
    auto result = is_problematic(entry1, vec1, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // No conflict with delete
    SymbolListEntry entry2{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec2{
            {0, 1_s, ActionType::DELETE},
            {1, 2_s, ActionType::ADD},
            {1, 3_s, ActionType::DELETE},
            {2, 4_s, ActionType::ADD},
            {2, 5_s, ActionType::ADD}
    };
    result = is_problematic(entry2, vec2, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Conflict between stored and update, but not most recent is okay
    SymbolListEntry entry3{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec3{
            {0, 1_s, ActionType::ADD},
            {0, 2_s, ActionType::DELETE},
            {1, 3_s, ActionType::ADD},
            {2, 4_s, ActionType::ADD}
    };

    result = is_problematic(entry3, vec3, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Version conflict but same action is fine
    SymbolListEntry entry4{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec4{{0, 1_s, ActionType::ADD}, {0, 2_s, ActionType::ADD}};
    result = is_problematic(entry4, vec4, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Conflicting version in update returns most recent update
    SymbolListEntry entry5{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec5{
            {0, 1_s, ActionType::DELETE},
            {1, 2_s, ActionType::DELETE},
            {1, 3_s, ActionType::ADD},
            {1, 4_s, ActionType::DELETE}
    };
    result = is_problematic(entry5, vec5, min_interval);
    SymbolEntryData expected{1, 4_s, ActionType::DELETE};
    ASSERT_EQ(result_equals(result, expected), true);

    // Conflict exists but there is an old-style symbol list key
    SymbolListEntry entry6{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec6{
            {unknown_version_id, 1_s, ActionType::ADD}, {0, 2_s, ActionType::ADD}, {0, 3_s, ActionType::DELETE}
    };
    result = is_problematic(entry6, vec6, min_interval);
    ASSERT_EQ(result.contains_unknown_reference_ids_, true);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Simple conflict between update and existing
    SymbolListEntry entry7{"test", 0, 0, ActionType::ADD};
    std::vector<SymbolEntryData> vec7{{0, 2_s, ActionType::ADD}, {0, 3_s, ActionType::DELETE}};
    result = is_problematic(entry7, vec7, min_interval);
    expected = SymbolEntryData{0, 3_s, ActionType::DELETE};
    ASSERT_EQ(result_equals(result, expected), true);

    // Update conflicts with existing
    SymbolListEntry entry8{"test", 0, 0, ActionType::DELETE};
    std::vector<SymbolEntryData> vec8{{0, 1_s, ActionType::ADD}, {0, 2_s, ActionType::ADD}};
    result = is_problematic(entry8, vec8, min_interval);
    expected = SymbolEntryData{0, 2_s, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected), true);

    // Update and existing timestamps too close
    SymbolListEntry entry9{"test", 0, 0, ActionType::DELETE};
    std::vector<SymbolEntryData> vec9{{1, 100, ActionType::ADD}};

    result = is_problematic(entry9, vec9, min_interval);
    expected = SymbolEntryData{1, 100, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected), true);

    // Update and existing timestamps too close, same action is okay
    SymbolListEntry entry10{"test", 0, 0, ActionType::DELETE};
    std::vector<SymbolEntryData> vec10{{1, 100, ActionType::DELETE}};

    result = is_problematic(entry10, vec10, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);

    // Timestamps too close, but not most recent
    SymbolListEntry entry11{"test", 0, 0, ActionType::DELETE};
    std::vector<SymbolEntryData> vec11{
            {1, 100, ActionType::ADD},
            {2, 2_s, ActionType::ADD},
    };

    result = is_problematic(entry11, vec11, min_interval);
    ASSERT_EQ(static_cast<bool>(result), false);
}

TEST(Problematic, RealTimestamps2) {
    // SymbolListEntry entry1{"test", 0, 1694701083539622714, ActionType::ADD};
    std::vector<SymbolEntryData> vec1{
            {0, 1696255639552055287, ActionType::ADD}, {0, 1696255639570862954, ActionType::ADD}
    };

    auto result = is_problematic(vec1, 100000);
    ASSERT_EQ(static_cast<bool>(result), false);
}

TEST(Problematic, RealTimestamps) {
    SymbolListEntry entry1{"test", 0, 1694701083539622714, ActionType::ADD};
    std::vector<SymbolEntryData> vec1{
            {0, 1694701083516771231, ActionType::ADD},
            {0, 1694701083531817347, ActionType::ADD},
            {0, 1694701083541496287, ActionType::ADD},
            {0, 1694701083560192503, ActionType::ADD},
            {0, 1694701084093532954, ActionType::DELETE},
            {1, 1694701083552042983, ActionType::ADD}
    };

    auto result = is_problematic(entry1, vec1, 100000);
    auto expected = SymbolEntryData{1, 1694701083552042983, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected), true);

    SymbolListEntry entry2{"test", 2, 1694779989680380390, ActionType::ADD};
    std::vector<SymbolEntryData> vec2{
            {0, 1694779976040611297, ActionType::ADD}, {0, 1694779976054908858, ActionType::ADD},
            {0, 1694779976062913894, ActionType::ADD}, {0, 1694779976086496686, ActionType::ADD},
            {0, 1694779976095000098, ActionType::ADD}, {0, 1694779976098613575, ActionType::ADD},
            {0, 1694779976107390800, ActionType::ADD}, {0, 1694779976111358260, ActionType::ADD},
            {0, 1694779976143999710, ActionType::ADD}, {0, 1694779976168307639, ActionType::ADD},
            {1, 1694779989420016576, ActionType::ADD}, {1, 1694779989444335735, ActionType::ADD},
            {1, 1694779989498340862, ActionType::ADD}, {1, 1694779989512893237, ActionType::ADD},
            {2, 1694779989574879134, ActionType::ADD}, {2, 1694779989620457221, ActionType::ADD},
            {2, 1694779989677524006, ActionType::ADD}, {2, 1694779989686341802, ActionType::ADD},
            {2, 1694779989705203871, ActionType::ADD}, {3, 1694779989698388246, ActionType::ADD}
    };

    result = is_problematic(entry2, vec2, 100000);
    ASSERT_EQ(static_cast<bool>(result), false);

    SymbolListEntry entry3{"test", 0, 1696510154249460459, ActionType::ADD};
    std::vector<SymbolEntryData> vec3{
            {0, 1696510154081738353, ActionType::ADD},
            {0, 1696510154273679131, ActionType::ADD},
            {0, 1696510154277544441, ActionType::ADD},
            {0, 1696510154352448935, ActionType::DELETE},
            {1, 1696510154280568615, ActionType::ADD}
    };

    result = is_problematic(entry3, vec3, 100000);
    expected = SymbolEntryData{1, 1696510154280568615, ActionType::ADD};
    ASSERT_EQ(result_equals(result, expected), true);
}

bool is_compacted(const std::shared_ptr<Store>& store) {
    std::vector<AtomKey> symbol_keys;
    store->iterate_type(KeyType::SYMBOL_LIST, [&symbol_keys](const auto& key) { symbol_keys.push_back(to_atom(key)); });
    return symbol_keys.size() == 1 && symbol_keys[0].id() == StreamId{std::string{CompactionId}};
}

bool all_symbols_match(const std::shared_ptr<Store>& store, SymbolList& symbol_list, std::vector<StreamId>& expected) {
    auto symbols = symbol_list.get_symbols(store, true);
    if (symbols != expected)
        return false;

    auto old_symbols = backwards_compat_get_symbols(store);
    std::set<StreamId> expected_set;
    expected_set.insert(std::begin(expected), std::end(expected));
    if (old_symbols != expected_set)
        return false;

    return true;
}

TEST_F(SymbolListSuite, BackwardsCompatInterleave) {
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 1);
    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();
    auto version_map = std::make_shared<VersionMap>();
    SymbolList symbol_list{version_map};

    SymbolList::add_symbol(store, "s", 0);
    symbol_list.get_symbols(store, false); // trigger a compaction

    SymbolList::add_symbol(store, "symbol", 0);
    backwards_compat_write_journal(store, "symbol", std::string{AddSymbol});
    SymbolList::add_symbol(store, "symbol", 2);
    backwards_compat_write_journal(store, "symbol", std::string{DeleteSymbol});
    SymbolList::add_symbol(store, "symbol", 3);

    auto syms = symbol_list.get_symbols(store, false);
    ASSERT_EQ(syms.size(), 2);
}

TEST_F(SymbolListSuite, ExtremelyBackwardsCompatInterleavedWithSomewhatBackwardsCompat) {
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 1);
    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();
    auto version_map = std::make_shared<VersionMap>();
    SymbolList symbol_list{version_map};

    SymbolList::add_symbol(store, "s", 0);
    symbol_list.get_symbols(store, false); // trigger a compaction

    // Very old arcticc clients wrote version numbers on the symbol list entries, which needs special handling.
    extremely_backwards_compat_write_journal(store, "symbol", std::string{AddSymbol}, 0);
    extremely_backwards_compat_write_journal(store, "symbol", std::string{DeleteSymbol}, 1);
    backwards_compat_write_journal(store, "symbol", std::string{AddSymbol});

    auto syms = symbol_list.get_symbols(store, false);
    ASSERT_EQ(syms.size(), 2);
}

TEST_F(SymbolListSuite, ExtremelyBackwardsCompatInterleavedWithNewStyle) {
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 1);
    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();
    auto version_map = std::make_shared<VersionMap>();
    SymbolList symbol_list{version_map};

    SymbolList::add_symbol(store, "s", 0);
    symbol_list.get_symbols(store, false); // trigger a compaction

    extremely_backwards_compat_write_journal(store, "symbol", std::string{AddSymbol}, 0);
    extremely_backwards_compat_write_journal(store, "symbol", std::string{DeleteSymbol}, 1);
    SymbolList::add_symbol(store, "symbol", 2);

    auto syms = symbol_list.get_symbols(store, false);
    ASSERT_EQ(syms.size(), 2);
}

TEST_F(SymbolListSuite, BackwardsCompat) {
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 5);
    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();
    auto version_map = std::make_shared<VersionMap>();
    SymbolList symbol_list{version_map};
    std::vector<StreamId> expected;

    for (auto i = 0U; i < 10; i += 2) {
        SymbolList::add_symbol(store, fmt::format("symbol_{}", i), 0);
        expected.emplace_back(fmt::format("symbol_{}", i));
        backwards_compat_write_journal(store, fmt::format("symbol_{}", i + 1), std::string{AddSymbol});
        expected.emplace_back(fmt::format("symbol_{}", i + 1));
    }

    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);

    symbol_list.get_symbols(store, false);
    ASSERT_EQ(is_compacted(store), true);

    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);

    for (auto i = 10U; i < 20; i += 2) {
        SymbolList::add_symbol(store, fmt::format("symbol_{}", i), 0);
        expected.emplace_back(fmt::format("symbol_{}", i));
        backwards_compat_write_journal(store, fmt::format("symbol_{}", i + 1), std::string{AddSymbol});
        expected.emplace_back(fmt::format("symbol_{}", i + 1));
    }

    std::sort(std::begin(expected), std::end(expected));
    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);

    auto old_keys = backwards_compat_get_all_symbol_list_keys(store);
    auto old_symbols = backwards_compat_get_symbols(store);
    backwards_compat_compact(store, std::move(old_keys), old_symbols);

    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);

    for (auto i = 0U; i < 10; i += 2) {
        SymbolList::remove_symbol(store, fmt::format("symbol_{}", i), 0);
        backwards_compat_write_journal(store, fmt::format("symbol_{}", i + 1), std::string{DeleteSymbol});
    }

    expected.clear();
    for (auto i = 10U; i < 20; ++i) {
        expected.emplace_back(fmt::format("symbol_{}", i));
    }

    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);

    symbol_list.get_symbols(store, false);
    ASSERT_EQ(is_compacted(store), true);
    ASSERT_EQ(all_symbols_match(store, symbol_list, expected), true);
}

struct TestSymbolListTask {
    std::shared_ptr<SymbolListState> state_;
    std::shared_ptr<Store> store_;
    std::shared_ptr<SymbolList> symbol_list_;

    TestSymbolListTask(
            const std::shared_ptr<SymbolListState>& state, const std::shared_ptr<Store>& store,
            const std::shared_ptr<SymbolList>& symbol_list
    ) :
        state_(state),
        store_(store),
        symbol_list_(symbol_list) {}

    Future<Unit> operator()() {
        for (size_t i = 0; i < 20; ++i) {
            state_->do_action(symbol_list_);
        }
        return makeFuture(Unit{});
    }
};

TEST_F(SymbolListSuite, MultiThreadStress) {
    ScopedConfig max_delta("SymbolList.MaxDelta", 10);
    log::version().set_pattern("%Y%m%d %H:%M:%S.%f %t %L %n | %v");
    std::vector<Future<Unit>> futures;
    auto state = std::make_shared<SymbolListState>(store_, version_map_);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{20};
    write_initial_compaction_key();
    for (size_t i = 0; i < 3; ++i) {
        auto symbol_list = std::make_shared<SymbolList>(version_map_);
        futures.emplace_back(exec.addFuture(TestSymbolListTask{state, store_, symbol_list}));
    }
    collect(futures).get();
}

TEST_F(SymbolListSuite, CompactionThreshold) {
    // given
    ScopedConfig min("SymbolList.MinCompactionThreshold", 2);
    ScopedConfig max("SymbolList.MaxCompactionThreshold", 2);
    std::shared_ptr<InMemoryStore> store = std::make_shared<InMemoryStore>();
    std::shared_ptr<VersionMap> version_map = std::make_shared<VersionMap>();
    auto symbol_list = SymbolList{version_map};
    auto state = std::make_shared<SymbolListState>(store, version_map);
    symbol_list.get_symbols(store, false);

    // when
    SymbolList::add_symbol(store, "1", 0);
    SymbolList::add_symbol(store, "2", 0);
    SymbolList::add_symbol(store, "3", 0);

    // then
    auto symbols = symbol_list.get_symbol_set(store);
    std::set<StreamId> expected_symbols = {"1", "2", "3"};
    ASSERT_EQ(symbols, expected_symbols);
    std::vector<VariantKey> keys;
    store->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](auto&& k) { keys.push_back(k); });
    ASSERT_EQ(keys.size(), 1);
}

TEST_F(SymbolListSuite, CompactionThresholdMaxDeltaWins) {
    // given
    ScopedConfig min("SymbolList.MinCompactionThreshold", 1);
    ScopedConfig max("SymbolList.MaxCompactionThreshold", 1);
    ScopedConfig max_delta("SymbolList.MaxDelta", 2);
    std::shared_ptr<InMemoryStore> store = std::make_shared<InMemoryStore>();
    std::shared_ptr<VersionMap> version_map = std::make_shared<VersionMap>();
    auto symbol_list = SymbolList{version_map};
    symbol_list.get_symbols(store, false);

    // when
    SymbolList::add_symbol(store, "1", 0);

    // then
    symbol_list.get_symbols(store, false);
    {
        std::vector<VariantKey> keys;
        store->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](auto&& k) { keys.push_back(k); });
        ASSERT_EQ(keys.size(), 2); // should be no compaction yet
    }

    // when
    SymbolList::add_symbol(store, "2", 0);

    // then
    symbol_list.get_symbols(store, false);
    {
        std::vector<VariantKey> keys;
        store->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](auto&& k) { keys.push_back(k); });
        ASSERT_EQ(keys.size(), 1); // should be compacted now
    }
}

TEST_F(SymbolListSuite, CompactionThresholdRandomChoice) {
    // given
    // Compaction thresholds [1, 10] form random choice, seeded with value 1 => choice of threshold is 5 (or 6 on Mac)
    ScopedConfig min("SymbolList.MinCompactionThreshold", 1);
    ScopedConfig max("SymbolList.MaxCompactionThreshold", 10);
    std::shared_ptr<InMemoryStore> store = std::make_shared<InMemoryStore>();
    std::shared_ptr<VersionMap> version_map = std::make_shared<VersionMap>();
    auto symbol_list = SymbolList{version_map, StringId(), 1};
    symbol_list.get_symbols(store, false);

    // when
    for (int i = 0; i < 6; i++) {
        SymbolList::add_symbol(store, fmt::format("sym{}", i), 0);
    }
    symbol_list.get_symbols(store, false);

    // then
    std::vector<VariantKey> keys;
    store->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](auto&& k) { keys.push_back(k); });
    ASSERT_EQ(keys.size(), 1); // should be compacted
}

TEST_F(SymbolListSuite, KeyHashIsDifferent) {
    auto version_store = get_test_engine();
    const auto& store = version_store._test_get_store();

    SymbolList::add_symbol(store, symbol_1, 0);
    SymbolList::add_symbol(store, symbol_2, 1);
    SymbolList::add_symbol(store, symbol_3, 2);

    std::unordered_set<uint64_t> hashes;
    store->iterate_type(KeyType::SYMBOL_LIST, [&hashes](const auto& key) {
        hashes.insert(to_atom(key).content_hash());
    });

    ASSERT_EQ(hashes.size(), 3);
}

struct ReferenceVersionMap {
    std::unordered_map<StreamId, VersionId> versions_;
    std::mutex mutex_;

    VersionId get_incremented(const StreamId& stream_id) {
        std::lock_guard lock{mutex_};
        auto it = versions_.find(stream_id);
        if (it == std::end(versions_)) {
            versions_.try_emplace(stream_id, 1);
            return 1;
        } else {
            return ++it->second;
        }
    }
};

struct WriteSymbolsTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;
    size_t offset_;
    std::shared_ptr<ReferenceVersionMap> versions_;

    WriteSymbolsTask(
            const std::shared_ptr<Store>& store, size_t offset, const std::shared_ptr<ReferenceVersionMap>& versions
    ) :
        store_(store),
        symbol_list_(std::make_shared<SymbolList>(version_map_)),
        offset_(offset),
        versions_(versions) {}

    Future<Unit> operator()() {
        for (auto x = 0; x < 5000; ++x) {
            auto symbol = fmt::format("symbol_{}", offset_++ % 1000);
            SymbolList::add_symbol(store_, symbol, versions_->get_incremented(symbol));
        }
        return makeFuture(Unit{});
    }
};

struct CheckSymbolsTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;

    explicit CheckSymbolsTask(const std::shared_ptr<Store>& store) :
        store_(store),
        symbol_list_(std::make_shared<SymbolList>(version_map_)) {}

    void body() const { // gtest macros must be used in a function that returns void....
        for (auto x = 0; x < 100; ++x) {
            auto num_symbols = symbol_list_->get_symbol_set(store_);
            ASSERT_EQ(num_symbols.size(), 1000) << "@iteration x=" << x;
        }
    }

    Future<Unit> operator()() const {
        body();
        return {};
    }
};

TEST_F(SymbolListSuite, AddAndCompact) {
    log::version().set_pattern("%Y%m%d %H:%M:%S.%f %t %L %n | %v");
    std::vector<Future<Unit>> futures;
    std::optional<AtomKey> previous_key;
    for (auto x = 0; x < 1000; ++x) {
        auto symbol = fmt::format("symbol_{}", x);
        SymbolList::add_symbol(store_, symbol, 0);
        auto key = atom_key_builder().build(symbol, KeyType::TABLE_INDEX);
        version_map_->write_version(store_, key, previous_key);
        previous_key = key;
    }
    (void)symbol_list_->get_symbol_set(store_);
    auto versions = std::make_shared<ReferenceVersionMap>();

    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{10};
    futures.emplace_back(exec.addFuture(WriteSymbolsTask{store_, 0, versions}));
    futures.emplace_back(exec.addFuture(WriteSymbolsTask{store_, 500, versions}));
    futures.emplace_back(exec.addFuture(CheckSymbolsTask{store_}));
    futures.emplace_back(exec.addFuture(CheckSymbolsTask{store_}));
    collect(futures).get();
}

struct SymbolListRace : SymbolListSuite, testing::WithParamInterface<std::tuple<char, bool, bool, bool>> {};

TEST_P(SymbolListRace, Run) {
    override_max_delta(1);
    auto [source, remove_old, add_new, add_other] = GetParam();

    // Set up source
    if (source == 'S') {
        write_initial_compaction_key();
        SymbolList::add_symbol(store_, symbol_1, 0);
    } else {
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(4).end_index(5).build(
                symbol_1, KeyType::TABLE_INDEX
        );
        version_map_->write_version(store_, key1, std::nullopt);
    }

    // Variables for capturing the symbol list state before compaction:
    std::vector<VariantKey> before;

    // Emulate concurrent actions by intercepting try_lock
    StorageFailureSimulator::instance()->configure(
            {{FailureType::WRITE,
              {FailureAction(
                       "concurrent",
                       [&before, remove_old, add_new, add_other, this](auto) {
                           if (remove_old) {
                               store_->remove_keys(get_symbol_list_keys(), {});
                           }
                           if (add_new) {
                               store_->write(
                                       KeyType::SYMBOL_LIST,
                                       0,
                                       StringId{CompactionId},
                                       PilotedClock::nanos_since_epoch(),
                                       NumericIndex{0},
                                       NumericIndex{0},
                                       SegmentInMemory{}
                               );
                           }
                           if (add_other) {
                               SymbolList::add_symbol(store_, symbol_2, 0);
                           }

                           before = get_symbol_list_keys();
                       }
               ),
               no_op}}}
    );

    // Check compaction
    std::vector<StreamId> symbols = symbol_list_->get_symbols(store_);
    ASSERT_THAT(symbols, ElementsAre(symbol_1));

    if (!remove_old && !add_new) { // A normal compaction should have happened
        auto keys = get_symbol_list_keys(StringId{CompactionId});
        ASSERT_THAT(keys, SizeIs(1));
    } else {
        // Should not have changed any keys
        ASSERT_THAT(get_symbol_list_keys(), UnorderedElementsAreArray(before));
    }
}

// For symbol list source (which is used for subsequent compactions), all flag combinations are valid:
INSTANTIATE_TEST_SUITE_P(SymbolListSource, SymbolListRace, Combine(Values('S'), Bool(), Bool(), Bool()));
// For version keys source (initial compaction), there's no old compaction key to remove:
INSTANTIATE_TEST_SUITE_P(VersionKeysSource, SymbolListRace, Combine(Values('V'), Values(false), Bool(), Bool()));
