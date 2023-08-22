/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h> // Included in gtest 1.10

#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>

#include <shared_mutex>

#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

using namespace arcticdb;
using namespace folly;
using namespace arcticdb::entity;
using namespace arcticdb::action_factories;
using namespace ::testing;

MAKE_GTEST_FMT(FailureType, "{}")

static const StorageFailureSimulator::ParamActionSequence RAISE_ONCE = {fault(), no_op};
static const StorageFailureSimulator::ParamActionSequence RAISE_ON_2ND_CALL = {no_op, fault(), no_op};

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

    void TearDown() override {
        ConfigsMap::instance()->unset_int("SymbolList.MaxDelta");
    }

    const StreamId symbol_1 {"aaa"};
    const StreamId symbol_2 {"bbb"};
    const StreamId symbol_3 {"ccc"};

    std::shared_ptr<InMemoryStore> store = std::make_shared<InMemoryStore>();
    std::shared_ptr<VersionMap> version_map = std::make_shared<VersionMap>();
    SymbolList symbol_list{version_map, };

    // Need at least one compaction key to avoid using the version keys as source
    void write_initial_compaction_key() {
        log::version().set_level(spdlog::level::warn);
        symbol_list.load(store, false);
        log::version().set_level(spdlog::level::debug);
    }

    // The max_delta_ can be set at construction time only, so to change requires a new instance:
    void override_max_delta(int size) {
        ConfigsMap::instance()->set_int("SymbolList.MaxDelta", size);
        symbol_list.~SymbolList();
        new(&symbol_list) SymbolList(version_map);
    }

    auto get_symbol_list_keys(const std::string& prefix = "") {
        std::vector<VariantKey> keys;
        store->iterate_type(entity::KeyType::SYMBOL_LIST, [&keys](auto&& k){keys.push_back(k);}, prefix);
        return keys;
    }
};

using FailSimParam = StorageFailureSimulator::Params;

/** Adds storage failure cases to some tests. */
struct SymbolListWithReadFailures : SymbolListSuite, testing::WithParamInterface<FailSimParam> {
    void setup_failure_sim_if_any() {
        StorageFailureSimulator::instance()->configure(GetParam());
    }
};

TEST_P(SymbolListWithReadFailures, FromSymbolListSource) {
    write_initial_compaction_key();
    setup_failure_sim_if_any();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    std::vector<StreamId> copy = symbol_list.get_symbols(store, true);

    ASSERT_EQ(copy.size(), 2) << fmt::format("got {}", copy);
    ASSERT_EQ(copy[0], StreamId{"aaa"});
    ASSERT_EQ(copy[1], StreamId{"bbb"});
}

TEST_F(SymbolListSuite, Persistence) {
    write_initial_compaction_key();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    SymbolList output_list{version_map};
    std::vector<StreamId> symbols = output_list.get_symbols(store, true);

    ASSERT_EQ(symbols.size(), 2) << fmt::format("got {}", symbols);
    ASSERT_EQ(symbols[0], StreamId{symbol_1});
    ASSERT_EQ(symbols[1], StreamId{symbol_2});
}

TEST_P(SymbolListWithReadFailures, VersionMapSource) {

    auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
            4).end_index(5).build(symbol_3, KeyType::TABLE_INDEX);

    auto key2 = atom_key_builder().version_id(2).creation_ts(3).content_hash(4).start_index(
            5).end_index(6).build(symbol_1, KeyType::TABLE_INDEX);

    auto key3 = atom_key_builder().version_id(3).creation_ts(4).content_hash(5).start_index(
            6).end_index(7).build(symbol_2, KeyType::TABLE_INDEX);

    version_map->write_version(store, key1);
    version_map->write_version(store, key2);
    version_map->write_version(store, key3);

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list.get_symbols(store);
    ASSERT_THAT(symbols, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));
}

INSTANTIATE_TEST_SUITE_P(, SymbolListWithReadFailures, Values(
        FailSimParam{}, // No failure
        FailSimParam{{FailureType::ITERATE, RAISE_ONCE}},
        FailSimParam{{FailureType::READ, RAISE_ONCE}},
        FailSimParam{{FailureType::READ, {fault(), fault(), fault(), fault(), no_op}}},
        FailSimParam{{FailureType::READ, {fault(0.4), no_op}}}, // 40% chance of exception
        FailSimParam{{FailureType::ITERATE, RAISE_ONCE}, {FailureType::READ, {no_op, fault(), no_op}}}
));

TEST_F(SymbolListSuite, MultipleWrites) {
    write_initial_compaction_key();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    SymbolList another_instance{version_map};
    another_instance.add_symbol(store, symbol_3);

    std::vector<StreamId> copy = symbol_list.get_symbols(store, true);
    ASSERT_THAT(copy, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));

    std::vector<StreamId> symbols = another_instance.get_symbols(store, true);
    ASSERT_THAT(symbols, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));
}

enum CompactOutcome: size_t {
    NOT_WRITTEN = 0, WRITTEN, NOT_CLEANED_UP
};

using WriteFailuresParams = std::tuple<FailSimParam, CompactOutcome>;

struct SymbolListWithWriteFailures : SymbolListSuite, testing::WithParamInterface<WriteFailuresParams> {
    CompactOutcome expected_outcome;

    void setup_failure_sim_if_any() {
        StorageFailureSimulator::instance()->configure(std::get<0>(GetParam()));
        expected_outcome = std::get<1>(GetParam());
    }

    void check_num_symbol_list_keys_match_expectation(std::map<CompactOutcome, size_t> size_by_outcome) {
        auto keys = get_symbol_list_keys();
        ASSERT_THAT(keys, SizeIs(size_by_outcome.at(expected_outcome)));
    }
};

TEST_P(SymbolListWithWriteFailures, SubsequentCompaction) {
    write_initial_compaction_key();

    std::vector<StreamId> expected;
    for(size_t i = 0; i < 500; ++i) {
        auto symbol = fmt::format("sym{}", i);
        symbol_list.add_symbol(store, symbol);
        expected.emplace_back(symbol);
    }

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list.get_symbols(store, false);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

    // Extra 1 in 501 is the initial compaction key
    // NOT_CLEANED_UP case checks that deletion happens after the compaction key is written
    check_num_symbol_list_keys_match_expectation({{{NOT_WRITTEN, 501}, {WRITTEN, 1}, {NOT_CLEANED_UP, 502}}});

    // Retry:
    if (expected_outcome != WRITTEN) {
        symbols = symbol_list.get_symbols(store, false);
        ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

        check_num_symbol_list_keys_match_expectation({{{NOT_WRITTEN, 1}, {NOT_CLEANED_UP, 1}}});
    }
}

TEST_P(SymbolListWithWriteFailures, InitialCompact) {
    int64_t n = 20;
    override_max_delta(n);

    std::vector<StreamId> expected;
    for(int64_t i = 0; i < n + 1; ++i) {
        auto symbol = fmt::format("sym{}", i);
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
                4).end_index(5).build(symbol, KeyType::TABLE_INDEX);
        version_map->write_version(store, key1);
        expected.emplace_back(symbol);
    }

    setup_failure_sim_if_any();
    std::vector<StreamId> symbols = symbol_list.get_symbols(store);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

    check_num_symbol_list_keys_match_expectation({{{NOT_WRITTEN, 0}, {WRITTEN, 1}, {NOT_CLEANED_UP, 1}}});
}

INSTANTIATE_TEST_SUITE_P(, SymbolListWithWriteFailures, Values(
        WriteFailuresParams{FailSimParam{}, CompactOutcome::WRITTEN}, // No failures
        WriteFailuresParams{{{FailureType::WRITE, RAISE_ONCE}}, CompactOutcome::NOT_WRITTEN}, // Interferes with locking
        WriteFailuresParams{{{FailureType::WRITE, RAISE_ON_2ND_CALL}}, CompactOutcome::NOT_WRITTEN},
        WriteFailuresParams{{{FailureType::DELETE, RAISE_ONCE}}, CompactOutcome::NOT_CLEANED_UP}
));

template <typename T, typename U>
std::optional<T> random_choice(const std::set<T>& set, U& gen) {
    if (set.empty()) {
        return std::nullopt;
    }
    std::uniform_int_distribution<> dis(0, std::min(size_t{0}, set.size() - 1));
    size_t index = dis(gen);
    auto it = set.begin();
    std::advance(it, index);
    return *it;
}

class SymbolListState {
public:
    SymbolListState(std::shared_ptr<Store> store) :
            store_(std::move(store)),
            gen_(42) {}

    void do_action(std::shared_ptr<SymbolList> symbol_list) {
        std::scoped_lock<std::mutex> lock{mutex_};
        std::uniform_int_distribution<> dis(0, 10);
        auto action = dis(gen_);
        switch(action) {
            case 0:
                do_delete(symbol_list);
                break;
            case 1:
                do_readd(symbol_list);
                break;
            case 2:
                do_list_symbols(symbol_list);
                break;
            default:
                do_add(symbol_list);
                break;
        }
        assert_invariants(symbol_list);
    };
    void do_list_symbols(std::shared_ptr<SymbolList> symbol_list) {
        ASSERT_EQ(symbol_list->get_symbol_set(store_), live_symbols_);
    };
private:
    void do_add(std::shared_ptr<SymbolList> symbol_list) {
        std::uniform_int_distribution<> dis(0);
        int id = dis(gen_);
        auto symbol = fmt::format("sym-{}", id);
        if (live_symbols_.count(symbol) == 0) {
            live_symbols_.insert(symbol);
            symbol_list->add_symbol(store_, symbol);
            ARCTICDB_DEBUG(log::version(), "Adding {}", symbol);
        }
    };
    void do_delete(std::shared_ptr<SymbolList> symbol_list) {
        auto symbol = random_choice(live_symbols_, gen_);
        if (symbol.has_value()) {
            live_symbols_.erase(*symbol);
            deleted_symbols_.insert(*symbol);
            symbol_list->remove_symbol(store_, *symbol);
            ARCTICDB_DEBUG(log::version(), "Removing {}", *symbol);
        }
    };
    void do_readd(std::shared_ptr<SymbolList> symbol_list) {
        auto symbol = random_choice(deleted_symbols_, gen_);
        if (symbol.has_value()) {
            deleted_symbols_.erase(*symbol);
            live_symbols_.insert(*symbol);
            symbol_list->add_symbol(store_, *symbol);
            ARCTICDB_DEBUG(log::version(), "Re-adding {}", *symbol);
        }
    };
    void assert_invariants(std::shared_ptr<SymbolList> symbol_list) {
        for(const auto& symbol: live_symbols_) {
            ASSERT_EQ(deleted_symbols_.count(symbol), 0);
        }
        for(const auto& symbol: deleted_symbols_) {
            ASSERT_EQ(live_symbols_.count(symbol), 0);
        }
        auto symbol_list_symbols_vec = symbol_list->get_symbols(store_, true);
        std::set<StreamId> symbol_list_symbols_set{symbol_list_symbols_vec.begin(), symbol_list_symbols_vec.end()};
        ASSERT_EQ(symbol_list_symbols_set, live_symbols_);
    }

    std::shared_ptr<Store> store_;
    std::mt19937 gen_;
    std::set<StreamId> live_symbols_;
    std::set<StreamId> deleted_symbols_;
    std::mutex mutex_;
};

TEST_F(SymbolListSuite, AddDeleteReadd) {
    auto symbol_list = std::make_shared<SymbolList>(version_map);
    write_initial_compaction_key();

    SymbolListState state(store);
    for (size_t i = 0; i < 10; ++i) {
        state.do_action(symbol_list);
    }
    state.do_list_symbols(symbol_list);
}

struct TestSymbolListTask {
    std::shared_ptr<SymbolListState> state_;
    std::shared_ptr<Store> store_;
    std::shared_ptr<SymbolList> symbol_list_;

    TestSymbolListTask(const std::shared_ptr<SymbolListState> state,
            const std::shared_ptr<Store>& store,
            const std::shared_ptr<SymbolList> symbol_list) :
            state_(state),
            store_(store),
            symbol_list_(symbol_list) {}

    Future<Unit> operator()() {
        for (size_t i = 0; i < 10; ++i) {
            state_->do_action(symbol_list_);
        }
        return makeFuture(Unit{});
    }
};

TEST_F(SymbolListSuite, MultiThreadStress) {
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", 10);
    log::version().set_pattern("%Y%m%d %H:%M:%S.%f %t %L %n | %v");
    std::vector<Future<Unit>> futures;
    auto state = std::make_shared<SymbolListState>(store);
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{10};
    write_initial_compaction_key();
    for(size_t i = 0; i < 5; ++i) {
        auto symbol_list = std::make_shared<SymbolList>(version_map);
        futures.emplace_back(exec.addFuture(TestSymbolListTask{state, store, symbol_list}));
    }
    collect(futures).get();
}

TEST_F(SymbolListSuite, KeyHashIsDifferent) {
    auto version_store = get_test_engine();
    auto& store = version_store._test_get_store();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);
    symbol_list.add_symbol(store, symbol_3);

    std::unordered_set<uint64_t> hashes;
    store->iterate_type(KeyType::SYMBOL_LIST, [&hashes] (const auto& key) {
        hashes.insert(to_atom(key).content_hash());
    });

    ASSERT_EQ(hashes.size(), 3);
}

struct WriteSymbolsTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;
    size_t offset_;

    WriteSymbolsTask(const std::shared_ptr<Store>& store, size_t offset) :
            store_(store),
            symbol_list_(std::make_shared<SymbolList>(version_map_)),
            offset_(offset) {}

    Future<Unit> operator()() {
        for(auto x = 0; x < 5000; ++x) {
            auto symbol = fmt::format("symbol_{}", offset_++ % 1000);
            symbol_list_->add_symbol(store_, symbol);
        }
        return makeFuture(Unit{});
    }
};

struct CheckSymbolsTask {
    std::shared_ptr<Store> store_;
    std::shared_ptr<VersionMap> version_map_ = std::make_shared<VersionMap>();
    std::shared_ptr<SymbolList> symbol_list_;

    CheckSymbolsTask(const std::shared_ptr<Store>& store) :
            store_(store),
            symbol_list_(std::make_shared<SymbolList>(version_map_)){}

    void body() { // gtest macros must be used in a function that returns void....
        for(auto x = 0; x < 100; ++x) {
            auto num_symbols = symbol_list_->get_symbol_set(store_);
            ASSERT_EQ(num_symbols.size(), 1000) << "@iteration x=" << x;
        }
    }

    Future<Unit> operator()() {
        body();
        return {};
    }
};

TEST_F(SymbolListSuite, AddAndCompact) {
    log::version().set_pattern("%Y%m%d %H:%M:%S.%f %t %L %n | %v");
    std::vector<Future<Unit>> futures;
    for(auto x = 0; x < 1000; ++x) {
        auto symbol = fmt::format("symbol_{}", x);
        symbol_list.add_symbol(store, symbol);
        version_map->write_version(store, atom_key_builder().build(symbol, KeyType::TABLE_INDEX));
    }
    folly::FutureExecutor<folly::CPUThreadPoolExecutor> exec{10};
    futures.emplace_back(exec.addFuture(WriteSymbolsTask{store, 0}));
    futures.emplace_back(exec.addFuture(WriteSymbolsTask{store, 500}));
    futures.emplace_back(exec.addFuture(CheckSymbolsTask{store}));
    futures.emplace_back(exec.addFuture(CheckSymbolsTask{store}));
    collect(futures).get();
}

TEST_F(SymbolListSuite, KeySortingAndLatestTimestampPreservation) {
    write_initial_compaction_key();
    override_max_delta(2);

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    timestamp saved = PilotedClock::time_;
    symbol_list.remove_symbol(store, symbol_1);

    auto symbols = symbol_list.load(store, false);
    ASSERT_THAT(symbols, ElementsAre(symbol_2));

    // Even though symbol_1 don't even appear in the result, its timestamp should be used:
    size_t count = 0;
    store->iterate_type(entity::KeyType::SYMBOL_LIST, [saved=saved, &count](const auto& k){
        ASSERT_EQ(to_atom(k).creation_ts(), saved);
        count++;
    }, "");
    ASSERT_EQ(count, 1);
}

struct SymbolListRace: SymbolListSuite, testing::WithParamInterface<std::tuple<char, bool, bool, bool>> {};

TEST_P(SymbolListRace, Run) {
    override_max_delta(1);
    auto [source, remove_old, add_new, add_other] = GetParam();
    timestamp expected_ts;

    // Set up source
    if (source == 'S') {
        write_initial_compaction_key();
        symbol_list.add_symbol(store, symbol_1);
        // Compaction keys should use the highest timestamp of the keys it contains (the one we just added):
        expected_ts = PilotedClock::time_ - 1;
    } else {
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
                4).end_index(5).build(symbol_1, KeyType::TABLE_INDEX);
        version_map->write_version(store, key1);
        expected_ts = PilotedClock::time_;
    }

    // Variables for capturing the symbol list state before compaction:
    std::vector<VariantKey> before;

    // Emulate concurrent actions by intercepting try_lock
    StorageFailureSimulator::instance()->configure({{FailureType::WRITE,
        { FailureAction("concurrent", [&before, this](auto) {
            //FUTURE(C++20): structured binding cannot be captured prior to 20
            auto [unused, remove_old2, add_new2, add_other2] = GetParam();
            if (remove_old2) {
                store->remove_keys(get_symbol_list_keys(), {});
            }
            if (add_new2) {
                store->write(KeyType::SYMBOL_LIST, 0, CompactionId, PilotedClock::nanos_since_epoch(), 0, 0,
                        SegmentInMemory{});
            }
            if (add_other2) {
                symbol_list.add_symbol(store, symbol_2);
            }

            before = get_symbol_list_keys();
        }),
        no_op}}});

    // Check compaction
    std::vector<StreamId> symbols = symbol_list.get_symbols(store);
    ASSERT_THAT(symbols, ElementsAre(symbol_1));

    if (!remove_old && !add_new) { // A normal compaction should have happened
        auto keys = get_symbol_list_keys(CompactionId);
        ASSERT_THAT(keys, SizeIs(1));
        ASSERT_THAT(to_atom(keys.at(0)).creation_ts(), Eq(expected_ts));
    } else {
        // Should not have changed any keys
        ASSERT_THAT(get_symbol_list_keys(), UnorderedElementsAreArray(before));
    }
}

// For symbol list source (which is used for subsequent compactions), all flag combinations are valid:
INSTANTIATE_TEST_SUITE_P(SymbolListSource, SymbolListRace, Combine(Values('S'), Bool(), Bool(), Bool()));
// For version keys source (initial compaction), there's no old compaction key to remove:
INSTANTIATE_TEST_SUITE_P(VersionKeysSource, SymbolListRace, Combine(Values('V'), Values(false), Bool(), Bool()));
