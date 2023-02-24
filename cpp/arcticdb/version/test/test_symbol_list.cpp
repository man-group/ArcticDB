/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h> // Included in gtest 1.10
#include <arcticdb/util/test/gtest_utils.hpp>

#include <arcticdb/version/symbol_list.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <shared_mutex>

#include <folly/executors/FutureExecutor.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <arcticdb/version/version_map.hpp>
#include <arcticdb/util/test/generators.hpp>


using namespace arcticdb;
using namespace folly;
using namespace arcticdb::entity;
using namespace ::testing;

class SymbolListSuite : public Test {
protected:
    static void SetUpTestCase() {
        Test::SetUpTestCase(); // Fail compilation when the name changes in a later version

        ConfigsMap::instance()->set_int("Logging.ALL", 1);
    }
    
    static void TearDownTestCase() {
        Test::TearDownTestCase();
        ConfigsMap::instance()->unset_int("Logging.ALL");
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
        symbol_list.load(store, false);
    }
};

#undef TEST
#define TEST(suite, ...) TEST_F(suite ## Suite, __VA_ARGS__)

TEST(SymbolList, InMemory) {
    write_initial_compaction_key();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    std::vector<StreamId> copy = symbol_list.get_symbols(store, true);

    ASSERT_EQ(copy.size(), 2) << fmt::format("got {}", copy);
    ASSERT_EQ(copy[0], StreamId{"aaa"});
    ASSERT_EQ(copy[1], StreamId{"bbb"});
}

TEST(SymbolList, Persistence) {
    write_initial_compaction_key();

    symbol_list.add_symbol(store, symbol_1);
    symbol_list.add_symbol(store, symbol_2);

    SymbolList output_list{version_map};
    std::vector<StreamId> symbols = output_list.get_symbols(store, true);

    ASSERT_EQ(symbols.size(), 2) << fmt::format("got {}", symbols);
    ASSERT_EQ(symbols[0], StreamId{symbol_1});
    ASSERT_EQ(symbols[1], StreamId{symbol_2});
}

TEST(SymbolList, CreateMissing) {

    auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
            4).end_index(5).build(symbol_3, KeyType::TABLE_INDEX);

    auto key2 = atom_key_builder().version_id(2).creation_ts(3).content_hash(4).start_index(
            5).end_index(6).build(symbol_1, KeyType::TABLE_INDEX);

    auto key3 = atom_key_builder().version_id(3).creation_ts(4).content_hash(5).start_index(
            6).end_index(7).build(symbol_2, KeyType::TABLE_INDEX);

    version_map->write_version(store, key1);
    version_map->write_version(store, key2);
    version_map->write_version(store, key3);

    std::vector<StreamId> symbols = symbol_list.get_symbols(store);
    ASSERT_THAT(symbols, UnorderedElementsAre(symbol_1, symbol_2, symbol_3));
}

TEST(SymbolList, MultipleWrites) {
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

TEST(SymbolList, WriteWithCompaction) {
    write_initial_compaction_key();

    std::vector<StreamId> expected;
    for(size_t i = 0; i < 500; ++i) {
        auto symbol = fmt::format("sym{}", i);
        symbol_list.add_symbol(store, symbol);
        expected.emplace_back(symbol);
    }

    std::vector<StreamId> symbols = symbol_list.get_symbols(store, true);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));
}

TEST(SymbolList, InitialCompact) {
    int64_t n = 500;
    ConfigsMap::instance()->set_int("SymbolList.MaxDelta", n);

    std::vector<StreamId> expected;
    for(int64_t i = 0; i < n + 1; ++i) {
        auto symbol = fmt::format("sym{}", i);
        auto key1 = atom_key_builder().version_id(1).creation_ts(2).content_hash(3).start_index(
                4).end_index(5).build(symbol, KeyType::TABLE_INDEX);
        version_map->write_version(store, key1);
        expected.emplace_back(symbol);
    }

    std::vector<StreamId> symbols = symbol_list.get_symbols(store);
    ASSERT_THAT(symbols, UnorderedElementsAreArray(expected));

    std::vector<entity::AtomKey> remaining_keys;
    store->iterate_type(entity::KeyType::SYMBOL_LIST, [&remaining_keys](const auto& k){remaining_keys.push_back(to_atom(k));}, "");
    ASSERT_EQ(1u, remaining_keys.size());
}

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

TEST(SymbolList, AddDeleteReadd) {
    auto symbol_list = std::make_shared<SymbolList>(version_map);
    write_initial_compaction_key();

    SymbolListState state(store);
    for (size_t i = 0; i < 100; ++i) {
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

TEST(SymbolList, MultiThreadStress) {
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

TEST(SymbolList, KeyHashIsDifferent) {
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

TEST(SymbolList, AddAndCompact) {
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
    // futures.emplace_back(exec.addFuture(CheckSymbolsTask{store}));
    collect(futures).get();
}
