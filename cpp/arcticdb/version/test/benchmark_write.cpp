#include <random>

#include <benchmark/benchmark.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/test/common.hpp>

using namespace arcticdb;

inline const fs::path TEST_DATABASES_PATH = "./test_databases";
const std::string TEST_LIB_NAME = "test_lib";
const uint64_t MAP_SIZE = 128ULL * (1ULL << 20); // 128 MiB
struct LMDBStore {

    std::unique_ptr<storage::lmdb::LmdbStorage> storage;

    LMDBStore(bool use_mock) {
        setup();

        arcticdb::proto::lmdb_storage::Config cfg;

        cfg.set_path((TEST_DATABASES_PATH / "test_lmdb").generic_string());
        cfg.set_map_size(MAP_SIZE);
        cfg.set_recreate_if_exists(true);
        cfg.set_use_mock_storage_for_testing(use_mock);

        arcticdb::storage::LibraryPath library_path(TEST_LIB_NAME, '/');

        storage = std::make_unique<arcticdb::storage::lmdb::LmdbStorage>(
                library_path, arcticdb::storage::OpenMode::DELETE, cfg
        );
    }

    ~LMDBStore() { clear(); }

    void setup() {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
    }

    void clear() {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }
    }
};

[[maybe_unused]] static void BM_write_lmdb(benchmark::State& state, bool include_clone_time, bool use_mock) {
    auto num_rows = state.range(0);

    auto sym = "symbol";
    auto key = RefKey(sym, KeyType::APPEND_REF);
    auto segment_in_memory = get_test_timeseries_frame(sym, num_rows, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    auto segment = encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);

    auto lmdb = LMDBStore(use_mock);

    for (auto _ : state) {
        if (!include_clone_time) {
            state.PauseTiming();
        }
        auto segment_copy = segment.clone();
        auto key_segment_pair_copy = storage::KeySegmentPair(key, std::move(segment_copy));
        if (!include_clone_time) {
            state.ResumeTiming();
        }
        lmdb.storage->write(std::move(key_segment_pair_copy));
        state.PauseTiming();
        lmdb.storage->remove(key, storage::RemoveOpts{});
        state.ResumeTiming();
    }
}

// TODO: Re-enable real lmdb benchmarks once running on Windows CI is fixed
// BENCHMARK_CAPTURE(BM_write_lmdb, mock_with_clone, true, true)->Arg(100'000)->Arg(1'000'000);
// BENCHMARK_CAPTURE(BM_write_lmdb, mock_no_clone, false, true)->Arg(100'000)->Arg(1'000'000);
// BENCHMARK_CAPTURE(BM_write_lmdb, real_with_clone, true, false)->Arg(100'000)->Arg(1'000'000);
// BENCHMARK_CAPTURE(BM_write_lmdb, real_no_clone, false, false)->Arg(100'000)->Arg(1'000'000);
