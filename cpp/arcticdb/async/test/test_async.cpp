/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the
 * file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source
 * License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/util/test/config_common.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/util/random.h>

#include <fmt/format.h>

#include <string>
#include <vector>

using namespace arcticdb;
namespace aa = arcticdb::async;
namespace as = arcticdb::storage;
namespace asl = arcticdb::storage::lmdb;
namespace ast = arcticdb::stream;

TEST(Async, SinkBasic) {

  as::EnvironmentName environment_name{"research"};
  as::StorageName storage_name("lmdb_local");
  as::LibraryPath library_path{"a", "b"};

  auto env_config = arcticdb::get_test_environment_config(library_path, storage_name,
                                                          environment_name);
  auto config_resolver = as::create_in_memory_resolver(env_config);
  as::LibraryIndex library_index{environment_name, config_resolver};

  as::UserAuth au{"abc"};
  auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, au);
  auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();
  aa::TaskScheduler sched{1};

  auto seg = SegmentInMemory();
  aa::EncodeAtomTask enc{entity::KeyType::GENERATION,
                         entity::VersionId{6},
                         NumericId{123},
                         NumericId{456},
                         timestamp{457},
                         entity::NumericIndex{999},
                         std::move(seg),
                         codec_opt,
                         EncodingVersion::V2};

  auto v = sched.submit_cpu_task(std::move(enc))
               .via(&aa::io_executor())
               .thenValue(aa::WriteSegmentTask{lib})
               .get();

  HashAccum h;
  auto default_content_hash = h.digest();

  ASSERT_EQ(entity::atom_key_builder()
                .gen_id(6)
                .start_index(456)
                .end_index(457)
                .creation_ts(999)
                .content_hash(default_content_hash)
                .build(NumericId{123}, entity::KeyType::GENERATION),
            to_atom(v));
}

TEST(Async, DeDupTest) {
  as::EnvironmentName environment_name{"research"};
  as::StorageName storage_name("lmdb_local");
  as::LibraryPath library_path{"a", "b"};
  namespace ap = arcticdb::pipelines;

  auto env_config = arcticdb::get_test_environment_config(library_path, storage_name,
                                                          environment_name);
  auto config_resolver = as::create_in_memory_resolver(env_config);
  as::LibraryIndex library_index{environment_name, config_resolver};

  as::UserAuth au{"abc"};
  auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, au);
  auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();
  aa::AsyncStore store(lib, *codec_opt, EncodingVersion::V2);
  auto seg = SegmentInMemory();

  std::vector<std::pair<ast::StreamSink::PartialKey, SegmentInMemory>> key_segments;

  key_segments.emplace_back(ast::StreamSink::PartialKey{entity::KeyType::TABLE_DATA, 1,
                                                        "", entity::NumericIndex{0},
                                                        entity::NumericIndex{1}},
                            seg);
  key_segments.emplace_back(ast::StreamSink::PartialKey{entity::KeyType::TABLE_DATA, 2,
                                                        "", entity::NumericIndex{1},
                                                        entity::NumericIndex{2}},
                            seg);

  HashAccum h;
  auto default_content_hash = h.digest();

  auto de_dup_map = std::make_shared<DeDupMap>();
  auto k = entity::atom_key_builder()
               .gen_id(3)
               .start_index(0)
               .end_index(1)
               .creation_ts(999)
               .content_hash(default_content_hash)
               .build("", entity::KeyType::TABLE_DATA);
  de_dup_map->insert_key(k);

  std::vector<folly::Future<arcticdb::pipelines::SliceAndKey>> slice_key_futures;
  for (auto& [key, segment] : key_segments) {
    auto input =
        std::make_tuple<ast::StreamSink::PartialKey, SegmentInMemory, ap::FrameSlice>(
            std::move(key), std::move(segment), {});
    auto fut = folly::makeFuture(std::move(input));
    slice_key_futures.emplace_back(store.async_write(std::move(fut), de_dup_map));
  }
  auto slice_keys = folly::collect(slice_key_futures).get();
  std::vector<AtomKey> keys;
  for (const auto& slice_key : slice_keys)
    keys.emplace_back(slice_key.key());

  // The first key will be de-duped, second key will be fresh because indexes dont match
  ASSERT_EQ(2ULL, keys.size());
  ASSERT_EQ(k, to_atom(keys[0]));
  ASSERT_NE(k, to_atom(keys[1]));
  ASSERT_NE(999, to_atom(keys[1]).creation_ts());
  ASSERT_EQ(2, to_atom(keys[1]).version_id());
}

struct MaybeThrowTask : arcticdb::async::BaseTask {
  bool do_throw_;
  explicit MaybeThrowTask(bool do_throw) : do_throw_(do_throw) {}

  folly::Unit operator()() const {
    using namespace arcticdb;
    util::check(!do_throw_, "Test intentionally throwing");
    return folly::Unit{};
  }
};

TEST(Async, CollectWithThrow) {
  std::vector<folly::Future<folly::Unit>> stuff;
  using namespace arcticdb;

  async::TaskScheduler sched{20};
  try {
    for (auto i = 0u; i < 1000; ++i) {
      stuff.push_back(sched.submit_io_task(MaybeThrowTask(i == 3)));
    }
    auto vec_fut = folly::collectAll(stuff).get();
  } catch (std::exception&) {
    ARCTICDB_DEBUG(log::version(), "Caught something");
  }

  ARCTICDB_DEBUG(log::version(), "Collect returned");
}

using IndexSegmentReader = int;

int get_index_segment_reader_impl(arcticdb::StreamId id) {
  std::cout << "Getting " << fmt::format("{}", id) << std::endl;
  return 5;
}

folly::Future<int> get_index_segment_reader(folly::Future<arcticdb::StreamId>&& fut) {
  return std::move(fut)
      .via(&arcticdb::async::io_executor())
      .thenValue(get_index_segment_reader_impl);
}

std::string do_read_impl(IndexSegmentReader&& idx) { return fmt::format("{}", idx); }

folly::Future<std::string> do_read(folly::Future<IndexSegmentReader>&& fut) {
  return std::move(fut).via(&arcticdb::async::cpu_executor()).thenValue(do_read_impl);
}

TEST(Async, SemiFuturePassing) {
  using namespace folly;
  using namespace arcticdb;
  Promise<StreamId> p;
  Future<StreamId> f = p.getFuture();
  auto f2 = get_index_segment_reader(std::move(f));

  auto f3 = do_read(std::move(f2));
  p.setValue("symbol");
  auto thing = std::move(f3).get();
  std::cout << thing << std::endl;
}

folly::Future<int> num_slices(folly::Future<int>&& f) {
  return std::move(f).thenValue([](auto x) { return x; });
}

struct Thing : arcticdb::async::BaseTask {
  int x_;

  explicit Thing(int x) : x_(x) {}

  int operator()() const { return x_ + 2; }
};

auto multiplex(folly::Future<int>&& n) {
  using namespace arcticdb;

  return std::move(n).thenValue([](auto i) {
    std::vector<folly::Future<int>> futures;
    for (auto x = 0; x < i; ++x) {
      futures.push_back(async::submit_cpu_task(Thing{x}));
    }
    return folly::collect(futures);
  });
}

TEST(Async, DynamicSizing) {
  using namespace folly;
  using namespace arcticdb;
  Promise<int> p;
  Future<int> f = p.getFuture();
  auto f1 = num_slices(std::move(f));
  auto f2 = multiplex(std::move(f1));
  p.setValue(5);
  (void)std::move(f2).get();
}

TEST(Async, NumCoresCgroupV1) {
  std::string test_path{"./test_v1"};
  std::string cpu_quota_path{"./test_v1/cpu/cpu.cfs_quota_us"};
  std::string cpu_period_path{"./test_v1/cpu/cpu.cfs_period_us"};
  std::filesystem::create_directories("./test_v1/cpu");

  // Test the happy path
  std::ofstream cpuset(cpu_quota_path);
  cpuset << "100000\n";
  cpuset.close();

  std::ofstream cpuset2(cpu_period_path);
  cpuset2 << "100000\n";
  cpuset2.close();

  int64_t def_cpu_core = arcticdb::async::get_default_num_cpus(test_path);

  int64_t hardware_cpu_count = std::thread::hardware_concurrency() == 0
                                   ? 16
                                   : std::thread::hardware_concurrency();
#ifdef _WIN32
  ASSERT_EQ(hardware_cpu_count, def_cpu_core);
#else
  ASSERT_EQ(1, def_cpu_core);

  // test the error value path
  std::ofstream cpuset3(cpu_period_path);
  cpuset3 << "-1\n";
  cpuset3.close();

  def_cpu_core = arcticdb::async::get_default_num_cpus(test_path);

  ASSERT_EQ(hardware_cpu_count, def_cpu_core);

  // test the string value path - should raise an exception
  std::ofstream cpuset4(cpu_period_path);
  cpuset4 << "test\n";
  cpuset4.close();

  ASSERT_THROW(arcticdb::async::get_default_num_cpus(test_path), std::invalid_argument);
#endif
}

TEST(Async, NumCoresCgroupV2) {
  std::string test_path{"./test_v2"};
  std::string cpu_max_path{"./test_v2/cpu.max"};
  std::filesystem::create_directories(test_path);

  // Test the happy path
  std::ofstream cpuset(cpu_max_path);
  cpuset << "100000 100000\n";
  cpuset.close();

  int64_t def_cpu_core = arcticdb::async::get_default_num_cpus(test_path);

  int64_t hardware_cpu_count = std::thread::hardware_concurrency() == 0
                                   ? 16
                                   : std::thread::hardware_concurrency();
#ifdef _WIN32
  ASSERT_EQ(hardware_cpu_count, def_cpu_core);
#else
  ASSERT_EQ(1, def_cpu_core);

  // test the error value path
  std::ofstream cpuset2(cpu_max_path);
  cpuset2 << "-1 100000\n";
  cpuset2.close();

  def_cpu_core = arcticdb::async::get_default_num_cpus(test_path);

  ASSERT_EQ(hardware_cpu_count, def_cpu_core);

  // test the max value - should be the hardware cpu count
  std::ofstream cpuset3(cpu_max_path);
  cpuset3 << "max 100000\n";
  cpuset3.close();

  def_cpu_core = arcticdb::async::get_default_num_cpus(test_path);

  ASSERT_EQ(hardware_cpu_count, def_cpu_core);

  // test the string value path - should raise an exception
  std::ofstream cpuset4(cpu_max_path);
  cpuset4 << "test 100000\n";
  cpuset4.close();

  ASSERT_THROW(arcticdb::async::get_default_num_cpus(test_path), std::invalid_argument);
#endif
}

std::shared_ptr<arcticdb::Store>
create_store(const storage::LibraryPath& library_path, as::LibraryIndex& library_index,
             const storage::UserAuth& user_auth,
             std::shared_ptr<proto::encoding::VariantCodec>& codec_opt) {
  auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, user_auth);
  auto store = aa::AsyncStore(lib, *codec_opt, EncodingVersion::V1);
  return std::make_shared<aa::AsyncStore<>>(std::move(store));
}

TEST(Async, CopyCompressedInterStore) {
  using namespace arcticdb::async;

  // Given
  as::EnvironmentName environment_name{"research"};
  as::StorageName storage_name("storage_name");
  as::LibraryPath library_path{"a", "b"};
  namespace ap = arcticdb::pipelines;

  auto config = proto::nfs_backed_storage::Config();
  config.set_use_mock_storage_for_testing(true);

  auto env_config = arcticdb::get_test_environment_config(
      library_path, storage_name, environment_name, std::make_optional(config));
  auto config_resolver = as::create_in_memory_resolver(env_config);
  as::LibraryIndex library_index{environment_name, config_resolver};

  as::UserAuth user_auth{"abc"};
  auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();

  auto source_store = create_store(library_path, library_index, user_auth, codec_opt);

  // When - we write a key to the source and copy it
  const arcticdb::entity::RefKey& key =
      arcticdb::entity::RefKey{"abc", KeyType::VERSION_REF};
  auto segment_in_memory =
      get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
  auto row_count = segment_in_memory.row_count();
  ASSERT_GT(row_count, 0);
  auto segment = encode_dispatch(std::move(segment_in_memory), *codec_opt,
                                 arcticdb::EncodingVersion::V1);
  (void)segment.calculate_size();
  source_store->write_compressed_sync(as::KeySegmentPair{key, std::move(segment)});

  auto targets = std::vector<std::shared_ptr<arcticdb::Store>>{
      create_store(library_path, library_index, user_auth, codec_opt),
      create_store(library_path, library_index, user_auth, codec_opt),
      create_store(library_path, library_index, user_auth, codec_opt)};

  CopyCompressedInterStoreTask task{key,
                                    std::nullopt,
                                    false,
                                    false,
                                    source_store,
                                    targets,
                                    std::shared_ptr<BitRateStats>()};

  arcticdb::async::TaskScheduler sched{1};
  auto res = sched.submit_io_task(std::move(task)).get();

  // Then
  ASSERT_TRUE(std::holds_alternative<CopyCompressedInterStoreTask::AllOk>(res));
  for (const auto& target_store : targets) {
    auto read_result = target_store->read_sync(key);
    ASSERT_EQ(std::get<RefKey>(read_result.first), key);
    ASSERT_EQ(read_result.second.row_count(), row_count);
  }
}
