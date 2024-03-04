/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>
#include <arcticdb/storage/common.hpp>
#include <arcticdb/storage/config_resolvers.hpp>
#include <arcticdb/storage/library_index.hpp>
#include <arcticdb/storage/storage_factory.hpp>
#include <arcticdb/async/async_store.hpp>
#include <arcticdb/util/test/config_common.hpp>
#include <arcticdb/pipeline/frame_slice.hpp>
#include <arcticdb/util/random.h>

#include <fmt/format.h>

#include <string>
#include <vector>

namespace ac = arcticdb;
namespace aa = arcticdb::async;
namespace as = arcticdb::storage;
namespace asl = arcticdb::storage::lmdb;
namespace ast = arcticdb::stream;

TEST(Async, SinkBasic) {

    as::EnvironmentName environment_name{"research"};
    as::StorageName storage_name("lmdb_local");
    as::LibraryPath library_path{"a", "b"};

    auto env_config = arcticdb::get_test_environment_config(library_path, storage_name, environment_name);
    auto config_resolver = as::create_in_memory_resolver(env_config);
    as::LibraryIndex library_index{environment_name, config_resolver};

    as::UserAuth au{"abc"};
    auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, au);
    auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();
    aa::TaskScheduler sched{1};

    auto seg = ac::SegmentInMemory();
    aa::EncodeAtomTask enc{
        ac::entity::KeyType::GENERATION, 6, 123, 456, 457, 999, std::move(seg), codec_opt, ac::EncodingVersion::V2
    };

    auto v = sched.submit_cpu_task(std::move(enc)).via(&aa::io_executor()).thenValue(aa::WriteSegmentTask{lib}).get();

    ac::HashAccum h;
    auto default_content_hash = h.digest();

    ASSERT_EQ(ac::entity::atom_key_builder().gen_id(6).start_index(456).end_index(457).creation_ts(999)
                  .content_hash(default_content_hash).build(123, ac::entity::KeyType::GENERATION),
              to_atom(v)
    );
}

TEST(Async, DeDupTest) {
    as::EnvironmentName environment_name{"research"};
    as::StorageName storage_name("lmdb_local");
    as::LibraryPath library_path{"a", "b"};
    namespace ap = arcticdb::pipelines;

    auto env_config = arcticdb::get_test_environment_config(library_path, storage_name, environment_name);
    auto config_resolver = as::create_in_memory_resolver(env_config);
    as::LibraryIndex library_index{environment_name, config_resolver};

    as::UserAuth au{"abc"};
    auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, au);
    auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();
    aa::AsyncStore store(lib, *codec_opt, ac::EncodingVersion::V2);
    auto seg = ac::SegmentInMemory();

    std::vector<std::pair<ast::StreamSink::PartialKey, ac::SegmentInMemory>> key_segments;

    key_segments.emplace_back(ast::StreamSink::PartialKey{ac::entity::KeyType::TABLE_DATA, 1, "", 0, 1}, seg);
    key_segments.emplace_back(ast::StreamSink::PartialKey{ac::entity::KeyType::TABLE_DATA, 2, "", 1, 2}, seg);

    ac::HashAccum h;
    auto default_content_hash = h.digest();

    auto de_dup_map = std::make_shared<ac::DeDupMap>();
    auto k = ac::entity::atom_key_builder().gen_id(3).start_index(0).end_index(1).creation_ts(999)
            .content_hash(default_content_hash).build("", ac::entity::KeyType::TABLE_DATA);
    de_dup_map->insert_key(k);

    std::vector<folly::Future<arcticdb::pipelines::SliceAndKey>> slice_key_futures;
    for(auto& [key, segment] : key_segments) {
        auto input = std::make_tuple<ast::StreamSink::PartialKey, ac::SegmentInMemory, ap::FrameSlice>(std::move(key), std::move(segment), {});
        auto fut = folly::makeFuture(std::move(input));
        slice_key_futures.emplace_back(store.async_write(std::move(fut), de_dup_map));
    }
    auto slice_keys = folly::collect(slice_key_futures).get();
    std::vector<ac::AtomKey> keys;
    for(const auto& slice_key : slice_keys)
        keys.emplace_back(slice_key.key());

    //The first key will be de-duped, second key will be fresh because indexes dont match
    ASSERT_EQ(2ULL, keys.size());
    ASSERT_EQ(k, to_atom(keys[0]));
    ASSERT_NE(k, to_atom(keys[1]));
    ASSERT_NE(999, to_atom(keys[1]).creation_ts());
    ASSERT_EQ(2, to_atom(keys[1]).version_id());
}

struct MaybeThrowTask : arcticdb::async::BaseTask {
    bool do_throw_;
    explicit MaybeThrowTask(bool do_throw) :
        do_throw_(do_throw) {
    }

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
       for(auto i = 0u; i < 1000; ++i) {
           stuff.push_back(sched.submit_io_task(MaybeThrowTask(i==3)));
       }
       auto vec_fut = folly::collectAll(stuff).get();
   } catch(std::exception&) {
       log::version().info("Caught something");
   }

   log::version().info("Collect returned");
}

using IndexSegmentReader = int;

int get_index_segment_reader_impl(arcticdb::StreamId id) {
    std::cout << "Getting " << fmt::format("{}", id) << std::endl;
    return 5;
}

folly::Future<int> get_index_segment_reader(folly::Future<arcticdb::StreamId>&& fut) {
    return std::move(fut).via(&arcticdb::async::io_executor()).thenValue(get_index_segment_reader_impl);
}

std::string do_read_impl(IndexSegmentReader&& idx) {
    return fmt::format("{}", idx);
}

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
    return std::move(f).thenValue([] (auto x) {
        return x;
    });
}

struct Thing : arcticdb::async::BaseTask {
    int x_;

    explicit Thing(int x) : x_(x) {}

    int operator ()() const {
        return x_ + 2;
    }
};

auto multiplex(folly::Future<int> &&n) {
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