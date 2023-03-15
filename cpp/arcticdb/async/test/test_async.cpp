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
#include <arcticdb/util/random.h>

#include <fmt/format.h>
#include <google/protobuf/text_format.h>

#include <string>
#include <vector>

namespace aa = arcticdb::async;
namespace ac = arcticdb;
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
        ac::entity::KeyType::GENERATION, 6, 123, 456, 457, 999, std::move(seg), lib, codec_opt, size_t(0)
    };

    auto v = sched.submit_cpu_task(enc).via(&aa::io_executor()).thenValue(aa::WriteSegmentTask{lib}).get();

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

    auto env_config = arcticdb::get_test_environment_config(library_path, storage_name, environment_name);
    auto config_resolver = as::create_in_memory_resolver(env_config);
    as::LibraryIndex library_index{environment_name, config_resolver};

    as::UserAuth au{"abc"};
    auto lib = library_index.get_library(library_path, as::OpenMode::WRITE, au);
    auto codec_opt = std::make_shared<arcticdb::proto::encoding::VariantCodec>();
    aa::AsyncStore store(lib, *codec_opt);
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

    auto keys = store.batch_write(std::move(key_segments), de_dup_map, ast::StreamSink::BatchWriteArgs()).get();

    //The first key will be de-duped, second key will be fresh because indexes dont match
    ASSERT_EQ(2ul, keys.size());
    ASSERT_EQ(k, to_atom(keys[0]));
    ASSERT_NE(k, to_atom(keys[1]));
    ASSERT_NE(999, to_atom(keys[1]).creation_ts());
    ASSERT_EQ(2, to_atom(keys[1]).version_id());
}

struct DummyTask : arcticdb::async::BaseTask {
    folly::Future<int> operator()() {
        using namespace arcticdb;
        arcticdb::init_random(42);
        ::sleep(3);
        auto x = 0;
        for(auto i = 0; i < 250; ++i) {
            x += arcticdb::random_int();
        }
        return x;
    }
};

struct MetaTask : arcticdb::async::BaseTask {
    folly::Future<int> operator()(int x) {
        return x * 2;
    }
};

struct MaybeThrowTask : arcticdb::async::BaseTask {
    int id_;
    bool do_throw_;
    MaybeThrowTask(int id, bool do_throw) :
        id_(id),
        do_throw_(do_throw) {
    }

    folly::Unit operator()() {
        using namespace arcticdb;
        if(do_throw_)
            log::version().info("Thread {} throwing", id_);
        else
            log::version().info("Thread {} running", id_);

        util::check(!do_throw_, "Had to throw");
        return folly::Unit{};
    }
};

TEST(Async, CollectWithThrow) {
   std::vector<folly::Future<folly::Unit>> stuff;
   using namespace arcticdb;

   async::TaskScheduler sched{20};
   try {
       for(auto i = 0u; i < 1000; ++i) {
           stuff.push_back(sched.submit_io_task(MaybeThrowTask(i, i==3)));
       }
       auto vec_fut = folly::collectAll(stuff).get();
   } catch(std::exception&) {
       log::version().info("Caught something");
   }

   log::version().info("Collect returned");
}