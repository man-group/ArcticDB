/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>

#include <arcticdb/storage/test/test_s3_storage_common.hpp>
#include <arcticdb/util/composite.hpp>

TEST(TestS3Storage, basic) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;

    ac::entity::AtomKey
            k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>("test_symbol");

    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> ka{std::move(kv)};
    arcticdb::storage::s3::detail::do_write_impl(arcticdb::Composite<as::KeySegmentPair>{std::move(ka)}, root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(a)),
            [&](auto&& k, auto&& seg) {
                res.atom_key() = std::get<as::AtomKey>(k);
                res.segment() = std::move(seg);
                res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{});

    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    bool executed = false;
    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_DATA,
            [&](auto&& found_key) {
                ASSERT_EQ(to_atom(found_key), k);
                executed = true;
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());

    ASSERT_TRUE(executed);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().header().set_start_ts(4321);
    update_kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> kb{std::move(update_kv)};
    arcticdb::storage::s3::detail::do_update_impl(arcticdb::Composite<as::KeySegmentPair>{std::move(kb)}, root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
    std::vector<arcticdb::entity::VariantKey> b{k};
    as::KeySegmentPair update_res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(b)),
            [&](auto&& k, auto&& seg) {
                update_res.atom_key() = std::get<as::AtomKey>(k);
                update_res.segment() = std::move(seg);
                update_res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{});

    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    executed = false;
    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_DATA,
            [&](auto&& found_key) {
                ASSERT_EQ(to_atom(found_key), k);
                executed = true;
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());

    ASSERT_TRUE(executed);

}

TEST(TestS3Storage, refkey) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;
    ae::RefKey k{"test_ref_key", ae::KeyType::VERSION_REF};
    write_test_ref_key(k, root_folder, bucket_name, client);
    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;

    arcticdb::storage::s3::detail::do_read_impl(
            arcticdb::Composite<ae::VariantKey>(std::move(a)),
            [&](auto&& k, auto&& seg) {
                res.variant_key() = k;
                res.segment() = std::move(seg);
                res.segment().force_own_buffer();
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{});

    ASSERT_EQ(res.segment().header().start_ts(), 1234);
}

void stress_test_s3_storage() {
 //   auto[cfg, lib_path] = test_s3_path_and_config();
 //   auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

    const int NumThreads = 100;
    const int Repeats = 1000;

    ae::RefKey key{"test_ref_key", ae::KeyType::VERSION_REF};

    as::KeySegmentPair kv(key);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());
//    std::array<as::KeySegmentPair, 1> ka{std::move(kv)};
//    arcticdb::storage::s3::detail::do_write_impl(folly::range(ka), root_folder, bucket_name, client);
    const bool TestRead = false;
    std::atomic<int> write_count{0};

    auto thread_func = [&write_count, &key] () {
        auto[cfg, lib_path] = test_s3_path_and_config();
        auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

        for (auto i = 0; i < Repeats; ++i) {
            bool written = false;
            if(!written && write_count == i) {
                arcticdb::log::storage().info("Thread {} writing", thread_id());
                written = true;
                ++write_count;
                write_test_ref_key(key, root_folder, bucket_name, client);
            }
            if(i % 100 == 1)
                arcticdb::log::storage().info("thread {} done {} repeats", thread_id(), i);
            if(TestRead) {
                as::KeySegmentPair res;

                arcticdb::storage::s3::detail::do_read_impl(
                        arcticdb::Composite<ae::VariantKey>(std::move(key)),
                        [&](auto&& k, auto&& seg) {
                            res.variant_key() = k;
                            res.segment() = std::move(seg);
                            res.segment().force_own_buffer();
                        },
                        root_folder,
                        bucket_name,
                        client,
                        as::s3::detail::FlatBucketizer{});
            }
            else {
                bool res = arcticdb::storage::s3::detail::do_key_exists_impl(
                        key,
                        root_folder,
                        bucket_name,
                        client,
                        as::s3::detail::FlatBucketizer{});

                if (!res)
                    throw std::runtime_error("failed");
            }
        }
    };

    std::vector<std::unique_ptr<std::thread>> threads;

    for(auto i = 0; i < NumThreads; ++i) {
        threads.push_back(std::make_unique<std::thread>(thread_func));
    }

    for(auto& thread : threads)
        thread->join();
}


/* This test exposes an open issue with Pure S3 where if a file exists and a new one is
 * written to the same key, the file can return KeyNotFound as it is being updated.
 */
TEST(TestS3Storage, ReadStress) {
#ifndef USE_FAKE_CLIENT
    //stress_test_s3_storage();
#endif
}

TEST(TestS3Storage, remove) {
    auto[cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = GET_S3;
    std::vector<as::KeySegmentPair> writes;
    std::vector<ac::entity::VariantKey> keys ;
    for (size_t i = 0; i < 100; i++) {
        ac::entity::AtomKey
                k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_INDEX>("test_symbol" + std::to_string(i));

        as::KeySegmentPair kv(k);
        kv.segment().header().set_start_ts(i);
        kv.segment().set_buffer(std::make_shared<ac::Buffer>());

        writes.emplace_back(std::move(kv));
        keys.emplace_back(std::move(k));
    }
    arcticdb::storage::s3::detail::do_write_impl(arcticdb::Composite<as::KeySegmentPair>(std::move(writes)), root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});

    arcticdb::storage::s3::detail::do_remove_impl(arcticdb::Composite<ae::VariantKey>(std::move(keys)), root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});

    arcticdb::storage::s3::detail::do_iterate_type_impl(
            arcticdb::entity::KeyType::TABLE_INDEX,
            [&](auto && found_key) {
                ASSERT_TRUE(std::find(keys.begin(), keys.end(), found_key) == keys.end());
            },
            root_folder,
            bucket_name,
            client,
            as::s3::detail::FlatBucketizer{},
            as::s3::detail::default_prefix_handler());
}

TEST(TestS3Storage, remove_ignores_missing) {
    auto [cfg, lib_path] = test_s3_path_and_config();
    auto [root_folder, bucket_name, client] = real_s3_storage(lib_path, cfg);

    ae::RefKey k{"should_not_exist", ae::KeyType::VERSION_REF};
    arcticdb::storage::s3::detail::do_remove_impl(arcticdb::Composite<ae::VariantKey>(k),
                                                 root_folder, bucket_name, client, as::s3::detail::FlatBucketizer{});
}
