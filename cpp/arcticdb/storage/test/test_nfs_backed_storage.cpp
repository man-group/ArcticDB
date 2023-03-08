/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <gtest/gtest.h>

#include <arcticdb/storage/s3/nfs_backed_storage.hpp>
#include <arcticdb/storage/test/test_s3_storage_common.hpp>
#include <arcticdb/util/composite.hpp>

TEST(TestNfsBackedStorage, basic) {
    auto[cfg, lib_path] = test_vast_path_and_config();
    arcticdb::storage::S3TestForwarder<arcticdb::storage::nfs_backed::NfsBackedStorage> store{lib_path, arcticdb::storage::OpenMode::DELETE, cfg};
    arcticdb::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>("test_symbol");
    arcticdb::entity::AtomKey k2 = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>("test_symbol/suffix");

    try {
        store.do_remove(arcticdb::Composite<ae::VariantKey>(std::vector<arcticdb::entity::VariantKey>{k, k2}));
    } catch(const arcticdb::storage::KeyNotFoundException&) {
        // best effort
    }

    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    as::KeySegmentPair kv2(k2);
    kv2.segment().header().set_start_ts(4321);
    kv2.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> ka{std::move(kv)};
    store.do_write(arcticdb::Composite<as::KeySegmentPair>{std::move(ka)});

    bool executed = false;
    store.do_iterate_type(
        arcticdb::entity::KeyType::TABLE_DATA,
        [&](auto&& found_key) {
            ASSERT_EQ(to_atom(found_key), k);
            executed = true;
            },
            std::string{});

    ASSERT_TRUE(executed);

    std::vector<as::KeySegmentPair> ka2{std::move(kv2)};
    store.do_write(arcticdb::Composite<as::KeySegmentPair>{std::move(ka2)});

    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;
    store.do_read(
        arcticdb::Composite<ae::VariantKey>(std::move(a)),
        [&](auto&& k, auto&& seg) {
            res.atom_key() = std::get<as::AtomKey>(k);
            res.segment() = std::move(seg);
            res.segment().force_own_buffer();
            });

    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    std::vector<arcticdb::entity::VariantKey> a2{k2};
    as::KeySegmentPair res2;
    store.do_read(
        arcticdb::Composite<ae::VariantKey>(std::move(a2)),
        [&](auto&& k, auto&& seg) {
            res2.atom_key() = std::get<as::AtomKey>(k);
            res2.segment() = std::move(seg);
            res2.segment().force_own_buffer();
        });

    ASSERT_EQ(res2.segment().header().start_ts(), 4321);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().header().set_start_ts(4321);
    update_kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> kb{std::move(update_kv)};
    store.do_update(arcticdb::Composite<as::KeySegmentPair>{std::move(kb)}, as::UpdateOpts{});
    std::vector<arcticdb::entity::VariantKey> b{k};
    as::KeySegmentPair update_res;

    store.do_read(
        arcticdb::Composite<ae::VariantKey>(std::move(b)),
        [&](auto&& k, auto&& seg) {
            update_res.atom_key() = std::get<as::AtomKey>(k);
            update_res.segment() = std::move(seg);
            update_res.segment().force_own_buffer();
            });

    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    executed = false;
    store.do_iterate_type(
        arcticdb::entity::KeyType::TABLE_DATA,
        [&](auto&& found_key) {
            ASSERT_TRUE(to_atom(found_key) == k || to_atom(found_key) == k2);
            executed = true;
            },
            std::string{});

    ASSERT_TRUE(executed);
    store.do_remove(arcticdb::Composite<ae::VariantKey>(std::vector<arcticdb::entity::VariantKey>{k, k2}));
}

arcticdb::entity::RefKey write_ref_key(
    arcticdb::entity::StreamId id,
    arcticdb::storage::S3TestForwarder<arcticdb::storage::nfs_backed::NfsBackedStorage>& store,
    arcticdb::entity::timestamp ts) {
    arcticdb::entity::RefKey k{id, ae::KeyType::VERSION_REF};
    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(ts);
    kv.segment().set_buffer(std::make_shared<ac::Buffer>());

    std::vector<as::KeySegmentPair> ka{std::move(kv)};
    store.do_write(ac::Composite<as::KeySegmentPair>(std::move(ka)));

    return k;
}

TEST(TestNfsBackedStorage, refkey) {
    auto[cfg, lib_path] = test_vast_path_and_config();
    arcticdb::storage::S3TestForwarder<arcticdb::storage::nfs_backed::NfsBackedStorage> store{lib_path, arcticdb::storage::OpenMode::DELETE, cfg};
    auto k = write_ref_key("test_ref", store, 1234);
    auto k2 = write_ref_key("test_ref/suffix", store, 4321);

    std::vector<arcticdb::entity::VariantKey> a{k};
    as::KeySegmentPair res;
    store.do_read(
        arcticdb::Composite<ae::VariantKey>(std::move(a)),
        [&](auto&& k, auto&& seg) {
            res.variant_key() = k;
            res.segment() = std::move(seg);
            res.segment().force_own_buffer();
            });

    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    std::vector<arcticdb::entity::VariantKey> a2{k2};
    as::KeySegmentPair res2;
    store.do_read(
        arcticdb::Composite<ae::VariantKey>(std::move(a2)),
        [&](auto&& k, auto&& seg) {
            res2.variant_key() = k;
            res2.segment() = std::move(seg);
            res2.segment().force_own_buffer();
        });

    ASSERT_EQ(res2.segment().header().start_ts(), 4321);
    store.do_remove(arcticdb::Composite<ae::VariantKey>(std::vector<arcticdb::entity::VariantKey>{k, k2}));
}