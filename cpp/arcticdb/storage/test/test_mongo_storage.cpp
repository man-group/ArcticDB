/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>

#include <thread>
#include <filesystem>
#include <mongocxx/uri.hpp>

const std::string test_server("mongodb://localhost:27017");

namespace fs = std::filesystem;

TEST(MongoStorage, ClientSession) {
    return; // need to run local mongo with /opt/mongo/bin/mongod --dbpath /tmp/something
    namespace ac = arcticdb;
    namespace as = arcticdb::storage;
    namespace asmongo = arcticdb::storage::mongo;

    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"mongo 01"};

    arcticdb::proto::mongo_storage::Config cfg;
    cfg.set_uri(test_server.c_str());

    asmongo::MongoStorage storage({"testdb", "stuff"}, as::OpenMode::WRITE, cfg);

    std::array<std::string, 2> lib_parts{"testdb", "stuff"};
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>("999");

    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);

    storage.write(std::move(kv));

    as::KeySegmentPair res;

    storage.read(k, [&](auto &&k, auto &&seg) {
        res.atom_key() = std::get<as::AtomKey>(k);
        res.segment() = std::move(seg);
        res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});
    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    res = storage.read(k, as::ReadKeyOpts{});
    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    bool executed = false;
    storage.iterate_type(ac::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });

    storage.iterate_type(ac::entity::KeyType::SNAPSHOT,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().header().set_start_ts(4321);

    storage.update(std::move(update_kv), as::UpdateOpts{});

    as::KeySegmentPair update_res;

    storage.read(k, [&](auto &&k, auto &&seg) {
        update_res.atom_key() = std::get<as::AtomKey>(k);
        update_res.segment() = std::move(seg);
        update_res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});
    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    update_res = storage.read(k, as::ReadKeyOpts{});
    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    executed = false;
    storage.iterate_type(ac::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);

    ac::entity::AtomKey numeric_k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::STREAM_GROUP>(ac::entity::NumericId{999});
    as::KeySegmentPair numeric_kv(numeric_k);
    numeric_kv.segment().header().set_start_ts(7890);

    storage.write(std::move(numeric_kv));

    as::KeySegmentPair numeric_res;

    storage.read(numeric_k, [&](auto &&k, auto &&seg) {
        numeric_res.atom_key() = std::get<as::AtomKey>(k);
        numeric_res.segment() = std::move(seg);
        numeric_res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});
    ASSERT_EQ(numeric_res.segment().header().start_ts(), 7890);

    numeric_res = storage.read(numeric_k, as::ReadKeyOpts{});
    ASSERT_EQ(numeric_res.segment().header().start_ts(), 7890);
}
