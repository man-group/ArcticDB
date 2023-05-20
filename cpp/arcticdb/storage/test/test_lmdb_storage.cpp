/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>

#include <google/protobuf/util/message_differencer.h>
#include <filesystem>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/protobuf_mappings.hpp>

namespace ac = arcticdb;
namespace as = arcticdb::storage;
namespace asl = arcticdb::storage::lmdb;

TEST(TestLmdbStorage, Example) {
    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"lmdb_01"};

    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path("./");
    cfg.set_recreate_if_exists(true);

    asl::LmdbStorage storage({"A", "BB"}, as::OpenMode::WRITE, cfg);

    std::vector<std::string> lib_parts{"A", "BB"};
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(999);

    as::KeySegmentPair kv(k);
    kv.segment().header().set_start_ts(1234);
    kv.segment().set_buffer(std::make_shared<Buffer>());

    storage.write(std::move(kv));

    as::KeySegmentPair res;
    storage.read(k, [&](auto &&k, auto &&seg) {
        res.atom_key() = std::get<AtomKey>(k);
        res.segment() = std::move(seg);
        res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, storage::ReadKeyOpts{});
    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    res = storage.read(k, as::ReadKeyOpts{});
    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    bool executed = false;
    storage.iterate_type(arcticdb::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().header().set_start_ts(4321);
    update_kv.segment().set_buffer(std::make_shared<Buffer>());

    storage.update(std::move(update_kv), as::UpdateOpts{});

    as::KeySegmentPair update_res;
    storage.read(k, [&](auto &&k, auto &&seg) {
        update_res.atom_key() = std::get<AtomKey>(k);
        update_res.segment() = std::move(seg);
        update_res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});
    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    update_res = storage.read(k, as::ReadKeyOpts{});
    ASSERT_EQ(update_res.segment().header().start_ts(), 4321);

    executed = false;
    storage.iterate_type(arcticdb::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);
}

TEST(TestLmdbStorage, Strings) {
    const auto tsd = create_tsd<DataTypeTag<DataType::ASCII_DYNAMIC64>, Dimension::Dim0>();
    SegmentInMemory s{StreamDescriptor{std::move(tsd)}};
    s.set_scalar(0, timestamp(123));
    s.set_string(1, "happy");
    s.set_string(2, "muppets");
    s.set_string(3, "happy");
    s.set_string(4, "trousers");
    s.end_row();
    s.set_scalar(0, timestamp(124));
    s.set_string(1, "soggy");
    s.set_string(2, "muppets");
    s.set_string(3, "baggy");
    s.set_string(4, "trousers");
    s.end_row();

    google::protobuf::Any any;
    arcticdb::TimeseriesDescriptor metadata;
    metadata.mutable_proto().set_total_rows(12);
    metadata.mutable_proto().mutable_stream_descriptor()->CopyFrom(s.descriptor().proto());
    any.PackFrom(metadata.proto());
    s.set_metadata(std::move(any));

    arcticdb::proto::encoding::VariantCodec opt;
    auto lz4ptr = opt.mutable_lz4();
    lz4ptr->set_acceleration(1);
    Segment seg = encode_v1(s.clone(), opt);

    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"lmdb_01"};

    arcticdb::proto::lmdb_storage::Config cfg;
    cfg.set_path("./");
    cfg.set_recreate_if_exists(true);

    asl::LmdbStorage storage({"A", "BB"}, as::OpenMode::WRITE, cfg);
    std::vector<std::string> lib_parts{"A", "BB"};
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(999);
    auto save_k = k;
    as::KeySegmentPair kv(std::move(k), std::move(seg));
    kv.segment().header().set_start_ts(1234);
    storage.write(std::move(kv));

    as::KeySegmentPair res;
    storage.read(save_k, [&](auto &&k, auto &&seg) {
        res.atom_key() = std::get<AtomKey>(k);
        res.segment() = std::move(seg);
        res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});
    ASSERT_EQ(res.segment().header().start_ts(), 1234);

    SegmentInMemory res_mem = decode_segment(std::move(res.segment()));
    ASSERT_EQ(s.string_at(0, 1), res_mem.string_at(0, 1));
    ASSERT_EQ(std::string("happy"), res_mem.string_at(0, 1));
    ASSERT_EQ(s.string_at(1, 3), res_mem.string_at(1, 3));
    ASSERT_EQ(std::string("baggy"), res_mem.string_at(1, 3));
}

