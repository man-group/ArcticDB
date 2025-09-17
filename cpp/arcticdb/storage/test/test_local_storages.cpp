/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>

#include <filesystem>
#include <stdexcept>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>

namespace {

namespace ac = arcticdb;
namespace as = arcticdb::storage;

class LocalStorageTestSuite : public testing::TestWithParam<StorageGenerator> {
    void SetUp() override { GetParam().delete_any_test_databases(); }

    void TearDown() override { GetParam().delete_any_test_databases(); }
};

TEST_P(LocalStorageTestSuite, ConstructDestruct) { std::unique_ptr<as::Storage> storage = GetParam().new_storage(); }

TEST_P(LocalStorageTestSuite, CoreFunctions) {
    std::unique_ptr<as::Storage> storage = GetParam().new_storage();
    ac::entity::AtomKey k =
            ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(NumericId{999});

    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    auto segment = encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
    arcticdb::storage::KeySegmentPair kv(k, std::move(segment));

    storage->write(std::move(kv));

    ASSERT_TRUE(storage->key_exists(k));

    as::KeySegmentPair res;
    storage->read(
            k,
            [&](auto&& k, auto&& seg) {
                auto key_copy = k;
                res = as::KeySegmentPair{std::move(key_copy), std::move(seg)};
                res.segment_ptr()->force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
            },
            storage::ReadKeyOpts{}
    );

    res = storage->read(k, as::ReadKeyOpts{});

    bool executed = false;
    storage->iterate_type(arcticdb::entity::KeyType::TABLE_DATA, [&](auto&& found_key) {
        ASSERT_EQ(to_atom(found_key), k);
        executed = true;
    });
    ASSERT_TRUE(executed);

    segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    codec_opts = proto::encoding::VariantCodec();
    segment = encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
    arcticdb::storage::KeySegmentPair update_kv(k, std::move(segment));

    storage->update(std::move(update_kv), as::UpdateOpts{});

    as::KeySegmentPair update_res;
    storage->read(
            k,
            [&](auto&& k, auto&& seg) {
                auto key_copy = k;
                update_res = as::KeySegmentPair{std::move(key_copy), std::move(seg)};
                update_res.segment_ptr()->force_own_buffer(
                ); // necessary since the non-owning buffer won't survive the visit
            },
            as::ReadKeyOpts{}
    );

    update_res = storage->read(k, as::ReadKeyOpts{});

    executed = false;
    storage->iterate_type(arcticdb::entity::KeyType::TABLE_DATA, [&](auto&& found_key) {
        ASSERT_EQ(to_atom(found_key), k);
        executed = true;
    });
    ASSERT_TRUE(executed);
}

TEST_P(LocalStorageTestSuite, Strings) {
    auto tsd = create_tsd<DataTypeTag<DataType::ASCII_DYNAMIC64>, Dimension::Dim0>();
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
    metadata.set_total_rows(12);
    metadata.set_stream_descriptor(s.descriptor());
    any.PackFrom(metadata.proto());
    s.set_metadata(std::move(any));

    arcticdb::proto::encoding::VariantCodec opt;
    auto lz4ptr = opt.mutable_lz4();
    lz4ptr->set_acceleration(1);
    Segment seg = encode_dispatch(s.clone(), opt, EncodingVersion::V1);

    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"lmdb_01"};

    std::unique_ptr<as::Storage> storage = GetParam().new_storage();

    ac::entity::AtomKey k =
            ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(NumericId{999});
    auto save_k = k;
    as::KeySegmentPair kv(std::move(k), std::move(seg));
    storage->write(std::move(kv));

    as::KeySegmentPair res;
    storage->read(
            save_k,
            [&](auto&& k, auto&& seg) {
                auto key_copy = k;
                res = as::KeySegmentPair{std::move(key_copy), std::move(seg)};
                res.segment_ptr()->force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
            },
            as::ReadKeyOpts{}
    );

    SegmentInMemory res_mem = decode_segment(*res.segment_ptr());
    ASSERT_EQ(s.string_at(0, 1), res_mem.string_at(0, 1));
    ASSERT_EQ(std::string("happy"), res_mem.string_at(0, 1));
    ASSERT_EQ(s.string_at(1, 3), res_mem.string_at(1, 3));
    ASSERT_EQ(std::string("baggy"), res_mem.string_at(1, 3));
}

using namespace std::string_literals;

std::vector<StorageGenerator> get_storage_generators() { return {"lmdb"s, "mem"s}; }

INSTANTIATE_TEST_SUITE_P(
        TestLocalStorages, LocalStorageTestSuite, testing::ValuesIn(get_storage_generators()),
        [](const testing::TestParamInfo<LocalStorageTestSuite::ParamType>& info) { return info.param.get_name(); }
);

} // namespace