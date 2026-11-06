/* Copyright 2026 Man Group Operations Limited
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

#include <atomic>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/storage/storage_exceptions.hpp>
#include <arcticdb/util/configs_map.hpp>
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

ac::entity::AtomKey test_key(int64_t id) {
    return ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(NumericId{id});
}

as::KeySegmentPair test_key_segment(int64_t id) {
    auto segment_in_memory = get_test_frame<arcticdb::stream::TimeseriesIndex>("symbol", {}, 10, 0).segment_;
    auto codec_opts = proto::encoding::VariantCodec();
    auto segment = encode_dispatch(std::move(segment_in_memory), codec_opts, arcticdb::EncodingVersion::V2);
    return as::KeySegmentPair{test_key(id), std::move(segment)};
}

// Not run over MemoryStorage: its map is not written under a lock. Parameterised over LMDBStorage.GroupCommit.
class LmdbConcurrentWrites : public testing::TestWithParam<int64_t> {
  protected:
    void SetUp() override {
        generator_.delete_any_test_databases();
        storage_ = generator_.new_storage();
    }
    void TearDown() override {
        storage_.reset();
        generator_.delete_any_test_databases();
    }
    arcticdb::ScopedConfig group_commit_{"LMDBStorage.GroupCommit", GetParam()};
    StorageGenerator generator_{"lmdb"s};
    std::unique_ptr<as::Storage> storage_;
};

constexpr int64_t concurrent_writer_count = 8;
constexpr int64_t keys_per_writer = 16;
constexpr int64_t duplicate_key_rounds = 100;

TEST_P(LmdbConcurrentWrites, EveryKeyLandsAndIsReadableOnceAcknowledged) {
    std::atomic<int64_t> unreadable{0};
    std::vector<std::thread> writers;
    for (int64_t writer = 0; writer < concurrent_writer_count; ++writer) {
        writers.emplace_back([this, &unreadable, writer]() {
            for (int64_t index = 0; index < keys_per_writer; ++index) {
                const int64_t id = writer * keys_per_writer + index;
                storage_->write(test_key_segment(id));
                if (!storage_->key_exists(test_key(id))) {
                    ++unreadable;
                }
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }

    ASSERT_EQ(unreadable.load(), 0);
    size_t found = 0;
    storage_->iterate_type(ac::entity::KeyType::TABLE_DATA, [&found](auto&&) { ++found; });
    ASSERT_EQ(found, size_t(concurrent_writer_count * keys_per_writer));
    for (int64_t id = 0; id < concurrent_writer_count * keys_per_writer; ++id) {
        ASSERT_TRUE(storage_->key_exists(test_key(id))) << "missing key " << id;
    }
}

TEST_P(LmdbConcurrentWrites, AFailingKeyOnlyFailsItsOwnWriter) {
    // Each round, one of the concurrent writers rewrites an existing key and must be the only one to fail.
    for (int64_t round = 0; round < duplicate_key_rounds; ++round) {
        const int64_t first_id = round * concurrent_writer_count;
        storage_->write(test_key_segment(first_id));

        std::atomic<int> duplicates{0};
        std::atomic<int> other_errors{0};
        std::vector<std::thread> writers;
        for (int64_t writer = 0; writer < concurrent_writer_count; ++writer) {
            writers.emplace_back([this, &duplicates, &other_errors, id = first_id + writer]() {
                try {
                    storage_->write(test_key_segment(id));
                } catch (const as::DuplicateKeyException&) {
                    ++duplicates;
                } catch (...) {
                    ++other_errors;
                }
            });
        }
        for (auto& writer : writers) {
            writer.join();
        }

        ASSERT_EQ(duplicates.load(), 1) << "round " << round;
        ASSERT_EQ(other_errors.load(), 0) << "round " << round;
        for (int64_t id = first_id; id < first_id + concurrent_writer_count; ++id) {
            ASSERT_TRUE(storage_->key_exists(test_key(id))) << "missing key " << id;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
        GroupCommit, LmdbConcurrentWrites, testing::Values(int64_t{1}, int64_t{0}),
        [](const testing::TestParamInfo<int64_t>& info) { return info.param ? "On"s : "Off"s; }
);

std::vector<StorageGenerator> get_storage_generators() { return {"lmdb"s, "mem"s}; }

INSTANTIATE_TEST_SUITE_P(
        TestLocalStorages, LocalStorageTestSuite, testing::ValuesIn(get_storage_generators()),
        [](const testing::TestParamInfo<LocalStorageTestSuite::ParamType>& info) { return info.param.get_name(); }
);

} // namespace