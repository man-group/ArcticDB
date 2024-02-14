/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/storage/storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>

#ifdef ARCTICDB_INCLUDE_ROCKSDB
#include <arcticdb/storage/rocksdb/rocksdb_storage.hpp>
#endif

#include <filesystem>
#include <stdexcept>
#include <arcticdb/entity/atom_key.hpp>
#include <arcticdb/util/buffer.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>

namespace ac = arcticdb;
namespace as = arcticdb::storage;

class BackendGenerator {
public:
    BackendGenerator(std::string backend) : backend_(std::move(backend)) {}

    std::unique_ptr<as::Storage> new_backend() const {
        if (!fs::exists(TEST_DATABASES_PATH)) {
            fs::create_directories(TEST_DATABASES_PATH);
        }
        if (backend_ == "lmdb") {
            arcticdb::proto::lmdb_storage::Config cfg;
            fs::path db_name = "test_lmdb";
            cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());
            cfg.set_map_size(128ULL * (1ULL << 20) );
            cfg.set_recreate_if_exists(true);

            as::LibraryPath library_path{"a", "b"};
            return std::make_unique<as::lmdb::LmdbStorage>(library_path, as::OpenMode::WRITE, cfg);
        } else if (backend_ == "mem") {
            arcticdb::proto::memory_storage::Config cfg;

            as::LibraryPath library_path{"a", "b"};
            return std::make_unique<as::memory::MemoryStorage>(library_path, as::OpenMode::WRITE, cfg);
        }
#ifdef ARCTICDB_INCLUDE_ROCKSDB
        else if (backend_ == "rocksdb") {
            arcticdb::proto::rocksdb_storage::Config cfg;
            fs::path db_name = "test_rocksdb";
            cfg.set_path((TEST_DATABASES_PATH / db_name).generic_string());

            as::LibraryPath library_path{"a", "b"};
            return std::make_unique<as::rocksdb::RocksDBStorage>(library_path, as::OpenMode::WRITE, cfg);
        }
#endif
        throw std::runtime_error("Unknown backend generator type.");
    }

    void delete_any_test_databases() const {
        if (fs::exists(TEST_DATABASES_PATH)) {
            fs::remove_all(TEST_DATABASES_PATH);
        }
    }

    std::string get_name() const {return backend_;}
private:
    const std::string backend_;
    inline static const fs::path TEST_DATABASES_PATH = "./test_databases";
};

class SimpleTestSuite : public testing::TestWithParam<BackendGenerator> {
    void SetUp() override {
        GetParam().delete_any_test_databases();
    }

    void TearDown() override {
       GetParam().delete_any_test_databases();
    }
};

TEST_P(SimpleTestSuite, ConstructDestruct) {
    // The rocksdb destructor was hard to get working, so this is useful
    std::unique_ptr<as::Storage> storage = GetParam().new_backend();
}

TEST_P(SimpleTestSuite, Example) {
    std::unique_ptr<as::Storage> storage = GetParam().new_backend();
    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(999);

    as::KeySegmentPair kv(k);
    kv.segment().set_buffer(std::make_shared<Buffer>());

    storage->write(std::move(kv));

    ASSERT_TRUE(storage->key_exists(k));

    as::KeySegmentPair res;
    storage->read(k, [&](auto &&k, auto &&seg) {
        res.atom_key() = std::get<AtomKey>(k);
        res.segment() = std::move(seg);
        res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, storage::ReadKeyOpts{});

    res = storage->read(k, as::ReadKeyOpts{});

    bool executed = false;
    storage->iterate_type(arcticdb::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);

    as::KeySegmentPair update_kv(k);
    update_kv.segment().set_buffer(std::make_shared<Buffer>());

    storage->update(std::move(update_kv), as::UpdateOpts{});

    as::KeySegmentPair update_res;
    storage->read(k, [&](auto &&k, auto &&seg) {
        update_res.atom_key() = std::get<AtomKey>(k);
        update_res.segment() = std::move(seg);
        update_res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});

    update_res = storage->read(k, as::ReadKeyOpts{});

    executed = false;
    storage->iterate_type(arcticdb::entity::KeyType::TABLE_DATA,
                         [&](auto &&found_key) {
                             ASSERT_EQ(to_atom(found_key), k);
                             executed = true;
                         });
    ASSERT_TRUE(executed);
}

TEST_P(SimpleTestSuite, Strings) {
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
    metadata.mutable_proto().set_total_rows(12);
    metadata.mutable_proto().mutable_stream_descriptor()->CopyFrom(s.descriptor().proto());
    any.PackFrom(metadata.proto());
    s.set_metadata(std::move(any));

    arcticdb::proto::encoding::VariantCodec opt;
    auto lz4ptr = opt.mutable_lz4();
    lz4ptr->set_acceleration(1);
    Segment seg = encode_dispatch(s.clone(), opt, EncodingVersion::V1);

    auto environment_name = as::EnvironmentName{"res"};
    auto storage_name = as::StorageName{"lmdb_01"};

    std::unique_ptr<as::Storage> storage = GetParam().new_backend();

    ac::entity::AtomKey k = ac::entity::atom_key_builder().gen_id(1).build<ac::entity::KeyType::TABLE_DATA>(999);
    auto save_k = k;
    as::KeySegmentPair kv(std::move(k), std::move(seg));
    storage->write(std::move(kv));

    as::KeySegmentPair res;
    storage->read(save_k, [&](auto &&k, auto &&seg) {
        res.atom_key() = std::get<AtomKey>(k);
        res.segment() = std::move(seg);
        res.segment().force_own_buffer(); // necessary since the non-owning buffer won't survive the visit
    }, as::ReadKeyOpts{});

    SegmentInMemory res_mem = decode_segment(std::move(res.segment()));
    ASSERT_EQ(s.string_at(0, 1), res_mem.string_at(0, 1));
    ASSERT_EQ(std::string("happy"), res_mem.string_at(0, 1));
    ASSERT_EQ(s.string_at(1, 3), res_mem.string_at(1, 3));
    ASSERT_EQ(std::string("baggy"), res_mem.string_at(1, 3));
}

using namespace std::string_literals;

std::vector<BackendGenerator> get_backend_generators() {
    std::vector<BackendGenerator> backend_generators{"lmdb"s, "mem"s};
#ifdef ARCTICDB_INCLUDE_ROCKSDB
    backend_generators.emplace_back("rocksdb"s);
#endif
    return backend_generators;
}
auto backend_generators = get_backend_generators();

INSTANTIATE_TEST_SUITE_P(TestEmbedded, SimpleTestSuite, testing::ValuesIn(backend_generators),
    [](const testing::TestParamInfo<SimpleTestSuite::ParamType>& info) { return info.param.get_name(); });
