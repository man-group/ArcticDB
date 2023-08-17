/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <string>
#include <arcticdb/util/test/gtest.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/stream/stream_reader.hpp>
#include <arcticdb/stream/stream_writer.hpp>
#include <arcticdb/storage/store.hpp>

#include <arcticdb/entity/atom_key.hpp>
#include <folly/gen/Base.h>
#include <folly/futures/Future.h>
#include <arcticdb/util/variant.hpp>
#include <arcticdb/pipeline/slicing.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/stream/index.hpp>
#include <filesystem>
#include <arcticdb/entity/protobufs.hpp>

namespace fg = folly::gen;

namespace arcticdb {

template<typename T, typename U>
void check_value(const T& t, const U& u) {
    if(t != u)
        std::cout << "Oops";

    ASSERT_EQ(t, u);
}

using MockAgg = Aggregator<TimeseriesIndex, FixedSchema, NeverSegmentPolicy>;

template<class SegmentFiller>
arcticdb::SegmentInMemory fill_test_data_segment(const StreamDescriptor &tsd, SegmentFiller &&filler) {
    SegmentInMemory seg;
    auto index = index_type_from_descriptor(tsd);

    MockAgg agg{FixedSchema{tsd, index}, [&](auto &&s) {
        seg = std::move(s);
    }};

    filler(agg);
    agg.commit();
    return seg;
}

template<class TsIndexKeyGen>
SegmentInMemory fill_test_index_segment(const StreamId &tsid, TsIndexKeyGen &&ts_key_gen) {
    SegmentInMemory seg;
    IndexAggregator<TimeseriesIndex> idx_agg{tsid, [&](auto &&s) {
        seg = std::move(s);
    }};

    ts_key_gen.foreach([&](auto &key) {
        idx_agg.add_key(key);
    });
    idx_agg.commit();
    return seg;
}

struct PilotedClock {
    static std::atomic<timestamp> time_;
    static timestamp nanos_since_epoch() ARCTICDB_UNUSED {
        return PilotedClock::time_++;
    }

    static void reset() ARCTICDB_UNUSED {
        PilotedClock::time_ = 0;
    }
};

inline auto get_simple_data_descriptor(const StreamId &id) {
    return TimeseriesIndex::default_index().create_stream_descriptor(
        id, {scalar_field(DataType::UINT64, "val")}
    );
}

template <class DataType>
uint64_t digit_mask() {
    return (uint64_t(1) << (std::numeric_limits<DataType>::digits - 1)) - 1;
}

template<typename DataType, std::enable_if_t<std::is_integral_v<DataType>, int> = 0>
DataType get_integral_value_for_offset(size_t start_val, size_t i) {
    return static_cast<DataType>((start_val + i) & digit_mask<DataType>());
}

template<typename DataType, std::enable_if_t<std::is_floating_point_v<DataType>, int> = 0>
DataType get_floating_point_value_for_offset(size_t start_val, size_t i) {
    return DataType(start_val) + i / (10 *digit_mask<DataType>());
}

template<typename DataType, class ContainerType, std::enable_if_t<std::is_integral_v<DataType>, int> = 0>
void fill_test_index_vector(ContainerType &container, DataType, size_t num_rows, size_t start_val) {
    for (size_t i = 0; i < num_rows; ++i) {
        container.push_back(static_cast<DataType>(start_val + i));
    }
}

template<typename DataType, class ContainerType, std::enable_if_t<std::is_integral_v<DataType>, int> = 0>
void fill_test_value_vector(ContainerType &container, DataType, size_t num_rows, size_t start_val) {
    for (size_t i = 0; i < num_rows; ++i) {
        container.push_back(get_integral_value_for_offset<DataType>(start_val, i));
    }
}

template<typename DataType, class ContainerType, std::enable_if_t<std::is_floating_point_v<DataType>, int> = 0>
void fill_test_value_vector(ContainerType &container, DataType, size_t num_rows, size_t start_val) {
    for (size_t i = 0; i < num_rows; ++i) {
        container.push_back(get_floating_point_value_for_offset<DataType>(start_val, i));
    }
}

template<typename DataType, class ContainerType, std::enable_if_t<std::is_floating_point_v<DataType>, int> = 0>
void fill_test_index_vector(ContainerType &container, DataType, size_t num_rows, size_t start_val) {
    for (auto i = start_val; i < start_val + num_rows; ++i) {
        container.push_back(DataType(i));
    }
}

struct DefaultStringGenerator {
    static const stride_t strides_ = 5;

    stride_t strides() { return strides_; }

    static void fill_string_vector(std::vector<uint8_t> &vec, size_t num_rows) ARCTICDB_UNUSED {
        vec.resize(num_rows * strides_);
        const char *strings[] = {"dog", "cat", "horse"};
        for (size_t i = 0; i < num_rows; ++i) {
            memcpy(&vec[i * strides_], &strings[i % 3], sizeof(strings[i % 3]));
        }
    }
};

template<class ContainerType, typename DTT,
    std::enable_if_t<std::is_floating_point_v<typename DTT::raw_type> || std::is_integral_v<typename DTT::raw_type>,
                     int> = 0>
NativeTensor test_column(ContainerType &container, DTT, size_t num_rows, size_t start_val, bool is_index) {
    using RawType = typename DTT::raw_type;
    constexpr auto dt = DTT::data_type;

    shape_t shapes = num_rows;
    stride_t strides;
    ssize_t elsize;

    strides = sizeof(RawType);
    elsize = static_cast<ssize_t>(get_type_size(dt));
    if (is_index)
        fill_test_index_vector(container, RawType{}, num_rows, start_val);
    else
        fill_test_value_vector(container, RawType{}, num_rows, start_val);

    ssize_t bytes = shapes * strides;
    return NativeTensor{bytes, 1, &strides, &shapes, dt, elsize, container.ptr()};
}

template<class ContainerType, typename DTT, class StringGenerator = DefaultStringGenerator>
NativeTensor test_string_column(ContainerType &vec, DTT, size_t num_rows) {
    constexpr auto dt = DTT::data_type;
    shape_t shapes = num_rows;
    stride_t strides;
    ssize_t elsize;

    StringGenerator string_gen;
    strides = string_gen.strides();
    elsize = strides;
    string_gen.fill_string_vector(vec, num_rows);

    ssize_t bytes = shapes * strides;
    return NativeTensor{bytes, 1, &strides, &shapes, dt, elsize, vec.data()};
}

inline std::vector<entity::FieldRef> get_test_timeseries_fields() {
    using namespace arcticdb::entity;

    return {
        scalar_field(DataType::UINT8, "smallints"),
        scalar_field(DataType::INT64, "bigints"),
        scalar_field(DataType::FLOAT64, "floats"),
        scalar_field(DataType::ASCII_FIXED64, "strings"),
    };
}

inline std::vector<entity::FieldRef> get_test_simple_fields() {
    using namespace arcticdb::entity;

    return {
        scalar_field(DataType::UINT32, "index"),
        scalar_field(DataType::FLOAT64,  "floats"),
    };
}

struct TestTensorFrame {
    TestTensorFrame(StreamDescriptor desc, size_t num_rows) :
        segment_(std::move(desc), num_rows) {}

    SegmentInMemory segment_;
    arcticdb::pipelines::InputTensorFrame frame_;
};

template<class ContainerType, typename DTT>
void fill_test_column(arcticdb::pipelines::InputTensorFrame &frame,
                      ContainerType &container,
                      DTT data_type_tag,
                      size_t num_rows,
                      size_t start_val,
                      bool is_index) {
    using RawType = typename decltype(data_type_tag)::raw_type;
    if (!is_index) {
        if constexpr (std::is_integral_v<RawType> || std::is_floating_point_v<RawType>)
            frame.field_tensors.push_back(test_column(container, data_type_tag, num_rows, start_val, is_index));
        else
            frame.field_tensors.push_back(test_string_column(container, data_type_tag, num_rows, start_val, is_index));
    } else {
        if constexpr (std::is_integral_v<RawType>)
            frame.index_tensor =
                std::make_optional<NativeTensor>(test_column(container, data_type_tag, num_rows, start_val, is_index));
        else
            util::raise_rte("Unexpected type in index column");
    }
}

inline void fill_test_frame(SegmentInMemory &segment,
                            arcticdb::pipelines::InputTensorFrame &frame,
                            size_t num_rows,
                            size_t start_val,
                            size_t opt_row_offset) {
    util::check(!segment.descriptor().empty(), "Can't construct test frame with empty descriptor");

    auto field = segment.descriptor().begin();
    if (frame.has_index()) {
        visit_field(*field, [&](auto type_desc_tag) {
            using DTT = typename decltype(type_desc_tag)::DataTypeTag;
            fill_test_column(frame, segment.column(0), DTT{}, num_rows, start_val, true);
        });
        std::advance(field, 1);
    }

    for (; field != segment.descriptor().end(); ++field) {
        visit_field(*field, [&](auto type_desc_tag) {
            using DTT = typename decltype(type_desc_tag)::DataTypeTag;
            fill_test_column(frame,
                             segment.column(std::distance(segment.descriptor().begin(), field)),
                             DTT{},
                             num_rows,
                             start_val + opt_row_offset,
                             false);
        });
    }
    segment.set_row_data(num_rows - 1);
}

template<typename IndexType>
StreamDescriptor get_test_descriptor(const StreamId &id, const std::vector<FieldRef> &fields) {
    return IndexType::default_index().create_stream_descriptor(id, folly::Range(fields.begin(), fields.end()));
}

template<typename IndexType>
TestTensorFrame get_test_frame(const StreamId &id,
                               const std::vector<FieldRef> &fields,
                               size_t num_rows,
                               size_t start_val,
                               size_t opt_row_offset = 0) {
    using namespace arcticdb::pipelines;
    TestTensorFrame output(get_test_descriptor<IndexType>(id, fields), num_rows);

    output.frame_.desc = get_test_descriptor<IndexType>(id, fields);
    output.frame_.index = index_type_from_descriptor(output.frame_.desc);
    output.frame_.num_rows = num_rows;
    output.frame_.desc.set_sorted(SortedValue::ASCENDING);

    fill_test_frame(output.segment_, output.frame_, num_rows, start_val, opt_row_offset);

    output.frame_.set_index_range();

    return output;
}

inline auto get_test_empty_timeseries_segment(StreamId id, size_t num_rows) {
    return SegmentInMemory{get_test_descriptor<stream::TimeseriesIndex>(id, get_test_timeseries_fields()), num_rows};
}

inline auto get_test_timeseries_frame(const StreamId &id, size_t num_rows, size_t start_val) {
    return get_test_frame<stream::TimeseriesIndex>(id, get_test_timeseries_fields(), num_rows, start_val);
}

inline auto get_test_simple_frame(const StreamId &id, size_t num_rows, size_t start_val) {
    return get_test_frame<stream::RowCountIndex>(id, get_test_simple_fields(), num_rows, start_val);
}

inline std::pair<storage::LibraryPath, arcticdb::proto::storage::LibraryConfig> test_config(const std::string &lib_name) {
    auto unique_lib_name = fmt::format("{}_{}", lib_name, util::SysClock::nanos_since_epoch());
    arcticdb::proto::storage::LibraryConfig config;

    config.mutable_lib_desc()->set_name(unique_lib_name);
    auto temp_path = std::filesystem::temp_directory_path();
    // on windows, path is only implicitly converted to wstring, not string
    auto lmdb_config =  arcticdb::storage::lmdb::pack_config(temp_path.string());
    auto library_path = storage::LibraryPath::from_delim_path(unique_lib_name);
    auto storage_id = fmt::format("{}_store", unique_lib_name);
    config.mutable_lib_desc()->add_storage_ids(storage_id);
    config.mutable_storage_by_id()->insert(google::protobuf::MapPair<std::decay_t<decltype(storage_id)>, std::decay_t<decltype(lmdb_config)> >(storage_id, lmdb_config));
    return std::make_pair(library_path, config);
}

inline std::shared_ptr<storage::Library> test_library_from_config(const storage::LibraryPath& lib_path,  const arcticdb::proto::storage::LibraryConfig& lib_cfg) {
    auto storage_cfg = lib_cfg.storage_by_id().at(lib_cfg.lib_desc().storage_ids(0));
    auto vs_cfg = lib_cfg.lib_desc().has_version()
            ? storage::LibraryDescriptor::VariantStoreConfig{lib_cfg.lib_desc().version()}
            : std::monostate{};
    return std::make_shared<storage::Library>(
            lib_path,
            storage::create_storages(lib_path, storage::OpenMode::DELETE, storage_cfg),
            std::move(vs_cfg)
            );

}

/**
 * Uses the argument to create a LibraryConfig for LMDB storage and instantiate a Library with it.
 *
 * Note: the VariantStoreConfig will be monostate. If you need a version store with special config, then inline this
 * function and modify the config.
 */
inline std::shared_ptr<storage::Library> test_library(const std::string &lib_name) {
    auto [lib_path, config] = test_config(lib_name);
    return test_library_from_config(lib_path, config);
}

/**
 * Creates a PythonVersionStore with test_library() (LMDB storage and monostate VariantStoreConfig).
 *
 * See generators.hpp for various in-memory alternatives.
 */
inline auto test_store(const std::string &lib_name) {
    auto library = test_library(lib_name);
    auto version_store = std::make_shared<version_store::PythonVersionStore>(library);
    return version_store;
}

struct TestStore : ::testing::Test {
protected:
    virtual std::string get_name() = 0;

    void SetUp() override {
        test_store_ = test_store(get_name());
    }

    void TearDown() override {
        test_store_->clear();
    }

    std::shared_ptr<arcticdb::version_store::PythonVersionStore> test_store_;
};

} //namespace arcticdb