/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/column_store/column.hpp>
#include <arcticdb/pipeline/input_tensor_frame.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/storage/storages.hpp>
#include <arcticdb/storage/variant_storage.hpp>
#include <arcticdb/storage/test/in_memory_store.hpp>
#include <arcticdb/stream/segment_aggregator.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <random>

namespace arcticdb {

struct SegmentsSink {
    SegmentsSink() = default;

    std::vector<SegmentInMemory> segments_;

    ARCTICDB_MOVE_ONLY_DEFAULT(SegmentsSink)
};

template<typename CommitFunc>
auto get_test_aggregator(CommitFunc&& func, StreamId stream_id, std::vector<FieldDescriptor>&& fields)
{
    using namespace arcticdb::stream;
    using TestAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
    auto index = TimeseriesIndex::default_index();

    FixedSchema schema{index.create_stream_descriptor(std::move(stream_id), fields_proto_from_range(fields)), index};

    return TestAggregator(std::move(schema), std::forward<CommitFunc>(func), stream::NeverSegmentPolicy{});
}

template<typename AggregatorType>
struct SinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType = typename AggregatorType::IndexType;

    SinkWrapperImpl(StreamId stream_id, std::initializer_list<FieldDescriptor::Proto> fields)
        : index_(IndexType::default_index()),
          sink_(std::make_shared<SegmentsSink>()),
          aggregator_(
              SchemaPolicy{index_.create_stream_descriptor(std::move(stream_id), fields), index_},
              [=](SegmentInMemory&& mem) { sink_->segments_.push_back(std::move(mem)); },
              typename AggregatorType::SegmentingPolicyType{})
    {
    }

    auto& segment()
    {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    IndexType index_;
    std::shared_ptr<SegmentsSink> sink_;
    AggregatorType aggregator_;
};

using TestAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SinkWrapper = SinkWrapperImpl<TestAggregator>;
using TestSparseAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy, SparseColumnPolicy>;
using SparseSinkWrapper = SinkWrapperImpl<TestSparseAggregator>;

inline SegmentInMemory get_standard_timeseries_segment(const std::string& name, size_t num_rows = 10)
{
    auto wrapper = SinkWrapper(name,
        {scalar_field_proto(DataType::INT8, "int8"),
            scalar_field_proto(DataType::UINT64, "uint64"),
            scalar_field_proto(DataType::UTF_DYNAMIC64, "strings")});

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto&& rb) {
            rb.set_scalar(1, int8_t(i));
            rb.set_scalar(2, uint64_t(i) * 2);
            rb.set_string(3, fmt::format("string_{}", i));
        });
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_groupable_timeseries_segment(const std::string& name,
    size_t rows_per_group,
    std::initializer_list<size_t> group_ids)
{
    auto wrapper = SinkWrapper(name,
        {
            scalar_field_proto(DataType::INT8, "int8"),
            scalar_field_proto(DataType::UTF_DYNAMIC64, "strings"),
        });

    int i = 0;
    for (auto group_id : group_ids) {
        for (size_t j = 0; j < rows_per_group; j++) {
            wrapper.aggregator_.start_row(timestamp{static_cast<timestamp>(rows_per_group * i + j)})([&](auto&& rb) {
                rb.set_scalar(1, int8_t(group_id));
                rb.set_string(2, fmt::format("string_{}", group_id));
            });
        }
        i++;
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_sparse_timeseries_segment(const std::string& name, size_t num_rows = 10)
{
    auto wrapper = SparseSinkWrapper(name,
        {
            scalar_field_proto(DataType::INT8, "int8"),
            scalar_field_proto(DataType::UINT64, "uint64"),
            scalar_field_proto(DataType::UTF_DYNAMIC64, "strings"),
        });

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto&& rb) {
            rb.set_scalar(1, int8_t(i));
            if (i % 2 == 1)
                rb.set_scalar(2, uint64_t(i) * 2);

            if (i % 3 == 2)
                rb.set_string(3, fmt::format("string_{}", i));
        });
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_sparse_timeseries_segment_floats(const std::string& name, size_t num_rows = 10)
{
    auto wrapper = SparseSinkWrapper(name,
        {
            scalar_field_proto(DataType::FLOAT64, "col1"),
            scalar_field_proto(DataType::FLOAT64, "col2"),
            scalar_field_proto(DataType::FLOAT64, "col3"),
        });

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto&& rb) {
            rb.set_scalar(1, double(i));
            if (i % 2 == 1)
                rb.set_scalar(2, double(i) * 2);

            if (i % 3 == 2)
                rb.set_scalar(3, double(i) / 2);
        });
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

/**
 * Creates the first two c'tor arguments needed to create a MemoryStorage-backed Library.
 *
 * To use LMDB storage, try the factories in stream_test_common.hpp.
 */
inline auto get_test_config_data()
{
    using namespace arcticdb::storage;
    LibraryPath path{"test", "store"};
    auto storages = create_storages(path, OpenMode::DELETE, memory::pack_config(1000));
    return std::make_tuple(path, std::move(storages));
}

/**
 * Creates a LocalVersionedEngine from get_test_config_data().
 *
 * See also python_version_store_in_memory() and stream_test_common.hpp for alternatives using LMDB.
 */
template<typename VerStoreType = version_store::LocalVersionedEngine>
inline VerStoreType get_test_engine(LibraryDescriptor::VariantStoreConfig cfg = {})
{
    auto [path, storages] = get_test_config_data();
    auto library = std::make_shared<arcticdb::storage::Library>(path, std::move(storages), std::move(cfg));
    return VerStoreType(library);
}

/**
 * Similar to get_test_engine() but replaces the AsyncStore with InMemoryStore and returns it.
 */
inline auto python_version_store_in_memory()
{
    auto pvs = get_test_engine<version_store::PythonVersionStore>();
    auto replace_store = std::make_shared<InMemoryStore>();
    pvs._test_set_store(replace_store);
    return std::make_tuple(std::move(pvs), replace_store);
}

inline constexpr ssize_t to_tensor_dim(Dimension dim)
{
    return static_cast<int>(dim) + 1;
}

inline NativeTensor tensor_from_column(const Column& column)
{
    return column.type().visit_tag([&column](auto&& tag) {
        using TypeDescriptorTag = std::decay_t<decltype(tag)>;
        shape_t scalar_shape = 0;
        const shape_t* shape_ptr;
        constexpr auto dim = TypeDescriptorTag::DimensionTag::value;
        constexpr auto data_type = TypeDescriptorTag::DataTypeTag::data_type;
        if constexpr (dim == Dimension::Dim0) {
            scalar_shape = column.row_count();
            shape_ptr = &scalar_shape;
        } else {
            shape_ptr = column.shape_ptr();
        }

        auto tensor = NativeTensor{static_cast<ssize_t>(column.bytes()),
            to_tensor_dim(dim),
            nullptr,
            shape_ptr,
            data_type,
            get_type_size(data_type),
            column.ptr()};
        return tensor;
    });
}

struct SegmentToInputFrameAdapter {
    SegmentInMemory segment_;
    pipelines::InputTensorFrame input_frame_;

    explicit SegmentToInputFrameAdapter(SegmentInMemory&& segment)
        : segment_(std::move(segment))
    {
        input_frame_.desc = segment_.descriptor();
        input_frame_.num_rows = segment_.row_count();
        size_t col{0};
        if (segment_.descriptor().index().type() != IndexDescriptor::ROWCOUNT) {
            for (size_t i = 0; i < segment_.descriptor().index().field_count(); ++i) {
                input_frame_.index_tensor = tensor_from_column(segment_.column(col));
                ++col;
            }
        }

        while (col < segment_.num_columns()) {
            input_frame_.field_tensors.push_back(tensor_from_column(segment_.column(col++)));
        }

        input_frame_.set_index_range();
    }

    void synthesize_norm_meta()
    {
        // Copied from read_incompletes_to_pipeline()
        if (segment_.metadata()) {
            auto segment_tsd = timeseries_descriptor_from_segment(segment_);
            input_frame_.norm_meta.CopyFrom(segment_tsd.normalization());
        }
        ensure_norm_meta(input_frame_.norm_meta, input_frame_.desc.id(), false);
    }
};

template<typename AggregatorType>
struct SegmentSinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType = typename AggregatorType::IndexType;

    SegmentSinkWrapperImpl(StreamId stream_id, const IndexType& index, std::vector<FieldDescriptor>&& fields)
        : sink_(std::make_shared<SegmentsSink>()),
          aggregator_([](const pipelines::FrameSlice&) {},
              SchemaPolicy{index.create_stream_descriptor(std::move(stream_id), fields_proto_from_range(fields)),
                  index},
              [=](SegmentInMemory&& mem) { sink_->segments_.push_back(std::move(mem)); },
              typename AggregatorType::SegmentingPolicyType{})
    {
    }

    auto& segment()
    {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    std::shared_ptr<SegmentsSink> sink_;
    AggregatorType aggregator_;
};

using TestSegmentAggregatorNoSegment = SegmentAggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SegmentSinkWrapper = SegmentSinkWrapperImpl<TestSegmentAggregatorNoSegment>;

} //namespace arcticdb