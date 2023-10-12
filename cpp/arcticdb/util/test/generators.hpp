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
#include <arcticdb/storage/memory/memory_storage.hpp>
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
auto get_test_aggregator(CommitFunc &&func, StreamId stream_id, std::vector<FieldRef> &&fields) {
    using namespace arcticdb::stream;
    using TestAggregator =  Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
    auto index = TimeseriesIndex::default_index();

    FixedSchema schema{
        index.create_stream_descriptor(std::move(stream_id), fields_from_range(fields)), index
    };

    return TestAggregator(std::move(schema), std::forward<CommitFunc>(func), stream::NeverSegmentPolicy{});
}

template <typename AggregatorType>
struct SinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType =  typename AggregatorType::IndexType;

    SinkWrapperImpl(StreamId stream_id, std::initializer_list<FieldRef> fields) :
        index_(IndexType::default_index()),
        sink_(std::make_shared<SegmentsSink>()),
        aggregator_(
            SchemaPolicy{
                index_.create_stream_descriptor(std::move(stream_id), fields), index_
            },
            [=](
                SegmentInMemory &&mem
            ) {
                sink_->segments_.push_back(std::move(mem));
            },
           typename AggregatorType::SegmentingPolicyType{}
        ) {

    }

    auto& segment() {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    IndexType index_;
    std::shared_ptr<SegmentsSink> sink_;
    AggregatorType aggregator_;
};

using TestAggregator =  Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SinkWrapper = SinkWrapperImpl<TestAggregator>;
using TestSparseAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy, SparseColumnPolicy>;
using SparseSinkWrapper = SinkWrapperImpl<TestSparseAggregator>;

// Generates an int64_t Column where the value in each row is equal to the row index
inline Column generate_int_column(size_t num_rows) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(size_t idx = 0; idx < num_rows; ++idx) {
        column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
    }
    return column;
}

// Generates an int64_t Column where the value in each row is equal to the row index modulo the number of unique values
inline Column generate_int_column_repeated_values(size_t num_rows, size_t unique_values) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    for(size_t idx = 0; idx < num_rows; ++idx) {
        column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx % unique_values));
    }
    return column;
}

// Generates a Column of empty type
inline Column generate_empty_column() {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::EMPTYVAL>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, false, false);
    return column;
}

// Generate a segment in memory suitable for testing groupby's empty type column behaviour with 5 columns:
// * int_repeating_values - an int64_t column with unique_values repeating values
// * empty_<agg> - an empty column for each supported aggregation
inline SegmentInMemory generate_groupby_testing_segment(size_t num_rows, size_t unique_values) {
    SegmentInMemory seg;
    auto int_repeated_values_col = std::make_shared<Column>(generate_int_column_repeated_values(num_rows, unique_values));
    seg.add_column(scalar_field(int_repeated_values_col->type().data_type(), "int_repeated_values"), int_repeated_values_col);
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_sum"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_min"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_max"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_mean"), std::make_shared<Column>(generate_empty_column()));
    seg.set_row_id(num_rows - 1);
    return seg;
}

inline SegmentInMemory get_standard_timeseries_segment(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SinkWrapper(name, {
        scalar_field(DataType::INT8, "int8"),
        scalar_field(DataType::UINT64, "uint64"),
        scalar_field(DataType::UTF_DYNAMIC64, "strings")
    });

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto &&rb) {
            rb.set_scalar(1, int8_t(i));
            rb.set_scalar(2, uint64_t(i) * 2);
            rb.set_string(3, fmt::format("string_{}", i));
        });
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_groupable_timeseries_segment(const std::string& name, size_t rows_per_group, std::initializer_list<size_t> group_ids) {
    auto wrapper = SinkWrapper(name, {
            scalar_field(DataType::INT8, "int8"),
            scalar_field(DataType::UTF_DYNAMIC64, "strings"),
    });

    int i = 0;
    for (auto group_id : group_ids) {
        for (size_t j = 0; j < rows_per_group; j++) {
            wrapper.aggregator_.start_row(timestamp{static_cast<timestamp>(rows_per_group*i + j)})([&](auto &&rb) {
                rb.set_scalar(1, int8_t(group_id));
                rb.set_string(2, fmt::format("string_{}", group_id));
            });
        }
        i++;
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_sparse_timeseries_segment(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SparseSinkWrapper(name, {
        scalar_field(DataType::INT8, "int8"),
        scalar_field(DataType::UINT64,  "uint64"),
        scalar_field(DataType::UTF_DYNAMIC64, "strings"),
    });

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto &&rb) {
            rb.set_scalar(1, int8_t(i));
            if(i % 2 == 1)
                rb.set_scalar(2, uint64_t(i) * 2);

            if(i % 3 == 2)
                rb.set_string(3, fmt::format("string_{}", i));
        });
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_sparse_timeseries_segment_floats(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SparseSinkWrapper(name, {
        scalar_field(DataType::FLOAT64, "col1"),
        scalar_field(DataType::FLOAT64, "col2"),
        scalar_field(DataType::FLOAT64, "col3"),
    });

    for (timestamp i = 0u; i < timestamp(num_rows); ++i) {
        wrapper.aggregator_.start_row(timestamp{i})([&](auto &&rb) {
            rb.set_scalar(1, double(i));
            if(i % 2 == 1)
                rb.set_scalar(2, double(i) * 2);

            if(i % 3 == 2)
                rb.set_scalar(3, double(i)/ 2);
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
inline auto get_test_config_data() {
    using namespace arcticdb::storage;
    LibraryPath path{"test", "store"};
    auto storages = create_storages(path, OpenMode::DELETE, memory::pack_config());
    return std::make_tuple(path, std::move(storages));
}

/**
 * Creates a LocalVersionedEngine from get_test_config_data().
 *
 * See also python_version_store_in_memory() and stream_test_common.hpp for alternatives using LMDB.
 */
template<typename VerStoreType = version_store::LocalVersionedEngine>
inline VerStoreType get_test_engine(storage::LibraryDescriptor::VariantStoreConfig cfg = {}) {
    auto [path, storages] = get_test_config_data();
    auto library = std::make_shared<arcticdb::storage::Library>(path, std::move(storages), std::move(cfg));
    return VerStoreType(library);
}

/**
 * Similar to get_test_engine() but replaces the AsyncStore with InMemoryStore and returns it.
 */
inline auto python_version_store_in_memory() {
    auto pvs = get_test_engine<version_store::PythonVersionStore>();
    auto replace_store = std::make_shared<InMemoryStore>();
    pvs._test_set_store(replace_store);
    return std::make_tuple(std::move(pvs), replace_store);
}

inline constexpr ssize_t to_tensor_dim(Dimension dim) {
    return static_cast<int>(dim) + 1;
}

inline NativeTensor tensor_from_column(const Column &column) {
    return column.type().visit_tag([&column](auto &&tag) {
        using TypeDescriptorTag = std::decay_t<decltype(tag)>;
        shape_t scalar_shape = 0;
        const shape_t *shape_ptr;
        constexpr auto dim = TypeDescriptorTag::DimensionTag::value;
        constexpr auto data_type = TypeDescriptorTag::DataTypeTag::data_type;
        if constexpr (dim == Dimension::Dim0) {
            scalar_shape = column.row_count();
            shape_ptr = &scalar_shape;
        } else {
            shape_ptr = column.shape_ptr();
        }

        auto tensor = NativeTensor{
            static_cast<ssize_t>(column.bytes()),
            to_tensor_dim(dim),
            nullptr,
            shape_ptr,
            data_type,
            get_type_size(data_type),
            column.ptr()
        };
        return tensor;
    });
}

struct SegmentToInputFrameAdapter {
    SegmentInMemory segment_;
    pipelines::InputTensorFrame input_frame_;

    explicit SegmentToInputFrameAdapter(SegmentInMemory &&segment) :
        segment_(std::move(segment)) {
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

    void synthesize_norm_meta() {
        if (segment_.metadata()) {
            auto segment_tsd = segment_.index_descriptor();
            input_frame_.norm_meta.CopyFrom(segment_tsd.proto().normalization());
        }
        ensure_norm_meta(input_frame_.norm_meta, input_frame_.desc.id(), false);
    }

};

template<typename AggregatorType>
struct SegmentSinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType =  typename AggregatorType::IndexType;

        SegmentSinkWrapperImpl(StreamId stream_id, const IndexType& index, FieldCollection&& fields) :
        sink_(std::make_shared<SegmentsSink>()),
        aggregator_(
            [](pipelines::FrameSlice&&) {},
            SchemaPolicy{
                index.create_stream_descriptor(std::move(stream_id), fields_from_range(fields)), index
            },
            [=](SegmentInMemory&& mem) {
                sink_->segments_.push_back(std::move(mem));
            },
            typename AggregatorType::SegmentingPolicyType{}
        ) {}

    auto& segment() {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    std::shared_ptr<SegmentsSink> sink_;
    AggregatorType aggregator_;
};

using TestSegmentAggregatorNoSegment = SegmentAggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SegmentSinkWrapper = SegmentSinkWrapperImpl<TestSegmentAggregatorNoSegment>;


} //namespace arcticdb