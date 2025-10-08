/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
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
auto get_test_aggregator(CommitFunc&& func, StreamId stream_id, std::vector<FieldRef>&& fields) {
    using namespace arcticdb::stream;
    using TestAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
    auto index = TimeseriesIndex::default_index();

    FixedSchema schema{index.create_stream_descriptor(std::move(stream_id), fields_from_range(fields)), index};

    return TestAggregator(std::move(schema), std::forward<CommitFunc>(func), stream::NeverSegmentPolicy{});
}

template<typename AggregatorType>
struct SinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType = typename AggregatorType::IndexType;

    SinkWrapperImpl(StreamId stream_id, std::initializer_list<FieldRef> fields) :
        index_(IndexType::default_index()),
        sink_(std::make_shared<SegmentsSink>()),
        aggregator_(
                SchemaPolicy{index_.create_stream_descriptor(std::move(stream_id), fields), index_},
                [this](SegmentInMemory&& mem) { sink_->segments_.push_back(std::move(mem)); },
                typename AggregatorType::SegmentingPolicyType{}
        ) {}

    auto& segment() {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    IndexType index_;
    std::shared_ptr<SegmentsSink> sink_;
    AggregatorType aggregator_;
};

using TestAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SinkWrapper = SinkWrapperImpl<TestAggregator>;
using TestRowCountAggregator = Aggregator<RowCountIndex, FixedSchema, stream::NeverSegmentPolicy>;
using RowCountSinkWrapper = SinkWrapperImpl<TestRowCountAggregator>;
using TestSparseAggregator = Aggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy, SparseColumnPolicy>;
using SparseSinkWrapper = SinkWrapperImpl<TestSparseAggregator>;

// Generates an int64_t Column where the value in each row is equal to the row index
inline Column generate_int_column(size_t num_rows) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
    }
    return column;
}

// Generates an int64_t Column where the value in one row out of two is equal to the row index
inline Column generate_int_sparse_column(size_t num_rows) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (idx % 2 == 0) {
            column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx));
        }
    }
    return column;
}

// Generates an int64_t Column where the value in each row is equal to the row index modulo the number of unique values
inline Column generate_int_column_repeated_values(size_t num_rows, size_t unique_values) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx % unique_values));
    }
    return column;
}

// Similar to generate_int_column_repeated_values, but with missing values where index % (unique_values + 1) == 0
inline Column generate_int_column_sparse_repeated_values(size_t num_rows, size_t unique_values) {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (idx % (unique_values + 1) != 0) {
            column.set_scalar<int64_t>(static_cast<ssize_t>(idx), static_cast<int64_t>(idx % unique_values));
        }
    }
    return column;
}

// Generates a Column of empty type
inline Column generate_empty_column() {
    using TDT = TypeDescriptorTag<DataTypeTag<DataType::EMPTYVAL>, DimensionTag<Dimension ::Dim0>>;
    Column column(static_cast<TypeDescriptor>(TDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED);
    return column;
}

// Generate a segment in memory suitable for testing filters and projections with sparse columns:
// * sparse_floats_1 - a sparse column of doubles with 5 rows including missing values and NaNs
// * sparse_floats_2 - a sparse column of doubles with 10 rows including missing values and NaNs
// * dense_floats_1 - a dense column of doubles with 7 rows
// * dense_floats_2 - a dense column of doubles with 15 rows
// * sparse_bools - a sparse column of bools with 4 rows including missing values
// The columns deliberately have differing row counts, as the sparse map bitset in a sparse column is not padded with
// zeros to the end of the SegmentInMemory, they just stop at the last row which contains a value
// The two dense columns of differing lengths are to cover the corner case where a column is "sparse", but happens to
// have the first n values populated, and so appears dense, but with fewer rows than the containing SegmentInMemory
inline SegmentInMemory generate_filter_and_project_testing_sparse_segment() {
    SegmentInMemory seg;
    using FTDT = ScalarTagType<DataTypeTag<DataType::FLOAT64>>;
    using BTDT = ScalarTagType<DataTypeTag<DataType::BOOL8>>;
    auto sparse_floats_1 = std::make_shared<Column>(
            static_cast<TypeDescriptor>(FTDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    auto sparse_floats_2 = std::make_shared<Column>(
            static_cast<TypeDescriptor>(FTDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );
    auto dense_floats_1 = std::make_shared<Column>(
            static_cast<TypeDescriptor>(FTDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED
    );
    auto dense_floats_2 = std::make_shared<Column>(
            static_cast<TypeDescriptor>(FTDT{}), 0, AllocationType::DYNAMIC, Sparsity::NOT_PERMITTED
    );
    auto sparse_bools = std::make_shared<Column>(
            static_cast<TypeDescriptor>(BTDT{}), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED
    );

    constexpr auto nan = std::numeric_limits<double>::quiet_NaN();

    // Row 0 - both sparse float columns have non-NaN values
    sparse_floats_1->set_scalar<double>(0, 1.0);
    sparse_floats_2->set_scalar<double>(0, 2.0);
    // Row 1 - 1 non-NaN value, 1 missing
    sparse_floats_1->set_scalar<double>(1, 3.0);
    // Row 2 - 1 NaN value, 1 missing
    sparse_floats_2->set_scalar<double>(2, nan);
    // Row 3 - both missing
    // Row 4 - 1 NaN value, 1 non-NaN value
    sparse_floats_1->set_scalar<double>(4, 4.0);
    sparse_floats_2->set_scalar<double>(4, nan);

    // Add another 5 rows to sparse_floats_2 so that the 2 sparse float columns are of differing lengths
    sparse_floats_2->set_scalar<double>(5, 5.0);
    sparse_floats_2->set_scalar<double>(9, nan);

    // Dense float column values are just idx or zero so some are equal and some are not
    for (auto idx = 0; idx < 15; idx++) {
        dense_floats_1->set_scalar<double>(idx, static_cast<double>(idx));
        dense_floats_2->set_scalar<double>(idx, idx % 2 == 0 ? static_cast<double>(idx) : double(0.0));
    }

    // Sparse bool column goes missing, true, missing, false
    sparse_bools->set_scalar<bool>(1, true);
    sparse_bools->set_scalar<bool>(3, false);

    seg.add_column(scalar_field(sparse_floats_1->type().data_type(), "sparse_floats_1"), sparse_floats_1);
    seg.add_column(scalar_field(sparse_floats_2->type().data_type(), "sparse_floats_2"), sparse_floats_2);
    seg.add_column(scalar_field(dense_floats_1->type().data_type(), "dense_floats_1"), dense_floats_1);
    seg.add_column(scalar_field(dense_floats_2->type().data_type(), "dense_floats_2"), dense_floats_2);
    seg.add_column(scalar_field(sparse_bools->type().data_type(), "sparse_bools"), sparse_bools);

    // 1 less than the number of rows in the largest column
    sparse_floats_1->set_row_data(14);
    sparse_floats_2->set_row_data(14);
    dense_floats_1->set_row_data(14);
    dense_floats_2->set_row_data(14);
    sparse_bools->set_row_data(14);
    seg.set_row_id(14);
    return seg;
}

// Generate a segment in memory suitable for testing groupby's empty type column behaviour with 6 columns:
// * int_repeating_values - an int64_t column with unique_values repeating values
// * empty_<agg> - an empty column for each supported aggregation
inline SegmentInMemory generate_groupby_testing_empty_segment(size_t num_rows, size_t unique_values) {
    SegmentInMemory seg;
    auto int_repeated_values_col =
            std::make_shared<Column>(generate_int_column_repeated_values(num_rows, unique_values));
    seg.add_column(
            scalar_field(int_repeated_values_col->type().data_type(), "int_repeated_values"), int_repeated_values_col
    );
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_sum"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_min"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_max"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_mean"), std::make_shared<Column>(generate_empty_column()));
    seg.add_column(scalar_field(DataType::EMPTYVAL, "empty_count"), std::make_shared<Column>(generate_empty_column()));
    seg.set_row_id(num_rows - 1);
    return seg;
}

inline SegmentInMemory generate_groupby_testing_segment(size_t num_rows, size_t unique_values) {
    SegmentInMemory seg;
    auto int_repeated_values_col =
            std::make_shared<Column>(generate_int_column_repeated_values(num_rows, unique_values));
    seg.add_column(
            scalar_field(int_repeated_values_col->type().data_type(), "int_repeated_values"), int_repeated_values_col
    );
    std::array<std::string, 5> col_names = {"sum_int", "min_int", "max_int", "mean_int", "count_int"};
    for (const auto& name : col_names) {
        auto col = std::make_shared<Column>(generate_int_column(num_rows));
        seg.add_column(scalar_field(col->type().data_type(), name), col);
    }
    seg.set_row_id(num_rows - 1);
    return seg;
}

inline SegmentInMemory generate_groupby_testing_sparse_segment(size_t num_rows, size_t unique_values) {
    SegmentInMemory seg;
    auto int_repeated_values_col =
            std::make_shared<Column>(generate_int_column_repeated_values(num_rows, unique_values));
    seg.add_column(
            scalar_field(int_repeated_values_col->type().data_type(), "int_repeated_values"),
            std::move(int_repeated_values_col)
    );
    const std::array<std::string, 5> col_names = {"sum_int", "min_int", "max_int", "mean_int", "count_int"};
    for (const auto& name : col_names) {
        auto col = std::make_shared<Column>(generate_int_sparse_column(num_rows));
        seg.add_column(scalar_field(col->type().data_type(), name), col);
    }
    seg.set_row_id(num_rows - 1);
    return seg;
}

inline SegmentInMemory generate_sparse_groupby_testing_segment(size_t num_rows, size_t unique_values) {
    SegmentInMemory seg;
    auto int_sparse_repeated_values_col =
            std::make_shared<Column>(generate_int_column_sparse_repeated_values(num_rows, unique_values));
    int_sparse_repeated_values_col->mark_absent_rows(num_rows - 1);
    seg.add_column(
            scalar_field(int_sparse_repeated_values_col->type().data_type(), "int_sparse_repeated_values"),
            std::move(int_sparse_repeated_values_col)
    );
    const std::array<std::string_view, 5> col_names = {"sum_int", "min_int", "max_int", "mean_int", "count_int"};
    for (const auto& name : col_names) {
        auto col = std::make_shared<Column>(generate_int_column(num_rows));
        seg.add_column(scalar_field(col->type().data_type(), name), col);
    }
    seg.set_row_id(num_rows - 1);
    return seg;
}

inline SegmentInMemory get_standard_timeseries_segment(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SinkWrapper(
            name,
            {scalar_field(DataType::INT8, "int8"),
             scalar_field(DataType::UINT64, "uint64"),
             scalar_field(DataType::UTF_DYNAMIC64, "strings")}
    );

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

inline SegmentInMemory get_seqnum_timeseries_segment(
        const std::string& name, size_t num_rows = 10, size_t num_seq = 3
) {
    auto wrapper = SinkWrapper(
            name,
            {scalar_field(DataType::UINT64, "seqnum"),
             scalar_field(DataType::INT8, "int8"),
             scalar_field(DataType::UTF_DYNAMIC64, "strings")}
    );

    uint64_t seqnum = 0UL;
    for (timestamp i = 0UL; i < timestamp(num_rows / num_seq); ++i) {
        for (auto j = 0UL; j < num_seq; ++j) {
            wrapper.aggregator_.start_row(timestamp{i})([&](auto&& rb) {
                rb.set_scalar(1, seqnum++);
                rb.set_scalar(2, int8_t(i * 2));
                rb.set_string(3, fmt::format("string_{}", i));
            });
        }
    }
    wrapper.aggregator_.commit();
    return wrapper.segment();
}

inline SegmentInMemory get_groupable_timeseries_segment(
        const std::string& name, size_t rows_per_group, std::initializer_list<size_t> group_ids
) {
    auto wrapper = SinkWrapper(
            name,
            {
                    scalar_field(DataType::INT8, "int8"),
                    scalar_field(DataType::UTF_DYNAMIC64, "strings"),
            }
    );

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

inline SegmentInMemory get_sparse_timeseries_segment(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SparseSinkWrapper(
            name,
            {
                    scalar_field(DataType::INT8, "int8"),
                    scalar_field(DataType::UINT64, "uint64"),
                    scalar_field(DataType::UTF_DYNAMIC64, "strings"),
            }
    );

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

inline SegmentInMemory get_sparse_timeseries_segment_floats(const std::string& name, size_t num_rows = 10) {
    auto wrapper = SparseSinkWrapper(
            name,
            {
                    scalar_field(DataType::FLOAT64, "col1"),
                    scalar_field(DataType::FLOAT64, "col2"),
                    scalar_field(DataType::FLOAT64, "col3"),
            }
    );

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
inline auto get_test_config_data(std::string name = "test") {
    using namespace arcticdb::storage;
    LibraryPath path{name.c_str(), "store"};
    auto storages = create_storages(path, OpenMode::DELETE, {memory::pack_config()});
    return std::make_tuple(path, std::move(storages));
}

inline std::shared_ptr<arcticdb::storage::Library> get_test_library(
        storage::LibraryDescriptor::VariantStoreConfig cfg = {}, std::string name = "test"
) {
    auto [path, storages] = get_test_config_data(name);
    auto library = std::make_shared<arcticdb::storage::Library>(path, std::move(storages), std::move(cfg));
    return library;
}

/**
 * Creates a LocalVersionedEngine from get_test_config_data().
 *
 * See also python_version_store_in_memory() and stream_test_common.hpp for alternatives using LMDB.
 */
template<typename VersionStoreType = version_store::LocalVersionedEngine>
inline VersionStoreType get_test_engine(
        storage::LibraryDescriptor::VariantStoreConfig cfg = {}, std::string name = "test"
) {
    return VersionStoreType(get_test_library(cfg, name));
}

/**
 * Similar to get_test_engine() but replaces the AsyncStore with InMemoryStore and returns it.
 */
inline auto python_version_store_in_memory(storage::LibraryDescriptor::VariantStoreConfig cfg = {}) {
    auto pvs = get_test_engine<version_store::PythonVersionStore>(std::move(cfg));
    auto replace_store = std::make_shared<InMemoryStore>();
    pvs._test_set_store(replace_store);
    return std::make_tuple(std::move(pvs), replace_store);
}

inline constexpr ssize_t to_tensor_dim(Dimension dim) { return static_cast<int>(dim) + 1; }

inline NativeTensor tensor_from_column(const Column& column) {
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

        auto tensor = NativeTensor{
                static_cast<ssize_t>(column.bytes()),
                to_tensor_dim(dim),
                nullptr,
                shape_ptr,
                data_type,
                get_type_size(data_type),
                column.ptr(),
                to_tensor_dim(dim)
        };
        return tensor;
    });
}

struct SegmentToInputFrameAdapter {
    SegmentInMemory segment_;
    std::shared_ptr<pipelines::InputTensorFrame> input_frame_ = std::make_shared<pipelines::InputTensorFrame>();

    explicit SegmentToInputFrameAdapter(SegmentInMemory&& segment) : segment_(std::move(segment)) {
        input_frame_->desc = segment_.descriptor();
        input_frame_->num_rows = segment_.row_count();
        size_t col{0};
        if (segment_.descriptor().index().type() != IndexDescriptorImpl::Type::ROWCOUNT) {
            for (size_t i = 0; i < segment_.descriptor().index().field_count(); ++i) {
                input_frame_->index_tensor = tensor_from_column(segment_.column(col));
                ++col;
            }
        }

        while (col < segment_.num_columns())
            input_frame_->field_tensors.emplace_back(tensor_from_column(segment_.column(col++)));

        input_frame_->set_index_range();
    }
};

template<typename AggregatorType>
struct SegmentSinkWrapperImpl {
    using SchemaPolicy = typename AggregatorType::SchemaPolicy;
    using IndexType = typename AggregatorType::IndexType;

    SegmentSinkWrapperImpl(StreamId stream_id, const IndexType& index, const FieldCollection& fields) :
        aggregator_(
                [](pipelines::FrameSlice&&) {
                    // Do nothing
                },
                SchemaPolicy{index.create_stream_descriptor(std::move(stream_id), fields_from_range(fields)), index},
                [this](SegmentInMemory&& mem) { sink_->segments_.push_back(std::move(mem)); },
                typename AggregatorType::SegmentingPolicyType{}
        ) {}

    auto& segment() {
        util::check(!sink_->segments_.empty(), "Segment vector empty");
        return sink_->segments_[0];
    }

    std::shared_ptr<SegmentsSink> sink_ = std::make_shared<SegmentsSink>();
    AggregatorType aggregator_;
};

using TestSegmentAggregatorNoSegment = SegmentAggregator<TimeseriesIndex, FixedSchema, stream::NeverSegmentPolicy>;
using SegmentSinkWrapper = SegmentSinkWrapperImpl<TestSegmentAggregatorNoSegment>;

inline ResampleClause<ResampleBoundary::LEFT> generate_resample_clause(
        const std::vector<NamedAggregator>& named_aggregators
) {
    ResampleClause<ResampleBoundary::LEFT> res{
            "dummy_rule",
            ResampleBoundary::LEFT,
            [](timestamp, timestamp, std::string_view, ResampleBoundary, timestamp, ResampleOrigin
            ) -> std::vector<timestamp> { return {}; },
            0,
            "dummy_origin"
    };
    res.set_aggregations(named_aggregators);
    return res;
}

} // namespace arcticdb
