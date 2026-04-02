/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <folly/container/Enumerate.h>
#include <gtest/gtest.h>
#include <sparrow/record_batch.hpp>

#include <arcticdb/arrow/test/arrow_test_utils.hpp>
#include <arcticdb/arrow/arrow_c_interface.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/pipeline/input_frame.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/util/allocator.hpp>
#include <arcticdb/util/native_handler.hpp>
#include <arcticdb/util/test/generators.hpp>

using namespace arcticdb;

// Duplicated from arrow_utils.cpp to keep build times down until sparrow array formatting is moved out of
// sparrow/array.hpp
template<typename T>
sparrow::timestamp_without_timezone_nanoseconds_array create_timestamp_array(
        T* data_ptr, size_t data_size, std::optional<sparrow::validity_bitmap>&& validity_bitmap
) {
    static_assert(sizeof(T) == sizeof(sparrow::zoned_time_without_timezone_nanoseconds));
    // We default to using timestamps without timezones. If the normalization metadata contains a timezone it will be
    // applied during normalization in python layer.
    sparrow::u8_buffer<sparrow::zoned_time_without_timezone_nanoseconds> buffer(
            reinterpret_cast<sparrow::zoned_time_without_timezone_nanoseconds*>(data_ptr),
            data_size,
            get_detachable_allocator()
    );
    if (validity_bitmap) {
        return sparrow::timestamp_without_timezone_nanoseconds_array{
                std::move(buffer), data_size, std::move(*validity_bitmap)
        };
    } else {
        return sparrow::timestamp_without_timezone_nanoseconds_array{std::move(buffer), data_size};
    }
}

template<typename types>
class ArrowDataToSegmentNumeric : public testing::Test {};

using test_types =
        ::testing::Types<bool, uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t, float, double>;

TYPED_TEST_SUITE(ArrowDataToSegmentNumeric, test_types);

TYPED_TEST(ArrowDataToSegmentNumeric, Simple) {
    size_t num_rows = 10;
    std::vector<TypeParam> data(num_rows);
    if constexpr (std::is_same_v<TypeParam, bool>) {
        data[1] = true;
        data[2] = true;
        data[4] = true;
        data[7] = true;
    } else {
        std::iota(data.begin(), data.end(), 0UL);
    }
    auto array = create_array(data);
    auto record_batch = create_record_batch({{"col", array}});

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches).first;

    ASSERT_EQ(seg.fields().size(), 1);
    ASSERT_EQ(seg.num_columns(), 1);
    ASSERT_EQ(seg.row_count(), num_rows);
    const auto column_index = seg.column_index("col");
    ASSERT_TRUE(column_index.has_value());
    ASSERT_EQ(*column_index, 0);
    const auto& col = seg.column(0);
    ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
    ASSERT_EQ(col.row_count(), num_rows);
    ASSERT_EQ(col.last_row(), num_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    if constexpr (std::is_same_v<TypeParam, bool>) {
        const auto& blocks = col.data().buffer().blocks();
        ASSERT_EQ(blocks.size(), 1);
        ASSERT_EQ(blocks[0]->get_type(), MemBlockType::EXTERNAL_PACKED);
    } else {
        for (size_t idx = 0; idx < num_rows; ++idx) {
            ASSERT_EQ(*col.scalar_at<TypeParam>(idx), data.at(idx));
        }
    }
}

TYPED_TEST(ArrowDataToSegmentNumeric, MultiColumn) {
    size_t num_rows = 10;
    size_t num_columns = 10;
    std::vector<std::pair<std::string, sparrow::array>> columns;
    for (size_t idx = 0; idx < num_columns; ++idx) {
        std::vector<TypeParam> data(num_rows);
        if constexpr (std::is_same_v<TypeParam, bool>) {
            data[1] = true;
            data[3] = true;
            data[5] = true;
            data[7] = true;
            data[9] = true;
        } else {
            std::iota(data.begin(), data.end(), num_rows * idx);
        }
        auto array = create_array(data);
        columns.emplace_back(fmt::format("col{}", idx), array);
    }
    auto record_batch = create_record_batch(columns);

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches).first;

    ASSERT_EQ(seg.fields().size(), num_columns);
    ASSERT_EQ(seg.num_columns(), num_columns);
    ASSERT_EQ(seg.row_count(), num_rows);
    for (size_t idx = 0; idx < num_columns; ++idx) {
        const auto column_index = seg.column_index(fmt::format("col{}", idx));
        ASSERT_TRUE(column_index.has_value());
        ASSERT_EQ(*column_index, idx);
        const auto& col = seg.column(idx);
        ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
        ASSERT_EQ(col.row_count(), num_rows);
        ASSERT_EQ(col.last_row(), num_rows - 1);
        ASSERT_FALSE(col.is_sparse());
        if constexpr (std::is_same_v<TypeParam, bool>) {
            const auto& blocks = col.data().buffer().blocks();
            ASSERT_EQ(blocks.size(), 1);
            ASSERT_EQ(blocks[0]->get_type(), MemBlockType::EXTERNAL_PACKED);
        } else {
            for (size_t row = 0; row < num_rows; ++row) {
                ASSERT_EQ(*col.scalar_at<TypeParam>(row), (idx * num_rows) + row);
            }
        }
    }
}

TYPED_TEST(ArrowDataToSegmentNumeric, MultipleRecordBatches) {
    std::vector<sparrow::record_batch> record_batches;
    std::vector<size_t> rows_per_batch{1, 10, 100};
    size_t total_rows{0};
    for (auto num_rows : rows_per_batch) {
        std::vector<TypeParam> data(num_rows);
        if constexpr (std::is_same_v<TypeParam, bool>) {
            for (size_t idx = 0; idx < data.size(); ++idx) {
                data[idx] = (total_rows + idx) % 3 == 0;
            }
        } else {
            std::iota(data.begin(), data.end(), total_rows);
        }
        total_rows += num_rows;
        auto array = create_array(data);
        record_batches.emplace_back(create_record_batch({{"col", array}}));
    }
    auto seg = arrow_data_to_segment(record_batches).first;

    ASSERT_EQ(seg.fields().size(), 1);
    ASSERT_EQ(seg.num_columns(), 1);
    ASSERT_EQ(seg.row_count(), total_rows);
    const auto column_index = seg.column_index("col");
    ASSERT_TRUE(column_index.has_value());
    ASSERT_EQ(*column_index, 0);
    const auto& col = seg.column(0);
    ASSERT_EQ(col.type(), make_scalar_type(data_type_from_raw_type<TypeParam>()));
    ASSERT_EQ(col.row_count(), total_rows);
    ASSERT_EQ(col.last_row(), total_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    if constexpr (!std::is_same_v<TypeParam, bool>) {
        for (size_t idx = 0; idx < total_rows; ++idx) {
            ASSERT_EQ(*col.scalar_at<TypeParam>(idx), idx);
        }
    }
    const auto& buffer = col.data().buffer();
    ASSERT_EQ(buffer.bytes(), total_rows * sizeof(TypeParam));
    ASSERT_EQ(buffer.blocks().size(), rows_per_batch.size());
    ASSERT_EQ(buffer.block_offsets().size(), rows_per_batch.size() + 1);
    size_t size{0};
    for (size_t idx = 0; idx < rows_per_batch.size(); ++idx) {
        if constexpr (std::is_same_v<TypeParam, bool>) {
            ASSERT_EQ(buffer.blocks()[idx]->get_type(), MemBlockType::EXTERNAL_PACKED);
        } else {
            ASSERT_EQ(buffer.blocks()[idx]->get_type(), MemBlockType::EXTERNAL_WITH_EXTRA_BYTES);
            ASSERT_EQ(buffer.blocks()[idx]->physical_bytes(), rows_per_batch[idx] * sizeof(TypeParam));
        }
        ASSERT_EQ(buffer.blocks()[idx]->logical_size(), rows_per_batch[idx] * sizeof(TypeParam));
        ASSERT_EQ(buffer.blocks()[idx]->offset(), size);
        ASSERT_EQ(buffer.block_offsets()[idx], size);
        size += buffer.blocks()[idx]->logical_size();
    }
    ASSERT_EQ(buffer.block_offsets().back(), size);
}

TEST(ArrowDataToSegmentTimestamp, Simple) {
    size_t num_rows = 10;
    auto* data_ptr = reinterpret_cast<timestamp*>(allocate_detachable_memory(num_rows * sizeof(timestamp)));
    std::iota(data_ptr, data_ptr + num_rows, 0UL);
    auto array = sparrow::array{create_timestamp_array(data_ptr, num_rows, std::nullopt)};
    auto record_batch = create_record_batch({{"col", array}});

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches).first;

    ASSERT_EQ(seg.fields().size(), 1);
    ASSERT_EQ(seg.num_columns(), 1);
    ASSERT_EQ(seg.row_count(), num_rows);
    const auto column_index = seg.column_index("col");
    ASSERT_TRUE(column_index.has_value());
    ASSERT_EQ(*column_index, 0);
    const auto& col = seg.column(0);
    ASSERT_EQ(col.type(), make_scalar_type(DataType::NANOSECONDS_UTC64));
    ASSERT_EQ(col.row_count(), num_rows);
    ASSERT_EQ(col.last_row(), num_rows - 1);
    ASSERT_FALSE(col.is_sparse());
    for (size_t idx = 0; idx < num_rows; ++idx) {
        ASSERT_EQ(*col.scalar_at<timestamp>(idx), idx);
    }
}

TEST(ArrowDataToSegment, MultiColumnDifferentTypes) {
    size_t num_rows{10};
    std::vector<DataType> numeric_data_types{
            DataType::UINT8,
            DataType::UINT16,
            DataType::UINT32,
            DataType::UINT64,
            DataType::INT8,
            DataType::INT16,
            DataType::INT32,
            DataType::INT64,
            DataType::FLOAT32,
            DataType::FLOAT64
    };
    std::vector<std::pair<std::string, sparrow::array>> columns;
    for (auto data_type : numeric_data_types) {
        details::visit_type(data_type, [&](auto tag) {
            using type_info = ScalarTypeInfo<decltype(tag)>;
            std::vector<typename type_info::RawType> data(num_rows);
            std::iota(data.begin(), data.end(), 0);
            auto array = create_array(data);
            columns.emplace_back(fmt::format("{}", data_type), array);
        });
    }
    auto record_batch = create_record_batch(columns);

    std::vector<sparrow::record_batch> record_batches;
    record_batches.emplace_back(std::move(record_batch));
    auto seg = arrow_data_to_segment(record_batches).first;

    auto num_columns = numeric_data_types.size();
    ASSERT_EQ(seg.fields().size(), num_columns);
    ASSERT_EQ(seg.num_columns(), num_columns);
    ASSERT_EQ(seg.row_count(), num_rows);
    for (auto [idx, data_type] : folly::enumerate(numeric_data_types)) {
        const auto column_index = seg.column_index(fmt::format("{}", data_type));
        ASSERT_TRUE(column_index.has_value());
        ASSERT_EQ(*column_index, idx);
        const auto& col = seg.column(idx);
        ASSERT_EQ(col.type(), make_scalar_type(data_type));
        ASSERT_EQ(col.row_count(), num_rows);
        ASSERT_EQ(col.last_row(), num_rows - 1);
        ASSERT_FALSE(col.is_sparse());
        details::visit_type(data_type, [&](auto tag) {
            using type_info = ScalarTypeInfo<decltype(tag)>;
            for (size_t row = 0; row < num_rows; ++row) {
                ASSERT_EQ(*col.scalar_at<typename type_info::RawType>(row), row);
            }
        });
    }
}

struct AllocationCounter {
    size_t num_allocations = 0;
    size_t num_deallocations = 0;
};

// Allocator that counts allocations and deallocations
template<typename T>
struct TrackingAllocator {
    using value_type = T;
    AllocationCounter* counter_;

    explicit TrackingAllocator(AllocationCounter* counter) : counter_(counter) {}

    T* allocate(std::size_t n) {
        ++counter_->num_allocations;
        return std::allocator<T>{}.allocate(n);
    }

    void deallocate(T* p, std::size_t n) {
        ++counter_->num_deallocations;
        std::allocator<T>{}.deallocate(p, n);
    }

    bool operator==(const TrackingAllocator& other) const { return counter_ == other.counter_; }
};

std::shared_ptr<RecordBatchData> create_record_batch_with_allocation_tracking(
        AllocationCounter* counter, size_t num_rows = 100
) {
    auto allocator = TrackingAllocator<uint8_t>(counter);
    auto* ints = allocator.allocate(num_rows);
    std::iota(ints, ints + num_rows, 0);

    auto data_buffer = sparrow::u8_buffer<uint8_t>(ints, num_rows, allocator);
    auto primitive_array = sparrow::primitive_array<uint8_t>(std::move(data_buffer), num_rows);
    auto array = sparrow::array{std::move(primitive_array)};
    array.set_name("col");
    auto record_batch = sparrow::record_batch{};
    record_batch.add_column("col", std::move(array));
    auto [arr, schema] = sparrow::extract_arrow_structures(std::move(record_batch));
    return std::make_shared<RecordBatchData>(arr, schema);
}

TEST(ArrowWriteMemoryLifetime, SparrowCallsReleaseOnDestruction) {
    AllocationCounter counter;

    auto rbd = create_record_batch_with_allocation_tracking(&counter);
    ASSERT_EQ(counter.num_allocations, 1);
    ASSERT_EQ(counter.num_deallocations, 0);

    {
        // Move the arrow structs into an owning sparrow::record_batch
        sparrow::record_batch owning_batch(std::move(rbd->array_), std::move(rbd->schema_));
    }
    // sparrow::record_batch destroyed — should have called release, freeing the buffer
    ASSERT_EQ(counter.num_allocations, 1);
    ASSERT_EQ(counter.num_deallocations, 1);
}

TEST(ArrowWriteMemoryLifetime, InputFrameKeepsBufferAlive) {
    constexpr auto num_record_batches = 5;
    constexpr auto num_rows_per_record_batch = 100;
    AllocationCounter counter;
    auto engine = get_test_engine();
    const std::string symbol = "input_frame_keeps_buffer_alive";

    auto record_batches = std::vector<std::shared_ptr<RecordBatchData>>();
    for (auto i = 0; i < num_record_batches; ++i) {
        record_batches.emplace_back(create_record_batch_with_allocation_tracking(&counter, num_rows_per_record_batch));
    }

    // Record batches are allocated
    ASSERT_EQ(counter.num_allocations, num_record_batches);
    ASSERT_EQ(counter.num_deallocations, 0);

    auto input_frame = std::make_shared<pipelines::InputFrame>();
    input_frame->norm_meta.mutable_experimental_arrow()->set_has_index(false);
    convert::record_batches_to_frame(record_batches, *input_frame);
    input_frame->desc().set_id(symbol);
    input_frame->set_index_range();

    record_batches.clear();
    // record_batches vector is cleared but we don't deallocate yet because input frame keeps the buffers alive
    ASSERT_EQ(counter.num_allocations, num_record_batches);
    ASSERT_EQ(counter.num_deallocations, 0);

    engine.write_versioned_dataframe_internal(symbol, input_frame, false, false, false);
    input_frame.reset();
    // After the write is done and the InputFrame is destructed and we deallocate the input buffers
    ASSERT_EQ(counter.num_allocations, num_record_batches);
    ASSERT_EQ(counter.num_deallocations, num_record_batches);

    // Read back and verify
    auto read_query = std::make_shared<ReadQuery>();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result =
            engine.read_dataframe_version_internal(symbol, VersionQuery{}, read_query, ReadOptions{}, handler_data);
    const auto& seg = read_result.root_.frame_and_descriptor_.frame_;
    constexpr auto num_rows = num_record_batches * num_rows_per_record_batch;
    ASSERT_EQ(seg.row_count(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(*seg.scalar_at<uint8_t>(i, 0), static_cast<uint8_t>(i % num_rows_per_record_batch));
    }
}
