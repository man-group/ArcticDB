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
#include <arcticdb/arrow/arrow_output_frame.hpp>
#include <arcticdb/arrow/arrow_utils.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/util/allocator.hpp>

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

// --- RecordBatchData RAII release tests ---
// These tests verify that RecordBatchData's destructor correctly calls the Arrow C Data Interface
// release callbacks, preventing memory leaks when Arrow data is exported from Python.

namespace {
// Static flags used by release-counting shims. Non-capturing lambdas decay to C function pointers,
// which is required by the ArrowArray/ArrowSchema release callback signature.
static bool s_array_released = false;
static bool s_schema_released = false;
static void (*s_original_array_release)(ArrowArray*) = nullptr;
static void (*s_original_schema_release)(ArrowSchema*) = nullptr;

void counting_array_release(ArrowArray* array) {
    s_array_released = true;
    // Restore the original release pointer before calling through, because sparrow's
    // release_arrow_array asserts that array->release points to itself.
    array->release = s_original_array_release;
    if (s_original_array_release) {
        s_original_array_release(array);
    }
}

void counting_schema_release(ArrowSchema* schema) {
    s_schema_released = true;
    // Same as above - restore before calling sparrow's release_arrow_schema.
    schema->release = s_original_schema_release;
    if (s_original_schema_release) {
        s_original_schema_release(schema);
    }
}

void reset_release_flags() {
    s_array_released = false;
    s_schema_released = false;
    s_original_array_release = nullptr;
    s_original_schema_release = nullptr;
}
} // namespace

TEST(RecordBatchDataRelease, DestructorCallsRelease) {
    // Verify that RecordBatchData's destructor calls release on both ArrowArray and ArrowSchema.
    // This is the core RAII property that prevents memory leaks.
    reset_release_flags();
    {
        // Step 1: Build a sparrow record_batch with real data
        std::vector<int64_t> data{1, 2, 3, 4, 5};
        auto array = create_array(data);
        auto record_batch = create_record_batch({{"col", array}});

        // Step 2: Extract to C structs (same path as ArrowOutputFrame::extract_record_batches)
        auto struct_array = sparrow::array{record_batch.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));

        // Step 3: Create RecordBatchData (takes ownership of the C structs)
        RecordBatchData rbd(arr, schema);
        ASSERT_NE(rbd.array_.release, nullptr) << "ArrowArray release should be set after extraction";
        ASSERT_NE(rbd.schema_.release, nullptr) << "ArrowSchema release should be set after extraction";

        // Step 4: Install counting wrappers
        s_original_array_release = rbd.array_.release;
        s_original_schema_release = rbd.schema_.release;
        rbd.array_.release = counting_array_release;
        rbd.schema_.release = counting_schema_release;

        // RecordBatchData goes out of scope here - destructor should call release
    }
    EXPECT_TRUE(s_array_released) << "ArrowArray release callback was NOT called by destructor (memory leak!)";
    EXPECT_TRUE(s_schema_released) << "ArrowSchema release callback was NOT called by destructor (memory leak!)";
}

TEST(RecordBatchDataRelease, WritePathReleasesArrowMemory) {
    // End-to-end test: simulates the write path where Python exports Arrow data,
    // C++ borrows it via pointer-constructed record_batch, processes it, then the
    // RecordBatchData must release the Arrow memory on destruction.
    reset_release_flags();
    {
        // Step 1: Create Arrow data (simulates what Python's _export_to_c produces)
        std::vector<int64_t> data{10, 20, 30};
        auto array = create_array(data);
        auto record_batch = create_record_batch({{"value", array}});

        auto struct_array = sparrow::array{record_batch.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));
        RecordBatchData rbd(arr, schema);

        // Step 2: Simulate the write path borrow (python_to_tensor_frame.cpp:348)
        // sparrow::record_batch constructed from pointers = non-owning
        sparrow::record_batch borrowed{&rbd.array_, &rbd.schema_};

        // Step 3: Process via arrow_data_to_segment (the actual write-path consumption)
        std::vector<sparrow::record_batch> batches;
        batches.emplace_back(std::move(borrowed));
        auto [seg, _] = arrow_data_to_segment(batches);

        // Step 4: Verify the borrow didn't consume the release callbacks
        ASSERT_NE(rbd.array_.release, nullptr) << "Borrow should not consume array release";
        ASSERT_NE(rbd.schema_.release, nullptr) << "Borrow should not consume schema release";

        // Step 5: Install counting wrappers
        s_original_array_release = rbd.array_.release;
        s_original_schema_release = rbd.schema_.release;
        rbd.array_.release = counting_array_release;
        rbd.schema_.release = counting_schema_release;

        // RecordBatchData goes out of scope here
    }
    EXPECT_TRUE(s_array_released) << "ArrowArray release was NOT called after write-path consumption (memory leak!)";
    EXPECT_TRUE(s_schema_released) << "ArrowSchema release was NOT called after write-path consumption (memory leak!)";
}

TEST(RecordBatchDataRelease, MoveTransfersOwnership) {
    // Verify that move construction transfers ownership: the source should have nullptr release,
    // and the destination should call release on destruction.
    reset_release_flags();
    {
        std::vector<int64_t> data{1, 2, 3};
        auto array = create_array(data);
        auto rb = create_record_batch({{"col", array}});
        auto struct_array = sparrow::array{rb.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));

        auto rbd_src = std::make_unique<RecordBatchData>(arr, schema);

        // Install counting wrappers before the move
        s_original_array_release = rbd_src->array_.release;
        s_original_schema_release = rbd_src->schema_.release;
        rbd_src->array_.release = counting_array_release;
        rbd_src->schema_.release = counting_schema_release;

        // Move construct
        RecordBatchData rbd_dst(std::move(*rbd_src));

        // Source should be cleared
        EXPECT_EQ(rbd_src->array_.release, nullptr) << "Source array release should be nullptr after move";
        EXPECT_EQ(rbd_src->schema_.release, nullptr) << "Source schema release should be nullptr after move";

        // Destroy the source - should NOT call release (it was moved away)
        rbd_src.reset();
        EXPECT_FALSE(s_array_released) << "Release should NOT be called when destroying moved-from source";
        EXPECT_FALSE(s_schema_released) << "Release should NOT be called when destroying moved-from source";

        // rbd_dst goes out of scope here and should call release
    }
    EXPECT_TRUE(s_array_released) << "ArrowArray release was NOT called by move destination destructor";
    EXPECT_TRUE(s_schema_released) << "ArrowSchema release was NOT called by move destination destructor";
}

TEST(RecordBatchDataRelease, MoveAssignmentReleasesOldAndTransfers) {
    // Verify that move assignment releases the destination's existing resources
    // before taking ownership from the source.
    reset_release_flags();

    // Flags for the first (destination) resource
    static bool s_old_array_released = false;
    static bool s_old_schema_released = false;
    static void (*s_old_original_array_release)(ArrowArray*) = nullptr;
    static void (*s_old_original_schema_release)(ArrowSchema*) = nullptr;
    s_old_array_released = false;
    s_old_schema_released = false;

    {
        // Create two separate RecordBatchData objects
        std::vector<int64_t> data1{1, 2, 3};
        auto arr1 = create_array(data1);
        auto rb1 = create_record_batch({{"col", arr1}});
        auto sa1 = sparrow::array{rb1.extract_struct_array()};
        auto [a1, s1] = sparrow::extract_arrow_structures(std::move(sa1));
        RecordBatchData rbd_dst(a1, s1);

        std::vector<int64_t> data2{4, 5, 6};
        auto arr2 = create_array(data2);
        auto rb2 = create_record_batch({{"col", arr2}});
        auto sa2 = sparrow::array{rb2.extract_struct_array()};
        auto [a2, s2] = sparrow::extract_arrow_structures(std::move(sa2));
        auto rbd_src = std::make_unique<RecordBatchData>(a2, s2);

        // Install counting wrappers on the destination's (old) resources
        s_old_original_array_release = rbd_dst.array_.release;
        s_old_original_schema_release = rbd_dst.schema_.release;
        rbd_dst.array_.release = [](ArrowArray* array) {
            s_old_array_released = true;
            array->release = s_old_original_array_release;
            if (s_old_original_array_release)
                s_old_original_array_release(array);
        };
        rbd_dst.schema_.release = [](ArrowSchema* schema) {
            s_old_schema_released = true;
            schema->release = s_old_original_schema_release;
            if (s_old_original_schema_release)
                s_old_original_schema_release(schema);
        };

        // Install counting wrappers on the source's (new) resources
        s_original_array_release = rbd_src->array_.release;
        s_original_schema_release = rbd_src->schema_.release;
        rbd_src->array_.release = counting_array_release;
        rbd_src->schema_.release = counting_schema_release;

        // Move assign: should release old, take new
        rbd_dst = std::move(*rbd_src);

        // Old resources should have been released by the assignment
        EXPECT_TRUE(s_old_array_released) << "Old ArrowArray was NOT released during move assignment";
        EXPECT_TRUE(s_old_schema_released) << "Old ArrowSchema was NOT released during move assignment";

        // Source should be cleared
        EXPECT_EQ(rbd_src->array_.release, nullptr) << "Source array release should be nullptr after move assign";
        EXPECT_EQ(rbd_src->schema_.release, nullptr) << "Source schema release should be nullptr after move assign";

        // New resources should NOT be released yet (rbd_dst still alive)
        EXPECT_FALSE(s_array_released) << "New resources should not be released before destination destruction";

        // Destroy source - should be no-op
        rbd_src.reset();
        EXPECT_FALSE(s_array_released) << "Release should NOT be called when destroying moved-from source";

        // rbd_dst goes out of scope - should release the new resources
    }
    EXPECT_TRUE(s_array_released) << "New ArrowArray was NOT released by destination destructor";
    EXPECT_TRUE(s_schema_released) << "New ArrowSchema was NOT released by destination destructor";
}

TEST(RecordBatchDataRelease, SelfMoveAssignmentIsNoOp) {
    // Verify that self-move-assignment doesn't release or corrupt the resource.
    reset_release_flags();
    {
        std::vector<int64_t> data{1, 2, 3};
        auto array = create_array(data);
        auto rb = create_record_batch({{"col", array}});
        auto struct_array = sparrow::array{rb.extract_struct_array()};
        auto [arr, schema] = sparrow::extract_arrow_structures(std::move(struct_array));
        RecordBatchData rbd(arr, schema);

        // Install counting wrappers
        s_original_array_release = rbd.array_.release;
        s_original_schema_release = rbd.schema_.release;
        rbd.array_.release = counting_array_release;
        rbd.schema_.release = counting_schema_release;

        // Self-move-assign (suppress compiler warning about self-move)
        auto& ref = rbd;
        rbd = std::move(ref);

        // Release should NOT have been called
        EXPECT_FALSE(s_array_released) << "Self-assignment should NOT call release";
        EXPECT_FALSE(s_schema_released) << "Self-assignment should NOT call release";

        // Resource should still be valid (non-null release)
        EXPECT_NE(rbd.array_.release, nullptr) << "Self-assignment corrupted array release pointer";
        EXPECT_NE(rbd.schema_.release, nullptr) << "Self-assignment corrupted schema release pointer";

        // rbd goes out of scope - release should be called exactly once
    }
    EXPECT_TRUE(s_array_released) << "ArrowArray was NOT released after self-assigned object destroyed";
    EXPECT_TRUE(s_schema_released) << "ArrowSchema was NOT released after self-assigned object destroyed";
}
