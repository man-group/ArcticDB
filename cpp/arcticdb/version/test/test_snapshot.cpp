/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#include <gtest/gtest.h>

#include <arcticdb/version/version_store_api.hpp>
#include <arcticdb/version/version_pipelines.hpp>
#include <arcticdb/storage/library.hpp>
#include <arcticdb/storage/library_path.hpp>
#include <arcticdb/storage/open_mode.hpp>
#include <arcticdb/storage/memory/memory_storage.hpp>
#include <arcticdb/storage/mongo/mongo_storage.hpp>
#include <arcticdb/storage/lmdb/lmdb_storage.hpp>
#include <arcticdb/util/allocator.hpp>
#include <filesystem>
#include <chrono>
#include <thread>

TEST(SnapshotCreate, Basic) {
    using namespace arcticdb;
    using namespace arcticdb::storage;
    using namespace arcticdb::stream;
    using namespace arcticdb::pipelines;

    InstanceUri instance_uri("localhost");
    auto library_path = LibraryPath::from_delim_path("test.test", '.');
    auto temp_path = std::filesystem::temp_directory_path();
    auto library = create_library(library_path, OpenMode::WRITE, arcticdb::storage::lmdb::pack_config(temp_path));
    auto version_store = std::make_shared<version_store::PythonVersionStore>(library);
    auto store = version_store->_test_get_store();

    using c1_type = uint32_t;
    using c2_type = double;

    auto c1_dt = DataType::UINT32;
    auto c2_dt = DataType::FLOAT64;

    std::string stream_id("test_versioned_engine_write");
    VersionId version_id(0);
    FixedSchema schema{
            RowCountIndex::create_stream_descriptor(stream_id, {
                    FieldDescriptor(TypeDescriptor(c1_dt, Dimension::Dim0)),
                    FieldDescriptor(TypeDescriptor(c2_dt, Dimension::Dim0))
            })
    };

    constexpr size_t num_rows = 100;

    std::vector<uint32_t> col1{num_rows};
    std::vector<double> col2{num_rows};
    for(size_t i = 0; i < num_rows; ++i) {
        col1.push_back(i);
        col2.push_back(double(i) / 2);
    }

    stride_t c1_strides = sizeof(c1_type);
    shape_t c1_shapes = num_rows;
    ssize_t c1_bytes = c1_shapes * c1_strides;
    auto t1 = [&]{ return NativeTensor{c1_bytes, 1, &c1_strides, &c1_shapes, c1_dt, col1.data()}; };


    stride_t c2_strides = sizeof(c2_type);
    shape_t c2_shapes = num_rows;
    ssize_t c2_bytes = c2_shapes * c2_strides;
    auto t2 = [&]{ return NativeTensor{c2_bytes, 1, &c2_strides, &c2_shapes, c2_dt, col2.data()}; };

    write::SlicingPolicy slicing = write::FixedSlicer{};

    IndexRange index_range(0, num_rows);

    write::IndexPartialKey pk{stream_id, version_id};
    InputTensorFrame frame;
    frame.desc = schema.descriptor();
    frame.index = RowCountIndex();
    frame.field_tensors.push_back(t1());
    frame.field_tensors.push_back(t2());
    frame.index_range = IndexRange{0, num_rows};
    frame.num_rows = num_rows;

    auto fut = write::write_frame(std::move(pk), frame, slicing, store);


    auto version_key = std::move(fut.wait().value());
    ::sleep(1);
    log::root().info("{}", version_key);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    version_store->snapshot("blah");
    version_store->list_snapshots();
    version_store->read_snapshot("test_versioned_engine_write", "blah");
    ASSERT_TRUE(Allocator::empty());
}