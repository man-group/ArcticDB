/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include "util/constants.hpp"
#include <gtest/gtest.h>

#include <arcticdb/stream/index.hpp>
#include <arcticdb/stream/schema.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/stream/test/stream_test_common.hpp>
#include <arcticdb/python/python_to_tensor_frame.hpp>
#include <arcticdb/python/python_handlers.hpp>
#include <arcticdb/python/python_strings.hpp>
#include <arcticdb/python/gil_lock.hpp>
#include <arcticdb/python/python_types.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/native_handler.hpp>
#include <arcticdb/util/test/gtest_utils.hpp>

struct SparseTestStore : arcticdb::TestStore {
  protected:
    std::string get_name() override { return "test.sparse"; }
};

TEST(ReadOptions, PandasNoneValueForType) {
    using namespace arcticdb;

    ReadOptions options;
    const auto utf = make_scalar_type(DataType::UTF_DYNAMIC64);
    const auto ascii = make_scalar_type(DataType::ASCII_DYNAMIC64);
    const auto empty = make_scalar_type(DataType::EMPTYVAL);
    const auto fixed_utf = make_scalar_type(DataType::UTF_FIXED64);
    EXPECT_EQ(options.pandas_none_value_for_type(utf), PandasMissingValue::None);
    options.set_output_config(PandasOutputConfig{PandasStringFormat::OBJECT_WITH_NANS_ONLY});
    const ReadOptions& const_options = options;
    EXPECT_EQ(const_options.pandas_none_value_for_type(utf), PandasMissingValue::NaN);
    EXPECT_EQ(const_options.pandas_none_value_for_type(ascii), PandasMissingValue::None);
    EXPECT_EQ(const_options.pandas_none_value_for_type(empty), PandasMissingValue::None);
    EXPECT_EQ(const_options.pandas_none_value_for_type(fixed_utf), PandasMissingValue::None);

    options.set_output_config(PandasOutputConfig{PandasStringFormat::ARROW_LARGE_STRING});
    EXPECT_EQ(const_options.pandas_none_value_for_type(utf), PandasMissingValue::None);
    EXPECT_EQ(const_options.pandas_none_value_for_type(ascii), PandasMissingValue::None);
    EXPECT_EQ(const_options.pandas_none_value_for_type(empty), PandasMissingValue::None);

    options.set_output_config(ArrowOutputConfig{});
    EXPECT_THROW(static_cast<void>(const_options.pandas_none_value_for_type(utf)), InternalException);
}

TEST(PythonEmptyHandler, ConvertTypeRespectsDestinationType) {
    using namespace arcticdb;

    auto check = [](DataType dest_type, PandasStringFormat format, bool expect_nan) {
        const auto source_desc = make_scalar_type(DataType::EMPTYVAL);
        const auto dest_desc = make_scalar_type(dest_type);
        const entity::FieldWrapper field(dest_desc, "column");
        const ColumnMapping mapping(
                source_desc, dest_desc, field.field(), sizeof(PyObject*), 1, 0, 0, sizeof(PyObject*), 0
        );
        Column source(source_desc);
        Column dest(dest_desc);
        dest.allocate_and_advance_by(sizeof(PyObject*));
        ReadOptions options;
        options.set_output_config(PandasOutputConfig{format});
        std::any handler_data{PythonHandlerData{}};

        PythonEmptyHandler{}.convert_type(
                source, dest, mapping, DecodePathData{}, handler_data, std::make_shared<StringPool>(), options
        );

        const auto actual = *reinterpret_cast<PyObject**>(dest.bytes_at(0, sizeof(PyObject*)));
        const auto expected =
                expect_nan ? std::any_cast<PythonHandlerData&>(handler_data).non_owning_nan_handle() : Py_None;
        EXPECT_EQ(actual, expected);
    };

    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, true);
    check(DataType::ASCII_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, false);
    check(DataType::EMPTYVAL, PandasStringFormat::OBJECT_WITH_NANS_ONLY, false);
    check(DataType::BOOL_OBJECT8, PandasStringFormat::OBJECT_WITH_NANS_ONLY, false);
    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT, false);
}

TEST(PythonEmptyHandler, DefaultInitializeKeepsEmptyAsNone) {
    using namespace arcticdb;

    ReadOptions options;
    options.set_output_config(PandasOutputConfig{PandasStringFormat::OBJECT_WITH_NANS_ONLY});
    std::any handler_data{PythonHandlerData{}};
    ChunkedBuffer buffer = ChunkedBuffer::presized(sizeof(PyObject*));

    PythonEmptyHandler{}.default_initialize(buffer, 0, sizeof(PyObject*), DecodePathData{}, handler_data, options);

    EXPECT_EQ(*buffer.ptr_cast<PyObject*>(0, sizeof(PyObject*)), Py_None);
}

TEST(PythonHandlerData, GetMissingValue) {
    using namespace arcticdb;

    PythonHandlerData handler_data;
    EXPECT_EQ(handler_data.get_missing_value(PandasMissingValue::None), Py_None);
    EXPECT_EQ(handler_data.get_missing_value(PandasMissingValue::NaN), handler_data.non_owning_nan_handle());
}

TEST(PythonStringHandler, DefaultInitializeRespectsStringType) {
    using namespace arcticdb;

    register_python_string_types();

    auto check = [](DataType type, PandasStringFormat format, bool expect_nan) {
        auto handler = get_type_handler(OutputFormat::PANDAS, make_scalar_type(type));
        ASSERT_NE(handler, nullptr);

        ReadOptions options;
        options.set_output_config(PandasOutputConfig{format});
        std::any handler_data{PythonHandlerData{}};
        ChunkedBuffer buffer = ChunkedBuffer::presized(sizeof(PyObject*));
        handler->default_initialize(buffer, 0, sizeof(PyObject*), DecodePathData{}, handler_data, options);

        auto actual = *buffer.ptr_cast<PyObject*>(0, sizeof(PyObject*));
        auto expected = expect_nan ? std::any_cast<PythonHandlerData&>(handler_data).non_owning_nan_handle() : Py_None;
        EXPECT_EQ(actual, expected);
    };

    check(DataType::ASCII_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, false);
    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, true);
    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT, false);
}

TEST(PythonStringHandler, PresentNullRespectsStringFormatAndEncoding) {
    using namespace arcticdb;

    StringPool string_pool;
    DecodePathData shared_data;

    auto check = [&](DataType type, PandasStringFormat format, bool expect_nan) {
        const auto descriptor = make_scalar_type(type);
        const entity::FieldWrapper field(descriptor, "string");
        const ColumnMapping mapping(
                descriptor, descriptor, field.field(), sizeof(PyObject*), 1, 0, 0, sizeof(PyObject*), 0
        );
        Column source(descriptor);
        source.set_scalar(0, string_none);
        PythonHandlerData handler_data;
        PyObject* output = nullptr;

        ReadOptions options;
        options.set_output_config(PandasOutputConfig{format});
        write_python_dynamic_strings_to_dest(
                &output,
                source,
                mapping,
                string_pool,
                shared_data,
                handler_data,
                options.pandas_none_value_for_type(descriptor)
        );
        EXPECT_EQ(output, expect_nan ? handler_data.non_owning_nan_handle() : Py_None);
    };

    check(DataType::ASCII_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, false);
    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT_WITH_NANS_ONLY, true);
    check(DataType::UTF_DYNAMIC64, PandasStringFormat::OBJECT, false);
}

TEST(Sparse, Simple) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    aggregator.start_row(timestamp{0})([](auto& rb) { rb.set_scalar_by_name("first", uint32_t(5), DataType::UINT32); });

    aggregator.start_row(timestamp{1})([](auto& rb) { rb.set_scalar_by_name("second", uint64_t(6), DataType::UINT64); }
    );

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    ASSERT_EQ(segment.row_count(), 2);
    auto val1 = segment.scalar_at<uint32_t>(0, 1);
    ASSERT_EQ(val1, 5);
    auto val2 = segment.scalar_at<uint32_t>(0, 2);
    ASSERT_EQ(static_cast<bool>(val2), false);
    auto val3 = segment.scalar_at<uint32_t>(1, 1);
    ASSERT_EQ(static_cast<bool>(val3), false);
    auto val4 = segment.scalar_at<uint32_t>(1, 2);
    ASSERT_EQ(val4, 6);
}

TEST_F(SparseTestStore, SimpleRoundtrip) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    aggregator.start_row(timestamp{0})([](auto& rb) { rb.set_scalar_by_name("first", uint32_t(5), DataType::UINT32); });

    aggregator.start_row(timestamp{1})([](auto& rb) { rb.set_scalar_by_name("second", uint64_t(6), DataType::UINT64); }
    );

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    test_store_->append_incomplete_segment(stream_id, std::move(segment));

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    ASSERT_EQ(frame.row_count(), 2);
    auto val1 = frame.scalar_at<uint32_t>(0, 1);
    ASSERT_EQ(val1, 5);
    auto val2 = frame.scalar_at<uint64_t>(0, 2);
    ASSERT_EQ(val2, 0);
    auto val3 = frame.scalar_at<uint32_t>(1, 1);
    ASSERT_EQ(val3, 0);
    auto val4 = frame.scalar_at<uint64_t>(1, 2);
    ASSERT_EQ(val4, 6);
}

// Just write a dummy StreamDescriptor to the TimeseriesDescriptor to avoid duplicating it
// as the same information is available in the segment header. This is how arcticc behaves so the test is important to
// check compat with existing data.
//
// This functions helps to test that readers look for the StreamDescriptor on the header, not in the
// TimeseriesDescriptor.
//
// Compare with `append_incomplete_segment`. Keep this function even if it duplicates `append_incomplete_segment` so
// that we have protection against `append_incomplete_segment` changing how it writes the descriptors in future.
void append_incomplete_segment_backwards_compat(
        const std::shared_ptr<arcticdb::Store>& store, const arcticdb::StreamId& stream_id,
        arcticdb::SegmentInMemory&& seg
) {
    using namespace arcticdb::proto::descriptors;
    using namespace arcticdb::stream;

    auto [next_key, total_rows] = read_head(store, stream_id);

    auto start_index = TimeseriesIndex::start_value_for_segment(seg);
    auto end_index = TimeseriesIndex::end_value_for_segment(seg);
    auto seg_row_count = seg.row_count();

    // Dummy StreamDescriptor
    auto desc = stream_descriptor(stream_id, RowCountIndex{}, {});

    auto tsd = arcticdb::make_timeseries_descriptor(
            seg_row_count, std::move(desc), NormalizationMetadata{}, std::nullopt, std::move(next_key), false
    );

    seg.set_timeseries_descriptor(std::move(tsd));
    auto new_key =
            store->write(arcticdb::stream::KeyType::APPEND_DATA, 0, stream_id, start_index, end_index, std::move(seg))
                    .get();

    total_rows += seg_row_count;
    write_head(store, to_atom(std::move(new_key)), total_rows);
}

TEST_F(SparseTestStore, SimpleRoundtripBackwardsCompat) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    aggregator.start_row(timestamp{0})([](auto& rb) { rb.set_scalar_by_name("first", uint32_t(5), DataType::UINT32); });

    aggregator.start_row(timestamp{1})([](auto& rb) { rb.set_scalar_by_name("second", uint64_t(6), DataType::UINT64); }
    );

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    append_incomplete_segment_backwards_compat(test_store_->_test_get_store(), stream_id, std::move(segment));

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    ASSERT_EQ(frame.row_count(), 2);
    auto val1 = frame.scalar_at<uint32_t>(0, 1);
    ASSERT_EQ(val1, 5);
    auto val2 = frame.scalar_at<uint64_t>(0, 2);
    ASSERT_EQ(val2, 0);
    auto val3 = frame.scalar_at<uint32_t>(1, 1);
    ASSERT_EQ(val3, 0);
    auto val4 = frame.scalar_at<uint64_t>(1, 2);
    ASSERT_EQ(val4, 6);
}

TEST_F(SparseTestStore, DenseToSparse) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    for (auto i = 0; i < 5; ++i) {
        aggregator.start_row(timestamp{i})([&](auto& rb) {
            rb.set_scalar_by_name("first", uint32_t(i + 1), DataType::UINT32);
        });
    }

    aggregator.start_row(timestamp{5})([](auto& rb) { rb.set_scalar_by_name("second", uint64_t(6), DataType::UINT64); }
    );

    aggregator.start_row(timestamp{6})([](auto& rb) { rb.set_scalar_by_name("first", uint32_t(7), DataType::UINT32); });

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    test_store_->append_incomplete_segment(stream_id, std::move(segment));

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    ASSERT_EQ(frame.row_count(), 7);
    for (int i = 0; i < 5; ++i) {
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 1);
    }
    auto val2 = frame.scalar_at<uint64_t>(5, 2);
    ASSERT_EQ(val2, 6);
    auto val3 = frame.scalar_at<uint32_t>(6, 1);
    ASSERT_EQ(val3, 7);
}

TEST_F(SparseTestStore, SimpleRoundtripStrings) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    std::optional<ScopedGILLock> scoped_gil_lock;
    register_python_string_types();
    register_python_handler_data_factory();
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    aggregator.start_row(timestamp{0})([](auto& rb) {
        rb.set_scalar_by_name(std::string_view{"first"}, std::string_view{"five"}, DataType::UTF_DYNAMIC64);
    });

    aggregator.start_row(timestamp{1})([](auto& rb) {
        rb.set_scalar_by_name(std::string_view{"second"}, std::string_view{"six"}, DataType::UTF_FIXED64);
    });

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    test_store_->append_incomplete_segment(stream_id, std::move(segment));

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::PANDAS));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    apply_global_refcounts(*handler_data, OutputFormat::PANDAS);

    ASSERT_EQ(frame.row_count(), 2);
    auto val1 = frame.scalar_at<PyObject*>(0, 1);
    auto str_wrapper = convert::py_unicode_to_buffer(val1.value(), scoped_gil_lock);
    ASSERT_TRUE(std::holds_alternative<convert::PyStringWrapper>(str_wrapper));
    ASSERT_EQ(strcmp(std::get<convert::PyStringWrapper>(str_wrapper).buffer_, "five"), 0);

    auto val2 = frame.string_at(0, 2);
    ASSERT_EQ(val2.value()[0], char{0});

    auto val3 = frame.scalar_at<PyObject*>(1, 1);
    ASSERT_TRUE(is_py_none(val3.value()));
    auto val4 = frame.string_at(1, 2);
    ASSERT_EQ(val4, "six");
}

TEST_F(SparseTestStore, Multiblock) {
    SKIP_WIN("Works OK but fills up LMDB");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::NeverSegmentPolicy, stream::SparseColumnPolicy>;
    using DynamicSinkWrapper = SinkWrapperImpl<DynamicAggregator>;

    const std::string stream_id("test_sparse");

    DynamicSinkWrapper wrapper(stream_id, {});
    auto& aggregator = wrapper.aggregator_;

    constexpr size_t num_rows = 1000000;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    wrapper.aggregator_.commit();

    auto segment = wrapper.segment();
    test_store_->append_incomplete_segment(stream_id, std::move(segment));

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    for (size_t i = 0; i < num_rows; i += 2) {
        ASSERT_EQ(frame.row_count(), num_rows);
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 1);
        auto val2 = frame.scalar_at<uint64_t>(i, 2);
        ASSERT_EQ(val2, 0);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        ASSERT_EQ(val3, 0);
        auto val4 = frame.scalar_at<uint64_t>(i + 1, 2);
        ASSERT_EQ(val4, i + 2);
    }
}

TEST_F(SparseTestStore, Segment) {
    SKIP_WIN("Works OK but fills up LMDB");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");
    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) { test_store_->append_incomplete_segment(stream_id, std::move(segment)); },
            RowCountSegmentPolicy{1000}
    );

    constexpr size_t num_rows = 1000000;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    aggregator.commit();

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    for (size_t i = 0; i < num_rows; i += 2) {
        ASSERT_EQ(frame.row_count(), num_rows);
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 1);
        auto val2 = frame.scalar_at<uint64_t>(i, 2);
        ASSERT_EQ(val2, 0);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        ASSERT_EQ(val3, 0);
        auto val4 = frame.scalar_at<uint64_t>(i + 1, 2);
        ASSERT_EQ(val4, i + 2);
    }
}

TEST_F(SparseTestStore, SegmentWithExistingIndex) {
    SKIP_WIN("Flaky in Windows CI Monday: 8416830355");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");

    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    bool written = false;
    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) {
                if (!written) {
                    test_store_->write_segment(
                            stream_id, std::move(segment), false, arcticdb::version_store::Slicing::NoSlicing
                    );
                    written = true;
                } else {
                    test_store_->append_incomplete_segment(stream_id, std::move(segment));
                }
            },
            RowCountSegmentPolicy{1000}
    );

    constexpr size_t num_rows = 100000;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    aggregator.commit();

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    ASSERT_EQ(frame.row_count(), num_rows);
    for (size_t i = 0; i < num_rows; i += 2) {
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        check_value(val1, i + 1);
        auto val2 = frame.scalar_at<uint64_t>(i, 2);
        check_value(val2, 0);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        check_value(val3, 0);
        auto val4 = frame.scalar_at<uint64_t>(i + 1, 2);
        check_value(val4, i + 2);
    }
}

TEST_F(SparseTestStore, SegmentAndFilterColumn) {
    SKIP_WIN("Flaky in Windows CI Monday: 8416830355");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");

    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    bool written = false;
    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) {
                if (!written) {
                    test_store_->write_segment(
                            stream_id, std::move(segment), false, arcticdb::version_store::Slicing::NoSlicing
                    );
                    written = true;
                } else {
                    test_store_->append_incomplete_segment(stream_id, std::move(segment));
                }
            },
            RowCountSegmentPolicy{1000}
    );

    constexpr size_t num_rows = 100000;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    aggregator.commit();

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->columns = {"time", "first"};
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;
    ASSERT_EQ(frame.row_count(), num_rows);
    ASSERT_EQ(frame.descriptor().field_count(), 2);

    for (size_t i = 0; i < num_rows; i += 2) {
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 1);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        ASSERT_EQ(val3, 0);
    }
}

TEST_F(SparseTestStore, SegmentWithRangeFilter) {
    SKIP_WIN("Flaky in Windows CI Monday: 8416830355");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");
    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    bool written = false;
    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) {
                if (!written) {
                    test_store_->write_segment(
                            stream_id, std::move(segment), false, arcticdb::version_store::Slicing::NoSlicing
                    );
                    written = true;
                } else {
                    test_store_->append_incomplete_segment(stream_id, std::move(segment));
                }
            },
            RowCountSegmentPolicy{1000}
    );

    constexpr size_t num_rows = 10000;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    aggregator.commit();

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    read_options.set_incompletes(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = IndexRange(timestamp{3000}, timestamp{6999});
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ;

    ASSERT_EQ(frame.row_count(), 4000);
    for (size_t i = 0; i < frame.row_count(); i += 2) {
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 3001);
        auto val2 = frame.scalar_at<uint64_t>(i, 2);
        ASSERT_EQ(val2, 0);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        ASSERT_EQ(val3, 0);
        auto val4 = frame.scalar_at<uint64_t>(i + 1, 2);
        ASSERT_EQ(val4, i + 3002);
    }
}

TEST_F(SparseTestStore, Compact) {
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    const std::string stream_id("test_sparse");
    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) {
                ASSERT_TRUE(segment.is_index_sorted());
                segment.descriptor().set_sorted(SortedValue::ASCENDING);
                test_store_->append_incomplete_segment(stream_id, std::move(segment));
            },
            RowCountSegmentPolicy{10}
    );

    constexpr size_t num_rows = 100;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"first"}, uint32_t(i + 1), DataType::UINT32);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            rb.set_scalar_by_name(std::string_view{"second"}, uint64_t(i + 2), DataType::UINT64);
        });
    }

    aggregator.commit();
    test_store_->compact_incomplete(stream_id, false, false, false);

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    register_native_handler_data_factory();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::NATIVE));
    auto read_result = test_store_->read_dataframe_version(
            stream_id, pipelines::VersionQuery{}, read_query, read_options, handler_data
    );
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();

    ASSERT_EQ(frame.row_count(), num_rows);
    for (size_t i = 0; i < num_rows; i += 2) {
        auto val1 = frame.scalar_at<uint32_t>(i, 1);
        ASSERT_EQ(val1, i + 1);
        auto val2 = frame.scalar_at<uint64_t>(i, 2);
        ASSERT_EQ(val2, 0);
        auto val3 = frame.scalar_at<uint32_t>(i + 1, 1);
        ASSERT_EQ(val3, 0);
        auto val4 = frame.scalar_at<uint64_t>(i + 1, 2);
        ASSERT_EQ(val4, i + 2);
    }
}

TEST_F(SparseTestStore, CompactWithStrings) {
    SKIP_WIN("Flaky in Windows CI Monday: 8416830355");
    using namespace arcticdb;
    using namespace arcticdb::stream;
    using DynamicAggregator =
            Aggregator<TimeseriesIndex, DynamicSchema, stream::RowCountSegmentPolicy, stream::SparseColumnPolicy>;

    register_python_string_types();
    register_python_handler_data_factory();
    const std::string stream_id("test_sparse");

    const auto index = TimeseriesIndex::default_index();
    DynamicSchema schema{index.create_stream_descriptor(stream_id, {}), index};

    DynamicAggregator aggregator(
            std::move(schema),
            [&](SegmentInMemory&& segment) {
                ASSERT_TRUE(segment.is_index_sorted());
                segment.descriptor().set_sorted(SortedValue::ASCENDING);
                test_store_->append_incomplete_segment(stream_id, std::move(segment));
            },
            RowCountSegmentPolicy{10}
    );

    constexpr size_t num_rows = 100;

    for (size_t i = 0; i < num_rows; i += 2) {
        aggregator.start_row(timestamp(i))([&](auto& rb) {
            auto val = fmt::format("{}", i + 1);
            rb.set_scalar_by_name(std::string_view{"first"}, std::string_view{val}, DataType::UTF_DYNAMIC64);
        });

        aggregator.start_row(timestamp(i + 1))([&](auto& rb) {
            auto val = fmt::format("{}", i + 2);
            rb.set_scalar_by_name(std::string_view{"second"}, std::string_view{val}, DataType::UTF_FIXED64);
        });
    }

    aggregator.commit();
    test_store_->compact_incomplete(stream_id, false, false, false);

    ReadOptions read_options;
    read_options.set_dynamic_schema(true);
    auto read_query = std::make_shared<ReadQuery>();
    read_query->row_filter = universal_range();
    auto handler_data =
            std::make_shared<std::any>(TypeHandlerRegistry::instance()->get_handler_data(OutputFormat::PANDAS));
    auto read_result =
            test_store_->read_dataframe_version(stream_id, VersionQuery{}, read_query, read_options, handler_data);
    apply_global_refcounts(*handler_data, OutputFormat::PANDAS);
    const auto& frame = std::get<PandasOutputFrame>(read_result.frame_data).frame();
    ASSERT_EQ(frame.row_count(), num_rows);

    for (size_t i = 0; i < num_rows; i += 2) {
        auto val1 = frame.scalar_at<PyObject*>(i, 1);
        std::optional<ScopedGILLock> scoped_gil_lock;
        auto str_wrapper = convert::py_unicode_to_buffer(val1.value(), scoped_gil_lock);
        auto expected = fmt::format("{}", i + 1);
        ASSERT_TRUE(std::holds_alternative<convert::PyStringWrapper>(str_wrapper));
        ASSERT_EQ(strcmp(std::get<convert::PyStringWrapper>(str_wrapper).buffer_, expected.data()), 0);

        auto val2 = frame.string_at(i, 2);
        ASSERT_EQ(val2.value()[0], char{0});

        auto val3 = frame.scalar_at<PyObject*>(i + 1, 1);
        ASSERT_TRUE(is_py_none(val3.value()));

        auto val4 = frame.string_at(i + 1, 2);

        expected = fmt::format("{}", i + 2);
        ASSERT_EQ(strncmp(val4.value().data(), expected.data(), expected.size()), 0);
    }
}
