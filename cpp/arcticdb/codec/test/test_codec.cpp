/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>

#include <gtest/gtest.h>

using namespace arcticdb;

TEST(FieldEncoderTest, PassthroughDim0) {

    using TD = TypeDescriptorTag<
        DataTypeTag<DataType::FLOAT64>,
        DimensionTag<Dimension::Dim0>
    >;
    using Encoder = BlockEncoder<TD>;

    std::vector<double> v{0.1, 0.2, 0.3};
    const shape_t *shape = nullptr;
    using Field = Encoder::FieldType;

    Field f(v.data(), shape, v.size() * sizeof(double), v.size(), nullptr);

    arcticdb::proto::encoding::EncodedField field;
    arcticdb::proto::encoding::VariantCodec opt;

    Buffer out{Encoder::max_compressed_size(opt, f)};
    std::ptrdiff_t pos = 0;

    Encoder::encode(opt, f, field, out, pos);

    auto &nd = field.ndarray();
    ASSERT_EQ(nd.items_count(), 3);
    ASSERT_FALSE(nd.shapes_size() > 0);
    auto &vals = nd.values();

    auto expected_bytes = v.size() * sizeof(TD::DataTypeTag::raw_type);
    ASSERT_EQ(vals[0].in_bytes(), expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos , expected_bytes);
}

TEST(FieldEncoderTest, PassthroughDim0_Dynamic) {
    TypeDescriptor td{DataType::FLOAT64, Dimension::Dim0};
    std::vector<double> v{0.1};
    ChunkedBuffer cbuf;
    cbuf.ensure(sizeof(double));
    memcpy(cbuf.ptr_cast<uint8_t>(0, sizeof(double)), v.data(), sizeof(double));
    Buffer shapes;
    ColumnData f(&cbuf, &shapes, td, nullptr);

    arcticdb::proto::encoding::EncodedField field;
    arcticdb::proto::encoding::VariantCodec opt;

    std::ptrdiff_t pos = 0;

    ColumnEncoder enc;
    const auto [uncomp, req] = enc.max_compressed_size(opt, f);
    Buffer out(req);
    f.reset();
    enc.encode(opt, f, field, out, pos);

    auto &nd = field.ndarray();
    ASSERT_EQ(nd.items_count(), 1);
    ASSERT_FALSE(nd.shapes_size() > 0);
    auto &vals = nd.values();

    auto expected_bytes = v.size() * sizeof(v[0]);
    ASSERT_EQ(vals[0].in_bytes(), expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, expected_bytes);
}

TEST(FieldEncoderTest, PassthroughDim1) {
    using TD = TypeDescriptorTag<
        DataTypeTag<DataType::FLOAT64>,
        DimensionTag<Dimension::Dim1>
    >;

    using Encoder = BlockEncoder<TD>;
    using Field = Encoder::FieldType;

    std::vector<double> v{0.1, 0.2, 0.3, 0.4};
    std::vector<shape_t> s{2, 2};
    Field f(v.data(), s.data(), v.size() * sizeof(double), s.size(), nullptr);

    arcticdb::proto::encoding::EncodedField field;
    arcticdb::proto::encoding::VariantCodec opt;

    Buffer out(Encoder::max_compressed_size(opt, f));
    std::ptrdiff_t pos = 0;

    Encoder::encode(opt, f, field, out, pos);

    auto &nd = field.ndarray();
    ASSERT_EQ(nd.items_count(), 2);
    auto &shapes = nd.shapes();
    auto shapes_bytes = 2 * sizeof(shape_t);

    ASSERT_EQ(shapes[0].in_bytes(), shapes_bytes);
    ASSERT_EQ(shapes[0].out_bytes(), shapes_bytes);
    ASSERT_NE(0, shapes[0].hash());

    auto &vals = nd.values();
    auto expected_bytes = v.size() * sizeof(TD::DataTypeTag::raw_type);
    ASSERT_EQ(vals[0].in_bytes(), expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, expected_bytes + shapes_bytes);
}

TEST(SegmentencoderTest, EncodeStringsBasic) {
    const auto tsd = create_tsd<DataTypeTag<DataType::ASCII_DYNAMIC64>, Dimension::Dim0>();
    SegmentInMemory s(StreamDescriptor{tsd});
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
    arcticdb::proto::encoding::VariantCodec opt;
    auto lz4ptr = opt.mutable_lz4();
    lz4ptr->set_acceleration(1);
    auto copy = s.clone();
    Segment seg = encode(SegmentInMemory{s}, opt);

    SegmentInMemory res = decode(std::move(seg));
    ASSERT_EQ(copy.string_at(0, 1), res.string_at(0, 1));
    ASSERT_EQ(std::string("happy"), res.string_at(0, 1));
    ASSERT_EQ(copy.string_at(1, 3), res.string_at(1, 3));
    ASSERT_EQ(std::string("baggy"), res.string_at(1, 3));
}

using namespace arcticdb;
namespace as = arcticdb::stream;

TEST(SegmentEncoderTest, StressTestString) {
    const size_t NumTests = 100000;
    const size_t VectorSize = 0x1000;
    const size_t NumColumns = 7;
    init_random(21);
    auto strings = random_string_vector(VectorSize);

    SegmentsSink sink;
    auto index = as::TimeseriesIndex::default_index();
    as::FixedSchema schema{
        index.create_stream_descriptor(123, {
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_1"),
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_2"),
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_3"),
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_4"),
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_5"),
            scalar_field_proto(DataType::ASCII_DYNAMIC64, "col_6"),
            }), index
    };

    TestAggregator agg(std::move(schema), [&](SegmentInMemory &&mem) {
        sink.segments_.push_back(std::move(mem));
    }, as::NeverSegmentPolicy{});

    for (size_t i = 0; i < NumTests; ++i) {
        agg.start_row(timestamp(i))([&](auto &rb) {
            for (size_t j = 1; j < NumColumns; ++j)
                rb.set_string(timestamp(j), strings[(i + j) & (VectorSize - 1)]);
        });
    }
}