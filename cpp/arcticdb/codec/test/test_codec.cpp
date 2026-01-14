/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/util/buffer.hpp>
#include <arcticdb/codec/codec.hpp>
#include <arcticdb/entity/types.hpp>
#include <arcticdb/util/test/test_utils.hpp>
#include <arcticdb/util/test/generators.hpp>
#include <arcticdb/util/random.h>
#include <arcticdb/stream/row_builder.hpp>
#include <arcticdb/stream/aggregator.hpp>
#include <arcticdb/codec/typed_block_encoder_impl.hpp>

#include <gtest/gtest.h>

namespace arcticdb {
struct ColumnEncoderV1 {
    static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data
    );

    static void encode(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data,
            EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos
    );
};

struct ColumnEncoderV2 {
  public:
    static void encode(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data,
            EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos
    );
    static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data
    );

  private:
    static void encode_shapes(
            const ColumnData& column_data, EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos_in_buffer
    );

    static void encode_blocks(
            const arcticdb::proto::encoding::VariantCodec& codec_opts, ColumnData& column_data,
            EncodedFieldImpl& variant_field, Buffer& out, std::ptrdiff_t& pos
    );
};

size_t calc_column_blocks_size(const Column& col);
} // namespace arcticdb

using namespace arcticdb;

using EncodingVersions = ::testing::Types<
        std::integral_constant<EncodingVersion, EncodingVersion::V1>,
        std::integral_constant<EncodingVersion, EncodingVersion::V2>>;

class FieldEncoderTestDim0Base : public testing::Test {
  protected:
    using ValuesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension::Dim0>>;
    static constexpr TypeDescriptor type_descriptor = static_cast<TypeDescriptor>(ValuesTypeDescriptorTag());
    static constexpr std::array<double, 3> values = {0.1, 0.2, 0.3};
    static constexpr size_t values_byte_size = values.size() * sizeof(decltype(values)::value_type);
    static constexpr size_t expected_bytes = values.size() * sizeof(ValuesTypeDescriptorTag::DataTypeTag::raw_type);
    static_assert(std::is_same_v<ValuesTypeDescriptorTag::DataTypeTag::raw_type, decltype(values)::value_type>);
    arcticdb::proto::encoding::VariantCodec passthorugh_encoding_options;
};

template<typename EncodedFieldType>
class FieldEncoderTestDim0 : public FieldEncoderTestDim0Base {};

using EncodedFieldsType = ::testing::Types<EncodedFieldImpl>;
TYPED_TEST_SUITE(FieldEncoderTestDim0, EncodedFieldsType);

TYPED_TEST(FieldEncoderTestDim0, Passthrough_v1) {
    using Encoder =
            TypedBlockEncoderImpl<TypedBlockData, typename TestFixture::ValuesTypeDescriptorTag, EncodingVersion::V1>;
    const TypedBlockData<typename TestFixture::ValuesTypeDescriptorTag> values_block(
            TestFixture::values.data(), nullptr, TestFixture::values_byte_size, TestFixture::values.size(), nullptr
    );
    TypeParam encoded_field;
    Buffer out{Encoder::max_compressed_size(TestFixture::passthorugh_encoding_options, values_block)};
    std::ptrdiff_t pos = 0;
    Encoder::encode(TestFixture::passthorugh_encoding_options, values_block, encoded_field, out, pos);
    auto& nd = encoded_field.ndarray();
    ASSERT_EQ(nd.items_count(), TestFixture::values.size());
    ASSERT_EQ(nd.shapes_size(), 0);
    const auto& vals = nd.values();
    ASSERT_EQ(vals[0].in_bytes(), TestFixture::expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), TestFixture::expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, TestFixture::expected_bytes);
}

TYPED_TEST(FieldEncoderTestDim0, Passthrough_v2) {
    using Encoder =
            TypedBlockEncoderImpl<TypedBlockData, typename TestFixture::ValuesTypeDescriptorTag, EncodingVersion::V2>;
    const TypedBlockData<typename TestFixture::ValuesTypeDescriptorTag> values_block(
            TestFixture::values.data(), nullptr, TestFixture::values_byte_size, TestFixture::values.size(), nullptr
    );
    TypeParam encoded_field;
    Buffer out{Encoder::max_compressed_size(TestFixture::passthorugh_encoding_options, values_block)};
    std::ptrdiff_t pos = 0;
    Encoder::encode_values(TestFixture::passthorugh_encoding_options, values_block, encoded_field, out, pos);
    auto& nd = encoded_field.ndarray();
    ASSERT_EQ(nd.items_count(), TestFixture::values.size());
    ASSERT_EQ(nd.shapes_size(), 0);
    const auto& vals = nd.values();
    ASSERT_EQ(vals[0].in_bytes(), TestFixture::expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), TestFixture::expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, TestFixture::expected_bytes);
}

template<typename EncodingVersionConstant>
class FieldEncoderTestFromColumnDim0 : public FieldEncoderTestDim0Base {};

/// @brief Cartesian product between the type of the encoded field and the encoding version.
/// (EncodedField, arcticdb::proto::encoding::EncodedField) x (EncodingVersion::V1, EncodingVersion::V2)
using FieldVersionT =
        ::testing::Types<std::pair<EncodedFieldImpl, ColumnEncoderV1>, std::pair<EncodedFieldImpl, ColumnEncoderV2>>;
TYPED_TEST_SUITE(FieldEncoderTestFromColumnDim0, FieldVersionT);

TYPED_TEST(FieldEncoderTestFromColumnDim0, Passthrough) {
    using EncodedFieldType = typename TypeParam::first_type;
    using ColumnEncoder = typename TypeParam::second_type;

    ChunkedBuffer values_buffer;
    values_buffer.ensure(TestFixture::values_byte_size);
    memcpy(values_buffer.ptr_cast<uint8_t>(0, TestFixture::values_byte_size),
           TestFixture::values.data(),
           TestFixture::values_byte_size);
    Buffer shapes_buffer;
    ColumnData column_data(&values_buffer, &shapes_buffer, TestFixture::type_descriptor, nullptr);
    EncodedFieldType field;
    std::ptrdiff_t pos = 0;
    const auto [_, max_compressed_size] =
            ColumnEncoder::max_compressed_size(TestFixture::passthorugh_encoding_options, column_data);
    Buffer out(max_compressed_size);
    column_data.reset();
    ColumnEncoder::encode(TestFixture::passthorugh_encoding_options, column_data, field, out, pos);
    auto& nd = field.ndarray();
    ASSERT_EQ(nd.items_count(), TestFixture::values.size());
    ASSERT_EQ(nd.shapes_size(), 0);
    const auto& vals = nd.values();
    ASSERT_EQ(vals[0].in_bytes(), TestFixture::expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), TestFixture::expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, TestFixture::expected_bytes);
}

class FieldEncoderTestDim1 : public testing::Test {
  protected:
    using ValuesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::FLOAT64>, DimensionTag<Dimension::Dim1>>;
    static constexpr std::array<double, 5> values = {0.1, 0.2, 0.3, 0.4, 0.5};
    static constexpr size_t values_byte_size = values.size() * sizeof(decltype(values)::value_type);
    static constexpr std::array<shape_t, 2> shapes = {2, 3};
    static constexpr size_t shapes_byte_size = shapes.size() * sizeof(decltype(shapes)::value_type);
    static constexpr size_t values_expected_bytes =
            values.size() * sizeof(ValuesTypeDescriptorTag::DataTypeTag::raw_type);
    static_assert(std::is_same_v<ValuesTypeDescriptorTag::DataTypeTag::raw_type, decltype(values)::value_type>);
    arcticdb::proto::encoding::VariantCodec passthorugh_encoding_options;
};

TEST_F(FieldEncoderTestDim1, PassthroughV1NativeField) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ValuesTypeDescriptorTag, EncodingVersion::V1>;
    const TypedBlockData<ValuesTypeDescriptorTag> block(
            values.data(), shapes.data(), values_byte_size, shapes.size(), nullptr
    );

    // one block for shapes and one for values
    constexpr size_t encoded_field_size = EncodedFieldImpl::Size + 2 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_memory;
    EncodedFieldImpl* field = new (encoded_field_memory.data()) EncodedFieldImpl;

    Buffer out(Encoder::max_compressed_size(passthorugh_encoding_options, block));
    std::ptrdiff_t pos = 0;
    Encoder::encode(passthorugh_encoding_options, block, *field, out, pos);

    const auto& nd = field->ndarray();
    ASSERT_EQ(nd.items_count(), shapes.size());

    const auto& shapes = nd.shapes();
    ASSERT_EQ(shapes[0].in_bytes(), shapes_byte_size);
    ASSERT_EQ(shapes[0].out_bytes(), shapes_byte_size);
    ASSERT_NE(0, shapes[0].hash());

    const auto& vals = nd.values();
    ASSERT_EQ(vals[0].in_bytes(), values_expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), values_expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, values_expected_bytes + shapes_byte_size);
}

TEST_F(FieldEncoderTestDim1, PassthroughV2NativeField) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ValuesTypeDescriptorTag, EncodingVersion::V2>;
    using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, arcticdb::ShapesBlockTDT, EncodingVersion::V2>;
    const TypedBlockData<ValuesTypeDescriptorTag> values_block(
            values.data(), shapes.data(), values_byte_size, shapes.size(), nullptr
    );

    const TypedBlockData<arcticdb::ShapesBlockTDT> shapes_block(shapes.data(), nullptr, shapes_byte_size, 0, nullptr);

    const size_t values_max_compressed_size = Encoder::max_compressed_size(passthorugh_encoding_options, values_block);
    const size_t shapes_max_compressed_size =
            ShapesEncoder::max_compressed_size(passthorugh_encoding_options, shapes_block);
    const size_t total_max_compressed_size = values_max_compressed_size + shapes_max_compressed_size;
    // one block for shapes and one for values
    constexpr size_t encoded_field_size = EncodedFieldImpl::Size + 2 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_memory;
    EncodedFieldImpl* field = new (encoded_field_memory.data()) EncodedFieldImpl;
    Buffer out(total_max_compressed_size);
    std::ptrdiff_t pos = 0;
    ShapesEncoder::encode_shapes(passthorugh_encoding_options, shapes_block, *field, out, pos);
    Encoder::encode_values(passthorugh_encoding_options, values_block, *field, out, pos);

    const auto& nd = field->ndarray();
    ASSERT_EQ(nd.items_count(), shapes.size());

    const auto& shapes = nd.shapes();
    ASSERT_EQ(shapes[0].in_bytes(), shapes_byte_size);
    ASSERT_EQ(shapes[0].out_bytes(), shapes_byte_size);
    ASSERT_NE(0, shapes[0].hash());

    const auto& vals = nd.values();
    ASSERT_EQ(vals[0].in_bytes(), values_expected_bytes);
    ASSERT_EQ(vals[0].out_bytes(), values_expected_bytes);
    ASSERT_NE(0, vals[0].hash());
    ASSERT_EQ(pos, values_expected_bytes + shapes_byte_size);
}

class TestMultiblockData_Dim1 : public testing::Test {
  protected:
    void SetUp() override {
        data_buffer.add_block(first_block_data_byte_size, 0);
        data_buffer.blocks()[0]->resize(first_block_data_byte_size);
        data_buffer.add_block(second_block_data_byte_size, first_block_data_byte_size);
        data_buffer.blocks()[1]->resize(second_block_data_byte_size);
        shapes_buffer.ensure(shapes_data_byte_size);
        data_buffer.blocks()[0]->copy_from(
                reinterpret_cast<const uint8_t*>(first_block_data.data()), first_block_data_byte_size, 0
        );
        data_buffer.blocks()[1]->copy_from(
                reinterpret_cast<const uint8_t*>(second_block_data.data()), second_block_data_byte_size, 0
        );
        memcpy(shapes_buffer.data(), shapes_data.data(), shapes_data_byte_size);
    }

    using ValuesTypeDescriptorTag = TypeDescriptorTag<DataTypeTag<DataType::INT64>, DimensionTag<Dimension::Dim1>>;
    static constexpr TypeDescriptor type_descriptor = static_cast<TypeDescriptor>(ValuesTypeDescriptorTag());
    static constexpr std::array<int64_t, 8> first_block_data = {1, 2, 3, 4, 5, 6, 7, 8};
    static constexpr size_t first_block_data_byte_size =
            sizeof(decltype(first_block_data)::value_type) * first_block_data.size();
    static constexpr std::array<int64_t, 2> second_block_data = {9, 10};
    static constexpr size_t second_block_data_byte_size =
            sizeof(decltype(second_block_data)::value_type) * second_block_data.size();
    static constexpr std::array<shape_t, 2> shapes_data = {first_block_data.size(), second_block_data.size()};
    static constexpr size_t shapes_data_byte_size = sizeof(decltype(shapes_data)::value_type) * shapes_data.size();
    arcticdb::proto::encoding::VariantCodec passthorugh_encoding_options;
    ChunkedBuffer data_buffer;
    Buffer shapes_buffer;
};

TEST_F(TestMultiblockData_Dim1, EncodingVersion_2) {
    constexpr size_t encoded_field_size = EncodedFieldImpl::Size + 3 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_owner;
    EncodedFieldImpl* encoded_field = new (encoded_field_owner.data()) EncodedFieldImpl;
    ColumnData column_data(&data_buffer, &shapes_buffer, type_descriptor, nullptr);
    const auto [_, max_compressed_size] =
            ColumnEncoderV2::max_compressed_size(passthorugh_encoding_options, column_data);
    Buffer out(max_compressed_size);
    ptrdiff_t out_pos = 0;
    column_data.reset();
    ColumnEncoderV2::encode(passthorugh_encoding_options, column_data, *encoded_field, out, out_pos);
    const auto& ndarray = encoded_field->ndarray();
    ASSERT_EQ(ndarray.shapes_size(), 1);
    ASSERT_EQ(ndarray.values_size(), 2);
    ASSERT_EQ(ndarray.items_count(), shapes_data.size());
}

template<typename EncodedFieldType>
class SegmentStringEncodingTest : public testing::Test {};

TYPED_TEST_SUITE(SegmentStringEncodingTest, EncodingVersions);

TYPED_TEST(SegmentStringEncodingTest, EncodeSingleString) {
    const auto tsd = create_tsd<DataTypeTag<DataType::ASCII_DYNAMIC64>, Dimension::Dim0>("thing", 1);
    SegmentInMemory s(StreamDescriptor{tsd});
    s.set_scalar(0, timestamp(123));
    s.set_string(1, "happy");
    s.end_row();
    arcticdb::proto::encoding::VariantCodec opt;
    auto lz4ptr = opt.mutable_lz4();
    lz4ptr->set_acceleration(1);
    auto copy = s.clone();
    constexpr EncodingVersion encoding_version = TypeParam::value;
    Segment seg = encode_dispatch(s.clone(), opt, encoding_version);

    SegmentInMemory res = decode_segment(seg);
    ASSERT_EQ(copy.string_at(0, 1), res.string_at(0, 1));
    ASSERT_EQ(std::string("happy"), res.string_at(0, 1));
}

TYPED_TEST(SegmentStringEncodingTest, EncodeStringsBasic) {
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
    constexpr EncodingVersion encoding_version = TypeParam::value;
    Segment seg = encode_dispatch(SegmentInMemory{s}, opt, encoding_version);

    SegmentInMemory res = decode_segment(seg);
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
            index.create_stream_descriptor(
                    NumericId{123},
                    {
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_1"),
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_2"),
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_3"),
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_4"),
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_5"),
                            scalar_field(DataType::ASCII_DYNAMIC64, "col_6"),
                    }
            ),
            index
    };

    TestAggregator agg(
            std::move(schema),
            [&](SegmentInMemory&& mem) { sink.segments_.push_back(std::move(mem)); },
            as::NeverSegmentPolicy{}
    );

    for (size_t i = 0; i < NumTests; ++i) {
        agg.start_row(timestamp(i))([&](auto& rb) {
            for (size_t j = 1; j < NumColumns; ++j)
                rb.set_string(timestamp(j), strings[(i + j) & (VectorSize - 1)]);
        });
    }
}

struct TransactionalThing {
    arcticdb::util::MagicNum<'K', 'e', 'e', 'p'> magic_;
    static bool destroyed;
    ~TransactionalThing() { TransactionalThing::destroyed = true; }
};

bool TransactionalThing::destroyed = false;

TEST(Segment, KeepAlive) {
    {
        auto buf = std::make_shared<Buffer>();
        Segment segment;
        segment.set_buffer(std::move(buf));
        segment.set_keepalive(std::any(TransactionalThing{}));

        auto seg1 = std::move(segment);
        Segment seg2{std::move(seg1)};
        auto seg3 = seg2.clone();
        Segment seg4{seg3.clone()};

        std::any_cast<TransactionalThing>(seg2.keepalive()).magic_.check();
    }
    ASSERT_EQ(TransactionalThing::destroyed, true);
}

TEST(Segment, RoundtripTimeseriesDescriptorV1) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT8, "ints")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    in_mem_seg.set_scalar<uint8_t>(0, 23);
    in_mem_seg.end_row();
    TimeseriesDescriptor tsd;
    tsd.set_total_rows(23);
    tsd.set_stream_descriptor(stream_desc);
    in_mem_seg.set_timeseries_descriptor(tsd);
    auto copy = in_mem_seg.clone();
    auto seg = encode_v1(std::move(in_mem_seg), codec::default_lz4_codec());
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v1(seg, seg.header(), decoded, seg.descriptor());
    ASSERT_EQ(decoded.index_descriptor().total_rows(), 23);
    ASSERT_EQ(decoded, copy);
}

TEST(Segment, RoundtripTimeseriesDescriptorWriteToBufferV1) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT8, "ints")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    in_mem_seg.set_scalar<uint8_t>(0, 23);
    in_mem_seg.end_row();
    TimeseriesDescriptor tsd;
    tsd.set_total_rows(23);
    tsd.set_stream_descriptor(stream_desc);
    in_mem_seg.set_timeseries_descriptor(tsd);
    auto copy = in_mem_seg.clone();
    auto seg = encode_v1(std::move(in_mem_seg), codec::default_lz4_codec());
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    vec.resize(bytes);
    seg.write_to(vec.data());
    auto unserialized = Segment::from_bytes(vec.data(), bytes);
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v1(unserialized, unserialized.header(), decoded, unserialized.descriptor());
    ASSERT_EQ(decoded.index_descriptor().total_rows(), 23);
    ASSERT_EQ(decoded, copy);
}

TEST(Segment, RoundtripStringsWriteToBufferV1) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UTF_DYNAMIC64, "ints")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    in_mem_seg.set_string(0, "kismet");
    in_mem_seg.end_row();
    auto copy = in_mem_seg.clone();
    auto seg = encode_v1(std::move(in_mem_seg), codec::default_lz4_codec());
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    vec.resize(bytes);
    seg.write_to(vec.data());
    auto unserialized = Segment::from_bytes(vec.data(), bytes);
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v1(unserialized, unserialized.header(), decoded, unserialized.descriptor());
    ASSERT_EQ(decoded.string_at(0, 0).value(), "kismet");
    ASSERT_EQ(decoded, copy);
}

TEST(Segment, RoundtripTimeseriesDescriptorV2) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT8, "ints")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    in_mem_seg.set_scalar<uint8_t>(0, 23);
    in_mem_seg.end_row();
    TimeseriesDescriptor tsd;
    tsd.set_total_rows(23);
    tsd.set_stream_descriptor(stream_desc);
    in_mem_seg.set_timeseries_descriptor(tsd);
    auto copy = in_mem_seg.clone();
    auto seg = encode_v2(std::move(in_mem_seg), codec::default_lz4_codec());
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v2(seg, seg.header(), decoded, seg.descriptor());
    ASSERT_EQ(decoded.index_descriptor().total_rows(), 23);
    ASSERT_EQ(decoded, copy);
}

TEST(Segment, RoundtripTimeseriesDescriptorWriteToBufferV2) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT8, "ints")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    in_mem_seg.set_scalar<uint8_t>(0, 23);
    in_mem_seg.end_row();
    TimeseriesDescriptor tsd;
    tsd.set_total_rows(23);
    tsd.set_stream_descriptor(stream_desc);
    in_mem_seg.set_timeseries_descriptor(tsd);
    auto copy = in_mem_seg.clone();
    auto seg = encode_v2(std::move(in_mem_seg), codec::default_lz4_codec());
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    ARCTICDB_DEBUG(log::codec(), "## Resizing buffer to {} bytes", bytes);
    vec.resize(bytes);
    seg.write_to(vec.data());
    auto unserialized = Segment::from_bytes(vec.data(), bytes);
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v2(unserialized, unserialized.header(), decoded, unserialized.descriptor());
    ASSERT_EQ(decoded.index_descriptor().total_rows(), 23);
    ASSERT_EQ(decoded, copy);
}

TEST(Segment, RoundtripStatisticsV1) {
    ScopedConfig reload_interval("Statistics.GenerateOnWrite", 1);
    const auto stream_desc = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "int8"), scalar_field(DataType::FLOAT64, "doubles")}
    );

    SegmentInMemory in_mem_seg{stream_desc.clone()};
    constexpr size_t num_rows = 10;
    for (auto i = 0UL; i < num_rows; ++i) {
        in_mem_seg.set_scalar<uint8_t>(0, static_cast<uint8_t>(i));
        in_mem_seg.set_scalar<double>(1, static_cast<double>(i * 2));
        in_mem_seg.end_row();
    }
    in_mem_seg.calculate_statistics();
    auto copy = in_mem_seg.clone();
    auto seg = encode_v1(std::move(in_mem_seg), codec::default_lz4_codec());
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    vec.resize(bytes);
    seg.write_to(vec.data());
    auto unserialized = Segment::from_bytes(vec.data(), bytes);
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v1(unserialized, unserialized.header(), decoded, unserialized.descriptor());
    auto col1_stats = decoded.column(0).get_statistics();
    ASSERT_TRUE(col1_stats.has_max());
    ASSERT_EQ(col1_stats.get_max<uint8_t>(), 9);
    ASSERT_TRUE(col1_stats.has_max());
    ASSERT_EQ(col1_stats.get_min<uint8_t>(), 0);
    ASSERT_TRUE(col1_stats.has_unique());
    ASSERT_EQ(col1_stats.get_unique_count(), 10);
    auto col2_stats = decoded.column(1).get_statistics();
    ASSERT_TRUE(col2_stats.has_max());
    ASSERT_EQ(col2_stats.get_max<double>(), 18.0);
    ASSERT_TRUE(col2_stats.has_max());
    ASSERT_EQ(col2_stats.get_min<uint8_t>(), 0);
    ASSERT_TRUE(col2_stats.has_unique());
    ASSERT_EQ(col2_stats.get_unique_count(), 10);
}

TEST(Segment, RoundtripStatisticsV2) {
    ScopedConfig reload_interval("Statistics.GenerateOnWrite", 1);
    const auto stream_desc = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "int8"), scalar_field(DataType::FLOAT64, "doubles")}
    );

    SegmentInMemory in_mem_seg{stream_desc.clone()};
    constexpr size_t num_rows = 10;
    for (auto i = 0UL; i < num_rows; ++i) {
        in_mem_seg.set_scalar<uint8_t>(0, static_cast<uint8_t>(i));
        in_mem_seg.set_scalar<double>(1, static_cast<double>(i * 2));
        in_mem_seg.end_row();
    }
    in_mem_seg.calculate_statistics();
    auto copy = in_mem_seg.clone();
    auto seg = encode_v2(std::move(in_mem_seg), codec::default_lz4_codec());
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    vec.resize(bytes);
    seg.write_to(vec.data());
    auto unserialized = Segment::from_bytes(vec.data(), bytes);
    SegmentInMemory decoded{stream_desc.clone()};
    decode_v2(unserialized, unserialized.header(), decoded, unserialized.descriptor());
    auto col1_stats = decoded.column(0).get_statistics();
    ASSERT_TRUE(col1_stats.has_max());
    ASSERT_EQ(col1_stats.get_max<uint8_t>(), 9);
    ASSERT_TRUE(col1_stats.has_max());
    ASSERT_EQ(col1_stats.get_min<uint8_t>(), 0);
    ASSERT_TRUE(col1_stats.has_unique());
    ASSERT_EQ(col1_stats.get_unique_count(), 10);
    auto col2_stats = decoded.column(1).get_statistics();
    ASSERT_TRUE(col2_stats.has_max());
    ASSERT_EQ(col2_stats.get_max<double>(), 18.0);
    ASSERT_TRUE(col2_stats.has_max());
    ASSERT_EQ(col2_stats.get_min<uint8_t>(), 0);
    ASSERT_TRUE(col2_stats.has_unique());
    ASSERT_EQ(col2_stats.get_unique_count(), 10);
}

TEST(Segment, ColumnNamesProduceDifferentHashes) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints1"),
             scalar_field(DataType::UINT8, "ints2"),
             scalar_field(DataType::UINT8, "ints3"),
             scalar_field(DataType::UINT8, "ints4"),
             scalar_field(DataType::UINT8, "ints5")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    in_mem_seg_1.set_scalar(0, uint8_t(0));
    in_mem_seg_1.set_scalar(1, uint8_t(0));
    in_mem_seg_1.set_scalar(2, uint8_t(0));
    in_mem_seg_1.set_scalar(3, uint8_t(0));
    in_mem_seg_1.set_scalar(4, uint8_t(0));
    in_mem_seg_1.end_row();

    const auto stream_desc_2 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints6"),
             scalar_field(DataType::UINT8, "ints7"),
             scalar_field(DataType::UINT8, "ints8"),
             scalar_field(DataType::UINT8, "ints9"),
             scalar_field(DataType::UINT8, "ints10")}
    );

    SegmentInMemory in_mem_seg_2{stream_desc_2.clone()};

    in_mem_seg_2.set_scalar(0, uint8_t(0));
    in_mem_seg_2.set_scalar(1, uint8_t(0));
    in_mem_seg_2.set_scalar(2, uint8_t(0));
    in_mem_seg_2.set_scalar(3, uint8_t(0));
    in_mem_seg_2.set_scalar(4, uint8_t(0));
    in_mem_seg_2.end_row();

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V1);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V1);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_NE(hash_1, hash_2);
}

TEST(Segment, ColumnNamesProduceDifferentHashesEmpty) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints1"),
             scalar_field(DataType::UINT8, "ints2"),
             scalar_field(DataType::UINT8, "ints3"),
             scalar_field(DataType::UINT8, "ints4"),
             scalar_field(DataType::UINT8, "ints5")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    const auto stream_desc_2 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints6"),
             scalar_field(DataType::UINT8, "ints7"),
             scalar_field(DataType::UINT8, "ints8"),
             scalar_field(DataType::UINT8, "ints9"),
             scalar_field(DataType::UINT8, "ints10")}
    );

    SegmentInMemory in_mem_seg_2{stream_desc_2.clone()};

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V1);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V1);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_NE(hash_1, hash_2);
}

TEST(Segment, ColumnNamesProduceDifferentHashesV2) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints1"),
             scalar_field(DataType::UINT8, "ints2"),
             scalar_field(DataType::UINT8, "ints3"),
             scalar_field(DataType::UINT8, "ints4"),
             scalar_field(DataType::UINT8, "ints5")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    in_mem_seg_1.set_scalar(0, uint8_t(0));
    in_mem_seg_1.set_scalar(1, uint8_t(0));
    in_mem_seg_1.set_scalar(2, uint8_t(0));
    in_mem_seg_1.set_scalar(3, uint8_t(0));
    in_mem_seg_1.set_scalar(4, uint8_t(0));
    in_mem_seg_1.end_row();

    const auto stream_desc_2 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints6"),
             scalar_field(DataType::UINT8, "ints7"),
             scalar_field(DataType::UINT8, "ints8"),
             scalar_field(DataType::UINT8, "ints9"),
             scalar_field(DataType::UINT8, "ints10")}
    );

    SegmentInMemory in_mem_seg_2{stream_desc_2.clone()};

    in_mem_seg_2.set_scalar(0, uint8_t(0));
    in_mem_seg_2.set_scalar(1, uint8_t(0));
    in_mem_seg_2.set_scalar(2, uint8_t(0));
    in_mem_seg_2.set_scalar(3, uint8_t(0));
    in_mem_seg_2.set_scalar(4, uint8_t(0));
    in_mem_seg_2.end_row();

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V2);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V2);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_NE(hash_1, hash_2);
}

TEST(Segment, ColumnNamesProduceDifferentHashesEmptyV2) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints1"),
             scalar_field(DataType::UINT8, "ints2"),
             scalar_field(DataType::UINT8, "ints3"),
             scalar_field(DataType::UINT8, "ints4"),
             scalar_field(DataType::UINT8, "ints5")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    const auto stream_desc_2 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "ints6"),
             scalar_field(DataType::UINT8, "ints7"),
             scalar_field(DataType::UINT8, "ints8"),
             scalar_field(DataType::UINT8, "ints9"),
             scalar_field(DataType::UINT8, "ints10")}
    );

    SegmentInMemory in_mem_seg_2{stream_desc_2.clone()};

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V2);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V2);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_NE(hash_1, hash_2);
}

TEST(Segment, TestIdenticalProduceSameHashes) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "a"),
             scalar_field(DataType::UINT8, "b"),
             scalar_field(DataType::UINT8, "c"),
             scalar_field(DataType::UINT8, "d"),
             scalar_field(DataType::UINT8, "e")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    in_mem_seg_1.set_scalar(0, uint8_t(0));
    in_mem_seg_1.set_scalar(1, uint8_t(0));
    in_mem_seg_1.set_scalar(2, uint8_t(0));
    in_mem_seg_1.set_scalar(3, uint8_t(0));
    in_mem_seg_1.set_scalar(4, uint8_t(0));
    in_mem_seg_1.end_row();

    SegmentInMemory in_mem_seg_2{stream_desc_1.clone()};

    in_mem_seg_2.set_scalar(0, uint8_t(0));
    in_mem_seg_2.set_scalar(1, uint8_t(0));
    in_mem_seg_2.set_scalar(2, uint8_t(0));
    in_mem_seg_2.set_scalar(3, uint8_t(0));
    in_mem_seg_2.set_scalar(4, uint8_t(0));
    in_mem_seg_2.end_row();

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V1);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V1);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_EQ(hash_1, hash_2);
}

TEST(Segment, TestIdenticalProduceSameHashesV2) {
    const auto stream_desc_1 = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT8, "a"),
             scalar_field(DataType::UINT8, "b"),
             scalar_field(DataType::UINT8, "c"),
             scalar_field(DataType::UINT8, "d"),
             scalar_field(DataType::UINT8, "e")}
    );

    SegmentInMemory in_mem_seg_1{stream_desc_1.clone()};

    in_mem_seg_1.set_scalar(0, uint8_t(0));
    in_mem_seg_1.set_scalar(1, uint8_t(0));
    in_mem_seg_1.set_scalar(2, uint8_t(0));
    in_mem_seg_1.set_scalar(3, uint8_t(0));
    in_mem_seg_1.set_scalar(4, uint8_t(0));
    in_mem_seg_1.end_row();

    SegmentInMemory in_mem_seg_2{stream_desc_1.clone()};

    in_mem_seg_2.set_scalar(0, uint8_t(0));
    in_mem_seg_2.set_scalar(1, uint8_t(0));
    in_mem_seg_2.set_scalar(2, uint8_t(0));
    in_mem_seg_2.set_scalar(3, uint8_t(0));
    in_mem_seg_2.set_scalar(4, uint8_t(0));
    in_mem_seg_2.end_row();

    auto seg_1 = encode_dispatch(std::move(in_mem_seg_1), codec::default_lz4_codec(), EncodingVersion::V2);
    auto seg_2 = encode_dispatch(std::move(in_mem_seg_2), codec::default_lz4_codec(), EncodingVersion::V2);

    auto hash_1 = get_segment_hash(seg_1);
    auto hash_2 = get_segment_hash(seg_2);

    ASSERT_EQ(hash_1, hash_2);
}
