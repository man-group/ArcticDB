/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
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
            const arcticdb::proto::encoding::VariantCodec& codec_opts,
            ColumnData& column_data);

        static void encode(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
    };

    struct ColumnEncoderV2 {
    public:
        static void encode(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
        static std::pair<size_t, size_t> max_compressed_size(
            const arcticdb::proto::encoding::VariantCodec& codec_opts,
            ColumnData& column_data);
    private:
        static void encode_shapes(
            const ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos_in_buffer);

        static void encode_blocks(
            const arcticdb::proto::encoding::VariantCodec &codec_opts,
            ColumnData& column_data,
            std::variant<EncodedField*, arcticdb::proto::encoding::EncodedField*> variant_field,
            Buffer& out,
            std::ptrdiff_t& pos);
    };

    size_t calc_column_blocks_size(const Column& col);
}

using namespace arcticdb;

using EncoginVersions = ::testing::Types<
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
class FieldEncoderTestDim0 : public FieldEncoderTestDim0Base{};

using EncodedFieldsType = ::testing::Types<arcticdb::proto::encoding::EncodedField, EncodedField>;
TYPED_TEST_SUITE(FieldEncoderTestDim0, EncodedFieldsType);

TYPED_TEST(FieldEncoderTestDim0, Passthrough_v1) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, typename TestFixture::ValuesTypeDescriptorTag, EncodingVersion::V1>;
    const TypedBlockData<typename TestFixture::ValuesTypeDescriptorTag> values_block(
        TestFixture::values.data(),
        nullptr,
        TestFixture::values_byte_size,
        TestFixture::values.size(),
        nullptr);
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
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, typename TestFixture::ValuesTypeDescriptorTag, EncodingVersion::V2>;
    const TypedBlockData<typename TestFixture::ValuesTypeDescriptorTag> values_block(
        TestFixture::values.data(),
        nullptr,
        TestFixture::values_byte_size,
        TestFixture::values.size(),
        nullptr);
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
class FieldEncoderTestFromColumnDim0 : public FieldEncoderTestDim0Base{};

/// @brief Cartesian product between the type of the encoded field and the encoding version.
/// (EncodedField, arcticdb::proto::encoding::EncodedField) x (EncodingVersion::V1, EncodingVersion::V2)
using FieldVersionT = ::testing::Types<
    std::pair<arcticdb::proto::encoding::EncodedField, ColumnEncoderV1>,
	std::pair<arcticdb::proto::encoding::EncodedField, ColumnEncoderV2>,
	std::pair<EncodedField, ColumnEncoderV1>,
	std::pair<EncodedField, ColumnEncoderV2>>;
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
    const auto [_, max_compressed_size] = ColumnEncoder::max_compressed_size(TestFixture::passthorugh_encoding_options,
        column_data);
    Buffer out(max_compressed_size);
    column_data.reset();
    ColumnEncoder::encode(TestFixture::passthorugh_encoding_options, column_data, &field, out, pos);
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

TEST_F(FieldEncoderTestDim1, PassthroughV1ProtoField) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ValuesTypeDescriptorTag, EncodingVersion::V1>;
    const TypedBlockData<ValuesTypeDescriptorTag> block(
        values.data(),
        shapes.data(),
        values_byte_size,
        shapes.size(),
        nullptr);
    arcticdb::proto::encoding::EncodedField field;
    Buffer out(Encoder::max_compressed_size(passthorugh_encoding_options, block));
    std::ptrdiff_t pos = 0;
    Encoder::encode(passthorugh_encoding_options, block, field, out, pos);

    const auto& nd = field.ndarray();
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

TEST_F(FieldEncoderTestDim1, PassthroughV1NativeField) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ValuesTypeDescriptorTag, EncodingVersion::V1>;
    const TypedBlockData<ValuesTypeDescriptorTag> block(
        values.data(),
        shapes.data(),
        values_byte_size,
        shapes.size(),
        nullptr);
    // one block for shapes and one for values
    constexpr size_t encoded_field_size = EncodedField::Size + 2 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_memory;
    EncodedField* field = new(encoded_field_memory.data()) EncodedField;

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

TEST_F(FieldEncoderTestDim1, PassthroughV2ProtoField) {
    using Encoder = TypedBlockEncoderImpl<TypedBlockData, ValuesTypeDescriptorTag, EncodingVersion::V2>;
    using ShapesEncoder = TypedBlockEncoderImpl<TypedBlockData, arcticdb::ShapesBlockTDT, EncodingVersion::V2>;
    const TypedBlockData<ValuesTypeDescriptorTag> values_block(
        values.data(),
        shapes.data(),
        values_byte_size,
        shapes.size(),
        nullptr);
    const TypedBlockData<arcticdb::ShapesBlockTDT> shapes_block(
        shapes.data(),
        nullptr,
        shapes_byte_size,
        0,
        nullptr);
    const size_t values_max_compressed_size = Encoder::max_compressed_size(passthorugh_encoding_options,
        values_block);
    const size_t shapes_max_compressed_size = ShapesEncoder::max_compressed_size(passthorugh_encoding_options,
        shapes_block);
    const size_t total_max_compressed_size = values_max_compressed_size + shapes_max_compressed_size;
    arcticdb::proto::encoding::EncodedField field;
    Buffer out(total_max_compressed_size);
    std::ptrdiff_t pos = 0;
    ShapesEncoder::encode_shapes(passthorugh_encoding_options, shapes_block, field, out, pos);
    Encoder::encode_values(passthorugh_encoding_options, values_block, field, out, pos);

    const auto& nd = field.ndarray();
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
        values.data(),
        shapes.data(),
        values_byte_size,
        shapes.size(),
        nullptr);
    const TypedBlockData<arcticdb::ShapesBlockTDT> shapes_block(
        shapes.data(),
        nullptr,
        shapes_byte_size,
        0,
        nullptr);
    const size_t values_max_compressed_size = Encoder::max_compressed_size(passthorugh_encoding_options,
        values_block);
    const size_t shapes_max_compressed_size = ShapesEncoder::max_compressed_size(passthorugh_encoding_options,
        shapes_block);
    const size_t total_max_compressed_size = values_max_compressed_size + shapes_max_compressed_size;
    // one block for shapes and one for values
    constexpr size_t encoded_field_size = EncodedField::Size + 2 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_memory;
    EncodedField* field = new(encoded_field_memory.data()) EncodedField;
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
        data_buffer.blocks()[0]->copy_from(reinterpret_cast<const uint8_t*>(first_block_data.data()),
            first_block_data_byte_size,
            0);
        data_buffer.blocks()[1]->copy_from(reinterpret_cast<const uint8_t*>(second_block_data.data()),
            second_block_data_byte_size,
            0);
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

TEST_F(TestMultiblockData_Dim1, EncodingVersion_1) {
    arcticdb::proto::encoding::EncodedField encoded_field;
    ColumnData column_data(&data_buffer, &shapes_buffer, type_descriptor, nullptr);
    const auto [_, max_compressed_size] = ColumnEncoderV1::max_compressed_size(passthorugh_encoding_options, column_data);
    Buffer out(max_compressed_size);
    ptrdiff_t out_pos = 0;
    column_data.reset();
    ColumnEncoderV1::encode(passthorugh_encoding_options, column_data, &encoded_field, out, out_pos);
    const auto ndarray = encoded_field.ndarray();
    ASSERT_EQ(ndarray.shapes_size(), 2);
    ASSERT_EQ(ndarray.values_size(), 2);
    ASSERT_EQ(ndarray.items_count(), shapes_data.size());
}

TEST_F(TestMultiblockData_Dim1, EncodingVersion_2) {
    constexpr size_t encoded_field_size = EncodedField::Size + 3 * sizeof(EncodedBlock);
    std::array<uint8_t, encoded_field_size> encoded_field_owner;
    EncodedField* encoded_field = new(encoded_field_owner.data()) EncodedField;
    ColumnData column_data(&data_buffer, &shapes_buffer, type_descriptor, nullptr);
    const auto [_, max_compressed_size] = ColumnEncoderV2::max_compressed_size(passthorugh_encoding_options, column_data);
    Buffer out(max_compressed_size);
    ptrdiff_t out_pos = 0;
    column_data.reset();
    ColumnEncoderV2::encode(passthorugh_encoding_options, column_data, encoded_field, out, out_pos);
    const auto ndarray = encoded_field->ndarray();
    ASSERT_EQ(ndarray.shapes_size(), 1);
    ASSERT_EQ(ndarray.values_size(), 2);
    ASSERT_EQ(ndarray.items_count(), shapes_data.size());
}

template<typename EncodedFieldType>
class SegmentStringEncodingTest : public testing::Test{};

TYPED_TEST_SUITE(SegmentStringEncodingTest, EncoginVersions);

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

    SegmentInMemory res = decode_segment(std::move(seg));
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

    SegmentInMemory res = decode_segment(std::move(seg));
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
        index.create_stream_descriptor(NumericId{123}, {
            scalar_field(DataType::ASCII_DYNAMIC64, "col_1"),
            scalar_field(DataType::ASCII_DYNAMIC64, "col_2"),
            scalar_field(DataType::ASCII_DYNAMIC64, "col_3"),
            scalar_field(DataType::ASCII_DYNAMIC64, "col_4"),
            scalar_field(DataType::ASCII_DYNAMIC64, "col_5"),
            scalar_field(DataType::ASCII_DYNAMIC64, "col_6"),
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

struct TransactionalThing {
    arcticdb::util::MagicNum<'K', 'e', 'e', 'p'> magic_;
    static bool destroyed;
    ~TransactionalThing() {
        TransactionalThing::destroyed = true;
    }
};

bool TransactionalThing::destroyed = false;

TEST(Segment, KeepAlive) {
    {
        Segment segment;
        segment.set_keepalive(std::any(TransactionalThing{}));

        auto seg1 = std::move(segment);
        Segment seg2{std::move(seg1)};
        auto seg3 = seg2;
        Segment seg4{seg3};

        std::any_cast<TransactionalThing>(seg4.keepalive()).magic_.check();
    }
    ASSERT_EQ(TransactionalThing::destroyed, true);
}
