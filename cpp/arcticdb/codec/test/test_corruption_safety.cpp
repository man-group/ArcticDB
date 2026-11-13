/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */

#include <arcticdb/codec/codec.hpp>
#include <arcticdb/codec/segment.hpp>
#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field_collection.hpp>
#include <arcticdb/codec/input_bounds.hpp>
#include <arcticdb/codec/segment_identifier.hpp>
#include <arcticdb/util/sparse_utils.hpp>
#include <arcticdb/pipeline/string_pool_utils.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/column_store/memory_segment.hpp>
#include <arcticdb/column_store/string_pool.hpp>
#include <arcticdb/pipeline/column_mapping.hpp>
#include <arcticdb/python/python_handlers.hpp>
#include <arcticdb/arrow/arrow_handlers.hpp>
#include <arcticdb/python/python_handler_data.hpp>
#include <arcticdb/util/decode_path_data.hpp>
#include <arcticdb/util/offset_string.hpp>
#include <arcticdb/util/error_code.hpp>
#include <arcticdb/util/test/generators.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <optional>
#include <string_view>
#include <variant>
#include <vector>

// Regression tests for crashes on corrupted storage objects. Each test hand-crafts corrupt
// bytes (no S3) that previously crashed the process (SIGBUS/SIGSEGV), and asserts that the
// read path now raises an ArcticDB exception instead.

namespace arcticdb {

namespace {
// Asserts the call raises the decode-corruption error from these checks, not some unrelated exception.
template<typename F>
void expect_corrupt(F&& f, std::string_view message = "Corrupt segment") {
    try {
        std::forward<F>(f)();
        ADD_FAILURE() << "expected a corrupt-segment exception";
    } catch (const ArcticException& e) {
        EXPECT_NE(std::string_view(e.what()).find(message), std::string_view::npos) << e.what();
    }
}

std::vector<uint8_t> serialize_segment(Segment&& seg) {
    std::vector<uint8_t> vec;
    const auto bytes = seg.calculate_size();
    vec.resize(bytes);
    seg.write_to(vec.data());
    return vec;
}

Segment encode_segment(
        EncodingVersion version, SegmentInMemory&& seg, const arcticdb::proto::encoding::VariantCodec& codec
) {
    return version == EncodingVersion::V1 ? encode_v1(std::move(seg), codec) : encode_v2(std::move(seg), codec);
}

Segment make_numeric_segment(
        EncodingVersion version, const arcticdb::proto::encoding::VariantCodec& codec = codec::default_lz4_codec()
) {
    const auto stream_desc =
            stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT64, "vals")});
    SegmentInMemory in_mem_seg{stream_desc.clone()};
    for (uint64_t i = 0; i < 100; ++i) {
        in_mem_seg.set_scalar<uint64_t>(0, i);
        in_mem_seg.end_row();
    }
    return encode_segment(version, std::move(in_mem_seg), codec);
}

Segment make_numeric_segment_v2() { return make_numeric_segment(EncodingVersion::V2); }
} // namespace

// C1: garbage in FixedHeader.header_bytes (offset 4, uint32). The header is parsed using this
// length before check_size validates it, reading far past the buffer -> SIGBUS.
TEST(CorruptionSafety, C1_FixedHeaderHeaderBytesGarbage) {
    for (auto version : {EncodingVersion::V1, EncodingVersion::V2}) {
        auto vec = serialize_segment(make_numeric_segment(version));
        ASSERT_GT(vec.size(), 8u);
        // Overwrite the uint32 header_bytes field at offset 4 with a huge value.
        uint32_t garbage = 0xdeadbeef;
        std::memcpy(vec.data() + 4, &garbage, sizeof(garbage));
        expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
    }
}

// A header_bytes value that makes FIXED_HEADER_SIZE + header_bytes exactly one past the readable
// size must be rejected (the boundary case), not only absurd values.
TEST(CorruptionSafety, C1_FixedHeaderHeaderBytesOffByOneV2) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    ASSERT_GT(vec.size(), 8u);
    // FIXED_HEADER_SIZE + header_bytes == readable_size + 1
    uint32_t too_big = static_cast<uint32_t>(vec.size()) - FIXED_HEADER_SIZE + 1;
    std::memcpy(vec.data() + 4, &too_big, sizeof(too_big));
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

// An object shorter than the fixed header must be rejected before the fixed header is read.
TEST(CorruptionSafety, C1_ObjectShorterThanFixedHeader) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), FIXED_HEADER_SIZE - 1); });
}

// A V2 header_bytes too small to hold HeaderData must be rejected before HeaderData is copied.
TEST(CorruptionSafety, C1_V2HeaderBytesSmallerThanHeaderData) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    uint32_t tiny = 4;
    std::memcpy(vec.data() + 4, &tiny, sizeof(tiny));
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

// C2: a string column whose segment header has no string pool field (e.g. a corrupt V1
// protobuf header). The read path previously dereferenced a null string pool -> SIGSEGV.
TEST(CorruptionSafety, C2_NullStringPoolForStringColumn) {
    using namespace pipelines;
    const auto source_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const auto dest_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const entity::FieldWrapper field(dest_desc, "column");
    const ColumnMapping mapping(
            source_desc, dest_desc, field.field(), sizeof(PyObject*), 1, 0, 0, sizeof(PyObject*), 0
    );

    Column source(source_desc);
    source.set_scalar<entity::position_t>(0, 0); // references offset 0 of the (absent) pool
    source.set_row_data(0);
    Column dest(dest_desc);
    dest.allocate_and_advance_by(sizeof(PyObject*));

    ReadOptions options;
    options.set_output_config(PandasOutputConfig{PandasStringFormat::OBJECT});
    std::any handler_data{PythonHandlerData{}};

    std::shared_ptr<StringPool> null_pool; // simulates a header with no string pool field
    expect_corrupt([&] {
        PythonStringHandler{DataType::UTF_DYNAMIC64}.convert_type(
                source, dest, mapping, DecodePathData{}, handler_data, null_pool, options
        );
    });
}

// Negative control for C2: a valid all-None string column legitimately has no string pool (the
// encoder omits an empty pool). The guard must NOT reject it -- it must decode to None.
TEST(CorruptionSafety, C2_AllNoneStringColumnWithNoPoolIsValid) {
    using namespace pipelines;
    const auto source_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const auto dest_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const entity::FieldWrapper field(dest_desc, "column");
    const ColumnMapping mapping(
            source_desc, dest_desc, field.field(), sizeof(PyObject*), 3, 0, 0, 3 * sizeof(PyObject*), 0
    );

    Column source(source_desc);
    for (ssize_t i = 0; i < 3; ++i)
        source.set_scalar<entity::position_t>(i, not_a_string()); // all None, references no pooled string
    source.set_row_data(2);
    Column dest(dest_desc);
    dest.allocate_and_advance_by(3 * sizeof(PyObject*));

    ReadOptions options;
    options.set_output_config(PandasOutputConfig{PandasStringFormat::OBJECT});
    std::any handler_data{PythonHandlerData{}};

    std::shared_ptr<StringPool> null_pool; // valid: an all-None column stores no pool
    EXPECT_NO_THROW(PythonStringHandler{DataType::UTF_DYNAMIC64}.convert_type(
            source, dest, mapping, DecodePathData{}, handler_data, null_pool, options
    ));
}

// C3: a string pool containing invalid UTF-8 (e.g. 16 garbage bytes over the end of the pool).
// PyUnicode_FromStringAndSize returns null on invalid UTF-8; the null was then inc_ref'd -> SIGSEGV.
TEST(CorruptionSafety, C3_InvalidUtf8InStringPool) {
    using namespace pipelines;
    const auto source_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const auto dest_desc = make_scalar_type(DataType::UTF_DYNAMIC64);
    const entity::FieldWrapper field(dest_desc, "column");
    const ColumnMapping mapping(
            source_desc, dest_desc, field.field(), sizeof(PyObject*), 1, 0, 0, sizeof(PyObject*), 0
    );

    auto pool = std::make_shared<StringPool>();
    const char invalid[] = {char(0xff), char(0xfe), char(0xff), char(0xfe)};
    auto offset_string = pool->get(invalid, sizeof(invalid), false);

    Column source(source_desc);
    source.set_scalar<entity::position_t>(0, offset_string.offset());
    source.set_row_data(0);
    Column dest(dest_desc);
    dest.allocate_and_advance_by(sizeof(PyObject*));

    ReadOptions options;
    options.set_output_config(PandasOutputConfig{PandasStringFormat::OBJECT});
    std::any handler_data{PythonHandlerData{}};

    expect_corrupt([&] {
        PythonStringHandler{DataType::UTF_DYNAMIC64}.convert_type(
                source, dest, mapping, DecodePathData{}, handler_data, pool, options
        );
    });
}

// ---- S1: segment identifier size read from the segment ----

TEST(CorruptionSafety, S1_StringIdentifierLongerThanInput) {
    std::vector<uint8_t> buf(16, 0);
    SegmentIdentifierHeader hdr{IdentifierType::STRING, 1000};
    std::memcpy(buf.data(), &hdr, sizeof(hdr));
    const uint8_t* data = buf.data();
    expect_corrupt([&] { (void)read_identifier(data, buf.data() + buf.size()); });
    data = buf.data();
    expect_corrupt([&] { skip_identifier(data, buf.data() + buf.size()); });
}

TEST(CorruptionSafety, S1_IdentifierHeaderTruncated) {
    std::vector<uint8_t> buf(SegmentIdentifierSize - 1, 0);
    const uint8_t* data = buf.data();
    expect_corrupt([&] { (void)read_identifier(data, buf.data() + buf.size()); });
}

TEST(CorruptionSafety, S1_ValidStringIdentifierStillReads) {
    std::vector<uint8_t> buf(SegmentIdentifierSize + 3, 0);
    SegmentIdentifierHeader hdr{IdentifierType::STRING, 3};
    std::memcpy(buf.data(), &hdr, sizeof(hdr));
    std::memcpy(buf.data() + SegmentIdentifierSize, "abc", 3);
    const uint8_t* data = buf.data();
    EXPECT_EQ(std::get<StringId>(read_identifier(data, buf.data() + buf.size())), "abc");
    EXPECT_EQ(data, buf.data() + buf.size());
}

// ---- S2: fixed-size descriptors read without an end bound ----

TEST(CorruptionSafety, S2_SegmentDescriptorTruncated) {
    std::vector<uint8_t> buf(sizeof(SegmentDescriptorMagic) + 4, 0);
    new (buf.data()) SegmentDescriptorMagic{};
    const uint8_t* data = buf.data();
    expect_corrupt([&] { (void)read_segment_descriptor(data, buf.data() + buf.size()); });
}

TEST(CorruptionSafety, S2_MagicBeyondEnd) {
    std::vector<uint8_t> buf(sizeof(MetadataMagic) - 1, 0);
    const uint8_t* data = buf.data();
    expect_corrupt([&] { check_magic_bounded<MetadataMagic>(data, buf.data() + buf.size()); });
}

// ---- S3: a field's stored compressed sizes used as decompressor input length ----

namespace {
EncodedFieldImpl& passthrough_field(EncodedFieldCollection& coll, uint32_t value_bytes, uint32_t sparse_map_bytes = 0) {
    coll.reserve(1, calc_field_bytes(1));
    auto* field = coll.add_field(1)->mutable_ndarray();
    auto* block = field->add_values(EncodingVersion::V2);
    block->mutable_codec()->mutable_passthrough();
    block->set_in_bytes(value_bytes);
    block->set_out_bytes(value_bytes);
    field->set_sparse_map_bytes(sparse_map_bytes);
    return *field;
}

// Builds `prefix` + value bytes + a delimited serialized sparse map, and returns the sparse map's stored size.
std::vector<uint8_t> value_and_sparse_map_input(
        const util::BitMagic& bitmap, const std::vector<uint8_t>& prefix, size_t value_bytes, uint32_t& sparse_map_bytes
) {
    bm::serializer<util::BitMagic> serializer;
    bm::serializer<util::BitMagic>::buffer serialized;
    serializer.serialize(bitmap, serialized);
    sparse_map_bytes = static_cast<uint32_t>(serialized.size() + util::combined_bit_magic_delimiters_size());
    std::vector<uint8_t> input(prefix.size() + value_bytes + sparse_map_bytes, 0);
    std::ranges::copy(prefix, input.begin());
    uint8_t* pos = input.data() + prefix.size() + value_bytes;
    util::write_magic<util::BitMagicStart>(pos);
    std::memcpy(pos, serialized.data(), serialized.size());
    pos += serialized.size();
    util::write_magic<util::BitMagicEnd>(pos);
    return input;
}
} // namespace

TEST(CorruptionSafety, S3_FieldLongerThanInputRejected) {
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, 1 << 20);
    std::vector<uint8_t> input(64, 0);
    expect_corrupt([&] {
        check_encoded_field_input_available(
                field, input.data(), input.data() + input.size(), EncodingVersion::V1, "column"
        );
    });
}

TEST(CorruptionSafety, S3_FieldThatFitsAccepted) {
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, 64);
    std::vector<uint8_t> input(64, 0);
    EXPECT_NO_THROW(check_encoded_field_input_available(
            field, input.data(), input.data() + input.size(), EncodingVersion::V1, "column"
    ));
    // V2 also needs room for the column magic.
    expect_corrupt([&] {
        check_encoded_field_input_available(
                field, input.data(), input.data() + input.size(), EncodingVersion::V2, "column"
        );
    });
}

// ---- S4: sparse map (BitMagic) body ----

TEST(CorruptionSafety, S4_SparseMapSizeBelowDelimitersRejected) {
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, sizeof(uint64_t), 1); // 1 < start+end delimiters
    std::vector<uint8_t> input(64, 0);
    Column sink(make_scalar_type(DataType::UINT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::optional<util::BitMagic> bv;
    expect_corrupt([&] {
        (void)decode_ndarray(make_scalar_type(DataType::UINT64), field, input.data(), sink, bv, EncodingVersion::V2);
    });
}

TEST(CorruptionSafety, S4_SparseMapWithMoreBitsThanValuesRejected) {
    util::BitMagic bitmap;
    bitmap.set(0);
    bitmap.set(1);
    bitmap.set(2); // three set bits, but only one value is stored
    uint32_t sparse_map_bytes = 0;
    const auto input = value_and_sparse_map_input(bitmap, {}, sizeof(uint64_t), sparse_map_bytes);
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, sizeof(uint64_t), sparse_map_bytes);
    Column sink(make_scalar_type(DataType::UINT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::optional<util::BitMagic> bv;
    expect_corrupt([&] {
        (void)decode_ndarray(make_scalar_type(DataType::UINT64), field, input.data(), sink, bv, EncodingVersion::V2);
    });
}

namespace {
void expand(const util::BitMagic& bitmap, const std::vector<uint64_t>& dense, std::vector<uint64_t>& expanded) {
    util::expand_dense_buffer_and_promote_type<uint64_t>(
            bitmap,
            reinterpret_cast<const uint8_t*>(dense.data()),
            dense.size(),
            reinterpret_cast<uint8_t*>(expanded.data()),
            expanded.size()
    );
}
} // namespace

TEST(CorruptionSafety, S4_ExpandRejectsBitBeyondDestinationRows) {
    util::BitMagic bitmap;
    bitmap.set(1000); // destination only has 10 rows
    std::vector<uint64_t> dense{7};
    std::vector<uint64_t> expanded(10, 0);
    expect_corrupt([&] { expand(bitmap, dense, expanded); });
}

TEST(CorruptionSafety, S4_ExpandRejectsMoreBitsThanDenseValues) {
    util::BitMagic bitmap;
    bitmap.set(0);
    bitmap.set(1);
    std::vector<uint64_t> dense{7};
    std::vector<uint64_t> expanded(10, 0);
    expect_corrupt([&] { expand(bitmap, dense, expanded); });
}

TEST(CorruptionSafety, S4_ExpandValidSparseMapStillWorks) {
    util::BitMagic bitmap;
    bitmap.set(2);
    bitmap.set(5);
    std::vector<uint64_t> dense{7, 9};
    std::vector<uint64_t> expanded(6, 0);
    expand(bitmap, dense, expanded);
    EXPECT_EQ(expanded, (std::vector<uint64_t>{0, 0, 7, 0, 0, 9}));
}

// ---- Encoded field block counts read from the segment ----

TEST(CorruptionSafety, EncodedFieldBlocksPastBufferRejected) {
    EncodedFieldCollection source;
    (void)passthrough_field(source, 8); // one block declared
    // Keep only the field header, so the declared block lies past the buffer.
    ChunkedBuffer truncated;
    truncated.ensure(EncodedFieldImpl::Size);
    std::memcpy(truncated.data(), source.data_buffer(), EncodedFieldImpl::Size);
    Buffer offsets{sizeof(uint64_t)};
    const uint64_t zero = 0;
    std::memcpy(offsets.data(), &zero, sizeof(zero));
    EncodedFieldCollection corrupt{std::move(truncated), std::move(offsets)};
    expect_corrupt([&] { (void)corrupt.at(0); });
}

// ---- Stored size decoded straight into the destination rows ----

TEST(CorruptionSafety, StringColumnLargerThanDestinationRowsRejected) {
    using namespace pipelines;
    constexpr size_t dest_rows = 2;
    constexpr size_t stored_rows = 16;
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, stored_rows * sizeof(entity::position_t));
    std::vector<entity::position_t> stored(stored_rows, not_a_string());
    std::vector<uint8_t> input(sizeof(ColumnMagic) + stored_rows * sizeof(entity::position_t));
    uint8_t* pos = input.data();
    util::write_magic<ColumnMagic>(pos);
    std::memcpy(pos, stored.data(), stored_rows * sizeof(entity::position_t));

    const auto type = make_scalar_type(DataType::UTF_DYNAMIC64);
    const entity::FieldWrapper fw(type, "column");
    const ColumnMapping mapping(
            type, type, fw.field(), sizeof(PyObject*), dest_rows, 0, 0, dest_rows * sizeof(PyObject*), 0
    );
    Column dest(type);
    dest.allocate_and_advance_by(dest_rows * sizeof(PyObject*));
    ReadOptions options;
    std::any handler_data{PythonHandlerData{}};
    const uint8_t* data = input.data();
    expect_corrupt([&] {
        PythonStringHandler{DataType::UTF_DYNAMIC64}.handle_type(
                data,
                dest,
                field,
                mapping,
                DecodePathData{},
                handler_data,
                EncodingVersion::V2,
                std::make_shared<StringPool>(),
                options
        );
    });
}

// ---- Further header, sparse-map, string-pool and handler checks ----

namespace {
Segment make_mixed_segment(EncodingVersion version) {
    const auto stream_desc = stream_descriptor(
            StreamId{"thing"},
            RowCountIndex{},
            {scalar_field(DataType::UINT64, "ints"), scalar_field(DataType::UTF_DYNAMIC64, "strs")}
    );
    SegmentInMemory seg{stream_desc.clone()};
    for (uint64_t i = 0; i < 6; ++i) {
        seg.set_scalar<uint64_t>(0, i);
        seg.set_string(1, fmt::format("value_{}", i % 3));
        seg.end_row();
    }
    TimeseriesDescriptor tsd;
    tsd.set_total_rows(6);
    tsd.set_stream_descriptor(stream_desc);
    seg.set_timeseries_descriptor(tsd);
    return version == EncodingVersion::V1 ? encode_v1(std::move(seg), codec::default_lz4_codec())
                                          : encode_v2(std::move(seg), codec::default_lz4_codec());
}

void decode_bytes(const std::vector<uint8_t>& bytes, size_t readable) {
    auto seg = Segment::from_bytes(bytes.data(), readable);
    (void)decode_segment(seg);
}
} // namespace

TEST(CorruptionSafety, V2HeaderFieldsLargerThanHeaderRejected) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    const uint32_t huge = 0x7fffffff;
    std::memcpy(
            vec.data() + FIXED_HEADER_SIZE + offsetof(HeaderData, field_buffer_) + offsetof(FieldBuffer, fields_bytes_),
            &huge,
            sizeof(huge)
    );
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

TEST(CorruptionSafety, V2HeaderFieldOffsetOutOfRangeRejected) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    uint32_t header_bytes;
    std::memcpy(&header_bytes, vec.data() + 4, sizeof(header_bytes));
    // The five per-field offsets end the variable header; point the column field past the stored fields.
    const auto column_offset_pos = FIXED_HEADER_SIZE + header_bytes - 5 * sizeof(uint32_t) +
                                   static_cast<size_t>(FieldOffset::COLUMN) * sizeof(uint32_t);
    const uint32_t bad = 99;
    std::memcpy(vec.data() + column_offset_pos, &bad, sizeof(bad));
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

TEST(CorruptionSafety, S4_SparseMapLongerThanStoredSizeRejected) {
    util::BitMagic bitmap;
    bitmap.set(0);
    uint32_t sparse_map_bytes = 0;
    const auto input = value_and_sparse_map_input(bitmap, {}, sizeof(uint64_t), sparse_map_bytes);
    EncodedFieldCollection coll;
    // Declare one byte less than the serialized bitmap really occupies.
    auto& field = passthrough_field(coll, sizeof(uint64_t), sparse_map_bytes - 1);
    Column sink(make_scalar_type(DataType::UINT64), 0, AllocationType::DYNAMIC, Sparsity::PERMITTED);
    std::optional<util::BitMagic> bv;
    expect_corrupt([&] {
        (void)decode_ndarray(make_scalar_type(DataType::UINT64), field, input.data(), sink, bv, EncodingVersion::V2);
    });
}

TEST(CorruptionSafety, StringOffsetOutsidePoolRejected) {
    StringPool pool;
    auto offset = pool.get("abc").offset();
    EXPECT_EQ(pool.get_const_view(offset), "abc");
    expect_corrupt([&] { (void)pool.get_const_view(1LL << 40); });
    expect_corrupt([&] { (void)pool.get_const_view(-8); });
    expect_corrupt([&] { (void)pool.get_const_view(static_cast<entity::position_t>(pool.size())); });
}

TEST(CorruptionSafety, ArrowStringColumnLargerThanDestinationRowsRejected) {
    using namespace pipelines;
    constexpr size_t dest_rows = 2;
    constexpr size_t stored_rows = 16;
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, stored_rows * sizeof(entity::position_t));
    std::vector<uint8_t> input(sizeof(ColumnMagic) + stored_rows * sizeof(entity::position_t), 0);
    uint8_t* pos = input.data();
    util::write_magic<ColumnMagic>(pos);

    const auto type = make_scalar_type(DataType::UTF_DYNAMIC64);
    const entity::FieldWrapper fw(type, "column");
    const ColumnMapping mapping(
            type, type, fw.field(), sizeof(int64_t), dest_rows, 0, 0, dest_rows * sizeof(int64_t), 0
    );
    Column dest(type);
    dest.allocate_and_advance_by((dest_rows + 1) * sizeof(int64_t));
    ReadOptions options;
    std::any handler_data;
    const uint8_t* data = input.data();
    expect_corrupt([&] {
        ArrowStringHandler{}.handle_type(
                data,
                dest,
                field,
                mapping,
                DecodePathData{},
                handler_data,
                EncodingVersion::V2,
                std::make_shared<StringPool>(),
                options
        );
    });
}

// Every truncation of a valid segment must raise rather than read past the buffer.
TEST(CorruptionSafety, TruncationSweepRaisesForEveryPrefix) {
    for (auto version : {EncodingVersion::V1, EncodingVersion::V2}) {
        const auto vec = serialize_segment(make_mixed_segment(version));
        EXPECT_NO_THROW(decode_bytes(vec, vec.size()));
        for (size_t len = 0; len < vec.size(); ++len) {
            // Copy to an exact-size buffer so any overread leaves the allocation.
            std::vector<uint8_t> prefix(vec.begin(), vec.begin() + static_cast<std::ptrdiff_t>(len));
            EXPECT_THROW(decode_bytes(prefix, prefix.size()), ArcticException)
                    << "version " << static_cast<int>(version) << " prefix " << len;
        }
    }
}

// Flipping any single byte of a valid segment must either decode or raise, never crash.
TEST(CorruptionSafety, ByteFlipSweepNeverCrashes) {
    for (auto version : {EncodingVersion::V1, EncodingVersion::V2}) {
        const auto vec = serialize_segment(make_mixed_segment(version));
        size_t raised = 0;
        for (size_t i = 0; i < vec.size(); ++i) {
            auto flipped = vec;
            flipped[i] ^= 0xFF;
            try {
                decode_bytes(flipped, flipped.size());
            } catch (const std::exception&) {
                ++raised;
            }
        }
        // Succeeding is fine for flips that land in the payload; most structural flips must raise.
        EXPECT_GT(raised, 0u);
    }
}

// ---- Sparse-map rows used as destination indices by the type handlers ----

namespace {

template<typename Handler>
void expect_sparse_row_past_destination_rejected(Handler handler, DataType type, size_t dest_type_size) {
    using namespace pipelines;
    constexpr size_t dest_rows = 10;
    const auto value_bytes = get_type_size(type);
    util::BitMagic bitmap;
    bitmap.set(1'000'000);
    std::vector<uint8_t> magic(sizeof(ColumnMagic));
    uint8_t* magic_pos = magic.data();
    util::write_magic<ColumnMagic>(magic_pos);
    uint32_t sparse_map_bytes = 0;
    auto input = value_and_sparse_map_input(bitmap, magic, value_bytes, sparse_map_bytes);
    if (type == DataType::UTF_DYNAMIC64) {
        const auto none = not_a_string();
        std::memcpy(input.data() + magic.size(), &none, sizeof(none));
    }
    EncodedFieldCollection coll;
    auto& field = passthrough_field(coll, static_cast<uint32_t>(value_bytes), sparse_map_bytes);
    const auto td = make_scalar_type(type);
    const entity::FieldWrapper fw(td, "column");
    const ColumnMapping mapping(td, td, fw.field(), dest_type_size, dest_rows, 0, 0, dest_rows * dest_type_size, 0);
    Column dest(td);
    dest.allocate_and_advance_by((dest_rows + 1) * dest_type_size);
    ReadOptions options;
    std::any handler_data{PythonHandlerData{}};
    const uint8_t* data = input.data();
    expect_corrupt([&] {
        handler.handle_type(
                data,
                dest,
                field,
                mapping,
                DecodePathData{},
                handler_data,
                EncodingVersion::V2,
                std::make_shared<StringPool>(),
                options
        );
    });
}
} // namespace

TEST(CorruptionSafety, SparseRowPastDestinationRejectedPythonString) {
    expect_sparse_row_past_destination_rejected(
            PythonStringHandler{DataType::UTF_DYNAMIC64}, DataType::UTF_DYNAMIC64, sizeof(PyObject*)
    );
}

TEST(CorruptionSafety, SparseRowPastDestinationRejectedPythonBool) {
    expect_sparse_row_past_destination_rejected(PythonBoolHandler{}, DataType::BOOL8, sizeof(PyObject*));
}

TEST(CorruptionSafety, SparseRowPastDestinationRejectedArrowTimestamp) {
    expect_sparse_row_past_destination_rejected(ArrowTimestampHandler{}, DataType::NANOSECONDS_UTC64, sizeof(int64_t));
}

TEST(CorruptionSafety, SparseRowPastDestinationRejectedArrowString) {
    expect_sparse_row_past_destination_rejected(ArrowStringHandler{}, DataType::UTF_DYNAMIC64, sizeof(int64_t));
}

TEST(CorruptionSafety, SparseRowPastDestinationRejectedArrowBool) {
    expect_sparse_row_past_destination_rejected(ArrowBoolHandler{}, DataType::BOOL8, sizeof(uint8_t));
}

// ---- Descriptor field collections: stored field sizes and name lengths ----

namespace {
// Fields "first" and "second" as decode_ndarray leaves a stored descriptor before regenerate_offsets, optionally with
// one field's stored size, or one field's stored name length, overwritten.
struct FieldOverride {
    size_t field_index;
    int64_t value;
};

FieldCollection stored_field_collection(
        std::optional<FieldOverride> stored_size = std::nullopt, std::optional<FieldOverride> name_size = std::nullopt,
        bool with_stored_sizes = true
) {
    FieldCollection fields;
    size_t field_index = 0;
    for (const std::string_view name : {"first", "second"}) {
        const auto size = Field::calc_size(name);
        auto* data = fields.allocate_data(size);
        Field::emplace(make_scalar_type(DataType::INT64), name, data);
        if (name_size && name_size->field_index == field_index) {
            const auto stored_name_size = static_cast<uint32_t>(name_size->value);
            std::memcpy(data, &stored_name_size, sizeof(stored_name_size));
        }
        fields.advance_data(size);
        if (with_stored_sizes) {
            const bool corrupt = stored_size && stored_size->field_index == field_index;
            *fields.allocate_shapes(sizeof(shape_t)) = corrupt ? stored_size->value : static_cast<shape_t>(size);
            fields.advance_shapes(sizeof(shape_t));
        }
        ++field_index;
    }
    return fields;
}
} // namespace

TEST(CorruptionSafety, DescriptorFieldStartPastBufferRejected) {
    auto fields = stored_field_collection(FieldOverride{0, 1'000'000});
    expect_corrupt([&] { fields.regenerate_offsets(); });
}

TEST(CorruptionSafety, DescriptorFieldNegativeSizeRejected) {
    auto fields = stored_field_collection(FieldOverride{0, -1'000});
    expect_corrupt([&] { fields.regenerate_offsets(); });
}

TEST(CorruptionSafety, DescriptorFieldNamePastBufferRejected) {
    auto fields = stored_field_collection(std::nullopt, FieldOverride{1, 1'000'000});
    expect_corrupt([&] { fields.regenerate_offsets(); });
}

// The field iterator steps by each field's own name length, so a name length that disagrees with the stored sizes
// would walk it off the buffer.
TEST(CorruptionSafety, DescriptorFieldNameSizeBreakingIterationRejected) {
    auto fields = stored_field_collection(std::nullopt, FieldOverride{0, 0});
    expect_corrupt([&] { fields.regenerate_offsets(); });
}

TEST(CorruptionSafety, DescriptorWithoutStoredSizesStillChecksIteration) {
    auto fields = stored_field_collection(std::nullopt, FieldOverride{0, 0}, false);
    expect_corrupt([&] { fields.regenerate_offsets(); });
}

// The last stored size only marks the end of the buffer; nothing reads with it, so it does not reject a readable
// descriptor.
TEST(CorruptionSafety, DescriptorLastFieldSizeIsNotUsedToRead) {
    auto fields = stored_field_collection(FieldOverride{1, 1'000'000});
    fields.regenerate_offsets();
    ASSERT_EQ(fields.size(), 2u);
    EXPECT_EQ(fields[0].name(), "first");
    EXPECT_EQ(fields[1].name(), "second");
}

TEST(CorruptionSafety, ValidDescriptorFieldCollectionStillReads) {
    auto fields = stored_field_collection();
    fields.regenerate_offsets();
    ASSERT_EQ(fields.size(), 2u);
    EXPECT_EQ(fields[0].name(), "first");
    EXPECT_EQ(fields[1].name(), "second");
}

TEST(CorruptionSafety, V2HeaderNegativeFieldOffsetRejected) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    uint32_t header_bytes;
    std::memcpy(&header_bytes, vec.data() + 4, sizeof(header_bytes));
    const auto column_offset_pos = FIXED_HEADER_SIZE + header_bytes - 5 * sizeof(uint32_t) +
                                   static_cast<size_t>(FieldOffset::COLUMN) * sizeof(uint32_t);
    const uint32_t minus_one = 0xFFFFFFFF;
    std::memcpy(vec.data() + column_offset_pos, &minus_one, sizeof(minus_one));
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

TEST(CorruptionSafety, V2FooterOffsetOverflowRejected) {
    auto vec = serialize_segment(make_numeric_segment_v2());
    const uint64_t footer_offset = std::numeric_limits<uint64_t>::max() - 7;
    std::memcpy(
            vec.data() + FIXED_HEADER_SIZE + offsetof(HeaderData, footer_offset_), &footer_offset, sizeof(footer_offset)
    );
    expect_corrupt([&] { (void)Segment::from_bytes(vec.data(), vec.size()); });
}

TEST(CorruptionSafety, FixedWidthStringWithoutPoolRejected) {
    ChunkedBuffer offsets;
    offsets.ensure(2 * sizeof(entity::position_t));
    const entity::position_t values[] = {not_a_string(), 0};
    std::memcpy(offsets.data(), values, sizeof(values));
    EXPECT_TRUE(std::holds_alternative<entity::position_t>(get_string_from_buffer(0, offsets, nullptr)));
    expect_corrupt([&] { (void)get_string_from_buffer(1, offsets, nullptr); });
}

namespace {
// Overwrites the start of the one occurrence of `from` with `to`; the rest of `from` only makes the match unique.
void replace_once(std::vector<uint8_t>& bytes, const std::vector<uint8_t>& from, const std::vector<uint8_t>& to) {
    const auto found = std::ranges::search(bytes, from);
    ASSERT_FALSE(found.empty()) << "pattern not found";
    ASSERT_TRUE(std::ranges::search(found.end(), bytes.end(), from.begin(), from.end()).empty())
            << "pattern not unique";
    std::ranges::copy(to, found.begin());
}

// A corrupt descriptor can store a field type whose size bits match no SizeBits value. Clears the size bits of the
// stored UINT64 field `name`, a one-byte change; the field's name makes the match unique.
void clear_uint64_size_bits(std::vector<uint8_t>& bytes, EncodingVersion version, std::string_view name) {
    const auto name_size = static_cast<uint8_t>(name.size());
    std::vector<uint8_t> stored;
    std::vector<uint8_t> corrupted;
    if (version == EncodingVersion::V1) {
        // Protobuf FieldDescriptor {type_desc {value_type: UINT (1), size_bits: S64 (4)}, name}
        stored = {0x08, 0x01, 0x10, 0x04, 0x12, name_size};
        corrupted = {0x08, 0x01, 0x10, 0x00};
    } else {
        // Field {uint32 name size, data type UINT64 (0x0c), Dim0, name}; UINT with no size bits is 0x08
        stored = {name_size, 0x00, 0x00, 0x00, 0x0c, 0x00};
        corrupted = {name_size, 0x00, 0x00, 0x00, 0x08};
    }
    stored.insert(stored.end(), name.begin(), name.end());
    replace_once(bytes, stored, corrupted);
}

constexpr std::string_view invalid_size_bits = "with invalid size bits";
} // namespace

TEST(CorruptionSafety, FieldTypeWithoutSizeBitsRejected) {
    for (auto version : {EncodingVersion::V1, EncodingVersion::V2}) {
        SCOPED_TRACE(static_cast<int>(version));
        // The passthrough codec leaves V2's descriptor fields unencoded
        auto bytes = serialize_segment(make_numeric_segment(version, codec::default_passthrough_codec()));
        ASSERT_NO_FATAL_FAILURE(clear_uint64_size_bits(bytes, version, "vals"));
        expect_corrupt([&] { (void)Segment::from_bytes(bytes.data(), bytes.size()); }, invalid_size_bits);
    }
}

// An index key's timeseries descriptor holds the frame's fields, which reads use to allocate the output columns. The
// frame field is named differently from the segment's own field so that only the timeseries descriptor is corrupted.
TEST(CorruptionSafety, FrameFieldTypeWithoutSizeBitsRejected) {
    for (auto version : {EncodingVersion::V1, EncodingVersion::V2}) {
        SCOPED_TRACE(static_cast<int>(version));
        SegmentInMemory seg{
                stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT64, "vals")})
        };
        seg.set_scalar<uint64_t>(0, 1);
        seg.end_row();
        TimeseriesDescriptor tsd;
        tsd.set_total_rows(1);
        tsd.set_stream_descriptor(
                stream_descriptor(StreamId{"thing"}, RowCountIndex{}, {scalar_field(DataType::UINT64, "frame_vals")})
        );
        seg.set_timeseries_descriptor(tsd);
        auto bytes = serialize_segment(encode_segment(version, std::move(seg), codec::default_passthrough_codec()));
        ASSERT_NO_FATAL_FAILURE(clear_uint64_size_bits(bytes, version, "frame_vals"));
        auto segment = Segment::from_bytes(bytes.data(), bytes.size());
        expect_corrupt([&] { (void)decode_timeseries_descriptor(segment); }, invalid_size_bits);
        expect_corrupt([&] { (void)decode_segment(segment); }, invalid_size_bits);
    }
}

} // namespace arcticdb
