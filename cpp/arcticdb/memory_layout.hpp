#pragma once

#include <cstdint>
#include <arcticdb/codec/magic_words.hpp>

namespace arcticdb {

#pragma pack(push)
#pragma pack(1)

constexpr size_t encoding_size = 6;
enum class Codec : uint16_t {
    UNKNOWN = 0,
    ZSTD,
    PFOR,
    LZ4,
    PASS,
    RLE,
    FSST,
    GORILLA_RLE,
    CONSTANT
};

struct ZstdCodec {
    static constexpr Codec type_ = Codec::ZSTD;

    int32_t level_ = 0;
    bool is_streaming_ = false;
    uint8_t padding_ = 0;
};

static_assert(sizeof(ZstdCodec) == encoding_size);

struct Lz4Codec {
    static constexpr Codec type_ = Codec::LZ4;

    int32_t acceleration_ = 1;
    int16_t padding_ = 0;
};

static_assert(sizeof(Lz4Codec) == encoding_size);

struct PassthroughCodec {
    static constexpr Codec type_ = Codec::PASS;

    uint32_t unused_ = 0;
    uint16_t padding_ = 0;
};

struct PforCodec {
    static constexpr Codec type_ = Codec::PFOR;

    uint32_t unused_ = 0;
    uint16_t padding_ = 0;

};

struct BlockCodec {
    Codec codec_ = Codec::UNKNOWN;
    constexpr static size_t DataSize = 24;
    std::array<uint8_t, DataSize> data_ = {};
};

struct Block {
    uint32_t in_bytes_ = 0;
    uint32_t out_bytes_ = 0;
    uint64_t hash_ = 0;
    uint16_t encoder_version_ = 0;
    bool is_shape_ = false;
    uint8_t num_codecs_ = 0;
    std::array<BlockCodec, 1> codecs_;

    Block() = default;
};

enum class EncodedFieldType : uint8_t {
    UNKNOWN,
    NDARRAY,
    DICTIONARY
};

enum class BitmapFormat : uint8_t {
    UNKNOWN,
    DENSE,
    BITMAGIC,
    ROARING
};

struct EncodedField {
    EncodedFieldType type_ = EncodedFieldType::UNKNOWN;
    uint8_t shapes_count_ = 0u;
    uint16_t values_count_ = 0u;
    uint32_t sparse_map_bytes_ = 0u;
    uint32_t items_count_ = 0u;
    BitmapFormat format_ = BitmapFormat::UNKNOWN;
    std::array<Block, 1> blocks_;
};


enum class EncodingVersion : uint16_t {
    V1 = 0,
    V2 = 1,
    COUNT = 2
};

constexpr static uint16_t MAGIC_NUMBER = 0xFA57;

struct FixedHeader {
    std::uint16_t magic_number;
    std::uint16_t encoding_version;
    std::uint32_t header_bytes;
};

constexpr static std::size_t FIXED_HEADER_SIZE = sizeof(FixedHeader);

enum class HeaderFlag : uint8_t {
    COMPACTED,
};

struct FieldBuffer {
    mutable uint32_t fields_bytes_ = 0U;
    mutable uint32_t offset_bytes_ = 0U;
};

struct HeaderData { ;
    EncodingVersion encoding_version_ = EncodingVersion::V1;
    uint16_t fields_ = 0U;
    uint8_t flags_ = 0U;
    uint64_t footer_offset_ = 0U;
    FieldBuffer field_buffer_;
};

enum class SortedValue : uint8_t {
    UNKNOWN = 0,
    UNSORTED = 1,
    ASCENDING = 2,
    DESCENDING = 3,
};

struct IndexData {
    enum class Type : int32_t {
        UNKNOWN = 0,
        ROWCOUNT = 82,
        STRING = 83,
        TIMESTAMP = 84
    };

    Type type_ = Type::UNKNOWN;
    uint32_t field_count_ = 0U;
};

enum class SchemaType : uint8_t {
    STATIC,
    DYNAMIC
};

struct IndexDescriptor {
    enum class Type : int32_t {
        UNKNOWN = 0,
        ROWCOUNT = 82,
        STRING = 83,
        TIMESTAMP = 84
    };

    IndexDescriptor() = default;

    IndexDescriptor(Type type, uint32_t field_count) :
        type_(type),
        field_count_(field_count) {
    }

    Type type_ = Type::UNKNOWN;
    uint32_t field_count_ = 0U;
};

struct FrameDescriptor {
    uint64_t total_rows_ = 0UL;
    bool column_groups_ = false;
};

struct SegmentDescriptor {
    SortedValue sorted_ = SortedValue::UNKNOWN;
    uint64_t compressed_bytes_ = 0UL;
    uint64_t uncompressed_bytes_ = 0UL;
    IndexDescriptor index_;
};

// A segment header contains a set of optional fields that describe the contents of a given segment
enum class FieldOffset : uint8_t {
    METADATA, // Opaque field for user and normalization metadata
    STRING_POOL, // Deduplicated compressed field of string data
    DESCRIPTOR, // Collection of field names and types for the current segment
    INDEX, // Optional additional set of fields used when this segment indexes a dataframe
    COLUMN, // Set of encoded fields that represent the body (user) data of the segment
    COUNT
};

/*
 * Note. The structures below contain variable-length fields (represented by named structures) and should not be
 * used to implement the data format directly. They are intended as a syntactically-correct representation
 * of the storage format only.
 */
struct FieldList {
    // A list of field descriptors containing the name, type and dimensionality of a column of data
};

struct EncodedFieldList {
    // A list of encoded fields that describes the contents of other fields. An encoded field is a list of blocks
    // with a specific set of compression types
};

struct OpaqueField {
    // An opaque field to be filled with user-determined content, used for things like
    // language-specific normalization data
};

struct ColumnField {
    ColumnMagic column_magic_;
    // A data field described by an EncodedField, consists of a set of compressed blocks that may represent
    // shapes and values, and an optional sparse bitmap
};

template <typename FieldType>
struct CompressedField {
    // A compressed block of data containing some other structure. A compressed field is represented by an EncodedField
    // which contains a set of Block objects describing the compression stype
};

template <typename FieldType>
struct RepeatedField {
    // A set of fields that are repeated, whose number corresponds to a unary field describing this set. For example, the
    // number of repeated column fields should correspond to the number of entries in the descriptor (which describes the
    // user-facing information about a column's contents, and the number of EncodedFields in the body fields, which describe
    // the block structure and compression
};

// Binrary representation of a segment header. Contains positioning information about the structure of the segment,
// and the list of fields representing the segment metadata fields
struct SegmentHeaderData {
    HeaderData data_;
    EncodedFieldList header_fields_; // Header fields containing the fields described by FieldOffsets
    std::array<uint32_t, 5> offset_ = {}; // Maps the entries in the FieldOffset enumeration to the header field entries
};

// The overall memory layout of an ArcticDB segment
struct MemoryLayout {
    FixedHeader fixed_header_;
    SegmentHeaderData variable_header_;

    MetadataMagic metadata_magic_;
    OpaqueField metadata_;

    DescriptorMagic descriptor_magic_;
    CompressedField<FieldList> descriptor_fields_;

    IndexMagic index_magic_;
    // Optional fields present if this segment refers to a complete dataframe, i.e. if it is a primary index
    FrameDescriptor index_frame_descriptor_;
    SegmentDescriptor index_segment_descriptor_;
    OpaqueField index_metadata_;
    CompressedField<FieldList> index_descriptor_fields_;

    RepeatedField<ColumnField> columns_;

    StringPoolMagic string_pool_magic_;
    ColumnField string_pool_field_;

    EncodedFieldList body_fields_;  // Encoded field list representing the user data fields (columns)
};

#pragma pack(pop)



} //namespace arcticdb