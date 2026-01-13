/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <cstdint>
#include <array>
#include "arcticdb/codec/magic_words.hpp"

namespace arcticdb {

#pragma pack(push)
#pragma pack(1)

enum class SortedValue : uint8_t {
    UNKNOWN = 0,
    UNSORTED = 1,
    ASCENDING = 2,
    DESCENDING = 3,
};

constexpr size_t encoding_size = 6;
// And extendable list of codecs supported by ArcticDB
// N.B. this list is likely to change
enum class Codec : uint16_t {
    UNKNOWN = 0,
    ZSTD,
    PFOR,
    LZ4,
    PASS,
};

// Codecs form a discriminated union of same-sized objects
// within the BlockCodec structure
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
    uint16_t padding_ = 0;
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

static_assert(sizeof(Block) == 46);

// Possible types of encoded fields, which are
// sets of blocks representing a column of data
enum class EncodedFieldType : uint8_t { UNKNOWN, NDARRAY, DICTIONARY };

enum class BitmapFormat : uint8_t { UNKNOWN, DENSE, BITMAGIC };

enum class UniqueCountType : uint8_t {
    PRECISE,
    HYPERLOGLOG,
};

struct FieldStats {

    FieldStats() = default;

    uint64_t min_ = 0UL;
    uint64_t max_ = 0UL;
    uint32_t unique_count_ = 0UL;
    UniqueCountType unique_count_precision_ = UniqueCountType::PRECISE;
    SortedValue sorted_ = SortedValue::UNKNOWN;
    uint8_t set_ = 0U;
    bool unused_ = false;
};

static_assert(sizeof(FieldStats) == 24);

// Each encoded field will have zero or one shapes blocks,
// a potentially large number of values (data) blocks, and
// an optional sparse bitmap. The block array serves as a
// pointer to the first block
struct EncodedField {
    EncodedFieldType type_ = EncodedFieldType::UNKNOWN;
    uint32_t shapes_count_ = 0u;
    uint32_t values_count_ = 0u;
    uint32_t sparse_map_bytes_ = 0u;
    uint32_t items_count_ = 0u;
    BitmapFormat format_ = BitmapFormat::UNKNOWN;
    FieldStats stats_;
    std::array<Block, 1> blocks_;
};

static_assert(sizeof(EncodedField) == 88);

enum class EncodingVersion : uint16_t { V1 = 0, V2 = 1, COUNT = 2 };

constexpr static uint16_t MAGIC_NUMBER = 0xFA57;

// FixedHeader is the first thing in every encoded segment,
// and is invariant between all encodings. The encoding version
// allows for everything after this header to potentially change,
// but is currently not used
struct FixedHeader {
    std::uint16_t magic_number;
    std::uint16_t encoding_version;
    std::uint32_t header_bytes;
};

constexpr static std::size_t FIXED_HEADER_SIZE = sizeof(FixedHeader);

// This flag indicates that a segment is a compacted set of other segments
enum class HeaderFlag : uint8_t {
    COMPACTED,
};

// The segment is bootstrapped by a set of uncompressed EncodedFields,
// that describe the sets of fields referencing the segment data. The
// location of these initial fields is described by the following
// structure.
struct FieldBuffer {
    mutable uint32_t fields_bytes_ = 0U;
    mutable uint32_t offset_bytes_ = 0U;
};

// HeaderData describes the format of the data contained within
// the segment. At the moment there are two encoding versions, a
// legacy encoding utilizing a protobuf header, and the binary
// encoding described by the MemoryLayout structure below.
struct HeaderData {
    ;
    EncodingVersion encoding_version_ = EncodingVersion::V1;
    uint16_t fields_ = 0U;
    uint8_t flags_ = 0U;
    uint64_t footer_offset_ = 0U;
    FieldBuffer field_buffer_;
};

// Dynamic schema frames can change their schema over time,
// adding and removing columns and changing types. A dynamic
// schema type indicates that for each row group, not all of
// the columns in the global descriptor will necessarily
// be found
enum class SchemaType : uint8_t { STATIC, DYNAMIC };

// The type of indexing of a frame as a whole
struct IndexDescriptor {
    enum class Type : int32_t { UNKNOWN = 0, EMPTY = 69, ROWCOUNT = 82, STRING = 83, TIMESTAMP = 84 };

    IndexDescriptor() = default;

    IndexDescriptor(Type type, uint32_t field_count) : type_(type), field_count_(field_count) {}

    Type type_ = Type::UNKNOWN;
    uint32_t field_count_ = 0U;
};

// Implementers can store additional metadata in an opaque field
// to control the denormalization of normalized frame data into
// language-specific objects such as Pandas dataframes. It is
// assumed that this information is non-essential to the data
// objects and can be ignored when denormalizing to different
// languages and libraries
enum class FrameMetadataEncoding : uint8_t { PROTOBUF = 0 };

// A FrameDescriptor describes a dataframe as a whole; it is used on
// segments that describe and index other segments
struct FrameDescriptor {
    uint64_t total_rows_ = 0UL;
    bool column_groups_ = false;
    FrameMetadataEncoding metadata_encoding_ = FrameMetadataEncoding::PROTOBUF;
};

// A SegmentDescriptor is present in every segment, and describes the
// contents of this particular segment, rather than other segments
// to which it refers
struct SegmentDescriptor {
    SortedValue sorted_ = SortedValue::UNKNOWN;
    uint64_t compressed_bytes_ = 0UL;
    uint64_t uncompressed_bytes_ = 0UL;
    uint64_t row_count_ = 0UL;
    IndexDescriptor index_;
};

// Frame identifiers can be of either numeric or string type
enum class IdentifierType : uint8_t { NUMERIC = 0, STRING = 1 };

struct SegmentIdentifierHeader {
    IdentifierType type_ = IdentifierType::NUMERIC;
    uint32_t size_ = 0;
};

// A segment header contains a set of optional fields that describe the contents of a given segment
enum class FieldOffset : uint8_t {
    METADATA,    // Opaque field for user and normalization metadata
    STRING_POOL, // Deduplicated compressed field of string data
    DESCRIPTOR,  // Collection of field names and types for the current segment
    INDEX,       // Optional additional set of fields used when this segment indexes a dataframe
    COLUMN,      // Set of encoded fields that represent the body (user) data of the segment
    COUNT
};

// Note. The structures below contain variable-length fields (represented by named structures) and should not be
// used to implement the data format directly. They are intended as a syntactically-correct representation
// of the storage format only.

// Denotes a field that may or may not be present
template<typename FieldType>
struct Optional {};

// A list of field descriptors containing the name, type and dimensionality of a column of data
struct FieldList {};

// A list of encoded fields that describes the contents of other fields. An encoded field is a list of blocks
// with a specific set of compression types
struct EncodedFieldList {};

// An opaque field to be filled with user-determined content, used for things like
// language-specific normalization data
struct OpaqueField {};

// A data field described by an EncodedField, consists of a set of compressed blocks that may represent
// shapes and values, and an optional sparse bitmap
struct ColumnField {
    ColumnMagic column_magic_;
};

// A compressed block of data containing some other structure. A compressed field is represented by an EncodedField
// which contains a set of Block objects describing the compression type
template<typename FieldType>
struct CompressedField {};

// A set of fields that are repeated, whose number corresponds to a unary field describing this set. For example, the
// number of repeated column fields should correspond to the number of entries in the descriptor (which describes the
// user-facing information about a column's contents, and the number of EncodedFields in the body fields, which describe
// the block structure and compression
template<typename FieldType>
struct RepeatedField {};

// Binary representation of a segment header. Contains positioning information about the structure of the segment,
// and the list of fields representing the segment metadata fields
struct SegmentHeaderData {
    HeaderData data_;
    EncodedFieldList header_fields_;      // Header fields containing the fields described by FieldOffsets
    std::array<uint32_t, 5> offset_ = {}; // Maps the entries in the FieldOffset enumeration to the header field entries
};

// The overall memory layout of an ArcticDB segment. Note that this class need not necessarily be used
// to implement a reader or writer of the memory layout - refer to comment above
struct MemoryLayout {
    FixedHeader fixed_header_;
    SegmentHeaderData variable_header_;

    MetadataMagic metadata_magic_;
    Optional<OpaqueField> metadata_;

    SegmentDescriptorMagic segment_descriptor_magic_;
    SegmentDescriptor segment_descriptor_;
    SegmentIdentifierHeader identifier_header_;
    Optional<OpaqueField> identifier_data_;
    DescriptorFieldsMagic descriptor_magic_;
    CompressedField<FieldList> descriptor_fields_;

    IndexMagic index_magic_;
    // Optional fields present if this segment refers to a complete dataframe, i.e. if it is a primary index
    Optional<FrameDescriptor> index_frame_descriptor_;
    Optional<SegmentDescriptorMagic> index_segment_descriptor_magic_;
    Optional<SegmentDescriptor> index_segment_descriptor_;
    Optional<SegmentIdentifierHeader> index_identifier_header_;
    Optional<OpaqueField> index_identifier_data_;
    Optional<CompressedField<FieldList>> index_descriptor_fields_;

    RepeatedField<ColumnField> columns_;

    StringPoolMagic string_pool_magic_;
    Optional<ColumnField> string_pool_field_;

    EncodedFieldList body_fields_; // Encoded field list representing the user data fields (columns)
};

#pragma pack(pop)

} // namespace arcticdb