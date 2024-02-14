#pragma once

#include <cstdint>

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
constexpr static bool UNSET = false;

struct HeaderData { ;
    EncodingVersion encoding_version_ = EncodingVersion::V1;
    std::array<bool, 5> optional_header_fields_ = {UNSET, UNSET,UNSET, UNSET, UNSET};
    bool compacted_ = false;
    uint64_t footer_offset_ = 0;
};

struct StreamDescriptorData {
    SortedValue sorted_ = SortedValue::UNKNOWN;
    uint64_t compressed_bytes_ = 0UL;
    uint64_t uncompressed_bytes_ = 0UL;
    StreamId stream_id_;
    IndexDescriptor index_ = IndexDescriptor{};
};

#pragma pack(pop)
}