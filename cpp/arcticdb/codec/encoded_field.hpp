#pragma once

#include <arcticdb/codec/segment.hpp>

namespace arcticdb {

inline std::pair<const uint8_t*, const uint8_t*> get_segment_begin_end(const Segment& segment, const arcticdb::proto::encoding::SegmentHeader& hdr)  {
    const uint8_t* data = segment.buffer().data();
    util::check(data != nullptr, "Got null data ptr from segment");
    const uint8_t* begin = data;

    const auto fields_offset = hdr.column_fields().offset();
    const auto end = begin + fields_offset;
    return {begin, end};
}

constexpr size_t encoding_size = 6;
enum class Codec : uint16_t {
    Unknown = 0,
    Zstd,
    TurboPfor,
    Lz4,
    Passthrough
};

struct ZstdCodec { //: public CodecBase<ZstdCodec> {
    static constexpr Codec type_ = Codec::Zstd;

    void MergeFrom(const arcticdb::proto::encoding::VariantCodec::Zstd &zstd) {
        level_ = zstd.level();
        is_streaming = zstd.is_streaming();
    }

    int32_t level_ = 0;
    bool is_streaming = false;
    uint8_t padding_ = 0;
} __attribute__((packed));

static_assert(sizeof(ZstdCodec) == encoding_size);

struct TurboPforCodec {
    static constexpr Codec type_ = Codec::TurboPfor;

    void MergeFrom(const arcticdb::proto::encoding::VariantCodec::TurboPfor &tp4) {
        sub_codec_ = SubCodec(tp4.sub_codec());
    }

    enum class SubCodec : uint32_t {
        UNKNOWN = 0,
        P4 = 16,
        P4_DELTA = 17,
        P4_DELTA_RLE = 18,
        P4_ZZ = 20,

        FP_DELTA = 32, // fpp
        FP_DELTA2_ZZ = 33,  // fpzz
        FP_GORILLA_RLE = 34, // fpg
        FP_ZZ = 36, // bvz
        FP_ZZ_DELTA = 40, // bvz
    };

    SubCodec sub_codec_ = SubCodec::UNKNOWN;
    uint16_t padding_ = 0;
} __attribute__((packed));

static_assert(sizeof(TurboPforCodec) == encoding_size);

struct Lz4Codec {
    static constexpr Codec type_ = Codec::Lz4;

    void MergeFrom(arcticdb::proto::encoding::VariantCodec::Lz4 lz4) {
        acceleration_ = lz4.acceleration();
    }

    int32_t acceleration_;
    int16_t padding_ = 0;
} __attribute__((packed));;

static_assert(sizeof(Lz4Codec) == encoding_size);

struct PassthroughCodec {
    static constexpr Codec type_ = Codec::Passthrough;

    uint32_t unused_ = 0;
    uint16_t padding_ = 0;
} __attribute__((packed));

static_assert(sizeof(PassthroughCodec) == encoding_size);

struct BlockCodec {
    Codec codec_ = Codec::Unknown;
    constexpr static size_t DataSize = 24;
    uint8_t data_[DataSize];

    BlockCodec() {
        memset(data_, 0, DataSize);
    }

    ZstdCodec *mutable_zstd() {
        codec_ = Codec::Zstd;
        auto zstd = new(data_) ZstdCodec{};
        return zstd;
    }

    Lz4Codec *mutable_lz4() {
        codec_ = Codec::Lz4;
        auto lz4 = new(data_) Lz4Codec{};
        return lz4;
    }

    TurboPforCodec *mutable_turbopfor() {
        codec_ = Codec::TurboPfor;
        auto pfor = new(data_) TurboPforCodec{};
        return pfor;
    }

    PassthroughCodec *mutable_passthrough() {
        codec_ = Codec::Passthrough;
        auto pass = new(data_) PassthroughCodec{};
        return pass;
    }

    arcticdb::proto::encoding::VariantCodec::CodecCase codec_case() const {
        switch(codec_) {
        case Codec::Zstd:
            return arcticdb::proto::encoding::VariantCodec::kLz4;
        case Codec::Lz4:
            return arcticdb::proto::encoding::VariantCodec::kLz4;
        case Codec::TurboPfor:
            return arcticdb::proto::encoding::VariantCodec::kTp4;
        case Codec::Passthrough:
            return arcticdb::proto::encoding::VariantCodec::kPassthrough;
        default:
            util::raise_rte("Unknown codec");
        }
    }

    template<class CodecType>
    explicit BlockCodec(const CodecType &codec) :
        codec_(CodecType::type) {
        memcpy(data_, &codec, encoding_size);
    }
};

struct EncodedBlock {
    uint32_t in_bytes_ = 0;
    uint32_t out_bytes_ = 0;
    uint64_t hash_ = 0;
    uint16_t encoder_version_ = 0;
    bool is_shape_ = false;
    uint8_t pad_ = 0;
    BlockCodec codec_;

    EncodedBlock() = default;

    EncodedBlock(bool is_shape) :
        is_shape_(is_shape) {
    }

    std::string DebugString() const {
        return "";
    }

    bool has_codec() const {
        return true; //TODO wrong
    }

    auto encoder_version() const {
        return encoder_version_;
    }

    auto codec() const {
        return codec_;
    }

    void set_in_bytes(uint32_t bytes) {
        in_bytes_ = bytes;
    }

    void set_encoder_version(uint16_t version) {
        encoder_version_ = version;
    }

    void set_out_bytes(uint32_t bytes) {
        out_bytes_ = bytes;
    }

    void set_hash(uint64_t hash) {
        hash_ = hash;
    }

    uint32_t out_bytes() const {
        return out_bytes_;
    }

    uint32_t in_bytes() const {
        return in_bytes_;
    }

    void set_version(uint32_t version) {
        encoder_version_ = version;
    }

    BlockCodec *mutable_codec() {
        return &codec_;
    }
};

struct EncodedField {

    enum class EncodedFieldType : uint8_t {
        Unknown,
        kNdarray,
        Dictionary
    };

    EncodedFieldType type_ = EncodedFieldType::Unknown;
    google::protobuf::uint8 shapes_count_ = 0u;
    uint16_t values_count_ = 0u;
    uint32_t sparse_map_bytes_ = 0u;
    uint64_t items_count_ = 0u;
    EncodedBlock blocks_[1];

    static constexpr EncodedFieldType kNdarray = EncodedFieldType::kNdarray;

    static constexpr size_t Size =
            sizeof(type_) +
            sizeof(shapes_count_) +
            sizeof(values_count_) +
            sizeof(sparse_map_bytes_) +
            sizeof(items_count_);

    EncodedField() = default;

    EncodedBlock *blocks() {
        return blocks_;
    }

    struct EncodedBlockCollection {
        EncodedBlockCollection(const EncodedField& field, bool is_shapes) :
        field_(field),
        is_shapes_(is_shapes){
        }

        template<class ValueType>
            class EncodedBlockCollectionIterator : public boost::iterator_facade<EncodedBlockCollectionIterator<ValueType>,
                ValueType,
                boost::forward_traversal_tag> {
        public:
            explicit EncodedBlockCollectionIterator(EncodedBlock *blocks) :
            blocks_(blocks) {}

            explicit EncodedBlockCollectionIterator(EncodedBlock *blocks, size_t pos) :
            blocks_(blocks),
            pos_(pos) {}

            template<class OtherValue>
                explicit EncodedBlockCollectionIterator(const EncodedBlockCollectionIterator<OtherValue> &other) :
                blocks_(other.blocks_),
                pos_(other.pos_) {}

                EncodedBlockCollectionIterator() = default;

            EncodedBlockCollectionIterator &operator=(const EncodedBlockCollectionIterator &other) {
                if (&other != this) {
                    pos_ = other.pos_;
                    blocks_ = other.blocks_;
                }

                return *this;
            }

            EncodedBlockCollectionIterator(const EncodedBlockCollectionIterator &other) :
            blocks_(other.blocks_),
            pos_(other.pos_) {
            }

            template<class OtherValue>
                bool equal(const EncodedBlockCollectionIterator<OtherValue> &other) const {
                return pos_ == other.pos_ && blocks_ == other.blocks_;
            }

            void increment() {
                ++pos_;
            }

            [[nodiscard]] ValueType &dereference() const {
                return blocks_[pos_];
            }

            EncodedBlock* blocks_ = nullptr;
            size_t pos_ = 0;
        };

        EncodedBlock* blocks() const {
            return const_cast<EncodedBlock*>(const_cast<EncodedField&>(field_).blocks());
        }

        [[nodiscard]] auto begin() {
            return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), first());
        }

        [[nodiscard]] auto end() {
            return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), last());
        }

        [[nodiscard]] auto begin() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(const_cast<EncodedBlock*>(blocks()), first());
        }

        [[nodiscard]] auto end() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(const_cast<EncodedBlock*>(blocks()), last());
        }

        size_t first() const {
            return is_shapes_ ? 0u : field_.shapes_count_;
        }

        size_t last() const {
            return is_shapes_ ? field_.shapes_count_ : field_.shapes_count_ + field_.values_count_;
        }

        const EncodedField& field_;
        bool is_shapes_;
    };

    EncodedFieldType encoding_case() const {
        return type_;
    }

    const EncodedBlock& shapes(size_t n) const {
        util::check(n == 0, "Expected only one shape");
        util::check(shapes_count_ != 0, "No shape allocated");
        return blocks_[0];
    }

    const EncodedBlock& values(size_t n) const {
        util::check(n < items_count(), "Cannot return block {} from {} blocks", n, items_count());
        return blocks_[shapes_count_ + n];
    }

    EncodedBlockCollection shapes() const {
        return {*this, true};
    }

    EncodedBlockCollection values() const {
        return {*this, false};
    }

    EncodedBlock *add_shapes() {
        util::check(shapes_count_ == 0, "Expected single shapes block");
        auto block = new(blocks() + items_count()) EncodedBlock{true};
        ++shapes_count_;
        return block;
    }

    int shapes_size() const {
        return shapes_count_;
    }

    int values_size() const {
        return values_count_;
    }

    void set_sparse_map_bytes(uint32_t bytes) {
        sparse_map_bytes_ = bytes;
    }

    EncodedBlock *add_values() {
        auto block = new(static_cast<void *>(blocks() + shapes_count_ + values_count_)) EncodedBlock{false};
        ++values_count_;
        return block;
    }

    EncodedField *mutable_ndarray() {
        type_ = EncodedFieldType::kNdarray;
        return this;
    }

    const EncodedField& ndarray() const {
        return *this;
    }

    bool has_ndarray() const {
        return type_ == EncodedFieldType::kNdarray;
    }

    std::string DebugString() const {
        return "";
    }

    [[nodiscard]] size_t items_count() const {
        return items_count_;
    }

    size_t sparse_map_bytes() const {
        return sparse_map_bytes_;
    }


    void set_items_count(size_t count) {
        items_count_ = count;
    }
};

static_assert(sizeof(EncodedField) - sizeof(EncodedBlock) == EncodedField::Size);

template <size_t x>
struct none_type;

//none_type<sizeof(EncodedField)> thing;
//none_type<sizeof(EncodedBlock)> thing2;
//none_type<EncodedField::Size> thing3;

inline size_t encoded_field_bytes(const EncodedField &encoded_field) {
    return sizeof(EncodedField) + (sizeof(EncodedBlock) * ((encoded_field.shapes_count_ + encoded_field.values_count_) - 1));
}

} //namespace arcticc
