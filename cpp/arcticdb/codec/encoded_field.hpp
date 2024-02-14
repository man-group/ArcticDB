/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <utility>
#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/entity/protobufs.hpp>
#include <arcticdb/memory_layout.hpp>
#include <boost/iterator/iterator_facade.hpp>
namespace arcticdb {

class Segment;
class SegmentHeader;

std::pair<const uint8_t*, const uint8_t*> get_segment_begin_end(
    const Segment &segment,
    const SegmentHeader& hdr);


constexpr std::string_view codec_type_to_string(Codec codec) {
    switch(codec) {
    case Codec::LZ4:
        return "LZ4";
    case Codec::ZSTD:
        return "ZSTD";
    case Codec::PFOR:
        return "PFOR";
    case Codec::PASS:
        return "PASS";
    default:
        return "Unknown";
    }
}

struct BlockCodecImpl : public BlockCodec {
    uint8_t* data() {
        return &data_[0];
    }

    Codec codec_type() const {
        return codec_;
    }

    [[nodiscard]] const uint8_t* data() const {
        return &data_[0];
    }

    BlockCodecImpl() {
        memset(data(), 0, DataSize);
    }

    ZstdCodec *mutable_zstd() {
        codec_ = Codec::ZSTD;
        auto zstd = new(data()) ZstdCodec{};
        return zstd;
    }

    Lz4Codec *mutable_lz4() {
        codec_ = Codec::LZ4;
        auto lz4 = new(data()) Lz4Codec{};
        return lz4;
    }

    PforCodec *mutable_pfor() {
        codec_ = Codec::PFOR;
        auto pfor = new(data()) PforCodec{};
        return pfor;
    }

    PassthroughCodec *mutable_passthrough() {
        codec_ = Codec::PASS;
        auto pass = new(data()) PassthroughCodec{};
        return pass;
    }

    [[nodiscard]] const ZstdCodec& zstd() const {
        util::check(codec_ == Codec::ZSTD, "Not a zstd codec");
        return *reinterpret_cast<const ZstdCodec*>(data());
    }

    [[nodiscard]] const Lz4Codec& lz4() const {
        util::check(codec_ == Codec::LZ4, "Not an lz4 codec");
        return *reinterpret_cast<const Lz4Codec*>(data());
    }

    [[nodiscard]] const PforCodec& pfor() const {
        util::check(codec_ == Codec::PFOR, "Not a pfor codec");
        return *reinterpret_cast<const PforCodec*>(data());
    }

    [[nodiscard]] const PassthroughCodec& passthrough() const {
        util::check(codec_ == Codec::PASS, "Not a passthrough codec");
        return *reinterpret_cast<const PassthroughCodec*>(data());
    }

    template<class CodecType>
    explicit BlockCodecImpl(const CodecType &codec) {
        codec_ = CodecType::type;
        memcpy(data_, &codec, encoding_size);
    }
};

struct EncodedBlock : Block {
    explicit EncodedBlock(bool is_shape) {
        is_shape_ = is_shape;
    }

    EncodedBlock() = default;

    [[nodiscard]] bool has_codec() const {
        return codecs_[0].codec_ != Codec::PASS;
    }

    [[nodiscard]] auto encoder_version() const {
        return encoder_version_;
    }

    [[nodiscard]] auto codec() const {
        return *reinterpret_cast<const BlockCodecImpl*>(&codecs_[0]);
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

    [[nodiscard]] uint64_t hash() const {
        return hash_;
    }

    [[nodiscard]] uint32_t out_bytes() const {
        return out_bytes_;
    }

    [[nodiscard]] uint32_t in_bytes() const {
        return in_bytes_;
    }

    BlockCodecImpl *mutable_codec() {
        return reinterpret_cast<BlockCodecImpl*>(&codecs_[0]);
    }
};

struct EncodedFieldImpl : public EncodedField {
    static constexpr size_t Size =
        sizeof(type_) +
            sizeof(shapes_count_) +
            sizeof(values_count_) +
            sizeof(sparse_map_bytes_) +
            sizeof(items_count_);

    EncodedFieldImpl() = default;

    EncodedBlock *blocks() {
        return reinterpret_cast<EncodedBlock*>(&blocks_[0]);
    }

    [[nodiscard]] const EncodedBlock* blocks() const {
        return reinterpret_cast<const EncodedBlock*>(&blocks_[0]);
    }

    struct EncodedBlockCollection {
        EncodedBlockCollection(const EncodedFieldImpl &field, bool is_shapes) :
            field_(field),
            is_shapes_(is_shapes) {
        }

        template<class ValueType>
        class EncodedBlockCollectionIterator : public boost::iterator_facade<EncodedBlockCollectionIterator<ValueType>,
                                                                             ValueType,
                                                                             boost::forward_traversal_tag> {
        public:
            explicit EncodedBlockCollectionIterator(EncodedBlock *blocks) :
                blocks_(blocks) {}

           ~EncodedBlockCollectionIterator() = default;

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

            EncodedBlock *blocks_ = nullptr;
            size_t pos_ = 0;
        };

        [[nodiscard]] EncodedBlock *blocks() const {
            return const_cast<EncodedFieldImpl&>(field_).blocks();
        }

        [[nodiscard]] auto begin() {
            return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), first());
        }

        [[nodiscard]] auto end() {
            return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), last());
        }

        [[nodiscard]] auto begin() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(blocks(), first());
        }

        [[nodiscard]] auto end() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(blocks(), last());
        }

        [[nodiscard]] size_t first() const {
            return is_shapes_ ? 0u : field_.shapes_count_;
        }

        [[nodiscard]] size_t last() const {
            return is_shapes_ ? field_.shapes_count_ : field_.shapes_count_ + field_.values_count_;
        }

        [[nodiscard]] const EncodedBlock& operator[](const size_t idx) const {
            // Shape blocks are located before values blocks in the field. In case this is a collection of value blocks
            // we have to skip all shape blocks. In case this is a collection of shapes we can start from 0 index.
            const size_t shape_offset = is_shapes_ ? 0 : field_.shapes_count_;
            return field_.blocks()[shape_offset + idx];
        }
        const EncodedFieldImpl& field_;
        bool is_shapes_;
    };

    [[nodiscard]] EncodedFieldType encoding_case() const {
        return type_;
    }

    [[nodiscard]] const EncodedBlock& shapes(size_t n) const {
        util::check(n == 0, "Expected only one shape");
        util::check(shapes_count_ != 0, "No shape allocated");
        return *reinterpret_cast<const EncodedBlock*>(&blocks_[0]);
    }

    [[nodiscard]] const EncodedBlock &values(size_t n) const {
        util::check(n < values_count_ + shapes_count_, "Cannot return block {} from {} blocks ({} shapes)", n, values_count_, shapes_count_);
        return blocks()[shapes_count_ + n];
    }

    [[nodiscard]] EncodedBlockCollection shapes() const {
        return {*this, true};
    }

    [[nodiscard]] EncodedBlockCollection values() const {
        return {*this, false};
    }

    EncodedBlock *add_shapes() {
        util::check(shapes_count_ == 0, "Expected single shapes block");
        auto block = new(blocks() + items_count()) EncodedBlock{true};
        ++shapes_count_;
        return block;
    }

    [[nodiscard]] int shapes_size() const {
        return shapes_count_;
    }

    [[nodiscard]] int values_size() const {
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

    EncodedFieldImpl *mutable_ndarray() {
        type_ = EncodedFieldType::NDARRAY;
        return this;
    }

    [[nodiscard]] const EncodedFieldImpl &ndarray() const {
        return *this;
    }

    [[nodiscard]] bool has_ndarray() const {
        return type_ == EncodedFieldType::NDARRAY;
    }

    [[nodiscard]] std::string DebugString() const {
        return "";
    }

    [[nodiscard]] size_t items_count() const {
        return items_count_;
    }

    [[nodiscard]] size_t sparse_map_bytes() const {
        return sparse_map_bytes_;
    }

    void set_items_count(uint32_t count) {
        items_count_ = count;
    }
};

inline size_t encoded_field_bytes(const EncodedField &encoded_field) {
    return sizeof(EncodedField)
        + (sizeof(EncodedBlock) * ((encoded_field.shapes_count_ + encoded_field.values_count_) - 1));
}



} //namespace arcticc


namespace fmt {
template<>
struct formatter<arcticdb::BlockCodecImpl> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(arcticdb::BlockCodecImpl codec, FormatContext &ctx) const {

        return format_to(ctx.out(), "{}", arcticdb::codec_type_to_string(codec.codec_type()));
    }
};

template<>
struct formatter<arcticdb::EncodedFieldImpl> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext &ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const arcticdb::EncodedFieldImpl& field, FormatContext &ctx) const {
        return format_to(ctx.out(), "{}", field.has_ndarray() ? "NDARRAY" : "DICT"); //TODO better formatting
    }
};

} // namespace fmt
