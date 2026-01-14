/* Copyright 2026 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, version 2.0.
 */
#pragma once

#include <arcticdb/util/preconditions.hpp>
#include <arcticdb/util/constructors.hpp>
#include <arcticdb/storage/memory_layout.hpp>

#include <utility>
#include <boost/iterator/iterator_facade.hpp>
namespace arcticdb {

class Segment;
class SegmentHeader;

std::pair<const uint8_t*, const uint8_t*> get_segment_begin_end(const Segment& segment, const SegmentHeader& hdr);

constexpr std::string_view codec_type_to_string(Codec codec) {
    switch (codec) {
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
    uint8_t* data() { return &data_[0]; }

    [[nodiscard]] Codec codec_type() const { return codec_; }

    [[nodiscard]] const uint8_t* data() const { return &data_[0]; }

    BlockCodecImpl() { memset(data(), 0, DataSize); }

    ZstdCodec* mutable_zstd() {
        codec_ = Codec::ZSTD;
        auto zstd = new (data()) ZstdCodec{};
        return zstd;
    }

    Lz4Codec* mutable_lz4() {
        codec_ = Codec::LZ4;
        auto lz4 = new (data()) Lz4Codec{};
        return lz4;
    }

    PforCodec* mutable_pfor() {
        codec_ = Codec::PFOR;
        auto pfor = new (data()) PforCodec{};
        return pfor;
    }

    PassthroughCodec* mutable_passthrough() {
        codec_ = Codec::PASS;
        auto pass = new (data()) PassthroughCodec{};
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
    explicit BlockCodecImpl(const CodecType& codec) {
        codec_ = CodecType::type;
        memcpy(data_, &codec, encoding_size);
    }
};

struct EncodedBlock : Block {
    explicit EncodedBlock(bool is_shape) { is_shape_ = is_shape; }

    EncodedBlock() = default;

    [[nodiscard]] bool has_codec() const { return codecs_[0].codec_ != Codec::PASS; }

    [[nodiscard]] auto encoder_version() const { return encoder_version_; }

    [[nodiscard]] auto codec() const { return *reinterpret_cast<const BlockCodecImpl*>(&codecs_[0]); }

    void set_in_bytes(uint32_t bytes) { in_bytes_ = bytes; }

    void set_encoder_version(uint16_t version) { encoder_version_ = version; }

    void set_out_bytes(uint32_t bytes) { out_bytes_ = bytes; }

    void set_hash(uint64_t hash) { hash_ = hash; }

    [[nodiscard]] uint64_t hash() const { return hash_; }

    [[nodiscard]] uint32_t out_bytes() const { return out_bytes_; }

    [[nodiscard]] uint32_t in_bytes() const { return in_bytes_; }

    BlockCodecImpl* mutable_codec() { return reinterpret_cast<BlockCodecImpl*>(&codecs_[0]); }
};

struct EncodedFieldImpl : public EncodedField {
    static constexpr size_t Size = sizeof(type_) + sizeof(shapes_count_) + sizeof(values_count_) +
                                   sizeof(sparse_map_bytes_) + sizeof(items_count_) + sizeof(format_) + sizeof(stats_);

    EncodedFieldImpl() = default;

    ARCTICDB_NO_MOVE_OR_COPY(EncodedFieldImpl)

    EncodedBlock* blocks() { return reinterpret_cast<EncodedBlock*>(&blocks_[0]); }

    [[nodiscard]] const EncodedBlock* blocks() const { return reinterpret_cast<const EncodedBlock*>(&blocks_[0]); }

    struct EncodedBlockCollection {
        EncodedBlockCollection(const EncodedFieldImpl& field, bool is_shapes) : field_(field), is_shapes_(is_shapes) {}

        template<class ValueType>
        class EncodedBlockCollectionIterator
            : public boost::iterator_facade<
                      EncodedBlockCollectionIterator<ValueType>, ValueType, boost::forward_traversal_tag> {
          public:
            EncodedBlockCollectionIterator(EncodedBlock* blocks, size_t increment) :
                blocks_(blocks),
                increment_(increment) {}

            ~EncodedBlockCollectionIterator() = default;

            EncodedBlockCollectionIterator(EncodedBlock* blocks, size_t pos, size_t increment) :
                blocks_(blocks),
                pos_(pos),
                increment_(increment) {}

            template<class OtherValue>
            explicit EncodedBlockCollectionIterator(const EncodedBlockCollectionIterator<OtherValue>& other) :
                blocks_(other.blocks_),
                pos_(other.pos_),
                increment_(other.increment_) {}

            EncodedBlockCollectionIterator() = default;

            EncodedBlockCollectionIterator& operator=(const EncodedBlockCollectionIterator& other) {
                if (&other != this) {
                    pos_ = other.pos_;
                    blocks_ = other.blocks_;
                    increment_ = other.increment_;
                }

                return *this;
            }

            EncodedBlockCollectionIterator(const EncodedBlockCollectionIterator& other) :
                blocks_(other.blocks_),
                pos_(other.pos_),
                increment_(other.increment_) {}

            template<class OtherValue>
            [[nodiscard]] bool equal(const EncodedBlockCollectionIterator<OtherValue>& other) const {
                return pos_ == other.pos_ && blocks_ == other.blocks_ && increment_ == other.increment_;
            }

            void increment() { pos_ += increment_; }

            [[nodiscard]] ValueType& dereference() const { return blocks_[pos_]; }

            EncodedBlock* blocks_ = nullptr;
            size_t pos_ = 0;
            size_t increment_ = 1;
        };

        [[nodiscard]] EncodedBlock* blocks() const { return const_cast<EncodedFieldImpl&>(field_).blocks(); }

        [[nodiscard]] size_t increment() const { return field_.is_scalar() || !field_.is_old_style_shapes() ? 1 : 2; }

        [[nodiscard]] auto begin() {
            return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), first(), increment());
        }

        [[nodiscard]] auto end() { return EncodedBlockCollectionIterator<EncodedBlock>(blocks(), last(), increment()); }

        [[nodiscard]] auto begin() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(blocks(), first(), increment());
        }

        [[nodiscard]] auto end() const {
            return EncodedBlockCollectionIterator<const EncodedBlock>(blocks(), last(), increment());
        }

        [[nodiscard]] size_t shape_value_offset() const { return is_shapes_ || field_.is_scalar() ? 0U : 1U; }

        [[nodiscard]] size_t first() const { return shape_value_offset(); }

        [[nodiscard]] size_t last() const {
            if (field_.is_scalar())
                return is_shapes_ ? 0 : field_.values_count_;

            if (field_.is_old_style_shapes())
                return field_.values_count_ + field_.shapes_count_ + shape_value_offset();
            else
                return is_shapes_ ? field_.shapes_count_ : field_.shapes_count_ + field_.values_count_;
        }

        [[nodiscard]] const EncodedBlock& operator[](const size_t idx) const {
            const size_t shape_offset = is_shapes_ ? 0 : field_.shapes_count_;
            return field_.blocks()[shape_offset + idx];
        }

        const EncodedFieldImpl& field_;
        bool is_shapes_;
    };

    [[nodiscard]] bool is_scalar() const { return shapes_count_ == 0; }

    [[nodiscard]] bool is_old_style_shapes() const { return shapes_size() == values_size(); }

    [[nodiscard]] EncodedFieldType encoding_case() const { return type_; }

    [[nodiscard]] const EncodedBlock& shapes(size_t n) const {
        util::check(shapes_count_ != 0, "No shape allocated");
        if (!is_old_style_shapes()) {
            util::check(n == 0, "Block index must be 0 not {} if not using old style shapes", n);
            return blocks()[0];
        } else {
            return blocks()[n * 2];
        }
    }

    [[nodiscard]] const EncodedBlock& values(size_t n) const {
        util::check(
                n < values_count_ + shapes_count_,
                "Cannot return block {} from {} blocks ({} shapes)",
                n,
                values_count_,
                shapes_count_
        );
        if (is_scalar() || !is_old_style_shapes())
            return blocks()[shapes_count_ + n];
        else
            return blocks()[(n * 2) + 1];
    }

    [[nodiscard]] EncodedBlockCollection shapes() const { return {*this, true}; }

    [[nodiscard]] EncodedBlockCollection values() const { return {*this, false}; }

    void validate() const {
        size_t shapes_count = 0;
        for (const auto& shape : shapes()) {
            util::check(shape.is_shape_, "Expected shape to have is_shape_set");
            util::check(shape.codecs_[0].codec_ != Codec::UNKNOWN, "Unknown shape codec");
            ++shapes_count;
        }
        util::check(
                shapes_count == static_cast<size_t>(shapes_size()),
                "Shape size mismatch: {} != {}",
                shapes_count,
                shapes_size()
        );

        size_t values_count = 0;
        for (const auto& value : values()) {
            util::check(!value.is_shape_, "Value has is_shape set");
            util::check(value.codec().codec_type() != Codec::UNKNOWN, "Unknown codec in block {}", values_count);
            ++values_count;
        }
        util::check(
                values_count == static_cast<size_t>(values_size()),
                "Shape size mismatch: {} != {}",
                values_count,
                values_size()
        );
    }

    EncodedBlock* add_shapes() {
        auto block = new (blocks() + (shapes_count_ * 2)) EncodedBlock{true};
        ++shapes_count_;
        return block;
    }

    [[nodiscard]] int shapes_size() const { return shapes_count_; }

    [[nodiscard]] int values_size() const { return values_count_; }

    void set_sparse_map_bytes(uint32_t bytes) { sparse_map_bytes_ = bytes; }

    void set_statistics(FieldStats stats) { stats_ = stats; }

    FieldStats get_statistics() const { return stats_; }

    EncodedBlock* add_values(EncodingVersion encoding_version) {
        const bool old_style = encoding_version == EncodingVersion::V1;
        size_t pos;
        if (!old_style || is_scalar())
            pos = shapes_count_ + values_count_;
        else
            pos = (values_count_ * 2) + 1;

        auto block = new (static_cast<void*>(blocks() + pos)) EncodedBlock{false};
        ++values_count_;
        return block;
    }

    EncodedFieldImpl* mutable_ndarray() {
        type_ = EncodedFieldType::NDARRAY;
        return this;
    }

    [[nodiscard]] const EncodedFieldImpl& ndarray() const { return *this; }

    [[nodiscard]] bool has_ndarray() const { return type_ == EncodedFieldType::NDARRAY; }

    [[nodiscard]] std::string DebugString() const {
        return fmt::format("{}: {} shapes {} values", has_ndarray() ? "NDARRAY" : "DICT", shapes_size(), values_size());
    }

    [[nodiscard]] size_t items_count() const { return items_count_; }

    [[nodiscard]] size_t sparse_map_bytes() const { return sparse_map_bytes_; }

    void set_items_count(uint32_t count) { items_count_ = count; }
};

static_assert(EncodedFieldImpl::Size == sizeof(EncodedFieldImpl) - sizeof(EncodedBlock));

inline size_t calc_field_bytes(size_t num_blocks) {
    return EncodedFieldImpl::Size + (sizeof(EncodedBlock) * num_blocks);
}

inline size_t encoded_field_bytes(const EncodedField& encoded_field) {
    return calc_field_bytes(encoded_field.shapes_count_ + encoded_field.values_count_);
}

} // namespace arcticdb

namespace fmt {
template<>
struct formatter<arcticdb::BlockCodecImpl> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(arcticdb::BlockCodecImpl codec, FormatContext& ctx) const {
        return format_to(ctx.out(), "{}", arcticdb::codec_type_to_string(codec.codec_type()));
    }
};

template<>
struct formatter<arcticdb::EncodedFieldImpl> {
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const arcticdb::EncodedFieldImpl& field, FormatContext& ctx) const {
        const char* label = field.has_ndarray() ? "NDARRAY\n" : "DICT\n";
        fmt::format_to(ctx.out(), "{}", label);

        fmt::format_to(ctx.out(), "Shapes: {}\n", field.shapes_size());
        for (const auto& shape : field.shapes()) {
            fmt::format_to(
                    ctx.out(),
                    "\tCodec: {} in_bytes: {}, out_bytes {}\n",
                    arcticdb::codec_type_to_string(shape.codecs_[0].codec_),
                    shape.in_bytes(),
                    shape.out_bytes()
            );
        }

        fmt::format_to(ctx.out(), "Values: {}\n", field.values_size());
        for (const auto& value : field.values()) {
            fmt::format_to(
                    ctx.out(),
                    "\tCodec: {} in_bytes: {}, out_bytes {}\n",
                    arcticdb::codec_type_to_string(value.codecs_[0].codec_),
                    value.in_bytes(),
                    value.out_bytes()
            );
        }
        return fmt::format_to(ctx.out(), "\n");
    }
};

} // namespace fmt
