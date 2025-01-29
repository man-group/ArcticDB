#pragma once

#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>


namespace arcticdb {

struct CandidateList {
    boost::container::small_vector<BlockCodecImpl, 4> candidates_;
    void set_candidate(BlockCodecImpl candidate) {
        candidates_.emplace_back(candidate);
    }

    auto begin() { return candidates_.begin(); }

    auto end() { return candidates_.end();}
};

enum class EncodingTypes {
    PLAIN,
    FFOR,
    DELTA,
    FREQUENCY,
    CONSTANT,
    RLE,
    ALP
};

constexpr std::array<Codec, 4> integer_codec_types = { };

struct IntegerScanner {

    BlockCodecImpl scan(const ColumnData) {

    }
};

struct FloatScanner {
    BlockCodecImpl scan(const ColumnData) {

    }
};

struct StringScanner {
    BlockCodecImpl scan(const ColumnData) {
        return codec::default_lz4_codec();
    }
};

} // namespace arcticdb