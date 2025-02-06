#pragma once

#include <arcticdb/codec/default_codecs.hpp>
#include <arcticdb/codec/encoded_field.hpp>
#include <arcticdb/column_store/column_data.hpp>
#include <arcticdb/column_store/statistics.hpp>
#include <arcticdb/storage/memory_layout.hpp>
#include <arcticdb/codec/compression/encoders.hpp>

namespace arcticdb {

class EncodingsList {
    uint64_t data_;

    static constexpr uint8_t to_pos(EncodingType encoding) {
        return 1 << (static_cast<uint8_t>(encoding));
    }

public:
    constexpr EncodingsList() : data_(0) {}

    constexpr EncodingsList(std::initializer_list<EncodingType> encodings) : data_(0) {
        for (const auto& encoding : encodings) {
            set(encoding);
        }
    }

    constexpr void set(EncodingType encoding) {
        data_ |= to_pos(encoding);
    }

    constexpr void unset(EncodingType encoding) {
        data_ &= ~to_pos(encoding);
    }

    [[nodiscard]] constexpr bool is_set(EncodingType encoding) const {
        return data_ & to_pos(encoding);
    }
};

constexpr EncodingsList IntegerEncodings {
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FFOR,
    EncodingType::FREQUENCY
    //EncodingType::RLE
};

constexpr EncodingsList FloatEncodings {
    EncodingType::CONSTANT,
    EncodingType::ALP
};

constexpr EncodingsList StringEncodings {
    EncodingType::CONSTANT,
    EncodingType::DELTA,
    EncodingType::FREQUENCY
    //EncodingType::RLE
};

template <typename TypeDescriptorTag>
EncodingsList viable_encodings(
        EncodingsList input,
        FieldStatsImpl field_stats,
        DataType data_type,
        size_t row_count) {
    EncodingsList output;
    for(auto i = 0U; i < static_cast<uint8_t>(EncodingType::COUNT); ++i) {
        const auto type = EncodingType(i);
        if(input.is_set(type) && is_viable(type, field_stats, data_type, row_count))
            output.set(type);
    }
    return output;
}



} // namespace arcticdb