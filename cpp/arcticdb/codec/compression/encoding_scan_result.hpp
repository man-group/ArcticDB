#pragma once

#include <cstddef>
#include <cstdint>

#include <arcticdb/codec/compression/encoder_data.hpp>

namespace arcticdb {

struct EncodingScanResult {

    EncodingScanResult() = default;

    EncodingScanResult(
        size_t cost,
        size_t estimated_size,
        EncodingType type,
        bool is_deterministic
    ) :
        cost_(cost),
        estimated_size_(estimated_size),
        type_(type),
        is_deterministic_(is_deterministic) {
    }

    ARCTICDB_MOVE_COPY_DEFAULT(EncodingScanResult)

    size_t cost_ = std::numeric_limits<size_t>::max();
    size_t estimated_size_ = 0;
    EncodingType type_;
    bool is_deterministic_ = false;
    EncoderData data_;
};



EncodingScanResult create_scan_result(EncodingType encoding_type, size_t estimated_size, size_t speed, size_t original_size, bool is_deterministic) {
    static size_t weight = ConfigsMap::instance()->get_int("Scanner.SizeWeighting", 17);
    auto score = calculate_score(speed, estimated_size, original_size, weight);
    return EncodingScanResult(score, estimated_size, encoding_type, is_deterministic);
}

}
