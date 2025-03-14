#pragma once

#include <cstddef>
#include <cstdint>

namespace arcticdb {
struct DeltaCompressData {
    uint32_t simd_bit_width_ = 0;
    uint32_t remainder_bit_width_ = 0;
    uint32_t compressed_rows_ = 0;
    size_t full_blocks_ = 0;
    size_t remainder_ = 0;
};

using EncoderData = std::variant<std::monostate, DeltaCompressData>;

}