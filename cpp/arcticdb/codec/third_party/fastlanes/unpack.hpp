#ifndef BITPACK_BITPACK_HPP
#define BITPACK_BITPACK_HPP

#include <cstdint>

namespace generated { namespace unpack {
namespace fallback {
namespace scalar {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace scalar

namespace unit64 {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace unit64
} // namespace fallback

namespace helper { namespace scalar {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
}} // namespace helper::scalar

namespace x86_64 {
namespace avx2 {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace avx2

namespace avx512f {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace avx512f

namespace avx512bw {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace avx512f

namespace sse {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace sse
} // namespace x86_64

namespace wasm { namespace simd128 {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
}} // namespace wasm::simd128

namespace arm64v8 {
namespace neon {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace neon

namespace sve {
void unpack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void unpack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void unpack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void unpack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
} // namespace sve
} // namespace arm64v8
}} // namespace generated::unpack

#endif