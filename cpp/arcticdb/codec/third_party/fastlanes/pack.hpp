#ifndef FLS_PACK_HPP
#define FLS_PACK_HPP

#include <cstdint>

namespace generated { namespace pack { namespace fallback { namespace scalar {
void pack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void pack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void pack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void pack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
}}}} // namespace generated::pack::fallback::scalar

namespace generated { namespace pack { namespace helper { namespace scalar {
void pack(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw);
void pack(const uint32_t* __restrict in, uint32_t* __restrict out, uint8_t bw);
void pack(const uint16_t* __restrict in, uint16_t* __restrict out, uint8_t bw);
void pack(const uint8_t* __restrict in, uint8_t* __restrict out, uint8_t bw);
}}}} // namespace generated::pack::helper::scalar

#endif // FLS_PACK_HPP