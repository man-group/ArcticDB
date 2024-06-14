/**
* This code is released under the
* Apache License Version 2.0 http://www.apache.org/licenses/.
*
* (c) Daniel Lemire, http://lemire.me/en/
*/
#pragma once

#include <cstdint>
#include <string>
#include <stdexcept>

namespace arcticdb_pforlib {

__attribute__((const)) inline bool divisibleby(size_t a, uint32_t x) {
    return (a % x == 0);
}

inline void checkifdivisibleby(size_t a, uint32_t x) {
    if (!divisibleby(a, x)) {
        std::string msg = std::to_string(a) + " not divisible by "
            + std::to_string(x);
        throw std::logic_error(msg);
    }
}

__attribute__((const)) inline uint32_t asmbits(const uint32_t v) {
#ifdef _MSC_VER
    return gccbits(v);
#elif defined(__aarch64__)
    return gccbits(v);
#else
    if (v == 0)
        return 0;

    uint32_t answer;
    __asm__("bsr %1, %0;" : "=r"(answer) : "r"(v));
    return answer + 1;
#endif
}

__attribute__((const)) inline uint32_t gccbits(const uint32_t v) {
#ifdef _MSC_VER
    if (v == 0) {
    return 0;
  }
  unsigned long answer;
  _BitScanReverse(&answer, v);
  return answer + 1;
#else
    return v == 0 ? 0 : 32 - __builtin_clz(v);
#endif
}

__attribute__((const)) inline uint32_t gccbits(const uint64_t v) {
    if (v == 0) {
        return 0;
    }
#ifdef _MSC_VER
    unsigned long index;
  #ifdef _WIN64
    _BitScanReverse64(&index, v);
    return static_cast<uint32_t>(index + 1);
  #else
    if (v >> 32 == 0) {
      _BitScanReverse(&index, (uint32_t)v);
      return static_cast<uint32_t>(index + 1);
    } else {
      _BitScanReverse(&index, (uint32_t)(v >> 32));
      return static_cast<uint32_t>(index + 32 + 1);
    }
  #endif
#elif defined(__aarch64__)
    return 64 - __builtin_clzll(v);
#else
    uint32_t answer;
    __asm__("bsr %1, %0;" : "=r"(answer) : "r"(v));
    return answer + 1;
#endif
}

}