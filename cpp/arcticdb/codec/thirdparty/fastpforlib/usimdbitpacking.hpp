/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire
 */
#pragma once

#include <cstdint>
#include <immintrin.h>

namespace arcticdb_pforlib {

void usimdpack(const uint32_t *__restrict__ in, __m128i *__restrict__ out,
               uint32_t bit);
void usimdpackwithoutmask(const uint32_t *__restrict__ in,
                          __m128i *__restrict__ out, uint32_t bit);
void usimdunpack(const __m128i *__restrict__ in, uint32_t *__restrict__ out,
                 uint32_t bit);

} // namespace arcticdb_pforlib
