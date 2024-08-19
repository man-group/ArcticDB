/* Copyright 2023 Man Group Operations Limited
 *
 * Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
 */

#pragma once

#include <cstdint>
#include <cstdlib>

#include <arcticdb/util/preprocess.hpp>

static constexpr size_t CSV_BUFFER_SIZE = 4;

namespace arcticdb {

#ifdef __AVX2__
struct InputBlock {
    __m256i lo;
    __m256i hi;
};
#elif defined(__ARM_NEON)
    uint8x16_t i0;
    uint8x16_t i1;
    uint8x16_t i2;
    uint8x16_t i3;
#else
struct InputBlock {
    uint8_t data[64];
};
#endif

#ifdef __AVX2__
ARCTICDB_ALWAYS_INLINE InputBlock fill_input(const uint8_t * ptr) {
    InputBlock in;
    in.lo = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 0));
    in.hi = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + 32));
    return in;
}
#elif defined(__ARM_NEON)
ARCTICDB_ALWAYS_INLINE InputBlock fill_input(const uint8_t * ptr) {
    InputBlock in;
    in.i0 = vld1q_u8(ptr + 0);
    in.i1 = vld1q_u8(ptr + 16);
    in.i2 = vld1q_u8(ptr + 32);
    in.i3 = vld1q_u8(ptr + 48);
    return in;
}
#else
InputBlock fill_input(const uint8_t* ptr) {
    InputBlock in;
    for (int i = 0; i < 64; ++i) {
        in.data[i] = ptr[i];
    }
    return in;
}
#endif


#ifdef __AVX2__
ARCTICDB_FORCE_INLINE uint64_t cmp_mask_against_input(InputBlock in, uint8_t m) {
    const __m256i mask = _mm256_set1_epi8(m);
    __m256i cmp_res_0 = _mm256_cmpeq_epi8(in.lo, mask);
    uint64_t res_0 = static_cast<uint32_t>(_mm256_movemask_epi8(cmp_res_0));
    __m256i cmp_res_1 = _mm256_cmpeq_epi8(in.hi, mask);
    uint64_t res_1 = _mm256_movemask_epi8(cmp_res_1);
    return res_0 | (res_1 << 32);
}
#elif defined(__ARM_NEON)
ARCTICDB_FORCE_INLINE uint64_t cmp_mask_against_input(InputBlock in, uint8_t m) {
    const uint8x16_t mask = vmovq_n_u8(m);
    uint8x16_t cmp_res_0 = vceqq_u8(in.i0, mask);
    uint8x16_t cmp_res_1 = vceqq_u8(in.i1, mask);
    uint8x16_t cmp_res_2 = vceqq_u8(in.i2, mask);
    uint8x16_t cmp_res_3 = vceqq_u8(in.i3, mask);
    return neonmovemask_bulk(cmp_res_0, cmp_res_1, cmp_res_2, cmp_res_3);
}
#else
ARCTICDB_FORCE_INLINE uint64_t cmp_mask_against_input(InputBlock in, uint8_t m) {
    uint64_t res_0 = 0;
    uint64_t res_1 = 0;
    for (int i = 0; i < 32; ++i) {
        res_0 |= (in.data[i] == m) ? (1ULL << i) : 0;
        res_1 |= (in.data[i + 32] == m) ? (1ULL << i) : 0;
    }
    return res_0 | (res_1 << 32);
}
#endif

uint64_t find_quote_mask(InputBlock in, uint64_t& prev_iter_inside_quote) {
    uint64_t quote_bits = cmp_mask_against_input(in, '"');
#ifdef __AVX2__
    uint64_t quote_mask = _mm_cvtsi128_si64(_mm_clmulepi64_si128(
      _mm_set_epi64x(0ULL, quote_bits), _mm_set1_epi8(0xFF), 0));
#elif defined(__ARM_NEON)
    uint64_t quote_mask = vmull_p64( -1ULL, quote_bits);
#else
    uint64_t quote_mask = 0;
    for (int i = 0; i < 64; ++i) {
        if (quote_bits & (1ULL << i)) {
            quote_mask ^= ~0ULL << i;
        }
    }
#endif
    quote_mask ^= prev_iter_inside_quote;
    prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);
    return quote_mask;
}

struct CsvIndexes {
    uint32_t n_indexes{0};
    uint32_t *indexes;
};

//#define CRLF

ARCTICDB_FORCE_INLINE void flatten_bits(
        uint32_t *base_ptr,
        uint32_t &base,
        uint32_t idx,
        uint64_t bits) {
    if (bits != 0u) {
        uint32_t cnt = hamming(bits);
        uint32_t next_base = base + cnt;
        base_ptr[base + 0] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 1] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 2] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 3] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 4] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 5] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 6] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        base_ptr[base + 7] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
        bits = bits & (bits - 1);
        if (cnt > 8) {
            base_ptr[base + 8] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 9] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 10] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 11] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 12] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 13] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 14] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
            base_ptr[base + 15] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
            bits = bits & (bits - 1);
        }
        if (cnt > 16) {
            base += 16;
            do {
                base_ptr[base] = static_cast<uint32_t>(idx) + trailingzeroes(bits);
                bits = bits & (bits - 1);
                base++;
            } while (bits != 0);
        }
        base = next_base;
    }
}

bool find_indexes(const uint8_t *buf, size_t len, CsvIndexes& parsed_output) {
    parsed_output.indexes = new (std::nothrow) uint32_t[len];
    uint64_t prev_iter_inside_quote = 0ULL;  // either all zeros or all ones
#ifdef CRLF
    uint64_t prev_iter_cr_end = 0ULL;
#endif
    size_t bufferable_length = len < 64 ? 0 : len - 64;
    size_t idx = 0;
    uint32_t *base_ptr = parsed_output.indexes;
    *base_ptr = 0;
    ++base_ptr;
    uint32_t base = 0;

    if (bufferable_length > 64 * CSV_BUFFER_SIZE) {
        uint64_t fields[CSV_BUFFER_SIZE];
        for (; idx < bufferable_length - 64 * CSV_BUFFER_SIZE + 1; idx += 64 * CSV_BUFFER_SIZE) {
            for (size_t b = 0; b < CSV_BUFFER_SIZE; b++) {
                size_t internal_idx = 64 * b + idx;
                ARCTICDB_PREFETCH(buf + internal_idx + 128);
                InputBlock in = fill_input(buf + internal_idx);
                uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
                uint64_t sep = cmp_mask_against_input(in, ',');
#ifdef CRLF
                uint64_t cr = cmp_mask_against_input(in, 0x0d);
                uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
                uint64_t lf = cmp_mask_against_input(in, 0x0a);
                uint64_t end = lf & cr_adjusted;
                prev_iter_cr_end = cr >> 63;
#else
                uint64_t end = cmp_mask_against_input(in, 0x0a);
#endif
                fields[b] = (end | sep) & ~quote_mask;
            }
            for (size_t b = 0; b < CSV_BUFFER_SIZE; b++) {
                size_t internal_idx = 64 * b + idx;
                flatten_bits(base_ptr, base, internal_idx, fields[b]);
            }
        }
    }
    for (; idx < bufferable_length; idx += 64) {
        ARCTICDB_PREFETCH(buf + idx + 128);
        InputBlock in = fill_input(buf + idx);
        uint64_t quote_mask = find_quote_mask(in, prev_iter_inside_quote);
        uint64_t sep = cmp_mask_against_input(in, ',');
#ifdef CRLF
        uint64_t cr = cmp_mask_against_input(in, 0x0d);
        uint64_t cr_adjusted = (cr << 1) | prev_iter_cr_end;
        uint64_t lf = cmp_mask_against_input(in, 0x0a);
        uint64_t end = lf & cr_adjusted;
        prev_iter_cr_end = cr >> 63;
#else
        uint64_t end = cmp_mask_against_input(in, 0x0a);
#endif
        // note - a bit of a high-wire act here with quotes
        // we can't put something inside the quotes with the CR
        // then outside the quotes with LF so it's OK to "and off"
        // the quoted bits here. Some other quote convention would
        // need to be thought about carefully
        uint64_t field_sep = (end | sep) & ~quote_mask;
        flatten_bits(base_ptr, base, idx, field_sep);
    }
    parsed_output.n_indexes = base;
    return true;
}

} //namespace arcticdb