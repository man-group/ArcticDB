#ifndef BMAVX512__H__INCLUDED__
#define BMAVX512__H__INCLUDED__
/*
Copyright(c) 2002-2017 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

For more information please visit:  http://bitmagic.io
*/

// some of the algorithms here is based on modified libpopcnt library by Kim Walisch
// https://github.com/kimwalisch/libpopcnt/
//
/*
 * libpopcnt.h - C/C++ library for counting the number of 1 bits (bit
 * population count) in an array as quickly as possible using
 * specialized CPU instructions i.e. POPCNT, AVX2, AVX512, NEON.
 *
 * Copyright (c) 2016 - 2017, Kim Walisch
 * Copyright (c) 2016 - 2017, Wojciech Muła
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


/** @defgroup AVX512 AVX512 functions
    Processor specific optimizations for AVX2 instructions (internals)
    @ingroup bvector
    @internal
 */


// Header implements processor specific intrinsics declarations for AVX2
// instruction set
//
#include<emmintrin.h>
#include<immintrin.h>
///#include<zmmintrin.h>
#include<x86intrin.h>

#include "bmdef.h"
#include "bmbmi2.h"

namespace bm
{
/*
inline
void avx2_print256(const char* prefix, const __m256i & value)
{
    const size_t n = sizeof(__m256i) / sizeof(unsigned);
    unsigned buffer[n];
    _mm256_storeu_si256((__m256i*)buffer, value);
    std::cout << prefix << " [ ";
    for (int i = n-1; 1; --i)
    {
        std::cout << buffer[i] << " ";
        if (i == 0)
            break;
    }
    std::cout << "]" << std::endl;
}
*/

#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif


/**
    @return true if all zero
    @ingroup AVX512
*/
inline
bool avx512_test_zero(__m512i m)
{
     const __mmask16 m16F = __mmask16(~0u); // 0xFF
    __mmask16 eq_m = _mm512_cmpeq_epi32_mask(m, _mm512_set1_epi64(0ull));
    return (eq_m == m16F);
}

/**
    @return true if all one
    @ingroup AVX512
*/
inline
bool avx512_test_one(__m512i m)
{
     const __mmask16 m16F = __mmask16(~0u); // 0xFF
    __mmask16 eq_m = _mm512_cmpeq_epi32_mask(m, _mm512_set1_epi64(-1));
    return (eq_m == m16F);
}



#define BM_CSA256(h, l, a, b, c) \
{ \
    __m256i u = _mm256_xor_si256(a, b); \
    h = _mm256_or_si256(_mm256_and_si256(a, b), _mm256_and_si256(u, c)); \
    l = _mm256_xor_si256(u, c); \
}

#define BM_AVX2_BIT_COUNT(ret, v) \
{ \
    __m256i lo = _mm256_and_si256(v, low_mask); \
    __m256i hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), low_mask); \
    __m256i cnt1 = _mm256_shuffle_epi8(lookup1, lo); \
    __m256i cnt2 = _mm256_shuffle_epi8(lookup2, hi); \
    ret = _mm256_sad_epu8(cnt1, cnt2); \
} 

#define BM_AVX2_DECL_LOOKUP1 \
  __m256i lookup1 = _mm256_setr_epi8(4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8, \
                                     4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8);
#define BM_AVX2_DECL_LOOKUP2 \
__m256i lookup2 = _mm256_setr_epi8(4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1, 0, \
                                   4, 3, 3, 2, 3, 2, 2, 1, 3, 2, 2, 1, 2, 1, 1, 0);

#define BM_AVX2_POPCNT_PROLOG \
  BM_AVX2_DECL_LOOKUP1 \
  BM_AVX2_DECL_LOOKUP2 \
  __m256i low_mask = _mm256_set1_epi8(0x0f); \
  __m256i bc;

/*!
    @brief AVX2 Harley-Seal popcount
  The algorithm is based on the paper "Faster Population Counts
  using AVX2 Instructions" by Daniel Lemire, Nathan Kurz and
  Wojciech Mula (23 Nov 2016).
  @see https://arxiv.org/abs/1611.07612

  @ingroup AVX2
*/
inline
bm::id_t avx2_bit_count(const __m256i* BMRESTRICT block,
                        const __m256i* BMRESTRICT block_end)
{
  __m256i cnt = _mm256_setzero_si256();
  __m256i ones = _mm256_setzero_si256();
  __m256i twos = _mm256_setzero_si256();
  __m256i fours = _mm256_setzero_si256();
  __m256i eights = _mm256_setzero_si256();
  __m256i sixteens = _mm256_setzero_si256();
  __m256i twosA, twosB, foursA, foursB, eightsA, eightsB;
  __m256i b, c;

  BM_AVX2_POPCNT_PROLOG
  bm::id64_t* cnt64;

  do
  {
        b = _mm256_load_si256(block+0); c = _mm256_load_si256(block+1);
        BM_CSA256(twosA, ones, ones, b, c);
      
        b = _mm256_load_si256(block+2); c = _mm256_load_si256(block+3);
        BM_CSA256(twosB, ones, ones, b, c);
        BM_CSA256(foursA, twos, twos, twosA, twosB);
      
        b = _mm256_load_si256(block+4); c = _mm256_load_si256(block+5);
        BM_CSA256(twosA, ones, ones, b, c);
      
        b = _mm256_load_si256(block+6); c = _mm256_load_si256(block+7);
        BM_CSA256(twosB, ones, ones, b, c);
        BM_CSA256(foursB, twos, twos, twosA, twosB);
        BM_CSA256(eightsA, fours, fours, foursA, foursB);
      
        b = _mm256_load_si256(block+8); c = _mm256_load_si256(block+9);
        BM_CSA256(twosA, ones, ones, b, c);
      
        b = _mm256_load_si256(block+10); c = _mm256_load_si256(block+11);
        BM_CSA256(twosB, ones, ones, b, c);
        BM_CSA256(foursA, twos, twos, twosA, twosB);
      
        b = _mm256_load_si256(block+12); c = _mm256_load_si256(block+13);
        BM_CSA256(twosA, ones, ones, b, c);
      
        b = _mm256_load_si256(block+14); c = _mm256_load_si256(block+15);
        BM_CSA256(twosB, ones, ones, b, c);
        BM_CSA256(foursB, twos, twos, twosA, twosB);
        BM_CSA256(eightsB, fours, fours, foursA, foursB);
        BM_CSA256(sixteens, eights, eights, eightsA, eightsB);
        
        BM_AVX2_BIT_COUNT(bc, sixteens);
        cnt = _mm256_add_epi64(cnt, bc);

        block += 16;
  } while (block < block_end);
  
  cnt = _mm256_slli_epi64(cnt, 4);
  BM_AVX2_BIT_COUNT(bc, eights)
  cnt = _mm256_add_epi64(cnt, _mm256_slli_epi64(bc, 3));
  BM_AVX2_BIT_COUNT(bc, fours);
  cnt = _mm256_add_epi64(cnt, _mm256_slli_epi64(bc, 2));
  BM_AVX2_BIT_COUNT(bc, twos); 
  cnt = _mm256_add_epi64(cnt, _mm256_slli_epi64(bc, 1));
  BM_AVX2_BIT_COUNT(bc, ones);
  cnt = _mm256_add_epi64(cnt, bc);

  cnt64 = (bm::id64_t*) &cnt;

  return (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
}

/*!
  @brief AND bit count for two aligned bit-blocks
  @ingroup AVX2
*/
inline
bm::id_t avx2_bit_count_and(const __m256i* BMRESTRICT block,
                            const __m256i* BMRESTRICT block_end,
                            const __m256i* BMRESTRICT mask_block)
{
    bm::id64_t* cnt64;
    BM_AVX2_POPCNT_PROLOG;
    __m256i cnt = _mm256_setzero_si256();
    __m256i ymm0, ymm1;

    
    do
    {
        ymm0 = _mm256_load_si256(block);
        ymm1 = _mm256_load_si256(mask_block);
        ymm0 = _mm256_and_si256(ymm0, ymm1);
        ++block; ++mask_block;
        BM_AVX2_BIT_COUNT(bc, ymm0)
        cnt = _mm256_add_epi64(cnt, bc);

        ymm0 = _mm256_load_si256(block);
        ymm1 = _mm256_load_si256(mask_block);
        ymm0 = _mm256_and_si256(ymm0, ymm1);
        ++block; ++mask_block;
        BM_AVX2_BIT_COUNT(bc, ymm0)
        cnt = _mm256_add_epi64(cnt, bc);

        ymm0 = _mm256_load_si256(block);
        ymm1 = _mm256_load_si256(mask_block);
        ymm0 = _mm256_and_si256(ymm0, ymm1);
        ++block; ++mask_block;
        BM_AVX2_BIT_COUNT(bc, ymm0)
        cnt = _mm256_add_epi64(cnt, bc);

        ymm0 = _mm256_load_si256(block);
        ymm1 = _mm256_load_si256(mask_block);
        ymm0 = _mm256_and_si256(ymm0, ymm1);
        ++block; ++mask_block;
        BM_AVX2_BIT_COUNT(bc, ymm0)
        cnt = _mm256_add_epi64(cnt, bc);

    } while (block < block_end);

    cnt64 = (bm::id64_t*)&cnt;
    return (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
}

inline
bm::id_t avx2_bit_count_or(const __m256i* BMRESTRICT block,
    const __m256i* BMRESTRICT block_end,
    const __m256i* BMRESTRICT mask_block)
{
    bm::id64_t* cnt64;
    BM_AVX2_POPCNT_PROLOG;
    __m256i cnt = _mm256_setzero_si256();
    do
    {
        __m256i tmp0 = _mm256_load_si256(block);
        __m256i tmp1 = _mm256_load_si256(mask_block);

        tmp0 = _mm256_or_si256(tmp0, tmp1);

        BM_AVX2_BIT_COUNT(bc, tmp0)
        cnt = _mm256_add_epi64(cnt, bc);

        ++block; ++mask_block;

    } while (block < block_end);

    cnt64 = (bm::id64_t*)&cnt;
    return (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
}


/*!
  @brief XOR bit count for two aligned bit-blocks
  @ingroup AVX2
*/
inline
bm::id_t avx2_bit_count_xor(const __m256i* BMRESTRICT block,
                            const __m256i* BMRESTRICT block_end,
                            const __m256i* BMRESTRICT mask_block)
{
    bm::id64_t* cnt64;
    BM_AVX2_POPCNT_PROLOG
    __m256i cnt = _mm256_setzero_si256();
    __m256i mA, mB, mC, mD;
    do
    {
        mA = _mm256_xor_si256(_mm256_load_si256(block+0),
                              _mm256_load_si256(mask_block+0));
        BM_AVX2_BIT_COUNT(bc, mA)
        cnt = _mm256_add_epi64(cnt, bc);

        mB = _mm256_xor_si256(_mm256_load_si256(block+1),
                              _mm256_load_si256(mask_block+1));
        BM_AVX2_BIT_COUNT(bc, mB);
        cnt = _mm256_add_epi64(cnt, bc);

        mC = _mm256_xor_si256(_mm256_load_si256(block+2),
                              _mm256_load_si256(mask_block+2));
        BM_AVX2_BIT_COUNT(bc, mC);
        cnt = _mm256_add_epi64(cnt, bc);

        mD = _mm256_xor_si256(_mm256_load_si256(block+3),
                              _mm256_load_si256(mask_block+3));
        BM_AVX2_BIT_COUNT(bc, mD);
        cnt = _mm256_add_epi64(cnt, bc);
        
        block += 4; mask_block += 4;
        
    } while (block < block_end);

    cnt64 = (bm::id64_t*)&cnt;
    return (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
}



/*!
  @brief AND NOT bit count for two aligned bit-blocks
  @ingroup AVX2
*/
inline
bm::id_t avx2_bit_count_sub(const __m256i* BMRESTRICT block,
    const __m256i* BMRESTRICT block_end,
    const __m256i* BMRESTRICT mask_block)
{
    bm::id64_t* cnt64;
    BM_AVX2_POPCNT_PROLOG
    __m256i cnt = _mm256_setzero_si256();
    do
    {
        __m256i tmp0 = _mm256_load_si256(block);
        __m256i tmp1 = _mm256_load_si256(mask_block);

        tmp0 = _mm256_andnot_si256(tmp1, tmp0);

        BM_AVX2_BIT_COUNT(bc, tmp0)
        cnt = _mm256_add_epi64(cnt, bc);

        ++block; ++mask_block;

    } while (block < block_end);

    cnt64 = (bm::id64_t*)&cnt;
    return (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
}



/*!
    @brief XOR array elements to specified mask
    *dst = *src ^ mask

    @ingroup AVX512
*/
inline
void avx512_xor_arr_2_mask(__m512i* BMRESTRICT dst,
                           const __m512i* BMRESTRICT src,
                           const __m512i* BMRESTRICT src_end,
                           bm::word_t mask)
{
     __m512i yM = _mm512_set1_epi32(int(mask));
     do
     {
        _mm512_store_si512(dst+0, _mm512_xor_si512(_mm512_load_si512(src+0), yM)); // ymm1 = (~ymm1) & ymm2
        _mm512_store_si512(dst+1, _mm512_xor_si512(_mm512_load_si512(src+1), yM));
        _mm512_store_si512(dst+2, _mm512_xor_si512(_mm512_load_si512(src+2), yM));
        _mm512_store_si512(dst+3, _mm512_xor_si512(_mm512_load_si512(src+3), yM));
        
        dst += 4; src += 4;
     } while (src < src_end);
}


/*!
    @brief Inverts array elements and NOT them to specified mask
    *dst = ~*src & mask

    @ingroup AVX512
*/
inline
void avx512_andnot_arr_2_mask(__m512i* BMRESTRICT dst,
                            const __m512i* BMRESTRICT src,
                            const __m512i* BMRESTRICT src_end,
                            bm::word_t mask)
{
     __m512i yM = _mm512_set1_epi32(int(mask));
     do
     {
        _mm512_store_si512(dst+0, _mm512_andnot_si512(_mm512_load_si512(src+0), yM)); // ymm1 = (~ymm1) & ymm2
        _mm512_store_si512(dst+1, _mm512_andnot_si512(_mm512_load_si512(src+1), yM));
        _mm512_store_si512(dst+2, _mm512_andnot_si512(_mm512_load_si512(src+2), yM));
        _mm512_store_si512(dst+3, _mm512_andnot_si512(_mm512_load_si512(src+3), yM));
        
        dst += 4; src += 4;
     } while (src < src_end);
}

/*!
    @brief AND array elements against another array
    *dst &= *src
    @return 0 if destination does not have any bits
    @ingroup AVX512
*/
inline
unsigned avx512_and_block(__m512i* BMRESTRICT dst,
                          const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i accA, accB, accC, accD;

    const __m512i* BMRESTRICT src_end =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size);

    accA = accB = accC = accD = _mm512_setzero_si512();

    do
    {
        m1A = _mm512_and_si512(_mm512_load_si512(src+0), _mm512_load_si512(dst+0));
        m1B = _mm512_and_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));
        m1C = _mm512_and_si512(_mm512_load_si512(src+2), _mm512_load_si512(dst+2));
        m1D = _mm512_and_si512(_mm512_load_si512(src+3), _mm512_load_si512(dst+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);
        
        accA = _mm512_or_si512(accA, m1A);
        accB = _mm512_or_si512(accB, m1B);
        accC = _mm512_or_si512(accC, m1C);
        accD = _mm512_or_si512(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm512_or_si512(accA, accB); // A = A | B
    accC = _mm512_or_si512(accC, accD); // C = C | D
    accA = _mm512_or_si512(accA, accC); // A = A | C

    return !avx512_test_zero(accA);
}

/*!
    @brief AND block digest stride
    *dst &= *src
 
    @return true if stide is all zero
    @ingroup AVX512
*/
inline
bool avx512_and_digest(__m512i* BMRESTRICT dst,
                       const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B;
    
    m1A = _mm512_and_si512(_mm512_load_si512(src+0), _mm512_load_si512(dst+0));
    m1B = _mm512_and_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));

    _mm512_store_si512(dst+0, m1A);
    _mm512_store_si512(dst+1, m1B);
    
     m1A = _mm512_or_si512(m1A, m1B);
    
     return avx512_test_zero(m1A);
}

/*!
    @brief AND block digest stride 2 way
    *dst = *src1 & *src2
 
    @return true if stide is all zero
    @ingroup AVX512
*/
inline
bool avx512_and_digest_2way(__m512i* BMRESTRICT dst,
                            const __m512i* BMRESTRICT src1,
                            const __m512i* BMRESTRICT src2)
{
    __m512i m1A, m1B;

    m1A = _mm512_and_si512(_mm512_load_si512(src1+0), _mm512_load_si512(src2+0));
    m1B = _mm512_and_si512(_mm512_load_si512(src1+1), _mm512_load_si512(src2+1));

    _mm512_store_si512(dst+0, m1A);
    _mm512_store_si512(dst+1, m1B);
    
     m1A = _mm512_or_si512(m1A, m1B);

    return avx512_test_zero(m1A);
}



/*!
    @brief AND array elements against another array (unaligned)
    *dst &= *src
    @return 0 if destination does not have any bits
    @ingroup AVX2
*/
inline
unsigned avx2_and_arr_unal(__m256i* BMRESTRICT dst,
                  const __m256i* BMRESTRICT src,
                  const __m256i* BMRESTRICT src_end)
{
    __m256i m1A, m2A, m1B, m2B, m1C, m2C, m1D, m2D;
    __m256i accA, accB, accC, accD;
    
    accA = _mm256_setzero_si256();
    accB = _mm256_setzero_si256();
    accC = _mm256_setzero_si256();
    accD = _mm256_setzero_si256();

    do
    {
        m1A = _mm256_loadu_si256(src+0);
        m2A = _mm256_load_si256(dst+0);
        m1A = _mm256_and_si256(m1A, m2A);
        _mm256_store_si256(dst+0, m1A);
        accA = _mm256_or_si256(accA, m1A);
        
        m1B = _mm256_loadu_si256(src+1);
        m2B = _mm256_load_si256(dst+1);
        m1B = _mm256_and_si256(m1B, m2B);
        _mm256_store_si256(dst+1, m1B);
        accB = _mm256_or_si256(accB, m1B);

        m1C = _mm256_loadu_si256(src+2);
        m2C = _mm256_load_si256(dst+2);
        m1C = _mm256_and_si256(m1C, m2C);
        _mm256_store_si256(dst+2, m1C);
        accC = _mm256_or_si256(accC, m1C);

        m1D = _mm256_loadu_si256(src+3);
        m2D = _mm256_load_si256(dst+3);
        m1D = _mm256_and_si256(m1D, m2D);
        _mm256_store_si256(dst+3, m1D);
        accD = _mm256_or_si256(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm256_or_si256(accA, accB); // A = A | B
    accC = _mm256_or_si256(accC, accD); // C = C | D
    accA = _mm256_or_si256(accA, accC); // A = A | C
    
    return !_mm256_testz_si256(accA, accA);
}


/*!
    @brief OR array elements against another array
    *dst |= *src
    @return true if all bits are 1

    @ingroup AVX512
*/
inline
bool avx512_or_block(__m512i* BMRESTRICT dst,
                     const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B, m1C, m1D;
    
    __m512i mAccF0, mAccF1;
    mAccF0 = mAccF1 = _mm512_set1_epi32(~0u); // broadcast 0xFF
    
    __m512i* BMRESTRICT dst2 =
        (__m512i*)((bm::word_t*)(dst) + bm::set_block_size/2);
    const __m512i* BMRESTRICT src2 =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size/2);
    const __m512i* BMRESTRICT src_end =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size);
    do
    {
        m1A = _mm512_or_si512(_mm512_load_si512(src), _mm512_load_si512(dst));
        m1B = _mm512_or_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));
        mAccF0 = _mm512_and_si512(mAccF0, m1A);
        mAccF0 = _mm512_and_si512(mAccF0, m1B);
        
        _mm512_stream_si512(dst,   m1A);
        _mm512_stream_si512(dst+1, m1B);

        src += 2; dst += 2;

        m1C = _mm512_or_si512(_mm512_load_si512(src2), _mm512_load_si512(dst2));
        m1D = _mm512_or_si512(_mm512_load_si512(src2+1), _mm512_load_si512(dst2+1));
        mAccF1 = _mm512_and_si512(mAccF1, m1C);
        mAccF1 = _mm512_and_si512(mAccF1, m1D);
        
        _mm512_stream_si512(dst2, m1C);
        _mm512_stream_si512(dst2+1, m1D);

        src2 += 2; dst2 += 2;
    } while (src2 < src_end);

     mAccF0 = _mm512_and_si512(mAccF0, mAccF1);
    
    return avx512_test_one(mAccF0);
}


/*!
    @brief OR array elements against another unaligned array
    *dst |= *src
    @return true if all bits are 1

    @ingroup AVX2
*/
inline
bool avx2_or_arr_unal(__m256i* BMRESTRICT dst,
                      const __m256i* BMRESTRICT src,
                      const __m256i* BMRESTRICT src_end)
{
    __m256i m1A, m2A, m1B, m2B, m1C, m2C, m1D, m2D;
    __m256i mAccF0 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    __m256i mAccF1 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    do
    {
        m1A = _mm256_loadu_si256(src+0);
        m2A = _mm256_load_si256(dst+0);
        m1A = _mm256_or_si256(m1A, m2A);
        _mm256_store_si256(dst+0, m1A);
        
        m1B = _mm256_loadu_si256(src+1);
        m2B = _mm256_load_si256(dst+1);
        m1B = _mm256_or_si256(m1B, m2B);
        _mm256_store_si256(dst+1, m1B);

        m1C = _mm256_loadu_si256(src+2);
        m2C = _mm256_load_si256(dst+2);
        m1C = _mm256_or_si256(m1C, m2C);
        _mm256_store_si256(dst+2, m1C);

        m1D = _mm256_loadu_si256(src+3);
        m2D = _mm256_load_si256(dst+3);
        m1D = _mm256_or_si256(m1D, m2D);
        _mm256_store_si256(dst+3, m1D);

        mAccF1 = _mm256_and_si256(mAccF1, m1C);
        mAccF1 = _mm256_and_si256(mAccF1, m1D);
        mAccF0 = _mm256_and_si256(mAccF0, m1A);
        mAccF0 = _mm256_and_si256(mAccF0, m1B);

        src += 4; dst += 4;

    } while (src < src_end);

    __m256i maskF = _mm256_set1_epi32(~0u);
    mAccF0 = _mm256_and_si256(mAccF0, mAccF1);
    __m256i wcmpA = _mm256_cmpeq_epi8(mAccF0, maskF);
    unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
    return (maskA == ~0u);
}

/*!
    @brief OR 2 blocks, copy to destination
    *dst = *src1 | src2
    @return true if all bits are 1

    @ingroup AVX512
*/
inline
bool avx512_or_block_2way(__m512i* BMRESTRICT dst,
                        const __m512i* BMRESTRICT src1,
                        const __m512i* BMRESTRICT src2)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i mAccF0, mAccF1;
    
    mAccF0 = mAccF1 = _mm512_set1_epi32(~0u); // broadcast 0xFF
    const __m512i* BMRESTRICT src_end1 =
        (const __m512i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm512_or_si512(_mm512_load_si512(src1+0), _mm512_load_si512(src2+0));
        m1B = _mm512_or_si512(_mm512_load_si512(src1+1), _mm512_load_si512(src2+1));
        m1C = _mm512_or_si512(_mm512_load_si512(src1+2), _mm512_load_si512(src2+2));
        m1D = _mm512_or_si512(_mm512_load_si512(src1+3), _mm512_load_si512(src2+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);

        mAccF1 = _mm512_and_si512(mAccF1, m1C);
        mAccF1 = _mm512_and_si512(mAccF1, m1D);
        mAccF0 = _mm512_and_si512(mAccF0, m1A);
        mAccF0 = _mm512_and_si512(mAccF0, m1B);

        src1 += 4; src2 += 4; dst += 4;

    } while (src1 < src_end1);

     mAccF0 = _mm512_and_si512(mAccF0, mAccF1);
    return avx512_test_one(mAccF0);
}


/*!
    @brief OR array elements against another 2 arrays
    *dst |= *src1 | src2
    @return true if all bits are 1

    @ingroup AVX512
*/
inline
bool avx512_or_block_3way(__m512i* BMRESTRICT dst,
                        const __m512i* BMRESTRICT src1,
                        const __m512i* BMRESTRICT src2)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i mAccF0, mAccF1;
    
    mAccF0 = mAccF1 = _mm512_set1_epi32(~0u); // broadcast 0xFF
    const __m512i* BMRESTRICT src_end1 =
        (const __m512i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm512_or_si512(_mm512_load_si512(src1+0), _mm512_load_si512(dst+0));
        m1B = _mm512_or_si512(_mm512_load_si512(src1+1), _mm512_load_si512(dst+1));
        m1C = _mm512_or_si512(_mm512_load_si512(src1+2), _mm512_load_si512(dst+2));
        m1D = _mm512_or_si512(_mm512_load_si512(src1+3), _mm512_load_si512(dst+3));

        m1A = _mm512_or_si512(m1A, _mm512_load_si512(src2+0));
        m1B = _mm512_or_si512(m1B, _mm512_load_si512(src2+1));
        m1C = _mm512_or_si512(m1C, _mm512_load_si512(src2+2));
        m1D = _mm512_or_si512(m1D, _mm512_load_si512(src2+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);

        mAccF1 = _mm512_and_si512(mAccF1, m1C);
        mAccF1 = _mm512_and_si512(mAccF1, m1D);
        mAccF0 = _mm512_and_si512(mAccF0, m1A);
        mAccF0 = _mm512_and_si512(mAccF0, m1B);

        src1 += 4; src2 += 4; dst += 4;

    } while (src1 < src_end1);

     mAccF0 = _mm512_and_si512(mAccF0, mAccF1);
    return avx512_test_one(mAccF0);
}


/*!
    @brief OR array elements against another 4 arrays
    *dst |= *src1 | src2
    @return true if all bits are 1

    @ingroup AVX512
*/
inline
bool avx512_or_block_5way(__m512i* BMRESTRICT dst,
                        const __m512i* BMRESTRICT src1,
                        const __m512i* BMRESTRICT src2,
                        const __m512i* BMRESTRICT src3,
                        const __m512i* BMRESTRICT src4)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i mAccF0, mAccF1;
    mAccF0 = mAccF1 = _mm512_set1_epi32(~0u); // broadcast 0xFF

    const __m512i* BMRESTRICT src_end1 =
        (const __m512i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm512_or_si512(_mm512_load_si512(src1+0), _mm512_load_si512(dst+0));
        m1B = _mm512_or_si512(_mm512_load_si512(src1+1), _mm512_load_si512(dst+1));
        m1C = _mm512_or_si512(_mm512_load_si512(src1+2), _mm512_load_si512(dst+2));
        m1D = _mm512_or_si512(_mm512_load_si512(src1+3), _mm512_load_si512(dst+3));

        m1A = _mm512_or_si512(m1A, _mm512_load_si512(src2+0));
        m1B = _mm512_or_si512(m1B, _mm512_load_si512(src2+1));
        m1C = _mm512_or_si512(m1C, _mm512_load_si512(src2+2));
        m1D = _mm512_or_si512(m1D, _mm512_load_si512(src2+3));

        m1A = _mm512_or_si512(m1A, _mm512_load_si512(src3+0));
        m1B = _mm512_or_si512(m1B, _mm512_load_si512(src3+1));
        m1C = _mm512_or_si512(m1C, _mm512_load_si512(src3+2));
        m1D = _mm512_or_si512(m1D, _mm512_load_si512(src3+3));

        m1A = _mm512_or_si512(m1A, _mm512_load_si512(src4+0));
        m1B = _mm512_or_si512(m1B, _mm512_load_si512(src4+1));
        m1C = _mm512_or_si512(m1C, _mm512_load_si512(src4+2));
        m1D = _mm512_or_si512(m1D, _mm512_load_si512(src4+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);

        mAccF1 = _mm512_and_si512(mAccF1, m1C);
        mAccF1 = _mm512_and_si512(mAccF1, m1D);
        mAccF0 = _mm512_and_si512(mAccF0, m1A);
        mAccF0 = _mm512_and_si512(mAccF0, m1B);

        src1 += 4; src2 += 4;
        src3 += 4; src4 += 4;
        _mm_prefetch ((const char*)src3, _MM_HINT_T0);
        _mm_prefetch ((const char*)src4, _MM_HINT_T0);

        dst += 4;

    } while (src1 < src_end1);

     mAccF0 = _mm512_and_si512(mAccF0, mAccF1);
    return avx512_test_one(mAccF0);
}


/*!
    @brief XOR block against another
    *dst ^= *src
    @return 0 if destination does not have any bits
    @ingroup AVX512
*/
inline
unsigned avx512_xor_block(__m512i* BMRESTRICT dst,
                          const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i accA, accB, accC, accD;

    const __m512i* BMRESTRICT src_end =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size);

    accA = accB = accC = accD = _mm512_setzero_si512();

    do
    {
        m1A = _mm512_xor_si512(_mm512_load_si512(src+0), _mm512_load_si512(dst+0));
        m1B = _mm512_xor_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));
        m1C = _mm512_xor_si512(_mm512_load_si512(src+2), _mm512_load_si512(dst+2));
        m1D = _mm512_xor_si512(_mm512_load_si512(src+3), _mm512_load_si512(dst+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);
        
        accA = _mm512_or_si512(accA, m1A);
        accB = _mm512_or_si512(accB, m1B);
        accC = _mm512_or_si512(accC, m1C);
        accD = _mm512_or_si512(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm512_or_si512(accA, accB); // A = A | B
    accC = _mm512_or_si512(accC, accD); // C = C | D
    accA = _mm512_or_si512(accA, accC); // A = A | C

    return !avx512_test_zero(accA);
}

/*!
    @brief 3-operand XOR     
    *dst = *src1 ^ *src2
    @return 0 if destination does not have any bits
    @ingroup AVX512
*/
inline
unsigned avx512_xor_block_2way(__m512i* BMRESTRICT dst,
                               const __m512i* BMRESTRICT src1,
                               const __m512i* BMRESTRICT src2)
{
    __m512i m1A, m1B, m1C, m1D;
    __m512i accA, accB, accC, accD;

    const __m512i* BMRESTRICT src1_end =
        (const __m512i*)((bm::word_t*)(src1) + bm::set_block_size);

    accA = accB = accC = accD = _mm512_setzero_si512();

    do
    {
        m1A = _mm512_xor_si512(_mm512_load_si512(src1 + 0), _mm512_load_si512(src2 + 0));
        m1B = _mm512_xor_si512(_mm512_load_si512(src1 + 1), _mm512_load_si512(src2 + 1));
        m1C = _mm512_xor_si512(_mm512_load_si512(src1 + 2), _mm512_load_si512(src2 + 2));
        m1D = _mm512_xor_si512(_mm512_load_si512(src1 + 3), _mm512_load_si512(src2 + 3));

        _mm512_store_si512(dst + 0, m1A);
        _mm512_store_si512(dst + 1, m1B);
        _mm512_store_si512(dst + 2, m1C);
        _mm512_store_si512(dst + 3, m1D);

        accA = _mm512_or_si512(accA, m1A);
        accB = _mm512_or_si512(accB, m1B);
        accC = _mm512_or_si512(accC, m1C);
        accD = _mm512_or_si512(accD, m1D);

        src1 += 4; src2 += 4; dst += 4;

    } while (src1 < src1_end);

    accA = _mm512_or_si512(accA, accB); // A = A | B
    accC = _mm512_or_si512(accC, accD); // C = C | D
    accA = _mm512_or_si512(accA, accC); // A = A | C

    return !avx512_test_zero(accA);
}


/*!
    @brief AND-NOT (SUB) array elements against another array
    *dst &= ~*src
 
    @return 0 if destination does not have any bits

    @ingroup AVX512
*/
inline
unsigned avx512_sub_block(__m512i* BMRESTRICT dst,
                          const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B,  m1C, m1D;
    __m512i accA, accB, accC, accD;
    
    accA = accB = accC = accD = _mm512_setzero_si512();

    const __m512i* BMRESTRICT src_end =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        m1A = _mm512_andnot_si512(_mm512_load_si512(src), _mm512_load_si512(dst));
        m1B = _mm512_andnot_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));
        m1C = _mm512_andnot_si512(_mm512_load_si512(src+2), _mm512_load_si512(dst+2));
        m1D = _mm512_andnot_si512(_mm512_load_si512(src+3), _mm512_load_si512(dst+3));

        _mm512_store_si512(dst+0, m1A);
        _mm512_store_si512(dst+1, m1B);
        _mm512_store_si512(dst+2, m1C);
        _mm512_store_si512(dst+3, m1D);
        
        accA = _mm512_or_si512(accA, m1A);
        accB = _mm512_or_si512(accB, m1B);
        accC = _mm512_or_si512(accC, m1C);
        accD = _mm512_or_si512(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm512_or_si512(accA, accB); // A = A | B
    accC = _mm512_or_si512(accC, accD); // C = C | D
    accA = _mm512_or_si512(accA, accC); // A = A | C
    
    return !avx512_test_zero(accA);
}

/*!
    @brief SUB (AND NOT) block digest stride
    *dst &= *src
 
    @return true if stide is all zero
    @ingroup AVX512
*/
inline
bool avx512_sub_digest(__m512i* BMRESTRICT dst,
                       const __m512i* BMRESTRICT src)
{
    __m512i m1A, m1B;

    m1A = _mm512_andnot_si512(_mm512_load_si512(src+0), _mm512_load_si512(dst+0));
    m1B = _mm512_andnot_si512(_mm512_load_si512(src+1), _mm512_load_si512(dst+1));

    _mm512_store_si512(dst+0, m1A);
    _mm512_store_si512(dst+1, m1B);
    
     m1A = _mm512_or_si512(m1A, m1B);

    return avx512_test_zero(m1A);
}


/*!
    @brief AVX512 block memset
    *dst = value

    @ingroup AVX512
*/
BMFORCEINLINE
void avx512_set_block(__m512i* BMRESTRICT dst, bm::word_t value)
{
    __m512i* BMRESTRICT dst_end =
        (__m512i*)((bm::word_t*)(dst) + bm::set_block_size);

    __m512i zmm0 = _mm512_set1_epi32(int(value));
    do
    {
        _mm512_store_si512(dst,   zmm0);
        _mm512_store_si512(dst+1, zmm0);
        _mm512_store_si512(dst+2, zmm0);
        _mm512_store_si512(dst+3, zmm0);
        
        dst += 4;
    } while (dst < dst_end);
}



/*!
    @brief  block copy
    *dst = *src

    @ingroup AVX512
*/
inline
void avx512_copy_block(__m512i* BMRESTRICT dst,
                     const __m512i* BMRESTRICT src)
{
    __m512i ymm0, ymm1, ymm2, ymm3;

    const __m512i* BMRESTRICT src_end =
        (const __m512i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        ymm0 = _mm512_load_si512(src+0);
        ymm1 = _mm512_load_si512(src+1);
        ymm2 = _mm512_load_si512(src+2);
        ymm3 = _mm512_load_si512(src+3);
        
        _mm512_store_si512(dst+0, ymm0);
        _mm512_store_si512(dst+1, ymm1);
        _mm512_store_si512(dst+2, ymm2);
        _mm512_store_si512(dst+3, ymm3);
        
        src += 4; dst += 4;

    } while (src < src_end);
}

/*!
    @brief Invert bit-block
    *dst = ~*dst
    or
    *dst ^= *dst

    @ingroup AVX512
*/
inline
void avx512_invert_block(__m512i* BMRESTRICT dst)
{
    __m512i maskFF = _mm512_set1_epi64(-1); // broadcast 0xFF
    const __m512i* BMRESTRICT dst_end =
        (const __m512i*)((bm::word_t*)(dst) + bm::set_block_size);

    __m512i ymm0, ymm1;
    do
    {
        ymm0 = _mm512_xor_si512(_mm512_load_si512(dst+0), maskFF);
        ymm1 = _mm512_xor_si512(_mm512_load_si512(dst+1), maskFF);

        _mm512_store_si512(dst+0, ymm0);
        _mm512_store_si512(dst+1, ymm1);

        ymm0 = _mm512_xor_si512(_mm512_load_si512(dst+2), maskFF);
        ymm1 = _mm512_xor_si512(_mm512_load_si512(dst+3), maskFF);
        
        _mm512_store_si512(dst+2, ymm0);
        _mm512_store_si512(dst+3, ymm1);
        
        dst += 4;
        
    } while (dst < dst_end);
}

/*!
    @brief check if block is all zero bits
    @ingroup AVX512
*/
inline
bool avx512_is_all_zero(const __m512i* BMRESTRICT block)
{
    const __m512i* BMRESTRICT block_end =
        (const __m512i*)((bm::word_t*)(block) + bm::set_block_size);
    do
    {
        __m512i w0 = _mm512_load_si512(block+0);
        __m512i w1 = _mm512_load_si512(block+1);

        __m512i wA = _mm512_or_si512(w0, w1);
        
        __m512i w2 = _mm512_load_si512(block+2);
        __m512i w3 = _mm512_load_si512(block+3);

        __m512i wB = _mm512_or_si512(w2, w3);
        wA = _mm512_or_si512(wA, wB);
        
        bool z = avx512_test_zero(wA);
        if (!z)
            return false;

        block += 4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if digest stride is all zero bits
    @ingroup AVX512
*/
inline
bool avx512_is_digest_zero(const __m512i* BMRESTRICT block)
{
    __m512i mA =
        _mm512_or_si512(_mm512_load_si512(block+0),
                        _mm512_load_si512(block+1));
    return avx512_test_zero(mA);
}


/*!
    @brief check if block is all one bits
    @return true if all bits are 1
    @ingroup AVX512
*/
inline
bool avx512_is_all_one(const __m512i* BMRESTRICT block)
{
     const __mmask16 m16F = __mmask16(~0u); // 0xFF

    __m512i maskF = _mm512_set1_epi64(-1); //  0xFF
    const __m512i* BMRESTRICT block_end =
        (const __m512i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        __mmask16 eq_m = _mm512_cmpeq_epi32_mask(_mm512_load_si512(block), maskF);
        if (eq_m != m16F)
            return false;
        
        eq_m = _mm512_cmpeq_epi32_mask(_mm512_load_si512(block+1), maskF);
        if (eq_m != m16F)
            return false;
        
        block += 2;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if wave of pointers is all NULL
    @ingroup AVX2
*/
BMFORCEINLINE
bool avx2_test_all_zero_wave(const void* ptr)
{
    __m256i w0 = _mm256_loadu_si256((__m256i*)ptr);
    return _mm256_testz_si256(w0, w0);
}


/*!
    @brief check if 2 wave of pointers are all NULL
    @ingroup AVX2
*/
BMFORCEINLINE
bool avx2_test_all_zero_wave2(const void* ptr0, const void* ptr1)
{
    __m256i w0 = _mm256_loadu_si256((__m256i*)ptr0);
    __m256i w1 = _mm256_loadu_si256((__m256i*)ptr1);
    w0 = _mm256_or_si256(w0, w1);
    return _mm256_testz_si256(w0, w0);
}

/*!
    @brief check if 2 wave of pointers are all the same (NULL or FULL)
    @ingroup AVX2
*/
BMFORCEINLINE
bool avx2_test_all_eq_wave2(const void* ptr0, const void* ptr1)
{
    __m256i w0 = _mm256_loadu_si256((__m256i*)ptr0);
    __m256i w1 = _mm256_loadu_si256((__m256i*)ptr1);
    w0 = _mm256_xor_si256(w0, w1);
    return _mm256_testz_si256(w0, w0);
}



/*!
    SSE4.2 optimized bitcounting and number of GAPs
    @ingroup SSE4
*/
inline
bm::id_t sse42_bit_block_calc_count_change(const __m128i* BMRESTRICT block,
                                          const __m128i* BMRESTRICT block_end,
                                               unsigned* BMRESTRICT bit_count)
{
//   __m128i mask1 = _mm_set_epi32(0x1, 0x1, 0x1, 0x1);
   unsigned count = (unsigned)(block_end - block)*4;

   bm::word_t  w0, w_prev;
   const int w_shift = sizeof(w0) * 8 - 1;
   bool first_word = true;
   *bit_count = 0;
 
   // first word
   {
       bm::word_t  w;
       const bm::word_t* blk = (const bm::word_t*) block;
       w = w0 = blk[0];
       *bit_count += unsigned(_mm_popcnt_u32(w));
       w ^= (w >> 1);
       count += unsigned(_mm_popcnt_u32(w));
       count -= (w_prev = (w0 >> w_shift));
   }

   do
   {
       __m128i b = _mm_load_si128(block);
       __m128i tmp2 = _mm_xor_si128(b, _mm_srli_epi32(b, 1)); // tmp2=(b >> 1) ^ b;
       __m128i tmp3 = _mm_srli_epi32(b, w_shift); // tmp3 = w0 >> w_shift
//       __m128i tmp4 = _mm_and_si128(b, mask1);    // tmp4 = w0 & 1

       // ---------------------------------------------------------------------
       {
           if (first_word)
           {
               first_word = false;
           }
           else
           {
               w0 = unsigned(_mm_extract_epi32(b, 0));
               if (w0)
               {
                   *bit_count += unsigned(_mm_popcnt_u32(w0));
                   count += unsigned(_mm_popcnt_u32((unsigned)_mm_extract_epi32(tmp2, 0)));
                   count -= !(w_prev ^ (w0 & 1));
                   count -= w_prev = unsigned(_mm_extract_epi32(tmp3, 0));
               }
               else
               {
                   count -= !w_prev; w_prev ^= w_prev;
               }
           }
           w0 = unsigned(_mm_extract_epi32(b, 1));
           if (w0)
           {
               *bit_count += unsigned(_mm_popcnt_u32(w0));
               count += unsigned(_mm_popcnt_u32((unsigned)_mm_extract_epi32(tmp2, 1)));
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = unsigned(_mm_extract_epi32(tmp3, 1));
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }
           w0 = unsigned(_mm_extract_epi32(b, 2));
           if (w0)
           {
               *bit_count += unsigned(_mm_popcnt_u32(w0));
               count += unsigned(_mm_popcnt_u32((unsigned)_mm_extract_epi32(tmp2, 2)));
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = unsigned(_mm_extract_epi32(tmp3, 2));
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }
           w0 = unsigned(_mm_extract_epi32(b, 3));
           if (w0)
           {
               *bit_count += unsigned(_mm_popcnt_u32(w0));
               count += unsigned(_mm_popcnt_u32((unsigned)_mm_extract_epi32(tmp2, 3)));
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = unsigned(_mm_extract_epi32(tmp3, 3));
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }
       }
   } while (++block < block_end);

   return count;
}



/* @brief Gap block population count (array sum) utility
   @param pbuf - unrolled, aligned to 1-start GAP buffer
   @param avx_vect_waves - number of AVX vector lines to process
   @param sum - result acumulator
   @return tail pointer

   @internal
*/
inline
const bm::gap_word_t* avx2_gap_sum_arr(const bm::gap_word_t*  pbuf,
                                       unsigned               avx_vect_waves,
                                       unsigned*              sum)
{
    __m256i xcnt = _mm256_setzero_si256();

    // accumulate odd and even elements of the vector the result is 
    // correct based on modulus 16 (max element value in gap blocks is 65535)
    // overflow is not an issue here
    for (unsigned i = 0; i < avx_vect_waves; ++i)
    {
        __m256i ymm0 = _mm256_loadu_si256((__m256i*)(pbuf - 1));
        __m256i ymm1 = _mm256_loadu_si256((__m256i*)(pbuf + 16 - 1));
        __m256i ymm_s2 = _mm256_add_epi16(ymm1, ymm0);
        xcnt = _mm256_add_epi16(xcnt, ymm_s2);
        pbuf += 32;
    }
    // odd minus even vector elements clears the result for 1111 blocks
    // bsrli - byte shifts the vector element by 2 bytes (1 short int)
    xcnt = _mm256_sub_epi16(_mm256_bsrli_epi128(xcnt, 2), xcnt);

    // horizontal sum of vector elements
    // cnt16[0] + cnt16[2] + cnt16[4] + cnt16[6] + cnt16[8] + cnt16[10] + cnt16[12] + cnt16[14];
    //
    xcnt = _mm256_add_epi16(_mm256_bsrli_epi128(xcnt, 4), xcnt);
    xcnt = _mm256_add_epi16(_mm256_bsrli_epi128(xcnt, 8), xcnt);
    __m128i xcnt2 = _mm_add_epi16(_mm256_extracti128_si256(xcnt, 1), _mm256_extracti128_si256(xcnt, 0));

    // extract 32-bit word and mask to take first 16 bits
    *sum += _mm_cvtsi128_si32(xcnt2) & 0xffff;
    return pbuf;
}


/*!
     AVX2 index lookup to check what belongs to the same block (8 elements)
     \internal
*/
inline
unsigned avx2_idx_arr_block_lookup(const unsigned* idx, unsigned size,
                                   unsigned nb, unsigned start)
{
    const unsigned unroll_factor = 16;
    const unsigned len = (size - start);
    const unsigned len_unr = len - (len % unroll_factor);
    unsigned k;

    idx += start;

    __m256i nbM = _mm256_set1_epi32(int(nb));

    for (k = 0; k < len_unr; k+=unroll_factor)
    {
        __m256i idxA = _mm256_loadu_si256((__m256i*)(idx+k));
        __m256i nbA =  _mm256_srli_epi32(idxA, bm::set_block_shift); // idx[k] >> bm::set_block_shift

        __m256i wcmpA= _mm256_cmpeq_epi8(nbM, nbA);
        if (~0u != unsigned(_mm256_movemask_epi8(wcmpA)))
            break;
        __m256i idxB = _mm256_loadu_si256((__m256i*)(idx+k+8));
        __m256i nbB =  _mm256_srli_epi32(idxB, bm::set_block_shift);

        __m256i wcmpB = _mm256_cmpeq_epi8(nbM, nbB);
        if (~0u != unsigned(_mm256_movemask_epi8(wcmpB)))
            break;
    } // for k
    for (; k < len; ++k)
    {
        if (nb != unsigned(idx[k] >> bm::set_block_shift))
            break;
    }
    return start + k;
}



/*!
     AVX2 bit block gather-scatter
 
     @param arr - destination array to set bits
     @param blk - source bit-block
     @param idx - gather index array
     @param size - gather array size
     @param start - gaher start index
     @param bit_idx - bit to set in the target array
 
     \internal

    C algorithm:
 
    for (unsigned k = start; k < size; ++k)
    {
        nbit = unsigned(idx[k] & bm::set_block_mask);
        nword  = unsigned(nbit >> bm::set_word_shift);
        mask0 = 1u << (nbit & bm::set_word_mask);
        arr[k] |= TRGW(bool(blk[nword] & mask0) << bit_idx);
    }

*/
inline
void avx2_bit_block_gather_scatter(unsigned* BMRESTRICT arr,
                                   const unsigned* BMRESTRICT blk,
                                   const unsigned* BMRESTRICT idx,
                                   unsigned                   size,
                                   unsigned                   start,
                                   unsigned                   bit_idx)
{
    const unsigned unroll_factor = 8;
    const unsigned len = (size - start);
    const unsigned len_unr = len - (len % unroll_factor);
    
    __m256i sb_mask = _mm256_set1_epi32(bm::set_block_mask);
    __m256i sw_mask = _mm256_set1_epi32(bm::set_word_mask);
    __m256i maskFF  = _mm256_set1_epi32(~0u);

    __m256i mask_tmp, mask_0;
    
    unsigned BM_ALIGN32 mword_v[8] BM_ALIGN32ATTR;

    unsigned k = 0, mask, w_idx;
    for (; k < len_unr; k+=unroll_factor)
    {
        __m256i nbitA, nwordA;
        const unsigned base = start + k;
        __m256i* idx_ptr = (__m256i*)(idx+base);   // idx[base]
        
        nbitA = _mm256_and_si256 (_mm256_loadu_si256(idx_ptr), sb_mask); // nbit = idx[base] & bm::set_block_mask
        nwordA = _mm256_srli_epi32 (nbitA, bm::set_word_shift); // nword  = nbit >> bm::set_word_shift

        // shufffle + permute to prepare comparison vector
        mask_tmp = _mm256_shuffle_epi32 (nwordA, _MM_SHUFFLE(1,1,1,1));
        mask_tmp = _mm256_permute2x128_si256 (mask_tmp, mask_tmp, 0);
        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(mask_tmp, nwordA));
        _mm256_store_si256((__m256i*)mword_v, nwordA);

        if (mask == ~0u) // all idxs belongs the same word avoid (costly) gather
        {
            w_idx = mword_v[0];
            mask_tmp = _mm256_set1_epi32(blk[w_idx]); // use broadcast
        }
        else // gather for: blk[nword]  (.. & mask0 )
        {
            mask_tmp = _mm256_set_epi32(blk[mword_v[7]], blk[mword_v[6]],
                                        blk[mword_v[5]], blk[mword_v[4]],
                                        blk[mword_v[3]], blk[mword_v[2]],
                                        blk[mword_v[1]], blk[mword_v[0]]);
        }
        
        // mask0 = 1u << (nbit & bm::set_word_mask);
        //
        __m256i shiftA = _mm256_and_si256 (nbitA, sw_mask);
        __m256i mask1  = _mm256_srli_epi32 (maskFF, 31);
        mask_0 = _mm256_sllv_epi32(mask1, shiftA);
        
        mask_tmp = _mm256_and_si256(mask_tmp, mask_0);
        if (!_mm256_testz_si256(mask_tmp, mask_tmp)) // AND tests empty
        {
            __m256i* target_ptr = (__m256i*)(arr+base); // arr[base]
            // bool(blk[nword]  ... )
            __m256i maskZ   = _mm256_xor_si256(maskFF, maskFF); // all zero
            mask1 = _mm256_slli_epi32(mask1, bit_idx); // << bit_idx
            mask_tmp = _mm256_cmpeq_epi32 (mask_tmp, maskZ); // set 0xFF if==0
            mask_tmp = _mm256_xor_si256 (mask_tmp, maskFF);  // invert
            mask_tmp = _mm256_and_si256 (mask_tmp, mask1);
            _mm256_storeu_si256 (target_ptr,          // arr[base] |= MASK_EXPR
                         _mm256_or_si256 (mask_tmp,
                                          _mm256_loadu_si256(target_ptr)));
        }
        
    } // for
    
    for (; k < len; ++k)
    {
        const unsigned base = start + k;
        unsigned nbit = unsigned(idx[base] & bm::set_block_mask);
        arr[base] |= unsigned(bool(blk[nbit >> bm::set_word_shift] & (1u << (nbit & bm::set_word_mask))) << bit_idx);
    }

}

#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


#define VECT_XOR_ARR_2_MASK(dst, src, src_end, mask)\
    avx512_xor_arr_2_mask((__m512i*)(dst), (__m512i*)(src), (__m512i*)(src_end), (bm::word_t)mask)

#define VECT_ANDNOT_ARR_2_MASK(dst, src, src_end, mask)\
    avx512_andnot_arr_2_mask((__m512i*)(dst), (__m512i*)(src), (__m512i*)(src_end), (bm::word_t)mask)

#define VECT_BITCOUNT(first, last) \
    avx2_bit_count((__m256i*) (first), (__m256i*) (last))

#define VECT_BITCOUNT_AND(first, last, mask) \
    avx2_bit_count_and((__m256i*) (first), (__m256i*) (last), (__m256i*) (mask))

#define VECT_BITCOUNT_OR(first, last, mask) \
    avx2_bit_count_or((__m256i*) (first), (__m256i*) (last), (__m256i*) (mask))

#define VECT_BITCOUNT_XOR(first, last, mask) \
    avx2_bit_count_xor((__m256i*) (first), (__m256i*) (last), (__m256i*) (mask))

#define VECT_BITCOUNT_SUB(first, last, mask) \
    avx2_bit_count_sub((__m256i*) (first), (__m256i*) (last), (__m256i*) (mask))

#define VECT_INVERT_BLOCK(first) \
    avx512_invert_block((__m512i*)first);

#define VECT_AND_BLOCK(dst, src) \
    avx512_and_block((__m512i*) dst, (const __m512i*) (src))

#define VECT_AND_DIGEST(dst, src) \
    avx512_and_digest((__m512i*) dst, (const __m512i*) (src))

#define VECT_AND_DIGEST_2WAY(dst, src1, src2) \
    avx512_and_digest_2way((__m512i*) dst, (const __m512i*) (src1), (const __m512i*) (src2))

#define VECT_OR_BLOCK(dst, src) \
    avx512_or_block((__m512i*) dst, (__m512i*) (src))

#define VECT_OR_BLOCK_2WAY(dst, src1, src2) \
    avx512_or_block_2way((__m512i*) dst, (__m512i*) (src1), (__m512i*) (src2))

#define VECT_OR_BLOCK_3WAY(dst, src1, src2) \
    avx512_or_block_3way((__m512i*) dst, (__m512i*) (src1), (__m512i*) (src2))

#define VECT_OR_BLOCK_5WAY(dst, src1, src2, src3, src4) \
    avx512_or_block_5way((__m512i*) dst, (__m512i*) (src1), (__m512i*) (src2), (__m512i*) (src3), (__m512i*) (src4))

#define VECT_SUB_BLOCK(dst, src) \
    avx512_sub_block((__m512i*) dst, (__m512i*) (src))

#define VECT_SUB_DIGEST(dst, src) \
    avx512_sub_digest((__m512i*) dst, (const __m512i*) (src))

#define VECT_XOR_BLOCK(dst, src) \
    avx512_xor_block((__m512i*) dst, (__m512i*) (src))

#define VECT_XOR_BLOCK_2WAY(dst, src1, src2) \
    avx512_xor_block_2way((__m512i*) dst, (__m512i*) (src1), (__m512i*) (src2))

#define VECT_COPY_BLOCK(dst, src) \
    avx512_copy_block((__m512i*) dst, (__m512i*) (src))

#define VECT_SET_BLOCK(dst, value) \
    avx512_set_block((__m512i*) dst, (value))

#define VECT_IS_ZERO_BLOCK(dst) \
    avx512_is_all_zero((__m512i*) dst)

#define VECT_IS_ONE_BLOCK(dst) \
    avx512_is_all_one((__m512i*) dst)

#define VECT_IS_DIGEST_ZERO(start) \
    avx512_is_digest_zero((__m512i*)start)

#define VECT_ARR_BLOCK_LOOKUP(idx, size, nb, start) \
    avx2_idx_arr_block_lookup(idx, size, nb, start)



} // namespace




#endif
