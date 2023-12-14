#ifndef BMAVX2__H__INCLUDED__
#define BMAVX2__H__INCLUDED__
/*
Copyright(c) 2002-2022 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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


/** @defgroup AVX2 AVX2 functions
    Processor specific optimizations for AVX2 instructions (internals)
    @ingroup bvector
    @internal
 */


// Header implements processor specific intrinsics declarations for AVX2
// instruction set
//
#include<emmintrin.h>
#include<immintrin.h>

#include "bmdef.h"
#include "bmbmi2.h"
#include "bmutil.h"

namespace bm
{

// debugging utils
#if 0
inline
void avx2_print256_u32(const char* prefix, const __m256i & value)
{
    const size_t n = sizeof(__m256i) / sizeof(unsigned);
    unsigned buffer[n];
    _mm256_storeu_si256((__m256i*)buffer, value);
    std::cout << prefix << " [ ";
    for (int i = n-1; 1; --i)
    {
        std::cout << std::hex << buffer[i] << " ";
        if (i == 0)
            break;
    }
    std::cout << "]" << std::endl;
}

inline
void avx2_print256_u16(const char* prefix, const __m256i & value)
{
    const size_t n = sizeof(__m256i) / sizeof(unsigned short);
    unsigned short buffer[n];
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
#endif

#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif


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
    @brief Calculate population count based on digest

    @return popcnt
    @ingroup AVX2
*/
inline
bm::id_t avx2_bit_block_count(const bm::word_t* const block,
                              bm::id64_t digest)
{
    bm::id_t count = 0;
    bm::id64_t* cnt64;
    BM_AVX2_POPCNT_PROLOG;
    __m256i cnt = _mm256_setzero_si256();
    while (digest)
    {
        bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;

        unsigned wave = (unsigned)_mm_popcnt_u64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;

        const __m256i* BMRESTRICT wave_src = (__m256i*)&block[off];

        __m256i m1A, m1B, m1C, m1D;
        m1A = _mm256_load_si256(wave_src);
        m1B = _mm256_load_si256(wave_src+1);
        if (!_mm256_testz_si256(m1A, m1A))
        {
            BM_AVX2_BIT_COUNT(bc, m1A)
            cnt = _mm256_add_epi64(cnt, bc);
        }
        if (!_mm256_testz_si256(m1B, m1B))
        {
            BM_AVX2_BIT_COUNT(bc, m1B)
            cnt = _mm256_add_epi64(cnt, bc);
        }

        m1C = _mm256_load_si256(wave_src+2);
        m1D = _mm256_load_si256(wave_src+3);
        if (!_mm256_testz_si256(m1C, m1C))
        {
            BM_AVX2_BIT_COUNT(bc, m1C)
            cnt = _mm256_add_epi64(cnt, bc);
        }
        if (!_mm256_testz_si256(m1D, m1D))
        {
            BM_AVX2_BIT_COUNT(bc, m1D)
            cnt = _mm256_add_epi64(cnt, bc);
        }

        digest = bm::bmi_bslr_u64(digest); // d &= d - 1;
    } // while
    cnt64 = (bm::id64_t*)&cnt;
    count = (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
    return count;

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

    @ingroup AVX2
*/
inline
void avx2_xor_arr_2_mask(__m256i* BMRESTRICT dst,
                         const __m256i* BMRESTRICT src,
                         const __m256i* BMRESTRICT src_end,
                         bm::word_t mask)
{
     __m256i yM = _mm256_set1_epi32(int(mask));
     do
     {
        _mm256_store_si256(dst+0, _mm256_xor_si256(_mm256_load_si256(src+0), yM)); // ymm1 = (~ymm1) & ymm2
        _mm256_store_si256(dst+1, _mm256_xor_si256(_mm256_load_si256(src+1), yM));
        _mm256_store_si256(dst+2, _mm256_xor_si256(_mm256_load_si256(src+2), yM));
        _mm256_store_si256(dst+3, _mm256_xor_si256(_mm256_load_si256(src+3), yM));
        
        dst += 4; src += 4;
     } while (src < src_end);
}


/*!
    @brief Inverts array elements and NOT them to specified mask
    *dst = ~*src & mask

    @ingroup AVX2
*/
inline
void avx2_andnot_arr_2_mask(__m256i* BMRESTRICT dst,
                            const __m256i* BMRESTRICT src,
                            const __m256i* BMRESTRICT src_end,
                            bm::word_t mask)
{
     __m256i yM = _mm256_set1_epi32(int(mask));
     do
     {
        _mm256_store_si256(dst+0, _mm256_andnot_si256(_mm256_load_si256(src+0), yM)); // ymm1 = (~ymm1) & ymm2
        _mm256_store_si256(dst+1, _mm256_andnot_si256(_mm256_load_si256(src+1), yM));
        _mm256_store_si256(dst+2, _mm256_andnot_si256(_mm256_load_si256(src+2), yM));
        _mm256_store_si256(dst+3, _mm256_andnot_si256(_mm256_load_si256(src+3), yM));
        
        dst += 4; src += 4;
     } while (src < src_end);
}

/*!
    @brief AND array elements against another array
    *dst &= *src
    @return 0 if destination does not have any bits
    @ingroup AVX2
*/
inline
unsigned avx2_and_block(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i accA, accB, accC, accD;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    accA = accB = accC = accD = _mm256_setzero_si256();

    do
    {
        m1A = _mm256_and_si256(_mm256_load_si256(src+0), _mm256_load_si256(dst+0));
        m1B = _mm256_and_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
        m1C = _mm256_and_si256(_mm256_load_si256(src+2), _mm256_load_si256(dst+2));
        m1D = _mm256_and_si256(_mm256_load_si256(src+3), _mm256_load_si256(dst+3));

        _mm256_store_si256(dst+0, m1A);
        _mm256_store_si256(dst+1, m1B);
        _mm256_store_si256(dst+2, m1C);
        _mm256_store_si256(dst+3, m1D);
        
        accA = _mm256_or_si256(accA, m1A);
        accB = _mm256_or_si256(accB, m1B);
        accC = _mm256_or_si256(accC, m1C);
        accD = _mm256_or_si256(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm256_or_si256(accA, accB); // A = A | B
    accC = _mm256_or_si256(accC, accD); // C = C | D
    accA = _mm256_or_si256(accA, accC); // A = A | C
    
    return !_mm256_testz_si256(accA, accA);
}

/*!
    @brief AND block digest stride
    *dst &= *src
 
    @return true if stide is all zero
    @ingroup AVX2
*/
inline
bool avx2_and_digest(__m256i* BMRESTRICT dst,
                     const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B, m1C, m1D;

    m1A = _mm256_and_si256(_mm256_load_si256(src+0), _mm256_load_si256(dst+0));
    m1B = _mm256_and_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
    m1C = _mm256_and_si256(_mm256_load_si256(src+2), _mm256_load_si256(dst+2));
    m1D = _mm256_and_si256(_mm256_load_si256(src+3), _mm256_load_si256(dst+3));

    _mm256_store_si256(dst+0, m1A);
    _mm256_store_si256(dst+1, m1B);
    _mm256_store_si256(dst+2, m1C);
    _mm256_store_si256(dst+3, m1D);
    
     m1A = _mm256_or_si256(m1A, m1B);
     m1C = _mm256_or_si256(m1C, m1D);
     m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}

/*!
    @brief AND block digest stride 2 way
    *dst = *src1 & *src2
 
    @return true if stide is all zero
    @ingroup AVX2
*/
inline
bool avx2_and_digest_2way(__m256i* BMRESTRICT dst,
                     const __m256i* BMRESTRICT src1,
                     const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;

    m1A = _mm256_and_si256(_mm256_load_si256(src1+0), _mm256_load_si256(src2+0));
    m1B = _mm256_and_si256(_mm256_load_si256(src1+1), _mm256_load_si256(src2+1));
    m1C = _mm256_and_si256(_mm256_load_si256(src1+2), _mm256_load_si256(src2+2));
    m1D = _mm256_and_si256(_mm256_load_si256(src1+3), _mm256_load_si256(src2+3));

    _mm256_store_si256(dst+0, m1A);
    _mm256_store_si256(dst+1, m1B);
    _mm256_store_si256(dst+2, m1C);
    _mm256_store_si256(dst+3, m1D);
    
     m1A = _mm256_or_si256(m1A, m1B);
     m1C = _mm256_or_si256(m1C, m1D);
     m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}

/*!
    @brief AND-OR block digest stride 2 way
    *dst  |= *src1 & *src2

    @return true if stide is all zero
    @ingroup AVX2
*/
inline
bool avx2_and_or_digest_2way(__m256i* BMRESTRICT dst,
                            const __m256i* BMRESTRICT src1,
                            const __m256i* BMRESTRICT src2)
{
    const __m256i maskF = _mm256_set1_epi32(~0u); // brosdcast 0xFF

    __m256i m1A, m1B, m1C, m1D;
    __m256i mACC1;
    __m256i mSA, mSB, mSC, mSD;


    mSA = _mm256_load_si256(dst+0);
    mSB = _mm256_load_si256(dst+1);
    mACC1 = _mm256_and_si256(mSA, mSB);

    mSC = _mm256_load_si256(dst+2);
    mSD = _mm256_load_si256(dst+3);

    mACC1 = _mm256_and_si256(mACC1, _mm256_and_si256(mSC, mSD));

    mACC1 = _mm256_xor_si256(mACC1, maskF);
    if (_mm256_testz_si256(mACC1, mACC1)) // whole wave is saturated 1111s already
        return false;


    m1A = _mm256_and_si256(_mm256_load_si256(src1+0), _mm256_load_si256(src2+0));
    m1B = _mm256_and_si256(_mm256_load_si256(src1+1), _mm256_load_si256(src2+1));
    m1C = _mm256_and_si256(_mm256_load_si256(src1+2), _mm256_load_si256(src2+2));
    m1D = _mm256_and_si256(_mm256_load_si256(src1+3), _mm256_load_si256(src2+3));

    mACC1 =
        _mm256_or_si256(_mm256_or_si256(m1A, m1B), _mm256_or_si256(m1C, m1D));
    bool all_z = _mm256_testz_si256(mACC1, mACC1);
    if (all_z)
        return all_z;

    m1A = _mm256_or_si256(mSA, m1A);
    m1B = _mm256_or_si256(mSB, m1B);
    m1C = _mm256_or_si256(mSC, m1C);
    m1D = _mm256_or_si256(mSD, m1D);

    _mm256_store_si256(dst+0, m1A);
    _mm256_store_si256(dst+1, m1B);
    _mm256_store_si256(dst+2, m1C);
    _mm256_store_si256(dst+3, m1D);

    return all_z;
}


/*!
    @brief AND block digest stride
    @ingroup AVX2
*/
inline
bool avx2_and_digest_5way(__m256i* BMRESTRICT dst,
                          const __m256i* BMRESTRICT src1,
                          const __m256i* BMRESTRICT src2,
                          const __m256i* BMRESTRICT src3,
                          const __m256i* BMRESTRICT src4)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i m1E, m1F, m1G, m1H;

    {
        __m256i s1_0, s2_0, s1_1, s2_1;

        s1_0 = _mm256_load_si256(src1 + 0); s2_0 = _mm256_load_si256(src2 + 0);
        s1_1 = _mm256_load_si256(src1 + 1); s2_1 = _mm256_load_si256(src2 + 1);
        m1A = _mm256_and_si256(s1_0, s2_0);
        m1B = _mm256_and_si256(s1_1, s2_1);
        s1_0 = _mm256_load_si256(src1 + 2); s2_0 = _mm256_load_si256(src2 + 2);
        s1_1 = _mm256_load_si256(src1 + 3); s2_1 = _mm256_load_si256(src2 + 3);
        m1C = _mm256_and_si256(s1_0, s2_0);
        m1D = _mm256_and_si256(s1_1, s2_1);
    }
    {
        __m256i s3_0, s4_0, s3_1, s4_1;

        s3_0 = _mm256_load_si256(src3 + 0); s4_0 = _mm256_load_si256(src4 + 0);
        s3_1 = _mm256_load_si256(src3 + 1); s4_1 = _mm256_load_si256(src4 + 1);
        m1E = _mm256_and_si256(s3_0, s4_0);
        m1F = _mm256_and_si256(s3_1, s4_1);

        m1A = _mm256_and_si256(m1A, m1E);
        m1B = _mm256_and_si256(m1B, m1F);

        s3_0 = _mm256_load_si256(src3 + 2); s4_0 = _mm256_load_si256(src4 + 2);
        s3_1 = _mm256_load_si256(src3 + 3); s4_1 = _mm256_load_si256(src4 + 3);
        m1G = _mm256_and_si256(s3_0, s4_0);
        m1H = _mm256_and_si256(s3_1, s4_1);
    }
    {
        __m256i dst0, dst1;
        dst0 = _mm256_load_si256(dst + 0); dst1 = _mm256_load_si256(dst + 1);
        
        m1C = _mm256_and_si256(m1C, m1G);
        m1D = _mm256_and_si256(m1D, m1H);
        m1A = _mm256_and_si256(m1A, dst0);
        m1B = _mm256_and_si256(m1B, dst1);

        dst0 = _mm256_load_si256(dst + 2); dst1 = _mm256_load_si256(dst + 3);
        
        m1C = _mm256_and_si256(m1C, dst0);
        m1D = _mm256_and_si256(m1D, dst1);
    }
    _mm256_store_si256(dst + 0, m1A);
    _mm256_store_si256(dst + 1, m1B);
    _mm256_store_si256(dst + 2, m1C);
    _mm256_store_si256(dst + 3, m1D);

    m1A = _mm256_or_si256(m1A, m1B);
    m1C = _mm256_or_si256(m1C, m1D);
    m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}

/*!
    @brief AND block digest stride
    @ingroup AVX2
*/
inline
bool avx2_and_digest_3way(__m256i* BMRESTRICT dst,
                          const __m256i* BMRESTRICT src1,
                          const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;

    {
        __m256i s1_0, s2_0, s1_1, s2_1;

        s1_0 = _mm256_load_si256(src1 + 0); s2_0 = _mm256_load_si256(src2 + 0);
        s1_1 = _mm256_load_si256(src1 + 1); s2_1 = _mm256_load_si256(src2 + 1);
        m1A = _mm256_and_si256(s1_0, s2_0);
        m1B = _mm256_and_si256(s1_1, s2_1);
        s1_0 = _mm256_load_si256(src1 + 2); s2_0 = _mm256_load_si256(src2 + 2);
        s1_1 = _mm256_load_si256(src1 + 3); s2_1 = _mm256_load_si256(src2 + 3);
        m1C = _mm256_and_si256(s1_0, s2_0);
        m1D = _mm256_and_si256(s1_1, s2_1);
    }
    {
        __m256i dst0, dst1;
        dst0 = _mm256_load_si256(dst + 0); dst1 = _mm256_load_si256(dst + 1);

        m1A = _mm256_and_si256(m1A, dst0);
        m1B = _mm256_and_si256(m1B, dst1);

        dst0 = _mm256_load_si256(dst + 2); dst1 = _mm256_load_si256(dst + 3);

        m1C = _mm256_and_si256(m1C, dst0);
        m1D = _mm256_and_si256(m1D, dst1);
    }
    _mm256_store_si256(dst + 0, m1A);
    _mm256_store_si256(dst + 1, m1B);
    _mm256_store_si256(dst + 2, m1C);
    _mm256_store_si256(dst + 3, m1D);

    m1A = _mm256_or_si256(m1A, m1B);
    m1C = _mm256_or_si256(m1C, m1D);
    m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
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

    @ingroup AVX2
*/
inline
bool avx2_or_block(__m256i* BMRESTRICT dst,
                   const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B, m1C, m1D;
    
    __m256i mAccF0 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    __m256i mAccF1 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    
    __m256i* BMRESTRICT dst2 =
        (__m256i*)((bm::word_t*)(dst) + bm::set_block_size/2);
    const __m256i* BMRESTRICT src2 =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size/2);
    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);
    do
    {
        m1A = _mm256_or_si256(_mm256_load_si256(src), _mm256_load_si256(dst));
        m1B = _mm256_or_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
        mAccF0 = _mm256_and_si256(mAccF0, m1A);
        mAccF0 = _mm256_and_si256(mAccF0, m1B);
        
        _mm256_stream_si256(dst,   m1A);
        _mm256_stream_si256(dst+1, m1B);

        src += 2; dst += 2;

        m1C = _mm256_or_si256(_mm256_load_si256(src2), _mm256_load_si256(dst2));
        m1D = _mm256_or_si256(_mm256_load_si256(src2+1), _mm256_load_si256(dst2+1));
        mAccF1 = _mm256_and_si256(mAccF1, m1C);
        mAccF1 = _mm256_and_si256(mAccF1, m1D);
        
        _mm256_stream_si256(dst2, m1C);
        _mm256_stream_si256(dst2+1, m1D);

        src2 += 2; dst2 += 2;
    } while (src2 < src_end);

    __m256i maskF = _mm256_set1_epi32(~0u);
    mAccF0 = _mm256_and_si256(mAccF0, mAccF1);
    __m256i wcmpA = _mm256_cmpeq_epi8(mAccF0, maskF);
    unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
    return (maskA == ~0u);
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
    @brief OR 2 arrays and copy to the destination
    *dst = *src1 | src2
    @return true if all bits are 1

    @ingroup AVX2
*/
inline
bool avx2_or_block_2way(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src1,
                        const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i mAccF0 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    __m256i mAccF1 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    const __m256i* BMRESTRICT src_end1 =
        (const __m256i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm256_or_si256(_mm256_load_si256(src1+0), _mm256_load_si256(src2+0));
        m1B = _mm256_or_si256(_mm256_load_si256(src1+1), _mm256_load_si256(src2+1));
        m1C = _mm256_or_si256(_mm256_load_si256(src1+2), _mm256_load_si256(src2+2));
        m1D = _mm256_or_si256(_mm256_load_si256(src1+3), _mm256_load_si256(src2+3));

        _mm256_store_si256(dst+0, m1A);
        _mm256_store_si256(dst+1, m1B);
        _mm256_store_si256(dst+2, m1C);
        _mm256_store_si256(dst+3, m1D);

        mAccF1 = _mm256_and_si256(mAccF1, m1C);
        mAccF1 = _mm256_and_si256(mAccF1, m1D);
        mAccF0 = _mm256_and_si256(mAccF0, m1A);
        mAccF0 = _mm256_and_si256(mAccF0, m1B);

        src1 += 4; src2 += 4; dst += 4;

    } while (src1 < src_end1);
    
    __m256i maskF = _mm256_set1_epi32(~0u);
     mAccF0 = _mm256_and_si256(mAccF0, mAccF1);
    __m256i wcmpA= _mm256_cmpeq_epi8(mAccF0, maskF);
     unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
     return (maskA == ~0u);
}

/*!
    @brief OR array elements against another 2 arrays
    *dst |= *src1 | src2
    @return true if all bits are 1

    @ingroup AVX2
*/
inline
bool avx2_or_block_3way(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src1,
                        const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i mAccF0 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    __m256i mAccF1 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    const __m256i* BMRESTRICT src_end1 =
        (const __m256i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm256_or_si256(_mm256_load_si256(src1+0), _mm256_load_si256(dst+0));
        m1B = _mm256_or_si256(_mm256_load_si256(src1+1), _mm256_load_si256(dst+1));
        m1C = _mm256_or_si256(_mm256_load_si256(src1+2), _mm256_load_si256(dst+2));
        m1D = _mm256_or_si256(_mm256_load_si256(src1+3), _mm256_load_si256(dst+3));

        m1A = _mm256_or_si256(m1A, _mm256_load_si256(src2+0));
        m1B = _mm256_or_si256(m1B, _mm256_load_si256(src2+1));
        m1C = _mm256_or_si256(m1C, _mm256_load_si256(src2+2));
        m1D = _mm256_or_si256(m1D, _mm256_load_si256(src2+3));

        _mm256_store_si256(dst+0, m1A);
        _mm256_store_si256(dst+1, m1B);
        _mm256_store_si256(dst+2, m1C);
        _mm256_store_si256(dst+3, m1D);

        mAccF1 = _mm256_and_si256(mAccF1, m1C);
        mAccF1 = _mm256_and_si256(mAccF1, m1D);
        mAccF0 = _mm256_and_si256(mAccF0, m1A);
        mAccF0 = _mm256_and_si256(mAccF0, m1B);

        src1 += 4; src2 += 4; dst += 4;

    } while (src1 < src_end1);
    
    __m256i maskF = _mm256_set1_epi32(~0u);
     mAccF0 = _mm256_and_si256(mAccF0, mAccF1);
    __m256i wcmpA= _mm256_cmpeq_epi8(mAccF0, maskF);
     unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
     return (maskA == ~0u);    
}


/*!
    @brief OR array elements against another 4 arrays
    *dst |= *src1 | src2
    @return true if all bits are 1

    @ingroup AVX2
*/
inline
bool avx2_or_block_5way(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src1,
                        const __m256i* BMRESTRICT src2,
                        const __m256i* BMRESTRICT src3,
                        const __m256i* BMRESTRICT src4)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i mAccF0 = _mm256_set1_epi32(~0u); // broadcast 0xFF
    __m256i mAccF1 = _mm256_set1_epi32(~0u); // broadcast 0xFF

    const __m256i* BMRESTRICT src_end1 =
        (const __m256i*)((bm::word_t*)(src1) + bm::set_block_size);

    do
    {
        m1A = _mm256_or_si256(_mm256_load_si256(src1+0), _mm256_load_si256(dst+0));
        m1B = _mm256_or_si256(_mm256_load_si256(src1+1), _mm256_load_si256(dst+1));
        m1C = _mm256_or_si256(_mm256_load_si256(src1+2), _mm256_load_si256(dst+2));
        m1D = _mm256_or_si256(_mm256_load_si256(src1+3), _mm256_load_si256(dst+3));

        m1A = _mm256_or_si256(m1A, _mm256_load_si256(src2+0));
        m1B = _mm256_or_si256(m1B, _mm256_load_si256(src2+1));
        m1C = _mm256_or_si256(m1C, _mm256_load_si256(src2+2));
        m1D = _mm256_or_si256(m1D, _mm256_load_si256(src2+3));

        m1A = _mm256_or_si256(m1A, _mm256_load_si256(src3+0));
        m1B = _mm256_or_si256(m1B, _mm256_load_si256(src3+1));
        m1C = _mm256_or_si256(m1C, _mm256_load_si256(src3+2));
        m1D = _mm256_or_si256(m1D, _mm256_load_si256(src3+3));

        m1A = _mm256_or_si256(m1A, _mm256_load_si256(src4+0));
        m1B = _mm256_or_si256(m1B, _mm256_load_si256(src4+1));
        m1C = _mm256_or_si256(m1C, _mm256_load_si256(src4+2));
        m1D = _mm256_or_si256(m1D, _mm256_load_si256(src4+3));

        _mm256_stream_si256(dst+0, m1A);
        _mm256_stream_si256(dst+1, m1B);
        _mm256_stream_si256(dst+2, m1C);
        _mm256_stream_si256(dst+3, m1D);

        mAccF1 = _mm256_and_si256(mAccF1, m1C);
        mAccF1 = _mm256_and_si256(mAccF1, m1D);
        mAccF0 = _mm256_and_si256(mAccF0, m1A);
        mAccF0 = _mm256_and_si256(mAccF0, m1B);

        src1 += 4; src2 += 4;
        src3 += 4; src4 += 4;
        _mm_prefetch ((const char*)src3, _MM_HINT_T0);
        _mm_prefetch ((const char*)src4, _MM_HINT_T0);

        dst += 4;

    } while (src1 < src_end1);
    
    __m256i maskF = _mm256_set1_epi32(~0u);
     mAccF0 = _mm256_and_si256(mAccF0, mAccF1);
    __m256i wcmpA= _mm256_cmpeq_epi8(mAccF0, maskF);
     unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
     return (maskA == ~0u);
}


/*!
    @brief XOR block against another
    *dst ^= *src
    @return 0 if destination does not have any bits
    @ingroup AVX2
*/
inline
unsigned avx2_xor_block(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i accA, accB, accC, accD;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    accA = accB = accC = accD = _mm256_setzero_si256();

    do
    {
        m1A = _mm256_xor_si256(_mm256_load_si256(src+0), _mm256_load_si256(dst+0));
        m1B = _mm256_xor_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
        m1C = _mm256_xor_si256(_mm256_load_si256(src+2), _mm256_load_si256(dst+2));
        m1D = _mm256_xor_si256(_mm256_load_si256(src+3), _mm256_load_si256(dst+3));

        _mm256_store_si256(dst+0, m1A);
        _mm256_store_si256(dst+1, m1B);
        _mm256_store_si256(dst+2, m1C);
        _mm256_store_si256(dst+3, m1D);
        
        accA = _mm256_or_si256(accA, m1A);
        accB = _mm256_or_si256(accB, m1B);
        accC = _mm256_or_si256(accC, m1C);
        accD = _mm256_or_si256(accD, m1D);
        
        src += 4; dst += 4;

    } while (src < src_end);
    
    accA = _mm256_or_si256(accA, accB); // A = A | B
    accC = _mm256_or_si256(accC, accD); // C = C | D
    accA = _mm256_or_si256(accA, accC); // A = A | C
    
    return !_mm256_testz_si256(accA, accA);
}

/*!
    @brief 3 operand XOR 
    *dst = *src1 ^ src2
    @return 0 if destination does not have any bits
    @ingroup AVX2
*/
inline
unsigned avx2_xor_block_2way(__m256i* BMRESTRICT dst,
                             const __m256i* BMRESTRICT src1,
                             const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i accA, accB, accC, accD;

    const __m256i* BMRESTRICT src1_end =
        (const __m256i*)((bm::word_t*)(src1) + bm::set_block_size);

    accA = accB = accC = accD = _mm256_setzero_si256();

    do
    {
        m1A = _mm256_xor_si256(_mm256_load_si256(src1 + 0), _mm256_load_si256(src2 + 0));
        m1B = _mm256_xor_si256(_mm256_load_si256(src1 + 1), _mm256_load_si256(src2 + 1));
        m1C = _mm256_xor_si256(_mm256_load_si256(src1 + 2), _mm256_load_si256(src2 + 2));
        m1D = _mm256_xor_si256(_mm256_load_si256(src1 + 3), _mm256_load_si256(src2 + 3));

        _mm256_store_si256(dst + 0, m1A);
        _mm256_store_si256(dst + 1, m1B);
        _mm256_store_si256(dst + 2, m1C);
        _mm256_store_si256(dst + 3, m1D);

        accA = _mm256_or_si256(accA, m1A);
        accB = _mm256_or_si256(accB, m1B);
        accC = _mm256_or_si256(accC, m1C);
        accD = _mm256_or_si256(accD, m1D);

        src1 += 4; src2 += 4;  dst += 4;

    } while (src1 < src1_end);

    accA = _mm256_or_si256(accA, accB); // A = A | B
    accC = _mm256_or_si256(accC, accD); // C = C | D
    accA = _mm256_or_si256(accA, accC); // A = A | C

    return !_mm256_testz_si256(accA, accA);
}


/*!
    @brief AND-NOT (SUB) array elements against another array
    *dst &= ~*src
 
    @return 0 if destination does not have any bits

    @ingroup AVX2
*/
inline
unsigned avx2_sub_block(__m256i* BMRESTRICT dst,
                        const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B,  m1C, m1D;
    __m256i accA, accB, accC, accD;
    
    accA = accB = accC = accD = _mm256_setzero_si256();

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        m1A = _mm256_andnot_si256(_mm256_load_si256(src), _mm256_load_si256(dst));
        m1B = _mm256_andnot_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
        m1C = _mm256_andnot_si256(_mm256_load_si256(src+2), _mm256_load_si256(dst+2));
        m1D = _mm256_andnot_si256(_mm256_load_si256(src+3), _mm256_load_si256(dst+3));

        _mm256_store_si256(dst+2, m1C);
        _mm256_store_si256(dst+3, m1D);
        _mm256_store_si256(dst+0, m1A);
        _mm256_store_si256(dst+1, m1B);

        accA = _mm256_or_si256(accA, m1A);
        accB = _mm256_or_si256(accB, m1B);
        accC = _mm256_or_si256(accC, m1C);
        accD = _mm256_or_si256(accD, m1D);
        
        src += 4; dst += 4;
    } while (src < src_end);
    
    accA = _mm256_or_si256(accA, accB); // A = A | B
    accC = _mm256_or_si256(accC, accD); // C = C | D
    accA = _mm256_or_si256(accA, accC); // A = A | C
    
    return !_mm256_testz_si256(accA, accA);
}

/*!
    @brief SUB (AND NOT) block digest stride
    *dst &= ~*src
 
    @return true if stide is all zero
    @ingroup AVX2
*/
inline
bool avx2_sub_digest(__m256i* BMRESTRICT dst,
                     const __m256i* BMRESTRICT src)
{
    __m256i m1A, m1B, m1C, m1D;

    m1A = _mm256_andnot_si256(_mm256_load_si256(src+0), _mm256_load_si256(dst+0));
    m1B = _mm256_andnot_si256(_mm256_load_si256(src+1), _mm256_load_si256(dst+1));
    m1C = _mm256_andnot_si256(_mm256_load_si256(src+2), _mm256_load_si256(dst+2));
    m1D = _mm256_andnot_si256(_mm256_load_si256(src+3), _mm256_load_si256(dst+3));

    _mm256_store_si256(dst+0, m1A);
    _mm256_store_si256(dst+1, m1B);
    _mm256_store_si256(dst+2, m1C);
    _mm256_store_si256(dst+3, m1D);
    
     m1A = _mm256_or_si256(m1A, m1B);
     m1C = _mm256_or_si256(m1C, m1D);
     m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}

/*!
    @brief 2-operand SUB (AND NOT) block digest stride
    *dst = *src1 & ~*src2
 
    @return true if stide is all zero
    @ingroup AVX2
*/
inline
bool avx2_sub_digest_2way(__m256i* BMRESTRICT dst,
                          const __m256i* BMRESTRICT src1,
                          const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;

    m1A = _mm256_andnot_si256(_mm256_load_si256(src2+0), _mm256_load_si256(src1+0));
    m1B = _mm256_andnot_si256(_mm256_load_si256(src2+1), _mm256_load_si256(src1+1));
    m1C = _mm256_andnot_si256(_mm256_load_si256(src2+2), _mm256_load_si256(src1+2));
    m1D = _mm256_andnot_si256(_mm256_load_si256(src2+3), _mm256_load_si256(src1+3));

    _mm256_store_si256(dst+0, m1A);
    _mm256_store_si256(dst+1, m1B);
    _mm256_store_si256(dst+2, m1C);
    _mm256_store_si256(dst+3, m1D);
    
     m1A = _mm256_or_si256(m1A, m1B);
     m1C = _mm256_or_si256(m1C, m1D);
     m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}



/*!
    @brief SUB block digest stride
    @ingroup AVX2
*/
inline
bool avx2_sub_digest_5way(__m256i* BMRESTRICT dst,
                          const __m256i* BMRESTRICT src1,
                          const __m256i* BMRESTRICT src2,
                          const __m256i* BMRESTRICT src3,
                          const __m256i* BMRESTRICT src4)
{
    __m256i m1A, m1B, m1C, m1D;
    __m256i m1E, m1F, m1G, m1H;
    const __m256i maskF = _mm256_set1_epi32(~0u); // brosdcast 0xFF

    {
        __m256i s1_0, s2_0, s1_1, s2_1;

        s1_0 = _mm256_load_si256(src1 + 0); s2_0 = _mm256_load_si256(src2 + 0);
        s1_1 = _mm256_load_si256(src1 + 1); s2_1 = _mm256_load_si256(src2 + 1);
        s1_0 = _mm256_xor_si256(s1_0, maskF);s2_0 = _mm256_xor_si256(s2_0, maskF);
        s1_1 = _mm256_xor_si256(s1_1, maskF);s2_1 = _mm256_xor_si256(s2_1, maskF);

        m1A = _mm256_and_si256(s1_0, s2_0); m1B = _mm256_and_si256(s1_1, s2_1);

        s1_0 = _mm256_load_si256(src1 + 2); s2_0 = _mm256_load_si256(src2 + 2);
        s1_1 = _mm256_load_si256(src1 + 3); s2_1 = _mm256_load_si256(src2 + 3);
        s1_0 = _mm256_xor_si256(s1_0, maskF);s2_0 = _mm256_xor_si256(s2_0, maskF);
        s1_1 = _mm256_xor_si256(s1_1, maskF);s2_1 = _mm256_xor_si256(s2_1, maskF);

        m1C = _mm256_and_si256(s1_0, s2_0);
        m1D = _mm256_and_si256(s1_1, s2_1);
    }
    {
        __m256i s3_0, s4_0, s3_1, s4_1;

        s3_0 = _mm256_load_si256(src3 + 0); s4_0 = _mm256_load_si256(src4 + 0);
        s3_1 = _mm256_load_si256(src3 + 1); s4_1 = _mm256_load_si256(src4 + 1);
        s3_0 = _mm256_xor_si256(s3_0, maskF);s4_0 = _mm256_xor_si256(s4_0, maskF);
        s3_1 = _mm256_xor_si256(s3_1, maskF);s4_1 = _mm256_xor_si256(s4_1, maskF);

        m1E = _mm256_and_si256(s3_0, s4_0);
        m1F = _mm256_and_si256(s3_1, s4_1);

        m1A = _mm256_and_si256(m1A, m1E);
        m1B = _mm256_and_si256(m1B, m1F);

        s3_0 = _mm256_load_si256(src3 + 2); s4_0 = _mm256_load_si256(src4 + 2);
        s3_1 = _mm256_load_si256(src3 + 3); s4_1 = _mm256_load_si256(src4 + 3);
        s3_0 = _mm256_xor_si256(s3_0, maskF);s4_0 = _mm256_xor_si256(s4_0, maskF);
        s3_1 = _mm256_xor_si256(s3_1, maskF);s4_1 = _mm256_xor_si256(s4_1, maskF);

        m1G = _mm256_and_si256(s3_0, s4_0);
        m1H = _mm256_and_si256(s3_1, s4_1);
    }
    {
        __m256i dst0, dst1;
        dst0 = _mm256_load_si256(dst + 0); dst1 = _mm256_load_si256(dst + 1);

        m1C = _mm256_and_si256(m1C, m1G);
        m1D = _mm256_and_si256(m1D, m1H);
        m1A = _mm256_and_si256(m1A, dst0);
        m1B = _mm256_and_si256(m1B, dst1);

        dst0 = _mm256_load_si256(dst + 2); dst1 = _mm256_load_si256(dst + 3);

        m1C = _mm256_and_si256(m1C, dst0);
        m1D = _mm256_and_si256(m1D, dst1);
    }
    _mm256_store_si256(dst + 0, m1A);
    _mm256_store_si256(dst + 1, m1B);
    _mm256_store_si256(dst + 2, m1C);
    _mm256_store_si256(dst + 3, m1D);

    m1A = _mm256_or_si256(m1A, m1B);
    m1C = _mm256_or_si256(m1C, m1D);
    m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}


/*!
    @brief SUB block digest stride
    @ingroup AVX2
*/
inline
bool avx2_sub_digest_3way(__m256i* BMRESTRICT dst,
                          const __m256i* BMRESTRICT src1,
                          const __m256i* BMRESTRICT src2)
{
    __m256i m1A, m1B, m1C, m1D;
//    __m256i m1E, m1F, m1G, m1H;
    const __m256i maskF = _mm256_set1_epi32(~0u); // brosdcast 0xFF

    {
        __m256i s1_0, s2_0, s1_1, s2_1;

        s1_0 = _mm256_load_si256(src1 + 0); s2_0 = _mm256_load_si256(src2 + 0);
        s1_1 = _mm256_load_si256(src1 + 1); s2_1 = _mm256_load_si256(src2 + 1);
        s1_0 = _mm256_xor_si256(s1_0, maskF);s2_0 = _mm256_xor_si256(s2_0, maskF);
        s1_1 = _mm256_xor_si256(s1_1, maskF);s2_1 = _mm256_xor_si256(s2_1, maskF);

        m1A = _mm256_and_si256(s1_0, s2_0); m1B = _mm256_and_si256(s1_1, s2_1);

        s1_0 = _mm256_load_si256(src1 + 2); s2_0 = _mm256_load_si256(src2 + 2);
        s1_1 = _mm256_load_si256(src1 + 3); s2_1 = _mm256_load_si256(src2 + 3);
        s1_0 = _mm256_xor_si256(s1_0, maskF);s2_0 = _mm256_xor_si256(s2_0, maskF);
        s1_1 = _mm256_xor_si256(s1_1, maskF);s2_1 = _mm256_xor_si256(s2_1, maskF);

        m1C = _mm256_and_si256(s1_0, s2_0);
        m1D = _mm256_and_si256(s1_1, s2_1);
    }
    /*
    {
        __m256i s3_0, s4_0, s3_1, s4_1;

        s3_0 = _mm256_load_si256(src3 + 0); s4_0 = _mm256_load_si256(src4 + 0);
        s3_1 = _mm256_load_si256(src3 + 1); s4_1 = _mm256_load_si256(src4 + 1);
        s3_0 = _mm256_xor_si256(s3_0, maskF);s4_0 = _mm256_xor_si256(s4_0, maskF);
        s3_1 = _mm256_xor_si256(s3_1, maskF);s4_1 = _mm256_xor_si256(s4_1, maskF);

        m1E = _mm256_and_si256(s3_0, s4_0);
        m1F = _mm256_and_si256(s3_1, s4_1);

        m1A = _mm256_and_si256(m1A, m1E);
        m1B = _mm256_and_si256(m1B, m1F);

        s3_0 = _mm256_load_si256(src3 + 2); s4_0 = _mm256_load_si256(src4 + 2);
        s3_1 = _mm256_load_si256(src3 + 3); s4_1 = _mm256_load_si256(src4 + 3);
        s3_0 = _mm256_xor_si256(s3_0, maskF);s4_0 = _mm256_xor_si256(s4_0, maskF);
        s3_1 = _mm256_xor_si256(s3_1, maskF);s4_1 = _mm256_xor_si256(s4_1, maskF);

        m1G = _mm256_and_si256(s3_0, s4_0);
        m1H = _mm256_and_si256(s3_1, s4_1);
    }
    */
    {
        __m256i dst0, dst1;
        dst0 = _mm256_load_si256(dst + 0); dst1 = _mm256_load_si256(dst + 1);

//        m1C = _mm256_and_si256(m1C, m1G);
//        m1D = _mm256_and_si256(m1D, m1H);
        m1A = _mm256_and_si256(m1A, dst0);
        m1B = _mm256_and_si256(m1B, dst1);

        dst0 = _mm256_load_si256(dst + 2); dst1 = _mm256_load_si256(dst + 3);

        m1C = _mm256_and_si256(m1C, dst0);
        m1D = _mm256_and_si256(m1D, dst1);
    }
    _mm256_store_si256(dst + 0, m1A);
    _mm256_store_si256(dst + 1, m1B);
    _mm256_store_si256(dst + 2, m1C);
    _mm256_store_si256(dst + 3, m1D);

    m1A = _mm256_or_si256(m1A, m1B);
    m1C = _mm256_or_si256(m1C, m1D);
    m1A = _mm256_or_si256(m1A, m1C);

    return _mm256_testz_si256(m1A, m1A);
}



/*!
    @brief AVX2 block memset
    *dst = value

    @ingroup AVX2
*/
BMFORCEINLINE
void avx2_set_block(__m256i* BMRESTRICT dst, bm::word_t value)
{
    __m256i* BMRESTRICT dst_end =
        (__m256i*)((bm::word_t*)(dst) + bm::set_block_size);

    __m256i ymm0 = _mm256_set1_epi32(int(value));
    do
    {
        _mm256_store_si256(dst,   ymm0);
        _mm256_store_si256(dst+1, ymm0);
        _mm256_store_si256(dst+2, ymm0);
        _mm256_store_si256(dst+3, ymm0);
        
        dst += 4;
    } while (dst < dst_end);
}



/*!
    @brief AVX2 block copy
    *dst = *src

    @ingroup AVX2
*/
inline
void avx2_copy_block(__m256i* BMRESTRICT dst,
                     const __m256i* BMRESTRICT src)
{
    __m256i ymm0, ymm1, ymm2, ymm3;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        ymm0 = _mm256_load_si256(src+0);
        ymm1 = _mm256_load_si256(src+1);
        ymm2 = _mm256_load_si256(src+2);
        ymm3 = _mm256_load_si256(src+3);
        
        _mm256_store_si256(dst+0, ymm0);
        _mm256_store_si256(dst+1, ymm1);
        _mm256_store_si256(dst+2, ymm2);
        _mm256_store_si256(dst+3, ymm3);
        
        ymm0 = _mm256_load_si256(src+4);
        ymm1 = _mm256_load_si256(src+5);
        ymm2 = _mm256_load_si256(src+6);
        ymm3 = _mm256_load_si256(src+7);
        
        _mm256_store_si256(dst+4, ymm0);
        _mm256_store_si256(dst+5, ymm1);
        _mm256_store_si256(dst+6, ymm2);
        _mm256_store_si256(dst+7, ymm3);

        src += 8; dst += 8;

    } while (src < src_end);
}

/*!
    @brief AVX2 block copy (unaligned SRC)
    *dst = *src

    @ingroup AVX2
*/
inline
void avx2_copy_block_unalign(__m256i* BMRESTRICT dst,
                             const __m256i* BMRESTRICT src)
{
    __m256i ymm0, ymm1, ymm2, ymm3;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        ymm0 = _mm256_loadu_si256(src+0);
        ymm1 = _mm256_loadu_si256(src+1);
        ymm2 = _mm256_loadu_si256(src+2);
        ymm3 = _mm256_loadu_si256(src+3);

        _mm256_store_si256(dst+0, ymm0);
        _mm256_store_si256(dst+1, ymm1);
        _mm256_store_si256(dst+2, ymm2);
        _mm256_store_si256(dst+3, ymm3);

        ymm0 = _mm256_loadu_si256(src+4);
        ymm1 = _mm256_loadu_si256(src+5);
        ymm2 = _mm256_loadu_si256(src+6);
        ymm3 = _mm256_loadu_si256(src+7);

        _mm256_store_si256(dst+4, ymm0);
        _mm256_store_si256(dst+5, ymm1);
        _mm256_store_si256(dst+6, ymm2);
        _mm256_store_si256(dst+7, ymm3);

        src += 8; dst += 8;

    } while (src < src_end);
}



/*!
    @brief AVX2 block copy
    *dst = *src

    @ingroup AVX2
*/
inline
void avx2_stream_block(__m256i* BMRESTRICT dst,
                       const __m256i* BMRESTRICT src)
{
    __m256i ymm0, ymm1, ymm2, ymm3;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        ymm0 = _mm256_load_si256(src+0);
        ymm1 = _mm256_load_si256(src+1);
        ymm2 = _mm256_load_si256(src+2);
        ymm3 = _mm256_load_si256(src+3);
        
        _mm256_stream_si256(dst+0, ymm0);
        _mm256_stream_si256(dst+1, ymm1);
        _mm256_stream_si256(dst+2, ymm2);
        _mm256_stream_si256(dst+3, ymm3);
        
        ymm0 = _mm256_load_si256(src+4);
        ymm1 = _mm256_load_si256(src+5);
        ymm2 = _mm256_load_si256(src+6);
        ymm3 = _mm256_load_si256(src+7);
        
        _mm256_stream_si256(dst+4, ymm0);
        _mm256_stream_si256(dst+5, ymm1);
        _mm256_stream_si256(dst+6, ymm2);
        _mm256_stream_si256(dst+7, ymm3);

        src += 8; dst += 8;

    } while (src < src_end);
}

/*!
    @brief AVX2 block copy (unaligned SRC)
    *dst = *src

    @ingroup AVX2
*/
inline
void avx2_stream_block_unalign(__m256i* BMRESTRICT dst,
                              const __m256i* BMRESTRICT src)
{
    __m256i ymm0, ymm1, ymm2, ymm3;

    const __m256i* BMRESTRICT src_end =
        (const __m256i*)((bm::word_t*)(src) + bm::set_block_size);

    do
    {
        ymm0 = _mm256_loadu_si256(src+0);
        ymm1 = _mm256_loadu_si256(src+1);
        ymm2 = _mm256_loadu_si256(src+2);
        ymm3 = _mm256_loadu_si256(src+3);

        _mm256_stream_si256(dst+0, ymm0);
        _mm256_stream_si256(dst+1, ymm1);
        _mm256_stream_si256(dst+2, ymm2);
        _mm256_stream_si256(dst+3, ymm3);

        ymm0 = _mm256_loadu_si256(src+4);
        ymm1 = _mm256_loadu_si256(src+5);
        ymm2 = _mm256_loadu_si256(src+6);
        ymm3 = _mm256_loadu_si256(src+7);

        _mm256_stream_si256(dst+4, ymm0);
        _mm256_stream_si256(dst+5, ymm1);
        _mm256_stream_si256(dst+6, ymm2);
        _mm256_stream_si256(dst+7, ymm3);

        src += 8; dst += 8;

    } while (src < src_end);
}



/*!
    @brief Invert bit-block
    *dst = ~*dst
    or
    *dst ^= *dst

    @ingroup AVX2
*/
inline
void avx2_invert_block(__m256i* BMRESTRICT dst)
{
    __m256i maskFF = _mm256_set1_epi32(-1); // broadcast 0xFF
    const __m256i* BMRESTRICT dst_end =
        (const __m256i*)((bm::word_t*)(dst) + bm::set_block_size);

    __m256i ymm0, ymm1;
    do
    {
        ymm0 = _mm256_xor_si256(_mm256_load_si256(dst+0), maskFF);
        ymm1 = _mm256_xor_si256(_mm256_load_si256(dst+1), maskFF);

        _mm256_store_si256(dst+0, ymm0);
        _mm256_store_si256(dst+1, ymm1);

        ymm0 = _mm256_xor_si256(_mm256_load_si256(dst+2), maskFF);
        ymm1 = _mm256_xor_si256(_mm256_load_si256(dst+3), maskFF);
        
        _mm256_store_si256(dst+2, ymm0);
        _mm256_store_si256(dst+3, ymm1);
        
        dst += 4;
        
    } while (dst < dst_end);
}

/*!
    @brief check if block is all zero bits
    @ingroup AVX2
*/
inline
bool avx2_is_all_zero(const __m256i* BMRESTRICT block)
{
    const __m256i* BMRESTRICT block_end =
        (const __m256i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        __m256i w0 = _mm256_load_si256(block+0);
        __m256i w1 = _mm256_load_si256(block+1);

        __m256i wA = _mm256_or_si256(w0, w1);
        
        __m256i w2 = _mm256_load_si256(block+2);
        __m256i w3 = _mm256_load_si256(block+3);

        __m256i wB = _mm256_or_si256(w2, w3);
        wA = _mm256_or_si256(wA, wB);
        
        if (!_mm256_testz_si256(wA, wA))
            return false;
        block += 4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if digest stride is all zero bits
    @ingroup AVX2
*/
inline
bool avx2_is_digest_zero(const __m256i* BMRESTRICT block)
{
    __m256i wA = _mm256_or_si256(_mm256_load_si256(block+0), _mm256_load_si256(block+1));
    __m256i wB = _mm256_or_si256(_mm256_load_si256(block+2), _mm256_load_si256(block+3));
    wA = _mm256_or_si256(wA, wB);

    return _mm256_testz_si256(wA, wA);
}

/*!
    @brief set digest stride to 0xFF.. or 0x0 value
    @ingroup AVX2
*/
inline 
void avx2_block_set_digest(__m256i* dst, unsigned value)
{
    __m256i mV = _mm256_set1_epi32(int(value));
    _mm256_store_si256(dst, mV);
    _mm256_store_si256(dst + 1, mV);
    _mm256_store_si256(dst + 2, mV);
    _mm256_store_si256(dst + 3, mV);
}

/*!
    @brief check if block is all one bits
    @return true if all bits are 1
    @ingroup AVX2
*/
inline
bool avx2_is_all_one(const __m256i* BMRESTRICT block)
{
    const __m256i maskF = _mm256_set1_epi32(~0u); // brosdcast 0xFF
    const __m256i* BMRESTRICT block_end =
        (const __m256i*)((bm::word_t*)(block) + bm::set_block_size);
    do
    {
        __m256i m1A = _mm256_load_si256(block+0);
        __m256i m1B = _mm256_load_si256(block+1);
        m1A = _mm256_xor_si256(m1A, maskF);
        m1B = _mm256_xor_si256(m1B, maskF);
        m1A = _mm256_or_si256(m1A, m1B);
        if (!_mm256_testz_si256(m1A, m1A))
            return false;
        block += 2;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if wave of pointers is all 0xFFF
    @ingroup AVX2
*/
BMFORCEINLINE
bool avx2_test_all_one_wave(const void* ptr)
{
    __m256i maskF = _mm256_set1_epi32(~0u); // braodcast 0xFF
   __m256i wcmpA = _mm256_cmpeq_epi8(_mm256_loadu_si256((__m256i*)ptr), maskF); // (w0 == maskF)
    unsigned maskA = unsigned(_mm256_movemask_epi8(wcmpA));
    return (maskA == ~0u);
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
    @brief block shift left by 1
    @ingroup AVX2
*/
inline
bool avx2_shift_l1(__m256i* block, bm::word_t* empty_acc, unsigned co1)
{
    __m256i* block_end =
        (__m256i*)((bm::word_t*)(block) + bm::set_block_size);
    
    __m256i m1COshft, m2COshft;
    __m256i mAcc = _mm256_set1_epi32(0);
    __m256i mMask1 = _mm256_set1_epi32(1);
    __m256i mCOidx = _mm256_set_epi32(0, 7, 6, 5, 4, 3, 2, 1);
    unsigned co2;

    for (--block_end; block_end >= block; block_end -= 2)
    {
        __m256i m1A = _mm256_load_si256(block_end);
        __m256i m2A = _mm256_load_si256(block_end-1);

        __m256i m1CO = _mm256_and_si256(m1A, mMask1);
        __m256i m2CO = _mm256_and_si256(m2A, mMask1);

        co2 = _mm256_extract_epi32(m1CO, 0);

        m1A = _mm256_srli_epi32(m1A, 1); // (block[i] >> 1u)
        m2A = _mm256_srli_epi32(m2A, 1);

        // shift CO flags using -1 permute indexes, add CO to v[0]
        m1COshft = _mm256_permutevar8x32_epi32(m1CO, mCOidx);
        m1COshft = _mm256_insert_epi32(m1COshft, co1, 7); // v[7] = co_flag

        co1 = co2;
        
        co2 = _mm256_extract_epi32(m2CO, 0);
        
        m2COshft = _mm256_permutevar8x32_epi32(m2CO, mCOidx);
        m2COshft = _mm256_insert_epi32(m2COshft, co1, 7);

        m1COshft = _mm256_slli_epi32(m1COshft, 31);
        m2COshft = _mm256_slli_epi32(m2COshft, 31);

        m1A = _mm256_or_si256(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm256_or_si256(m2A, m2COshft);

        _mm256_store_si256(block_end,   m1A);
        _mm256_store_si256(block_end-1, m2A);

        mAcc = _mm256_or_si256(mAcc, m1A);
        mAcc = _mm256_or_si256(mAcc, m2A);
        
        co1 = co2;

    } // for
    
    *empty_acc = !_mm256_testz_si256(mAcc, mAcc);
    return co1;
}


/*!
    @brief block shift right by 1
    @ingroup AVX2
*/
inline
bool avx2_shift_r1(__m256i* block, bm::word_t* empty_acc, unsigned co1)
{
    const __m256i* block_end =
        (const __m256i*)((bm::word_t*)(block) + bm::set_block_size);
    
    __m256i m1COshft, m2COshft;
    __m256i mAcc = _mm256_set1_epi32(0);
    __m256i mCOidx = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);
    unsigned co2;

    for (;block < block_end; block+=2)
    {
        __m256i m1A = _mm256_load_si256(block);
        __m256i m2A = _mm256_load_si256(block+1);
        
        __m256i m1CO = _mm256_srli_epi32(m1A, 31);
        __m256i m2CO = _mm256_srli_epi32(m2A, 31);
        
        co2 = _mm256_extract_epi32(m1CO, 7);
        
        m1A = _mm256_slli_epi32(m1A, 1); // (block[i] << 1u)
        m2A = _mm256_slli_epi32(m2A, 1);

        // shift CO flags using +1 permute indexes, add CO to v[0]
        m1COshft = _mm256_permutevar8x32_epi32(m1CO, mCOidx);
        m1COshft = _mm256_insert_epi32(m1COshft, co1, 0); // v[0] = co_flag
        
        co1 = co2;
        
        co2 = _mm256_extract_epi32(m2CO, 7);
        m2COshft = _mm256_permutevar8x32_epi32(m2CO, mCOidx);
        m2COshft = _mm256_insert_epi32(m2COshft, co1, 0);

        m1A = _mm256_or_si256(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm256_or_si256(m2A, m2COshft);
        
        _mm256_store_si256(block, m1A);
        _mm256_store_si256(block+1, m2A);

        mAcc = _mm256_or_si256(mAcc, m1A);
        mAcc = _mm256_or_si256(mAcc, m2A);

        co1 = co2;
    } // for
    
    *empty_acc = !_mm256_testz_si256(mAcc, mAcc);
    return co1;
}


/*!
    @brief fused block shift right by 1 plus AND
    @ingroup AVX2
*/

inline
bool avx2_shift_r1_and(__m256i* BMRESTRICT block,
                       bm::word_t co1,
                       const __m256i* BMRESTRICT mask_block,
                       bm::id64_t* BMRESTRICT digest)
{
    BM_ASSERT(*digest);

    bm::word_t* wblock = (bm::word_t*) block;
    const bm::word_t* mblock = (const bm::word_t*) mask_block;

    __m256i m1COshft, m2COshft;
    __m256i mAcc = _mm256_set1_epi32(0);
    __m256i mCOidx = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);
    unsigned co2;
    
    bm::id64_t d, wd;
    wd = d = *digest;
    unsigned di = co1 ? 0 : unsigned(_tzcnt_u64(d)); // get first set bit
    for (; di < 64 ; ++di)
    {
        const unsigned d_base = di * bm::set_block_digest_wave_size;
        const bm::id64_t dmask = (1ull << di);
        if (d & dmask) // digest stride NOT empty
        {
            mAcc = _mm256_xor_si256(mAcc, mAcc); // mAcc = 0

            mask_block = (__m256i*) &mblock[d_base];
            _mm_prefetch ((const char*)mask_block, _MM_HINT_NTA);

            block = (__m256i*) &wblock[d_base];

            for (unsigned i = 0; i < 2; ++i, block += 2, mask_block += 2)
            {
                __m256i m1A = _mm256_load_si256(block);
                __m256i m2A = _mm256_load_si256(block+1);

                __m256i m1CO = _mm256_srli_epi32(m1A, 31);
                __m256i m2CO = _mm256_srli_epi32(m2A, 31);
                
                co2 = _mm256_extract_epi32(m1CO, 7);
                
                m1A = _mm256_slli_epi32(m1A, 1); // (block[i] << 1u)
                m2A = _mm256_slli_epi32(m2A, 1);
                
                __m256i m1M = _mm256_load_si256(mask_block);
                __m256i m2M = _mm256_load_si256(mask_block+1);

                // shift CO flags using +1 permute indexes, add CO to v[0]
                m1COshft = _mm256_insert_epi32(
                              _mm256_permutevar8x32_epi32(m1CO, mCOidx),
                              co1, 0); // v[0] = co_flag
                
                co1 = co2;
                co2 = _mm256_extract_epi32(m2CO, 7);
                m2COshft = _mm256_insert_epi32(
                                _mm256_permutevar8x32_epi32(m2CO, mCOidx),
                                co1, 0);

                m1A = _mm256_or_si256(m1A, m1COshft); // block[i] |= co_flag
                m2A = _mm256_or_si256(m2A, m2COshft);
                
                m1A = _mm256_and_si256(m1A, m1M); // block[i] &= mask_block[i]
                m2A = _mm256_and_si256(m2A, m2M);

                _mm256_store_si256(block, m1A);
                _mm256_store_si256(block+1, m2A);

                mAcc = _mm256_or_si256(mAcc, m1A);
                mAcc = _mm256_or_si256(mAcc, m2A);

                co1 = co2;
                
            } // for i
            
            if (_mm256_testz_si256(mAcc, mAcc)) // test if OR accum is zero
                d &= ~dmask;                    // clear the digest bit
            
            wd = _blsr_u64(wd); // wd &= wd - 1; // reset lowest set bit
        }
        else // stride is empty
        {
            if (co1)
            {
                BM_ASSERT(co1 == 1);
                BM_ASSERT(wblock[d_base] == 0);
                
                bm::id64_t w0 = wblock[d_base] = (co1 & mblock[d_base]);
                d |= (dmask & (w0 << di)); // update digest (branchless if (w0))
                co1 = 0;
            }
            if (!wd)  // digest is empty, no CO -> exit
                break;
        }
    } // for di
    
    *digest = d;
    return co1;
}



/*
inline
void avx2_i32_shift()
{
    unsigned shift_in = 80;

    __m256i mTest = _mm256_set_epi32(70, 60, 50, 40, 30, 20, 10, 100);
    __m256i mIdx  = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);
 
    __m256i m1shft = _mm256_permutevar8x32_epi32(mTest, mIdx);
     m1shft = _mm256_insert_epi32(m1shft, shift_in, 0);
 
    avx2_print256("m1shft=", m1shft);
}
*/



/*!
    AVX2 calculate number of bit changes from 0 to 1
    @ingroup AVX2
*/
inline
unsigned avx2_bit_block_calc_change(const __m256i* BMRESTRICT block,
                                    unsigned size)
{
    BM_AVX2_POPCNT_PROLOG;

    const __m256i* block_end =
        (const __m256i*)((bm::word_t*)(block) + size);
    
    __m256i m1COshft, m2COshft;
    __m256i mCOidx = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);
    __m256i cntAcc = _mm256_setzero_si256();

    unsigned w0 = *((bm::word_t*)(block));
    unsigned count = 1;

    bm::id64_t BM_ALIGN32 cnt_v[4] BM_ALIGN32ATTR;

    unsigned co2, co1 = 0;
    for (;block < block_end; block+=2)
    {
        __m256i m1A = _mm256_load_si256(block);
        __m256i m2A = _mm256_load_si256(block+1);
        
        __m256i m1CO = _mm256_srli_epi32(m1A, 31);
        __m256i m2CO = _mm256_srli_epi32(m2A, 31);
        
        co2 = _mm256_extract_epi32(m1CO, 7);
        
        __m256i m1As = _mm256_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m256i m2As = _mm256_slli_epi32(m2A, 1);

        // shift CO flags using +1 permute indexes, add CO to v[0]
        m1COshft = _mm256_permutevar8x32_epi32(m1CO, mCOidx);
        m1COshft = _mm256_insert_epi32(m1COshft, co1, 0); // v[0] = co_flag
        
        co1 = co2;
        
        co2 = _mm256_extract_epi32(m2CO, 7);
        m2COshft = _mm256_permutevar8x32_epi32(m2CO, mCOidx);
        m2COshft = _mm256_insert_epi32(m2COshft, co1, 0);

        m1As = _mm256_or_si256(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm256_or_si256(m2As, m2COshft);
        
        co1 = co2;
        
        // we now have two shifted AVX2 regs with carry-over
        m1A = _mm256_xor_si256(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm256_xor_si256(m2A, m2As);
        
        {
            BM_AVX2_BIT_COUNT(bc, m1A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
            BM_AVX2_BIT_COUNT(bc, m2A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
        }
    } // for
    
    // horizontal count sum
    _mm256_store_si256 ((__m256i*)cnt_v, cntAcc);
    count += (unsigned)(cnt_v[0] + cnt_v[1] + cnt_v[2] + cnt_v[3]);

    count -= (w0 & 1u); // correct initial carry-in error
    return count;
}

/*!
    AVX2 calculate number of bit changes from 0 to 1 from a XOR product
    @ingroup AVX2
*/
inline
void avx2_bit_block_calc_xor_change(const __m256i* BMRESTRICT block,
                                    const __m256i* BMRESTRICT xor_block,
                                    unsigned size,
                                    unsigned* BMRESTRICT gcount,
                                    unsigned* BMRESTRICT bcount)
{
    BM_AVX2_POPCNT_PROLOG;

    const __m256i* BMRESTRICT block_end =
        (const __m256i*)((bm::word_t*)(block) + size);

    __m256i m1COshft, m2COshft;
    __m256i mCOidx = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);

    __m256i cntAcc = _mm256_setzero_si256();
    __m256i cntAcc2 = _mm256_setzero_si256();

    unsigned w0 = *((bm::word_t*)(block));
    unsigned bit_count = 0;
    unsigned gap_count = 1;

    bm::id64_t BM_ALIGN32 cnt_v[4] BM_ALIGN32ATTR;

    unsigned co2, co1 = 0;
    for (;block < block_end; block+=2, xor_block+=2)
    {
        __m256i m1A = _mm256_load_si256(block);
        __m256i m2A = _mm256_load_si256(block+1);
        __m256i m1B = _mm256_load_si256(xor_block);
        __m256i m2B = _mm256_load_si256(xor_block+1);

        m1A = _mm256_xor_si256 (m1A, m1B);
        m2A = _mm256_xor_si256 (m2A, m2B);

        {
            BM_AVX2_BIT_COUNT(bc, m1A)
            cntAcc2 = _mm256_add_epi64(cntAcc2, bc);
            BM_AVX2_BIT_COUNT(bc, m2A)
            cntAcc2 = _mm256_add_epi64(cntAcc2, bc);
        }

        __m256i m1CO = _mm256_srli_epi32(m1A, 31);
        __m256i m2CO = _mm256_srli_epi32(m2A, 31);

        co2 = _mm256_extract_epi32(m1CO, 7);

        __m256i m1As = _mm256_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m256i m2As = _mm256_slli_epi32(m2A, 1);

        // shift CO flags using +1 permute indexes, add CO to v[0]
        m1COshft = _mm256_permutevar8x32_epi32(m1CO, mCOidx);
        m1COshft = _mm256_insert_epi32(m1COshft, co1, 0); // v[0] = co_flag

        co1 = co2;

        co2 = _mm256_extract_epi32(m2CO, 7);
        m2COshft = _mm256_permutevar8x32_epi32(m2CO, mCOidx);
        m2COshft = _mm256_insert_epi32(m2COshft, co1, 0);

        m1As = _mm256_or_si256(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm256_or_si256(m2As, m2COshft);

        co1 = co2;

        // we now have two shifted AVX2 regs with carry-over
        m1A = _mm256_xor_si256(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm256_xor_si256(m2A, m2As);

        {
            BM_AVX2_BIT_COUNT(bc, m1A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
            BM_AVX2_BIT_COUNT(bc, m2A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
        }
    } // for

    // horizontal count sum
    _mm256_store_si256 ((__m256i*)cnt_v, cntAcc);
    gap_count += (unsigned)(cnt_v[0] + cnt_v[1] + cnt_v[2] + cnt_v[3]);
    gap_count -= (w0 & 1u); // correct initial carry-in error
    if (!gap_count)
        ++gap_count; // always >0

    _mm256_store_si256 ((__m256i*)cnt_v, cntAcc2);
    bit_count += (unsigned)(cnt_v[0] + cnt_v[1] + cnt_v[2] + cnt_v[3]);

    *gcount = gap_count;
    *bcount = bit_count;
}



/*!
    AVX2 calculate number of bit changes from 0 to 1 and bitcount
    @ingroup AVX2
*/
inline
void avx2_bit_block_calc_change_bc(const __m256i* BMRESTRICT block,
                                   unsigned* gcount, unsigned* bcount)
{
    BM_AVX2_POPCNT_PROLOG;

    const __m256i* block_end =
        (const __m256i*)((bm::word_t*)(block) + bm::set_block_size);
    
    __m256i m1COshft, m2COshft;
    __m256i mCOidx = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 0);
    __m256i cntAcc = _mm256_setzero_si256();

    unsigned w0 = *((bm::word_t*)(block));
    unsigned bit_count = 0;
    unsigned gap_count = 1;

    bm::id64_t BM_ALIGN32 cnt_v[4] BM_ALIGN32ATTR;

    unsigned co2, co1 = 0;
    for (;block < block_end; block+=2)
    {
        __m256i m1A = _mm256_load_si256(block);
        __m256i m2A = _mm256_load_si256(block+1);

        // popcount
        {
            bm::id64_t* b64 = (bm::id64_t*)block;

            bit_count += (unsigned) (_mm_popcnt_u64(b64[0]) + _mm_popcnt_u64(b64[1]));
            bit_count += (unsigned)(_mm_popcnt_u64(b64[2]) + _mm_popcnt_u64(b64[3]));
       
            bit_count += (unsigned)(_mm_popcnt_u64(b64[4]) + _mm_popcnt_u64(b64[5]));
            bit_count += (unsigned)(_mm_popcnt_u64(b64[6]) + _mm_popcnt_u64(b64[7]));
        }
        
        __m256i m1CO = _mm256_srli_epi32(m1A, 31);
        __m256i m2CO = _mm256_srli_epi32(m2A, 31);
        
        co2 = _mm256_extract_epi32(m1CO, 7);
        
        __m256i m1As = _mm256_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m256i m2As = _mm256_slli_epi32(m2A, 1);

        // shift CO flags using +1 permute indexes, add CO to v[0]
        m1COshft = _mm256_permutevar8x32_epi32(m1CO, mCOidx);
        m1COshft = _mm256_insert_epi32(m1COshft, co1, 0); // v[0] = co_flag
        
        co1 = co2;
        
        co2 = _mm256_extract_epi32(m2CO, 7);
        m2COshft = _mm256_permutevar8x32_epi32(m2CO, mCOidx);
        m2COshft = _mm256_insert_epi32(m2COshft, co1, 0);

        m1As = _mm256_or_si256(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm256_or_si256(m2As, m2COshft);
        
        co1 = co2;
        
        // we now have two shifted AVX2 regs with carry-over
        m1A = _mm256_xor_si256(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm256_xor_si256(m2A, m2As);
        
        {
            BM_AVX2_BIT_COUNT(bc, m1A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
            BM_AVX2_BIT_COUNT(bc, m2A)
            cntAcc = _mm256_add_epi64(cntAcc, bc);
        }
    } // for
    
    // horizontal count sum
    _mm256_store_si256 ((__m256i*)cnt_v, cntAcc);
    gap_count += (unsigned)(cnt_v[0] + cnt_v[1] + cnt_v[2] + cnt_v[3]);
    gap_count -= (w0 & 1u); // correct initial carry-in error
    
    *gcount = gap_count;
    *bcount = bit_count;
}


/*!
   \brief Find first bit which is different between two bit-blocks
  @ingroup AVX2
*/
inline
bool avx2_bit_find_first_diff(const __m256i* BMRESTRICT block1,
                              const __m256i* BMRESTRICT block2,
                              unsigned* pos)
{
    unsigned BM_ALIGN32 simd_buf[8] BM_ALIGN32ATTR;

    const __m256i* block1_end =
        (const __m256i*)((bm::word_t*)(block1) + bm::set_block_size);
    __m256i maskZ = _mm256_setzero_si256();
    __m256i mA, mB;
    unsigned simd_lane = 0;
    do
    {
        mA = _mm256_xor_si256(_mm256_load_si256(block1),
                              _mm256_load_si256(block2));
        mB = _mm256_xor_si256(_mm256_load_si256(block1+1),
                              _mm256_load_si256(block2+1));
        __m256i mOR = _mm256_or_si256(mA, mB);
        if (!_mm256_testz_si256(mOR, mOR)) // test 2x256 lanes
        {
            if (!_mm256_testz_si256(mA, mA))
            {
                // invert to fing (w != 0)
                unsigned mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi32(mA, maskZ));
                BM_ASSERT(mask);
                int bsf = bm::bsf_asm32(mask); // find first !=0 (could use lzcnt())
                _mm256_store_si256 ((__m256i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx];// _mm256_extract_epi32 (mA, widx);
                bsf = bm::bsf_asm32(w); // find first bit != 0
                *pos = (simd_lane * 256) + (widx * 32) + bsf;
                return true;
            }
            // invert to fing (w != 0)
            unsigned mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi32(mB, maskZ));
            BM_ASSERT(mask);
            int bsf = bm::bsf_asm32(mask); // find first !=0 (could use lzcnt())
            _mm256_store_si256 ((__m256i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx];// _mm256_extract_epi32 (mB, widx);
            bsf = bm::bsf_asm32(w); // find first bit != 0
            *pos = ((++simd_lane) * 256) + (widx * 32) + bsf;
            return true;
        }

        simd_lane+=2;
        block1+=2; block2+=2;

    } while (block1 < block1_end);
    return false;
}


/*!
   \brief Find first bit set
  @ingroup AVX2
*/
inline
bool avx2_bit_find_first(const __m256i* BMRESTRICT block, unsigned off, unsigned* pos)
{
    unsigned BM_ALIGN32 simd_buf[8] BM_ALIGN32ATTR;

    block = (const __m256i*)((bm::word_t*)(block) + off);
    const __m256i* block_end =
        (const __m256i*)((bm::word_t*)(block) + bm::set_block_size);
    __m256i maskZ = _mm256_setzero_si256();
    __m256i mA, mB;
    unsigned simd_lane = 0;
    do
    {
        mA = _mm256_load_si256(block); mB = _mm256_load_si256(block+1);
        __m256i mOR = _mm256_or_si256(mA, mB);
        if (!_mm256_testz_si256(mOR, mOR)) // test 2x256 lanes
        {
            if (!_mm256_testz_si256(mA, mA))
            {
                // invert to fing (w != 0)
                unsigned mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi32(mA, maskZ));
                BM_ASSERT(mask);
                int bsf = bm::bsf_asm32(mask); // find first !=0 (could use lzcnt())
                _mm256_store_si256 ((__m256i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx];
                bsf = bm::bsf_asm32(w); // find first bit != 0
                *pos = (off * 32) + (simd_lane * 256) + (widx * 32) + bsf;
                return true;
            }
            // invert to fing (w != 0)
            unsigned mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi32(mB, maskZ));
            BM_ASSERT(mask);
            int bsf = bm::bsf_asm32(mask); // find first !=0 (could use lzcnt())
            _mm256_store_si256 ((__m256i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx];
            bsf = bm::bsf_asm32(w); // find first bit != 0
            *pos = (off * 32) + ((++simd_lane) * 256) + (widx * 32) + bsf;
            return true;
        }

        simd_lane+=2;
        block+=2;

    } while (block < block_end);
    return false;
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
    } // for k
    return start + k;
}


/*!
     SSE4.2 bulk bit set
     \internal
*/
inline
void avx2_set_block_bits(bm::word_t* BMRESTRICT block,
                          const unsigned* BMRESTRICT idx,
                          unsigned start, unsigned stop )
{
    const unsigned unroll_factor = 8;
    const unsigned len = (stop - start);
    const unsigned len_unr = len - (len % unroll_factor);

    idx += start;
    
    __m256i sb_mask = _mm256_set1_epi32(bm::set_block_mask);
    __m256i sw_mask = _mm256_set1_epi32(bm::set_word_mask);
    __m256i mask1 = _mm256_set1_epi32(1);
    __m256i mask_tmp;

    unsigned BM_ALIGN32 mask_v[8] BM_ALIGN32ATTR;
    unsigned BM_ALIGN32 mword_v[8] BM_ALIGN32ATTR;

    unsigned k = 0, mask, w_idx;
    for (; k < len_unr; k+=unroll_factor)
    {
        __m256i idxA = _mm256_loadu_si256((__m256i*)(idx+k));
        __m256i nbitA = _mm256_and_si256 (idxA, sb_mask); // nbit = idx[k] & bm::set_block_mask
        __m256i nwordA = _mm256_srli_epi32 (nbitA, bm::set_word_shift); // nword  = nbit >> bm::set_word_shift
        
        nbitA = _mm256_and_si256 (nbitA, sw_mask); // nbit &= bm::set_word_mask;

        __m256i maskA = _mm256_sllv_epi32(mask1, nbitA); // (1 << nbit)

        _mm256_store_si256 ((__m256i*)mword_v, nwordA); // store block word idxs

        // shufffle + permute to prepare comparison vector
        mask_tmp = _mm256_shuffle_epi32 (nwordA, _MM_SHUFFLE(1,1,1,1));
        mask_tmp = _mm256_permute2x128_si256 (mask_tmp, mask_tmp, 0);
        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(mask_tmp, nwordA));
        if (mask == ~0u) // all idxs belong the same word
        {
            w_idx = mword_v[0];
            mask_tmp = _mm256_xor_si256 (mask_tmp, mask_tmp); // zero bits
            mask_tmp = _mm256_or_si256 (mask_tmp, maskA); // set bits

            // horizontal OR via permutation of two 128-bit lanes
            // then byte-shifts + OR withing lower 128
            __m256i mtmp0 = _mm256_permute2x128_si256(mask_tmp, mask_tmp, 0);
            __m256i mtmp1 = _mm256_permute2x128_si256(mask_tmp, mask_tmp, 1);
            mask_tmp = _mm256_or_si256 (mtmp0, mtmp1);
            mtmp0 = _mm256_bsrli_epi128(mask_tmp, 4); // shift R by 1 int
            mask_tmp = _mm256_or_si256 (mtmp0, mask_tmp);
            mtmp0 = _mm256_bsrli_epi128(mask_tmp, 8); // shift R by 2 ints
            mask_tmp = _mm256_or_si256 (mtmp0, mask_tmp);

            int u0 = _mm256_extract_epi32(mask_tmp, 0); // final OR
            block[w_idx] |= u0;
        }
        else // whole 256-bit lane does NOT hit the same word...
        {
            _mm256_store_si256 ((__m256i*)mask_v, maskA);
            
            // compute horizonlal OR of set bit mask over lo-hi 128-bit lanes
            // it is used later if lo or hi lanes hit the same word
            // (probabilistic speculation)
            //
            int u0, u4;
            {
                mask_tmp = _mm256_bsrli_epi128(maskA, 4); // shift R by 1 int
                mask_tmp = _mm256_or_si256 (mask_tmp, maskA);
                __m256i m0 = _mm256_bsrli_epi128(mask_tmp, 8); // shift R by 2 ints
                mask_tmp = _mm256_or_si256 (m0, mask_tmp);

                u0 = _mm256_extract_epi32(mask_tmp, 0); // final OR (128-lo)
                u4 = _mm256_extract_epi32(mask_tmp, 4); // final OR (128-hi)
            }
            
            // check the lo 128-lane
            {
                mask_tmp = _mm256_permute2x128_si256 (nwordA, nwordA, 0); // lo
                __m256i m0 = _mm256_shuffle_epi32(mask_tmp, 0x0); // copy simd[0]
                mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(mask_tmp, m0));
                if (mask == ~0u) // all idxs belong the same word
                {
                    w_idx = mword_v[0];
                    block[w_idx] |= u0;
                }
                else  // different block words: use "shotgun" OR
                {
                    block[mword_v[0]] |= mask_v[0];
                    block[mword_v[1]] |= mask_v[1];
                    block[mword_v[2]] |= mask_v[2];
                    block[mword_v[3]] |= mask_v[3];

                }
            }

            // check the hi 128-lane
            {
                mask_tmp = _mm256_permute2x128_si256 (nwordA, nwordA, 1); // hi
                __m256i m0 = _mm256_shuffle_epi32(mask_tmp, 0x0);
                mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(mask_tmp, m0));
                if (mask == ~0u) // all idxs belong the same word
                {
                    w_idx = mword_v[4];
                    block[w_idx] |= u4;
                }
                else
                {
                    block[mword_v[4]] |= mask_v[4];
                    block[mword_v[5]] |= mask_v[5];
                    block[mword_v[6]] |= mask_v[6];
                    block[mword_v[7]] |= mask_v[7];
                }
            }
        }
    } // for k

    for (; k < len; ++k)
    {
        unsigned n = idx[k];
        unsigned nbit = unsigned(n & bm::set_block_mask);
        unsigned nword  = nbit >> bm::set_word_shift;
        nbit &= bm::set_word_mask;
        block[nword] |= (1u << nbit);
    } // for k
}


/** Set a bits in an AVX target, by indexes (int4) from the source
    @internal
*/
BMFORCEINLINE
__m256i avx2_setbit_256(__m256i target, __m256i source)
{
    __m256i stride_idx = _mm256_set_epi32(224, 192, 160, 128, 96, 64, 32, 0);
    __m256i mask1 = _mm256_set1_epi32(1);

    __m256i v0, v1, acc1, acc2;
    v0 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(0));
    v1 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(1));
    v0 = _mm256_sub_epi32(v0, stride_idx);
    v1 = _mm256_sub_epi32(v1, stride_idx);
    v0   = _mm256_sllv_epi32(mask1, v0);
    v1   = _mm256_sllv_epi32(mask1, v1);
    acc1 = _mm256_or_si256(v1, v0);
    v0 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(2));
    v1 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(3));
    v0 = _mm256_sub_epi32(v0, stride_idx);
    v1 = _mm256_sub_epi32(v1, stride_idx);
    v0   = _mm256_sllv_epi32(mask1, v0);
    v1   = _mm256_sllv_epi32(mask1, v1);
    acc2 = _mm256_or_si256(v1, v0);
    target = _mm256_or_si256(target, acc1);
    v0 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(4));
    v1 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(5));
    v0 = _mm256_sub_epi32(v0, stride_idx);
    v1 = _mm256_sub_epi32(v1, stride_idx);
    v0   = _mm256_sllv_epi32(mask1, v0);
    v1   = _mm256_sllv_epi32(mask1, v1);
    acc1 = _mm256_or_si256(v1, v0);
    target = _mm256_or_si256(target, acc2);
    v0 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(6));
    v1 = _mm256_permutevar8x32_epi32(source, _mm256_set1_epi32(7));
    v0 = _mm256_sub_epi32(v0, stride_idx);
    v1 = _mm256_sub_epi32(v1, stride_idx);
    v0   = _mm256_sllv_epi32(mask1, v0);
    v1   = _mm256_sllv_epi32(mask1, v1);
    acc2 = _mm256_or_si256(v1, v0);
    
    target = _mm256_or_si256(target, acc1);
    target = _mm256_or_si256(target, acc2);
    return target;
}


/** Experimental code to set bits via AVX strides
    @internal
*/
inline
void avx2_set_block_bits2(bm::word_t* BMRESTRICT block,
                          const unsigned* BMRESTRICT idx,
                          unsigned start, unsigned stop )
{
    __m256i stride_idx = _mm256_set_epi32(224, 192, 160, 128, 96, 64, 32, 0);
    __m256i mask1 = _mm256_set1_epi32(1);
    __m256i* block_avx = (__m256i*)block;
    
    unsigned stride = 0;
    __m256i* avx_stride_p = block_avx + stride;
    __m256i blkA = _mm256_load_si256(avx_stride_p);
    
    for (unsigned i = start; i < stop; ++i)
    {
        unsigned n = idx[i];
        unsigned nbit = unsigned(n & bm::set_block_mask);
        unsigned new_stride = nbit >> 8;   // (nbit / 256)
        unsigned stride_bit = nbit & 0xFF; // (nbit % 256)
        if (new_stride != stride)
        {
            _mm256_store_si256(avx_stride_p, blkA); // flush the avx2 accum
            stride = new_stride;
            avx_stride_p = block_avx + stride;
            blkA = _mm256_load_si256(avx_stride_p); // re-load the accum
        }
        // set avx2 stride bit
        __m256i v0 = _mm256_set1_epi32(stride_bit);
        __m256i s0 = _mm256_sub_epi32(v0, stride_idx);
        __m256i k0   = _mm256_sllv_epi32(mask1, s0);
        blkA = _mm256_or_si256(blkA, k0);
    } // for i
    
   _mm256_store_si256(avx_stride_p, blkA);
}

/** Experimental code to set bits via AVX strides
    @internal
*/
inline
void avx2_set_block_bits3(bm::word_t* BMRESTRICT block,
                          const unsigned* BMRESTRICT idx,
                          unsigned start, unsigned stop )
{
    const unsigned unroll_factor = 8;
    const unsigned len = (stop - start);
    const unsigned len_unr = len - (len % unroll_factor);

    idx += start;

    __m256i stride_idx = _mm256_set_epi32(224, 192, 160, 128, 96, 64, 32, 0);
    __m256i mask1 = _mm256_set1_epi32(1);

    __m256i sb_mask = _mm256_set1_epi32(bm::set_block_mask);
    __m256i stride_bit_mask = _mm256_set1_epi32(0xFF);

    unsigned BM_ALIGN32 mstride_v[8] BM_ALIGN32ATTR;
    int BM_ALIGN32 mstride_bit_v[8] BM_ALIGN32ATTR;

    // define the very first block stride based on index 0
    unsigned stride = unsigned(idx[0] & bm::set_block_mask) >> 8;
    
    __m256i* block_avx = (__m256i*)block;
    __m256i* avx_stride_p = block_avx + stride;
    
    __m256i blkA = _mm256_load_si256(avx_stride_p); // load the first accum

    unsigned k = 0, mask;
    for (; k < len_unr; k+=unroll_factor)
    {
        __m256i idxA = _mm256_loadu_si256((__m256i*)(idx+k));
        __m256i nbitA = _mm256_and_si256 (idxA, sb_mask); // nbit = idx[k] & bm::set_block_mask
        __m256i strideA = _mm256_srli_epi32 (nbitA, 8); // new_stride = nbit >> 8
        __m256i strideBitA = _mm256_and_si256 (nbitA, stride_bit_mask); // stride_bit = nbit & 0xFF;

        // construct a cmp vector from broadcasted v[0]
        __m256i mask_tmp = _mm256_shuffle_epi32 (strideA, 0x0);
        mask_tmp = _mm256_permute2x128_si256 (mask_tmp, mask_tmp, 0);
        mask = _mm256_movemask_epi8(_mm256_cmpeq_epi32(mask_tmp, strideA));
        if (mask == ~0u) // all idxs belong the same avx2 stride
        {
            unsigned new_stride = (unsigned)_mm256_extract_epi32(strideA, 0);
            if (new_stride != stride)
            {
                _mm256_store_si256(avx_stride_p, blkA); // flush avx2 accum
                stride = new_stride;
                avx_stride_p = block_avx + stride;
                blkA = _mm256_load_si256(avx_stride_p); // re-load accum
            }
            // set 8 bits all at once
            blkA = bm::avx2_setbit_256(blkA, strideBitA);
        }
        else // stride mix here, process one by one
        {
            _mm256_store_si256 ((__m256i*)mstride_bit_v, strideBitA); // store block stride-bit idxs
            _mm256_store_si256 ((__m256i*)mstride_v, strideA);
            for (unsigned j = 0; j < 8; ++j)
            {
                unsigned new_stride = mstride_v[j];
                if (new_stride != stride)
                {
                    _mm256_store_si256(avx_stride_p, blkA); // flush avx2 accum
                    stride = new_stride;
                    avx_stride_p = block_avx + stride;
                    blkA = _mm256_load_si256(avx_stride_p); // re-load accum
                }
                // set avx2 bits one by one
                mask_tmp = _mm256_set1_epi32(mstride_bit_v[j]);
                mask_tmp = _mm256_sub_epi32(mask_tmp, stride_idx);
                mask_tmp   = _mm256_sllv_epi32(mask1, mask_tmp);
                blkA = _mm256_or_si256(blkA, mask_tmp);
            } // for j
        }
    } // for k
   _mm256_store_si256(avx_stride_p, blkA);

    // set the tail bits conventionally
    for (; k < len; ++k)
    {
        unsigned n = idx[k];
        unsigned nbit = unsigned(n & bm::set_block_mask);
        unsigned nword  = nbit >> bm::set_word_shift;
        nbit &= bm::set_word_mask;
        block[nword] |= (1u << nbit);
    } // for k
}


/**
    Experiemntal. Set number of bits in AVX register from 0 to i
    [ 000000 00000 0000000 00011 11111 ] - i = 7
*/
inline
__m256i avx2_setbit_to256(unsigned i)
{
    __m256i stride_idx1 = _mm256_set_epi32(224, 192, 160, 128, 96, 64, 32, 0);
    __m256i stride_idx2 = _mm256_add_epi32(stride_idx1, _mm256_set1_epi32(32));
    __m256i maskFF = _mm256_set1_epi32(-1);
    __m256i maskZ = _mm256_setzero_si256();
    
    __m256i v0 = _mm256_set1_epi32(i);
    __m256i s0 = _mm256_sub_epi32(v0, stride_idx1);
    __m256i k1   = _mm256_sllv_epi32(maskFF, s0);

    {
        __m256i cmp_eq = _mm256_cmpeq_epi32(k1, maskZ);
        cmp_eq = _mm256_xor_si256(maskFF, cmp_eq); // invert: != 0  mask
        k1 = _mm256_xor_si256(k1, cmp_eq);  // [ 0 0 0 0 0 0 3 0 ]
    }

    __m256i cmp_gt = _mm256_cmpgt_epi32 (stride_idx2, v0);
    cmp_gt = _mm256_xor_si256(maskFF, cmp_gt); // invert as GT == LT|EQ (LE)
    __m256i r = _mm256_xor_si256(k1, cmp_gt); // invert all full words (right)

    return r;
}



/**
    Experimental (test) function to do SIMD vector search (lower bound)
    in sorted, growing array
    @ingroup AVX2

    \internal
*/
inline
int avx2_cmpge_u32(__m256i vect8, unsigned value)
{
    // a > b (unsigned, 32-bit) is the same as (a - 0x80000000) > (b - 0x80000000) (signed, 32-bit)
    // https://fgiesen.wordpress.com/2016/04/03/sse-mind-the-gap/
    //
    __m256i mask0x8 = _mm256_set1_epi32(0x80000000);
    __m256i mm_val  = _mm256_set1_epi32(value);

    __m256i norm_vect8 = _mm256_sub_epi32(vect8, mask0x8); // (signed) vect4 - 0x80000000
    __m256i norm_val   = _mm256_sub_epi32(mm_val, mask0x8);  // (signed) mm_val - 0x80000000

    __m256i cmp_mask_gt = _mm256_cmpgt_epi32(norm_vect8, norm_val);
    __m256i cmp_mask_eq = _mm256_cmpeq_epi32(mm_val, vect8);

    __m256i cmp_mask_ge = _mm256_or_si256(cmp_mask_gt, cmp_mask_eq);
    int mask = _mm256_movemask_epi8(cmp_mask_ge);
    if (mask)
    {
        int bsf = bm::bsf_asm32(mask); // could use lzcnt()
        return bsf / 4;
    }
    return -1;
}

/**
    Experimental (test) function to do SIMD vector search
    in sorted, growing array
    @ingroup AVX2

    \internal
*/
inline
int avx2_cmpge_u16(__m256i vect16, unsigned short value)
{
    __m256i mZ = _mm256_setzero_si256();
    __m256i mVal  = _mm256_set1_epi16(value);

    // subs_epu16 - unsigned substration with saturation, gives 0u if (a - b) < 0
    __m256i mSub = _mm256_subs_epu16(mVal, vect16);
    __m256i mge_mask = _mm256_cmpeq_epi16(mSub, mZ);
    unsigned mask = _mm256_movemask_epi8(mge_mask);
    if (mask)
    {
        int lz = _tzcnt_u32(mask);
        return lz / 2;
    }
    return -1;
}


/**
    Hybrid binary search, starts as binary, then switches to scan

    NOTE: AVX code uses _mm256_subs_epu16 - saturated substraction
    which gives 0 if A-B=0 if A < B (not negative a value).

   \param buf - GAP buffer pointer.
   \param pos - index of the element.
   \param is_set - output. GAP value (0 or 1).
   \return GAP index OR bit-test

    @ingroup AVX2
*/
template<bool RET_TEST=false>
unsigned avx2_gap_bfind(const unsigned short* BMRESTRICT buf,
                        unsigned pos, unsigned* BMRESTRICT is_set)
{
    BM_ASSERT(is_set || RET_TEST);

    const unsigned linear_cutoff = 48;
    const unsigned unroll_factor = 16;

    BM_ASSERT(pos < bm::gap_max_bits);

    unsigned res;
    unsigned start = 1;
    unsigned end = start + ((*buf) >> 3);

    const unsigned arr_end = end;
    if (unsigned dsize = end - start; dsize < unroll_factor) // too small for a full AVX stride
    {
        for (; start < end; ++start)
            if (buf[start] >= pos)
                goto ret;
        BM_ASSERT(0);
    }

    do
    {
        if (unsigned dsize = end - start; dsize < linear_cutoff)
        {
            // set wider scan window to possibly over-read the range,
            // but stay within allocated block memory
            //
            dsize = arr_end - start;

            __m256i mZ = _mm256_setzero_si256();
            __m256i mPos  = _mm256_set1_epi16((unsigned short)pos);
            __m256i vect16, mSub, mge_mask;

            for (unsigned len_unr = start + (dsize - (dsize % unroll_factor));
                        start < len_unr; start += unroll_factor)
            {
                vect16 = _mm256_loadu_si256((__m256i*)(&buf[start])); //16x u16s
                mSub = _mm256_subs_epu16(mPos, vect16);
                mge_mask = _mm256_cmpeq_epi16(mSub, mZ);

                if (int mask = _mm256_movemask_epi8(mge_mask); mask)
                {
                    int lz = _tzcnt_u32(mask);
                    start += (lz >> 1);
                    goto ret;
                }
            } // for
            if (unsigned tail = unroll_factor-(end-start); start > tail+1)
            {
                start -= tail; // rewind back, but stay within block
                vect16 = _mm256_loadu_si256((__m256i*)(&buf[start])); //16x u16s
                mSub = _mm256_subs_epu16(mPos, vect16);
                mge_mask = _mm256_cmpeq_epi16(mSub, mZ);
                int mask = _mm256_movemask_epi8(mge_mask);
                BM_ASSERT(mask); // the result MUST be here at this point

                int lz = _tzcnt_u32(mask);
                start += (lz >> 1);
                goto ret;
            }
            for (; start < end; ++start)
                if (buf[start] >= pos)
                    goto ret;
            BM_ASSERT(0);
        }

        if (unsigned mid = (start + end) >> 1; buf[mid] < pos)
            start = mid + 1;
        else
            end = mid;
        if (unsigned mid = (start + end) >> 1; buf[mid] < pos)
            start = mid + 1;
        else
            end = mid;
    } while (1);
ret:
    res = ((*buf) & 1) ^ ((start-1) & 1);
    if constexpr(RET_TEST)
        return res;
    else
    {
        *is_set = res;
        return start;
    }
}


/**
    Hybrid binary search, starts as binary, then switches to scan
    @ingroup AVX2
*/
inline
unsigned avx2_gap_test(const unsigned short* BMRESTRICT buf, unsigned pos)
{
    return bm::avx2_gap_bfind<true>(buf, pos, 0);
}

/**
    lower bound (great or equal) linear scan in ascending order sorted array
    @ingroup AVX2
    \internal
*/
inline
unsigned avx2_lower_bound_scan_u32(const unsigned* BMRESTRICT arr,
                                   unsigned target,
                                   unsigned from,
                                   unsigned to)
{
    // a > b (unsigned, 32-bit) is the same as (a - 0x80000000) > (b - 0x80000000) (signed, 32-bit)
    // see more at:
    // https://fgiesen.wordpress.com/2016/04/03/sse-mind-the-gap/

    const unsigned* BMRESTRICT arr_base = &arr[from]; // unrolled search base

    unsigned unroll_factor = 8;
    unsigned len = to - from + 1;
    unsigned len_unr = len - (len % unroll_factor);

    __m256i mask0x8 = _mm256_set1_epi32(0x80000000);
    __m256i vect_target = _mm256_set1_epi32(target);
    __m256i norm_target = _mm256_sub_epi32(vect_target, mask0x8);  // (signed) target - 0x80000000

    int mask;
    __m256i vect80, norm_vect80, cmp_mask_ge;

    unsigned k = 0;
    for (; k < len_unr; k += unroll_factor)
    {
        vect80 = _mm256_loadu_si256((__m256i*)(&arr_base[k])); // 8 u32s
        norm_vect80 = _mm256_sub_epi32(vect80, mask0x8); // (signed) vect4 - 0x80000000

        cmp_mask_ge = _mm256_or_si256(                              // GT | EQ
            _mm256_cmpgt_epi32(norm_vect80, norm_target),
            _mm256_cmpeq_epi32(vect80, vect_target)
        );
        mask = _mm256_movemask_epi8(cmp_mask_ge);
        if (mask)
        {
            int bsf = bm::bsf_asm32(mask); //_bit_scan_forward(mask);
            return from + k + (bsf / 4);
        }
    } // for

    for (; k < len; ++k)
    {
        if (arr_base[k] >= target)
            return from + k;
    }
    return to + 1;
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

        if (mask == ~0u) // all idxs belong the same word avoid (costly) gather
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

/**
    Convert bit block to GAP block
    @ingroup AVX2
    \internal
*/
inline
unsigned avx2_bit_to_gap(gap_word_t* BMRESTRICT dest,
                          const unsigned* BMRESTRICT block,
                          unsigned dest_len)
{
    const unsigned* BMRESTRICT block_end = block + bm::set_block_size;
    gap_word_t* BMRESTRICT pcurr = dest;
    gap_word_t* BMRESTRICT end = dest + dest_len; (void)end;

    unsigned bitval = (*block) & 1u;
    *pcurr++ = bm::gap_word_t(bitval);
    *pcurr = 0;
    unsigned bit_idx = 0;
    
    const unsigned vCAP = 64; // 64-bit system
    __m256i maskZ = _mm256_set1_epi32(0);

    for (; block < block_end; block += 8)
    {
        unsigned k = 0;
        if (!bitval)
        {
            // check number of trailing 64-bit words using AVX compare
            __m256i accA = _mm256_load_si256((__m256i*)block); // 4x u64s
            __m256i cmpA = _mm256_cmpeq_epi8(accA, maskZ);
            unsigned  mask = ~_mm256_movemask_epi8(cmpA);
            if (!mask)
            {
                bit_idx += 256;
                continue;
            }
            unsigned w64_idx = _tzcnt_u32(mask);
            k = w64_idx / 8; // 8 byte word offset
            bit_idx += k * vCAP;
        }

        for (; k < 4; ++k)
        {
            bm::id64_t val = (((bm::id64_t*)block)[k]);
            
            if (!val || val == ~0ull)
            {
                // branchless if
                bool cmp = (bool(bitval) != bool(val));
                unsigned mask = ~(cmp - 1u);
                *pcurr = mask & (gap_word_t)(bit_idx-cmp);
                bitval ^= unsigned(cmp);
                unsigned long long pcu = reinterpret_cast<unsigned long long>(pcurr);
                pcu += mask & sizeof(gap_word_t);
                pcurr = reinterpret_cast<gap_word_t*>(pcu);
                bit_idx += vCAP;
                continue;
            } // while
            

            // process "0100011" word
            //
            unsigned bits_consumed = 0;
            do
            {
                unsigned tz = 1u;
                if (bitval != (val & tz))
                {
                    bitval ^= tz;
                    *pcurr++ = (gap_word_t)(bit_idx-tz);
                    
                    BM_ASSERT((pcurr-1) == (dest+1) || *(pcurr-1) > *(pcurr-2));
                    BM_ASSERT(pcurr != end);
                }
                else // match, find the next idx
                {
                    tz = (unsigned)_tzcnt_u64(bitval ? ~val : val);
                }
                
                bool cmp = ((bits_consumed+=tz) < vCAP);
                bit_idx += tz;
                val >>= tz;
                
                if (!val)
                {
                    tz = ~(cmp - 1u); // generate 0xFFFF or 0x0000 mask
                    *pcurr = tz & (gap_word_t)(bit_idx-cmp);
                    bitval ^= unsigned(cmp);
                    bit_idx += tz & (vCAP - bits_consumed);
                    unsigned long long pcu = reinterpret_cast<unsigned long long>(pcurr);
                    pcu += tz & sizeof(gap_word_t);
                    pcurr = reinterpret_cast<gap_word_t*>(pcu);

                    BM_ASSERT((pcurr-1) == (dest+1) || *(pcurr-1) > *(pcurr-2));
                    BM_ASSERT(pcurr != end);
                    break;
                }
            }  while (1);
        } // for k

    } // for block < end

    *pcurr = (gap_word_t)(bit_idx-1);
    unsigned len = (unsigned)(pcurr - dest);
    *dest = (gap_word_t)((*dest & 7) + (len << 3));
    return len;
}

/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target := block ^ xor_block
    @param block - arg1
    @param xor_block - arg2
    @param digest - mask for each block wave to XOR (1) or just copy (0)

    @ingroup AVX2
    @internal
*/
inline
void avx2_bit_block_xor(bm::word_t*  target_block,
                   const bm::word_t*  block, const bm::word_t*  xor_block,
                   bm::id64_t digest)
{
    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        const bm::id64_t mask = (1ull << i);
        unsigned off = (i * bm::set_block_digest_wave_size);
        const __m256i* sub_block = (__m256i*) (block + off);
        __m256i* t_sub_block = (__m256i*)(target_block + off);

        if (digest & mask) // XOR filtered sub-block
        {
            const __m256i* xor_sub_block = (__m256i*) (xor_block + off);
            __m256i mA, mB, mC, mD;
            mA = _mm256_xor_si256(_mm256_load_si256(sub_block),
                                  _mm256_load_si256(xor_sub_block));
            mB = _mm256_xor_si256(_mm256_load_si256(sub_block+1),
                                  _mm256_load_si256(xor_sub_block+1));
            mC = _mm256_xor_si256(_mm256_load_si256(sub_block+2),
                                  _mm256_load_si256(xor_sub_block+2));
            mD = _mm256_xor_si256(_mm256_load_si256(sub_block+3),
                                  _mm256_load_si256(xor_sub_block+3));

            _mm256_store_si256(t_sub_block, mA);
            _mm256_store_si256(t_sub_block+1, mB);
            _mm256_store_si256(t_sub_block+2, mC);
            _mm256_store_si256(t_sub_block+3, mD);
        }
        else // just copy source
        {
            _mm256_store_si256(t_sub_block , _mm256_load_si256(sub_block));
            _mm256_store_si256(t_sub_block+1, _mm256_load_si256(sub_block+1));
            _mm256_store_si256(t_sub_block+2, _mm256_load_si256(sub_block+2));
            _mm256_store_si256(t_sub_block+3, _mm256_load_si256(sub_block+3));
        }
    } // for i
}


/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target ^= xor_block
    @param xor_block - arg1
    @param digest - mask for each block wave to XOR (1)

    @ingroup AVX2
    @internal
*/
inline
void avx2_bit_block_xor_2way(bm::word_t* target_block,
                             const bm::word_t*  xor_block,
                             bm::id64_t digest) BMNOEXCEPT
{
    while (digest)
    {
        bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
        unsigned wave = (unsigned)_mm_popcnt_u64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;

        const __m256i* sub_block = (const __m256i*) (xor_block + off);
        __m256i* t_sub_block = (__m256i*)(target_block + off);

        __m256i mA, mB, mC, mD;
        mA = _mm256_xor_si256(_mm256_load_si256(sub_block),
                              _mm256_load_si256(t_sub_block));
        mB = _mm256_xor_si256(_mm256_load_si256(sub_block+1),
                              _mm256_load_si256(t_sub_block+1));
        mC = _mm256_xor_si256(_mm256_load_si256(sub_block+2),
                              _mm256_load_si256(t_sub_block+2));
        mD = _mm256_xor_si256(_mm256_load_si256(sub_block+3),
                              _mm256_load_si256(t_sub_block+3));

        _mm256_store_si256(t_sub_block,   mA);
        _mm256_store_si256(t_sub_block+1, mB);
        _mm256_store_si256(t_sub_block+2, mC);
        _mm256_store_si256(t_sub_block+3, mD);

        digest = bm::bmi_bslr_u64(digest); // d &= d - 1;
    } // while

}



#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


#define VECT_XOR_ARR_2_MASK(dst, src, src_end, mask)\
    avx2_xor_arr_2_mask((__m256i*)(dst), (__m256i*)(src), (__m256i*)(src_end), (bm::word_t)mask)

#define VECT_ANDNOT_ARR_2_MASK(dst, src, src_end, mask)\
    avx2_andnot_arr_2_mask((__m256i*)(dst), (__m256i*)(src), (__m256i*)(src_end), (bm::word_t)mask)

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
    avx2_invert_block((__m256i*)first);

#define VECT_AND_BLOCK(dst, src) \
    avx2_and_block((__m256i*) dst, (const __m256i*) (src))

#define VECT_AND_DIGEST(dst, src) \
    avx2_and_digest((__m256i*) dst, (const __m256i*) (src))

#define VECT_AND_DIGEST_2WAY(dst, src1, src2) \
    avx2_and_digest_2way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2))

#define VECT_AND_OR_DIGEST_2WAY(dst, src1, src2) \
    avx2_and_or_digest_2way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2))

#define VECT_AND_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    avx2_and_digest_5way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2), (const __m256i*) (src3), (const __m256i*) (src4))

#define VECT_AND_DIGEST_3WAY(dst, src1, src2) \
    avx2_and_digest_3way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2))

#define VECT_OR_BLOCK(dst, src) \
    avx2_or_block((__m256i*) dst, (__m256i*) (src))

#define VECT_OR_BLOCK_3WAY(dst, src1, src2) \
    avx2_or_block_3way((__m256i*) dst, (__m256i*) (src1), (__m256i*) (src2))

#define VECT_OR_BLOCK_2WAY(dst, src1, src2) \
    avx2_or_block_2way((__m256i*) dst, (__m256i*) (src1), (__m256i*) (src2))

#define VECT_OR_BLOCK_3WAY(dst, src1, src2) \
    avx2_or_block_3way((__m256i*) dst, (__m256i*) (src1), (__m256i*) (src2))

#define VECT_OR_BLOCK_5WAY(dst, src1, src2, src3, src4) \
    avx2_or_block_5way((__m256i*) dst, (__m256i*) (src1), (__m256i*) (src2), (__m256i*) (src3), (__m256i*) (src4))

#define VECT_SUB_BLOCK(dst, src) \
    avx2_sub_block((__m256i*) dst, (__m256i*) (src))

#define VECT_SUB_DIGEST(dst, src) \
    avx2_sub_digest((__m256i*) dst, (const __m256i*) (src))

#define VECT_SUB_DIGEST_2WAY(dst, src1, src2) \
    avx2_sub_digest_2way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2))

#define VECT_SUB_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    avx2_sub_digest_5way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2), (const __m256i*) (src3), (const __m256i*) (src4))

#define VECT_SUB_DIGEST_3WAY(dst, src1, src2) \
    avx2_sub_digest_3way((__m256i*) dst, (const __m256i*) (src1), (const __m256i*) (src2))

#define VECT_XOR_BLOCK(dst, src) \
    avx2_xor_block((__m256i*) dst, (__m256i*) (src))

#define VECT_XOR_BLOCK_2WAY(dst, src1, src2) \
    avx2_xor_block_2way((__m256i*) dst, (__m256i*) (src1), (__m256i*) (src2))

#define VECT_COPY_BLOCK(dst, src) \
    avx2_copy_block((__m256i*) dst, (__m256i*) (src))

#define VECT_COPY_BLOCK_UNALIGN(dst, src) \
    avx2_copy_block_unalign((__m256i*) dst, (__m256i*) (src))

#define VECT_STREAM_BLOCK(dst, src) \
    avx2_stream_block((__m256i*) dst, (__m256i*) (src))

#define VECT_STREAM_BLOCK_UNALIGN(dst, src) \
    avx2_stream_block_unalign((__m256i*) dst, (__m256i*) (src))

#define VECT_SET_BLOCK(dst, value) \
    avx2_set_block((__m256i*) dst, (value))

#define VECT_IS_ZERO_BLOCK(dst) \
    avx2_is_all_zero((__m256i*) dst)

#define VECT_IS_ONE_BLOCK(dst) \
    avx2_is_all_one((__m256i*) dst)

#define VECT_IS_DIGEST_ZERO(start) \
    avx2_is_digest_zero((__m256i*)start)

#define VECT_BLOCK_SET_DIGEST(dst, val) \
    avx2_block_set_digest((__m256i*)dst, val)

#define VECT_LOWER_BOUND_SCAN_U32(arr, target, from, to) \
    avx2_lower_bound_scan_u32(arr, target, from, to)

#define VECT_SHIFT_L1(b, acc, co) \
    avx2_shift_l1((__m256i*)b, acc, co)

#define VECT_SHIFT_R1(b, acc, co) \
    avx2_shift_r1((__m256i*)b, acc, co)

#define VECT_SHIFT_R1_AND(b, co, m, digest) \
    avx2_shift_r1_and((__m256i*)b, co, (__m256i*)m, digest)
    
#define VECT_ARR_BLOCK_LOOKUP(idx, size, nb, start) \
    avx2_idx_arr_block_lookup(idx, size, nb, start)

#define VECT_SET_BLOCK_BITS(block, idx, start, stop) \
    avx2_set_block_bits3(block, idx, start, stop)
    
#define VECT_BLOCK_CHANGE(block, size) \
    avx2_bit_block_calc_change((__m256i*)block, size)

#define VECT_BLOCK_XOR_CHANGE(block, xor_block, size, gc, bc) \
    avx2_bit_block_calc_xor_change((__m256i*)block, (__m256i*)xor_block, size, gc, bc)

#define VECT_BLOCK_CHANGE_BC(block, gc, bc) \
    avx2_bit_block_calc_change_bc((__m256i*)block, gc, bc)

#define VECT_BIT_TO_GAP(dest, src, dest_len) \
    avx2_bit_to_gap(dest, src, dest_len)

#define VECT_BIT_FIND_FIRST(src1, off, pos) \
    avx2_bit_find_first((__m256i*) src1, off, pos)

#define VECT_BIT_FIND_DIFF(src1, src2, pos) \
    avx2_bit_find_first_diff((__m256i*) src1, (__m256i*) (src2), pos)

#define VECT_BIT_BLOCK_XOR(t, src, src_xor, d) \
    avx2_bit_block_xor(t, src, src_xor, d)

#define VECT_BIT_BLOCK_XOR_2WAY(t, src_xor, d) \
    avx2_bit_block_xor_2way(t, src_xor, d)

#define VECT_GAP_BFIND(buf, pos, is_set) \
    avx2_gap_bfind(buf, pos, is_set)

#define VECT_GAP_TEST(buf, pos) \
    avx2_gap_test(buf, pos)


#define VECT_BIT_COUNT_DIGEST(blk, d) \
    avx2_bit_block_count(blk, d)


} // namespace




#endif
