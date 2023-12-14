#ifndef BMSSE2__H__INCLUDED__
#define BMSSE2__H__INCLUDED__
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

/*! \file bmsse2.h
    \brief Compute functions for SSE2 SIMD instruction set (internal)
*/

#if !defined(__arm64__) && !defined(__arm__)
#ifndef BMWASMSIMDOPT
#include<mmintrin.h>
#endif
#include<emmintrin.h>
#endif

#include "bmdef.h"
#include "bmutil.h"
#include "bmsse_util.h"


#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

namespace bm
{


/*!
    SSE2 optimized bitcounting function implements parallel bitcounting
    algorithm for SSE2 instruction set.

<pre>
unsigned CalcBitCount32(unsigned b)
{
    b = (b & 0x55555555) + (b >> 1 & 0x55555555);
    b = (b & 0x33333333) + (b >> 2 & 0x33333333);
    b = (b + (b >> 4)) & 0x0F0F0F0F;
    b = b + (b >> 8);
    b = (b + (b >> 16)) & 0x0000003F;
    return b;
}
</pre>

    @ingroup SSE2

*/
inline 
bm::id_t sse2_bit_count(const __m128i* block, const __m128i* block_end)
{
    const unsigned mu1 = 0x55555555;
    const unsigned mu2 = 0x33333333;
    const unsigned mu3 = 0x0F0F0F0F;
    const unsigned mu4 = 0x0000003F;

    // Loading masks
    __m128i m1 = _mm_set_epi32 (mu1, mu1, mu1, mu1);
    __m128i m2 = _mm_set_epi32 (mu2, mu2, mu2, mu2);
    __m128i m3 = _mm_set_epi32 (mu3, mu3, mu3, mu3);
    __m128i m4 = _mm_set_epi32 (mu4, mu4, mu4, mu4);
    __m128i mcnt;
    mcnt = _mm_xor_si128(m1, m1); // cnt = 0

    __m128i tmp1, tmp2;
    do
    {        
        __m128i b = _mm_load_si128(block);
        ++block;

        // b = (b & 0x55555555) + (b >> 1 & 0x55555555);
        tmp1 = _mm_srli_epi32(b, 1);                    // tmp1 = (b >> 1 & 0x55555555)
        tmp1 = _mm_and_si128(tmp1, m1); 
        tmp2 = _mm_and_si128(b, m1);                    // tmp2 = (b & 0x55555555)
        b    = _mm_add_epi32(tmp1, tmp2);               //  b = tmp1 + tmp2

        // b = (b & 0x33333333) + (b >> 2 & 0x33333333);
        tmp1 = _mm_srli_epi32(b, 2);                    // (b >> 2 & 0x33333333)
        tmp1 = _mm_and_si128(tmp1, m2); 
        tmp2 = _mm_and_si128(b, m2);                    // (b & 0x33333333)
        b    = _mm_add_epi32(tmp1, tmp2);               // b = tmp1 + tmp2

        // b = (b + (b >> 4)) & 0x0F0F0F0F;
        tmp1 = _mm_srli_epi32(b, 4);                    // tmp1 = b >> 4
        b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 4)
        b = _mm_and_si128(b, m3);                       //           & 0x0F0F0F0F

        // b = b + (b >> 8);
        tmp1 = _mm_srli_epi32 (b, 8);                   // tmp1 = b >> 8
        b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 8)

        // b = (b + (b >> 16)) & 0x0000003F;
        tmp1 = _mm_srli_epi32 (b, 16);                  // b >> 16
        b = _mm_add_epi32(b, tmp1);                     // b + (b >> 16)
        b = _mm_and_si128(b, m4);                       // (b >> 16) & 0x0000003F;

        mcnt = _mm_add_epi32(mcnt, b);                  // mcnt += b

    } while (block < block_end);


    bm::id_t BM_ALIGN16 tcnt[4] BM_ALIGN16ATTR;
    _mm_store_si128((__m128i*)tcnt, mcnt);

    return tcnt[0] + tcnt[1] + tcnt[2] + tcnt[3];
}



template<class Func>
bm::id_t sse2_bit_count_op(const __m128i* BMRESTRICT block, 
                           const __m128i* BMRESTRICT block_end,
                           const __m128i* BMRESTRICT mask_block,
                           Func sse2_func)
{
    const unsigned mu1 = 0x55555555;
    const unsigned mu2 = 0x33333333;
    const unsigned mu3 = 0x0F0F0F0F;
    const unsigned mu4 = 0x0000003F;

    // Loading masks
    __m128i m1 = _mm_set_epi32 (mu1, mu1, mu1, mu1);
    __m128i m2 = _mm_set_epi32 (mu2, mu2, mu2, mu2);
    __m128i m3 = _mm_set_epi32 (mu3, mu3, mu3, mu3);
    __m128i m4 = _mm_set_epi32 (mu4, mu4, mu4, mu4);
    __m128i mcnt;
    mcnt = _mm_xor_si128(m1, m1); // cnt = 0
    do
    {
        __m128i tmp1, tmp2;
        __m128i b = _mm_load_si128(block++);

        tmp1 = _mm_load_si128(mask_block++);
        
        b = sse2_func(b, tmp1);
                        
        // b = (b & 0x55555555) + (b >> 1 & 0x55555555);
        tmp1 = _mm_srli_epi32(b, 1);                    // tmp1 = (b >> 1 & 0x55555555)
        tmp1 = _mm_and_si128(tmp1, m1); 
        tmp2 = _mm_and_si128(b, m1);                    // tmp2 = (b & 0x55555555)
        b    = _mm_add_epi32(tmp1, tmp2);               //  b = tmp1 + tmp2

        // b = (b & 0x33333333) + (b >> 2 & 0x33333333);
        tmp1 = _mm_srli_epi32(b, 2);                    // (b >> 2 & 0x33333333)
        tmp1 = _mm_and_si128(tmp1, m2); 
        tmp2 = _mm_and_si128(b, m2);                    // (b & 0x33333333)
        b    = _mm_add_epi32(tmp1, tmp2);               // b = tmp1 + tmp2

        // b = (b + (b >> 4)) & 0x0F0F0F0F;
        tmp1 = _mm_srli_epi32(b, 4);                    // tmp1 = b >> 4
        b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 4)
        b = _mm_and_si128(b, m3);                       //           & 0x0F0F0F0F

        // b = b + (b >> 8);
        tmp1 = _mm_srli_epi32 (b, 8);                   // tmp1 = b >> 8
        b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 8)
        
        // b = (b + (b >> 16)) & 0x0000003F;
        tmp1 = _mm_srli_epi32 (b, 16);                  // b >> 16
        b = _mm_add_epi32(b, tmp1);                     // b + (b >> 16)
        b = _mm_and_si128(b, m4);                       // (b >> 16) & 0x0000003F;

        mcnt = _mm_add_epi32(mcnt, b);                  // mcnt += b

    } while (block < block_end);

    bm::id_t BM_ALIGN16 tcnt[4] BM_ALIGN16ATTR;
    _mm_store_si128((__m128i*)tcnt, mcnt);

    return tcnt[0] + tcnt[1] + tcnt[2] + tcnt[3];
}

/*!
    @brief check if block is all zero bits
    @ingroup SSE2
*/
inline
bool sse2_is_all_zero(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    __m128i w;
    const __m128i maskz = _mm_setzero_si128();
    const __m128i* BMRESTRICT block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        w = _mm_or_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
        auto m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(w, maskz));
        w = _mm_or_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
        auto m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(w, maskz));
        if (m1 != 0xFFFF || m2 != 0xFFFF)
            return false;
        block += 4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if block is all ONE bits
    @ingroup SSE2
*/
inline
bool sse2_is_all_one(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    __m128i w;
    const __m128i mask1 = _mm_set_epi32 (~0u, ~0u, ~0u, ~0u);
    const __m128i* BMRESTRICT block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        w = _mm_and_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
        auto m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(w, mask1));
        w = _mm_and_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
        auto m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(w, mask1));
        if (m1 != 0xFFFF || m2 != 0xFFFF)
            return false;
        block+=4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if digest stride is all zero bits
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_is_digest_zero(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    const __m128i maskz = _mm_setzero_si128();

    __m128i wA = _mm_or_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
    __m128i wB = _mm_or_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
    wA = _mm_or_si128(wA, wB);
    auto m1 = _mm_movemask_epi8(_mm_cmpeq_epi8(wA, maskz));

    wA = _mm_or_si128(_mm_load_si128(block+4), _mm_load_si128(block+5));
    wB = _mm_or_si128(_mm_load_si128(block+6), _mm_load_si128(block+7));
    wA = _mm_or_si128(wA, wB);
    auto m2 = _mm_movemask_epi8(_mm_cmpeq_epi8(wA, maskz));

    if (m1 != 0xFFFF || m2 != 0xFFFF)
        return false;
    return true;
}

/*!
    @brief set digest stride to 0xFF.. or 0x0 value
    @ingroup SSE2
*/
BMFORCEINLINE
void sse2_block_set_digest(__m128i* dst, unsigned value) BMNOEXCEPT
{
    __m128i mV = _mm_set1_epi32(int(value));
    _mm_store_si128(dst, mV);     _mm_store_si128(dst + 1, mV);
    _mm_store_si128(dst + 2, mV); _mm_store_si128(dst + 3, mV);
    _mm_store_si128(dst + 4, mV); _mm_store_si128(dst + 5, mV);
    _mm_store_si128(dst + 6, mV); _mm_store_si128(dst + 7, mV);
}


/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target := block ^ xor_block
    @param block - arg1
    @param xor_block - arg2
    @param digest - mask for each block wave to XOR (1) or just copy (0)

    @ingroup SSE2
*/
inline
void sse2_bit_block_xor(bm::word_t*   target_block,
                   const bm::word_t*  block,
                   const bm::word_t*  xor_block,
                   bm::id64_t digest) BMNOEXCEPT
{
    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        const bm::id64_t mask = (1ull << i);
        unsigned off = (i * bm::set_block_digest_wave_size);
        const __m128i* sub_block = (__m128i*) (block + off);
        __m128i* t_sub_block = (__m128i*)(target_block + off);

        if (digest & mask) // XOR filtered sub-block
        {
            const __m128i* xor_sub_block = (__m128i*) (xor_block + off);
            __m128i mA, mB, mC, mD;
            mA = _mm_xor_si128(_mm_load_si128(sub_block),
                                  _mm_load_si128(xor_sub_block));
            mB = _mm_xor_si128(_mm_load_si128(sub_block+1),
                                  _mm_load_si128(xor_sub_block+1));
            mC = _mm_xor_si128(_mm_load_si128(sub_block+2),
                                  _mm_load_si128(xor_sub_block+2));
            mD = _mm_xor_si128(_mm_load_si128(sub_block+3),
                                  _mm_load_si128(xor_sub_block+3));

            _mm_store_si128(t_sub_block, mA);
            _mm_store_si128(t_sub_block+1, mB);
            _mm_store_si128(t_sub_block+2, mC);
            _mm_store_si128(t_sub_block+3, mD);

            mA = _mm_xor_si128(_mm_load_si128(sub_block+4),
                                  _mm_load_si128(xor_sub_block+4));
            mB = _mm_xor_si128(_mm_load_si128(sub_block+5),
                                  _mm_load_si128(xor_sub_block+5));
            mC = _mm_xor_si128(_mm_load_si128(sub_block+6),
                                  _mm_load_si128(xor_sub_block+6));
            mD = _mm_xor_si128(_mm_load_si128(sub_block+7),
                                  _mm_load_si128(xor_sub_block+7));

            _mm_store_si128(t_sub_block+4, mA);
            _mm_store_si128(t_sub_block+5, mB);
            _mm_store_si128(t_sub_block+6, mC);
            _mm_store_si128(t_sub_block+7, mD);

        }
        else // just copy source
        {
            _mm_store_si128(t_sub_block ,  _mm_load_si128(sub_block));
            _mm_store_si128(t_sub_block+1, _mm_load_si128(sub_block+1));
            _mm_store_si128(t_sub_block+2, _mm_load_si128(sub_block+2));
            _mm_store_si128(t_sub_block+3, _mm_load_si128(sub_block+3));

            _mm_store_si128(t_sub_block+4, _mm_load_si128(sub_block+4));
            _mm_store_si128(t_sub_block+5, _mm_load_si128(sub_block+5));
            _mm_store_si128(t_sub_block+6, _mm_load_si128(sub_block+6));
            _mm_store_si128(t_sub_block+7, _mm_load_si128(sub_block+7));
        }
    } // for i
}

/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target ^= xor_block
    @param xor_block - arg1
    @param digest - mask for each block wave to XOR (if 1)

    @ingroup SSE2
    @internal
*/
inline
void sse2_bit_block_xor_2way(bm::word_t* target_block,
                             const bm::word_t*  xor_block,
                             bm::id64_t digest) BMNOEXCEPT
{
    while (digest)
    {
        bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;

        const __m128i* sub_block = (const __m128i*) (xor_block + off);
        __m128i* t_sub_block = (__m128i*)(target_block + off);

        __m128i mA, mB, mC, mD;
        mA = _mm_xor_si128(_mm_load_si128(sub_block),
                              _mm_load_si128(t_sub_block));
        mB = _mm_xor_si128(_mm_load_si128(sub_block+1),
                              _mm_load_si128(t_sub_block+1));
        mC = _mm_xor_si128(_mm_load_si128(sub_block+2),
                              _mm_load_si128(t_sub_block+2));
        mD = _mm_xor_si128(_mm_load_si128(sub_block+3),
                              _mm_load_si128(t_sub_block+3));

        _mm_store_si128(t_sub_block, mA);
        _mm_store_si128(t_sub_block+1, mB);
        _mm_store_si128(t_sub_block+2, mC);
        _mm_store_si128(t_sub_block+3, mD);

        mA = _mm_xor_si128(_mm_load_si128(sub_block+4),
                              _mm_load_si128(t_sub_block+4));
        mB = _mm_xor_si128(_mm_load_si128(sub_block+5),
                              _mm_load_si128(t_sub_block+5));
        mC = _mm_xor_si128(_mm_load_si128(sub_block+6),
                              _mm_load_si128(t_sub_block+6));
        mD = _mm_xor_si128(_mm_load_si128(sub_block+7),
                              _mm_load_si128(t_sub_block+7));

        _mm_store_si128(t_sub_block+4, mA);
        _mm_store_si128(t_sub_block+5, mB);
        _mm_store_si128(t_sub_block+6, mC);
        _mm_store_si128(t_sub_block+7, mD);

        digest = bm::bmi_bslr_u64(digest); // d &= d - 1;
    } // while
}



/*!
    @brief AND block digest stride
    *dst &= *src
    @return true if stide is all zero
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_and_digest(__m128i* BMRESTRICT dst,
                     const __m128i* BMRESTRICT src) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    const __m128i maskz = _mm_setzero_si128();

    m1A = _mm_and_si128(_mm_load_si128(src+0), _mm_load_si128(dst+0));
    m1B = _mm_and_si128(_mm_load_si128(src+1), _mm_load_si128(dst+1));
    m1C = _mm_and_si128(_mm_load_si128(src+2), _mm_load_si128(dst+2));
    m1D = _mm_and_si128(_mm_load_si128(src+3), _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    bool z1 = _mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF;

    m1A = _mm_and_si128(_mm_load_si128(src+4), _mm_load_si128(dst+4));
    m1B = _mm_and_si128(_mm_load_si128(src+5), _mm_load_si128(dst+5));
    m1C = _mm_and_si128(_mm_load_si128(src+6), _mm_load_si128(dst+6));
    m1D = _mm_and_si128(_mm_load_si128(src+7), _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = _mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF;

     return z1 & z2;
}

/*!
    @brief AND-OR block digest stride
    *dst |= *src1 & src2

    @return true if stide is all zero
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_and_or_digest_2way(__m128i* BMRESTRICT dst,
                             const __m128i* BMRESTRICT src1,
                             const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i mACC1;
    const __m128i maskz = _mm_setzero_si128();

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));

    mACC1 = _mm_or_si128(_mm_or_si128(m1A, m1B), _mm_or_si128(m1C, m1D));
    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mACC1, maskz)) == 0xFFFF);

    m1A = _mm_or_si128(_mm_load_si128(dst+0), m1A);
    m1B = _mm_or_si128(_mm_load_si128(dst+1), m1B);
    m1C = _mm_or_si128(_mm_load_si128(dst+2), m1C);
    m1D = _mm_or_si128(_mm_load_si128(dst+3), m1D);

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);


    m1A = _mm_and_si128(_mm_load_si128(src1+4), _mm_load_si128(src2+4));
    m1B = _mm_and_si128(_mm_load_si128(src1+5), _mm_load_si128(src2+5));
    m1C = _mm_and_si128(_mm_load_si128(src1+6), _mm_load_si128(src2+6));
    m1D = _mm_and_si128(_mm_load_si128(src1+7), _mm_load_si128(src2+7));

    mACC1 = _mm_or_si128(_mm_or_si128(m1A, m1B), _mm_or_si128(m1C, m1D));
    bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mACC1, maskz)) == 0xFFFF);

    m1A = _mm_or_si128(_mm_load_si128(dst+4), m1A);
    m1B = _mm_or_si128(_mm_load_si128(dst+5), m1B);
    m1C = _mm_or_si128(_mm_load_si128(dst+6), m1C);
    m1D = _mm_or_si128(_mm_load_si128(dst+7), m1D);

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     return z1 & z2;
}


/*!
    @brief AND block digest stride
    @return true if stide is all zero
    @ingroup SSE2
*/
inline
bool sse2_and_digest_5way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2,
                          const __m128i* BMRESTRICT src3,
                          const __m128i* BMRESTRICT src4) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i m1E, m1F, m1G, m1H;

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));

    m1E = _mm_and_si128(_mm_load_si128(src3+0), _mm_load_si128(src4+0));
    m1F = _mm_and_si128(_mm_load_si128(src3+1), _mm_load_si128(src4+1));
    m1G = _mm_and_si128(_mm_load_si128(src3+2), _mm_load_si128(src4+2));
    m1H = _mm_and_si128(_mm_load_si128(src3+3), _mm_load_si128(src4+3));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);

    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+0));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+1));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+2));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, _mm_setzero_si128())) == 0xFFFF);

    m1A = _mm_and_si128(_mm_load_si128(src1+4), _mm_load_si128(src2+4));
    m1B = _mm_and_si128(_mm_load_si128(src1+5), _mm_load_si128(src2+5));
    m1C = _mm_and_si128(_mm_load_si128(src1+6), _mm_load_si128(src2+6));
    m1D = _mm_and_si128(_mm_load_si128(src1+7), _mm_load_si128(src2+7));

    m1E = _mm_and_si128(_mm_load_si128(src3+4), _mm_load_si128(src4+4));
    m1F = _mm_and_si128(_mm_load_si128(src3+5), _mm_load_si128(src4+5));
    m1G = _mm_and_si128(_mm_load_si128(src3+6), _mm_load_si128(src4+6));
    m1H = _mm_and_si128(_mm_load_si128(src3+7), _mm_load_si128(src4+7));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);

    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+4));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+5));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+6));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, _mm_setzero_si128())) == 0xFFFF);

     return z1 & z2;
}

/*!
    @brief AND block digest stride
    @return true if stide is all zero
    @ingroup SSE2
*/
inline
bool sse2_and_digest_3way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
//    __m128i m1E, m1F, m1G, m1H;

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));
/*
    m1E = _mm_and_si128(_mm_load_si128(src3+0), _mm_load_si128(src4+0));
    m1F = _mm_and_si128(_mm_load_si128(src3+1), _mm_load_si128(src4+1));
    m1G = _mm_and_si128(_mm_load_si128(src3+2), _mm_load_si128(src4+2));
    m1H = _mm_and_si128(_mm_load_si128(src3+3), _mm_load_si128(src4+3));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);
*/
    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+0));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+1));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+2));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, _mm_setzero_si128())) == 0xFFFF);

    m1A = _mm_and_si128(_mm_load_si128(src1+4), _mm_load_si128(src2+4));
    m1B = _mm_and_si128(_mm_load_si128(src1+5), _mm_load_si128(src2+5));
    m1C = _mm_and_si128(_mm_load_si128(src1+6), _mm_load_si128(src2+6));
    m1D = _mm_and_si128(_mm_load_si128(src1+7), _mm_load_si128(src2+7));
/*
    m1E = _mm_and_si128(_mm_load_si128(src3+4), _mm_load_si128(src4+4));
    m1F = _mm_and_si128(_mm_load_si128(src3+5), _mm_load_si128(src4+5));
    m1G = _mm_and_si128(_mm_load_si128(src3+6), _mm_load_si128(src4+6));
    m1H = _mm_and_si128(_mm_load_si128(src3+7), _mm_load_si128(src4+7));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);
*/
    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+4));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+5));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+6));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, _mm_setzero_si128())) == 0xFFFF);

     return z1 & z2;
}



/*!
    @brief AND block digest stride
    *dst = *src1 & src2

    @return true if stide is all zero
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_and_digest_2way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    const __m128i maskz = _mm_setzero_si128();
    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

    m1A = _mm_and_si128(_mm_load_si128(src1+4), _mm_load_si128(src2+4));
    m1B = _mm_and_si128(_mm_load_si128(src1+5), _mm_load_si128(src2+5));
    m1C = _mm_and_si128(_mm_load_si128(src1+6), _mm_load_si128(src2+6));
    m1D = _mm_and_si128(_mm_load_si128(src1+7), _mm_load_si128(src2+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

     return z1 & z2;
}

/*!
    @brief SUB (AND NOT) block digest stride
    *dst &= ~*src

    @return true if stide is all zero
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_sub_digest(__m128i* BMRESTRICT dst,
                     const __m128i* BMRESTRICT src) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    const __m128i maskz = _mm_setzero_si128();

    m1A = _mm_andnot_si128(_mm_load_si128(src+0), _mm_load_si128(dst+0));
    m1B = _mm_andnot_si128(_mm_load_si128(src+1), _mm_load_si128(dst+1));
    m1C = _mm_andnot_si128(_mm_load_si128(src+2), _mm_load_si128(dst+2));
    m1D = _mm_andnot_si128(_mm_load_si128(src+3), _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

    m1A = _mm_andnot_si128(_mm_load_si128(src+4), _mm_load_si128(dst+4));
    m1B = _mm_andnot_si128(_mm_load_si128(src+5), _mm_load_si128(dst+5));
    m1C = _mm_andnot_si128(_mm_load_si128(src+6), _mm_load_si128(dst+6));
    m1D = _mm_andnot_si128(_mm_load_si128(src+7), _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

     return z1 & z2;
}

/*!
    @brief 2-operand SUB (AND NOT) block digest stride
    *dst = src1 & ~*src2

    @return true if stide is all zero
    @ingroup SSE2
*/
BMFORCEINLINE
bool sse2_sub_digest_2way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    const __m128i maskz = _mm_setzero_si128();

    m1A = _mm_andnot_si128(_mm_load_si128(src2+0), _mm_load_si128(src1+0));
    m1B = _mm_andnot_si128(_mm_load_si128(src2+1), _mm_load_si128(src1+1));
    m1C = _mm_andnot_si128(_mm_load_si128(src2+2), _mm_load_si128(src1+2));
    m1D = _mm_andnot_si128(_mm_load_si128(src2+3), _mm_load_si128(src1+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

    m1A = _mm_or_si128(m1A, m1B);
    m1C = _mm_or_si128(m1C, m1D);
    m1A = _mm_or_si128(m1A, m1C);

    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

    m1A = _mm_andnot_si128(_mm_load_si128(src2+4), _mm_load_si128(src1+4));
    m1B = _mm_andnot_si128(_mm_load_si128(src2+5), _mm_load_si128(src1+5));
    m1C = _mm_andnot_si128(_mm_load_si128(src2+6), _mm_load_si128(src1+6));
    m1D = _mm_andnot_si128(_mm_load_si128(src2+7), _mm_load_si128(src1+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

     return z1 & z2;
}

/*!
    @brief SUB block digest stride
    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse2_sub_digest_5way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2,
                          const __m128i* BMRESTRICT src3,
                          const __m128i* BMRESTRICT src4) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i m1E, m1F, m1G, m1H;
    __m128i maskFF  = _mm_set1_epi32(~0u);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+0)), _mm_xor_si128(maskFF,_mm_load_si128(src2+0)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+1)), _mm_xor_si128(maskFF,_mm_load_si128(src2+1)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+2)), _mm_xor_si128(maskFF,_mm_load_si128(src2+2)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+3)), _mm_xor_si128(maskFF,_mm_load_si128(src2+3)));

    m1E = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+0)), _mm_xor_si128(maskFF,_mm_load_si128(src4+0)));
    m1F = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+1)), _mm_xor_si128(maskFF,_mm_load_si128(src4+1)));
    m1G = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+2)), _mm_xor_si128(maskFF,_mm_load_si128(src4+2)));
    m1H = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+3)), _mm_xor_si128(maskFF,_mm_load_si128(src4+3)));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);

    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+0));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+1));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+2));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

    const __m128i maskz = _mm_setzero_si128();
    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+4)), _mm_xor_si128(maskFF,_mm_load_si128(src2+4)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+5)), _mm_xor_si128(maskFF,_mm_load_si128(src2+5)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+6)), _mm_xor_si128(maskFF,_mm_load_si128(src2+6)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+7)), _mm_xor_si128(maskFF,_mm_load_si128(src2+7)));

    m1E = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+4)), _mm_xor_si128(maskFF,_mm_load_si128(src4+4)));
    m1F = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+5)), _mm_xor_si128(maskFF,_mm_load_si128(src4+5)));
    m1G = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+6)), _mm_xor_si128(maskFF,_mm_load_si128(src4+6)));
    m1H = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+7)), _mm_xor_si128(maskFF,_mm_load_si128(src4+7)));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);

    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+4));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+5));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+6));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

     return z1 & z2;
}


/*!
    @brief SUB block digest stride
    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse2_sub_digest_3way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
//    __m128i m1E, m1F, m1G, m1H;
    __m128i maskFF  = _mm_set1_epi32(~0u);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+0)), _mm_xor_si128(maskFF,_mm_load_si128(src2+0)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+1)), _mm_xor_si128(maskFF,_mm_load_si128(src2+1)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+2)), _mm_xor_si128(maskFF,_mm_load_si128(src2+2)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+3)), _mm_xor_si128(maskFF,_mm_load_si128(src2+3)));
/*
    m1E = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+0)), _mm_xor_si128(maskFF,_mm_load_si128(src4+0)));
    m1F = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+1)), _mm_xor_si128(maskFF,_mm_load_si128(src4+1)));
    m1G = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+2)), _mm_xor_si128(maskFF,_mm_load_si128(src4+2)));
    m1H = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+3)), _mm_xor_si128(maskFF,_mm_load_si128(src4+3)));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);
*/
    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+0));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+1));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+2));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+3));

    _mm_store_si128(dst+0, m1A);
    _mm_store_si128(dst+1, m1B);
    _mm_store_si128(dst+2, m1C);
    _mm_store_si128(dst+3, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

    const __m128i maskz = _mm_setzero_si128();
    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+4)), _mm_xor_si128(maskFF,_mm_load_si128(src2+4)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+5)), _mm_xor_si128(maskFF,_mm_load_si128(src2+5)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+6)), _mm_xor_si128(maskFF,_mm_load_si128(src2+6)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+7)), _mm_xor_si128(maskFF,_mm_load_si128(src2+7)));
/*
    m1E = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+4)), _mm_xor_si128(maskFF,_mm_load_si128(src4+4)));
    m1F = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+5)), _mm_xor_si128(maskFF,_mm_load_si128(src4+5)));
    m1G = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+6)), _mm_xor_si128(maskFF,_mm_load_si128(src4+6)));
    m1H = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src3+7)), _mm_xor_si128(maskFF,_mm_load_si128(src4+7)));

    m1A = _mm_and_si128(m1A, m1E);
    m1B = _mm_and_si128(m1B, m1F);
    m1C = _mm_and_si128(m1C, m1G);
    m1D = _mm_and_si128(m1D, m1H);
*/
    m1A = _mm_and_si128(m1A, _mm_load_si128(dst+4));
    m1B = _mm_and_si128(m1B, _mm_load_si128(dst+5));
    m1C = _mm_and_si128(m1C, _mm_load_si128(dst+6));
    m1D = _mm_and_si128(m1D, _mm_load_si128(dst+7));

    _mm_store_si128(dst+4, m1A);
    _mm_store_si128(dst+5, m1B);
    _mm_store_si128(dst+6, m1C);
    _mm_store_si128(dst+7, m1D);

     m1A = _mm_or_si128(m1A, m1B);
     m1C = _mm_or_si128(m1C, m1D);
     m1A = _mm_or_si128(m1A, m1C);

     bool z2 = (_mm_movemask_epi8(_mm_cmpeq_epi8(m1A, maskz)) == 0xFFFF);
     return z1 & z2;
}




/*!
   \brief Find first non-zero bit
  @ingroup SSE2
*/
inline
bool sse2_bit_find_first(const __m128i* BMRESTRICT block, unsigned off,
                          unsigned* pos) BMNOEXCEPT
{
    unsigned BM_ALIGN32 simd_buf[4] BM_ALIGN32ATTR;

    block = (const __m128i*)((bm::word_t*)(block) + off);
    const __m128i* block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    const __m128i maskZ = _mm_setzero_si128();
    __m128i mA, mB;
    unsigned simd_lane = 0;
    int bsf;
    do
    {
        mA = _mm_load_si128(block); mB = _mm_load_si128(block+1);
        __m128i mOR = _mm_or_si128(mA, mB);
        bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mOR, maskZ)) == 0xFFFF);
        if (!z1) // test 2x128 lanes
        {
            z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mA, maskZ)) == 0xFFFF);
            if (!z1)
            {
                unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mA, maskZ));
                mask = ~mask; // invert to find (w != 0)
                BM_ASSERT(mask);
                bsf = bm::bit_scan_forward32(mask); // find first !=0 (could use lzcnt())
                _mm_store_si128 ((__m128i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx];
                bsf = bm::bit_scan_forward32(w); // find first bit != 0
                *pos = (off * 32) +(simd_lane * 128) + (widx * 32) + bsf;
                return true;
            }
            unsigned mask = (_mm_movemask_epi8(_mm_cmpeq_epi32(mB, maskZ)));
            mask = ~mask; // invert to find (w != 0)
            BM_ASSERT(mask);
            bsf = bm::bit_scan_forward32(mask); // find first !=0 (could use lzcnt())
            _mm_store_si128 ((__m128i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx];
            bsf = bm::bit_scan_forward32(w); // find first bit != 0
            *pos = (off * 32) + ((++simd_lane) * 128) + (widx * 32) + bsf;
            return true;
        }
        simd_lane+=2;
        block+=2;
    } while (block < block_end);

    return false;
}

/*!
   \brief Find first bit which is different between two bit-blocks
  @ingroup SSE2
*/
inline
bool sse2_bit_find_first_diff(const __m128i* BMRESTRICT block1,
                              const __m128i* BMRESTRICT block2,
                              unsigned* pos) BMNOEXCEPT
{
    unsigned BM_ALIGN32 simd_buf[4] BM_ALIGN32ATTR;

    const __m128i* block1_end =
        (const __m128i*)((bm::word_t*)(block1) + bm::set_block_size);
    const __m128i maskZ = _mm_setzero_si128();
    __m128i mA, mB;
    unsigned simd_lane = 0;
    do
    {
        mA = _mm_xor_si128(_mm_load_si128(block1), _mm_load_si128(block2));
        mB = _mm_xor_si128(_mm_load_si128(block1+1), _mm_load_si128(block2+1));
        __m128i mOR = _mm_or_si128(mA, mB);
        bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mOR, maskZ)) == 0xFFFF);
        if (!z1) // test 2x128 lanes
        {
            z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mA, maskZ)) == 0xFFFF);
            if (!z1) // test 2x128 lanes
            {
                unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mA, maskZ));
                mask = ~mask; // invert to find (w != 0)
                BM_ASSERT(mask);
                int bsf = bm::bit_scan_forward32(mask); // find first !=0 (could use lzcnt())
                _mm_store_si128 ((__m128i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx]; // _mm_extract_epi32 (mA, widx);
                bsf = bm::bit_scan_forward32(w); // find first bit != 0
                *pos = (simd_lane * 128) + (widx * 32) + bsf;
                return true;
            }
            unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mB, maskZ));
            mask = ~mask; // invert to find (w != 0)
            BM_ASSERT(mask);
            int bsf = bm::bit_scan_forward32(mask); // find first !=0 (could use lzcnt())
            _mm_store_si128 ((__m128i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx]; // _mm_extract_epi32 (mB, widx);
            bsf = bm::bit_scan_forward32(w); // find first bit != 0
            *pos = ((++simd_lane) * 128) + (widx * 32) + bsf;
            return true;
        }
        simd_lane+=2;
        block1+=2; block2+=2;
    } while (block1 < block1_end);
    return false;
}

/*
Snippets to extract32 in SSE2:

inline int get_x(const __m128i& vec){return _mm_cvtsi128_si32 (vec);}
inline int get_y(const __m128i& vec){return _mm_cvtsi128_si32 (_mm_shuffle_epi32(vec,0x55));}
inline int get_z(const __m128i& vec){return _mm_cvtsi128_si32 (_mm_shuffle_epi32(vec,0xAA));}
inline int get_w(const __m128i& vec){return _mm_cvtsi128_si32 (_mm_shuffle_epi32(vec,0xFF));}
*/

/*!
    @brief block shift right by 1
    @ingroup SSE2
*/
inline
bool sse2_shift_r1(__m128i* block, unsigned* empty_acc, unsigned co1) BMNOEXCEPT
{
    __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    __m128i m1COshft, m2COshft;
    __m128i mAcc = _mm_set1_epi32(0);

    __m128i mMask0 = _mm_set_epi32(-1,-1,-1, 0);

    unsigned co2;
    for (;block < block_end; block += 2)
    {
        __m128i m1A = _mm_load_si128(block);
        __m128i m2A = _mm_load_si128(block+1);

        __m128i m1CO = _mm_srli_epi32(m1A, 31);
        __m128i m2CO = _mm_srli_epi32(m2A, 31);

        co2 = _mm_cvtsi128_si32(_mm_shuffle_epi32(m1CO, 0xFF));

        m1A = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
        m2A = _mm_slli_epi32(m2A, 1);

        m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift-l by 1 int32
        m2COshft = _mm_slli_si128 (m2CO, 4);

        m1COshft = _mm_and_si128(m1COshft, mMask0); // clear the vec[0]
        m1COshft = _mm_or_si128(m1COshft, _mm_set_epi32(0, 0, 0, co1)); // vec[0] = co1

        m2COshft = _mm_and_si128(m2COshft, mMask0); // clear the vec[0]
        m2COshft = _mm_or_si128(m2COshft, _mm_set_epi32(0, 0, 0, co2)); // vec[0] = co2

        m1A = _mm_or_si128(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm_or_si128(m2A, m2COshft);

        co1 = _mm_cvtsi128_si32(_mm_shuffle_epi32(m2CO, 0xFF));

        _mm_store_si128(block, m1A);
        _mm_store_si128(block+1, m2A);

        mAcc = _mm_or_si128(mAcc, m1A);
        mAcc = _mm_or_si128(mAcc, m2A);
    }
    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mAcc, _mm_set1_epi32(0))) == 0xFFFF);
    *empty_acc = !z1;
    return co1;
}

/*!
    @brief block shift left by 1
    @ingroup SSE2
*/
inline
bool sse2_shift_l1(__m128i* block, unsigned* empty_acc, unsigned co1) BMNOEXCEPT
{
    __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    __m128i mAcc = _mm_set1_epi32(0);
    __m128i mMask1 = _mm_set1_epi32(1);
    __m128i mMask0 = _mm_set_epi32(0, -1, -1, -1);

    unsigned co2;
    for (--block_end; block_end >= block; block_end -= 2)
    {
        __m128i m1A = _mm_load_si128(block_end);
        __m128i m2A = _mm_load_si128(block_end-1);

        __m128i m1CO = _mm_and_si128(m1A, mMask1);
        __m128i m2CO = _mm_and_si128(m2A, mMask1);

        co2 = _mm_cvtsi128_si32 (m1CO); // get vec[0]

        m1A = _mm_srli_epi32(m1A, 1); // (block[i] >> 1u)
        m2A = _mm_srli_epi32(m2A, 1);

        __m128i m1COshft = _mm_srli_si128 (m1CO, 4); // byte shift-r by 1 int32
        __m128i m2COshft = _mm_srli_si128 (m2CO, 4);

        // m1COshft = _mm_insert_epi32 (m1COshft, co1, 3);
        // m2COshft = _mm_insert_epi32 (m2COshft, co2, 3);
        m1COshft = _mm_and_si128(m1COshft, mMask0); // clear the vec[0]
        m1COshft = _mm_or_si128(m1COshft, _mm_set_epi32(co1, 0, 0, 0)); // vec[3] = co1
        m2COshft = _mm_and_si128(m2COshft, mMask0); // clear the vec[0]
        m2COshft = _mm_or_si128(m2COshft, _mm_set_epi32(co2, 0, 0, 0)); // vec[3] = co2


        m1COshft = _mm_slli_epi32(m1COshft, 31);
        m2COshft = _mm_slli_epi32(m2COshft, 31);

        m1A = _mm_or_si128(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm_or_si128(m2A, m2COshft);

        co1 = _mm_cvtsi128_si32 (m2CO); // get vec[0]

        _mm_store_si128(block_end, m1A);
        _mm_store_si128(block_end-1, m2A);

        mAcc = _mm_or_si128(mAcc, m1A);
        mAcc = _mm_or_si128(mAcc, m2A);
    } // for

    bool z1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(mAcc, _mm_set1_epi32(0))) == 0xFFFF);
    *empty_acc = !z1; // !_mm_testz_si128(mAcc, mAcc);
    return co1;
}



inline
bm::id_t sse2_bit_block_calc_count_change(const __m128i* BMRESTRICT block,
                                          const __m128i* BMRESTRICT block_end,
                                               unsigned* BMRESTRICT bit_count)
{
   const unsigned mu1 = 0x55555555;
   const unsigned mu2 = 0x33333333;
   const unsigned mu3 = 0x0F0F0F0F;
   const unsigned mu4 = 0x0000003F;

   // Loading masks
   __m128i m1 = _mm_set_epi32 (mu1, mu1, mu1, mu1);
   __m128i m2 = _mm_set_epi32 (mu2, mu2, mu2, mu2);
   __m128i m3 = _mm_set_epi32 (mu3, mu3, mu3, mu3);
   __m128i m4 = _mm_set_epi32 (mu4, mu4, mu4, mu4);
   __m128i mcnt;//, ccnt;
   mcnt = _mm_xor_si128(m1, m1); // bit_cnt = 0
   //ccnt = _mm_xor_si128(m1, m1); // change_cnt = 0

   __m128i tmp1, tmp2;

   int count = (int)(block_end - block)*4; //0;//1;

   bm::word_t  w, w0, w_prev;//, w_l;
   const int w_shift = sizeof(w) * 8 - 1;
   bool first_word = true;
 
   // first word
   {
       const bm::word_t* blk = (const bm::word_t*) block;
       w = w0 = blk[0];
       w ^= (w >> 1);
       count += bm::word_bitcount(w);
       count -= (w_prev = (w0 >> w_shift)); // negative value correction
   }

   bm::id_t BM_ALIGN16 tcnt[4] BM_ALIGN16ATTR;

   do
   {
       // compute bit-count
       // ---------------------------------------------------------------------
       {
       __m128i b = _mm_load_si128(block);

       // w ^(w >> 1)
       tmp1 = _mm_srli_epi32(b, 1);       // tmp1 = b >> 1
       tmp2 = _mm_xor_si128(b, tmp1);     // tmp2 = tmp1 ^ b;
       _mm_store_si128((__m128i*)tcnt, tmp2);
       

       // compare with zero
       // SSE4: _mm_test_all_zero()
       {
           // b = (b & 0x55555555) + (b >> 1 & 0x55555555);
           //tmp1 = _mm_srli_epi32(b, 1);                    // tmp1 = (b >> 1 & 0x55555555)
           tmp1 = _mm_and_si128(tmp1, m1);
           tmp2 = _mm_and_si128(b, m1);                    // tmp2 = (b & 0x55555555)
           b    = _mm_add_epi32(tmp1, tmp2);               //  b = tmp1 + tmp2

           // b = (b & 0x33333333) + (b >> 2 & 0x33333333);
           tmp1 = _mm_srli_epi32(b, 2);                    // (b >> 2 & 0x33333333)
           tmp1 = _mm_and_si128(tmp1, m2);
           tmp2 = _mm_and_si128(b, m2);                    // (b & 0x33333333)
           b    = _mm_add_epi32(tmp1, tmp2);               // b = tmp1 + tmp2

           // b = (b + (b >> 4)) & 0x0F0F0F0F;
           tmp1 = _mm_srli_epi32(b, 4);                    // tmp1 = b >> 4
           b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 4)
           b = _mm_and_si128(b, m3);                       //& 0x0F0F0F0F

           // b = b + (b >> 8);
           tmp1 = _mm_srli_epi32 (b, 8);                   // tmp1 = b >> 8
           b = _mm_add_epi32(b, tmp1);                     // b = b + (b >> 8)

           // b = (b + (b >> 16)) & 0x0000003F;
           tmp1 = _mm_srli_epi32 (b, 16);                  // b >> 16
           b = _mm_add_epi32(b, tmp1);                     // b + (b >> 16)
           b = _mm_and_si128(b, m4);                       // (b >> 16) & 0x0000003F;

           mcnt = _mm_add_epi32(mcnt, b);                  // mcnt += b
       }

       }
       // ---------------------------------------------------------------------
       {
           //__m128i b = _mm_load_si128(block);
           // TODO: SSE4...
           //w = _mm_extract_epi32(b, i);               

           const bm::word_t* BMRESTRICT blk = (const bm::word_t*) block;

           if (first_word)
           {
               first_word = false;
           }
           else
           {
               if (0!=(w0=blk[0]))
               {
                   count += bm::word_bitcount(tcnt[0]);
                   count -= !(w_prev ^ (w0 & 1));
                   count -= w_prev = (w0 >> w_shift);
               }
               else
               {
                   count -= !w_prev; w_prev ^= w_prev;
               }  
           }
           if (0!=(w0=blk[1]))
           {
               count += bm::word_bitcount(tcnt[1]);
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = (w0 >> w_shift);                    
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }    
           if (0!=(w0=blk[2]))
           {
               count += bm::word_bitcount(tcnt[2]);
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = (w0 >> w_shift);                    
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }      
           if (0!=(w0=blk[3]))
           {
               count += bm::word_bitcount(tcnt[3]);
               count -= !(w_prev ^ (w0 & 1));
               count -= w_prev = (w0 >> w_shift);                    
           }
           else
           {
               count -= !w_prev; w_prev ^= w_prev;
           }               
       }
   } while (++block < block_end);

   _mm_store_si128((__m128i*)tcnt, mcnt);
   *bit_count = tcnt[0] + tcnt[1] + tcnt[2] + tcnt[3];

   return unsigned(count);
}

#ifdef __GNUG__
// necessary measure to silence false warning from GCC about negative pointer arithmetics
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

/*!
SSE4.2 check for one to two (variable len) 128 bit SSE lines for gap search results (8 elements)
\internal
*/
inline
unsigned sse2_gap_find(const bm::gap_word_t* BMRESTRICT pbuf,
                       const bm::gap_word_t pos, unsigned size)
{
    BM_ASSERT(size <= 16);
    BM_ASSERT(size);

    const unsigned unroll_factor = 8;
    if (size < 4) // for very short vector use conventional scan
    {
        if      (pbuf[0] >= pos) { size = 0; }
        else if (pbuf[1] >= pos) { size = 1; }
        else                     { size = 2; BM_ASSERT(pbuf[2] >= pos); }
        return size;
    }

    __m128i m1, mz, maskF, maskFL;

    mz = _mm_setzero_si128();
    m1 = _mm_loadu_si128((__m128i*)(pbuf)); // load first 8 elements

    maskF = _mm_cmpeq_epi32(mz, mz); // set all FF
    maskFL = _mm_slli_si128(maskF, 4 * 2); // byle shift to make [0000 FFFF] 
    int shiftL = (64 - (unroll_factor - size) * 16);
    maskFL = _mm_slli_epi64(maskFL, shiftL); // additional bit shift to  [0000 00FF]

    m1 = _mm_andnot_si128(maskFL, m1); // m1 = (~mask) & m1
    m1 = _mm_or_si128(m1, maskFL);

    __m128i mp = _mm_set1_epi16(pos);  // broadcast pos into all elements of a SIMD vector
    __m128i  mge_mask = _mm_cmpeq_epi16(_mm_subs_epu16(mp, m1), mz); // unsigned m1 >= mp
    int mi = _mm_movemask_epi8(mge_mask);  // collect flag bits
    if (mi)
    {
        int bsr_i= bm::bit_scan_fwd(mi) >> 1;
        return bsr_i;   // address of first one element (target)
    }
    if (size == 8)
        return size;

    // inspect the next lane with possible step back (to avoid over-read the block boundaries)
    //   GCC gives a false warning for "- unroll_factor" here
    const bm::gap_word_t* BMRESTRICT pbuf2 = pbuf + size - unroll_factor;
    BM_ASSERT(pbuf2 > pbuf); // assert in place to make sure GCC warning is indeed false

    m1 = _mm_loadu_si128((__m128i*)(pbuf2)); // load next elements (with possible overlap)
    mge_mask = _mm_cmpeq_epi16(_mm_subs_epu16(mp, m1), mz); // m1 >= mp
    mi = _mm_movemask_epi8(mge_mask);
    if (mi)
    {
        int bsr_i = bm::bit_scan_fwd(mi) >> 1;
        return size - (unroll_factor - bsr_i);
    }
    return size;
}

/**
    Hybrid binary search, starts as binary, then switches to linear scan

   \param buf - GAP buffer pointer.
   \param pos - index of the element.
   \param is_set - output. GAP value (0 or 1).
   \return GAP index.

    @ingroup SSE2
*/
inline
unsigned sse2_gap_bfind(const unsigned short* BMRESTRICT buf,
                         unsigned pos, unsigned* BMRESTRICT is_set)
{
    unsigned start = 1;
    unsigned end = 1 + ((*buf) >> 3);

    const unsigned arr_end = end;
    BM_ASSERT(start != end);
    unsigned size = end - start;
    for (; size >= 16; size = end - start)
    {
        if (unsigned mid = (start + end) >> 1; buf[mid] < pos)
            start = mid + 1;
        else
            end = mid;
        if (unsigned mid = (start + end) >> 1; buf[mid] < pos)
            start = mid + 1;
        else
            end = mid;
    } // for

    size += (end != arr_end);
    start += bm::sse2_gap_find(buf + start, (bm::gap_word_t)pos, size);
    BM_ASSERT(buf[start] >= pos);
    BM_ASSERT(buf[start - 1] < pos || (start == 1));

    *is_set = ((*buf) & 1) ^ ((start-1) & 1);
    return start;
}

/**
    Hybrid binary search, starts as binary, then switches to scan
    @ingroup SSE2
*/
inline
unsigned sse2_gap_test(const unsigned short* BMRESTRICT buf, unsigned pos)
{
    unsigned is_set;
    bm::sse2_gap_bfind(buf, pos, &is_set);
    return is_set;
}




#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


#define VECT_XOR_ARR_2_MASK(dst, src, src_end, mask)\
    sse2_xor_arr_2_mask((__m128i*)(dst), (__m128i*)(src), (__m128i*)(src_end), (bm::word_t)mask)

#define VECT_ANDNOT_ARR_2_MASK(dst, src, src_end, mask)\
    sse2_andnot_arr_2_mask((__m128i*)(dst), (__m128i*)(src), (__m128i*)(src_end), (bm::word_t)mask)

#define VECT_BITCOUNT(first, last) \
    sse2_bit_count((__m128i*) (first), (__m128i*) (last))

#define VECT_BITCOUNT_AND(first, last, mask) \
    sse2_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_and)

#define VECT_BITCOUNT_OR(first, last, mask) \
    sse2_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_or)

#define VECT_BITCOUNT_XOR(first, last, mask) \
    sse2_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_xor)

#define VECT_BITCOUNT_SUB(first, last, mask) \
    sse2_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_sub)

#define VECT_INVERT_BLOCK(first) \
    sse2_invert_block((__m128i*)first);

#define VECT_AND_BLOCK(dst, src) \
    sse2_and_block((__m128i*) dst, (__m128i*) (src))

#define VECT_AND_DIGEST(dst, src) \
    sse2_and_digest((__m128i*) dst, (const __m128i*) (src))

#define VECT_AND_OR_DIGEST_2WAY(dst, src1, src2) \
    sse2_and_or_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_AND_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    sse2_and_digest_5way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2), (const __m128i*) (src3), (const __m128i*) (src4))

#define VECT_AND_DIGEST_3WAY(dst, src1, src2) \
    sse2_and_digest_3way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_AND_DIGEST_2WAY(dst, src1, src2) \
    sse2_and_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_OR_BLOCK(dst, src) \
    sse2_or_block((__m128i*) dst, (__m128i*) (src))

#define VECT_OR_BLOCK_2WAY(dst, src1, src2) \
    sse2_or_block_2way((__m128i*) (dst), (__m128i*) (src1), (__m128i*) (src2))

#define VECT_OR_BLOCK_3WAY(dst, src1, src2) \
    sse2_or_block_3way((__m128i*) (dst), (__m128i*) (src1), (__m128i*) (src2))

#define VECT_OR_BLOCK_5WAY(dst, src1, src2, src3, src4) \
    sse2_or_block_5way((__m128i*) (dst), (__m128i*) (src1), (__m128i*) (src2), (__m128i*) (src3), (__m128i*) (src4))

#define VECT_SUB_BLOCK(dst, src) \
    sse2_sub_block((__m128i*) dst, (__m128i*) (src))

#define VECT_SUB_DIGEST(dst, src) \
    sse2_sub_digest((__m128i*) dst, (const __m128i*) (src))

#define VECT_SUB_DIGEST_2WAY(dst, src1, src2) \
    sse2_sub_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_SUB_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    sse2_sub_digest_5way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2), (const __m128i*) (src3), (const __m128i*) (src4))

#define VECT_SUB_DIGEST_3WAY(dst, src1, src2) \
    sse2_sub_digest_3way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_XOR_BLOCK(dst, src) \
    sse2_xor_block((__m128i*) dst, (__m128i*) (src))

#define VECT_XOR_BLOCK_2WAY(dst, src1, src2) \
    sse2_xor_block_2way((__m128i*) (dst), (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_COPY_BLOCK(dst, src) \
    sse2_copy_block((__m128i*) dst, (__m128i*) (src))

#define VECT_COPY_BLOCK_UNALIGN(dst, src) \
    sse2_copy_block_unalign((__m128i*) dst, (__m128i*) (src))

#define VECT_STREAM_BLOCK(dst, src) \
    sse2_stream_block((__m128i*) dst, (__m128i*) (src))

#define VECT_STREAM_BLOCK_UNALIGN(dst, src) \
    sse2_stream_block_unalign((__m128i*) dst, (__m128i*) (src))

#define VECT_SET_BLOCK(dst, value) \
    sse2_set_block((__m128i*) dst, value)

#define VECT_IS_ZERO_BLOCK(dst) \
    sse2_is_all_zero((__m128i*) dst)

#define VECT_IS_ONE_BLOCK(dst) \
    sse2_is_all_one((__m128i*) dst)

#define VECT_IS_DIGEST_ZERO(start) \
    sse2_is_digest_zero((__m128i*)start)

#define VECT_BLOCK_SET_DIGEST(dst, val) \
    sse2_block_set_digest((__m128i*)dst, val)

#define VECT_LOWER_BOUND_SCAN_U32(arr, target, from, to) \
    sse2_lower_bound_scan_u32(arr, target, from, to)

#define VECT_SHIFT_R1(b, acc, co) \
    sse2_shift_r1((__m128i*)b, acc, co)


#define VECT_BIT_FIND_FIRST(src, off, pos) \
    sse2_bit_find_first((__m128i*) src, off, pos)

#define VECT_BIT_FIND_DIFF(src1, src2, pos) \
    sse2_bit_find_first_diff((__m128i*) src1, (__m128i*) (src2), pos)

#define VECT_BIT_BLOCK_XOR(t, src, src_xor, d) \
    sse2_bit_block_xor(t, src, src_xor, d)

#define VECT_BIT_BLOCK_XOR_2WAY(t, src_xor, d) \
    sse2_bit_block_xor_2way(t, src_xor, d)

#define VECT_GAP_BFIND(buf, pos, is_set) \
    sse2_gap_bfind(buf, pos, is_set)

#define VECT_GAP_TEST(buf, pos) \
    sse2_gap_test(buf, pos)

} // namespace


#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


#endif
