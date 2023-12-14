#ifndef BMSSE4__H__INCLUDED__
#define BMSSE4__H__INCLUDED__
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

/*! \file bmsse4.h
    \brief Compute functions for SSE4.2 SIMD instruction set (internal)

    Aside from SSE4.2 it also compiles in WASM SIMD mode
    for 128-bit SIMD portable target.
*/

#ifndef BMWASMSIMDOPT
#include<mmintrin.h>
#endif
#include<emmintrin.h>
#include<smmintrin.h>
#include<nmmintrin.h>
#include<immintrin.h>

#include "bmdef.h"
#include "bmutil.h"
#include "bmsse_util.h"

namespace bm
{

/** @defgroup SSE4 SSE4.2 funcions (internal)
    Processor specific optimizations for SSE4.2 instructions (internals)
    @internal
    @ingroup bvector
 */

#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4146)
#endif


// WASM build: define missing POPCNT intrinsics via GCC build-ins
#ifdef BMWASMSIMDOPT
# define _mm_popcnt_u32 __builtin_popcount
# define _mm_popcnt_u64 __builtin_popcountll
# define BM_BSF32 __builtin_ctz
#else
# define BM_BSF32 bm::bsf_asm32
#endif


/*
inline
void sse2_print128(const char* prefix, const __m128i & value)
{
    const size_t n = sizeof(__m128i) / sizeof(unsigned);
    unsigned buffer[n];
    _mm_storeu_si128((__m128i*)buffer, value);
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

/*!
    SSE4.2 optimized bitcounting .
    @ingroup SSE4
*/
inline 
bm::id_t sse4_bit_count(const __m128i* block, const __m128i* block_end) BMNOEXCEPT
{
    bm::id_t count = 0;
#ifdef BM64_SSE4
    const bm::id64_t* b = (bm::id64_t*) block;
    const bm::id64_t* b_end = (bm::id64_t*) block_end;
    do
    {
        count += unsigned( _mm_popcnt_u64(b[0]) +
                           _mm_popcnt_u64(b[1]) +
                           _mm_popcnt_u64(b[2]) +
                           _mm_popcnt_u64(b[3]));
        b += 4;
    } while (b < b_end);
#else
    do
    {
        const unsigned* b = (unsigned*) block;
        count += _mm_popcnt_u32(b[0]) +
                 _mm_popcnt_u32(b[1]) +
                 _mm_popcnt_u32(b[2]) +
                 _mm_popcnt_u32(b[3]);
    } while (++block < block_end);
#endif    
    return count;
}

#ifdef BM64_SSE4

/*!
    SSE4.2 optimized bitcounting, uses digest for positioning
    @ingroup SSE4
*/
inline
bm::id_t sse42_bit_count_digest(const bm::word_t* BMRESTRICT block,
                                bm::id64_t                   digest) BMNOEXCEPT
{
    BM_ASSERT(digest);

    bm::id_t count = 0;
    bm::id64_t d = digest;
    while (d)
    {
        const bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;
        const unsigned wave = (unsigned)_mm_popcnt_u64(t - 1);
        const unsigned off = wave * bm::set_block_digest_wave_size;

        const bm::bit_block_t::bunion_t* BMRESTRICT src_u =
                        (const bm::bit_block_t::bunion_t*)(&block[off]);
        unsigned j = 0;
        do
        {
            count +=
                    unsigned( _mm_popcnt_u64(src_u->w64[j]) +
                              _mm_popcnt_u64(src_u->w64[j+1]) +
                              _mm_popcnt_u64(src_u->w64[j+2]) +
                              _mm_popcnt_u64(src_u->w64[j+3]));
        } while ((j+=4) < bm::set_block_digest_wave_size/2);

        d = bm::bmi_bslr_u64(d); // d &= d - 1;
    }  // while (d);
    return count;
}

#endif

/*!
\internal
*/
BMFORCEINLINE 
unsigned op_xor(unsigned a, unsigned b) BMNOEXCEPT
{
    unsigned ret = (a ^ b);
    return ret;
}

/*!
\internal
*/
BMFORCEINLINE 
unsigned op_or(unsigned a, unsigned b) BMNOEXCEPT
{
    return (a | b);
}

/*!
\internal
*/
BMFORCEINLINE 
unsigned op_and(unsigned a, unsigned b) BMNOEXCEPT
{
    return (a & b);
}


template<class Func>
bm::id_t sse4_bit_count_op(const __m128i* BMRESTRICT block, 
                           const __m128i* BMRESTRICT block_end,
                           const __m128i* BMRESTRICT mask_block,
                           Func sse2_func) BMNOEXCEPT
{
    bm::id_t count = 0;
#ifdef BM64_SSE4
    bm::id64_t BM_ALIGN16 tcnt[2] BM_ALIGN16ATTR;
    do
    {
        __m128i b = sse2_func(_mm_load_si128(block), _mm_load_si128(mask_block));
        _mm_store_si128((__m128i*)tcnt, b);
        count += unsigned(_mm_popcnt_u64(tcnt[0]) + _mm_popcnt_u64(tcnt[1]));

        b = sse2_func(_mm_load_si128(block+1), _mm_load_si128(mask_block+1));
        _mm_store_si128((__m128i*)tcnt, b);
        count += unsigned(_mm_popcnt_u64(tcnt[0]) + _mm_popcnt_u64(tcnt[1]));
        block+=2; mask_block+=2;
    } while (block < block_end);
#else    
    do
    {
        __m128i tmp0 = _mm_load_si128(block);
        __m128i tmp1 = _mm_load_si128(mask_block);        
        __m128i b = sse2_func(tmp0, tmp1);

        count += _mm_popcnt_u32(_mm_extract_epi32(b, 0));
        count += _mm_popcnt_u32(_mm_extract_epi32(b, 1));
        count += _mm_popcnt_u32(_mm_extract_epi32(b, 2));
        count += _mm_popcnt_u32(_mm_extract_epi32(b, 3));

        ++block; ++mask_block;
    } while (block < block_end);
#endif
    
    return count;
}

/*!
    @brief check if block is all zero bits
    @ingroup SSE4
*/
inline
bool sse4_is_all_zero(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    __m128i w;
    __m128i maskz = _mm_setzero_si128();
    const __m128i* BMRESTRICT block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        w = _mm_or_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
        if (!_mm_test_all_ones(_mm_cmpeq_epi8(w, maskz))) // (w0 | w1) != maskz
            return false;
        w = _mm_or_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
        if (!_mm_test_all_ones(_mm_cmpeq_epi8(w, maskz))) // (w0 | w1) != maskz
            return false;
        block += 4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if digest stride is all zero bits
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse4_is_digest_zero(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    __m128i wA = _mm_or_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
    __m128i wB = _mm_or_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
    wA = _mm_or_si128(wA, wB);
    bool z1 = _mm_test_all_zeros(wA, wA);

    wA = _mm_or_si128(_mm_load_si128(block+4), _mm_load_si128(block+5));
    wB = _mm_or_si128(_mm_load_si128(block+6), _mm_load_si128(block+7));
    wA = _mm_or_si128(wA, wB);
    bool z2 = _mm_test_all_zeros(wA, wA);
    return z1 & z2;
}

/*!
    @brief set digest stride to 0xFF.. or 0x0 value
    @ingroup SSE4
*/
BMFORCEINLINE
void sse4_block_set_digest(__m128i* dst, unsigned value) BMNOEXCEPT
{
    __m128i mV = _mm_set1_epi32(int(value));
    _mm_store_si128(dst, mV);     _mm_store_si128(dst + 1, mV); 
    _mm_store_si128(dst + 2, mV); _mm_store_si128(dst + 3, mV); 
    _mm_store_si128(dst + 4, mV); _mm_store_si128(dst + 5, mV); 
    _mm_store_si128(dst + 6, mV); _mm_store_si128(dst + 7, mV);
}


/*!
    @brief AND blocks2
    *dst &= *src
 
    @return 0 if no bits were set
    @ingroup SSE4
*/
inline
unsigned sse4_and_block(__m128i* BMRESTRICT dst,
                       const __m128i* BMRESTRICT src) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i accA, accB, accC, accD;
    
    const __m128i* BMRESTRICT src_end =
        (const __m128i*)((bm::word_t*)(src) + bm::set_block_size);

    accA = accB = accC = accD = _mm_setzero_si128();

    do
    {
        m1A = _mm_and_si128(_mm_load_si128(src+0), _mm_load_si128(dst+0));
        m1B = _mm_and_si128(_mm_load_si128(src+1), _mm_load_si128(dst+1));
        m1C = _mm_and_si128(_mm_load_si128(src+2), _mm_load_si128(dst+2));
        m1D = _mm_and_si128(_mm_load_si128(src+3), _mm_load_si128(dst+3));

        _mm_store_si128(dst+0, m1A);
        _mm_store_si128(dst+1, m1B);
        _mm_store_si128(dst+2, m1C);
        _mm_store_si128(dst+3, m1D);

        accA = _mm_or_si128(accA, m1A);
        accB = _mm_or_si128(accB, m1B);
        accC = _mm_or_si128(accC, m1C);
        accD = _mm_or_si128(accD, m1D);
        
        src += 4; dst += 4;
    } while (src < src_end);
    
    accA = _mm_or_si128(accA, accB); // A = A | B
    accC = _mm_or_si128(accC, accD); // C = C | D
    accA = _mm_or_si128(accA, accC); // A = A | C
    
    return !_mm_testz_si128(accA, accA);
}


/*!
    @brief AND block digest stride
    *dst &= *src
 
    @return true if stide is all zero
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse4_and_digest(__m128i* BMRESTRICT dst,
                     const __m128i* BMRESTRICT src) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;

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
    
     bool z1 = _mm_testz_si128(m1A, m1A);
    
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
    
     bool z2 = _mm_testz_si128(m1A, m1A);
    
     return z1 & z2;
}

/*!
    @brief AND block digest stride
    *dst = *src1 & src2
 
    @return true if stide is all zero
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse4_and_digest_2way(__m128i* BMRESTRICT dst,
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
    
     bool z1 = _mm_testz_si128(m1A, m1A);
    
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
    
     bool z2 = _mm_testz_si128(m1A, m1A);
    
     return z1 & z2;
}

/*!
    @brief AND-OR block digest stride
    *dst |= *src1 & src2

    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse4_and_or_digest_2way(__m128i* BMRESTRICT dst,
                             const __m128i* BMRESTRICT src1,
                             const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i mACC1;

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));

    mACC1 = _mm_or_si128(_mm_or_si128(m1A, m1B), _mm_or_si128(m1C, m1D));
    bool z1 = _mm_testz_si128(mACC1, mACC1);

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
    bool z2 = _mm_testz_si128(mACC1, mACC1);

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
    @ingroup SSE4
*/
inline
bool sse4_and_digest_3way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;

    m1A = _mm_and_si128(_mm_load_si128(src1+0), _mm_load_si128(src2+0));
    m1B = _mm_and_si128(_mm_load_si128(src1+1), _mm_load_si128(src2+1));
    m1C = _mm_and_si128(_mm_load_si128(src1+2), _mm_load_si128(src2+2));
    m1D = _mm_and_si128(_mm_load_si128(src1+3), _mm_load_si128(src2+3));


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

     bool z1 = _mm_testz_si128(m1A, m1A);

    m1A = _mm_and_si128(_mm_load_si128(src1+4), _mm_load_si128(src2+4));
    m1B = _mm_and_si128(_mm_load_si128(src1+5), _mm_load_si128(src2+5));
    m1C = _mm_and_si128(_mm_load_si128(src1+6), _mm_load_si128(src2+6));
    m1D = _mm_and_si128(_mm_load_si128(src1+7), _mm_load_si128(src2+7));


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

     bool z2 = _mm_testz_si128(m1A, m1A);

     return z1 & z2;
}



/*!
    @brief AND block digest stride
    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse4_and_digest_5way(__m128i* BMRESTRICT dst,
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
    
     bool z1 = _mm_testz_si128(m1A, m1A);
    
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
    
     bool z2 = _mm_testz_si128(m1A, m1A);
    
     return z1 & z2;
}



/*!
    @brief SUB (AND NOT) block digest stride
    *dst &= ~*src
 
    @return true if stide is all zero
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse4_sub_digest(__m128i* BMRESTRICT dst,
                     const __m128i* BMRESTRICT src) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;

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
    
     bool z1 = _mm_testz_si128(m1A, m1A);
    
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
    
     bool z2 = _mm_testz_si128(m1A, m1A);
    
     return z1 & z2;
}


/*!
    @brief 2-operand SUB (AND NOT) block digest stride
    *dst = src1 & ~*src2
 
    @return true if stide is all zero
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse4_sub_digest_2way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;

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
    
     bool z1 = _mm_testz_si128(m1A, m1A);
    
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
    
     bool z2 = _mm_testz_si128(m1A, m1A);
    
     return z1 & z2;
}

/*!
    @brief SUB block digest stride
    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse4_sub_digest_5way(__m128i* BMRESTRICT dst,
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

     bool z1 = _mm_testz_si128(m1A, m1A);

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

     bool z2 = _mm_testz_si128(m1A, m1A);

     return z1 & z2;
}


/*!
    @brief SUB block digest stride
    @return true if stide is all zero
    @ingroup SSE4
*/
inline
bool sse4_sub_digest_3way(__m128i* BMRESTRICT dst,
                          const __m128i* BMRESTRICT src1,
                          const __m128i* BMRESTRICT src2) BMNOEXCEPT
{
    __m128i m1A, m1B, m1C, m1D;
    __m128i maskFF  = _mm_set1_epi32(~0u);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+0)), _mm_xor_si128(maskFF,_mm_load_si128(src2+0)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+1)), _mm_xor_si128(maskFF,_mm_load_si128(src2+1)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+2)), _mm_xor_si128(maskFF,_mm_load_si128(src2+2)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+3)), _mm_xor_si128(maskFF,_mm_load_si128(src2+3)));

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

     bool z1 = _mm_testz_si128(m1A, m1A);

    m1A = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+4)), _mm_xor_si128(maskFF,_mm_load_si128(src2+4)));
    m1B = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+5)), _mm_xor_si128(maskFF,_mm_load_si128(src2+5)));
    m1C = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+6)), _mm_xor_si128(maskFF,_mm_load_si128(src2+6)));
    m1D = _mm_and_si128(_mm_xor_si128(maskFF,_mm_load_si128(src1+7)), _mm_xor_si128(maskFF,_mm_load_si128(src2+7)));

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

     bool z2 = _mm_testz_si128(m1A, m1A);

     return z1 & z2;
}




/*!
    @brief check if block is all ONE bits
    @ingroup SSE4
*/
inline
bool sse4_is_all_one(const __m128i* BMRESTRICT block) BMNOEXCEPT
{
    __m128i w;
    const __m128i* BMRESTRICT block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);

    do
    {
        w = _mm_and_si128(_mm_load_si128(block+0), _mm_load_si128(block+1));
        if (!_mm_test_all_ones(w))
            return false;
        w = _mm_and_si128(_mm_load_si128(block+2), _mm_load_si128(block+3));
        if (!_mm_test_all_ones(w))
            return false;

        block+=4;
    } while (block < block_end);
    return true;
}

/*!
    @brief check if SSE wave is all oxFFFF...FFF
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse42_test_all_one_wave(const void* ptr) BMNOEXCEPT
{
    return _mm_test_all_ones(_mm_loadu_si128((__m128i*)ptr));
}


/*!
    @brief check if wave of pointers is all NULL
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse42_test_all_zero_wave(const void* ptr) BMNOEXCEPT
{
    __m128i w0 = _mm_loadu_si128((__m128i*)ptr);
    return _mm_testz_si128(w0, w0);
}

/*!
    @brief check if 2 waves of pointers are all NULL
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse42_test_all_zero_wave2(const void* ptr0, const void* ptr1) BMNOEXCEPT
{
    __m128i w0 = _mm_loadu_si128((__m128i*)ptr0);
    __m128i w1 = _mm_loadu_si128((__m128i*)ptr1);
    w0 = _mm_or_si128(w0, w1);
    return _mm_testz_si128(w0, w0);
}

/*!
    @brief check if wave of 2 pointers are the same (null or FULL)
    @ingroup SSE4
*/
BMFORCEINLINE
bool sse42_test_all_eq_wave2(const void* ptr0, const void* ptr1) BMNOEXCEPT
{
    __m128i w0 = _mm_loadu_si128((__m128i*)ptr0);
    __m128i w1 = _mm_loadu_si128((__m128i*)ptr1);
    w0 = _mm_xor_si128(w0, w1);
    return _mm_testz_si128(w0, w0);
}


/*!
    SSE4.2 calculate number of bit changes from 0 to 1
    @ingroup SSE4
*/
inline
unsigned sse42_bit_block_calc_change(const __m128i* BMRESTRICT block,
                                     unsigned size) BMNOEXCEPT
{
    bm::id64_t BM_ALIGN32 tcnt[2] BM_ALIGN32ATTR;

    const __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + size); // bm::set_block_size
    __m128i m1COshft, m2COshft;
    
    unsigned w0 = *((bm::word_t*)(block));
    unsigned count = 1;

    unsigned co2, co1 = 0;
    for (;block < block_end; block += 2)
    {
        __m128i m1A = _mm_load_si128(block);
        __m128i m2A = _mm_load_si128(block+1);

        __m128i m1CO = _mm_srli_epi32(m1A, 31);
        __m128i m2CO = _mm_srli_epi32(m2A, 31);
        
        co2 = _mm_extract_epi32(m1CO, 3);
        
        __m128i m1As = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m128i m2As = _mm_slli_epi32(m2A, 1);
        
        m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift left by 1 int32
        m1COshft = _mm_insert_epi32 (m1COshft, co1, 0);
        
        co1 = co2;
        
        co2 = _mm_extract_epi32(m2CO, 3);
        
        m2COshft = _mm_slli_si128 (m2CO, 4);
        m2COshft = _mm_insert_epi32 (m2COshft, co1, 0);
        
        m1As = _mm_or_si128(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm_or_si128(m2As, m2COshft);
        
        co1 = co2;
        
        // we now have two shifted SSE4 regs with carry-over
        m1A = _mm_xor_si128(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm_xor_si128(m2A, m2As);
        
#ifdef BM64_SSE4
       _mm_store_si128((__m128i*)tcnt, m1A);
        count += unsigned(_mm_popcnt_u64(tcnt[0]) + _mm_popcnt_u64(tcnt[1]));
       _mm_store_si128((__m128i*)tcnt, m2A);
        count += unsigned(_mm_popcnt_u64(tcnt[0]) + _mm_popcnt_u64(tcnt[1]));
#else
        bm::id_t m0 = _mm_extract_epi32(m1A, 0);
        bm::id_t m1 = _mm_extract_epi32(m1A, 1);
        bm::id_t m2 = _mm_extract_epi32(m1A, 2);
        bm::id_t m3 = _mm_extract_epi32(m1A, 3);
        count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                          _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));

        m0 = _mm_extract_epi32(m2A, 0);
        m1 = _mm_extract_epi32(m2A, 1);
        m2 = _mm_extract_epi32(m2A, 2);
        m3 = _mm_extract_epi32(m2A, 3);
        count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                          _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));
#endif

    }
    count -= (w0 & 1u); // correct initial carry-in error
    return count;
}


/*!
    SSE4.2 calculate number of bit changes from 0 to 1 of a XOR product
    @ingroup SSE4
*/
inline
void sse42_bit_block_calc_xor_change(const __m128i* BMRESTRICT block,
                                     const __m128i* BMRESTRICT xor_block,
                                     unsigned size,
                                     unsigned* BMRESTRICT gc,
                                     unsigned* BMRESTRICT bc) BMNOEXCEPT
{
#ifdef BM64_SSE4
    bm::id64_t BM_ALIGN32 simd_buf0[2] BM_ALIGN32ATTR;
    bm::id64_t BM_ALIGN32 simd_buf1[2] BM_ALIGN32ATTR;
#endif

    const __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + size);
    __m128i m1COshft, m2COshft;

    unsigned w0 = *((bm::word_t*)(block));
    unsigned gap_count = 1;
    unsigned bit_count = 0;

    unsigned co2, co1 = 0;
    for (;block < block_end; block += 2, xor_block += 2)
    {
        __m128i m1A = _mm_load_si128(block);
        __m128i m2A = _mm_load_si128(block+1);
        __m128i m1B = _mm_load_si128(xor_block);
        __m128i m2B = _mm_load_si128(xor_block+1);

        m1A = _mm_xor_si128(m1A, m1B);
        m2A = _mm_xor_si128(m2A, m2B);

        {
#ifdef BM64_SSE4
        _mm_store_si128 ((__m128i*)simd_buf0, m1A);
        _mm_store_si128 ((__m128i*)simd_buf1, m2A);
        bit_count += unsigned(_mm_popcnt_u64(simd_buf0[0]) + _mm_popcnt_u64(simd_buf0[1]));
        bit_count += unsigned(_mm_popcnt_u64(simd_buf1[0]) + _mm_popcnt_u64(simd_buf1[1]));
#else
        bm::id_t m0 = _mm_extract_epi32(m1A, 0);
        bm::id_t m1 = _mm_extract_epi32(m1A, 1);
        bm::id_t m2 = _mm_extract_epi32(m1A, 2);
        bm::id_t m3 = _mm_extract_epi32(m1A, 3);
        bit_count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                          _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));

        m0 = _mm_extract_epi32(m2A, 0);
        m1 = _mm_extract_epi32(m2A, 1);
        m2 = _mm_extract_epi32(m2A, 2);
        m3 = _mm_extract_epi32(m2A, 3);
        bit_count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                             _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));
#endif
        }

        __m128i m1CO = _mm_srli_epi32(m1A, 31);
        __m128i m2CO = _mm_srli_epi32(m2A, 31);

        co2 = _mm_extract_epi32(m1CO, 3);

        __m128i m1As = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m128i m2As = _mm_slli_epi32(m2A, 1);

        m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift left by 1 int32
        m1COshft = _mm_insert_epi32 (m1COshft, co1, 0);

        co1 = co2;

        co2 = _mm_extract_epi32(m2CO, 3);

        m2COshft = _mm_slli_si128 (m2CO, 4);
        m2COshft = _mm_insert_epi32 (m2COshft, co1, 0);

        m1As = _mm_or_si128(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm_or_si128(m2As, m2COshft);

        co1 = co2;

        // we now have two shifted SSE4 regs with carry-over
        m1A = _mm_xor_si128(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm_xor_si128(m2A, m2As);

#ifdef BM64_SSE4
        _mm_store_si128 ((__m128i*)simd_buf0, m1A);
        _mm_store_si128 ((__m128i*)simd_buf1, m2A);
        gap_count += unsigned(_mm_popcnt_u64(simd_buf0[0]) + _mm_popcnt_u64(simd_buf0[1]));
        gap_count += unsigned(_mm_popcnt_u64(simd_buf1[0]) + _mm_popcnt_u64(simd_buf1[1]));
#else
        bm::id_t m0 = _mm_extract_epi32(m1A, 0);
        bm::id_t m1 = _mm_extract_epi32(m1A, 1);
        bm::id_t m2 = _mm_extract_epi32(m1A, 2);
        bm::id_t m3 = _mm_extract_epi32(m1A, 3);
        gap_count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                          _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));

        m0 = _mm_extract_epi32(m2A, 0);
        m1 = _mm_extract_epi32(m2A, 1);
        m2 = _mm_extract_epi32(m2A, 2);
        m3 = _mm_extract_epi32(m2A, 3);
        gap_count += unsigned(_mm_popcnt_u32(m0) + _mm_popcnt_u32(m1) +
                          _mm_popcnt_u32(m2) + _mm_popcnt_u32(m3));
#endif

    }
    gap_count -= (w0 & 1u); // correct initial carry-in error
    if (!gap_count)
        ++gap_count; // must be >0
    *gc = gap_count;
    *bc = bit_count;
}



#ifdef BM64_SSE4

/*!
    SSE4.2 calculate number of bit changes from 0 to 1
    @ingroup SSE4
*/
inline
void sse42_bit_block_calc_change_bc(const __m128i* BMRESTRICT block,
                                    unsigned* gc, unsigned* bc) BMNOEXCEPT
{
    const __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    __m128i m1COshft, m2COshft;
    
    unsigned w0 = *((bm::word_t*)(block));
    unsigned bit_count = 0;
    unsigned gap_count = 1;

    unsigned co2, co1 = 0;
    for (;block < block_end; block += 2)
    {
        __m128i m1A = _mm_load_si128(block);
        __m128i m2A = _mm_load_si128(block+1);
        {
            bm::id64_t m0 = _mm_extract_epi64(m1A, 0);
            bm::id64_t m1 = _mm_extract_epi64(m1A, 1);
            bit_count += unsigned(_mm_popcnt_u64(m0) + _mm_popcnt_u64(m1));
            m0 = _mm_extract_epi64(m2A, 0);
            m1 = _mm_extract_epi64(m2A, 1);
            bit_count += unsigned(_mm_popcnt_u64(m0) + _mm_popcnt_u64(m1));
        }

        __m128i m1CO = _mm_srli_epi32(m1A, 31);
        __m128i m2CO = _mm_srli_epi32(m2A, 31);
        
        co2 = _mm_extract_epi32(m1CO, 3);
        
        __m128i m1As = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
        __m128i m2As = _mm_slli_epi32(m2A, 1);
        
        m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift left by 1 int32
        m1COshft = _mm_insert_epi32 (m1COshft, co1, 0);
        
        co1 = co2;
        
        co2 = _mm_extract_epi32(m2CO, 3);
        
        m2COshft = _mm_slli_si128 (m2CO, 4);
        m2COshft = _mm_insert_epi32 (m2COshft, co1, 0);
        
        m1As = _mm_or_si128(m1As, m1COshft); // block[i] |= co_flag
        m2As = _mm_or_si128(m2As, m2COshft);
        
        co1 = co2;
        
        // we now have two shifted SSE4 regs with carry-over
        m1A = _mm_xor_si128(m1A, m1As); // w ^= (w >> 1);
        m2A = _mm_xor_si128(m2A, m2As);
        {
            bm::id64_t m0 = _mm_extract_epi64(m1A, 0);
            bm::id64_t m1 = _mm_extract_epi64(m1A, 1);
            gap_count += unsigned(_mm_popcnt_u64(m0) + _mm_popcnt_u64(m1));
        }

        bm::id64_t m0 = _mm_extract_epi64(m2A, 0);
        bm::id64_t m1 = _mm_extract_epi64(m2A, 1);
        gap_count += unsigned(_mm_popcnt_u64(m0) + _mm_popcnt_u64(m1));

    }
    gap_count -= (w0 & 1u); // correct initial carry-in error
    *gc = gap_count;
    *bc = bit_count;
}

#endif


/*!
   \brief Find first bit which is different between two bit-blocks
  @ingroup SSE4
*/
inline
bool sse42_bit_find_first_diff(const __m128i* BMRESTRICT block1,
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
        if (!_mm_test_all_zeros(mOR, mOR)) // test 2x128 lanes
        {
            if (!_mm_test_all_zeros(mA, mA))
            {
                unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mA, maskZ));
                mask = ~mask; // invert to find (w != 0)
                BM_ASSERT(mask);
                int bsf = BM_BSF32(mask); // find first !=0 (could use lzcnt())
                _mm_store_si128 ((__m128i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx]; // _mm_extract_epi32 (mA, widx);
                bsf = BM_BSF32(w); // find first bit != 0
                *pos = (simd_lane * 128) + (widx * 32) + bsf;
                return true;
            }
            unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mB, maskZ));
            mask = ~mask; // invert to find (w != 0)
            BM_ASSERT(mask);
            int bsf = BM_BSF32(mask); // find first !=0 (could use lzcnt())
            _mm_store_si128 ((__m128i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx]; // _mm_extract_epi32 (mB, widx);
            bsf = BM_BSF32(w); // find first bit != 0
            *pos = ((++simd_lane) * 128) + (widx * 32) + bsf;
            return true;
        }

        simd_lane+=2;
        block1+=2; block2+=2;

    } while (block1 < block1_end);
    return false;
}


/*!
   \brief Find first non-zero bit
  @ingroup SSE4
*/
inline
bool sse42_bit_find_first(const __m128i* BMRESTRICT block,
                          unsigned off,
                          unsigned* pos) BMNOEXCEPT
{
    unsigned BM_ALIGN32 simd_buf[4] BM_ALIGN32ATTR;

    block = (const __m128i*)((const bm::word_t*)(block) + off);
    const __m128i* block_end =
        (const __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    const __m128i maskZ = _mm_setzero_si128();
    __m128i mA, mB;
    unsigned simd_lane = 0;
    do
    {
        mA = _mm_load_si128(block); mB = _mm_load_si128(block+1);
        __m128i mOR = _mm_or_si128(mA, mB);
        if (!_mm_test_all_zeros(mOR, mOR)) // test 2x128 lanes
        {
            if (!_mm_test_all_zeros(mA, mA))
            {
                unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mA, maskZ));
                mask = ~mask; // invert to find (w != 0)
                BM_ASSERT(mask);
                int bsf = BM_BSF32(mask); // find first !=0 (could use lzcnt())
                _mm_store_si128 ((__m128i*)simd_buf, mA);
                unsigned widx = bsf >> 2; // (bsf / 4);
                unsigned w = simd_buf[widx];
                bsf = BM_BSF32(w); // find first bit != 0
                *pos = (off * 32) + (simd_lane * 128) + (widx * 32) + bsf;
                return true;
            }
            unsigned mask = _mm_movemask_epi8(_mm_cmpeq_epi32(mB, maskZ));
            mask = ~mask; // invert to find (w != 0)
            BM_ASSERT(mask);
            int bsf = BM_BSF32(mask); // find first !=0 (could use lzcnt())
            _mm_store_si128 ((__m128i*)simd_buf, mB);
            unsigned widx = bsf >> 2; // (bsf / 4);
            unsigned w = simd_buf[widx];
            bsf = BM_BSF32(w); // find first bit != 0
            *pos = (off * 32) + ((++simd_lane) * 128) + (widx * 32) + bsf;
            return true;
        }

        simd_lane+=2;
        block+=2;

    } while (block < block_end);
    return false;
}




#ifdef __GNUG__
// necessary measure to silence false warning from GCC about negative pointer arithmetics
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

/*!
     SSE4.2 check for one to two (variable len) 128 bit SSE lines
     for gap search results (8 elements)
     @ingroup SSE4
     \internal
*/
inline
unsigned sse4_gap_find(const bm::gap_word_t* BMRESTRICT pbuf,
                       const bm::gap_word_t pos, const unsigned size) BMNOEXCEPT
{
    BM_ASSERT(size <= 16);
    BM_ASSERT(size >= 4);
    const unsigned unroll_factor = 8;

    __m128i m1, mz, maskF, maskFL;

    mz = _mm_setzero_si128();
    m1 = _mm_loadu_si128((__m128i*)(pbuf)); // load first 8 elements

    maskF = _mm_cmpeq_epi64(mz, mz); // set all FF
    maskFL = _mm_slli_si128(maskF, 4 * 2); // byte shift to make [0000 FFFF]
    int shiftL= (64 - (unroll_factor - size) * 16);
    maskFL = _mm_slli_epi64(maskFL, shiftL); // additional bit shift to  [0000 00FF]

    m1 = _mm_andnot_si128(maskFL, m1); // m1 = (~mask) & m1
    m1 = _mm_or_si128(m1, maskFL);

    __m128i mp = _mm_set1_epi16(pos);  // broadcast pos into all elements of a SIMD vector
    __m128i  mge_mask = _mm_cmpeq_epi16(_mm_subs_epu16(mp, m1), mz); // unsigned m1 >= mp
    __m128i  c_mask = _mm_slli_epi16(mge_mask, 15); // clear not needed flag bits by shift
    int mi = _mm_movemask_epi8(c_mask);  // collect flag bits
    if (unsigned bc = _mm_popcnt_u32(mi)) // gives us number of elements >= pos
        return unroll_factor - bc;   // address of first one element (target)
    // inspect the next lane with possible step back (to avoid over-read the block boundaries)
    //   GCC gives a false warning for "- unroll_factor" here
    const bm::gap_word_t* BMRESTRICT pbuf2 = pbuf + size - unroll_factor;
    BM_ASSERT(pbuf2 > pbuf || size == 8); // assert in place to make sure GCC warning is indeed false

    m1 = _mm_loadu_si128((__m128i*)(pbuf2)); // load next elements (with possible overlap)
    mge_mask = _mm_cmpeq_epi16(_mm_subs_epu16(mp, m1), mz); // m1 >= mp        
    mi = _mm_movemask_epi8(_mm_slli_epi16(mge_mask, 15));
    unsigned bc = _mm_popcnt_u32(mi); 

    return size - bc;
}

/**
    Hybrid binary search, starts as binary, then switches to linear scan

   \param buf - GAP buffer pointer.
   \param pos - index of the element.
   \param is_set - output. GAP value (0 or 1).
   \return GAP index.

    @ingroup SSE4
*/
inline
unsigned sse42_gap_bfind(const unsigned short* BMRESTRICT buf,
                         unsigned pos, unsigned* BMRESTRICT is_set) BMNOEXCEPT
{
    unsigned start = 1;
    unsigned end = 1 + ((*buf) >> 3);

    const unsigned arr_end = end;
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
    if (size < 4) // for very short vector use conventional scan
    {
        const unsigned short* BMRESTRICT pbuf = buf + start;
        if (pbuf[0] >= pos) { }
        else if (pbuf[1] >= pos) { start++; }
        else
        {
            BM_ASSERT(pbuf[2] >= pos);
            start+=2;
        }
    }
    else
    {
        start += bm::sse4_gap_find(buf+start, (bm::gap_word_t)pos, size);
    }
    *is_set = ((*buf) & 1) ^ ((start-1) & 1);
    return start;
}

/*
inline
unsigned sse42_gap_bfind(const unsigned short* BMRESTRICT buf,
                         unsigned pos, unsigned* BMRESTRICT is_set) BMNOEXCEPT
{
    unsigned start = 1;
    unsigned end = start + ((*buf) >> 3);

    if (unsigned dsize = end - start; dsize < 17)
    {
        start = bm::sse4_gap_find(buf+1, (bm::gap_word_t)pos, dsize);
        *is_set = ((*buf) & 1) ^ (start & 1);
        BM_ASSERT(buf[start+1] >= pos);
        BM_ASSERT(buf[start] < pos || (start==0));
        return start+1;
    }
    const unsigned arr_end = end;
    BM_ASSERT (start != end);
    do
    {
        if (unsigned curr = (start + end) >> 1; buf[curr] < pos)
            start = curr + 1;
        else
            end = curr;
        if (unsigned size = end - start; size < 16)
        {
            size += (end != arr_end);
            unsigned idx =
                bm::sse4_gap_find(buf + start, (bm::gap_word_t)pos, size);
            start += idx;

            BM_ASSERT(buf[start] >= pos);
            BM_ASSERT(buf[start - 1] < pos || (start == 1));
            break;
        }
    } while (start != end);

    *is_set = ((*buf) & 1) ^ ((start-1) & 1);
    return start;
}
*/

/**
    Hybrid binary search to test GAP value, starts as binary, then switches to scan
    @return test result
    @ingroup SSE4
*/
unsigned sse42_gap_test(const unsigned short* BMRESTRICT buf, unsigned pos) BMNOEXCEPT
{
    unsigned start = 1;
    unsigned end = start + ((*buf) >> 3);
    unsigned size = end - start;
    const unsigned arr_end = end;
    for (; size >= 64; size = end - start)
    {
        unsigned mid = (start + end) >> 1;
        if (buf[mid] < pos)
            start = mid+1;
        else
            end = mid;
        if (buf[mid = (start + end) >> 1] < pos)
            start = mid+1;
        else
            end = mid;
        if (buf[mid = (start + end) >> 1] < pos)
            start = mid+1;
        else
            end = mid;
        if (buf[mid = (start + end) >> 1] < pos)
            start = mid+1;
        else
            end = mid;
    } // for
    for (; size >= 16; size = end - start)
    {
        if (unsigned mid = (start + end) >> 1; buf[mid] < pos)
            start = mid+1;
        else
            end = mid;
    } // for
    size += (end != arr_end);
    if (size < 4) // for very short vector use conventional scan
    {
        const unsigned short* BMRESTRICT pbuf = buf + start;
        if (pbuf[0] >= pos) { }
        else if (pbuf[1] >= pos) { start++; }
        else
        {
            BM_ASSERT(pbuf[2] >= pos);
            start+=2;
        }
    }
    else
    {
        start += bm::sse4_gap_find(buf+start, (bm::gap_word_t)pos, size);
    }
    BM_ASSERT(buf[start] >= pos);
    BM_ASSERT(buf[start - 1] < pos || (start == 1));

    return ((*buf) & 1) ^ ((--start) & 1);
}


/*
unsigned sse42_gap_test(const unsigned short* BMRESTRICT buf, unsigned pos) BMNOEXCEPT
{
    unsigned start = 1;
    unsigned end = start + ((*buf) >> 3);

    unsigned size = end - start;
    if (size < 17)
    {
        start = bm::sse4_gap_find(buf + start, (bm::gap_word_t)pos, size);
        BM_ASSERT(buf[start+1] >= pos);
        BM_ASSERT(buf[start] < pos || (start==0));
        return ((*buf) & 1) ^ (start & 1);
    }
    const unsigned arr_end = end;
    BM_ASSERT (start != end);
    do
    {
        if (unsigned curr = (start + end) >> 1; buf[curr] < pos)
            start = curr + 1;
        else
            end = curr;
        if (unsigned curr = (start + end) >> 1; buf[curr] < pos)
            start = curr + 1;
        else
            end = curr;

        size = end - start;
        if (size < 16)
        {
            size += (end != arr_end);
            unsigned idx =
                bm::sse4_gap_find(buf + start, (bm::gap_word_t)pos, size);
            start += idx;
            BM_ASSERT(buf[start] >= pos);
            BM_ASSERT(buf[start - 1] < pos || (start == 1));
            break;
        }
    } while (1);

    return ((*buf) & 1) ^ ((--start) & 1);
}
*/

/**
    Experimental (test) function to do SIMD vector search (lower bound)
    in sorted, growing array
    @ingroup SSE4

    \internal
*/
inline
int sse42_cmpge_u32(__m128i vect4, unsigned value) BMNOEXCEPT
{
    // a > b (unsigned, 32-bit) is the same as (a - 0x80000000) > (b - 0x80000000) (signed, 32-bit)
    // https://fgiesen.wordpress.com/2016/04/03/sse-mind-the-gap/
    //
    __m128i mask0x8 = _mm_set1_epi32(0x80000000);
    __m128i mm_val = _mm_set1_epi32(value);
    
    __m128i norm_vect4 = _mm_sub_epi32(vect4, mask0x8); // (signed) vect4 - 0x80000000
    __m128i norm_val = _mm_sub_epi32(mm_val, mask0x8);  // (signed) mm_val - 0x80000000
    
    __m128i cmp_mask_gt = _mm_cmpgt_epi32 (norm_vect4, norm_val);
    __m128i cmp_mask_eq = _mm_cmpeq_epi32 (mm_val, vect4);
    
    __m128i cmp_mask_ge = _mm_or_si128 (cmp_mask_gt, cmp_mask_eq);
    int mask = _mm_movemask_epi8(cmp_mask_ge);
    if (mask)
    {
        int bsf = BM_BSF32(mask);//_bit_scan_forward(mask);
        return bsf / 4;
    }
    return -1;
}



/*!
     SSE4.2 index lookup to check what belongs to the same block (8 elements)
     \internal
*/
inline
unsigned sse42_idx_arr_block_lookup(const unsigned* idx, unsigned size,
                                   unsigned nb, unsigned start) BMNOEXCEPT
{
    const unsigned unroll_factor = 8;
    const unsigned len = (size - start);
    const unsigned len_unr = len - (len % unroll_factor);
    unsigned k;

    idx += start;

    __m128i nbM = _mm_set1_epi32(nb);

    for (k = 0; k < len_unr; k+=unroll_factor)
    {
        __m128i idxA = _mm_loadu_si128((__m128i*)(idx+k));
        __m128i idxB = _mm_loadu_si128((__m128i*)(idx+k+4));
        __m128i nbA =  _mm_srli_epi32(idxA, bm::set_block_shift); // idx[k] >> bm::set_block_shift
        __m128i nbB =  _mm_srli_epi32(idxB, bm::set_block_shift);
        
        if (!_mm_test_all_ones(_mm_cmpeq_epi32(nbM, nbA)) |
            !_mm_test_all_ones(_mm_cmpeq_epi32 (nbM, nbB)))
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
     SSE4.2 bulk bit set
     \internal
*/
inline
void sse42_set_block_bits(bm::word_t* BMRESTRICT block,
                          const unsigned* BMRESTRICT idx,
                          unsigned start, unsigned stop ) BMNOEXCEPT
{
    const unsigned unroll_factor = 4;
    const unsigned len = (stop - start);
    const unsigned len_unr = len - (len % unroll_factor);

    idx += start;

    unsigned BM_ALIGN16 mshift_v[4] BM_ALIGN16ATTR;
    unsigned BM_ALIGN16 mword_v[4] BM_ALIGN16ATTR;

    __m128i sb_mask = _mm_set1_epi32(bm::set_block_mask);
    __m128i sw_mask = _mm_set1_epi32(bm::set_word_mask);
    
    unsigned k = 0;
    for (; k < len_unr; k+=unroll_factor)
    {
        __m128i idxA = _mm_loadu_si128((__m128i*)(idx+k));
        __m128i nbitA = _mm_and_si128 (idxA, sb_mask); // nbit = idx[k] & bm::set_block_mask
        __m128i nwordA = _mm_srli_epi32 (nbitA, bm::set_word_shift); // nword  = nbit >> bm::set_word_shift
 
 
        nbitA = _mm_and_si128 (nbitA, sw_mask);
        _mm_store_si128 ((__m128i*)mshift_v, nbitA);

        // check-compare if all 4 bits are in the very same word
        //
        __m128i nwordA_0 = _mm_shuffle_epi32(nwordA, 0x0); // copy element 0
        __m128i cmpA = _mm_cmpeq_epi32(nwordA_0, nwordA);  // compare EQ
        if (_mm_test_all_ones(cmpA)) // check if all are in one word
        {
            unsigned nword = _mm_extract_epi32(nwordA, 0);
            block[nword] |= (1u << mshift_v[0]) | (1u << mshift_v[1])
                            |(1u << mshift_v[2]) | (1u << mshift_v[3]);
        }
        else // bits are in different words, use scalar scatter
        {
            _mm_store_si128 ((__m128i*)mword_v, nwordA);
            
            block[mword_v[0]] |= (1u << mshift_v[0]);
            block[mword_v[1]] |= (1u << mshift_v[1]);
            block[mword_v[2]] |= (1u << mshift_v[2]);
            block[mword_v[3]] |= (1u << mshift_v[3]);
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


/*!
     SSE4.2 bit block gather-scatter
 
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
void sse4_bit_block_gather_scatter(unsigned* BMRESTRICT arr,
                                   const unsigned* BMRESTRICT blk,
                                   const unsigned* BMRESTRICT idx,
                                   unsigned                   size,
                                   unsigned                   start,
                                   unsigned                   bit_idx) BMNOEXCEPT
{
    const unsigned unroll_factor = 4;
    const unsigned len = (size - start);
    const unsigned len_unr = len - (len % unroll_factor);
    
    __m128i sb_mask = _mm_set1_epi32(bm::set_block_mask);
    __m128i sw_mask = _mm_set1_epi32(bm::set_word_mask);
    __m128i maskFF  = _mm_set1_epi32(~0u);
    __m128i maskZ   = _mm_xor_si128(maskFF, maskFF);

    __m128i mask_tmp, mask_0;
    
    unsigned BM_ALIGN16 mshift_v[4] BM_ALIGN16ATTR;
    unsigned BM_ALIGN16 mword_v[4] BM_ALIGN16ATTR;

    unsigned k = 0;
    unsigned base = start + k;
    __m128i* idx_ptr = (__m128i*)(idx + base);   // idx[base]
    __m128i* target_ptr = (__m128i*)(arr + base); // arr[base]
    for (; k < len_unr; k+=unroll_factor)
    {
        __m128i nbitA = _mm_and_si128 (_mm_loadu_si128(idx_ptr), sb_mask); // nbit = idx[base] & bm::set_block_mask
        __m128i nwordA = _mm_srli_epi32 (nbitA, bm::set_word_shift); // nword  = nbit >> bm::set_word_shift
        // (nbit & bm::set_word_mask)
        _mm_store_si128 ((__m128i*)mshift_v, _mm_and_si128 (nbitA, sw_mask));
        _mm_store_si128 ((__m128i*)mword_v, nwordA);
        
        // mask0 = 1u << (nbit & bm::set_word_mask);
        //
#if 0
        // ifdefed an alternative SHIFT implementation using SSE and masks
        // (it is not faster than just doing scalar operations)
        {
        __m128i am_0    = _mm_set_epi32(0, 0, 0, ~0u);
        __m128i mask1   = _mm_srli_epi32 (maskFF, 31);
        mask_0 = _mm_and_si128 (_mm_slli_epi32 (mask1, mshift_v[0]), am_0);
        mask_tmp = _mm_and_si128 (_mm_slli_epi32(mask1, mshift_v[1]), _mm_slli_si128 (am_0, 4));
        mask_0 = _mm_or_si128 (mask_0, mask_tmp);
    
        __m128i mask_2 = _mm_and_si128 (_mm_slli_epi32 (mask1, mshift_v[2]),
                                        _mm_slli_si128 (am_0, 8));
        mask_tmp = _mm_and_si128 (
                      _mm_slli_epi32(mask1, mshift_v[3]),
                      _mm_slli_si128 (am_0, 12)
                      );
    
        mask_0 = _mm_or_si128 (mask_0,
                               _mm_or_si128 (mask_2, mask_tmp)); // assemble bit-test mask
        }
#endif
        mask_0 = _mm_set_epi32(1 << mshift_v[3], 1 << mshift_v[2], 1 << mshift_v[1], 1 << mshift_v[0]);


        //  gather for: blk[nword]  (.. & mask0 )
        //
        mask_tmp = _mm_and_si128(_mm_set_epi32(blk[mword_v[3]], blk[mword_v[2]],
                                               blk[mword_v[1]], blk[mword_v[0]]),
                                 mask_0);
        
        // bool(blk[nword]  ...)
        //maskFF  = _mm_set1_epi32(~0u);
        mask_tmp = _mm_cmpeq_epi32 (mask_tmp, maskZ); // set 0xFF where == 0
        mask_tmp = _mm_xor_si128 (mask_tmp, maskFF); // invert
        mask_tmp = _mm_srli_epi32 (mask_tmp, 31); // (bool) 1 only to the 0 pos
        
        mask_tmp = _mm_slli_epi32(mask_tmp, bit_idx); // << bit_idx
        
        _mm_storeu_si128 (target_ptr,                  // arr[base] |= MASK_EXPR
                          _mm_or_si128 (mask_tmp, _mm_loadu_si128(target_ptr)));
        
        ++idx_ptr; ++target_ptr;
        _mm_prefetch((const char*)target_ptr, _MM_HINT_T0);
    }

    for (; k < len; ++k)
    {
        base = start + k;
        unsigned nbit = unsigned(idx[base] & bm::set_block_mask);
        arr[base] |= unsigned(bool(blk[nbit >> bm::set_word_shift] & (1u << (nbit & bm::set_word_mask))) << bit_idx);
    }

}

/*!
    @brief block shift left by 1
    @ingroup SSE4
*/
inline
bool sse42_shift_l1(__m128i* block, unsigned* empty_acc, unsigned co1) BMNOEXCEPT
{
    __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    __m128i mAcc = _mm_set1_epi32(0);
    __m128i mMask1 = _mm_set1_epi32(1);

    unsigned co2;
    for (--block_end; block_end >= block; block_end -= 2)
    {
        __m128i m1A = _mm_load_si128(block_end);
        __m128i m2A = _mm_load_si128(block_end-1);

        __m128i m1CO = _mm_and_si128(m1A, mMask1);
        __m128i m2CO = _mm_and_si128(m2A, mMask1);
        
        co2 = _mm_extract_epi32(m1CO, 0);
        
        m1A = _mm_srli_epi32(m1A, 1); // (block[i] >> 1u)
        m2A = _mm_srli_epi32(m2A, 1);
        
        __m128i m1COshft = _mm_srli_si128 (m1CO, 4); // byte shift-r by 1 int32
        __m128i m2COshft = _mm_srli_si128 (m2CO, 4);
        m1COshft = _mm_insert_epi32 (m1COshft, co1, 3);
        m2COshft = _mm_insert_epi32 (m2COshft, co2, 3);
        m1COshft = _mm_slli_epi32(m1COshft, 31);
        m2COshft = _mm_slli_epi32(m2COshft, 31);

        m1A = _mm_or_si128(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm_or_si128(m2A, m2COshft);
        
        co1 = _mm_extract_epi32(m2CO, 0);

        _mm_store_si128(block_end, m1A);
        _mm_store_si128(block_end-1, m2A);
        
        mAcc = _mm_or_si128(mAcc, m1A);
        mAcc = _mm_or_si128(mAcc, m2A);
    } // for
    
    *empty_acc = !_mm_testz_si128(mAcc, mAcc);
    return co1;
}


/*!
    @brief block shift right by 1
    @ingroup SSE4
*/
inline
bool sse42_shift_r1(__m128i* block, unsigned* empty_acc, unsigned co1) BMNOEXCEPT
{
    __m128i* block_end =
        ( __m128i*)((bm::word_t*)(block) + bm::set_block_size);
    __m128i m1COshft, m2COshft;
    __m128i mAcc = _mm_set1_epi32(0);

    unsigned co2;
    for (;block < block_end; block += 2)
    {
        __m128i m1A = _mm_load_si128(block);
        __m128i m2A = _mm_load_si128(block+1);

        __m128i m1CO = _mm_srli_epi32(m1A, 31);
        __m128i m2CO = _mm_srli_epi32(m2A, 31);
        
        co2 = _mm_extract_epi32(m1CO, 3);
        
        m1A = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
        m2A = _mm_slli_epi32(m2A, 1);
        
        m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift-l by 1 int32
        m2COshft = _mm_slli_si128 (m2CO, 4);
        m1COshft = _mm_insert_epi32 (m1COshft, co1, 0);
        m2COshft = _mm_insert_epi32 (m2COshft, co2, 0);
        
        m1A = _mm_or_si128(m1A, m1COshft); // block[i] |= co_flag
        m2A = _mm_or_si128(m2A, m2COshft);

        co1 = _mm_extract_epi32(m2CO, 3);

        _mm_store_si128(block, m1A);
        _mm_store_si128(block+1, m2A);
        
        mAcc = _mm_or_si128(mAcc, m1A);
        mAcc = _mm_or_si128(mAcc, m2A);
    }
    *empty_acc = !_mm_testz_si128(mAcc, mAcc);
    return co1;
}



/*!
    @brief block shift right by 1 plus AND

    @return carry over flag
    @ingroup SSE4
*/
inline
bool sse42_shift_r1_and(__m128i* block,
                        bm::word_t co1,
                        const __m128i* BMRESTRICT mask_block,
                        bm::id64_t* digest) BMNOEXCEPT
{
    bm::word_t* wblock = (bm::word_t*) block;
    const bm::word_t* mblock = (const bm::word_t*) mask_block;
    
    __m128i m1COshft, m2COshft;
    __m128i mAcc = _mm_set1_epi32(0);
    unsigned co2;

    bm::id64_t d, wd;
    wd = d = *digest;

    unsigned di = 0;
    if (!co1)
    {
        bm::id64_t t = d & -d;
#ifdef BM64_SSE4
        di = unsigned(_mm_popcnt_u64(t - 1)); // find start bit-index
#else
        bm::id_t t32 = t & bm::id_max;
        if (t32 == 0) {
            di = 32;
            t32 = t >> 32;
        }
        di += unsigned(_mm_popcnt_u32(t32 - 1));
#endif
    }

    for (; di < 64 ; ++di)
    {
        const unsigned d_base = di * bm::set_block_digest_wave_size;
        bm::id64_t dmask = (1ull << di);
        if (d & dmask) // digest stride NOT empty
        {
            block = (__m128i*) &wblock[d_base];
            mask_block = (__m128i*) &mblock[d_base];
            mAcc = _mm_xor_si128(mAcc, mAcc); // mAcc = 0
            for (unsigned i = 0; i < 4; ++i, block += 2, mask_block += 2)
            {
                __m128i m1A = _mm_load_si128(block);
                __m128i m2A = _mm_load_si128(block+1);

                __m128i m1CO = _mm_srli_epi32(m1A, 31);
                __m128i m2CO = _mm_srli_epi32(m2A, 31);

                co2 = _mm_extract_epi32(m1CO, 3);
                
                m1A = _mm_slli_epi32(m1A, 1); // (block[i] << 1u)
                m2A = _mm_slli_epi32(m2A, 1);
                
                m1COshft = _mm_slli_si128 (m1CO, 4); // byte shift left by 1 int32
                m1COshft = _mm_insert_epi32 (m1COshft, co1, 0);
                
                co1 = co2;
                
                co2 = _mm_extract_epi32(m2CO, 3);
                
                m2COshft = _mm_slli_si128 (m2CO, 4);
                m2COshft = _mm_insert_epi32 (m2COshft, co1, 0);
                
                m1A = _mm_or_si128(m1A, m1COshft); // block[i] |= co_flag
                m2A = _mm_or_si128(m2A, m2COshft);
                
                m1A = _mm_and_si128(m1A, _mm_load_si128(mask_block)); // block[i] &= mask_block[i]
                m2A = _mm_and_si128(m2A, _mm_load_si128(mask_block+1)); // block[i] &= mask_block[i]

                mAcc = _mm_or_si128(mAcc, m1A);
                mAcc = _mm_or_si128(mAcc, m2A);

                _mm_store_si128(block, m1A);
                _mm_store_si128(block+1, m2A);

                co1 = co2;

            } // for i
            
            if (_mm_testz_si128(mAcc, mAcc))
                d &= ~dmask; // clear digest bit 
            wd &= wd - 1;
        }
        else
        {
            if (co1)
            {
                BM_ASSERT(co1 == 1);
                BM_ASSERT(wblock[d_base] == 0);
                
                bm::id64_t w0 = wblock[d_base] = co1 & mblock[d_base];
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

/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target := block ^ xor_block
    @param block - arg1
    @param xor_block - arg2
    @param digest - mask for each block wave to XOR (1) or just copy (0)

    @ingroup SSE4
    @internal
*/
inline
void sse42_bit_block_xor(bm::word_t*  target_block,
                   const bm::word_t*  block, const bm::word_t*  xor_block,
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
            _mm_store_si128(t_sub_block , _mm_load_si128(sub_block));
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

    @ingroup SSE4
    @internal
*/
inline
void sse42_bit_block_xor_2way(bm::word_t* target_block,
                              const bm::word_t*  xor_block,
                              bm::id64_t digest) BMNOEXCEPT
{
    while (digest)
    {
        bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
        unsigned wave = unsigned(_mm_popcnt_u64(t - 1));
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



#define VECT_XOR_ARR_2_MASK(dst, src, src_end, mask)\
    sse2_xor_arr_2_mask((__m128i*)(dst), (__m128i*)(src), (__m128i*)(src_end), (bm::word_t)mask)

#define VECT_ANDNOT_ARR_2_MASK(dst, src, src_end, mask)\
    sse2_andnot_arr_2_mask((__m128i*)(dst), (__m128i*)(src), (__m128i*)(src_end), (bm::word_t)mask)

#define VECT_BITCOUNT(first, last) \
    sse4_bit_count((__m128i*) (first), (__m128i*) (last))
/*
#ifdef BM64_SSE4
#define VECT_BIT_COUNT_DIGEST(src, digest) \
    sse42_bit_count_digest(src, digest)
#endif
*/
#define VECT_BITCOUNT_AND(first, last, mask) \
    sse4_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_and)

#define VECT_BITCOUNT_OR(first, last, mask) \
    sse4_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_or)

#define VECT_BITCOUNT_XOR(first, last, mask) \
    sse4_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_xor)

#define VECT_BITCOUNT_SUB(first, last, mask) \
    sse4_bit_count_op((__m128i*) (first), (__m128i*) (last), (__m128i*) (mask), sse2_sub)

#define VECT_INVERT_BLOCK(first) \
    sse2_invert_block((__m128i*)first);

#define VECT_AND_BLOCK(dst, src) \
    sse4_and_block((__m128i*) dst, (__m128i*) (src))

#define VECT_AND_DIGEST(dst, src) \
    sse4_and_digest((__m128i*) dst, (const __m128i*) (src))

#define VECT_AND_OR_DIGEST_2WAY(dst, src1, src2) \
    sse4_and_or_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_AND_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    sse4_and_digest_5way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2), (const __m128i*) (src3), (const __m128i*) (src4))

#define VECT_AND_DIGEST_3WAY(dst, src1, src2) \
    sse4_and_digest_3way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_AND_DIGEST_2WAY(dst, src1, src2) \
    sse4_and_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_OR_BLOCK(dst, src) \
    sse2_or_block((__m128i*) dst, (__m128i*) (src))

#define VECT_OR_BLOCK_2WAY(dst, src1, src2) \
    sse2_or_block_2way((__m128i*) (dst), (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_OR_BLOCK_3WAY(dst, src1, src2) \
    sse2_or_block_3way((__m128i*) (dst), (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_OR_BLOCK_5WAY(dst, src1, src2, src3, src4) \
    sse2_or_block_5way((__m128i*) (dst), (__m128i*) (src1), (__m128i*) (src2), (__m128i*) (src3), (__m128i*) (src4))

#define VECT_SUB_BLOCK(dst, src) \
    sse2_sub_block((__m128i*) dst, (const __m128i*) (src))

#define VECT_SUB_DIGEST(dst, src) \
    sse4_sub_digest((__m128i*) dst, (const __m128i*) (src))

#define VECT_SUB_DIGEST_2WAY(dst, src1, src2) \
    sse4_sub_digest_2way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

#define VECT_SUB_DIGEST_5WAY(dst, src1, src2, src3, src4) \
    sse4_sub_digest_5way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2), (const __m128i*) (src3), (const __m128i*) (src4))

#define VECT_SUB_DIGEST_3WAY(dst, src1, src2) \
    sse4_sub_digest_3way((__m128i*) dst, (const __m128i*) (src1), (const __m128i*) (src2))

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
    sse4_is_all_zero((__m128i*) dst)

#define VECT_IS_ONE_BLOCK(dst) \
    sse4_is_all_one((__m128i*) dst)

#define VECT_IS_DIGEST_ZERO(start) \
    sse4_is_digest_zero((__m128i*)start)

#define VECT_BLOCK_SET_DIGEST(dst, val) \
    sse4_block_set_digest((__m128i*)dst, val)

#define VECT_LOWER_BOUND_SCAN_U32(arr, target, from, to) \
    sse2_lower_bound_scan_u32(arr, target, from, to)

#define VECT_SHIFT_L1(b, acc, co) \
    sse42_shift_l1((__m128i*)b, acc, co)

#define VECT_SHIFT_R1(b, acc, co) \
    sse42_shift_r1((__m128i*)b, acc, co)

#define VECT_SHIFT_R1_AND(b, co, m, digest) \
    sse42_shift_r1_and((__m128i*)b, co, (__m128i*)m, digest)

#define VECT_ARR_BLOCK_LOOKUP(idx, size, nb, start) \
    sse42_idx_arr_block_lookup(idx, size, nb, start)
    
#define VECT_SET_BLOCK_BITS(block, idx, start, stop) \
    sse42_set_block_bits(block, idx, start, stop)

#define VECT_BLOCK_CHANGE(block, size) \
    sse42_bit_block_calc_change((__m128i*)block, size)

#define VECT_BLOCK_XOR_CHANGE(block, xor_block, size, gc, bc) \
    sse42_bit_block_calc_xor_change((__m128i*)block, (__m128i*)xor_block, size, gc, bc)

#ifdef BM64_SSE4
#define VECT_BLOCK_CHANGE_BC(block, gc, bc) \
    sse42_bit_block_calc_change_bc((__m128i*)block, gc, bc)
#endif

#define VECT_BIT_FIND_FIRST(src, off, pos) \
    sse42_bit_find_first((__m128i*) src, off, pos)

#define VECT_BIT_FIND_DIFF(src1, src2, pos) \
    sse42_bit_find_first_diff((__m128i*) src1, (__m128i*) (src2), pos)

#define VECT_BIT_BLOCK_XOR(t, src, src_xor, d) \
    sse42_bit_block_xor(t, src, src_xor, d)

#define VECT_BIT_BLOCK_XOR_2WAY(t, src_xor, d) \
    sse42_bit_block_xor_2way(t, src_xor, d)


#define VECT_GAP_BFIND(buf, pos, is_set) \
    sse42_gap_bfind(buf, pos, is_set)

#define VECT_GAP_TEST(buf, pos) \
    sse42_gap_test(buf, pos)

#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


// undefine local defines to avoid pre-proc space pollution
//
#undef BM_BSF32

#ifdef _MSC_VER
#pragma warning( pop )
#endif

} // namespace




#endif
