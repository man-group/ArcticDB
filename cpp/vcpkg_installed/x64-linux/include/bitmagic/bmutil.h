#ifndef BMUTIL__H__INCLUDED__
#define BMUTIL__H__INCLUDED__
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

/*! \file bmutil.h
    \brief Bit manipulation primitives (internal)
*/


#include "bmdef.h"
#include "bmconst.h"

#if defined(__arm64__) || defined(__arm__)
//#include "sse2neon.h"
#else
    #if defined(_M_AMD64) || defined(_M_X64)
    #include <intrin.h>
    #elif defined(__x86_64__)
    #include <x86intrin.h>
    #endif
#endif

#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4146)
#endif


namespace bm
{

        /**
        bit-block array wrapped into union for correct interpretation of
        32-bit vs 64-bit access vs SIMD
        @internal
        */
        struct bit_block_t
        {
            union bunion_t
            {
                bm::word_t BM_VECT_ALIGN w32[bm::set_block_size] BM_VECT_ALIGN_ATTR;
                bm::id64_t BM_VECT_ALIGN w64[bm::set_block_size / 2] BM_VECT_ALIGN_ATTR;
                
#if defined(BMAVX512OPT)
                __m512i  BM_VECT_ALIGN w512[bm::set_block_size / 16] BM_VECT_ALIGN_ATTR;
#endif
#if defined(BMAVX2OPT)
                __m256i  BM_VECT_ALIGN w256[bm::set_block_size / 8] BM_VECT_ALIGN_ATTR;
#endif
#if defined(BMSSE2OPT) || defined(BMSSE42OPT)
                __m128i  BM_VECT_ALIGN w128[bm::set_block_size / 4] BM_VECT_ALIGN_ATTR;
#endif
            } b_;

            operator bm::word_t*() { return &(b_.w32[0]); }
            operator const bm::word_t*() const { return &(b_.w32[0]); }
            explicit operator bm::id64_t*() { return &b_.w64[0]; }
            explicit operator const bm::id64_t*() const { return &b_.w64[0]; }
#ifdef BMAVX512OPT
            explicit operator __m512i*() { return &b_.w512[0]; }
            explicit operator const __m512i*() const { return &b_.w512[0]; }
#endif
#ifdef BMAVX2OPT
            explicit operator __m256i*() { return &b_.w256[0]; }
            explicit operator const __m256i*() const { return &b_.w256[0]; }
#endif
#if defined(BMSSE2OPT) || defined(BMSSE42OPT)
            explicit operator __m128i*() { return &b_.w128[0]; }
            explicit operator const __m128i*() const { return &b_.w128[0]; }
#endif

            const bm::word_t* begin() const { return (b_.w32 + 0); }
            bm::word_t* begin() { return (b_.w32 + 0); }
            const bm::word_t* end() const { return (b_.w32 + bm::set_block_size); }
            bm::word_t* end() { return (b_.w32 + bm::set_block_size); }
        };
    
/**
    Get minimum of 2 values
*/
template<typename T>
BMFORCEINLINE T min_value(T v1, T v2) BMNOEXCEPT
{
    return v1 < v2 ? v1 : v2;
}

/**
    \brief ad-hoc conditional expressions
    \internal
*/
template <bool b> struct conditional
{
    static bool test() { return true; }
};
template <> struct conditional<false>
{
    static bool test() { return false; }
};


/**
    Fast loop-less function to find LOG2
*/
template<typename T>
BMFORCEINLINE T ilog2(T x) BMNOEXCEPT
{
    unsigned int l = 0;
    
    if (x >= 1<<16) { x = (T)(x >> 16); l |= 16; }
    if (x >= 1<<8)  { x = (T)(x >> 8);  l |= 8; }
    if (x >= 1<<4)  { x = (T)(x >> 4);  l |= 4; }
    if (x >= 1<<2)  { x = (T)(x >> 2);  l |= 2; }
    if (x >= 1<<1)  l |=1;
    return (T)l;
}

template<>
BMFORCEINLINE
bm::gap_word_t ilog2(gap_word_t x) BMNOEXCEPT
{
    unsigned int l = 0;
    if (x >= 1<<8)  { x = (bm::gap_word_t)(x >> 8); l |= 8; }
    if (x >= 1<<4)  { x = (bm::gap_word_t)(x >> 4); l |= 4; }
    if (x >= 1<<2)  { x = (bm::gap_word_t)(x >> 2); l |= 2; }
    if (x >= 1<<1)  l |=1;
    return (bm::gap_word_t)l;
}

/**
     Mini auto-pointer for internal memory management
     @internal
*/
template<class T>
class ptr_guard
{
public:
    ptr_guard(T* p) BMNOEXCEPT : ptr_(p) {}
    ~ptr_guard() { delete ptr_; }
private:
    ptr_guard(const ptr_guard<T>& p);
    ptr_guard& operator=(const ptr_guard<T>& p);
private:
    T* ptr_;
};

/**
    Portable LZCNT with (uses minimal LUT)
    @ingroup bitfunc
    @internal
*/
BMFORCEINLINE
unsigned count_leading_zeros(unsigned x) BMNOEXCEPT
{
    unsigned n =
        (x >= (1U << 16)) ?
        ((x >= (1U << 24)) ? ((x >= (1 << 28)) ? 28u : 24u) : ((x >= (1U << 20)) ? 20u : 16u))
        :
        ((x >= (1U << 8)) ? ((x >= (1U << 12)) ? 12u : 8u) : ((x >= (1U << 4)) ? 4u : 0u));
    return unsigned(bm::lzcnt_table<true>::_lut[x >> n]) - n;
}

/**
    Portable TZCNT with (uses 37-LUT)
    @ingroup bitfunc
    @internal
*/
BMFORCEINLINE
unsigned count_trailing_zeros(unsigned v) BMNOEXCEPT
{
    // (v & -v) isolates the last set bit
    return unsigned(bm::tzcnt_table<true>::_lut[(-v & v) % 37]);
}

/**
    Lookup table based integer LOG2
*/
template<typename T>
BMFORCEINLINE T ilog2_LUT(T x) BMNOEXCEPT
{
    unsigned l = 0;
    if (x & 0xffff0000) 
    {
        l += 16; x >>= 16;
    }
    
    if (x & 0xff00) 
    {
        l += 8; x >>= 8;
    }
    return l + T(bm::first_bit_table<true>::_idx[x]);
}

/**
    Lookup table based short integer LOG2
*/
template<>
BMFORCEINLINE bm::gap_word_t ilog2_LUT<bm::gap_word_t>(bm::gap_word_t x) BMNOEXCEPT
{
    if (x & 0xff00)
    {
        x = bm::gap_word_t(x >> 8u);
        return bm::gap_word_t(8u + bm::first_bit_table<true>::_idx[x]);
    }
    return bm::gap_word_t(bm::first_bit_table<true>::_idx[x]);
}


// if we are running on x86 CPU we can use inline ASM 

#if defined(BM_x86) && !(defined(__arm__) || defined(__aarch64__))
#ifdef __GNUG__

BMFORCEINLINE
unsigned bsf_asm32(unsigned int v) BMNOEXCEPT
{
    unsigned r;
    asm volatile(" bsfl %1, %0": "=r"(r): "rm"(v) );
    return r;
}
 
BMFORCEINLINE
unsigned bsr_asm32(unsigned int v) BMNOEXCEPT
{
    unsigned r;
    asm volatile(" bsrl %1, %0": "=r"(r): "rm"(v) );
    return r;
}

#endif  // __GNUG__

#ifdef _MSC_VER

#if defined(_M_AMD64) || defined(_M_X64) // inline assembly not supported

BMFORCEINLINE
unsigned int bsr_asm32(unsigned int value) BMNOEXCEPT
{
    unsigned long r;
    _BitScanReverse(&r, value);
    return r;
}

BMFORCEINLINE
unsigned int bsf_asm32(unsigned int value) BMNOEXCEPT
{
    unsigned long r;
    _BitScanForward(&r, value);
    return r;
}

#else

BMFORCEINLINE
unsigned int bsr_asm32(unsigned int value) BMNOEXCEPT
{   
  __asm  bsr  eax, value
}

BMFORCEINLINE
unsigned int bsf_asm32(unsigned int value) BMNOEXCEPT
{   
  __asm  bsf  eax, value
}

#endif

#endif // _MSC_VER

#endif // BM_x86


// From:
// http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.37.8562
//
template<typename T>
BMFORCEINLINE T bit_scan_fwd(T v) BMNOEXCEPT
{
    return
        DeBruijn_bit_position<true>::_multiply[(((v & -v) * 0x077CB531U)) >> 27];
}

BMFORCEINLINE
unsigned bit_scan_reverse32(unsigned w) BMNOEXCEPT
{
    BM_ASSERT(w);
#if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
    return (unsigned) (31 - __builtin_clz(w));
#else
# if defined(BM_x86) && (defined(__GNUG__) || defined(_MSC_VER))
    return bm::bsr_asm32(w);
# else
    return bm::ilog2_LUT<unsigned int>(w);
# endif
#endif
}

BMFORCEINLINE
unsigned bit_scan_forward32(unsigned w) BMNOEXCEPT
{
    BM_ASSERT(w);
#if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
    return (unsigned) __builtin_ctz(w);
#else
# if defined(BM_x86) && (defined(__GNUG__) || defined(_MSC_VER))
    return bm::bsf_asm32(w);
# else
        return bit_scan_fwd(w);
# endif
#endif
}


BMFORCEINLINE
unsigned long long bmi_bslr_u64(unsigned long long w) BMNOEXCEPT
{
#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return _blsr_u64(w);
#else
    return w & (w - 1);
#endif
}

BMFORCEINLINE
unsigned long long bmi_blsi_u64(unsigned long long w)
{
#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return _blsi_u64(w);
#else
    return w & (-w);
#endif
}

/// 32-bit bit-scan reverse
inline
unsigned count_leading_zeros_u32(unsigned w) BMNOEXCEPT
{
    BM_ASSERT(w);
#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return (unsigned)_lzcnt_u32(w);
#else
    #if defined(BM_USE_GCC_BUILD) || defined(__GNUG__)
        return (unsigned) __builtin_clz(w);
    #else
        return bm::count_leading_zeros(w); // portable
    #endif
#endif
}


/// 64-bit bit-scan reverse
inline
unsigned count_leading_zeros_u64(bm::id64_t w) BMNOEXCEPT
{
    BM_ASSERT(w);
#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return (unsigned)_lzcnt_u64(w);
#else
    #if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
        return (unsigned) __builtin_clzll(w);
    #else
        unsigned z;
        unsigned w1 = unsigned(w >> 32);
        if (!w1)
        {
            z = 32;
            w1 = unsigned(w);
            z += 31 - bm::bit_scan_reverse32(w1);
        }
        else
        {
            z = 31 - bm::bit_scan_reverse32(w1);
        }
        return z;
    #endif
#endif
}

/// 32-bit bit-scan fwd
inline
unsigned count_trailing_zeros_u32(unsigned w) BMNOEXCEPT
{
    BM_ASSERT(w);

#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return (unsigned)_tzcnt_u32(w);
#else
    #if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
        return (unsigned) __builtin_ctz(w);
    #else
        return bm::bit_scan_forward32(w);
    #endif
#endif
}


/// 64-bit bit-scan fwd
inline
unsigned count_trailing_zeros_u64(bm::id64_t w) BMNOEXCEPT
{
    BM_ASSERT(w);

#if defined(BMAVX2OPT) || defined (BMAVX512OPT)
    return (unsigned)_tzcnt_u64(w);
#else
    #if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
        return (unsigned) __builtin_ctzll(w);
    #else
        unsigned z;
        unsigned w1 = unsigned(w);
        if (!w1)
        {
            z = 32;
            w1 = unsigned(w >> 32);
            z += bm::bit_scan_forward32(w1);
        }
        else
        {
            z = bm::bit_scan_forward32(w1);
        }
        return z;
    #endif
#endif
}



/*!
    Returns BSR value
    @ingroup bitfunc
*/
template <class T>
unsigned bit_scan_reverse(T value) BMNOEXCEPT
{
    BM_ASSERT(value);

    if (bm::conditional<sizeof(T)==8>::test())
    {
    #if defined(BM_USE_GCC_BUILD) || (defined(__GNUG__) && (defined(__arm__) || defined(__aarch64__)))
        return (unsigned) (63 - __builtin_clzll(value));
    #else
        bm::id64_t v8 = value;
        v8 >>= 32;
        unsigned v = (unsigned)v8;
        if (v)
        {
            v = bm::bit_scan_reverse32(v);
            return v + 32;
        }
    #endif
    }
    return bm::bit_scan_reverse32((unsigned)value);
}

/*! \brief and functor
    \internal
 */
struct and_func
{
    static
    BMFORCEINLINE unsigned op(unsigned v1, unsigned v2) BMNOEXCEPT2
        { return v1 & v2; }
};
/*! \brief xor functor
    \internal
 */
struct xor_func
{
    static
    BMFORCEINLINE unsigned op(unsigned v1, unsigned v2) BMNOEXCEPT2
        { return v1 ^ v2; }
};
/*! \brief or functor
    \internal
 */
struct or_func
{
    static
    BMFORCEINLINE unsigned op(unsigned v1, unsigned v2) BMNOEXCEPT2
        { return v1 | v2; }
};
/*! \brief sub functor
    \internal
 */
struct sub_func
{
    static
    BMFORCEINLINE unsigned op(unsigned v1, unsigned v2) BMNOEXCEPT2
        { return v1 & ~v2; }
};

BMFORCEINLINE
unsigned mask_r_u32(unsigned nbit) BMNOEXCEPT
{
    BM_ASSERT(nbit < 32);
    unsigned m = (~0u << nbit);
    BM_ASSERT(m == block_set_table<true>::_right[nbit]);
    return m;
}

BMFORCEINLINE
unsigned mask_l_u32(unsigned nbit) BMNOEXCEPT
{
    BM_ASSERT(nbit < 32);
    unsigned m = ~0u >> (31 - nbit);
    BM_ASSERT(m == block_set_table<true>::_left[nbit]);
    return m;
}

/// XOR swap two variables
///
/// @internal
template<typename W>
BMFORCEINLINE void xor_swap(W& x, W& y) BMNOEXCEPT
{
    BM_ASSERT(&x != &y);
    x ^= y; y ^= x; x ^= y;
}


#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif
#ifdef _MSC_VER
#pragma warning( pop )
#endif

/**
    Сompute mask of bytes presense in 64-bit word

    @param w - [in] input 64-bit word
    @return mask with 8 bits
    @internal
 */
inline
unsigned compute_h64_mask(unsigned long long w) BMNOEXCEPT
{
    unsigned h_mask = 0;
    for (unsigned i = 0; w && (i < 8); ++i, w >>= 8)
    {
        if ((unsigned char) w)
            h_mask |= (1u<<i);
    } // for
    return h_mask;
}

/**
    Returns true if INT64 contains 0 octet
 */
BMFORCEINLINE
bool has_zero_byte_u64(bm::id64_t v) BMNOEXCEPT
{
  return (v - 0x0101010101010101ULL) & ~(v) & 0x8080808080808080ULL;
}


/*!
    Returns bit count
    @ingroup bitfunc
*/
BMFORCEINLINE
bm::id_t word_bitcount(bm::id_t w) BMNOEXCEPT
{
#if defined(BMSSE42OPT) || defined(BMAVX2OPT) || defined(BMAVX512OPT)
    return bm::id_t(_mm_popcnt_u32(w));
#else
    #if defined(BM_USE_GCC_BUILD)
        return (bm::id_t)__builtin_popcount(w);
    #else
    return
        bm::bit_count_table<true>::_count[(unsigned char)(w)] +
        bm::bit_count_table<true>::_count[(unsigned char)((w) >> 8)] +
        bm::bit_count_table<true>::_count[(unsigned char)((w) >> 16)] +
        bm::bit_count_table<true>::_count[(unsigned char)((w) >> 24)];
    #endif
#endif
}


/*!
    Function calculates number of 1 bits in 64-bit word.
    @ingroup bitfunc
*/
BMFORCEINLINE
unsigned word_bitcount64(bm::id64_t x) BMNOEXCEPT
{
#if defined(BMSSE42OPT) || defined(BMAVX2OPT) || defined(BMAVX512OPT)
    #if defined(BM64_SSE4) || defined(BM64_AVX2) || defined(BM64_AVX512)
        return unsigned(_mm_popcnt_u64(x));
    #else // 32-bit
        return _mm_popcnt_u32(x >> 32) + _mm_popcnt_u32((unsigned)x);
    #endif
#else
    #if defined(BM_USE_GCC_BUILD) || defined(__arm64__)
        return (unsigned)__builtin_popcountll(x);
    #else
        #if (defined(__arm__)) // 32-bit
            return bm::word_bitcount(x >> 32) + bm::word_bitcount((unsigned)x);
        #else
            x = x - ((x >> 1) & 0x5555555555555555);
            x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333);
            x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0F;
            x = x + (x >> 8);
            x = x + (x >> 16);
            x = x + (x >> 32);
            return x & 0xFF;
        #endif
    #endif
#endif
}

/**
    Check pointer alignment
    @internal
 */
template< typename T >
bool is_aligned(T* p) BMNOEXCEPT
{
#if defined (BM_ALLOC_ALIGN)
    return !(reinterpret_cast<unsigned int*>(p) % BM_ALLOC_ALIGN);
#else
    (void)p;
    return true;
#endif
}


} // bm

#endif
