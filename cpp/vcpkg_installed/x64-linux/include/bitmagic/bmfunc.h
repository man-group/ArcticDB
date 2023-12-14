#ifndef BMFUNC__H__INCLUDED__
#define BMFUNC__H__INCLUDED__
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

/*! \file bmfunc.h
    \brief Bit manipulation primitives (internal)
*/

#include <memory.h>
#include <type_traits>

#include "bmdef.h"
#include "bmutil.h"


#ifdef _MSC_VER
# pragma warning( disable: 4146 )
#endif

namespace bm
{


template<bool LWA=false, bool RWA=false>
bm::id_t bit_block_calc_count_range(const bm::word_t* block,
                                    bm::word_t left,
                                    bm::word_t right) BMNOEXCEPT;

inline 
bm::id_t bit_block_any_range(const bm::word_t* block,
                             bm::word_t left,
                             bm::word_t right) BMNOEXCEPT;

/*!
    @brief Structure with statistical information about memory
            allocation footprint, serialization projection, number of vectors
    @ingroup bvector
*/
struct bv_statistics
{
    size_t bit_blocks;        ///< Number of bit blocks
    size_t gap_blocks;        ///< Number of GAP blocks
    size_t ptr_sub_blocks;    ///< Number of sub-blocks
    size_t bv_count;          ///< Number of bit-vectors
    size_t  max_serialize_mem; ///< estimated maximum memory for serialization
    size_t  memory_used; ///< memory usage for all blocks and service tables
    size_t  gap_cap_overhead; ///< gap memory overhead between length and capacity
    gap_word_t  gap_levels[bm::gap_levels]; ///< GAP block lengths in the bvect
    unsigned long long gaps_by_level[bm::gap_levels]; ///< number of GAP blocks at each level

    bv_statistics() BMNOEXCEPT { reset(); }

    /// cound bit block
    void add_bit_block() BMNOEXCEPT
    {
        ++bit_blocks;
        size_t mem_used = sizeof(bm::word_t) * bm::set_block_size;
        memory_used += mem_used;
        max_serialize_mem += mem_used;
    }

    /// count gap block
    void add_gap_block(unsigned capacity, unsigned length, unsigned level) BMNOEXCEPT
    {
        BM_ASSERT(level < bm::gap_levels);

        ++gap_blocks;
        size_t mem_used = (capacity * sizeof(gap_word_t));
        memory_used += mem_used;
        max_serialize_mem += (unsigned)(length * sizeof(gap_word_t));
        BM_ASSERT(length <= capacity);
        gap_cap_overhead += (capacity - length) * sizeof(gap_word_t);
        if (level < bm::gap_levels)
            gaps_by_level[level]++;
    }
    
    /// Reset statisctics
    void reset() BMNOEXCEPT
    {
        bit_blocks = gap_blocks = ptr_sub_blocks = bv_count = 0;
        max_serialize_mem = memory_used = gap_cap_overhead = 0;
        for (unsigned i = 0; i < bm::gap_levels; ++i)
            gaps_by_level[i] = 0;
    }
    
    /// Sum data from another sttructure
    void add(const bv_statistics& st) BMNOEXCEPT
    {
        bit_blocks += st.bit_blocks;
        gap_blocks += st.gap_blocks;
        ptr_sub_blocks += st.ptr_sub_blocks;
        bv_count += st.bv_count;
        max_serialize_mem += st.max_serialize_mem + 8;
        memory_used += st.memory_used;
        gap_cap_overhead += st.gap_cap_overhead;
    }
};

/*!
    @brief Structure with statistical information about memory
            allocation for arena based vectors
    @ingroup bvector
*/
struct bv_arena_statistics
{
    size_t bit_blocks_sz;      ///< Total size of bit blocks
    size_t gap_blocks_sz;      ///< Total size of gap blocks
    size_t ptr_sub_blocks_sz;  ///< Total size of sub-blocks ptrs
    unsigned top_block_size;   ///< size of top descriptor

    /// Reset statisctics
    void reset() BMNOEXCEPT
    {
        bit_blocks_sz = gap_blocks_sz = ptr_sub_blocks_sz = top_block_size = 0;
    }

    /// Get allocation size in bytes
    size_t get_alloc_size() const BMNOEXCEPT
    {
        size_t sz = bit_blocks_sz * sizeof(bm::word_t);

        if (gap_blocks_sz) // add padding space for SIMD vect overread
        {
            sz += (gap_blocks_sz + bm::gap_len_table_min<true>::_len[0])
                                                   * sizeof(bm::gap_word_t);
        }

        sz += (ptr_sub_blocks_sz + top_block_size) * sizeof(void*);
        return sz;
    }
};

/**
    @brief Pair type
*/
template<typename First, typename Second>
struct pair
{
    First   first;
    Second  second;
    
    pair(First f, Second s) : first(f), second(s) {}
};

/**
    \brief bit-decode cache structure
*/
struct  bit_decode_cache
{
    unsigned short bits[65]; //< decoded bits
    unsigned       bcnt;     //< length of bits array
    bm::id64_t     cvalue;   //< cache decoded value
    
    bit_decode_cache() : bcnt(0), cvalue(0) {}
};


/**
    \brief Recalc linear bvector block index into 2D matrix coordinates
    \internal
*/
template<typename BI_TYPE>
BMFORCEINLINE
void get_block_coord(BI_TYPE nb, unsigned& i, unsigned& j) BMNOEXCEPT
{
    i = unsigned(nb >> bm::set_array_shift); // top block address
    j = unsigned(nb &  bm::set_array_mask);  // address in sub-block
}

/**
    Compute bit address of the first bit in a superblock
    \internal
*/
template<typename RTYPE>
BMFORCEINLINE RTYPE get_super_block_start(unsigned i) BMNOEXCEPT
{
    return RTYPE(i) * bm::set_sub_total_bits;
}

/**
    Compute bit address of the first bit in a block
    \internal
*/
template<typename RTYPE>
BMFORCEINLINE RTYPE get_block_start(unsigned i, unsigned j) BMNOEXCEPT
{
    RTYPE idx = bm::get_super_block_start<RTYPE>(i);
    idx += (j) * bm::gap_max_bits;
    return idx;
}


/*! 
    @defgroup gapfunc GAP functions
    GAP functions implement different opereations on GAP compressed blocks (internals)
    and serve as a minimal building blocks.
    \internal
    @ingroup bvector
 */

/*! 
   @defgroup bitfunc BIT functions
   Bit functions implement different opereations on bit blocks (internals)
   and serve as a minimal building blocks.
   \internal
   @ingroup bvector
 */


/**
    32-bit paralle, bitcount
   \internal
   @ingroup bitfunc
 */
inline
int parallel_popcnt_32(unsigned int n) BMNOEXCEPT
{
   unsigned int tmp;

   tmp = n - ((n >> 1) & 033333333333)
           - ((n >> 2) & 011111111111);
   return ((tmp + (tmp >> 3)) & 030707070707) % 63;
}



/*!
    Parallel popcount on 4x 64-bit words
    @ingroup bitfunc
*/
inline
unsigned bitcount64_4way(bm::id64_t x, bm::id64_t y, 
                         bm::id64_t u, bm::id64_t v) BMNOEXCEPT
{
    const bm::id64_t m1 = 0x5555555555555555U;
    const bm::id64_t m2 = 0x3333333333333333U; 
    const bm::id64_t m3 = 0x0F0F0F0F0F0F0F0FU; 
    const bm::id64_t m4 = 0x000000FF000000FFU;

    x = x - ((x >> 1) & m1);
    y = y - ((y >> 1) & m1);
    u = u - ((u >> 1) & m1);
    v = v - ((v >> 1) & m1);
    x = (x & m2) + ((x >> 2) & m2);
    y = (y & m2) + ((y >> 2) & m2);
    u = (u & m2) + ((u >> 2) & m2);
    v = (v & m2) + ((v >> 2) & m2);
    x = x + y; 
    u = u + v; 
    x = (x & m3) + ((x >> 4) & m3);
    u = (u & m3) + ((u >> 4) & m3);
    x = x + u; 
    x = x + (x >> 8);
    x = x + (x >> 16);
    x = x & m4; 
    x = x + (x >> 32);
    return x & 0x000001FFU;
}


// --------------------------------------------------------------
// Functions for bit-scanning
// --------------------------------------------------------------

/*!
   \brief Templated algorithm to unpacks octet based word into list of ON bit indexes
   \param w - value
   \param func - bit functor

   @ingroup bitfunc
*/
template<typename T, typename F>
void bit_for_each_4(T w, F& func)
{
    for (unsigned sub_octet = 0; w != 0; w >>= 4, sub_octet += 4)
    {
        switch (w & 15) // 1111
        {
        case 0: // 0000
            break;
        case 1: // 0001
            func(sub_octet);
            break;
        case 2: // 0010
            func(sub_octet + 1);
            break;
        case 3:    // 0011
            func(sub_octet, sub_octet + 1);
            break;
        case 4: // 0100
            func(sub_octet + 2);
            break;
        case 5: // 0101
            func(sub_octet, sub_octet + 2);
            break;
        case 6: // 0110
            func(sub_octet + 1, sub_octet + 2);
            break;
        case 7: // 0111
            func(sub_octet, sub_octet + 1, sub_octet + 2);
            break;
        case 8: // 1000
            func(sub_octet + 3);
            break;
        case 9: // 1001
            func(sub_octet, sub_octet + 3);
            break;
        case 10: // 1010
            func(sub_octet + 1, sub_octet + 3);
            break;
        case 11: // 1011
            func(sub_octet, sub_octet + 1, sub_octet + 3);
            break;
        case 12: // 1100
            func(sub_octet + 2, sub_octet + 3);
            break;
        case 13: // 1101
            func(sub_octet, sub_octet + 2, sub_octet + 3);
            break;
        case 14: // 1110
            func(sub_octet + 1, sub_octet + 2, sub_octet + 3);
            break;
        case 15: // 1111
            func(sub_octet, sub_octet + 1, sub_octet + 2, sub_octet + 3);
            break;
        default:
            BM_ASSERT(0);
            break;
        }
        
    } // for
}


/*!
   \brief Templated algorithm to unpacks word into list of ON bit indexes
   \param w - value
   \param func - bit functor

   @ingroup bitfunc
*/
template<typename T, typename F>
void bit_for_each(T w, F& func)
{
    // Note: 4-bit table method works slower than plain check approach
    for (unsigned octet = 0; w != 0; w >>= 8, octet += 8)
    {
        if (w & 1)   func(octet + 0);
        if (w & 2)   func(octet + 1);
        if (w & 4)   func(octet + 2);
        if (w & 8)   func(octet + 3);
        if (w & 16)  func(octet + 4);
        if (w & 32)  func(octet + 5);
        if (w & 64)  func(octet + 6);
        if (w & 128) func(octet + 7);
        
    } // for
}

/**
    portable, switch based bitscan
    @internal
   @ingroup bitfunc
 */
inline
unsigned bitscan_nibble(unsigned w, unsigned* bits) BMNOEXCEPT
{
    unsigned cnt = 0;
    for (unsigned sub_octet = 0; w; w >>= 4, sub_octet+=4)
    {
        switch (w & 15) // 1111
        {
        //case 0: // 0000
        //    break;
        case 1: // 0001
            bits[cnt++] = 0 + sub_octet;
            break;
        case 2: // 0010
            bits[cnt++] = 1 + sub_octet;
            break;
        case 3:    // 0011
            bits[cnt]   = 0 + sub_octet;
            bits[cnt+1] = 1 + sub_octet;
            cnt += 2;
            break;
        case 4: // 0100
            bits[cnt++] = 2 + sub_octet;
            break;
        case 5: // 0101
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 2 + sub_octet;
            cnt += 2;
            break;
        case 6: // 0110
            bits[cnt+0] = 1 + sub_octet;
            bits[cnt+1] = 2 + sub_octet;
            cnt += 2;
            break;
        case 7: // 0111
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 1 + sub_octet;
            bits[cnt+2] = 2 + sub_octet;
            cnt += 3;
            break;
        case 8: // 1000
            bits[cnt++] = 3 + sub_octet;
            break;
        case 9: // 1001
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 3 + sub_octet;
            cnt += 2;
            break;
        case 10: // 1010
            bits[cnt+0] = 1 + sub_octet;
            bits[cnt+1] = 3 + sub_octet;
            cnt += 2;
            break;
        case 11: // 1011
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 1 + sub_octet;
            bits[cnt+2] = 3 + sub_octet;
            cnt += 3;
            break;
        case 12: // 1100
            bits[cnt+0] = 2 + sub_octet;
            bits[cnt+1] = 3 + sub_octet;
            cnt += 2;
            break;
        case 13: // 1101
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 2 + sub_octet;
            bits[cnt+2] = 3 + sub_octet;
            cnt += 3;
            break;
        case 14: // 1110
            bits[cnt+0] = 1 + sub_octet;
            bits[cnt+1] = 2 + sub_octet;
            bits[cnt+2] = 3 + sub_octet;
            cnt += 3;
            break;
        case 15: // 1111
            bits[cnt+0] = 0 + sub_octet;
            bits[cnt+1] = 1 + sub_octet;
            bits[cnt+2] = 2 + sub_octet;
            bits[cnt+3] = 3 + sub_octet;
            cnt += 4;
            break;
        default:
            break;
        }
    } // for
    return cnt;
}

#ifdef BM_NONSTANDARD_EXTENTIONS
#ifdef __GNUC__
/**
    bitscan based on computed goto (GCC/clang)
    @internal
    @ingroup bitfunc
 */
inline
unsigned bitscan_nibble_gcc(unsigned w, unsigned* bits) BMNOEXCEPT
{
    static void* d_table[] = { &&l0,
        &&l1, &&l3_1, &&l3, &&l7_1, &&l5, &&l7_0, &&l7, &&l15_1,
        &&l9, &&l11_0, &&l11, &&l15_0, &&l13, &&l14, &&l15 };
    unsigned cnt = 0;

    for (unsigned sub_octet = 0; w; w >>= 4, sub_octet+=4)
    {
        goto *d_table[w & 15];
        l1: // 0001
            bits[cnt++] = sub_octet;
            continue;
        l3: // 0011
            bits[cnt++] = sub_octet;
            l3_1:
            bits[cnt++] = 1 + sub_octet;
            continue;
        l5: // 0101
            bits[cnt++] = sub_octet;
            goto l7_1;
        l7: // 0111
            bits[cnt++] = sub_octet;
            l7_0:
            bits[cnt++] = 1 + sub_octet;
            l7_1:
            bits[cnt++] = 2 + sub_octet;
            continue;
        l9: // 1001
            bits[cnt++] = sub_octet;
            goto l15_1;
            continue;
        l11: // 1011
            bits[cnt++] = sub_octet;
            l11_0:
            bits[cnt++] = 1 + sub_octet;
            bits[cnt++] = 3 + sub_octet;
            continue;
        l13: // 1101
            bits[cnt++] = sub_octet;
            goto l15_0;
        l14: // 1110
            bits[cnt++] = 1 + sub_octet;
            goto l15_0;
        l15: // 1111
            bits[cnt++] = 0 + sub_octet;
            bits[cnt++] = 1 + sub_octet;
            l15_0:
            bits[cnt++] = 2 + sub_octet;
            l15_1:
            bits[cnt++] = 3 + sub_octet;
        l0:
            continue;
    } // for
    return cnt;
}

#endif
#endif

/*! @brief Adaptor to copy 1 bits to array
    @internal
*/
template<typename B>
class copy_to_array_functor
{
public:
    copy_to_array_functor(B* bits): bp_(bits)
    {}

    B* ptr() { return bp_; }
    
    void operator()(unsigned bit_idx) BMNOEXCEPT { *bp_++ = (B)bit_idx; }
    
    void operator()(unsigned bit_idx0,
                    unsigned bit_idx1) BMNOEXCEPT
    {
        bp_[0] = (B)bit_idx0; bp_[1] = (B)bit_idx1;
        bp_+=2;
    }
    
    void operator()(unsigned bit_idx0,
                    unsigned bit_idx1,
                    unsigned bit_idx2) BMNOEXCEPT
    {
        bp_[0] = (B)bit_idx0; bp_[1] = (B)bit_idx1; bp_[2] = (B)bit_idx2;
        bp_+=3;
    }
    
    void operator()(unsigned bit_idx0,
                    unsigned bit_idx1,
                    unsigned bit_idx2,
                    unsigned bit_idx3) BMNOEXCEPT
    {
        bp_[0] = (B)bit_idx0; bp_[1] = (B)bit_idx1;
        bp_[2] = (B)bit_idx2; bp_[3] = (B)bit_idx3;
        bp_+=4;
    }

private:
    copy_to_array_functor(const copy_to_array_functor&);
    copy_to_array_functor& operator=(const copy_to_array_functor&);
private:
    B* bp_;
};


/*!
   \brief Unpacks word into list of ON bit indexes
   \param w - value
   \param bits - pointer on the result array
   \return number of bits in the list

   @ingroup bitfunc
*/
template<typename T,typename B>
unsigned bit_list(T w, B* bits) BMNOEXCEPT
{
    copy_to_array_functor<B> func(bits);
    bit_for_each(w, func);
    return (unsigned)(func.ptr() - bits);
}



/*!
   \brief Unpacks word into list of ON bit indexes (quad-bit based)
   \param w - value
   \param bits - pointer on the result array
   \return number of bits in the list

   @ingroup bitfunc
*/
template<typename T,typename B>
unsigned bit_list_4(T w, B* bits) BMNOEXCEPT
{
    copy_to_array_functor<B> func(bits);
    bit_for_each_4(w, func);
    return (unsigned)(func.ptr() - bits);
}

/*!
    \brief Unpacks word into list of ON bit indexes using popcnt method
    \param w - value
    \param bits - pointer on the result array
    \param offs - value to add to bit position (programmed shift)
    \return number of bits in the list

    @ingroup bitfunc
    @internal
*/
template<typename B>
unsigned short
bitscan_popcnt(bm::id_t w, B* bits, unsigned short offs) BMNOEXCEPT
{
    unsigned pos = 0;
    while (w)
    {
        bm::id_t t = w & -w;
        bits[pos++] = (B)(bm::word_bitcount(t - 1) + offs);
        w &= w - 1;
    }
    return (unsigned short)pos;
}

/*!
    \brief Unpacks word into list of ON bit indexes using popcnt method
    \param w - value
    \param bits - pointer on the result array
    \return number of bits in the list
 
    @ingroup bitfunc
    @internal
*/
template<typename B>
unsigned short bitscan_popcnt(bm::id_t w, B* bits) BMNOEXCEPT
{
    unsigned pos = 0;
    while (w)
    {
        bm::id_t t = w & -w;
        bits[pos++] = (B)(bm::word_bitcount(t - 1));
        w &= w - 1;
    }
    return (unsigned short)pos;
}


/*!
    \brief Unpacks 64-bit word into list of ON bit indexes using popcnt method
    \param w - value
    \param bits - pointer on the result array
    \return number of bits in the list
    @ingroup bitfunc
*/
template<typename B>
unsigned short bitscan_popcnt64(bm::id64_t w, B* bits) BMNOEXCEPT
{
    unsigned short pos = 0;
    while (w)
    {
        bm::id64_t t = bmi_blsi_u64(w); // w & -w;
        bits[pos++] = (B) bm::word_bitcount64(t - 1);
        w = bmi_bslr_u64(w); // w &= w - 1;
    }
    return pos;
}

/*!
    \brief Unpacks word into list of ON bits (BSF/__builtin_ctz)
    \param w - value
    \param bits - pointer on the result array
    \return number of bits in the list

    @ingroup bitfunc
    @internal
*/
template<typename B>
unsigned short bitscan_bsf(unsigned w, B* bits) BMNOEXCEPT
{
    unsigned short pos = 0;
    while (w)
    {
        bits[pos++] = count_trailing_zeros_u32(w);
        w &= w - 1;
    }
    return pos;
}

template<typename B, typename OT>
unsigned short bitscan_bsf(unsigned w, B* bits, OT offs) BMNOEXCEPT
{
    unsigned short pos = 0;
    while (w)
    {
        bits[pos++] = (B) (bm::count_trailing_zeros_u32(w) + offs);
        w &= w - 1;
    }
    return pos;
}

/*!
    \brief Unpacks word into list of ON bits (BSF/__builtin_ctz)
    \param w - value
    \param bits - pointer on the result array
    \return number of bits in the list

    @ingroup bitfunc
    @internal
*/
template<typename B>
unsigned short bitscan_bsf64(bm::id64_t w, B* bits) BMNOEXCEPT
{
    unsigned short pos = 0;
    while (w)
    {
        bits[pos++] = bm::count_trailing_zeros_u64(w);
        w &= w - 1;
    }
    return pos;
}


/*!
  \brief Unpacks 64-bit word into list of ON bit indexes using popcnt method
  \param w - value
  \param bits - pointer on the result array
  \param offs - value to add to bit position (programmed shift)
  \return number of bits in the list
  @ingroup bitfunc
*/
template<typename B>
unsigned short
bitscan_popcnt64(bm::id64_t w, B* bits, unsigned short offs) BMNOEXCEPT
{
    unsigned short pos = 0;
    while (w)
    {
        bm::id64_t t = bmi_blsi_u64(w); // w & -w;
        bits[pos++] = B(bm::word_bitcount64(t - 1) + offs);
        w = bmi_bslr_u64(w); // w &= w - 1;
    }
    return pos;
}

/**
    Templated Bitscan with dynamic dispatch for best type
    @ingroup bitfunc
 */
template<typename V, typename B>
unsigned short bitscan(V w, B* bits) BMNOEXCEPT
{
    static_assert(std::is_unsigned<V>::value, "BM: unsigned type is required");
#if (defined(__arm__) || defined(__aarch64__))
    if constexpr (sizeof(V) == 8)
        return bm::bitscan_bsf64(w, bits);
    else
        return bm::bitscan_bsf((bm::word_t)w, bits);
#else
    if constexpr (sizeof(V) == 8)
        return bm::bitscan_popcnt64(w, bits);
    else
        return bm::bitscan_popcnt((bm::word_t)w, bits);
#endif
}

// --------------------------------------------------------------
// Functions for select
// --------------------------------------------------------------

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 64-bit work to search
    \param rank - rank to select (should be > 0)
 
    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal
*/
inline
unsigned word_select64_linear(bm::id64_t w, unsigned rank) BMNOEXCEPT
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    
    for (unsigned count = 0; w; w >>=1ull, ++count)
    {
        rank -= unsigned(w & 1ull);
        if (!rank)
            return count;
    }
    BM_ASSERT(0); // shoud not be here if rank is achievable
    return ~0u;
}

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 64-bit work to search
    \param rank - rank to select (should be > 0)
 
    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal

*/
inline
unsigned word_select64_bitscan_popcnt(bm::id64_t w, unsigned rank) BMNOEXCEPT
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= bm::word_bitcount64(w));

    do
    {
        --rank;
        if (!rank)
            break;
        w &= w - 1;
    } while (1);
    bm::id64_t t = w & -w;
    unsigned count = bm::word_bitcount64(t - 1);
    return count;
}

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 64-bit work to search
    \param rank - rank to select (should be > 0)

    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal

*/
inline
unsigned word_select64_bitscan_tz(bm::id64_t w, unsigned rank) BMNOEXCEPT
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= bm::word_bitcount64(w));

    do
    {
        if (!(--rank))
            break;
        w &= w - 1;
    } while (1);
    bm::id64_t t = w & -w;
    unsigned count = bm::count_trailing_zeros_u64(t);
    return count;
}



/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 32-bit work to search
    \param rank - rank to select (should be > 0)

    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal
*/
inline
unsigned word_select32_bitscan_popcnt(unsigned w, unsigned rank) BMNOEXCEPT
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= bm::word_bitcount(w));

    do
    {
        --rank;
        if (!rank)
            break;
        w &= w - 1;
    } while (1);
    unsigned t = w & -w;
    unsigned count = bm::word_bitcount(t - 1);
    return count;
}

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 32-bit work to search
    \param rank - rank to select (should be > 0)

    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal

*/
inline
unsigned word_select32_bitscan_tz(unsigned w, unsigned rank) BMNOEXCEPT
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= bm::word_bitcount(w));

    do
    {
        if (!(--rank))
            break;
        w &= w - 1;
    } while (1);
    return bm::count_trailing_zeros_u32(w & -w);
}


/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 64-bit work to search
    \param rank - rank to select (should be > 0)
 
    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal
*/
inline
unsigned word_select64(bm::id64_t w, unsigned rank) BMNOEXCEPT
{
#if defined(BMI2_SELECT64)
    return BMI2_SELECT64(w, rank);
#else
    #if defined(BMI1_SELECT64)
        return BMI2_SELECT64(w, rank);
    #else
        #if (defined(__arm__) || defined(__aarch64__))
            return bm::word_select64_bitscan_tz(w, rank);
        #else
            return bm::word_select64_bitscan_popcnt(w, rank);
        #endif
    #endif
#endif
}

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 32-bit work to search
    \param rank - rank to select (should be > 0)

    \return selected value (inxed of bit set)
    @ingroup bitfunc
    @internal
*/
inline
unsigned word_select32(unsigned w, unsigned rank) BMNOEXCEPT
{
#if defined(BMI2_SELECT64)
    return BMI2_SELECT64(w, rank);
#else
    #if defined(BMI1_SELECT64)
        return BMI2_SELECT64(w, rank);
    #else
        #if (defined(__arm__) || defined(__aarch64__))
            return bm::word_select32_bitscan_tz(w, rank);
        #else
            return bm::word_select32_bitscan_popcnt(w, rank);
        #endif
    #endif
#endif
}



// --------------------------------------------------------------
// Functions for bit-block digest calculation
// --------------------------------------------------------------

/*!
   \brief Compute digest mask for word address in block

    @return digest bit-mask
 
   @ingroup bitfunc
   @internal
*/
BMFORCEINLINE
bm::id64_t widx_to_digest_mask(unsigned w_idx) BMNOEXCEPT
{
    bm::id64_t mask(1ull);
    return mask << (w_idx / bm::set_block_digest_wave_size);
}

/** digest mask control generation (for debug and test only)
    @internal
 */
inline
bm::id64_t dm_control(unsigned from, unsigned to) BMNOEXCEPT
{
    bm::id64_t m = 0;
    for (unsigned i = from; i <= to; ++i)
        m |= (1ull << (i / 1024));
    return m;
}


/**
   \brief Compute digest mask for [from..to] positions
    \param from - range from (in bit-block coordinates)
    \param to - range to (in bit-block coordinates)

   @ingroup bitfunc
   @internal
*/
BMFORCEINLINE
bm::id64_t digest_mask(unsigned from, unsigned to) BMNOEXCEPT
{
    BM_ASSERT(from <= to);
    BM_ASSERT(to < bm::gap_max_bits);
    
    bm::id64_t digest_from = from >> bm::set_block_digest_pos_shift;
    bm::id64_t digest_to = to >> bm::set_block_digest_pos_shift;;
    bm::id64_t mask =
        ((~0ull) >> (63 - (digest_to - digest_from))) << digest_from;

    //BM_ASSERT(mask == bm::dm_control(from, to));

    return mask;
}


/*!
    \brief check if all digest bits for the range [from..to] are 0
 
    \param digest - current digest
    \param bitpos_from - range from (in bit-block coordinates)
    \param bitpos_to - range to (in bit-block coordinates)
 
    @return true if all range is zero
 
   @ingroup bitfunc
   @internal
*/
BMFORCEINLINE
bool check_zero_digest(bm::id64_t digest,
                       unsigned bitpos_from, unsigned bitpos_to) BMNOEXCEPT
{
    bm::id64_t mask = bm::digest_mask(bitpos_from, bitpos_to);
    return !(digest & mask);
}

/**
    \brief Is one range of 1s ( 0000110000 - one range, 000011000010 - more than one)
    @return true
    @internal
 */
inline
bool is_digest_one_range(bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(digest);
    bm::id64_t mask = 1;
    bool prev = digest & mask;
    unsigned cnt = prev;
    for (mask <<= 1; mask; mask <<= 1)
    {
        bool curr = digest & mask;
        if (curr && curr != prev)
            cnt++;
        prev = curr;
    } // for
    return cnt == 1;
}

/*!
   \brief Init block with 000111000 pattren based on digest
   \param block  - Bit block [out]
   \param digest - digest (used for block initialization)
 
   @ingroup bitfunc
   @internal
*/
inline
void block_init_digest0(bm::word_t* const block, bm::id64_t digest) BMNOEXCEPT
{
    for (unsigned i = 0; i < 64; ++i)
    {
        unsigned off = i * bm::set_block_digest_wave_size;
        bm::word_t mask = 0u - unsigned(digest & 1u); // (digest & 1) ? ~0u : 0u;
        BM_ASSERT(mask == ((digest & 1) ? ~0u : 0u));
#if defined(VECT_BLOCK_SET_DIGEST)
        VECT_BLOCK_SET_DIGEST(&block[off], mask);
#else
        for (; off < (i * bm::set_block_digest_wave_size)
                           + bm::set_block_digest_wave_size; off+=4)
            block[off] = block[off+1] = block[off+2] = block[off+3] = mask;
#endif
        digest >>= 1ull;
    } // for i
}

/*!
   \brief Compute digest for 64 non-zero areas
   \param block - Bit block
 
    @return digest bit-mask (0 means all block is empty)
 
   @ingroup bitfunc
   @internal
*/
inline
bm::id64_t calc_block_digest0(const bm::word_t* const block) BMNOEXCEPT
{
    bm::id64_t digest0 = 0;
    unsigned   off;
    
    for (unsigned i = 0; i < 64; ++i)
    {
        off = i * bm::set_block_digest_wave_size;
        #if defined(VECT_IS_DIGEST_ZERO)
            bool all_zero = VECT_IS_DIGEST_ZERO(&block[off]);
            digest0 |= bm::id64_t(!all_zero) << i;
        #else
            const bm::id64_t mask(1ull);
            for (unsigned j = 0; j < bm::set_block_digest_wave_size; j+=4)
            {
                bm::word_t w = block[off+j+0] | block[off+j+1] |
                               block[off+j+2] | block[off+j+3];
                if (w)
                {
                    digest0 |= (mask << i);
                    break;
                }
            } // for j
        #endif
        
    } // for i
    return digest0;
}

/*!
   \brief Compute digest for 64 non-zero areas based on existing digest
          (function revalidates zero areas)
   \param block - bit block
   \param digest - start digest
 
    @return digest bit-mask (0 means all block is empty)
 
   @ingroup bitfunc
   @internal
*/
inline
bm::id64_t
update_block_digest0(const bm::word_t* const block, bm::id64_t digest) BMNOEXCEPT
{
    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;

    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_IS_DIGEST_ZERO)
            bool all_zero = VECT_IS_DIGEST_ZERO(&block[off]);
            digest &= all_zero ? ~(mask << wave) : digest;
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u =
                            (const bm::bit_block_t::bunion_t*)(&block[off]);
            bm::id64_t w64 = 0;
            unsigned j = 0;
            do
            {
                w64 |=
                    src_u->w64[j+0] | src_u->w64[j+1] | src_u->w64[j+2] | src_u->w64[j+3];
                j += 4;
            } while ((j < bm::set_block_digest_wave_size/2) & !w64);
            digest &= w64 ? digest : ~(mask << wave);
        #endif
        
    } // while

//    BM_ASSERT(bm::calc_block_digest0(block) == digest);
    return digest;
}

// --------------------------------------------------------------

/**
    Compact sub-blocks by digest (rank compaction)
    \param t_block - target block
    \param block - src bit block
    \param digest - digest to use for copy
    \param zero_tail - flag to zero the tail memory

   @ingroup bitfunc
   @internal
 */
inline
void block_compact_by_digest(bm::word_t*   t_block,
                       const bm::word_t*   block,
                       bm::id64_t          digest,
                       bool zero_tail) BMNOEXCEPT
{
    unsigned t_wave = 0;
    bm::id64_t d = digest;

    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        unsigned t_off = t_wave * bm::set_block_digest_wave_size;

        const bm::word_t* sub_block = block + off;
        bm::word_t* t_sub_block = t_block + t_off;
        const bm::word_t* sub_block_end =
                    sub_block + bm::set_block_digest_wave_size;
        // TODO 64-bit optimization or SIMD
        for (; sub_block < sub_block_end; t_sub_block+=4, sub_block+=4)
        {
            t_sub_block[0] = sub_block[0];
            t_sub_block[1] = sub_block[1];
            t_sub_block[2] = sub_block[2];
            t_sub_block[3] = sub_block[3];
        } // for

        d = bm::bmi_bslr_u64(d); // d &= d - 1;
        ++t_wave;
    } // while

    // init rest as zeroes (maybe need to be parametric)
    //
    if (zero_tail)
    {
        for ( ;t_wave < bm::block_waves; ++t_wave)
        {
            unsigned t_off = t_wave * bm::set_block_digest_wave_size;
            bm::word_t* t_sub_block = t_block + t_off;
            const bm::word_t* t_sub_block_end =
                        t_sub_block + bm::set_block_digest_wave_size;
            for (; t_sub_block < t_sub_block_end; t_sub_block+=4)
            {
                t_sub_block[0] = 0;
                t_sub_block[1] = 0;
                t_sub_block[2] = 0;
                t_sub_block[3] = 0;
            } // for
        } // for
    }
}

// --------------------------------------------------------------

/**
    expand sub-blocks by digest (rank expansion)
    \param t_block - target block
    \param block - src bit block
    \param digest - digest to use for copy
    \param zero_subs - flag to zero the non-digest sub waves

   @ingroup bitfunc
   @internal
 */

inline
void block_expand_by_digest(bm::word_t*   t_block,
                       const bm::word_t*   block,
                       bm::id64_t          digest,
                       bool zero_subs) BMNOEXCEPT
{
    unsigned s_wave = 0;
    bm::id64_t d = digest;

    bm::id64_t mask1 = 1ULL;
    for (unsigned wave = 0; wave < bm::block_waves; ++wave)
    {
        unsigned s_off = s_wave * bm::set_block_digest_wave_size;
        const bm::word_t* sub_block = block + s_off;
        const bm::word_t* sub_block_end =
                    sub_block + bm::set_block_digest_wave_size;
        bm::word_t* t_sub_block =
            t_block + (wave * bm::set_block_digest_wave_size);
        if (d & mask1)
        {
            for (; sub_block < sub_block_end; t_sub_block+=4, sub_block+=4)
            {
                t_sub_block[0] = sub_block[0];
                t_sub_block[1] = sub_block[1];
                t_sub_block[2] = sub_block[2];
                t_sub_block[3] = sub_block[3];
            } // for

            ++s_wave;
        }
        else
        {
            if (zero_subs)
            {
                const bm::word_t* t_sub_block_end =
                            t_sub_block + bm::set_block_digest_wave_size;
                for (; t_sub_block < t_sub_block_end; t_sub_block+=4)
                {
                    t_sub_block[0] = 0; t_sub_block[1] = 0;
                    t_sub_block[2] = 0; t_sub_block[3] = 0;
                } // for
            }
        }
        mask1 <<= 1;
    } // for i
}


// --------------------------------------------------------------


/// Returns true if set operation is constant (bitcount)
inline
bool is_const_set_operation(set_operation op) BMNOEXCEPT
{
    return (int(op) >= int(set_COUNT));
}

/**
    Convert set operation to operation
*/
inline
bm::operation setop2op(bm::set_operation op) BMNOEXCEPT
{
    BM_ASSERT(op == set_AND || 
              op == set_OR  || 
              op == set_SUB || 
              op == set_XOR);
    return (bm::operation) op;
}

//---------------------------------------------------------------------

/** 
    Structure carries pointer on bit block with all bits 1
    @ingroup bitfunc
    @internal
*/
template<bool T> struct all_set
{
    struct BM_VECT_ALIGN all_set_block
    {
        bm::word_t BM_VECT_ALIGN* _s[bm::set_sub_array_size] BM_VECT_ALIGN_ATTR;
        bm::word_t BM_VECT_ALIGN  _p[bm::set_block_size] BM_VECT_ALIGN_ATTR;
        bm::word_t* _p_fullp;

        all_set_block() BMNOEXCEPT
        {
            ::memset(_p, 0xFF, sizeof(_p)); // set FULL BLOCK content (all 1s)
            if constexpr (sizeof(void*) == 8)
            {
                const unsigned long long magic_mask = 0xFFFFfffeFFFFfffe;
                ::memcpy(&_p_fullp, &magic_mask, sizeof(magic_mask));
                for (unsigned i = 0; i < bm::set_sub_array_size; ++i)
                    _s[i] = reinterpret_cast<bm::word_t*>(magic_mask);
            }
            else 
            {
                const unsigned magic_mask = 0xFFFFfffe;
                ::memcpy(&_p_fullp, &magic_mask, sizeof(magic_mask));
                for (unsigned i = 0; i < bm::set_sub_array_size; ++i)
                    _s[i] = reinterpret_cast<bm::word_t*>(magic_mask);
            }
        }
    };

    // version with minimal branching, super-scalar friendly
    //
    inline
    static bm::id64_t block_type(const bm::word_t* bp) BMNOEXCEPT
    {
        bm::id64_t type;
        if constexpr (sizeof(void*) == 8)
        {
            bm::id64_t w = reinterpret_cast<unsigned long long>(bp);
            type = (w & 3) | // FULL BLOCK or GAP
                ((bp == _block._p) << 1);
            type = type ? type : w;
        }
        else
        {
            unsigned w = reinterpret_cast<unsigned long>(bp);
            type = (w & 3) | // FULL BLOCK or GAP
                ((bp == _block._p) << 1);
            type = type ? type : w;
        }
        return type;
    }

    BMFORCEINLINE 
    static bool is_full_block(const bm::word_t* bp) BMNOEXCEPT
        { return (bp == _block._p || bp == _block._p_fullp); }

    BMFORCEINLINE 
    static bool is_valid_block_addr(const bm::word_t* bp) BMNOEXCEPT
        { return (bp && !(bp == _block._p || bp == _block._p_fullp)); }

    static all_set_block  _block;
};


template<bool T> typename all_set<T>::all_set_block all_set<T>::_block;


/*!
    Fini not NULL position
    @return index of not NULL pointer
    @internal
*/
template<typename N>
bool find_not_null_ptr(const bm::word_t* const * const* arr,
                       N start, N size, N* pos) BMNOEXCEPT
{
    BM_ASSERT(pos);
//    BM_ASSERT(start < size);
//#if defined(BM64_AVX2) || defined(BM64_AVX512)
// TODO: optimization for SIMD based next ptr scan
#if 0
    const unsigned unroll_factor = 4;
    const unsigned len = (size - start);
    const unsigned len_unr = len - (len % unroll_factor);
    unsigned k;
    
    arr += start;
    for (k = 0; k < len_unr; k+=unroll_factor)
    {
        if (!avx2_test_all_zero_wave(arr+k))
        {
            if (arr[k])
            {
                *pos = k + start;
                return true;
            }
            if (arr[k+1])
            {
                *pos = k + start + 1;
                return true;
            }
            if (arr[k+2])
            {
                *pos = k + start + 2;
                return true;
            }
            if (arr[k+3])
            {
                *pos = k + start + 3;
                return true;
            }
        }
    } // for k
    
    for (; k < len; ++k)
    {
        if (arr[k])
        {
            *pos = k + start;
            return true;
        }
    } // for k
#else
    for (; start < size; ++start)
    {
        if (arr[start])
        {
            *pos = start;
            return true;
        }
    } // for i
#endif
    return false;
}




//---------------------------------------------------------------------

/*! 
   \brief Lexicographical comparison of two words as bit strings (reference)
   Auxiliary implementation for testing and reference purposes.
   \param w1 - First word.
   \param w2 - Second word.
   \return  <0 - less, =0 - equal,  >0 - greater.

   @ingroup bitfunc 
*/
template<typename T> int wordcmp0(T w1, T w2) BMNOEXCEPT
{
    while (w1 != w2)
    {
        int res = (w1 & 1) - (w2 & 1);
        if (res != 0) return res;
        w1 >>= 1;
        w2 >>= 1;
    }
    return 0;
}


/*
template<typename T> int wordcmp(T w1, T w2)
{
    T diff = w1 ^ w2;
    return diff ? ((w1 & diff & (diff ^ (diff - 1)))? 1 : -1) : 0; 
}
*/
/*! 
   \brief Lexicographical comparison of two words as bit strings.
   Auxiliary implementation for testing and reference purposes.
   \param a - First word.
   \param b - Second word.
   \return  <0 - less, =0 - equal,  >0 - greater.

   @ingroup bitfunc 
*/
template<typename T> int wordcmp(T a, T b) BMNOEXCEPT
{
    T diff = a ^ b;
    return diff? ( (a & diff & -diff)? 1 : -1 ) : 0;
}


// Low bit extraction
// x & (x ^ (x-1))


// ----------------------------------------------------------------------


/*! @brief Returns "true" if all bits in the block are 0
    @ingroup bitfunc
*/
inline
bool bit_is_all_zero(const bm::word_t* BMRESTRICT start) BMNOEXCEPT
{
#if defined(VECT_IS_ZERO_BLOCK)
    return VECT_IS_ZERO_BLOCK(start);
#else
   const bm::wordop_t* BMRESTRICT blk = (bm::wordop_t*) (start);
   const bm::wordop_t* BMRESTRICT blk_end = (bm::wordop_t*)(start + bm::set_block_size);
   do
   {
        if (blk[0] | blk[1] | blk[2] | blk[3])
            return false;
        blk += 4;
   } while (blk < blk_end);
   return true;
#endif
}

// ----------------------------------------------------------------------

/*!
   \brief Checks if GAP block is all-zero.
   \param buf - GAP buffer pointer.
   \returns true if all-zero.

   @ingroup gapfunc
*/
BMFORCEINLINE
bool gap_is_all_zero(const bm::gap_word_t* BMRESTRICT buf) BMNOEXCEPT
{
    // (almost) branchless variant:
    return (!(*buf & 1u)) & (!(bm::gap_max_bits - 1 - buf[1]));
}

/*!
   \brief Checks if GAP block is all-one.
   \param buf - GAP buffer pointer.
   \returns true if all-one.
   @ingroup gapfunc
*/
BMFORCEINLINE
bool gap_is_all_one(const bm::gap_word_t* BMRESTRICT buf) BMNOEXCEPT
{
    return ((*buf & 1u) && (buf[1] == bm::gap_max_bits - 1));
}

/*!
   \brief Returs GAP block length.
   \param buf - GAP buffer pointer.
   \returns GAP block length.

   @ingroup gapfunc
*/
BMFORCEINLINE
bm::gap_word_t gap_length(const bm::gap_word_t* BMRESTRICT buf) BMNOEXCEPT
{
    return (bm::gap_word_t)((*buf >> 3) + 1);
}


/*!
   \brief Returs GAP block capacity
   \param buf - GAP buffer pointer
   \param glevel_len - pointer on GAP header word
   \returns GAP block capacity.

   @ingroup gapfunc
*/
template<typename T>
unsigned
gap_capacity(const T* BMRESTRICT buf, const T* BMRESTRICT glevel_len) BMNOEXCEPT
{
    return glevel_len[(*buf >> 1) & 3];
}


/*!
   \brief Returs GAP block capacity limit.
   \param buf - GAP buffer pointer.
   \param glevel_len - GAP lengths table (gap_len_table)
   \returns GAP block limit.

   @ingroup gapfunc
*/
template<typename T>
unsigned
gap_limit(const T* BMRESTRICT buf, const T* BMRESTRICT glevel_len) BMNOEXCEPT
{
    return glevel_len[(*buf >> 1) & 3]-4;
}


/*!
   \brief Returs GAP blocks capacity level.
   \param buf - GAP buffer pointer.
   \returns GAP block capacity level.

   @ingroup gapfunc
*/
template<typename T>
T gap_level(const T* BMRESTRICT buf) BMNOEXCEPT
{
    return T((*buf >> 1) & 3u);
}


/*!
    \brief GAP block find the last set bit

    \param buf - GAP buffer pointer.
    \param last - index of the last 1 bit
 
    \return 0 if 1 bit was NOT found

    @ingroup gapfunc
*/
template<typename T>
unsigned
gap_find_last(const T* BMRESTRICT buf, unsigned* BMRESTRICT last) BMNOEXCEPT
{
    BM_ASSERT(last);

    T is_set = (*buf) & 1u;
    T end = T((*buf) >> 3u);

    BM_ASSERT(buf[end] == bm::gap_max_bits - 1);

    is_set ^= T((end-1) & 1u);
    if (is_set)
    {
        *last = buf[end];
        return is_set;
    }
    *last = buf[--end];
    return end;
}

/*!
    \brief GAP block find the first set bit

    \param buf - GAP buffer pointer.
    \param first - index of the first 1 bit
 
    \return 0 if 1 bit was NOT found

    @ingroup gapfunc
*/
template<typename T>
unsigned
gap_find_first(const T* BMRESTRICT buf, unsigned* BMRESTRICT first) BMNOEXCEPT
{
    BM_ASSERT(first);

    T is_set = (*buf) & 1u;
    if (is_set)
    {
        *first = 0;
        return is_set;
    }
    if (buf[1] == bm::gap_max_bits - 1)
        return 0;
    *first = buf[1] + 1;
    return 1;
}



/*
   \brief Binary search for the block where bit = pos located.
   \param buf - GAP buffer pointer.
   \param pos - index of the element.
   \param is_set - output. GAP value (0 or 1). 
   \return GAP index.
   @ingroup gapfunc
*/
template<typename T> 
unsigned gap_bfind(const T* BMRESTRICT buf,
                   unsigned pos, unsigned* BMRESTRICT is_set) BMNOEXCEPT
{
    BM_ASSERT(pos < bm::gap_max_bits);
    #undef VECT_GAP_BFIND // TODO: VECTOR bfind causes performance degradation
    #ifdef VECT_GAP_BFIND
        return VECT_GAP_BFIND(buf, pos, is_set);
    #else
        *is_set = (*buf) & 1;
        unsigned start = 1;
        unsigned end = 1 + ((*buf) >> 3);
        while (start != end)
        {
            if ((end - start) < 16) // use direct scan on short span
            {
                do
                {
                    if (buf[start] >= pos)
                        goto break2;
                } while (++start);
                BM_ASSERT(0); // should not get here
                break;
            }
            unsigned curr = (start + end) >> 1;
            if ( buf[curr] < pos )
                start = curr + 1;
            else
                end = curr;
        } // while
        break2:
        *is_set ^= ((start-1) & 1);
        return start;
    #endif
}


/*!
   \brief Tests if bit = pos is true.
   \param buf - GAP buffer pointer.
   \param pos - index of the element.
   \return true if position is in "1" gap
   @ingroup gapfunc
*/
template<typename T>
unsigned gap_test(const T* BMRESTRICT buf, unsigned pos) BMNOEXCEPT
{
    BM_ASSERT(pos < bm::gap_max_bits);

    unsigned start = 1;
    unsigned end = 1 + ((*buf) >> 3);
    if (end - start < 10)
    {
        unsigned sv = *buf & 1;
        unsigned sv1= sv ^ 1;
        if (buf[1] >= pos) return sv;
        if (buf[2] >= pos) return sv1;
        if (buf[3] >= pos) return sv;
        if (buf[4] >= pos) return sv1;
        if (buf[5] >= pos) return sv;
        if (buf[6] >= pos) return sv1;
        if (buf[7] >= pos) return sv;
        if (buf[8] >= pos) return sv1;
        BM_ASSERT(buf[9] >= pos);
        return sv;
    }
    else
    {
        BM_ASSERT(start != end);
        do
        {
            if (unsigned curr = (start + end) >> 1; buf[curr] < pos)
                start = curr + 1;
            else
                end = curr;
        } while (start != end);
    }
    return ((*buf) & 1) ^ ((--start) & 1); 
}

/*!
    \brief Tests if bit = pos is true. Analog of bm::gap_test with SIMD unrolling.
    \param buf - GAP buffer pointer.
    \param pos - index of the element.
    \return true if position is in "1" gap
    @ingroup gapfunc
*/
template<typename T> 
unsigned gap_test_unr(const T* BMRESTRICT buf, const unsigned pos) BMNOEXCEPT
{
    BM_ASSERT(buf);
    BM_ASSERT(pos < bm::gap_max_bits);

#if defined(VECT_GAP_TEST)
    unsigned res = VECT_GAP_TEST(buf, pos);
    BM_ASSERT(res == bm::gap_test(buf, pos));
#else
    unsigned res = bm::gap_test(buf, pos);
#endif
    return res;
}

/*! For each non-zero block in [from, to] executes supplied functor
    \internal
*/
template<typename T, typename N, typename F>
void for_each_nzblock_range(T*** root,
                            N top_size, N nb_from, N nb_to, F& f) BMNOEXCEPT
{
    BM_ASSERT(top_size);
    if (nb_from > nb_to)
        return;
    unsigned i_from = unsigned(nb_from >> bm::set_array_shift);
    unsigned j_from = unsigned(nb_from &  bm::set_array_mask);
    unsigned i_to = unsigned(nb_to >> bm::set_array_shift);
    unsigned j_to = unsigned(nb_to &  bm::set_array_mask);
    
    if (i_from >= top_size)
        return;
    if (i_to >= top_size)
    {
        i_to = unsigned(top_size-1);
        j_to = bm::set_sub_array_size-1;
    }
    
    for (unsigned i = i_from; i <= i_to; ++i)
    {
        T** blk_blk = root[i];
        if (!blk_blk)
            continue;
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            unsigned j = (i == i_from) ? j_from : 0;
            if (!j && (i != i_to)) // full sub-block
                f.add_full(bm::set_sub_total_bits);
            else
            {
                do
                {
                    f.add_full(bm::gap_max_bits);
                    if ((i == i_to) && (j == j_to))
                        return;
                } while (++j < bm::set_sub_array_size);
            }
        }
        else
        {
            unsigned j = (i == i_from) ? j_from : 0;
            do
            {
                if (blk_blk[j])
                    f(blk_blk[j]);
                if ((i == i_to) && (j == j_to))
                    return;
            } while (++j < bm::set_sub_array_size);
        }
    } // for i
}

/*! For each non-zero block executes supplied function.
    \internal
*/
template<class T, class F> 
void for_each_nzblock(T*** root, unsigned size1, F& f)
{
    typedef typename F::size_type size_type;
    for (unsigned i = 0; i < size1; ++i)
    {
        T** blk_blk = root[i];
        if (!blk_blk) 
        {
            f.on_empty_top(i);
            continue;
        }
        f.on_non_empty_top(i);
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            size_type r = i * bm::set_sub_array_size;
            unsigned j = 0;
            do
            {
                f(FULL_BLOCK_FAKE_ADDR, r + j);
            } while (++j < bm::set_sub_array_size);
            continue;
        }

        unsigned non_empty_top = 0;
        size_type r = i * bm::set_sub_array_size;
        unsigned j = 0;
        do
        {
#if defined(BM64_AVX2) || defined(BM64_AVX512)
            if (!avx2_test_all_zero_wave(blk_blk + j))
            {
                non_empty_top = 1;
                T* blk0 = blk_blk[j + 0];
                T* blk1 = blk_blk[j + 1];
                T* blk2 = blk_blk[j + 2];
                T* blk3 = blk_blk[j + 3];

                size_type block_idx = r + j + 0;
                if (blk0)
                    f(blk0, block_idx);
                else
                    f.on_empty_block(block_idx);

                if (blk1)
                    f(blk1, block_idx + 1);
                else
                    f.on_empty_block(block_idx + 1);

                if (blk2)
                    f(blk2, block_idx + 2);
                else
                    f.on_empty_block(block_idx + 2);

                if (blk3)
                    f(blk3, block_idx + 3);
                else
                    f.on_empty_block(block_idx + 3);
            }
            else
            {
                f.on_empty_block(r + j + 0); f.on_empty_block(r + j + 1);
                f.on_empty_block(r + j + 2); f.on_empty_block(r + j + 3);
            }
            j += 4;
#elif defined(BM64_SSE4)
            if (!sse42_test_all_zero_wave((blk_blk + j)))
            {
                non_empty_top = 1;
                T* blk0 = blk_blk[j + 0];
                T* blk1 = blk_blk[j + 1];
                
                size_type block_idx = r + j + 0;
                if (blk0)
                    f(blk0, block_idx);
                else
                    f.on_empty_block(block_idx);

                ++block_idx;
                if (blk1)
                    f(blk1, block_idx);
                else
                    f.on_empty_block(block_idx);
            }
            else
            {
                f.on_empty_block(r + j + 0);
                f.on_empty_block(r + j + 1);
            }
            j += 2;
#else
            if (blk_blk[j])
            {
                f(blk_blk[j], r + j);
                non_empty_top = 1;
            }
            else
                f.on_empty_block(r + j);
            ++j;
#endif
        } while (j < bm::set_sub_array_size);

        if (non_empty_top == 0)
            f.on_empty_top(i);
    }  // for i
}

/*! For each non-zero block executes supplied function.
*/
template<class T, class F> 
void for_each_nzblock2(T*** root, unsigned size1, F& f)
{
#ifdef BM64_SSE4
    for (unsigned i = 0; i < size1; ++i)
    {
        T** blk_blk;
        if ((blk_blk = root[i])!=0)
        {
            if (blk_blk == (T**)FULL_BLOCK_FAKE_ADDR)
            {
                for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
                {
                    f(FULL_BLOCK_FAKE_ADDR);
                }
                continue;
            }
            {
                __m128i w0;
                for (unsigned j = 0; j < bm::set_sub_array_size; j+=4)
                {
                    w0 = _mm_loadu_si128((__m128i*)(blk_blk + j));
                    if (!_mm_testz_si128(w0, w0))
                    {
                        T* blk0 = blk_blk[j + 0];
                        T* blk1 = blk_blk[j + 1];

                        if (blk0)
                            f(blk0);
                        if (blk1)
                            f(blk1);
                    }
                    w0 = _mm_loadu_si128((__m128i*)(blk_blk + j + 2));
                    if (!_mm_testz_si128(w0, w0))
                    {
                        T* blk0 = blk_blk[j + 2];
                        T* blk1 = blk_blk[j + 3];
                        if (blk0)
                            f(blk0);
                        if (blk1)
                            f(blk1);
                    }
                } // for j
            }
        }
    }  // for i
#elif defined(BM64_AVX2) || defined(BM64_AVX512)
    for (unsigned i = 0; i < size1; ++i)
    {
        T** blk_blk;
        if ((blk_blk = root[i]) != 0)
        {
            if (blk_blk == (T**)FULL_BLOCK_FAKE_ADDR)
            {
                for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
                {
                    f(FULL_BLOCK_FAKE_ADDR);
                }
                continue;
            }
            {
                for (unsigned j = 0; j < bm::set_sub_array_size; j += 4)
                {
                    __m256i w0 = _mm256_loadu_si256((__m256i*)(blk_blk + j));
                    if (!_mm256_testz_si256(w0, w0))
                    {
                        // as a variant could use: blk0 = (T*)_mm256_extract_epi64(w0, 0);
                        // but it measures marginally slower
                        T* blk0 = blk_blk[j + 0];
                        T* blk1 = blk_blk[j + 1];
                        T* blk2 = blk_blk[j + 2];
                        T* blk3 = blk_blk[j + 3];
                        if (blk0)
                            f(blk0);
                        if (blk1)
                            f(blk1);
                        if (blk2)
                            f(blk2);
                        if (blk3)
                            f(blk3);
                    }
                } // for j
            }
        }
    }  // for i

#else // non-SIMD mode
    for (unsigned i = 0; i < size1; ++i)
    {
        T** blk_blk;
        if ((blk_blk = root[i])!=0) 
        {
            if (blk_blk == (T**)FULL_BLOCK_FAKE_ADDR)
            {
                for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
                {
                    f(FULL_BLOCK_FAKE_ADDR);
                }
                continue;
            }
            unsigned j = 0;
            do
            {
                if (blk_blk[j])
                    f(blk_blk[j]);
                if (blk_blk[j+1])
                    f(blk_blk[j+1]);
                if (blk_blk[j+2])
                    f(blk_blk[j+2]);
                if (blk_blk[j+3])
                    f(blk_blk[j+3]);
                j += 4;
            } while (j < bm::set_sub_array_size);
        }
    }  // for i
#endif
}


/*! For each non-zero block executes supplied function-predicate.
    Function returns if function-predicate returns true
*/
template<typename T, typename BI, typename F>
bool for_each_nzblock_if(T*** root, BI size1, F& f) BMNOEXCEPT
{
    BI block_idx = 0;
    for (BI i = 0; i < size1; ++i)
    {
        T** blk_blk = root[i];
        if (!blk_blk) 
        {
            block_idx += bm::set_sub_array_size;
            continue;
        }
        if (blk_blk == (T**)FULL_BLOCK_FAKE_ADDR)
        {
            for (unsigned j = 0; j < bm::set_sub_array_size; ++j, ++block_idx)
            {
                if (f(FULL_BLOCK_FAKE_ADDR, block_idx))
                    return true;
            } // for j
            continue;
        }

        for (unsigned j = 0;j < bm::set_sub_array_size; ++j, ++block_idx)
        {
            if (blk_blk[j]) 
                if (f(blk_blk[j], block_idx))
                    return true;
        } // for j
    } // for i
    return false;
}

/*! For each block executes supplied function.
*/
template<class T, class F, typename BLOCK_IDX>
void for_each_block(T*** root, unsigned size1, F& f, BLOCK_IDX start)
{
    BLOCK_IDX block_idx = start;
    for (unsigned i = 0; i < size1; ++i)
    {
        T** blk_blk = root[i];
        if (blk_blk)
        {
            if (blk_blk == (T**)FULL_BLOCK_FAKE_ADDR)
            {
                for (unsigned j = 0; j < bm::set_sub_array_size; ++j, ++block_idx)
                {
                    f(FULL_BLOCK_FAKE_ADDR, block_idx);
                }
                continue;
            }
            for (unsigned j = 0;j < bm::set_sub_array_size; ++j, ++block_idx)
            {
                f(blk_blk[j], block_idx);
            }
        }
        else
        {
            for (unsigned j = 0;j < bm::set_sub_array_size; ++j, ++block_idx)
            {
                f(0, block_idx);
            }
        }
    }  
}



/*! Special BM optimized analog of STL for_each
*/
template<class T, class F> F bmfor_each(T first, T last, F f)
{
    do
    {
        f(*first);
        ++first;
    } while (first < last);
    return f;
}

/*! Computes SUM of all elements of the sequence
*/
template<typename T>
bm::id64_t sum_arr(const T* first, const T* last) BMNOEXCEPT
{
    bm::id64_t sum = 0;
    for (;first < last; ++first)
        sum += *first;
    return sum;
}

/*!
    Extract short (len=1) exceptions from the GAP block
    \param buf - GAP buffer to split
    \param arr0 - [OUT] list of isolates 0 positions (clear list)
    \param arr1 - [OUT] list of isolated 1 positions (set list)
    \param arr0_cnt - [OUT] arr0 size
    \param arr1_cnt -
    @ingroup gapfunc
*/
template<typename T>
void gap_split(const T* buf,
              T* arr0, T* arr1, T& arr0_cnt, T& arr1_cnt) BMNOEXCEPT
{
    const T* pcurr = buf;
    unsigned len = (*pcurr >> 3);
    const T* pend = pcurr + len;

    T cnt0, cnt1;
    cnt0 = cnt1 = 0;
    unsigned is_set = (*buf & 1);

    if (*pcurr == 0)
    {
        if (is_set)
        {
            arr1[cnt1] = *pcurr;
            ++cnt1;
        }
        else
        {
            arr0[cnt0] = *pcurr;
            ++cnt0;
        }
    }
    T prev = *pcurr;
    ++pcurr;

    while (pcurr <= pend)
    {
        is_set ^= 1;
        T delta = *pcurr - prev;
        if (delta == 1)
        {
            if (is_set)
            {
                arr1[cnt1] = prev;
                ++cnt1;
            }
            else
            {
                arr0[cnt0] = prev;
                ++cnt0;
            }
        }
        prev = *pcurr++;
    } // while

    arr0_cnt = cnt0;
    arr1_cnt = cnt1;
}


/*!
   \brief Calculates number of bits ON in GAP buffer.
   \param buf - GAP buffer pointer.
   \param dsize - buffer size
   \return Number of non-zero bits.
   @ingroup gapfunc
*/
template<typename T>
unsigned gap_bit_count(const T* buf, unsigned dsize=0) BMNOEXCEPT
{
    const T* pcurr = buf;
    if (dsize == 0)
        dsize = (*pcurr >> 3);

    const T* pend = pcurr + dsize;

    unsigned bits_counter = 0;
    ++pcurr;

    if (*buf & 1)
    {
        bits_counter += *pcurr + 1;
        ++pcurr;
    }
    for (++pcurr; pcurr <= pend; pcurr += 2)
        bits_counter += *pcurr - *(pcurr-1);
    return bits_counter;
}

/*!
    \brief Calculates number of bits ON in GAP buffer. Loop unrolled version.
    \param buf - GAP buffer pointer.
    \return Number of non-zero bits.
    @ingroup gapfunc
*/
template<typename T>
unsigned gap_bit_count_unr(const T* buf) BMNOEXCEPT
{
    const T* pcurr = buf;
    unsigned dsize = (*pcurr >> 3);

    unsigned cnt = 0;
    pcurr = buf + 1; // set up start position
    T first_one = *buf & 1;
    if (first_one)
    {
        cnt += *pcurr + 1;
        ++pcurr;
    }
    ++pcurr;  // set GAP to 1

    #if defined(BMAVX2OPT) || defined(BMAVX512OPT)
    if (dsize > 34)
    {
        const unsigned unr_factor = 32;
        unsigned waves = (dsize-2) / unr_factor;
        pcurr = avx2_gap_sum_arr(pcurr, waves, &cnt);
    }
    #elif defined(BMSSE42OPT) || defined(BMSSE2OPT)
    if (dsize > 18)
    {
        const unsigned unr_factor = 16;
        unsigned waves = (dsize - 2) / unr_factor;
        pcurr = sse2_gap_sum_arr(pcurr, waves, &cnt);
    }
    #else
    if (dsize > 10)
    {
        const unsigned unr_factor = 8;
        unsigned waves = (dsize - 2) / unr_factor;
        for (unsigned i = 0; i < waves; i += unr_factor)
        {
            cnt += pcurr[0] - pcurr[0 - 1];
            cnt += pcurr[2] - pcurr[2 - 1];
            cnt += pcurr[4] - pcurr[4 - 1];
            cnt += pcurr[6] - pcurr[6 - 1];

            pcurr += unr_factor;
        } // for
    }
    #endif
    
    const T* pend = buf + dsize;
    for ( ; pcurr <= pend ; pcurr+=2)
        cnt += *pcurr - *(pcurr - 1);

    BM_ASSERT(cnt == bm::gap_bit_count(buf));
    return cnt;
}



/*!
   \brief Counts 1 bits in GAP buffer in the closed [left, right] range.
   \param buf - GAP buffer pointer.
   \param left - leftmost bit index to start from
   \param right - rightmost bit index
   \param pos - position in the
   \return Number of non-zero bits.
   @ingroup gapfunc
*/
template<typename T, bool RIGHT_END = false>
unsigned gap_bit_count_range(const T* const buf,
                             unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);
    
    unsigned is_set, bits_counter, prev_gap;
    unsigned start_pos = bm::gap_bfind(buf, left, &is_set);
    is_set = ~(is_set - 1u); // 0xFFF.. if true (mask for branchless code)

    const T* pcurr = buf + start_pos;
    if (right <= *pcurr) // we are in the target gap right now
        bits_counter = unsigned(right - left + 1u) & is_set;
    else
    {
        bits_counter = unsigned(*pcurr - left + 1u) & is_set;
        if constexpr (RIGHT_END) // count to the end
        {
            BM_ASSERT(right == bm::gap_max_bits-1);
            for (prev_gap = *pcurr++ ;true; prev_gap = *pcurr++)
            {
                bits_counter += (is_set ^= ~0u) & (*pcurr - prev_gap);
                if (*pcurr == bm::gap_max_bits-1)
                    break;
            } // for
        }
        else // true range search here
        {
            for (prev_gap = *pcurr++; right > *pcurr; prev_gap = *pcurr++)
                bits_counter += (is_set ^= ~0u) & (*pcurr - prev_gap);
            bits_counter += unsigned(right - prev_gap) & (is_set ^ ~0u);
        }
    }
    return bits_counter;

}

/*!
   \brief Counts 1 bits in GAP buffer in the closed [left, right] range using position hint to avoid bfind
   \param buf - GAP buffer pointer.
   \param left - leftmost bit index to start from
   \param right - rightmost bit index
   \param pos - position in the
   \param hint - position hint
   \return Number of non-zero bits.
   @ingroup gapfunc
*/
template<typename T>
unsigned gap_bit_count_range_hint(const T* const buf,
                  unsigned left, unsigned right, unsigned hint) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);

    unsigned is_set, bits_counter, prev_gap;

    // process the hint instead of binary search
    is_set = hint & 1;
    is_set = ~(is_set - 1u); // 0xFFF.. if true (mask for branchless code)
    unsigned start_pos = hint >> 1;
    {
        unsigned is_set_c; (void)is_set_c;
        unsigned pos; (void)pos;
        BM_ASSERT((pos = bm::gap_bfind(buf, left, &is_set_c))==start_pos);
        BM_ASSERT(bool(is_set) == bool(is_set_c));
    }

    const T* pcurr = buf + start_pos;
    if (right <= *pcurr) // we are in the target gap right now
        bits_counter = unsigned(right - left + 1u) & is_set;
    else
    {
        bits_counter = unsigned(*pcurr - left + 1u) & is_set;
        for (prev_gap = *pcurr++; right > *pcurr; prev_gap = *pcurr++)
            bits_counter += (is_set ^= ~0u) & (*pcurr - prev_gap);
        bits_counter += unsigned(right - prev_gap) & (is_set ^ ~0u);
    }
    return bits_counter;
}


/*!
   \brief Test if all bits are 1 in GAP buffer in the [left, right] range.
   \param buf - GAP buffer pointer.
   \param left - leftmost bit index to start from
   \param right- rightmost bit index
   \return true if all bits are "11111"
   @ingroup gapfunc
*/
template<typename T>
bool gap_is_all_one_range(const T* const BMRESTRICT buf,
                          unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, left, &is_set);
    if (!is_set) // GAP is 0
        return false;
    const T* const pcurr = buf + start_pos;
    return (right <= *pcurr);
}

/*!
   \brief Test if any bits are 1 in GAP buffer in the [left, right] range.
   \param buf - GAP buffer pointer.
   \param left - leftmost bit index to start from
   \param right- rightmost bit index
   \return true if at least 1 "00010"
   @ingroup gapfunc
*/
template<typename T>
bool gap_any_range(const T* const BMRESTRICT buf,
                    unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, left, &is_set);
    const T* const pcurr = buf + start_pos;

    if (!is_set) // start GAP is 0 ...
    {
        if (right <= *pcurr) // ...bit if the interval goes into at least 1 blk
            return false; // .. nope
        return true;
    }
    return true;
}

/*!
   \brief Test if any bits are 1 in GAP buffer in the [left, right] range
   and flanked with 0s
   \param buf - GAP buffer pointer.
   \param left - leftmost bit index to start from
   \param right- rightmost bit index
   \return true if "011110"
   @ingroup gapfunc
*/
template<typename T>
bool gap_is_interval(const T* const BMRESTRICT buf,
                     unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(left > 0); // cannot check left-1 otherwise
    BM_ASSERT(right < bm::gap_max_bits-1); // cannot check right+1 otherwise

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, left, &is_set);

    const T* pcurr = buf + start_pos;
    if (!is_set || (right != *pcurr) || (start_pos <= 1))
        return false;
    --pcurr;
    if (*pcurr != left-1)
        return false;
    return true;
}

/**
    \brief Searches for the last 1 bit in the 111 interval of a GAP block
    \param buf - BIT block buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup gapfunc
*/
template<typename T>
bool gap_find_interval_end(const T* const BMRESTRICT buf,
                           unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(pos);
    BM_ASSERT(nbit < bm::gap_max_bits);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, nbit, &is_set);
    if (!is_set)
        return false;
    *pos = buf[start_pos];
    return true;
}


/**
    \brief Searches for the first 1 bit in the 111 interval of a GAP block
    \param buf - GAP block buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup gapfunc
*/
template<typename T>
bool gap_find_interval_start(const T* const BMRESTRICT buf,
                           unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(pos);
    BM_ASSERT(nbit < bm::gap_max_bits);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, nbit, &is_set);
    if (!is_set)
        return false;
    --start_pos;
    if (!start_pos)
        *pos = 0;
    else
        *pos = buf[start_pos]+1;
    return true;
}

/**
    \brief reverse search for the first 1 bit of a GAP block
    \param buf - GAP block buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup gapfunc
*/
template<typename T>
bool gap_find_prev(const T* const BMRESTRICT buf,
                   unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(pos);
    BM_ASSERT(nbit < bm::gap_max_bits);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, nbit, &is_set);
    if (is_set)
    {
        *pos = nbit;
        return true;
    }
    --start_pos;
    if (!start_pos)
        return false;
    else
        *pos = buf[start_pos];
    return true;
}



/*!
    \brief GAP block find position for the rank

    \param block - bit block buffer pointer
    \param rank - rank to find (must be > 0)
    \param nbit_from - start bit position in block
    \param nbit_pos - found position
 
    \return 0 if position with rank was found, or
              the remaining rank (rank - population count)

    @ingroup gapfunc
*/
template<typename T, typename SIZE_TYPE>
SIZE_TYPE gap_find_rank(const T* const block,
                        SIZE_TYPE   rank,
                        unsigned   nbit_from,
                        unsigned&  nbit_pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(rank);

    const T* pcurr = block;
    const T* pend = pcurr + (*pcurr >> 3);

    unsigned bits_counter = 0;
    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(block, nbit_from, &is_set);
    is_set = ~(is_set - 1u); // 0xFFF.. if true (mask for branchless code)

    pcurr = block + start_pos;
    bits_counter += unsigned(*pcurr - nbit_from + 1u) & is_set;
    if (bits_counter >= rank) // found!
    {
        nbit_pos = nbit_from + unsigned(rank) - 1u;
        return 0;
    }
    rank -= bits_counter;
    unsigned prev_gap = *pcurr++;
    for (is_set ^= ~0u; pcurr <= pend; is_set ^= ~0u)
    {
        bits_counter = (*pcurr - prev_gap) & is_set;
        if (bits_counter >= rank) // found!
        {
            nbit_pos = prev_gap + unsigned(rank);
            return 0;
        }
        rank -= bits_counter;
        prev_gap = *pcurr++;
    } // for
    
    return rank;
}
                       


/*!
    \brief Counts 1 bits in GAP buffer in the closed [0, right] range.
    \param buf - GAP buffer pointer.
    \param right- rightmost bit index
    \param is_corrected - if true the result will be rank corrected
                       if right bit == true count=count-1
    \return Number of non-zero bits
    @ingroup gapfunc
*/
/*
template<typename T>
unsigned gap_bit_count_to(const T* const buf, T right,
                          bool is_corrected=false) BMNOEXCEPT
{
    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);

    unsigned bits_counter = 0;
    unsigned is_set = ~((unsigned(*buf) & 1u) - 1u); // 0xFFF.. if true (mask for branchless code)
    BM_ASSERT(is_set == 0u || is_set == ~0u);
    pcurr = buf + 1;

    if (right <= *pcurr) // we are in the target block right now
    {
        bits_counter = (right + 1u) & is_set; // & is_set == if (is_set)
        bits_counter -= (is_set & unsigned(is_corrected));
        return bits_counter;
    }
    bits_counter += (*pcurr + 1u) & is_set;

    unsigned prev_gap = *pcurr++;
    for (is_set ^= ~0u; right > *pcurr; is_set ^= ~0u)
    {
        bits_counter += (*pcurr - prev_gap) & is_set;
        if (pcurr == pend)
        {
            bits_counter -= (is_set & unsigned(is_corrected));
            return bits_counter;
        }
        prev_gap = *pcurr++;
    }
    bits_counter += (right - prev_gap) & is_set;
    bits_counter -= (is_set & unsigned(is_corrected));
    return bits_counter;
}
*/
template<typename T, bool TCORRECT=false>
unsigned gap_bit_count_to(const T* const buf, T right) BMNOEXCEPT
{
    BM_ASSERT(right < bm::gap_max_bits);

    unsigned bits_counter, prev_gap;

    unsigned is_set = ~((unsigned(*buf) & 1u) - 1u); // 0xFFF.. if true (mask for branchless code)
    const T* pcurr = buf + 1;
    if (right <= *pcurr) // we are in the target block right now
    {
        bits_counter = (right + 1u) & is_set; // & is_set == if (is_set)
    }
    else
    {
        bits_counter = (*pcurr + 1u) & is_set;
        prev_gap = *pcurr++;
        for (is_set ^= ~0u; right > *pcurr; is_set ^= ~0u, prev_gap = *pcurr++)
        {
            bits_counter += (*pcurr - prev_gap) & is_set;
            if (*pcurr == bm::gap_max_bits-1)
                goto cret;
        }
        bits_counter += (right - prev_gap) & is_set;
    }

    cret:
    if constexpr (TCORRECT)
        bits_counter -= (is_set & unsigned(TCORRECT));
    return bits_counter;
}



/*!
    D-GAP block for_each algorithm
    
    D-Gap Functor is called for each element but last one.
    
   \param gap_buf - GAP buffer 
   \param func - functor object
    
*/
template<class T, class Func> 
void for_each_dgap(const T* gap_buf, Func& func)
{
    const T* pcurr = gap_buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;
    
    T prev = *pcurr;
    func((T)(prev + 1)); // first element incremented to avoid 0
    ++pcurr;
    do
    {
        func((T)(*pcurr - prev)); // all others are [N] - [N-1]
        prev = *pcurr;
    } while (++pcurr < pend);
}

/** d-Gap copy functor
    @internal
*/
template<typename T> struct d_copy_func
{
    d_copy_func(T* dg_buf) : dgap_buf_(dg_buf) {}
    void operator()(T dgap) { *dgap_buf_++ = dgap; }

    T* dgap_buf_;
};

/*! 
   \brief Convert GAP buffer into D-GAP buffer
   
   Delta GAP representation is DGAP[N] = GAP[N] - GAP[N-1]    
   
   \param gap_buf - GAP buffer 
   \param dgap_buf - Delta-GAP buffer
   \param copy_head - flag to copy GAP header
   
   \internal
   
   @ingroup gapfunc
*/
template<typename T>
T* gap_2_dgap(const T* BMRESTRICT gap_buf,
              T* BMRESTRICT dgap_buf, bool copy_head=true) BMNOEXCEPT
{
    if (copy_head) // copy GAP header
    {
        *dgap_buf++ = *gap_buf;
    }

    d_copy_func<T> copy_func(dgap_buf);
    for_each_dgap<T, d_copy_func<T> >(gap_buf, copy_func);
    return copy_func.dgap_buf_;
}

/*! 
   \brief Convert D-GAP buffer into GAP buffer
   
   GAP representation is GAP[N] = DGAP[N] + DGAP[N-1]    
   
   \param dgap_buf - Delta-GAP buffer
   \param gap_header - GAP header word
   \param gap_buf  - GAP buffer

   \internal
   @ingroup gapfunc
*/
template<typename T>
void dgap_2_gap(const T* BMRESTRICT dgap_buf,
                T* BMRESTRICT gap_buf, T gap_header=0) BMNOEXCEPT
{
    const T* pcurr = dgap_buf;
    unsigned len;    
    if (!gap_header) // GAP header is already part of the stream
    {
        len = *pcurr >> 3;
        *gap_buf++ = *pcurr++; // copy GAP header
    }
    else // GAP header passed as a parameter
    {
        len = gap_header >> 3;
        *gap_buf++ = gap_header; // assign GAP header
    }    
    --len; // last element is actually not encoded
    const T* pend = pcurr + len;

    *gap_buf = *pcurr++; // copy first element
    if (*gap_buf == 0) 
        *gap_buf = 65535; // fix +1 overflow
    else
        *gap_buf = T(*gap_buf - 1);
    
    for (++gap_buf; pcurr < pend; ++pcurr)
    {
        T prev = *(gap_buf-1); // don't remove temp(undef expression!)           
        *gap_buf++ = T(*pcurr + prev);
    }
    *gap_buf = 65535; // add missing last element  
}


/*! 
   \brief Lexicographical comparison of GAP buffers.
   \param buf1 - First GAP buffer pointer.
   \param buf2 - Second GAP buffer pointer.
   \return  <0 - less, =0 - equal,  >0 - greater.

   @ingroup gapfunc
*/
template<typename T>
int gapcmp(const T* buf1, const T* buf2) BMNOEXCEPT
{
    const T* pcurr1 = buf1;
    const T* pend1 = pcurr1 + (*pcurr1 >> 3);
    unsigned bitval1 = *buf1 & 1;
    ++pcurr1;

    const T* pcurr2 = buf2;
    unsigned bitval2 = *buf2 & 1;
    ++pcurr2;

    while (pcurr1 <= pend1)
    {
        if (*pcurr1 == *pcurr2)
        {
            if (bitval1 != bitval2)
            {
                return (bitval1) ? 1 : -1;
            }
        }
        else
        {
            if (bitval1 == bitval2)
            {
                if (bitval1)
                {
                    return (*pcurr1 < *pcurr2) ? -1 : 1;
                }
                else
                {
                    return (*pcurr1 < *pcurr2) ? 1 : -1;
                }
            }
            else
            {
                return (bitval1) ? 1 : -1;
            }
        }
        ++pcurr1; ++pcurr2;
        bitval1 ^= 1;
        bitval2 ^= 1;
    }

    return 0;
}

/*!
   \brief Find first bit which is different between two GAP-blocks
   \param buf1 - block 1
   \param buf2 - block 2
   \param pos - out - position of difference (undefined if blocks are equal)
   \return  true if difference was found

   @ingroup gapfunc
*/
template<typename T>
bool gap_find_first_diff(const T* BMRESTRICT buf1,
                         const T* BMRESTRICT buf2,
                         unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(buf1 && buf2 && pos);

    const T* pcurr1 = buf1;
    const T* pend1 = pcurr1 + (*pcurr1 >> 3);
    const T* pcurr2 = buf2;
    for (++pcurr1, ++pcurr2; pcurr1 <= pend1; ++pcurr1, ++pcurr2)
    {
        if (*pcurr1 != *pcurr2)
        {
            *pos = 1 + ((*pcurr1 < *pcurr2) ? *pcurr1 : *pcurr2);
            return true;
        }
    } // for
    return false;
}

// -------------------------------------------------------------------------
//

/*!
   \brief Abstract operation for GAP buffers. 
          Receives functor F as a template argument
   \param dest - destination memory buffer.
   \param vect1 - operand 1 GAP encoded buffer.
   \param vect1_mask - XOR mask for starting bitflag for vector1 
   can be 0 or 1 (1 inverts the vector)
   \param vect2 - operand 2 GAP encoded buffer.
   \param vect2_mask - same as vect1_mask
   \param dlen - destination length after the operation

   \note Internal function.
   @internal

   @ingroup gapfunc
*/
template<typename T, class F> 
void gap_buff_op(T*         BMRESTRICT dest, 
                 const T*   BMRESTRICT vect1,
                 unsigned   vect1_mask, 
                 const T*   BMRESTRICT vect2,
                 unsigned   vect2_mask, 
                 unsigned&  dlen) BMNOEXCEPT2
{
    const T*  cur1 = vect1;
    const T*  cur2 = vect2;

    T bitval1 = (T)((*cur1++ & 1) ^ vect1_mask);
    T bitval2 = (T)((*cur2++ & 1) ^ vect2_mask);
    
    T bitval = (T) F::op(bitval1, bitval2);
    T bitval_prev = bitval;

    T* res = dest;
    *res = bitval;
    ++res;

    T c1 = *cur1; T c2 = *cur2;
    while (1)
    {
        bitval = (T) F::op(bitval1, bitval2);

        // Check if GAP value changes and we need to 
        // start the next one
        //
        res += (bitval != bitval_prev);
        bitval_prev = bitval;
        if (c1 < c2) // (*cur1 < *cur2)
        {
            *res = c1;
            ++cur1; c1 = *cur1;
            bitval1 ^= 1;
        }
        else // >=
        {
            *res = c2;
            if (c2 < c1) // (*cur2 < *cur1)
            {
                bitval2 ^= 1;
            }
            else  // equal
            {
                if (c2 == (bm::gap_max_bits - 1))
                    break;

                ++cur1; c1 = *cur1;
                bitval1 ^= 1; bitval2 ^= 1;
            }
            ++cur2; c2 = *cur2;
        }
    } // while

    dlen = (unsigned)(res - dest);
    *dest = (T)((*dest & 7) + (dlen << 3));
}



/*!
   \brief Abstract distance test operation for GAP buffers. 
          Receives functor F as a template argument
   \param vect1 - operand 1 GAP encoded buffer.
   \param vect1_mask - XOR mask for starting bitflag for vector1 
                       can be 0 or 1 (1 inverts the vector)
   \param vect2 - operand 2 GAP encoded buffer.
   \param vect2_mask - same as vect1_mask
   \note Internal function.
   \return non zero value if operation result returns any 1 bit 

   @ingroup gapfunc
*/
template<typename T, class F> 
unsigned gap_buff_any_op(const T*   BMRESTRICT vect1,
                         unsigned              vect1_mask, 
                         const T*   BMRESTRICT vect2,
                         unsigned              vect2_mask) BMNOEXCEPT2
{
    const T*  cur1 = vect1;
    const T*  cur2 = vect2;

    unsigned bitval1 = (*cur1++ & 1) ^ vect1_mask;
    unsigned bitval2 = (*cur2++ & 1) ^ vect2_mask;
    
    unsigned bitval = F::op(bitval1, bitval2);
    if (bitval)
        return bitval;
    unsigned bitval_prev = bitval;

    while (1)
    {
        bitval = F::op(bitval1, bitval2);
        if (bitval)
            return bitval;

        if (bitval != bitval_prev)
            bitval_prev = bitval;

        if (*cur1 < *cur2)
        {
            ++cur1;
            bitval1 ^= 1;
        }
        else // >=
        {
            if (*cur2 < *cur1)
            {
                bitval2 ^= 1;                
            }
            else  // equal
            {
                if (*cur2 == (bm::gap_max_bits - 1))
                {
                    break;
                }
                ++cur1;
                bitval1 ^= 1; bitval2 ^= 1;
            }
            ++cur2;
        }

    } // while

    return 0;
}



/*!
   \brief Abstract distance(similarity) operation for GAP buffers. 
          Receives functor F as a template argument
   \param vect1 - operand 1 GAP encoded buffer.
   \param vect2 - operand 2 GAP encoded buffer.
   \note Internal function.

   @ingroup gapfunc
*/
template<typename T, class F> 
unsigned gap_buff_count_op(const T*  vect1, const T*  vect2) BMNOEXCEPT2
{
    unsigned count;// = 0;
    const T* cur1 = vect1;
    const T* cur2 = vect2;

    unsigned bitval1 = (*cur1++ & 1);
    unsigned bitval2 = (*cur2++ & 1);
    unsigned bitval = count = F::op(bitval1, bitval2);
    unsigned bitval_prev = bitval;

    T res, res_prev;
    res = res_prev = 0;

    while (1)
    {
        bitval = F::op(bitval1, bitval2);
        // Check if GAP value changes and we need to 
        // start the next one.
        if (bitval != bitval_prev)
        {
            bitval_prev = bitval;
            res_prev = res;
        }

        if (*cur1 < *cur2)
        {
            res = *cur1;
            if (bitval)
            {
                count += res - res_prev; 
                res_prev = res;
            }
            ++cur1; bitval1 ^= 1;
        }
        else // >=
        {
            res = *cur2;
            if (bitval)
            {
                count += res - res_prev; 
                res_prev = res;
            }
            if (*cur2 < *cur1)
            {
                bitval2 ^= 1;                
            }
            else  // equal
            {
                if (*cur2 == (bm::gap_max_bits - 1))
                    break;

                ++cur1;
                bitval1 ^= 1; bitval2 ^= 1;
            }
            ++cur2;
        }

    } // while

    return count;
}


#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

/*!
   \brief Sets or clears bit in the GAP buffer.

   \param val - new bit value
   \param buf - GAP buffer.
   \param pos - Index of bit to set.
   \param is_set - (OUT) flag if bit was actually set.

   \return New GAP buffer length. 

   @ingroup gapfunc
*/
template<typename T>
unsigned gap_set_value(unsigned val,
                       T* BMRESTRICT buf,
                       unsigned pos,
                       unsigned* BMRESTRICT is_set) BMNOEXCEPT
{
    BM_ASSERT(pos < bm::gap_max_bits);

    unsigned curr = bm::gap_bfind(buf, pos, is_set);
    T end = (T)(*buf >> 3);
    if (*is_set == val)
    {
        *is_set = 0;
        return end;
    }
    *is_set = 1;

    T* pcurr = buf + curr;
    T* pprev = pcurr - 1;
    T* pend = buf + end;

    // Special case, first bit GAP operation. There is no platform beside it.
    // initial flag must be inverted.
    if (!pos)
    {
        *buf ^= 1;
        if (buf[1]) // We need to insert a 1 bit GAP here
        {
            ::memmove(&buf[2], &buf[1], (end - 1) * sizeof(gap_word_t));
            buf[1] = 0;
            ++end;
        }
        else // Only 1 bit in the GAP. We need to delete the first GAP.
        {
            pprev = buf + 1; pcurr = pprev + 1;
            goto copy_gaps;
        }
    }
    else
    if (curr > 1 && ((unsigned)(*pprev))+1 == pos) // Left border bit
    {
       ++(*pprev);
       if (*pprev == *pcurr)  // Curr. GAP to be merged with prev.GAP.
       {
            --end;
            if (pcurr != pend) // GAP merge: 2 GAPS to be deleted
            {
                ++pcurr;
                copy_gaps:
                --end;
                do { *pprev++ = *pcurr++; } while (pcurr < pend);
            }
       }    
    }
    else
    if (*pcurr == pos) // Rightmost bit in the GAP. Border goes left.
    {
        --(*pcurr);
        end += (pcurr == pend);
    }
    else  // Worst case: split current GAP
    {
        if (*pcurr != bm::gap_max_bits-1) // last gap does not need memmove
            ::memmove(pcurr+2, pcurr, (end - curr + 1)*(sizeof(T)));
        end += 2;
        pcurr[0] = (T)(pos-1);
        pcurr[1] = (T)pos;
    }

    // Set correct length word and last border word
    *buf = (T)((*buf & 7) + (end << 3));
    buf[end] = bm::gap_max_bits-1;
    return end;
}





/*!
   \brief Sets or clears bit in the GAP buffer.

   \param val - new bit value
   \param buf - GAP buffer.
   \param pos - Index of bit to set.

   \return New GAP buffer length.

   @ingroup gapfunc
*/
template<typename T>
unsigned gap_set_value(unsigned val,
                       T* BMRESTRICT buf,
                       unsigned pos) BMNOEXCEPT
{
    BM_ASSERT(pos < bm::gap_max_bits);
    unsigned is_set;
    unsigned curr = bm::gap_bfind(buf, pos, &is_set);
    T end = (T)(*buf >> 3);
    if (is_set == val)
        return end;

    T* pcurr = buf + curr;
    T* pprev = pcurr - 1;
    T* pend = buf + end;

    // Special case, first bit GAP operation. There is no platform beside it.
    // initial flag must be inverted.
    if (!pos)
    {
        *buf ^= 1;
        if (buf[1]) // We need to insert a 1 bit GAP here
        {
            ::memmove(&buf[2], &buf[1], (end - 1) * sizeof(gap_word_t));
            buf[1] = 0;
            ++end;
        }
        else // Only 1 bit in the GAP. We need to delete the first GAP.
        {
            pprev = buf + 1; pcurr = pprev + 1;
            goto copy_gaps;
        }
    }
    else
    if (curr > 1 && ((unsigned)(*pprev))+1 == pos) // Left border bit
    {
       ++(*pprev);
       if (*pprev == *pcurr)  // Curr. GAP to be merged with prev.GAP.
       {
            --end;
            if (pcurr != pend) // GAP merge: 2 GAPS to be deleted
            {
                ++pcurr;
                copy_gaps:
                --end;
                do { *pprev++ = *pcurr++; } while (pcurr < pend);
            }
       }
    }
    else
    if (*pcurr == pos) // Rightmost bit in the GAP. Border goes left.
    {
        --(*pcurr);
        end += (pcurr == pend);
    }
    else  // Worst case: split current GAP
    {
        if (*pcurr != bm::gap_max_bits-1) // last gap does not need memmove
            ::memmove(pcurr+2, pcurr, (end - curr + 1)*(sizeof(T)));
        end += 2;
        pcurr[0] = (T)(pos-1);
        pcurr[1] = (T)pos;
    }

    // Set correct length word and last border word
    *buf = (T)((*buf & 7) + (end << 3));
    buf[end] = bm::gap_max_bits-1;
    return end;
}

/*!
   \brief Add new value to the end of GAP buffer.

   \param buf - GAP buffer.
   \param pos - Index of bit to set.

   \return New GAP buffer length. 

   @ingroup gapfunc
*/
template<typename T> 
unsigned gap_add_value(T* buf, unsigned pos) BMNOEXCEPT
{
    BM_ASSERT(pos < bm::gap_max_bits);

    T end = (T)(*buf >> 3);
    T curr = end;
    T* pcurr = buf + end;
    T* pend  = pcurr;
    T* pprev = pcurr - 1;

    // Special case, first bit GAP operation. There is no platform beside it.
    // initial flag must be inverted.
    if (!pos)
    {
        *buf ^= 1;
        if ( buf[1] ) // We need to insert a 1 bit platform here.
        {
            ::memmove(&buf[2], &buf[1], (end - 1) * sizeof(gap_word_t));
            buf[1] = 0;
            ++end;
        }
        else // Only 1 bit in the GAP. We need to delete the first GAP.
        {
            pprev = buf + 1; pcurr = pprev + 1;
            --end;
            do { *pprev++ = *pcurr++; } while (pcurr < pend);
        }
    }
    else if (((unsigned)(*pprev))+1 == pos && (curr > 1) ) // Left border bit
    {
       ++(*pprev);
       if (*pprev == *pcurr)  // Curr. GAP to be merged with prev.GAP.
       {
            --end;
            BM_ASSERT(pcurr == pend);
       }
    }
    else if (*pcurr == pos) // Rightmost bit in the GAP. Border goes left.
    {
        --(*pcurr);       
        end += (pcurr == pend);
    }
    else  // Worst case we need to split current block.
    {
        pcurr[0] = (T)(pos-1);
        pcurr[1] = (T)pos;
        end = (T)(end+2);
    }

    // Set correct length word.
    *buf = (T)((*buf & 7) + (end << 3));
    buf[end] = bm::gap_max_bits - 1;
    return end;
}

#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif


/*!
    @brief Right shift GAP block by 1 bit
    @param buf - block pointer
    @param co_flag - carry over from the previous block
    @param new_len - output length of the GAP block after the operation
 
    @return carry over bit (1 or 0)
    @ingroup gapfunc
*/
template<typename T>
bool gap_shift_r1(T* BMRESTRICT buf,
                  unsigned co_flag, unsigned* BMRESTRICT new_len) BMNOEXCEPT
{
    BM_ASSERT(new_len);
    BM_ASSERT(co_flag <= 1);

    bool co, gap_set_flag;
    unsigned len = (*buf >> 3);
    // 1: increment all GAP values by 1
    {
        unsigned bitval = *buf & 1;
        gap_set_flag = (bitval != co_flag);
        if (buf[1] == bm::gap_max_bits-1) // full GAP block
            co = bitval;
        else
        {
            unsigned i = 1;
            for (; i < len; ++i)
            {
                buf[i]++;
                bitval ^= 1;
            } // for i
            BM_ASSERT(buf[i] == bm::gap_max_bits-1);
            if (buf[i-1] == bm::gap_max_bits-1) // last element shifts out
            {
                // Set correct length word
                --len;
                *buf = (T)((*buf & 7) + (len << 3));
            }
            co = bitval;
        }
    }
    // set bit bit 0 with carry-in flag
    unsigned is_set;
    if (gap_set_flag)
        *new_len = bm::gap_set_value(co_flag, buf, 0, &is_set);
    else
        *new_len = len;

    return co;
}

/*!
    @brief isnert bit into GAP compressed block
    @param buf - block pointer
    @param pos - insert position
    @param value - (0 or 1) - value to set
    @param new_len - output length of the GAP block after the operation

    @return carry over bit (1 or 0)
    @ingroup gapfunc
*/
template<typename T>
bool gap_insert(T* BMRESTRICT buf,
                unsigned pos, unsigned val, unsigned* BMRESTRICT new_len) BMNOEXCEPT
{
    BM_ASSERT(new_len);
    BM_ASSERT(val <= 1);

    bool co, gap_set_flag;
    unsigned is_set;
    unsigned idx = bm::gap_bfind(buf, pos, &is_set);
    BM_ASSERT(is_set <= 1);

    gap_set_flag = (val != is_set);
    unsigned len = (*buf >> 3);

    // 1: increment all GAP values by 1
    if (buf[idx] == bm::gap_max_bits-1)
    {
        co = is_set;
    }
    else
    {
        unsigned i = idx;
        for (; i < len; ++i)
        {
            buf[i]++;
            is_set ^= 1;
        } // for i
        BM_ASSERT(buf[i] == bm::gap_max_bits-1);
        if (buf[i-1] == bm::gap_max_bits-1) // last element shifts out
        {
            // Set correct length word
            --len;
            *buf = (T)((*buf & 7) + (len << 3));
            *new_len = len;
        }
        co = is_set;
    }

    if (gap_set_flag)
        *new_len = bm::gap_set_value(val, buf, pos, &is_set);
    else
        *new_len = len;

    return co;
}

/*!
    @brief Left shift GAP block by 1 bit
    @param buf - block pointer
    @param co_flag - carry over from the previous block
    @param new_len - new length of the block

    @return carry over bit (1 or 0)
    @ingroup gapfunc
*/
template<typename T>
bool gap_shift_l1(T* BMRESTRICT buf,
                  unsigned co_flag, unsigned* BMRESTRICT new_len) BMNOEXCEPT
{
    BM_ASSERT(new_len);
    BM_ASSERT(co_flag <= 1);
    
    unsigned is_set;

    // 1: decrement all GAP values by 1
    //
    unsigned bitval = *buf & 1;
    bool co0 = bitval;

    if (!buf[1]) // cannot decrement (corner case)
    {
        bitval ^= 1;
        *new_len = bm::gap_set_value(bitval, buf, 0, &is_set);
        
        BM_ASSERT(is_set);
        BM_ASSERT(buf[1]);
        BM_ASSERT(bitval == unsigned(*buf & 1u));
        
        if (*new_len == 1)
        {
            *new_len = bm::gap_set_value(co_flag, buf,
                                         bm::gap_max_bits-1, &is_set);
            return co0;
        }
    }
    if (buf[1] != bm::gap_max_bits-1) // full GAP block
    {
        BM_ASSERT(buf[1]);
        unsigned len = (*buf >> 3);
        unsigned i = 1;
        for (; i < len; ++i)
        {
            buf[i]--;
            bitval ^= 1;
        } // for i
        BM_ASSERT(buf[i] == bm::gap_max_bits-1);
    }
    // 2: set last bit position with carry-in flag
    //
    *new_len = bm::gap_set_value(co_flag, buf, bm::gap_max_bits-1, &is_set);
    return co0;
}


/*!
   \brief Convert array to GAP buffer.

   \param buf - GAP buffer.
   \param arr - array of values to set
   \param len - length of the array

   \return New GAP buffer length. 

   @ingroup gapfunc
*/

template<typename T> 
unsigned gap_set_array(T* buf, const T* arr, unsigned len) BMNOEXCEPT
{
    *buf = (T)((*buf & 6u) + (1u << 3)); // gap header setup

    T* pcurr = buf + 1;

    unsigned i = 0;
    T curr = arr[i];
    if (curr != 0) // need to add the first gap: (0 to arr[0]-1)
    {
        *pcurr = (T)(curr - 1);
        ++pcurr;
    }
    else
    {
        ++(*buf); // GAP starts with 1
    }
    T prev = curr; 
    T acc = prev;

    for (i = 1; i < len; ++i)
    {
        curr = arr[i];
        if (curr == prev + 1)
        {
            ++acc;
            prev = curr;
        }
        else
        {
            *pcurr++ = acc;
            acc = curr;
            *pcurr++ = (T)(curr-1);
        }
        prev = curr;
    }
    *pcurr = acc;
    if (acc != bm::gap_max_bits - 1)
    {
        ++pcurr;
        *pcurr = bm::gap_max_bits - 1;
    }

    unsigned gap_len = unsigned(pcurr - buf);
    BM_ASSERT(gap_len == ((gap_len << 3) >> 3));

    *buf = (T)((*buf & 7) + (gap_len << 3));
    return gap_len+1;
}


//------------------------------------------------------------------------

/**
    \brief Compute number of GAPs in bit-array
    \param arr - array of BITs
    \param len - array length

    @ingroup gapfunc
*/
template<typename T> 
unsigned bit_array_compute_gaps(const T* arr, unsigned len) BMNOEXCEPT
{
    unsigned gap_count = 1;
    T prev = arr[0];
    if (prev > 0)
        ++gap_count;
    for (unsigned i = 1; i < len; ++i)
    {
        T curr = arr[i];
        if (curr != prev + 1)
        {
            gap_count += 2;
        }
        prev = curr;
    }
    return gap_count;
}


//------------------------------------------------------------------------

/**
    \brief Searches for the next 1 bit in the GAP block
    \param buf - GAP buffer
    \param nbit - bit index to start checking from.
    \param prev - returns previously checked value
 
    \return 0 if not found

    @ingroup gapfunc
*/
template<typename T>
unsigned gap_block_find(const T* BMRESTRICT buf,
                        unsigned nbit,
                        bm::id_t* BMRESTRICT prev) BMNOEXCEPT
{
    BM_ASSERT(nbit < bm::gap_max_bits);

    unsigned bitval;
    unsigned gap_idx = bm::gap_bfind(buf, nbit, &bitval);

    if (bitval) // positive block.
    {
       *prev = nbit;
       return 1u;
    }
    unsigned val = buf[gap_idx] + 1;
    *prev = val;
    return (val != bm::gap_max_bits);  // no bug here.
}

//------------------------------------------------------------------------


/*! 
    \brief Set 1 bit in a block
    @ingroup bitfunc
*/
BMFORCEINLINE
void set_bit(unsigned* dest, unsigned  bitpos) BMNOEXCEPT
{
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask); 
    unsigned nword = unsigned(nbit >> bm::set_word_shift); 
    nbit &= bm::set_word_mask;
    dest[nword] |= unsigned(1u << nbit);
}

/*!
    \brief Set 1 bit in a block
    @ingroup bitfunc
*/
BMFORCEINLINE
void clear_bit(unsigned* dest, unsigned  bitpos) BMNOEXCEPT
{
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask);
    unsigned nword = unsigned(nbit >> bm::set_word_shift);
    nbit &= bm::set_word_mask;
    dest[nword] &= ~(unsigned(1u << nbit));
}

/*! 
    \brief Test 1 bit in a block
    
    @ingroup bitfunc
*/
BMFORCEINLINE
unsigned test_bit(const unsigned* block, unsigned  bitpos) BMNOEXCEPT
{
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask); 
    unsigned nword = unsigned(nbit >> bm::set_word_shift); 
    nbit &= bm::set_word_mask;
    return (block[nword] >> nbit) & 1u;
}


/*! 
   \brief Sets bits to 1 in the bitblock.
   \param dest - Bitset buffer.
   \param bitpos - Offset of the start bit.
   \param bitcount - number of bits to set.

   @ingroup bitfunc
*/
inline
void or_bit_block(unsigned* dest, unsigned bitpos, unsigned bitcount) BMNOEXCEPT
{
    dest += unsigned(bitpos >> bm::set_word_shift); // nword
    bitpos &= bm::set_word_mask;

    if (bitcount == 1u)  // special case (only 1 bit to set)
    {
        *dest |= (1u << bitpos);
        return;
    }

     const unsigned maskFF = ~0u;
   if (bitpos) // starting pos is not aligned
    {
        unsigned mask_r = maskFF << bitpos;
        if (unsigned right_margin = bitpos + bitcount; right_margin < 32)
        {
            *dest |= (maskFF >> (32 - right_margin)) & mask_r;
            return;
        }
        *dest++ |= mask_r;
        bitcount -= 32 - bitpos;
    }
    for ( ;bitcount >= 64; bitcount-=64, dest+=2)
        dest[0] = dest[1] = maskFF;
    if (bitcount >= 32)
    {
        *dest++ = maskFF; bitcount -= 32;
    }
    if (bitcount)
    {
        *dest |= maskFF >> (32 - bitcount);
    }
}


/*! 
   \brief SUB (AND NOT) bit interval to 1 in the bitblock.
   \param dest - Bitset buffer.
   \param bitpos - Offset of the start bit.
   \param bitcount - number of bits to set.

   @ingroup bitfunc
*/
inline
void sub_bit_block(unsigned* dest, unsigned bitpos, unsigned bitcount) BMNOEXCEPT
{
    BM_ASSERT(bitcount);

    dest += unsigned(bitpos >> bm::set_word_shift); // nword
    bitpos &= bm::set_word_mask;
    if (bitcount == 1u)  // special case (only 1 bit to set)
    {
        *dest &= ~(bitcount << bitpos);
        return;
    }
    const unsigned maskFF = ~0u;
    if (bitpos) // starting pos is not aligned
    {
        unsigned mask_r = maskFF << bitpos;
        if (unsigned right_margin = bitpos + bitcount; right_margin < 32)
        {
            *dest &= ~((maskFF >> (32 - right_margin)) & mask_r);
            return;
        }
        *dest++ &= ~mask_r;
        bitcount -= 32 - bitpos;
    }
    for ( ;bitcount >= 64; bitcount-=64, dest+=2)
        dest[0] = dest[1] = 0u;
    if (bitcount >= 32)
    {
        *dest++ = 0u; bitcount -= 32;
    }
    if (bitcount)
        *dest &= ~(maskFF >> (32 - bitcount));
}



/*! 
   \brief XOR bit interval to 1 in the bitblock.
   \param dest - Bitset buffer.
   \param bitpos - Offset of the start bit.
   \param bitcount - number of bits to set.

   @ingroup bitfunc
*/  
inline void xor_bit_block(unsigned* dest, 
                          unsigned bitpos, 
                          unsigned bitcount) BMNOEXCEPT
{
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask); 
    unsigned nword = unsigned(nbit >> bm::set_word_shift); 
    nbit &= bm::set_word_mask;

    bm::word_t* word = dest + nword;

    if (bitcount == 1)  // special case (only 1 bit to set)
    {
        *word ^= unsigned(1 << nbit);
        return;                             
    }

    if (nbit) // starting position is not aligned
    {
        unsigned right_margin = nbit + bitcount;

        // here we checking if we setting bits only in the current
        // word. Example: 00111000000000000000000000000000 (32 bits word)

        if (right_margin < 32) 
        {
            unsigned mask_r = bm::mask_r_u32(nbit);
            unsigned mask_l = bm::mask_l_u32(right_margin-1);
            unsigned mask = mask_r & mask_l;
            *word ^= mask;
            return;
        }
        *word ^= bm::mask_r_u32(nbit);
        bitcount -= 32 - nbit;
        ++word;
    }
    for ( ;bitcount >= 64; bitcount-=64, word+=2)
    {
        word[0] ^= ~0u; word[1] ^= ~0u;
    }
    if (bitcount >= 32)
    {
        *word++ ^= ~0u; bitcount -= 32;
    }
    if (bitcount)
    {
        *word ^= bm::mask_l_u32(bitcount-1);
    }
}


/*!
   \brief SUB (AND NOT) GAP block to bitblock.
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.

   @ingroup gapfunc
*/
template<typename T> 
void gap_sub_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);
    
    const T* pend = pcurr + (*pcurr >> 3);
    if (*pcurr & 1)  // Starts with 1
    {
        bm::sub_bit_block(dest, 0, 1 + pcurr[1]);
        ++pcurr;
    }
    for (pcurr += 2; pcurr <= pend; pcurr += 2)
    {
        BM_ASSERT(*pcurr > pcurr[-1]);
        bm::sub_bit_block(dest, 1 + pcurr[-1], *pcurr - pcurr[-1]);
    }
}


/*!
   \brief SUB (AND NOT) GAP block to bitblock with digest assist

   \param dest    - bitblock buffer pointer.
   \param pcurr   - GAP buffer pointer.
   \param digest0 - digest of 0 strides inside bit block

   @return new digest

   @ingroup gapfunc
*/
template<typename T>
bm::id64_t gap_sub_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr, bm::id64_t digest0) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);
    
    const T* BMRESTRICT pbuf = pcurr;
    const unsigned len = (*pcurr >> 3);
    const T* BMRESTRICT pend = pcurr + len;
    if (*pcurr & 1)  // Starts with 1
    {
        bool all_zero = bm::check_zero_digest(digest0, 0, pcurr[1]);
        if (!all_zero)
            bm::sub_bit_block(dest, 0, pcurr[1] + 1); // (not AND) - SUB [0] gaps
        pcurr += 3;
    }
    else
        pcurr += 2;

    // wind forward to digest start
    {
        unsigned tz = bm::count_trailing_zeros_u64(digest0);
        unsigned start_pos = tz << set_block_digest_pos_shift;
        if (len > 16)
        {
            unsigned is_set;
            unsigned found_pos = bm::gap_bfind(pbuf, start_pos, &is_set);
            if (found_pos > 2)
            {
                found_pos += !is_set; // to GAP "1" (can go out of scope)
                pcurr = pbuf + found_pos;
            }
            BM_ASSERT (pcurr > pend || *pcurr >= start_pos);
        }
        else
        {
            for (; pcurr <= pend; pcurr += 2) // now we are in GAP "1"
                if (*pcurr >= start_pos)
                    break;
        }
    }

    const unsigned lz = bm::count_leading_zeros_u64(digest0);
    unsigned stop_pos = (64u - lz) << set_block_digest_pos_shift;

    for (T prev; pcurr <= pend; pcurr += 2) // now we are in GAP "1" again
    {
        BM_ASSERT(*pcurr > *(pcurr-1));
        prev = pcurr[-1];
        unsigned pos = 1u + prev;
        bool all_zero = bm::check_zero_digest(digest0, prev, *pcurr);
        if (!all_zero)
            bm::sub_bit_block(dest, pos, *pcurr - prev);
        if (pos > stop_pos)
            break; // early break is possible based on digest tail
    } // for
    return bm::update_block_digest0(dest, digest0);
}



/*!
   \brief XOR GAP block to bitblock.
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.

   @ingroup gapfunc
*/
template<typename T> 
void gap_xor_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);

    const T* pend = pcurr + (*pcurr >> 3);
    if (*pcurr & 1)  // Starts with 1
    {
        bm::xor_bit_block(dest, 0, 1 + pcurr[1]);
        ++pcurr;
    }
    for (pcurr += 2; pcurr <= pend; pcurr += 2)
    {
        BM_ASSERT(*pcurr > pcurr[-1]);
        bm::xor_bit_block(dest, 1 + pcurr[-1], *pcurr - pcurr[-1]);
    }
}


/*!
   \brief Adds(OR) GAP block to bitblock.
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.
   \param len - gap length

   @ingroup gapfunc
*/
template<typename T>
void gap_add_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr, unsigned len) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);
    
    const T* pend = pcurr + len;
    BM_ASSERT(*pend == 65535);
    if (*pcurr & 1)  // Starts with 1
    {
        bm::or_bit_block(dest, 0, 1 + pcurr[1]);
        pcurr += 3;
    }
    else
        pcurr += 2;

    unsigned bc, pos;
    for (; pcurr <= pend; )
    {
        BM_ASSERT(*pcurr > pcurr[-1]);
        pos = 1u + pcurr[-1];
        bc = *pcurr - pcurr[-1];
        pcurr += 2;
        bm::or_bit_block(dest, pos, bc);
    }
}


/*!
   \brief Adds(OR) GAP block to bitblock.
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.

   @ingroup gapfunc
*/
template<typename T>
void gap_add_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    unsigned len = (*pcurr >> 3);
    gap_add_to_bitset(dest, pcurr, len);
}


/*!
   \brief ANDs GAP block to bitblock.
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.

   @ingroup gapfunc
*/
template<typename T> 
void gap_and_to_bitset(unsigned* BMRESTRICT dest,
                       const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);

    const T* pend = pcurr + (*pcurr >> 3);
    if (!(*pcurr & 1) )  // Starts with 0
    {
        bm::sub_bit_block(dest, 0, pcurr[1] + 1); // (not AND) - SUB [0] gaps
        pcurr += 3;
    }
    else
        pcurr += 2;
    
    unsigned bc, pos;
    for (; pcurr <= pend; ) // now we are in GAP "0" again
    {
        BM_ASSERT(*pcurr > *(pcurr-1));
        pos = 1u + pcurr[-1];
        bc = *pcurr - pcurr[-1];
        pcurr += 2;
        bm::sub_bit_block(dest, pos, bc);
    }
}


/*!
   \brief ANDs GAP block to bitblock with digest assist
   \param dest - bitblock buffer pointer.
   \param pcurr  - GAP buffer pointer.
   \param digest0 - digest of 0 strides for the destination

   @return new digest

   @ingroup gapfunc
*/
template<typename T>
bm::id64_t gap_and_to_bitset(unsigned* BMRESTRICT dest,
                    const T* BMRESTRICT pcurr, bm::id64_t digest0) BMNOEXCEPT
{
    BM_ASSERT(dest && pcurr);
    if (!digest0)
        return digest0;
    const T* BMRESTRICT pbuf = pcurr;
    const unsigned len = (*pcurr >> 3);
    const T* BMRESTRICT pend = pcurr + len;
    if (!(*pcurr & 1) )  // Starts with 0
    {
        bool all_zero = bm::check_zero_digest(digest0, 0, pcurr[1]);
        if (!all_zero)
            bm::sub_bit_block(dest, 0, pcurr[1] + 1); // (not AND) - SUB [0] gaps
        pcurr += 3;
    }
    else
        pcurr += 2;

    // wind forward to digest start
    {
        unsigned tz = bm::count_trailing_zeros_u64(digest0);
        unsigned start_pos = tz << set_block_digest_pos_shift;
        if (len > 16)
        {
            unsigned is_set;
            unsigned found_pos = bm::gap_bfind(pbuf, start_pos, &is_set);
            if (found_pos > 2)
            {
                found_pos += is_set; // to GAP "0" (can go out of scope)
                pcurr = pbuf + found_pos;
            }
            BM_ASSERT (pcurr > pend || *pcurr >= start_pos);
        }
        else
        {
            for (; pcurr <= pend; pcurr += 2) // now we are in GAP "0"
                if (*pcurr >= start_pos)
                    break;
        }

    }

    const unsigned lz = bm::count_leading_zeros_u64(digest0);
    const unsigned stop_pos = (64u - lz) << set_block_digest_pos_shift;

    for (T prev; pcurr <= pend; pcurr += 2) // now we are in GAP "0" again
    {
        BM_ASSERT(*pcurr > *(pcurr-1));
        prev = pcurr[-1];
        unsigned pos = 1u + prev;
        bool all_zero = bm::check_zero_digest(digest0, prev, *pcurr);
        if (!all_zero)
            bm::sub_bit_block(dest, pos, *pcurr - prev);
        if (pos > stop_pos) // early break is possible based on digest tail
            break;
    } // for pcurr

    return bm::update_block_digest0(dest, digest0);
}


/*!
   \brief Compute bitcount of bit block AND masked by GAP block
   \param block - bitblock buffer pointer
   \param pcurr  - GAP buffer pointer
   \return bitcount - cardinality of the AND product

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_and_count(const unsigned* BMRESTRICT block,
                              const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    BM_ASSERT(block);
    const T* pend = pcurr + (*pcurr >> 3);
    bm::id_t count = 0;
    if (*pcurr & 1)  // Starts with 1
    {
        count += bm::bit_block_calc_count_range(block, 0, pcurr[1]);
        ++pcurr;
    }
    for (pcurr +=2 ;pcurr <= pend; pcurr += 2)
    {
        count += bm::bit_block_calc_count_range(block, pcurr[-1]+1, *pcurr);
    }
    return count;
}


/*!
   \brief Bitcount test of bit block AND masked by GAP block.
   \param block - bitblock buffer pointer
   \param pcurr  - GAP buffer pointer
   \return non-zero value if AND produces any result

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_and_any(const unsigned* BMRESTRICT block,
                            const T* BMRESTRICT pcurr) BMNOEXCEPT
{
    BM_ASSERT(block);

    const T* pend = pcurr + (*pcurr >> 3);
    bm::id_t count = 0;
    if (*pcurr & 1)  // Starts with 1
    {
        count = bm::bit_block_any_range(block, 0, pcurr[1]);
        ++pcurr;
    }
    for (pcurr +=2 ;!count && pcurr <= pend; pcurr += 2)
    {
        count = bm::bit_block_any_range(block, pcurr[-1]+1, *pcurr);
    }
    return count;
}



/*!
   \brief Compute bitcount of bit block SUB masked by GAP block.
   \param block - bitblock buffer pointer.
   \param buf  - GAP buffer pointer.
   \return bit-count result of AND NOT operation

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_sub_count(const unsigned* BMRESTRICT block,
                              const T* BMRESTRICT buf) BMNOEXCEPT
{
    BM_ASSERT(block);

    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    bm::id_t count = 0;

    if (!(*buf & 1))  // Starts with 0
    {
        count += bit_block_calc_count_range(block, 0, *pcurr);
        ++pcurr;
    }
    ++pcurr; // now we are in GAP "0" again

    for (;pcurr <= pend; pcurr+=2)
    {
        count += bm::bit_block_calc_count_range(block, *(pcurr-1)+1, *pcurr);
    }
    return count;
}


/*!
   \brief Compute bitcount test of bit block SUB masked by GAP block
   \param block - bitblock buffer pointer
   \param buf  - GAP buffer pointer
   \return non-zero value if AND NOT produces any 1 bits

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_sub_any(const unsigned* BMRESTRICT block,
                            const T* BMRESTRICT buf) BMNOEXCEPT
{
    BM_ASSERT(block);

    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    bm::id_t count = 0;

    if (!(*buf & 1))  // Starts with 0
    {
        count += bit_block_any_range(block, 0, *pcurr);
        if (count)
            return count;
        ++pcurr;
    }
    ++pcurr; // now we are in GAP "0" again

    for (; !count && pcurr <= pend; pcurr+=2)
    {
        count += bm::bit_block_any_range(block, *(pcurr-1)+1, *pcurr);
    }
    return count;
}



/*!
   \brief Compute bitcount of bit block XOR masked by GAP block
   \param block - bitblock buffer pointer
   \param buf  - GAP buffer pointer
   \return bit count value of XOR operation

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_xor_count(const unsigned* BMRESTRICT block,
                              const T* BMRESTRICT buf) BMNOEXCEPT
{
    BM_ASSERT(block);

    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    unsigned bitval = *buf & 1;
    
    bm::id_t count = bm::bit_block_calc_count_range(block, 0, *pcurr);
    if (bitval)
    {
        count = *pcurr + 1 - count;
    }
    
    for (bitval^=1, ++pcurr; pcurr <= pend; bitval^=1, ++pcurr)
    {
        T prev = (T)(*(pcurr-1)+1);
        bm::id_t c = bit_block_calc_count_range(block, prev, *pcurr);
        
        if (bitval) // 1 gap; means Result = Total_Bits - BitCount;
            c = (*pcurr - prev + 1) - c;
        count += c;
    }
    return count;
}

/*!
   \brief Compute bitcount test of bit block XOR masked by GAP block.
   \param block - bitblock buffer pointer
   \param buf  - GAP buffer pointer
   \return non-zero value if XOR returns anything

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_xor_any(const unsigned* BMRESTRICT block,
                            const T* BMRESTRICT buf) BMNOEXCEPT
{
    BM_ASSERT(block);

    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    unsigned bitval = *buf & 1;
    
    bm::id_t count = bit_block_any_range(block, 0, *pcurr);
    if (bitval)
        count = *pcurr + 1 - count;
    
    for (bitval^=1, ++pcurr; !count && pcurr <= pend; bitval^=1, ++pcurr)
    {
        T prev = (T)(*(pcurr-1)+1);
        bm::id_t c = bit_block_any_range(block, prev, *pcurr);
        
        if (bitval) // 1 gap; means Result = Total_Bits - BitCount;
            c = (*pcurr - prev + 1) - c;
        count += c;
    }
    return count;
}



/*!
   \brief Compute bitcount of bit block OR masked by GAP block.
   \param block - bitblock buffer pointer.
   \param buf  - GAP buffer pointer.
   \return bit count of OR

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_or_count(const unsigned* BMRESTRICT block,
                             const T* BMRESTRICT buf) BMNOEXCEPT
{
    BM_ASSERT(block);
    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    unsigned bitval = *buf & 1;
    
    bm::id_t count = bitval ? *pcurr + 1
                            : bm::bit_block_calc_count_range(block, 0, *pcurr);
    for (bitval^=1, ++pcurr; pcurr <= pend; bitval^=1, ++pcurr)
    {
        T prev = (T)(*(pcurr-1)+1);
        bm::id_t c =
            bitval ? (*pcurr - prev + 1)
                   : bm::bit_block_calc_count_range(block, prev, *pcurr);
        count += c;
    }
    return count;
}

/*!
   \brief Compute bitcount test of bit block OR masked by GAP block
   \param block - bitblock buffer pointer
   \param buf  - GAP buffer pointer
   \return non zero value if union (OR) returns anything

   @ingroup gapfunc
*/
template<typename T> 
bm::id_t gap_bitset_or_any(const unsigned* BMRESTRICT block,
                           const T* BMRESTRICT buf) BMNOEXCEPT
{
    bool b = !bm::gap_is_all_zero(buf) ||
             !bm::bit_is_all_zero(block);
    return b;
}



/*!
   \brief Bitblock memset operation. 

   \param dst - destination block.
   \param value - value to set.

   @ingroup bitfunc
*/
inline 
void bit_block_set(bm::word_t* BMRESTRICT dst, bm::word_t value) BMNOEXCEPT
{
#ifdef BMVECTOPT
    VECT_SET_BLOCK(dst, value);
#else
    ::memset(dst, int(value), bm::set_block_size * sizeof(bm::word_t));
#endif
}


/*!
   \brief GAP block to bitblock conversion.
   \param dest - bitblock buffer pointer.
   \param buf  - GAP buffer pointer.
   \param len - GAP length

   @ingroup gapfunc
*/
template<typename T> 
void gap_convert_to_bitset(unsigned* BMRESTRICT dest,
                           const T* BMRESTRICT buf,
                           unsigned len=0) BMNOEXCEPT
{
    bm::bit_block_set(dest, 0);
    if (!len)
        len = bm::gap_length(buf)-1;
    bm::gap_add_to_bitset(dest, buf, len);
}



/*!
   \brief Smart GAP block to bitblock conversion.

    Checks if GAP block is ALL-ZERO or ALL-ON. In those cases returns 
    pointer on special static bitblocks.

   \param dest - bitblock buffer pointer.
   \param buf  - GAP buffer pointer.
   \param set_max - max possible bitset length

   @ingroup gapfunc
*/
template<typename T> 
unsigned* gap_convert_to_bitset_smart(unsigned* BMRESTRICT dest,
                                      const T* BMRESTRICT buf,
                                      id_t set_max) BMNOEXCEPT
{
    if (buf[1] == set_max - 1)
        return (buf[0] & 1) ? FULL_BLOCK_REAL_ADDR : 0;
    bm::gap_convert_to_bitset(dest, buf);
    return dest;
}


/*!
   \brief Calculates sum of all words in GAP block. (For debugging purposes)
   \note For debugging and testing ONLY.
   \param buf - GAP buffer pointer.
   \return Sum of all words.

   @ingroup gapfunc
   @internal
*/
template<typename T>
unsigned gap_control_sum(const T* buf) BMNOEXCEPT
{
    unsigned end = *buf >> 3;

    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);
    ++pcurr;

    if (*buf & 1)  // Starts with 1
    {
        ++pcurr;
    }
    ++pcurr; // now we are in GAP "1" again
    while (pcurr <= pend)
    {
        BM_ASSERT(*pcurr > *(pcurr-1));
        pcurr += 2;
    }
    return buf[end];
}


/*! 
   \brief Sets all bits to 0 or 1 (GAP)
   \param buf - GAP buffer pointer.
   \param set_max - max possible bitset length
   \param value - value to set

   @ingroup gapfunc
*/
template<class T>
void gap_set_all(T* buf, unsigned set_max, unsigned value) BMNOEXCEPT
{
    BM_ASSERT(value == 0 || value == 1);
    *buf = (T)((*buf & 6u) + (1u << 3) + value);
    *(++buf) = (T)(set_max - 1);
}


/*!
    \brief Init gap block so it has block in it (can be whole block)
    \param buf  - GAP buffer pointer.
    \param from - one block start
    \param to   - one block end
    \param value - (block value)1 or 0
 
   @ingroup gapfunc
*/
template<class T> 
void gap_init_range_block(T* buf,
                          T  from,
                          T  to,
                          T  value) BMNOEXCEPT
{
    BM_ASSERT(value == 0 || value == 1);
    const unsigned set_max = bm::bits_in_block;

    unsigned gap_len;
    if (from == 0)
    {
        if (to == set_max - 1)
        {
            bm::gap_set_all(buf, set_max, value);
        }
        else
        {
            gap_len = 2;
            buf[1] = (T)to;
            buf[2] = (T)(set_max - 1);
            buf[0] = (T)((*buf & 6u) + (gap_len << 3) + value);
        }
        return;
    }
    // from != 0

    value = !value;
    if (to == set_max - 1)
    {
        gap_len = 2;
        buf[1] = (T)(from - 1);
        buf[2] = (T)(set_max - 1);
    }
    else
    {
        gap_len = 3;
        buf[1] = (T) (from - 1);
        buf[2] = (T) to;
        buf[3] = (T)(set_max - 1);
    }
    buf[0] =  (T)((*buf & 6u) + (gap_len << 3) + value);
}


/*! 
   \brief Inverts all bits in the GAP buffer.
   \param buf - GAP buffer pointer.

   @ingroup gapfunc
*/
template<typename T> void gap_invert(T* buf) BMNOEXCEPT
{ 
    *buf ^= 1;
}


#ifdef __GNUG__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#endif

/*!
   \brief Sets GAP block capacity level.
   \param buf - GAP buffer pointer.
   \param level new GAP block capacity level.

   @ingroup gapfunc
*/
template<typename T> 
void set_gap_level(T* buf, int level) BMNOEXCEPT
{
    BM_ASSERT(level >= 0);
    BM_ASSERT(unsigned(level) < bm::gap_levels);
    
    *buf = (T)(((level & 3) << 1) | (*buf & 1) | (*buf & ~7));
}
#ifdef __GNUG__
#pragma GCC diagnostic pop
#endif



/*!
   \brief Calculates GAP block capacity level.
   \param len - GAP buffer length.
   \param glevel_len - GAP lengths table
   \return GAP block capacity level. 
            -1 if block does not fit any level.
   @ingroup gapfunc
*/
template<typename T>
int gap_calc_level(unsigned len, const T* glevel_len) BMNOEXCEPT
{
    if (len <= unsigned(glevel_len[0]-4)) return 0;
    if (len <= unsigned(glevel_len[1]-4)) return 1;
    if (len <= unsigned(glevel_len[2]-4)) return 2;
    if (len <= unsigned(glevel_len[3]-4)) return 3;

    BM_ASSERT(bm::gap_levels == 4);
    return -1;
}

/*! @brief Returns number of free elements in GAP block array. 
    Difference between GAP block capacity on this level and actual GAP length.
    
    @param buf - GAP buffer pointer
    @param glevel_len - GAP lengths table
    
    @return Number of free GAP elements
    @ingroup gapfunc
*/
template<typename T>
inline unsigned gap_free_elements(const T* BMRESTRICT buf,
                                  const T* BMRESTRICT glevel_len) BMNOEXCEPT
{
    unsigned len = bm::gap_length(buf);
    unsigned capacity = bm::gap_capacity(buf, glevel_len);
    return capacity - len;
}

/*! 
   \brief Lexicographical comparison of BIT buffers.
   \param buf1 - First buffer pointer.
   \param buf2 - Second buffer pointer.
   \param len - Buffer length in elements (T).
   \return  <0 - less, =0 - equal,  >0 - greater.

   @ingroup bitfunc 
*/
template<typename T> 
int bitcmp(const T* buf1, const T* buf2, unsigned len) BMNOEXCEPT
{
    BM_ASSERT(len);
    const T* pend1 = buf1 + len; 
    do
    {
        T w1 = *buf1++;
        T w2 = *buf2++;
        T diff = w1 ^ w2;
        if (diff)
            return (w1 & diff & -diff) ? 1 : -1;
    } while (buf1 < pend1);
    return 0;
}

/*!
   \brief Find first bit which is different between two bit-blocks
   \param blk1 - block 1
   \param blk2 - block 2
   \param pos - out - position of difference (undefined if blocks are equal)
   \return  true if difference was found

   @ingroup bitfunc
*/
inline
bool bit_find_first_diff(const bm::word_t* BMRESTRICT blk1,
                         const bm::word_t* BMRESTRICT blk2,
                         unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(blk1 && blk2 && pos);
#ifdef VECT_BIT_FIND_DIFF
    bool f = VECT_BIT_FIND_DIFF(blk1, blk2, pos);
    return f;
#else
    #ifdef BM64OPT
        BM_ASSERT(sizeof(bm::wordop_t) == 8);

        const bm::bit_block_t::bunion_t* BMRESTRICT b1_u =
                        (const bm::bit_block_t::bunion_t*)(blk1);
        const bm::bit_block_t::bunion_t* BMRESTRICT b2_u =
                        (const bm::bit_block_t::bunion_t*)(blk2);

        for (unsigned i = 0; i < bm::set_block_size/2; ++i)
        {
            bm::wordop_t w1 = b1_u->w64[i];
            bm::wordop_t w2 = b2_u->w64[i];
            bm::wordop_t diff = w1 ^ w2;
            if (diff)
            {
                unsigned idx = bm::count_trailing_zeros_u64(diff);
                *pos = unsigned(idx + (i * 8u * unsigned(sizeof(bm::wordop_t))));
                return true;
            }
        } // for
    #else
        for (unsigned i = 0; i < bm::set_block_size; ++i)
        {
            bm::word_t w1 = blk1[i]; bm::word_t w2 = blk2[i];
            bm::word_t diff = w1 ^ w2;
            if (diff)
            {
                unsigned idx = bm::bit_scan_forward32(diff); // trailing zeros
                *pos = unsigned(idx + (i * 8u * sizeof(bm::word_t)));
                return true;
            }
        } // for
    #endif
    return false;
#endif
}


//#ifndef BMAVX2OPT

/*!
   \brief Converts bit block to GAP.
   \param dest - Destinatio GAP buffer.
   \param block - Source bitblock buffer.
   \param dest_len - length of the destination buffer.
   \return  New length of GAP block or 0 if conversion failed
   (insufficicent space).

   @ingroup gapfunc
*/
inline
unsigned bit_block_to_gap(gap_word_t* BMRESTRICT dest,
                          const unsigned* BMRESTRICT block,
                          unsigned dest_len) BMNOEXCEPT
{
    const unsigned* BMRESTRICT block_end = block + bm::set_block_size;
    gap_word_t* BMRESTRICT pcurr = dest;
    gap_word_t* BMRESTRICT end = dest + dest_len; (void)end;

    unsigned bitval = (*block) & 1u;
    *pcurr++ = bm::gap_word_t(bitval);
    *pcurr = 0;
    unsigned bit_idx = 0;

    do
    {
        unsigned val = *block;
        while (!val || val == ~0u)
        {
           if (bitval != unsigned(bool(val)))
           {
               *pcurr++ = (gap_word_t)(bit_idx-1);
               bitval ^= 1u;
               BM_ASSERT((pcurr-1) == (dest+1) || *(pcurr-1) > *(pcurr-2));
               BM_ASSERT(pcurr != end);
           }
           bit_idx += unsigned(sizeof(*block) * 8);
           if (++block >= block_end)
                goto complete;
           val = *block;
        } // while

        // process "0100011" word
        //
        unsigned bits_consumed = 0;
        do
        {
            unsigned tz = 1u;
            if (bitval != (val & 1u))
            {
                *pcurr++ = (gap_word_t)(bit_idx-1);
                bitval ^= 1u;
                BM_ASSERT((pcurr-1) == (dest+1) || *(pcurr-1) > *(pcurr-2));
                BM_ASSERT(pcurr != end);
            }
            else // match, find the next idx
            {
                tz = bm::bit_scan_forward32(bitval ? ~val : val);
                // possible alternative:
                //   tz = bm::count_trailing_zeros(bitval ? ~val : val);
            }
            
            bits_consumed += tz;
            bit_idx += tz;
            val >>= tz;
            
            if (!val)
            {
                if (bits_consumed < 32u)
                {
                    *pcurr++ = (gap_word_t)(bit_idx-1);
                    bitval ^= 1u;
                    bit_idx += 32u - bits_consumed;
                    BM_ASSERT((pcurr-1) == (dest+1) || *(pcurr-1) > *(pcurr-2));
                    BM_ASSERT(pcurr != end);
                }
                break;
            }
        } while (1);

    } while(++block < block_end);

complete:
    *pcurr = (gap_word_t)(bit_idx-1);
    unsigned len = (unsigned)(pcurr - dest);
    *dest = (gap_word_t)((*dest & 7) + (len << 3));
    return len;
}
//#endif

/**
   Convert bit block to GAP representation
   @internal
   @ingroup bitfunc
*/
inline
unsigned bit_to_gap(gap_word_t* BMRESTRICT dest,
                    const unsigned* BMRESTRICT block,
                    unsigned dest_len) BMNOEXCEPT
{
#if defined(VECT_BIT_TO_GAP)
    return VECT_BIT_TO_GAP(dest, block, dest_len);
#else
    return bm::bit_block_to_gap(dest, block, dest_len);
#endif
}


/*!
   \brief Iterate gap block as delta-bits with a functor 
   @ingroup gapfunc
*/
template<class T, class F>
void for_each_gap_dbit(const T* buf, F& func)
{
    const T* pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);

    ++pcurr;

    unsigned prev = 0;
    unsigned first_inc;

    if (*buf & 1)
    {
        first_inc = 0;
        unsigned to = *pcurr;
        for (unsigned i = 0; i <= to; ++i) 
        {
            func(1);
        }
        prev = to;
        ++pcurr;
    }
    else
    {
        first_inc = 1;
    }
    ++pcurr;  // set GAP to 1

    while (pcurr <= pend)
    {
        unsigned from = *(pcurr-1)+1;
        unsigned to = *pcurr;
        if (first_inc)
        {
            func(from - prev + first_inc);
            first_inc = 0;
        }
        else
        {
            func(from - prev);
        }

        for (unsigned i = from+1; i <= to; ++i) 
        {
            func(1);
        }
        prev = to;
        pcurr += 2; // jump to the next positive GAP
    }
}

/*!
   \brief Convert gap block into array of ints corresponding to 1 bits
   @ingroup gapfunc
*/
template<typename D, typename T>
D gap_convert_to_arr(D* BMRESTRICT       dest, 
                     const T* BMRESTRICT buf,
                     unsigned            dest_len,
                     bool                invert = false) BMNOEXCEPT
{
    const T* BMRESTRICT pcurr = buf;
    const T* pend = pcurr + (*pcurr >> 3);

    D* BMRESTRICT dest_curr = dest;
    ++pcurr;

    int bitval = (*buf) & 1;
    if (invert) 
        bitval = !bitval; // invert the GAP buffer

    if (bitval)
    {
        if (unsigned(*pcurr + 1) >= dest_len)
            return 0; // insufficient space
        dest_len -= *pcurr;
        T to = *pcurr;
        for (T i = 0; ;++i) 
        {
            *dest_curr++ = i;
            if (i == to) break;
        }
        ++pcurr;
    }
    ++pcurr;  // set GAP to 1

    while (pcurr <= pend)
    {
        unsigned pending = *pcurr - *(pcurr-1);
        if (pending >= dest_len)
            return 0;
        dest_len -= pending;
        T from = (T)(*(pcurr-1)+1);
        T to = *pcurr;
        for (T i = from; ;++i) 
        {
            *dest_curr++ = i;
            if (i == to) break;
        }
        pcurr += 2; // jump to the next positive GAP
    }
    return (D) (dest_curr - dest);
}


/*!
    @brief Bitcount for bit block without agressive unrolling
    @ingroup bitfunc
*/
BMFORCEINLINE
unsigned bit_count_min_unroll(const bm::word_t* BMRESTRICT block,
                              const bm::word_t* BMRESTRICT block_end) BMNOEXCEPT
{
    unsigned count = 0;
#ifdef BM64OPT__
    do
    {
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u =
                        (const bm::bit_block_t::bunion_t*)(block);

        bm::id64_t x = src_u->w64[0]; bm::id64_t y = src_u->w64[1];
        bm::id64_t u = src_u->w64[2]; bm::id64_t v = src_u->w64[3];

        #if defined(BM_USE_GCC_BUILD)
           count += unsigned(__builtin_popcountll(x) + __builtin_popcountll(y)
                           + __builtin_popcountll(u) + __builtin_popcountll(v));
        #else
            // 64-bit optimized algorithm. No sparse vect opt
            // instead it uses 4-way parallel pipelined version
            if (x | y | u | v)
            {
                unsigned c = bitcount64_4way(x, y, u, v);
                BM_ASSERT(c);
                count += c;
            }
        #endif
        block += 8;
    } while (block < block_end);
#else
    do
    {
        unsigned c1= bm::word_bitcount(*block);
        unsigned c2 = bm::word_bitcount(block[1]);        
        count += c1 + c2;        
        c1= bm::word_bitcount(block[2]);
        c2 = bm::word_bitcount(block[3]);        
        count += c1 + c2;                
        block+=4;
    } while (block < block_end);
    
#endif
    return count;
}



/*! 
    @brief Bitcount for bit block
    
    Function calculates number of 1 bits in the given array of words.
    Make sure the addresses are aligned.

    @ingroup bitfunc 
*/
inline 
bm::id_t bit_block_count(const bm::word_t* block) BMNOEXCEPT
{
    const bm::word_t* block_end = block + bm::set_block_size;
#ifdef BMVECTOPT
    return VECT_BITCOUNT(block, block_end);
#else
    return bm::bit_count_min_unroll(block, block_end);
#endif
}

/*!
    @brief Bitcount for bit block
 
    Function calculates number of 1 bits in the given array of words.
    uses digest to understand zero areas
 
    @ingroup bitfunc
*/
inline
bm::id_t bit_block_count(const bm::word_t* BMRESTRICT const block,
                         bm::id64_t digest) BMNOEXCEPT
{
#ifdef VECT_BIT_COUNT_DIGEST
    return VECT_BIT_COUNT_DIGEST(block, digest);
#else
    bm::id_t count = 0;
    bm::id64_t d = digest;

    // TODO: use platform neutral bitscan to decode digest here

    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;
        
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;

#ifdef BM64OPT
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u =
                        (const bm::bit_block_t::bunion_t*)(&block[off]);
        unsigned j = 0;
        do
        {
            bm::id64_t x = src_u->w64[0+j]; bm::id64_t y = src_u->w64[1+j];
            bm::id64_t u = src_u->w64[2+j]; bm::id64_t v = src_u->w64[3+j];
            #if defined(BM_USE_GCC_BUILD)
               count += unsigned(__builtin_popcountll(x) + __builtin_popcountll(y)
                               + __builtin_popcountll(u) + __builtin_popcountll(v));
            #else
                // 64-bit optimized algorithm. No sparse vect opt
                // instead it uses 4-way parallel pipelined version
                if (x | y | u | v)
                {
                    unsigned c = bm::bitcount64_4way(x, y, u, v);
                    BM_ASSERT(c);
                    count += c;
                }
            #endif
            j += 4;
        } while (j < bm::set_block_digest_wave_size/2);
#else // 32-bit version
    const bm::word_t* BMRESTRICT blk = &block[off];
    const bm::word_t* BMRESTRICT blk_end = &block[off+bm::set_block_digest_wave_size];
    do
    {
        unsigned c1= bm::word_bitcount(*blk);
        unsigned c2 = bm::word_bitcount(blk[1]);
        count += c1 + c2;
        c1= bm::word_bitcount(blk[2]);
        c2 = bm::word_bitcount(blk[3]);
        count += c1 + c2;
        blk+=4;
    } while (blk < blk_end);
#endif
        
        d = bm::bmi_bslr_u64(d); // d &= d - 1;
    } // while
    return count;
#endif
}


/*!
    Function calculates number of times when bit value changed 
    (1-0 or 0-1).
    
    For 001 result is 2
        010 - 3
        011 - 2
        111 - 1
    
    @ingroup bitfunc 
*/
inline 
bm::id_t bit_count_change(bm::word_t w) BMNOEXCEPT
{
    unsigned count = 1;
    w ^= (w >> 1);

    count += bm::word_bitcount(w);
    count -= (w >> ((sizeof(w) * 8) - 1));
    return count;
}


/*!
    Function calculates number of times when bit value changed
    @internal
*/
inline
unsigned bit_block_change32(const bm::word_t* BMRESTRICT block,
                            unsigned                     size) BMNOEXCEPT
{
    unsigned gap_count = 1;

    bm::word_t  w, w0, w_prev, w_l;
    w = w0 = *block;
    
    const int w_shift = int(sizeof(w) * 8 - 1);
    w ^= (w >> 1);
    gap_count += bm::word_bitcount(w);
    gap_count -= (w_prev = (w0 >> w_shift)); // negative value correction

    const bm::word_t* block_end = block + size; 
    for (++block; block < block_end; ++block)
    {
        w = w0 = *block;
        ++gap_count;
        if (!w)
        {
            gap_count -= !w_prev;
            w_prev = 0;
        }
        else
        {
            w ^= (w >> 1);
            gap_count += bm::word_bitcount(w);
            w_l = w0 & 1;
            gap_count -= (w0 >> w_shift);  // negative value correction
            gap_count -= !(w_prev ^ w_l);  // word border correction
            
            w_prev = (w0 >> w_shift);
        }
    } // for
    return gap_count;
}

/*!
    Function calculates number of times when bit value changed
    @internal
*/
inline
unsigned bit_block_change64(const bm::word_t* BMRESTRICT in_block,
                            unsigned size) BMNOEXCEPT
{
    unsigned gap_count = 1;
    const bm::id64_t* BMRESTRICT block = (const bm::id64_t*) in_block;

    bm::id64_t  w, w0, w_prev, w_l;
    w = w0 = *block;

    const int w_shift = int(sizeof(w) * 8 - 1);
    w ^= (w >> 1);
    gap_count += bm::word_bitcount64(w);
    gap_count -= unsigned(w_prev = (w0 >> w_shift)); // negative value correction

    const bm::id64_t* block_end = block + (size/2);
    for (++block; block < block_end; ++block)
    {
        w = w0 = *block;
        ++gap_count;
        if (!w)
        {
            gap_count -= !w_prev;
            w_prev = 0;
        }
        else
        {
            w ^= (w >> 1);
            gap_count += bm::word_bitcount64(w);
            w_l = w0 & 1;
            gap_count -= unsigned(w0 >> w_shift);  // negative value correction
            gap_count -= !(w_prev ^ w_l);  // word border correction
            w_prev = (w0 >> w_shift);
        }
    } // for
    return gap_count;
}



/*!
    Function calculates basic bit-block statistics
     number of times when bit value changed (GAPS)
     and population count
    @param block - bit-block pointer
    @param gc - [output] gap_count
    @param bc - [output] bit count
    @internal
*/
inline
void bit_block_change_bc(const bm::word_t* BMRESTRICT block,
                unsigned* BMRESTRICT gc, unsigned* BMRESTRICT bc) BMNOEXCEPT
{
    BM_ASSERT(gc);
    BM_ASSERT(bc);
    
    #ifdef VECT_BLOCK_CHANGE_BC
        VECT_BLOCK_CHANGE_BC(block, gc, bc);
    #else
        // TODO: one pass algo
        #ifdef BM64OPT
            *gc = bm::bit_block_change64(block, bm::set_block_size);
        #else
            *gc = bm::bit_block_change32(block, bm::set_block_size);
        #endif
        *bc = bm::bit_block_count(block);
    #endif
}



/*!
    Function calculates number of times when bit value changed
    (1-0 or 0-1) in the bit block.
 
    @param block - bit-block start pointer
    @return number of 1-0, 0-1 transitions
 
    @ingroup bitfunc
*/
inline
unsigned bit_block_calc_change(const bm::word_t* block) BMNOEXCEPT
{
#if defined(VECT_BLOCK_CHANGE)
    return VECT_BLOCK_CHANGE(block, bm::set_block_size);
#else
    #ifdef BM64OPT
        return bm::bit_block_change64(block, bm::set_block_size);
    #else
        return bm::bit_block_change32(block, bm::set_block_size);
    #endif
#endif
}

/*!
    Check if all bits are 1 in [left, right] range
    @ingroup bitfunc
*/
inline
bool bit_block_is_all_one_range(const bm::word_t* const BMRESTRICT block,
                                bm::word_t left,
                                bm::word_t right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right <= bm::gap_max_bits-1);

    unsigned nword, nbit, bitcount, temp;
    nbit = left & bm::set_word_mask;
    const bm::word_t* word =
        block + (nword = unsigned(left >> bm::set_word_shift));
    if (left == right)  // special case (only 1 bit to check)
        return (*word >> nbit) & 1u;

    if (nbit) // starting position is not aligned
    {
        unsigned right_margin = nbit + right - left;
        if (right_margin < 32)
        {
            unsigned mask_r = bm::mask_r_u32(nbit);
            unsigned mask_l = bm::mask_l_u32(right_margin);
            unsigned mask = mask_r & mask_l;
            return mask == (*word & mask);
        }

        unsigned mask_r = bm::mask_r_u32(nbit);
        temp = *word & mask_r;
        if (temp != mask_r)
            return false;
        bitcount = (right - left + 1u) - (32 - nbit);
        ++word;
    }
    else
    {
        bitcount = right - left + 1u;
    }

    const bm::word_t maskFF = ~0u;
    // loop unrolled to evaluate 4 words at a time
    // SIMD showed no advantage, unless evaluate sub-wave intervals
    //
    #if defined(BM64OPT) || defined(BM64_SSE4) || defined(BMAVX2OPT) || defined(BMAVX512OPT)
        const bm::id64_t maskFF64 = ~0ull;
        for ( ;bitcount >= 128; bitcount-=128, word+=4)
        {
            bm::id64_t w64_0 = bm::id64_t(word[0]) + (bm::id64_t(word[1]) << 32);
            bm::id64_t w64_1 = bm::id64_t(word[2]) + (bm::id64_t(word[3]) << 32);
            if ((w64_0 ^ maskFF64) | (w64_1 ^ maskFF64))
                return false;
        } // for
    #else
        for ( ;bitcount >= 128; bitcount-=128, word+=4)
        {
            bm::word_t m = (word[0] != maskFF) || (word[1] != maskFF) |
                           (word[2] != maskFF) || (word[3] != maskFF);
            if (m)
                return false;
        } // for
    #endif

    for ( ;bitcount >= 32; bitcount-=32, ++word)
    {
        if (*word != maskFF)
            return false;
    } // for
    BM_ASSERT(bitcount < 32);
    if (bitcount)  // we have a tail to count
    {
        unsigned mask_l = bm::mask_l_u32(bitcount-1);
        temp = *word & mask_l;
        if (temp != mask_l)
            return false;
    }
    return true;
}




/*!
    Function calculates number of 1 bits in the given array of words in
    the range between left anf right bits (borders included)
    Make sure the addr is aligned.

    LWA - left word aligned
    RWA - right word aligned
    @ingroup bitfunc
*/
template<bool LWA, bool RWA>
bm::id_t bit_block_calc_count_range(const bm::word_t* block,
                                    bm::word_t left,
                                    bm::word_t right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right <= bm::gap_max_bits-1);
    
    unsigned nword, nbit, bitcount, count, right_margin;
    nbit = left & bm::set_word_mask;
    block += (nword = unsigned(left >> bm::set_word_shift));
    if (left == right)  // special case (only 1 bit to check)
        return (*block >> nbit) & 1u;

    bitcount = 1u + (right_margin = (right - left));
    if constexpr (LWA)
    {
        BM_ASSERT(!nbit);
        count = 0;
    }
    else
        if (nbit) // starting position is not aligned
        {
            right_margin += nbit;
            unsigned mask_r = bm::mask_r_u32(nbit);
            if (right_margin < 32)
            {
                unsigned mask_l = bm::mask_l_u32(right_margin);
                return bm::word_bitcount(mask_r & mask_l & *block);
            }
            count = bm::word_bitcount(*block++ & mask_r);
            bitcount -= 32 - nbit;
        }
        else
            count = 0;

    // now when we are word aligned, we can count bits the usual way
    //
    #if defined(BM64_SSE4) || defined(BM64_AVX2) || defined(BM64_AVX512)
        for ( ;bitcount >= 128; bitcount-=128)
        {
            const bm::id64_t* p64 = (bm::id64_t*) block;
            bm::id64_t w64_0 = p64[0]; // x86 unaligned(!) read
            bm::id64_t w64_1 = p64[1];
            count += unsigned(_mm_popcnt_u64(w64_0));
            count += unsigned(_mm_popcnt_u64(w64_1));
            block += 4;
        }
        for ( ;bitcount >= 64; bitcount-=64)
        {
            bm::id64_t w64 = *((bm::id64_t*)block); // x86 unaligned(!) read
            count += unsigned(_mm_popcnt_u64(w64));
            block += 2;
        }
        if (bitcount >= 32)
        {
            count += bm::word_bitcount(*block++);
            bitcount-=32;
        }
    #else
        for ( ;bitcount >= 32; bitcount-=32)
            count += bm::word_bitcount(*block++);
    #endif
    BM_ASSERT(bitcount < 32);
    
    if constexpr (RWA)
    {
        BM_ASSERT(!bitcount);
    }
    else
        if (bitcount)  // we have a tail to count
        {
            unsigned mask_l = bm::mask_l_u32(bitcount-1);
            count += bm::word_bitcount(*block & mask_l);
        }
    return count;
}


/*!
    Function calculates number of 1 bits in the given array of words in
    the range between 0 anf right bits (borders included)
    Make sure the addr is aligned.

    @ingroup bitfunc
*/
inline
bm::id_t bit_block_calc_count_to(const bm::word_t*  block,
                                 bm::word_t         right) BMNOEXCEPT
{
    BM_ASSERT(block);
    if (!right) // special case, first bit check
        return *block & 1u;
    bm::id_t count = 0;

    unsigned bitcount = right + 1;

    // AVX2 or 64-bit loop unroll
    #if defined(BMAVX2OPT) || defined(BMAVX512OPT)
        BM_AVX2_POPCNT_PROLOG
    
        __m256i cnt = _mm256_setzero_si256();
        bm::id64_t* cnt64;
    
        for ( ;bitcount >= 256; bitcount -= 256)
        {
            const __m256i* src = (__m256i*)block;
            __m256i xmm256 = _mm256_load_si256(src);
            BM_AVX2_BIT_COUNT(bc, xmm256);
            cnt = _mm256_add_epi64(cnt, bc);

            block += 8;
        } // for
        cnt64 = (bm::id64_t*)&cnt;
        count += (unsigned)(cnt64[0] + cnt64[1] + cnt64[2] + cnt64[3]);
    #endif
    
    for ( ;bitcount >= 64; bitcount -= 64)
    {
        bm::id64_t* p = (bm::id64_t*)block;
        count += bm::word_bitcount64(*p);
        block += 2;
    }
    if (bitcount >= 32)
    {
        count += bm::word_bitcount(*block++);
        bitcount-=32;
    }

    if (bitcount)  // we have a tail to count
    {
        unsigned mask_l = bm::mask_l_u32(bitcount-1);
        count += bm::word_bitcount(*block & mask_l);
    }
    return count;
}



/*!
    Cyclic rotation of bit-block left by 1 bit
    @ingroup bitfunc
*/
inline
void bit_block_rotate_left_1(bm::word_t* block) BMNOEXCEPT
{
    bm::word_t co_flag = (block[0] >> 31) & 1; // carry over bit
    for (unsigned i = 0; i < bm::set_block_size-1; ++i)
    {
        block[i] = (block[i] << 1) | (block[i + 1] >> 31);
    } 
    block[set_block_size - 1] = (block[set_block_size - 1] << 1) | co_flag;
}

/*!
    @brief Unrolled cyclic rotation of bit-block left by 1 bit
    @param block - bit-block pointer
    @ingroup bitfunc
*/
inline
void bit_block_rotate_left_1_unr(bm::word_t* block) BMNOEXCEPT
{
    bm::word_t co_flag = (block[0] >> 31) & 1; // carry over bit
    const unsigned unroll_factor = 4;
    bm::word_t w0, w1, w2, w3;

    unsigned i;
    for (i = 0; i < bm::set_block_size - unroll_factor; i += unroll_factor)
    {
        w0 = block[i + 1] >> 31;
        w1 = block[i + 2] >> 31;
        w2 = block[i + 3] >> 31;
        w3 = block[i + 4] >> 31;

        block[0 + i] = (block[0 + i] << 1) | w0;
        block[1 + i] = (block[1 + i] << 1) | w1;
        block[2 + i] = (block[2 + i] << 1) | w2;
        block[3 + i] = (block[3 + i] << 1) | w3;
    }
    block[i] = (block[i] << 1) | (block[i + 1] >> 31);
    block[i + 1] = (block[i + 1] << 1) | (block[i + 2] >> 31);
    block[i + 2] = (block[i + 2] << 1) | (block[i + 3] >> 31);
    block[set_block_size - 1] = (block[set_block_size - 1] << 1) | co_flag;
}

/*!
    @brief insert bit into position and shift the rest right with carryover
 
    @param block - bit-block pointer
    @param bitpos   - bit position to insert
    @param value - bit value (0|1) to insert
 
    @return carry over value
    @ingroup bitfunc
*/
inline
bm::word_t bit_block_insert(bm::word_t* BMRESTRICT block,
                            unsigned bitpos, bool value) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(bitpos < 65536);
    
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask);
    unsigned nword = unsigned(nbit >> bm::set_word_shift);
    nbit &= bm::set_word_mask;

    bm::word_t co_flag = 0;
    if (nbit)
    {
        unsigned mask_r = bm::mask_r_u32(nbit);
        bm::word_t w = block[nword] & mask_r;
        bm::word_t wl = block[nword] & ~mask_r;
        co_flag = w >> 31;
        w <<= 1u;
        block[nword] = w | (unsigned(value) << nbit) | wl;
        ++nword;
    }
    else
    {
        co_flag = value;
    }
    
    for (unsigned i = nword; i < bm::set_block_size; ++i)
    {
        bm::word_t w = block[i];
        bm::word_t co_flag1 = w >> 31;
        w = (w << 1u) | co_flag;
        block[i] = w;
        co_flag = co_flag1;
    } // for i
    return co_flag;
}



/*!
    @brief Right bit-shift bitblock by 1 bit (reference)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the previous block
 
    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_r1(bm::word_t* BMRESTRICT block,
                        bm::word_t* BMRESTRICT empty_acc,
                        bm::word_t             co_flag) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(empty_acc);
    
    bm::word_t acc(0);
    for (unsigned i = 0; i < bm::set_block_size; ++i)
    {
        bm::word_t w = block[i];
        bm::word_t co_flag1 = w >> 31;
        acc |= w = (w << 1u) | co_flag;

        block[i] = w;
        co_flag = co_flag1;
    } // for i
    *empty_acc = acc;
    return co_flag;
}

/*!
    @brief Right bit-shift bitblock by 1 bit (minimum unroll)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the previous block

    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_r1_unr_min(bm::word_t* BMRESTRICT block,
                        bm::word_t* BMRESTRICT empty_acc,
                        bm::id64_t             co_flag) BMNOEXCEPT
{

#if defined(BM64OPT)
    // warning: all this code maybe not safe on big-endian
    bm::bit_block_t::bunion_t* BMRESTRICT b_u =
                    (bm::bit_block_t::bunion_t*)(block);

    bm::id64_t acc0(0), acc1(0);
    unsigned i = 0;
    do
    {
        bm::id64_t w, co_flag1;
        w = b_u->w64[i];
        co_flag1 = w >> 63;
        acc0 |= w = (w << 1u) | co_flag;
        b_u->w64[i++] = w;
        co_flag = co_flag1;

        w = b_u->w64[i];
        co_flag1 = w >> 63;
        acc1 |= w = (w << 1u) | co_flag;
        b_u->w64[i] = w;
        co_flag = co_flag1;

    } while(++i < bm::set_block_size/2);
    *empty_acc = bool(acc0 | acc1);
    return co_flag;
#else
    return bm::bit_block_shift_r1(block, empty_acc, unsigned(co_flag));
#endif
}


/*!
    @brief Right bit-shift of bit-block by 1 bit (loop unrolled)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the previous block

    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_r1_unr(bm::word_t* BMRESTRICT block,
                            bm::word_t* BMRESTRICT empty_acc,
                            bm::word_t             co_flag) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(empty_acc);
    #if defined(VECT_SHIFT_R1)
        return VECT_SHIFT_R1(block, empty_acc, co_flag);
    #else
        return bm::bit_block_shift_r1_unr_min(block, empty_acc, co_flag);
    #endif
}


/*!
    @brief Left bit-shift bitblock by 1 bit (reference)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the prev/next block
 
    @return carry over bit (1 or 0)

    @ingroup bitfunc
*/
inline
bool bit_block_shift_l1(bm::word_t* block,
                        bm::word_t* empty_acc, bm::word_t co_flag) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(empty_acc);
    
    bm::word_t acc = 0;
    for (int i = bm::set_block_size-1; i >= 0; --i)
    {
        bm::word_t w = block[i];
        bm::word_t co_flag1 = w & 1u;
        acc |= w = (w >> 1u) | (co_flag << 31u);
        block[i] = w;
        co_flag = co_flag1;
    }

    *empty_acc = acc;
    return co_flag;
}

/*!
    @brief Left bit-shift bitblock by 1 bit (minimum unroll)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the prev/next block

    @return carry over bit (1 or 0)

    @ingroup bitfunc
*/
inline
bool bit_block_shift_l1_unr_min(bm::word_t* BMRESTRICT block,
                        bm::word_t* BMRESTRICT empty_acc,
                        unsigned             co_flag) BMNOEXCEPT
{
    bm::word_t acc = 0;
    for (int i = bm::set_block_size-1; i >= 0; i-=2)
    {
        bm::word_t w0, w1, co_flag1;
        w0 = block[i]; w1 = block[i-1];
        co_flag1 = w0 & 1u;
        acc |= w0 = (w0 >> 1u) | (co_flag << 31u);
        block[i] = w0;
        co_flag = co_flag1;
        co_flag1 = w1 & 1u;
        acc |= w1 = (w1 >> 1u) | (co_flag << 31u);
        block[i-1] = w1;
        co_flag = co_flag1;

        i-=2;
        w0 = block[i]; w1 = block[i-1];
        co_flag1 = w0 & 1u;
        acc |= w0 = (w0 >> 1u) | (co_flag << 31u);
        block[i] = w0;
        co_flag = co_flag1;
        co_flag1 = w1 & 1u;
        acc |= w1 = (w1 >> 1u) | (co_flag << 31u);
        block[i-1] = w1;
        co_flag = co_flag1;
    } // for i
    *empty_acc = acc;
    return co_flag;
}


/*!
    @brief Left bit-shift of bit-block by 1 bit (loop unrolled)
    @param block - bit-block pointer
    @param empty_acc - [out] contains 0 if block is empty
    @param co_flag - carry over from the prev/next block

    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_l1_unr(bm::word_t* block,
                            bm::word_t* empty_acc,
                            bm::word_t  co_flag) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(empty_acc);
    #if defined(VECT_SHIFT_L1)
        return VECT_SHIFT_L1(block, empty_acc, co_flag);
    #else
        return bm::bit_block_shift_l1_unr_min(block, empty_acc, co_flag);
    #endif
}

/*!
    @brief erase bit from position and shift the rest right with carryover
 
    @param block - bit-block pointer
    @param bitpos   - bit position to insert
    @param carry_over - bit value to add to the end (0|1)
 
    @ingroup bitfunc
*/
inline
void bit_block_erase(bm::word_t* block,
                     unsigned    bitpos,
                     bool        carry_over) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(bitpos < 65536);

    if (!bitpos)
    {
        bm::word_t acc;
        bm::bit_block_shift_l1_unr(block, &acc, carry_over);
        return;
    }
    
    unsigned nbit  = unsigned(bitpos & bm::set_block_mask);
    unsigned nword = unsigned(nbit >> bm::set_word_shift);
    nbit &= bm::set_word_mask;
    
    bm::word_t co_flag = carry_over;
    for (unsigned i = bm::set_block_size-1; i > nword; --i)
    {
        bm::word_t w = block[i];
        bm::word_t co_flag1 = w & 1u;
        w = (w >> 1u) | (co_flag << 31u);
        block[i] = w;
        co_flag = co_flag1;
    }
    
    if (nbit)
    {
        unsigned mask_r = bm::mask_r_u32(nbit);
        bm::word_t w = block[nword] & mask_r;
        bm::word_t wl = block[nword] & ~mask_r;
        w &= ~(1 << nbit); // clear the removed bit
        w >>= 1u;
        w |= wl | (co_flag << 31u);
        block[nword] = w;
    }
    else
    {
        block[nword] = (block[nword] >> 1u) | (co_flag << 31u);
    }
}


/*!
    @brief Right bit-shift of bit-block by 1 bit (reference) + AND
    @param block      - bit-block pointer
    @param co_flag    - carry over from the previous block
    @param mask_block - mask bit-block pointer
    @param digest     - block digest
 
    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_r1_and(bm::word_t* BMRESTRICT block,
                            bm::word_t co_flag,
                            const bm::word_t* BMRESTRICT mask_block,
                            bm::id64_t* BMRESTRICT digest) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(mask_block);
    BM_ASSERT(digest && *digest);
    
    
    bm::id64_t d = *digest;

    unsigned di = 0;
    if (!co_flag)
    {
        bm::id64_t t = d & -d;
        di = bm::word_bitcount64(t - 1); // find start bit-index
    }

    for (; di < 64; ++di)
    {
        const unsigned d_base = di * bm::set_block_digest_wave_size;
        bm::id64_t dmask = (1ull << di);
        if (d & dmask) // digest stride not empty
        {
            bm::word_t acc = 0;
            for (unsigned i = d_base; i < d_base + bm::set_block_digest_wave_size; ++i)
            {
                BM_ASSERT(i < bm::set_block_size);
                
                bm::word_t w = block[i];
                bm::word_t co_flag1 = w >> 31;
                w = (w << 1u) | co_flag;
                acc |= block[i] = w & mask_block[i];
                co_flag = co_flag1;
            }
            if (!acc)
                d &= ~dmask; // update digest: clear stride bit
        }
        else // stride is empty
        {
            BM_ASSERT(block[d_base + bm::set_block_digest_wave_size -1]==0);
            BM_ASSERT(block[d_base]==0);

            if (co_flag) // there is carry-over
            {
                BM_ASSERT(co_flag == 1);
                BM_ASSERT(block[d_base] == 0);
                
                block[d_base] = co_flag & mask_block[d_base];
                if (block[d_base])
                    d |= dmask; // update digest
                co_flag = 0;
            }
        }
    } // for di
    
    *digest = d;
    return co_flag;
}

/*!
    @brief Right bit-shift bitblock by 1 bit (reference) + AND
    @param block      - bit-block pointer
    @param co_flag    - carry over from the previous block
    @param mask_block - mask bit-block pointer
    @param digest     - block digest
 
    @return carry over bit (1 or 0)
    @ingroup bitfunc
*/
inline
bool bit_block_shift_r1_and_unr(bm::word_t* BMRESTRICT block,
                                bm::word_t co_flag,
                                const bm::word_t* BMRESTRICT mask_block,
                                bm::id64_t* BMRESTRICT digest) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(mask_block);
    BM_ASSERT(digest);
    
    #if defined(VECT_SHIFT_R1_AND)
        return VECT_SHIFT_R1_AND(block, co_flag, mask_block, digest);
    #else
        return bm::bit_block_shift_r1_and(block, co_flag, mask_block, digest);
    #endif
}


/*!
    Function calculates if there is any number of 1 bits 
    in the given array of words in the range between left anf right bits 
    (borders included). Make sure the addresses are aligned.

    @ingroup bitfunc
*/
inline 
bm::id_t bit_block_any_range(const bm::word_t* const BMRESTRICT block,
                             bm::word_t left,
                             bm::word_t right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    
    unsigned nbit  = left; // unsigned(left & bm::set_block_mask);
    unsigned nword = unsigned(nbit >> bm::set_word_shift);
    nbit &= bm::set_word_mask;

    const bm::word_t* word = block + nword;

    if (left == right)  // special case (only 1 bit to check)
    {
        return (*word >> nbit) & 1;
    }
    unsigned acc;
    unsigned bitcount = right - left + 1;

    if (nbit) // starting position is not aligned
    {
        unsigned right_margin = nbit + (right - left);
        if (right_margin < 32)
        {
            unsigned mask_r = bm::mask_r_u32(nbit);
            unsigned mask_l = bm::mask_l_u32(right_margin);
            unsigned mask = mask_r & mask_l;
            return *word & mask;
        }
        else
        {
            unsigned mask_r = bm::mask_r_u32(nbit);
            acc = *word & mask_r;
            if (acc) 
                return acc;
            bitcount -= 32 - nbit;
        }
        ++word;
    }

    // loop unrolled to evaluate 4 words at a time
    // SIMD showed no advantage, unless evaluate sub-wave intervals
    //
    for ( ;bitcount >= 128; bitcount-=128, word+=4)
    {
        acc = word[0] | word[1] | word[2] | word[3];
        if (acc)
            return acc;
    } // for

    acc = 0;
    for ( ;bitcount >= 32; bitcount -= 32)
    {
        acc |= *word++;
    } // for

    if (bitcount)  // we have a tail to count
    {
        unsigned mask_l = bm::mask_l_u32(bitcount-1);
        acc |= (*word) & mask_l;
    }
    return acc;
}

// ----------------------------------------------------------------------

/*! Function inverts block of bits 
    @ingroup bitfunc 
*/
template<typename T>
void bit_invert(T* start) BMNOEXCEPT
{
    BM_ASSERT(IS_VALID_ADDR((bm::word_t*)start));
#ifdef BMVECTOPT
    VECT_INVERT_BLOCK(start);
#else
    T* end = (T*)((unsigned*)(start) + bm::set_block_size);
    do
    {
        start[0] = ~start[0];
        start[1] = ~start[1];
        start[2] = ~start[2];
        start[3] = ~start[3];
        start+=4;
    } while (start < end);
#endif
}

// ----------------------------------------------------------------------

/*! @brief Returns "true" if all bits in the block are 1
    @ingroup bitfunc 
*/
inline
bool is_bits_one(const bm::wordop_t* start) BMNOEXCEPT
{
#if defined(VECT_IS_ONE_BLOCK)
    return VECT_IS_ONE_BLOCK(start);
#else
    const bm::word_t* BMRESTRICT src_end = (bm::word_t*)start + bm::set_block_size;
    const bm::wordop_t* end = (const bm::wordop_t*)src_end;
   do
   {
        bm::wordop_t tmp = 
            start[0] & start[1] & start[2] & start[3];
        if (tmp != bm::all_bits_mask) 
            return false;
        start += 4;
   } while (start < end);
   return true;
#endif
}

// ----------------------------------------------------------------------

/*! @brief Returns "true" if all bits are 1 in the block [left, right]
    Function check for block varieties
    @internal
*/
inline
bool block_is_all_one_range(const bm::word_t* const BMRESTRICT block,
                            unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);
    if (block)
    {
        if (BM_IS_GAP(block))
            return bm::gap_is_all_one_range(BMGAP_PTR(block), left, right);
        if (block == FULL_BLOCK_FAKE_ADDR)
            return true;
        return bm::bit_block_is_all_one_range(block, left, right);
    }
    return false;
}

/*! @brief Returns "true" if all bits are 1 in the block [left, right]
    and border bits are 0
    @internal
*/
inline
bool block_is_interval(const bm::word_t* const BMRESTRICT block,
                       unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits-1);

    if (block)
    {
        bool is_left, is_right, all_one;
        if (BM_IS_GAP(block))
        {
            const bm::gap_word_t* gap = BMGAP_PTR(block);
            all_one = bm::gap_is_interval(gap, left, right);
            return all_one;
        }
        else // bit-block
        {
            if (block == FULL_BLOCK_FAKE_ADDR)
                return false;
            unsigned nword = ((left-1) >> bm::set_word_shift);
            is_left = block[nword] & (1u << ((left-1) & bm::set_word_mask));
            if (is_left == false)
            {
                nword = ((right + 1) >> bm::set_word_shift);
                is_right = block[nword] & (1u << ((right + 1) & bm::set_word_mask));
                if (is_right == false)
                {
                    all_one = bm::bit_block_is_all_one_range(block, left, right);
                    return all_one;
                }
            }
        }
    }

    return false;
}

// ----------------------------------------------------------------------

/**
    \brief Searches for the last 1 bit in the 111 interval of a BIT block
    \param block - BIT buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup bitfunc
*/
inline
bool bit_block_find_interval_end(const bm::word_t* BMRESTRICT block,
                           unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(pos);

    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned bit_pos = (nbit & bm::set_word_mask);
    bm::word_t w = block[nword];
    w &= (1u << bit_pos);
    if (!w)
        return false;

    if (nbit == bm::gap_max_bits-1)
    {
        *pos = bm::gap_max_bits-1;
        return true;
    }
    *pos = nbit;

    ++nbit;
    nword  = unsigned(nbit >> bm::set_word_shift);
    bit_pos = (nbit & bm::set_word_mask);

    w = (~block[nword]) >> bit_pos;
    w <<= bit_pos; // clear the trailing bits
    if (w)
    {
        bit_pos = bm::bit_scan_forward32(w); // trailing zeros
        *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t)))-1);
        return true;
    }

    for (++nword; nword < bm::set_block_size; ++nword)
    {
        w = ~block[nword];
        if (w)
        {
            bit_pos = bm::bit_scan_forward32(w); // trailing zeros
            *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t)))-1);
            return true;
        }
    } // for nword

    // 0 not found, all block is 1s...
    *pos = bm::gap_max_bits-1;
    return true;
}


/*! @brief Find end of the current 111 interval
    @return search result code 0 - not found, 1 found, 2 - found at the end
    @internal
*/
inline
unsigned block_find_interval_end(const bm::word_t* BMRESTRICT block,
                                 unsigned  nbit_from,
                                 unsigned* BMRESTRICT found_nbit) BMNOEXCEPT
{
    BM_ASSERT(block && found_nbit);
    BM_ASSERT(nbit_from < bm::gap_max_bits);

    bool b;
    if (BM_IS_GAP(block))
    {
        const bm::gap_word_t* gap = BMGAP_PTR(block);
        b = bm::gap_find_interval_end(gap, nbit_from, found_nbit);
        if (b && *found_nbit == bm::gap_max_bits-1)
            return 2; // end of block, keep searching
    }
    else // bit-block
    {
        if (IS_FULL_BLOCK(block))
        {
            *found_nbit = bm::gap_max_bits-1;
            return 2;
        }
        b = bm::bit_block_find_interval_end(block, nbit_from, found_nbit);
        if (b && *found_nbit == bm::gap_max_bits-1)
            return 2; // end of block, keep searching
    }
    return b;
}

// ----------------------------------------------------------------------

/**
    \brief Searches for the first 1 bit in the 111 interval of a BIT block
    \param block - BIT buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup bitfunc
*/
inline
bool bit_block_find_interval_start(const bm::word_t* BMRESTRICT block,
                           unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(pos);

    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned bit_pos = (nbit & bm::set_word_mask);
    bm::word_t w = block[nword];
    w &= (1u << bit_pos);
    if (!w)
        return false;

    if (nbit == 0)
    {
        *pos = 0;
        return true;
    }
    *pos = nbit;

    --nbit;
    nword  = unsigned(nbit >> bm::set_word_shift);
    bit_pos = (nbit & bm::set_word_mask);

    unsigned mask_l = bm::mask_l_u32(bit_pos);
    w = (~block[nword]) & mask_l;
    if (w)
    {
        bit_pos = bm::bit_scan_reverse32(w);
        *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t)))+1);
        return true;
    }

    if (nword)
    {
        for (--nword; true; --nword)
        {
            w = ~block[nword];
            if (w)
            {
                bit_pos = bm::bit_scan_reverse32(w); // trailing zeros
                *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t)))+1);
                return true;
            }
            if (!nword)
                break;
        } // for nword
    }

    // 0 not found, all block is 1s...
    *pos = 0;
    return true;
}

/**
    \brief Reverse search for the previous 1 bit
    \param block - BIT buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return false if not found
    @ingroup bitfunc
*/
inline
bool bit_block_find_prev(const bm::word_t* BMRESTRICT block,
                           unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(pos);

    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned bit_pos = (nbit & bm::set_word_mask);
    bm::word_t w = block[nword];
    w &= (1u << bit_pos);
    if (w)
    {
        *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t))));
        return true;
    }

    if (!nbit)
        return false;

    --nbit;
    nword  = unsigned(nbit >> bm::set_word_shift);
    bit_pos = (nbit & bm::set_word_mask);

    unsigned mask_l = bm::mask_l_u32(bit_pos);
    w = block[nword] & mask_l;
    if (w)
    {
        bit_pos = bm::bit_scan_reverse32(w);
        *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t))));
        return true;
    }

    if (nword)
    {
        for (--nword; true; --nword)
        {
            w = block[nword];
            if (w)
            {
                bit_pos = bm::bit_scan_reverse32(w); // trailing zeros
                *pos =
                unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t))));
                return true;
            }
            if (!nword)
                break;
        } // for nword
    }
    return false;
}


// ----------------------------------------------------------------------

/*! @brief Find start of the current 111 interval
    @return search result code 0 - not found, 1 found, 2 - found at the start
    @internal
*/
inline
unsigned block_find_interval_start(const bm::word_t* BMRESTRICT block,
                                   unsigned  nbit_from,
                                   unsigned* BMRESTRICT found_nbit) BMNOEXCEPT
{
    BM_ASSERT(block && found_nbit);
    BM_ASSERT(nbit_from < bm::gap_max_bits);
    bool b;
    if (BM_IS_GAP(block))
    {
        const bm::gap_word_t* gap = BMGAP_PTR(block);
        b = bm::gap_find_interval_start(gap, nbit_from, found_nbit);
        if (b && *found_nbit == 0)
            return 2; // start of block, keep searching
    }
    else // bit-block
    {
        if (IS_FULL_BLOCK(block))
        {
            *found_nbit = 0;
            return 2;
        }
        b = bm::bit_block_find_interval_start(block, nbit_from, found_nbit);
        if (b && *found_nbit == 0)
            return 2; // start of block, keep searching
    }
    return b;
}

// ----------------------------------------------------------------------

/*! @brief Reverse find 1
    @return search result code 0 - not found, 1 found
    @internal
*/
inline
bool block_find_reverse(const bm::word_t* BMRESTRICT block,
                        unsigned  nbit_from,
                        unsigned* BMRESTRICT found_nbit) BMNOEXCEPT
{
    BM_ASSERT(block && found_nbit);
    BM_ASSERT(nbit_from < bm::gap_max_bits);
    bool b;
    if (BM_IS_GAP(block))
    {
        b = bm::gap_find_prev(BMGAP_PTR(block), nbit_from, found_nbit);
    }
    else // bit-block
    {
        if (IS_FULL_BLOCK(block))
        {
            *found_nbit = nbit_from;
            return true;
        }
        b = bm::bit_block_find_prev(block, nbit_from, found_nbit);
    }
    return b;
}

// ----------------------------------------------------------------------

/*! @brief Returns "true" if one bit is set in the block [left, right]
    Function check for block varieties
    @internal
*/
inline
bool block_any_range(const bm::word_t* const BMRESTRICT block,
                            unsigned left, unsigned right) BMNOEXCEPT
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::gap_max_bits);
    if (!block)
        return false;
    if (BM_IS_GAP(block))
        return bm::gap_any_range(BMGAP_PTR(block), left, right);
    if (IS_FULL_BLOCK(block))
        return true;
    return bm::bit_block_any_range(block, left, right);
}

// ----------------------------------------------------------------------

/*! @brief Returns "true" if one bit is set in the block
    Function check for block varieties
    @internal
*/
inline
bool block_any(const bm::word_t* const BMRESTRICT block) BMNOEXCEPT
{
    if (!block)
        return false;
    if (IS_FULL_BLOCK(block))
        return true;
    bool all_zero = (BM_IS_GAP(block)) ?
                        bm::gap_is_all_zero(BMGAP_PTR(block))
                      : bm::bit_is_all_zero(block);
    return !all_zero;
}



// ----------------------------------------------------------------------

// GAP blocks manipulation functions:


/*!
   \brief GAP AND operation.
   
   Function performs AND logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \param tmp_buf - pointer on temporary buffer
   \param dsize   - out size of the destination
   \return Result pointer (tmp_buf OR vect1)

   @ingroup gapfunc
*/
inline
gap_word_t* gap_operation_and(const gap_word_t* BMRESTRICT vect1,
                              const gap_word_t* BMRESTRICT vect2,
                              gap_word_t*       BMRESTRICT tmp_buf,
                              unsigned&         dsize) BMNOEXCEPT
{
    bm::gap_buff_op<bm::gap_word_t, bm::and_func>(
                                        tmp_buf, vect1, 0, vect2, 0, dsize);
    return tmp_buf;
}

/*!
   \brief GAP AND operation test.
   
   Function performs AND logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \return non zero value if operation returns any 1 bit

   @ingroup gapfunc
*/
inline
unsigned gap_operation_any_and(const gap_word_t* BMRESTRICT vect1,
                               const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return gap_buff_any_op<bm::gap_word_t, bm::and_func>(vect1, 0, vect2, 0);
}


/*!
   \brief GAP bitcount AND operation test.
   
   \param vect1   - operand 1
   \param vect2   - operand 2
   \return bitcount of vect1 AND vect2

   @ingroup gapfunc
*/
inline
unsigned gap_count_and(const gap_word_t* BMRESTRICT vect1,
                       const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return bm::gap_buff_count_op<bm::gap_word_t, bm::and_func>(vect1, vect2);
}



/*!
   \brief GAP XOR operation.
   
   Function performs XOR logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \param tmp_buf - pointer on temporary buffer
   \param dsize   - out destination size
   \return Result pointer (tmp_buf)

   @ingroup gapfunc
*/
inline
gap_word_t* gap_operation_xor(const gap_word_t*  BMRESTRICT vect1,
                              const gap_word_t*  BMRESTRICT vect2,
                              gap_word_t*        BMRESTRICT tmp_buf,
                              unsigned&                     dsize) BMNOEXCEPT
{
    bm::gap_buff_op<bm::gap_word_t, bm::xor_func>(
                                        tmp_buf, vect1, 0, vect2, 0, dsize);
    return tmp_buf;
}

/*! Light weight gap_operation_xor for len prediction
   @ingroup gapfunc
*/
/*
inline
bool gap_operation_dry_xor(const gap_word_t*  BMRESTRICT vect1,
                           const gap_word_t*  BMRESTRICT vect2,
                           unsigned&                     dsize,
                           unsigned limit) BMNOEXCEPT
{
    return
    bm::gap_buff_dry_op<bm::gap_word_t, bm::xor_func>(vect1, vect2, dsize, limit);
}
*/

/*!
   \brief GAP XOR operation test.
   
   Function performs AND logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \return non zero value if operation returns any 1 bit

   @ingroup gapfunc
*/
BMFORCEINLINE 
unsigned gap_operation_any_xor(const gap_word_t* BMRESTRICT vect1,
                               const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return gap_buff_any_op<bm::gap_word_t, bm::xor_func>(vect1, 0, vect2, 0);
}

/*!
   \brief GAP bitcount XOR operation test.
   
   \param vect1   - operand 1
   \param vect2   - operand 2
   \return bitcount of vect1 XOR vect2

   @ingroup gapfunc
*/
BMFORCEINLINE
unsigned gap_count_xor(const gap_word_t* BMRESTRICT vect1,
                       const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return bm::gap_buff_count_op<bm::gap_word_t, bm::xor_func>(vect1, vect2);
}


/*!
   \brief GAP OR operation.
   
   Function performs OR logical oparation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \param tmp_buf - pointer on temporary buffer
   \param dsize   - out destination size

   \return Result pointer (tmp_buf)

   @ingroup gapfunc
*/
inline 
gap_word_t* gap_operation_or(const gap_word_t*  BMRESTRICT vect1,
                             const gap_word_t*  BMRESTRICT vect2,
                             gap_word_t*        BMRESTRICT tmp_buf,
                             unsigned&                     dsize) BMNOEXCEPT
{
    bm::gap_buff_op<bm::gap_word_t, bm::and_func>(tmp_buf, vect1, 1, vect2, 1, dsize);
    bm::gap_invert(tmp_buf);
    return tmp_buf;
}

/*!
   \brief GAP bitcount OR operation test.
   
   \param vect1   - operand 1
   \param vect2   - operand 2
   \return bitcount of vect1 OR vect2

   @ingroup gapfunc
*/
BMFORCEINLINE 
unsigned gap_count_or(const gap_word_t* BMRESTRICT vect1,
                      const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return gap_buff_count_op<bm::gap_word_t, bm::or_func>(vect1, vect2);
}



/*!
   \brief GAP SUB (AND NOT) operation.
   
   Function performs SUB logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \param tmp_buf - pointer on temporary buffer
   \param dsize   - out destination size

   \return Result pointer (tmp_buf)

   @ingroup gapfunc
*/
inline
gap_word_t* gap_operation_sub(const gap_word_t*  BMRESTRICT vect1,
                              const gap_word_t*  BMRESTRICT vect2,
                              gap_word_t*        BMRESTRICT tmp_buf,
                              unsigned&                     dsize) BMNOEXCEPT
{
    bm::gap_buff_op<bm::gap_word_t, bm::and_func>( // no bug here
                                        tmp_buf, vect1, 0, vect2, 1, dsize);
    return tmp_buf;
}


/*!
   \brief GAP SUB operation test.
   
   Function performs AND logical operation on gap vectors.
   If possible function put the result into vect1 and returns this
   pointer.  Otherwise result is put into tmp_buf, which should be 
   twice of the vector size.

   \param vect1   - operand 1
   \param vect2   - operand 2
   \return non zero value if operation returns any 1 bit

   @ingroup gapfunc
*/
inline
unsigned gap_operation_any_sub(const gap_word_t* BMRESTRICT vect1,
                               const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return
    bm::gap_buff_any_op<bm::gap_word_t, bm::and_func>( // no bug here
                                               vect1, 0, vect2, 1);
}


/*!
\brief GAP bitcount SUB (AND NOT) operation test.

\param vect1   - operand 1
\param vect2   - operand 2
\return bitcount of vect1 SUB (AND NOT) vect2

@ingroup gapfunc
*/
BMFORCEINLINE 
unsigned gap_count_sub(const gap_word_t* BMRESTRICT vect1,
                       const gap_word_t* BMRESTRICT vect2) BMNOEXCEPT
{
    return bm::gap_buff_count_op<bm::gap_word_t, bm::sub_func>(vect1, vect2);
}


// ----------------------------------------------------------------------

// BIT blocks manipulation functions:


/*!
   \brief Bitblock copy operation.
   \param dst [out] - destination block.
   \param src [in] - source block.

   @ingroup bitfunc
*/
inline 
void bit_block_copy(bm::word_t* BMRESTRICT dst,
                    const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef BMVECTOPT
    VECT_COPY_BLOCK(dst, src);
#else
    ::memcpy(dst, src, bm::set_block_size * sizeof(bm::word_t));
#endif
}

/*!
   \brief Bitblock copy operation (unaligned src)
   \param dst [out] - destination block.
   \param src [in] - source block.

   @ingroup bitfunc
*/
inline
void bit_block_copy_unalign(bm::word_t* BMRESTRICT dst,
                            const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef VECT_COPY_BLOCK_UNALIGN
    VECT_COPY_BLOCK_UNALIGN(dst, src);
#else
    ::memcpy(dst, src, bm::set_block_size * sizeof(bm::word_t));
#endif
}



/*!
   \brief Bitblock copy/stream operation.

   \param dst - destination block.
   \param src - source block.

   @ingroup bitfunc
*/
inline
void bit_block_stream(bm::word_t* BMRESTRICT dst,
                      const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef VECT_STREAM_BLOCK
    VECT_STREAM_BLOCK(dst, src);
#else
    ::memcpy(dst, src, bm::set_block_size * sizeof(bm::word_t));
#endif
}

/*!
   \brief Bitblock copy/stream operation (unaligned src)
   \param dst [out] - destination block.
   \param src [in] - source block (unaligned address)

   @ingroup bitfunc
*/
inline
void bit_block_stream_unalign(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef VECT_STREAM_BLOCK_UNALIGN
    VECT_STREAM_BLOCK_UNALIGN(dst, src);
#else
    ::memcpy(dst, src, bm::set_block_size * sizeof(bm::word_t));
#endif
}



/*!
   \brief Plain bitblock AND operation. 
   Function does not analyse availability of source and destination blocks.

   \param dst - destination block.
   \param src - source block.
 
   \return 0 if AND operation did not produce anything (no 1s in the output)

   @ingroup bitfunc
*/
inline 
bm::id64_t bit_block_and(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src);
    BM_ASSERT(dst != src);

#ifdef BMVECTOPT
    bm::id64_t acc = VECT_AND_BLOCK(dst, src);
#else
    unsigned arr_sz = bm::set_block_size / 2;
    const bm::bit_block_t::bunion_t* BMRESTRICT src_u = (const bm::bit_block_t::bunion_t*)src;
    bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)dst;
    bm::id64_t acc = 0;
    for (unsigned i = 0; i < arr_sz; i+=4)
    {
        acc |= (dst_u->w64[i]   &= src_u->w64[i])   |
               (dst_u->w64[i+1] &= src_u->w64[i+1]) |
               (dst_u->w64[i+2] &= src_u->w64[i+2]) |
               (dst_u->w64[i+3] &= src_u->w64[i+3]);
    }
#endif
    return acc;
}

/*!
   \brief digest based bit-block AND

   \param dst - destination block.
   \param src - source block.
   \param digest - known digest of dst block
 
   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_and(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src,
                         bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src);
    BM_ASSERT(dst != src);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;
        
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_AND_DIGEST)
            bool all_zero = VECT_AND_DIGEST(&dst[off], &src[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u = (const bm::bit_block_t::bunion_t*)(&src[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)(&dst[off]);

            bm::id64_t acc = 0;
            unsigned j = 0;
            do
            {
                acc |= dst_u->w64[j+0] &= src_u->w64[j+0];
                acc |= dst_u->w64[j+1] &= src_u->w64[j+1];
                acc |= dst_u->w64[j+2] &= src_u->w64[j+2];
                acc |= dst_u->w64[j+3] &= src_u->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);
        
            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif
    } // while
    
    return digest;
}


/*!
   \brief digest based bit-block AND 5-way

   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_and_5way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src0,
                              const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2,
                              const bm::word_t* BMRESTRICT src3,
                              bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src0 && src1 && src2 && src3);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;
        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

#if defined(VECT_AND_DIGEST_5WAY)
        bool all_zero = VECT_AND_DIGEST_5WAY(&dst[off], &src0[off], &src1[off], &src2[off], &src3[off]);
        if (all_zero)
            digest &= ~(mask << wave);
#else
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u0 = (const bm::bit_block_t::bunion_t*)(&src0[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 = (const bm::bit_block_t::bunion_t*)(&src1[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 = (const bm::bit_block_t::bunion_t*)(&src2[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u3 = (const bm::bit_block_t::bunion_t*)(&src3[off]);
        bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)(&dst[off]);

        bm::id64_t acc = 0;
        unsigned j = 0;
        do
        {
            acc |= dst_u->w64[j + 0] &= src_u0->w64[j + 0] & src_u1->w64[j + 0] & src_u2->w64[j + 0] & src_u3->w64[j + 0];
            acc |= dst_u->w64[j + 1] &= src_u0->w64[j + 1] & src_u1->w64[j + 1] & src_u2->w64[j + 1] & src_u3->w64[j + 1];
            acc |= dst_u->w64[j + 2] &= src_u0->w64[j + 2] & src_u1->w64[j + 2] & src_u2->w64[j + 2] & src_u3->w64[j + 2];
            acc |= dst_u->w64[j + 3] &= src_u0->w64[j + 3] & src_u1->w64[j + 3] & src_u2->w64[j + 3] & src_u3->w64[j + 3];
            j += 4;
        } while (j < bm::set_block_digest_wave_size / 2);
        if (!acc) // all zero
            digest &= ~(mask << wave);
#endif
    } // while
    return digest;
}

/*!
   \brief digest based bit-block AND

   dst &= src1 AND src2

   \param dst - src/destination block.
   \param src1 - source block.
   \param src2 - source block.
   \param digest - known initial digest

   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_and_3way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2,
                              bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src1 && src2);
    BM_ASSERT(dst != src1 && dst != src2);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_AND_DIGEST_3WAY)
            bool all_zero = VECT_AND_DIGEST_3WAY(&dst[off], &src1[off], &src2[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 =
                                (const bm::bit_block_t::bunion_t*)(&src1[off]);
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 =
                                (const bm::bit_block_t::bunion_t*)(&src2[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u =
                                (bm::bit_block_t::bunion_t*)(&dst[off]);
            unsigned j = 0; bm::id64_t acc = 0;
            do
            {
                acc |= dst_u->w64[j] &= src_u1->w64[j] & src_u2->w64[j];
                acc |= dst_u->w64[j+1] &= src_u1->w64[j+1] & src_u2->w64[j+1];
                acc |= dst_u->w64[j+2] &= src_u1->w64[j+2] & src_u2->w64[j+2];
                acc |= dst_u->w64[j+3] &= src_u1->w64[j+3] & src_u2->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);

            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif

    } // while

    return digest;
}


/*!
   \brief digest based bit-block AND
 
   dst = src1 AND src2

   \param dst - destination block.
   \param src1 - source block.
   \param src2 - source block.
   \param digest - known initial digest
 
   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_and_2way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2,
                              bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src1 && src2);
    BM_ASSERT(dst != src1 && dst != src2);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_AND_DIGEST_2WAY)
            bool all_zero = VECT_AND_DIGEST_2WAY(&dst[off], &src1[off], &src2[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 =
                                (const bm::bit_block_t::bunion_t*)(&src1[off]);
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 =
                                (const bm::bit_block_t::bunion_t*)(&src2[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u =
                                (bm::bit_block_t::bunion_t*)(&dst[off]);
            unsigned j = 0; bm::id64_t acc = 0;
            do
            {
                acc |= dst_u->w64[j] = src_u1->w64[j] & src_u2->w64[j];
                acc |= dst_u->w64[j+1] = src_u1->w64[j+1] & src_u2->w64[j+1];
                acc |= dst_u->w64[j+2] = src_u1->w64[j+2] & src_u2->w64[j+2];
                acc |= dst_u->w64[j+3] = src_u1->w64[j+3] & src_u2->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);
        
            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif

    } // while
    
    return digest;
}

/*!
   \brief digest based bit-block AND (0 elements of digest will be zeroed)

   dst = src1 AND src2

   \param dst - destination block.
   \param src1 - source block.
   \param src2 - source block.
   \param digest - known initial digest

   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_init_and_2way(bm::word_t* BMRESTRICT dst,
                                   const bm::word_t* BMRESTRICT src1,
                                   const bm::word_t* BMRESTRICT src2,
                                   bm::id64_t digest) BMNOEXCEPT
{
    bm::id64_t d = digest;
    unsigned   off;
    for (unsigned i = 0; i < 64; ++i)
    {
        off = i * bm::set_block_digest_wave_size;
        if (digest & 1)
        {
        #if defined(VECT_AND_DIGEST_2WAY)
            bool all_zero = VECT_AND_DIGEST_2WAY(&dst[off], &src1[off], &src2[off]);
            if (all_zero)
                d &= ~(1ull << i);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 =
                                (const bm::bit_block_t::bunion_t*)(&src1[off]);
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 =
                                (const bm::bit_block_t::bunion_t*)(&src2[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u =
                                (bm::bit_block_t::bunion_t*)(&dst[off]);
            bm::id64_t acc = 0;
            unsigned j = 0;
            do
            {
                acc |= dst_u->w64[j] = src_u1->w64[j] & src_u2->w64[j];
                acc |= dst_u->w64[j+1] = src_u1->w64[j+1] & src_u2->w64[j+1];
                acc |= dst_u->w64[j+2] = src_u1->w64[j+2] & src_u2->w64[j+2];
                acc |= dst_u->w64[j+3] = src_u1->w64[j+3] & src_u2->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);

            if (!acc) // all zero
                d &= ~(1ull << i);
        #endif

        }
        else // init to all 0s
        {
        #if defined(VECT_BLOCK_SET_DIGEST)
            VECT_BLOCK_SET_DIGEST(&dst[off], 0u);
        #else
            for (; off < (i * bm::set_block_digest_wave_size) +
                               bm::set_block_digest_wave_size; off+=4)
                dst[off] = dst[off+1] = dst[off+2] = dst[off+3] = 0u;
        #endif
        }
        digest >>= 1ull;
    } // for
    return d;
}


/*!
   \brief digest based bit-block AND - OR

   dst =dst OR (src1 AND src2)

   \param dst - destination block.
   \param src1 - source block.
   \param src2 - source block.
   \param digest - known initial digest

   \return new digest (for the AND operation)

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_and_or_2way(bm::word_t* BMRESTRICT dst,
                                const bm::word_t* BMRESTRICT src1,
                                const bm::word_t* BMRESTRICT src2,
                                bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src1 && src2);
    BM_ASSERT(dst != src1 && dst != src2);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;

        #if defined(VECT_AND_OR_DIGEST_2WAY)
            bool all_zero =
                VECT_AND_OR_DIGEST_2WAY(&dst[off], &src1[off], &src2[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 =
                                (const bm::bit_block_t::bunion_t*)(&src1[off]);
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 =
                                (const bm::bit_block_t::bunion_t*)(&src2[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u =
                                (bm::bit_block_t::bunion_t*)(&dst[off]);

            bm::id64_t acc = 0;
            unsigned j = 0;
            do
            {
                acc |= dst_u->w64[j+0] |= src_u1->w64[j+0] & src_u2->w64[j+0];
                acc |= dst_u->w64[j+1] |= src_u1->w64[j+1] & src_u2->w64[j+1];
                acc |= dst_u->w64[j+2] |= src_u1->w64[j+2] & src_u2->w64[j+2];
                acc |= dst_u->w64[j+3] |= src_u1->w64[j+3] & src_u2->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);

            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif

        d = bm::bmi_bslr_u64(d); // d &= d - 1;
    } // while

    return digest;
}



/*!
   \brief Function ANDs two bitblocks and computes the bitcount. 
   Function does not analyse availability of source blocks.

   \param src1     - first bit block
   \param src2     - second bit block

   @ingroup bitfunc
*/
inline 
unsigned bit_block_and_count(const bm::word_t* BMRESTRICT src1,
                             const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count;
    const bm::word_t* src1_end = src1 + bm::set_block_size;
#ifdef BMVECTOPT
    count = VECT_BITCOUNT_AND(src1, src1_end, src2);
#else  
    count = 0;  
# ifdef BM64OPT
    const bm::id64_t* b1 = (bm::id64_t*) src1;
    const bm::id64_t* b1_end = (bm::id64_t*) src1_end;
    const bm::id64_t* b2 = (bm::id64_t*) src2;
    do
    {
        count += bm::bitcount64_4way(b1[0] & b2[0],
                                     b1[1] & b2[1],
                                     b1[2] & b2[2],
                                     b1[3] & b2[3]);
        b1 += 4; b2 += 4;
    } while (b1 < b1_end);
# else
    do
    {
        count +=
            bm::word_bitcount(src1[0] & src2[0]) +
            bm::word_bitcount(src1[1] & src2[1]) +
            bm::word_bitcount(src1[2] & src2[2]) +
            bm::word_bitcount(src1[3] & src2[3]);
        src1+=4; src2+=4;
    } while (src1 < src1_end);
# endif
#endif    
    return count;
}


/*!
   \brief Function ANDs two bitblocks and tests for any bit. 
   Function does not analyse availability of source blocks.

   \param src1     - first bit block
   \param src2     - second bit block

   @ingroup bitfunc
*/
inline 
unsigned bit_block_and_any(const bm::word_t* src1, 
                           const bm::word_t* src2) BMNOEXCEPT
{
    unsigned count = 0;
    const bm::word_t* src1_end = src1 + bm::set_block_size;
    do
    {
        count = (src1[0] & src2[0]) |
                (src1[1] & src2[1]) |
                (src1[2] & src2[2]) |
                (src1[3] & src2[3]);
        src1+=4; src2+=4;
    } while ((src1 < src1_end) && !count);
    return count;
}




/*!
   \brief Function XORs two bitblocks and computes the bitcount. 
   Function does not analyse availability of source blocks.

   \param src1     - first bit block
   \param src2     - second bit block

   @ingroup bitfunc
*/
inline 
unsigned bit_block_xor_count(const bm::word_t* BMRESTRICT src1,
                             const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count;
    const bm::word_t* BMRESTRICT src1_end = src1 + bm::set_block_size;
#ifdef BMVECTOPT
    count = VECT_BITCOUNT_XOR(src1, src1_end, src2);
#else  
    count = 0;  
# ifdef BM64OPT
    const bm::id64_t* b1 = (bm::id64_t*) src1;
    const bm::id64_t* b1_end = (bm::id64_t*) src1_end;
    const bm::id64_t* b2 = (bm::id64_t*) src2;
    do
    {
        count += bitcount64_4way(b1[0] ^ b2[0], 
                                 b1[1] ^ b2[1], 
                                 b1[2] ^ b2[2], 
                                 b1[3] ^ b2[3]);
        b1 += 4;
        b2 += 4;
    } while (b1 < b1_end);
# else
    do
    {
        count +=
            bm::word_bitcount(src1[0] ^ src2[0]) +
            bm::word_bitcount(src1[1] ^ src2[1]) +
            bm::word_bitcount(src1[2] ^ src2[2]) +
            bm::word_bitcount(src1[3] ^ src2[3]);
        src1+=4; src2+=4;
    } while (src1 < src1_end);
# endif
#endif
    return count;
}


/*!
   \brief Function XORs two bitblocks and and tests for any bit.
   Function does not analyse availability of source blocks.

   \param src1     - first bit block.
   \param src2     - second bit block.

   @ingroup bitfunc
*/
inline 
unsigned bit_block_xor_any(const bm::word_t* BMRESTRICT src1,
                           const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count = 0;
    const bm::word_t* BMRESTRICT src1_end = src1 + bm::set_block_size;
    do
    {
        count = (src1[0] ^ src2[0]) |
                (src1[1] ^ src2[1]) |
                (src1[2] ^ src2[2]) |
                (src1[3] ^ src2[3]);
        src1+=4; src2+=4;
    } while (!count && (src1 < src1_end));
    return count;
}

/*!
   \brief Function SUBs two bitblocks and computes the bitcount. 
   Function does not analyse availability of source blocks.

   \param src1     - first bit block.
   \param src2     - second bit block.

   @ingroup bitfunc
*/
inline 
unsigned bit_block_sub_count(const bm::word_t* BMRESTRICT src1,
                             const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count;
    const bm::word_t* BMRESTRICT src1_end = src1 + bm::set_block_size;
#ifdef BMVECTOPT
    count = VECT_BITCOUNT_SUB(src1, src1_end, src2);
#else  
    count = 0;  
# ifdef BM64OPT
    const bm::id64_t* b1 = (bm::id64_t*) src1;
    const bm::id64_t* b1_end = (bm::id64_t*) src1_end;
    const bm::id64_t* b2 = (bm::id64_t*) src2;
    do
    {
        count += bitcount64_4way(b1[0] & ~b2[0], 
                                 b1[1] & ~b2[1], 
                                 b1[2] & ~b2[2], 
                                 b1[3] & ~b2[3]);
        b1 += 4; b2 += 4;
    } while (b1 < b1_end);
# else
    do
    {
        count +=
            bm::word_bitcount(src1[0] & ~src2[0]) +
            bm::word_bitcount(src1[1] & ~src2[1]) +
            bm::word_bitcount(src1[2] & ~src2[2]) +
            bm::word_bitcount(src1[3] & ~src2[3]);
        src1+=4; src2+=4;
    } while (src1 < src1_end);
# endif
#endif
    return count;
}

/*!
   \brief Function SUBs two bitblocks and and tests for any bit.
   Function does not analyse availability of source blocks.

   \param src1     - first bit block.
   \param src2     - second bit block.

   @ingroup bitfunc
*/
inline 
unsigned bit_block_sub_any(const bm::word_t* BMRESTRICT src1,
                           const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count = 0;
    const bm::word_t* BMRESTRICT src1_end = src1 + bm::set_block_size;

    do
    {
        count = (src1[0] & ~src2[0]) |
                (src1[1] & ~src2[1]) |
                (src1[2] & ~src2[2]) |
                (src1[3] & ~src2[3]);
        src1+=4; src2+=4;
    } while ((src1 < src1_end) && (count == 0));
    return count;
}


/*!
   \brief Function ORs two bitblocks and computes the bitcount. 
   Function does not analyse availability of source blocks.

   \param src1     - first bit block
   \param src2     - second bit block.

   @ingroup bitfunc
*/
inline 
unsigned bit_block_or_count(const bm::word_t* src1, 
                            const bm::word_t* src2) BMNOEXCEPT
{
    unsigned count;
    const bm::word_t* src1_end = src1 + bm::set_block_size;
#ifdef BMVECTOPT
    count = VECT_BITCOUNT_OR(src1, src1_end, src2);
#else  
    count = 0;  
# ifdef BM64OPT
    const bm::id64_t* b1 = (bm::id64_t*) src1;
    const bm::id64_t* b1_end = (bm::id64_t*) src1_end;
    const bm::id64_t* b2 = (bm::id64_t*) src2;
    do
    {
        count += bitcount64_4way(b1[0] | b2[0], 
                                 b1[1] | b2[1], 
                                 b1[2] | b2[2], 
                                 b1[3] | b2[3]);
        b1 += 4;
        b2 += 4;
    } while (b1 < b1_end);
# else
    do
    {
        count +=
            bm::word_bitcount(src1[0] | src2[0]) +
            bm::word_bitcount(src1[1] | src2[1]) +
            bm::word_bitcount(src1[2] | src2[2]) +
            bm::word_bitcount(src1[3] | src2[3]);
        src1+=4; src2+=4;
    } while (src1 < src1_end);
# endif
#endif
    return count;
}

/*!
   \brief Function ORs two bitblocks and and tests for any bit.
   Function does not analyse availability of source blocks.

   \param src1     - first bit block.
   \param src2     - second bit block.

   @ingroup bitfunc
*/
inline 
unsigned bit_block_or_any(const bm::word_t* BMRESTRICT src1,
                          const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    unsigned count = 0;
    const bm::word_t* BMRESTRICT src1_end = src1 + bm::set_block_size;
    do
    {
        count = (src1[0] | src2[0]) |
                (src1[1] | src2[1]) |
                (src1[2] | src2[2]) |
                (src1[3] | src2[3]);

        src1+=4; src2+=4;
    } while (!count && (src1 < src1_end));
    return count;
}




/*!
   \brief bitblock AND operation. 

   \param dst - destination block.
   \param src - source block.

   \returns pointer on destination block. 
    If returned value  equal to src means that block mutation requested. 
    NULL is valid return value.

   @ingroup bitfunc
*/
inline
bm::word_t* bit_operation_and(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst || src);
    bm::word_t* ret = dst;
    if (IS_VALID_ADDR(dst))  // The destination block already exists
    {
        if (!IS_VALID_ADDR(src))
        {
            if (IS_EMPTY_BLOCK(src))
                return 0; //just clean the destination block
        }
        else
        {
            if (!bm::bit_block_and(dst, src))
                ret = 0;
        }
    }
    else // The destination block does not exist yet
    {
        if(!IS_VALID_ADDR(src))
        {
            if(IS_EMPTY_BLOCK(src)) 
                return 0; // One argument empty - all result is empty.
            // Src block is all ON, dst block remains as it is. nothing to do.
        }
        else // destination block does not exists, src - valid block
        {
            if (IS_FULL_BLOCK(dst))
                return const_cast<bm::word_t*>(src);
            // Dst block is all ZERO no combination required. Nothng to do.
        }
    }
    return ret;
}


/*!
   \brief Performs bitblock AND operation and calculates bitcount of the result. 

   \param src1     - first bit block.
   \param src2     - second bit block.

   \returns bitcount value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_and_count(const bm::word_t* BMRESTRICT src1,
                                 const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (IS_EMPTY_BLOCK(src1) || IS_EMPTY_BLOCK(src2))
        return 0;
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bit_block_and_count(src1, src2);
}

/*!
   \brief Performs bitblock AND operation test. 

   \param src1     - first bit block.
   \param src2     - second bit block.

   \returns non zero if there is any value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_and_any(const bm::word_t* BMRESTRICT src1,
                               const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (IS_EMPTY_BLOCK(src1) || IS_EMPTY_BLOCK(src2))
        return 0;
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;
    return bit_block_and_any(src1, src2);
}



/*!
   \brief Performs bitblock SUB operation and calculates bitcount of the result. 

   \param src1      - first bit block.
   \param src2      - second bit block

   \returns bitcount value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_sub_count(const bm::word_t* BMRESTRICT src1, 
                                 const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (src1 == src2)
        return 0;
    if (IS_EMPTY_BLOCK(src1))
        return 0;
    if (IS_EMPTY_BLOCK(src2)) // nothing to diff
    {
        if (IS_FULL_BLOCK(src1))
            return bm::gap_max_bits;
        return bm::bit_block_count(src1);
    }
    if (IS_FULL_BLOCK(src2))
        return 0;
    
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bm::bit_block_sub_count(src1, src2);
}


/*!
   \brief Performs inverted bitblock SUB operation and calculates 
          bitcount of the result. 

   \param src1      - first bit block.
   \param src2      - second bit block

   \returns bitcount value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_sub_count_inv(const bm::word_t* BMRESTRICT src1, 
                                     const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    return bit_operation_sub_count(src2, src1);
}


/*!
   \brief Performs bitblock test of SUB operation. 

   \param src1      - first bit block.
   \param src2      - second bit block

   \returns non zero value if there are any bits

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_sub_any(const bm::word_t* BMRESTRICT src1, 
                               const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (IS_EMPTY_BLOCK(src1))
        return 0;
    if (src1 == src2)
        return 0;

    if (IS_EMPTY_BLOCK(src2)) // nothing to diff
        return !bit_is_all_zero(src1);
    
    if (IS_FULL_BLOCK(src2))
        return 0;

    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bm::bit_block_sub_any(src1, src2);
}



/*!
   \brief Performs bitblock OR operation and calculates bitcount of the result. 

   \param src1     - first bit block.
   \param src2     - second bit block.

   \returns bitcount value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_or_count(const bm::word_t* BMRESTRICT src1,
                                const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (IS_FULL_BLOCK(src1) || IS_FULL_BLOCK(src2))
        return bm::gap_max_bits;

    if (IS_EMPTY_BLOCK(src1))
    {
        if (!IS_EMPTY_BLOCK(src2))
            return bm::bit_block_count(src2);
        else
            return 0; // both blocks are empty        
    }
    else
    {
        if (IS_EMPTY_BLOCK(src2))
            return bm::bit_block_count(src1);
    }
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bm::bit_block_or_count(src1, src2);
}

/*!
   \brief Performs bitblock OR operation test. 

   \param src1     - first bit block.
   \param src2     - second bit block.

   \returns non zero value if there are any bits

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_or_any(const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (IS_EMPTY_BLOCK(src1))
    {
        if (!IS_EMPTY_BLOCK(src2))
            return !bit_is_all_zero(src2);
        else
            return 0; // both blocks are empty        
    }
    else
    {
        if (IS_EMPTY_BLOCK(src2))
            return !bit_is_all_zero(src1);
    }
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bit_block_or_any(src1, src2);
}

/*!
   \brief Plain bitblock OR operation. 
   Function does not analyse availability of source and destination blocks.

   \param dst - destination block.
   \param src - source block.

   @ingroup bitfunc
*/
inline 
bool bit_block_or(bm::word_t* BMRESTRICT dst, 
                  const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef BMVECTOPT
    return VECT_OR_BLOCK(dst, src);
#else
    const bm::wordop_t* BMRESTRICT wrd_ptr = (wordop_t*)src;
    const bm::wordop_t* BMRESTRICT wrd_end = (wordop_t*)(src + bm::set_block_size);
    bm::wordop_t* BMRESTRICT dst_ptr = (wordop_t*)dst;

    bm::wordop_t acc = 0;
    const bm::wordop_t not_acc = acc = ~acc;

    do
    {
        acc &= (dst_ptr[0] |= wrd_ptr[0]);
        acc &= (dst_ptr[1] |= wrd_ptr[1]);
        acc &= (dst_ptr[2] |= wrd_ptr[2]);
        acc &= (dst_ptr[3] |= wrd_ptr[3]);

        dst_ptr+=4;wrd_ptr+=4;

    } while (wrd_ptr < wrd_end);
    return acc == not_acc;
#endif
}

/*!
   \brief 2 way (target := source1 | source2) bitblock OR operation.
   \param dst  - dest block [out]
   \param src1 - source 1
   \param src2 - source 2
 
   @return 1 if produced block of ALL ones
   @ingroup bitfunc
*/
inline
bool bit_block_or_2way(bm::word_t* BMRESTRICT dst,
                        const bm::word_t* BMRESTRICT src1,
                        const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
#ifdef BMVECTOPT
    return VECT_OR_BLOCK_2WAY(dst, src1, src2);
#else
    const bm::wordop_t* BMRESTRICT wrd_ptr1 = (wordop_t*)src1;
    const bm::wordop_t* BMRESTRICT wrd_end1 = (wordop_t*)(src1 + set_block_size);
    const bm::wordop_t* BMRESTRICT wrd_ptr2 = (wordop_t*)src2;
    bm::wordop_t* BMRESTRICT dst_ptr = (wordop_t*)dst;

    bm::wordop_t acc = 0;
    const bm::wordop_t not_acc = acc = ~acc;
    do
    {
        acc &= (dst_ptr[0] = wrd_ptr1[0] | wrd_ptr2[0]);
        acc &= (dst_ptr[1] = wrd_ptr1[1] | wrd_ptr2[1]);
        acc &= (dst_ptr[2] = wrd_ptr1[2] | wrd_ptr2[2]);
        acc &= (dst_ptr[3] = wrd_ptr1[3] | wrd_ptr2[3]);
        
        dst_ptr+=4; wrd_ptr1+=4; wrd_ptr2+=4;
        
    } while (wrd_ptr1 < wrd_end1);
    return acc == not_acc;
#endif
}


/*!
   \brief 2 way (target := source1 ^ source2) bitblock XOR operation.
   \param dst  - dest block [out]
   \param src1 - source 1
   \param src2 - source 2
 
   @return OR accumulator
   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_xor_2way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
#ifdef BMVECTOPT
    return VECT_XOR_BLOCK_2WAY(dst, src1, src2);
#else
    const bm::wordop_t* BMRESTRICT wrd_ptr1 = (wordop_t*)src1;
    const bm::wordop_t* BMRESTRICT wrd_end1 = (wordop_t*)(src1 + set_block_size);
    const bm::wordop_t* BMRESTRICT wrd_ptr2 = (wordop_t*)src2;
    bm::wordop_t* BMRESTRICT dst_ptr = (wordop_t*)dst;

    bm::wordop_t acc = 0;
    do
    {
        acc |= (dst_ptr[0] = wrd_ptr1[0] ^ wrd_ptr2[0]);
        acc |= (dst_ptr[1] = wrd_ptr1[1] ^ wrd_ptr2[1]);
        acc |= (dst_ptr[2] = wrd_ptr1[2] ^ wrd_ptr2[2]);
        acc |= (dst_ptr[3] = wrd_ptr1[3] ^ wrd_ptr2[3]);
        
        dst_ptr+=4; wrd_ptr1+=4; wrd_ptr2+=4;
        
    } while (wrd_ptr1 < wrd_end1);
    return acc;
#endif
}


/*!
   \brief 3 way (target | source1 | source2) bitblock OR operation.
   Function does not analyse availability of source and destination blocks.

   \param dst  - sst-dest block [in,out]
   \param src1 - source 1 
   \param src2 - source 2
 
   @return 1 if produced block of ALL ones

   @ingroup bitfunc
*/
inline
bool bit_block_or_3way(bm::word_t* BMRESTRICT dst,
                        const bm::word_t* BMRESTRICT src1,
                        const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
#ifdef BMVECTOPT
    return VECT_OR_BLOCK_3WAY(dst, src1, src2);
#else
    const bm::wordop_t* BMRESTRICT wrd_ptr1 = (wordop_t*)src1;
    const bm::wordop_t* BMRESTRICT wrd_end1 = (wordop_t*)(src1 + set_block_size);
    const bm::wordop_t* BMRESTRICT wrd_ptr2 = (wordop_t*)src2;
    bm::wordop_t* BMRESTRICT dst_ptr = (wordop_t*)dst;

    bm::wordop_t acc = 0;
    const bm::wordop_t not_acc = acc = ~acc;
    do
    {
        acc &= (dst_ptr[0] |= wrd_ptr1[0] | wrd_ptr2[0]);
        acc &= (dst_ptr[1] |= wrd_ptr1[1] | wrd_ptr2[1]);
        acc &= (dst_ptr[2] |= wrd_ptr1[2] | wrd_ptr2[2]);
        acc &= (dst_ptr[3] |= wrd_ptr1[3] | wrd_ptr2[3]);
        
        dst_ptr+=4; wrd_ptr1+=4;wrd_ptr2+=4;
        
    } while (wrd_ptr1 < wrd_end1);
    return acc == not_acc;
#endif
}


/*!
   \brief 5 way (target, source1, source2) bitblock OR operation.
   Function does not analyse availability of source and destination blocks.

   \param dst - destination block.
   \param src1 - source1, etc
   \param src2 - source1, etc
   \param src3 - source1, etc
   \param src4 - source1, etc
 
   @return 1 if produced block of ALL ones

   @ingroup bitfunc
*/
inline
bool bit_block_or_5way(bm::word_t* BMRESTRICT dst,
                        const bm::word_t* BMRESTRICT src1,
                        const bm::word_t* BMRESTRICT src2,
                        const bm::word_t* BMRESTRICT src3,
                        const bm::word_t* BMRESTRICT src4) BMNOEXCEPT
{
#ifdef BMVECTOPT
    return VECT_OR_BLOCK_5WAY(dst, src1, src2, src3, src4);
#else
    const bm::wordop_t* BMRESTRICT wrd_ptr1 = (wordop_t*)src1;
    const bm::wordop_t* BMRESTRICT wrd_end1 = (wordop_t*)(src1 + set_block_size);
    const bm::wordop_t* BMRESTRICT wrd_ptr2 = (wordop_t*)src2;
    const bm::wordop_t* BMRESTRICT wrd_ptr3 = (wordop_t*)src3;
    const bm::wordop_t* BMRESTRICT wrd_ptr4 = (wordop_t*)src4;
    bm::wordop_t* BMRESTRICT dst_ptr = (wordop_t*)dst;

    bm::wordop_t acc = 0;
    const bm::wordop_t not_acc = acc = ~acc;
    do
    {
        acc &= (dst_ptr[0] |= wrd_ptr1[0] | wrd_ptr2[0] | wrd_ptr3[0] | wrd_ptr4[0]);
        acc &= (dst_ptr[1] |= wrd_ptr1[1] | wrd_ptr2[1] | wrd_ptr3[1] | wrd_ptr4[1]);
        acc &= (dst_ptr[2] |= wrd_ptr1[2] | wrd_ptr2[2] | wrd_ptr3[2] | wrd_ptr4[2]);
        acc &= (dst_ptr[3] |= wrd_ptr1[3] | wrd_ptr2[3] | wrd_ptr3[3] | wrd_ptr4[3]);
        
        dst_ptr+=4;
        wrd_ptr1+=4;wrd_ptr2+=4;wrd_ptr3+=4;wrd_ptr4+=4;
        
    } while (wrd_ptr1 < wrd_end1);
    return acc == not_acc;
#endif
}




/*!
   \brief Block OR operation. Makes analysis if block is 0 or FULL. 

   \param dst - destination block.
   \param src - source block.

   \returns pointer on destination block. 
    If returned value  equal to src means that block mutation requested. 
    NULL is valid return value.

   @ingroup bitfunc
*/
inline 
bm::word_t* bit_operation_or(bm::word_t* BMRESTRICT dst, 
                             const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst || src);

    bm::word_t* ret = dst;

    if (IS_VALID_ADDR(dst)) // The destination block already exists
    {
        if (!IS_VALID_ADDR(src))
        {
            if (IS_FULL_BLOCK(src))
            {
                // if the source block is all set 
                // just set the destination block
                ::memset(dst, 0xFF, bm::set_block_size * sizeof(bm::word_t));
            }
        }
        else
        {
            // Regular operation OR on the whole block
            bm::bit_block_or(dst, src);
        }
    }
    else // The destination block does not exist yet
    {
        if (!IS_VALID_ADDR(src))
        {
            if (IS_FULL_BLOCK(src)) 
            {
                // The source block is all set, because dst does not exist
                // we can simply replace it.
                return const_cast<bm::word_t*>(FULL_BLOCK_FAKE_ADDR);
            }
        }
        else
        {
            if (dst == 0)
            {
                // The only case when we have to allocate the new block:
                // Src is all zero and Dst does not exist
                return const_cast<bm::word_t*>(src);
            }
        }
    }
    return ret;
}

/*!
   \brief Plain bitblock SUB (AND NOT) operation. 
   Function does not analyse availability of source and destination blocks.

   \param dst - destination block.
   \param src - source block.

   \return 0 if SUB operation did not produce anything (no 1s in the output)

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_sub(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
#ifdef BMVECTOPT
    bm::id64_t acc = VECT_SUB_BLOCK(dst, src);
    return acc;
#else
    unsigned arr_sz = bm::set_block_size / 2;

    const bm::bit_block_t::bunion_t* BMRESTRICT src_u = (const bm::bit_block_t::bunion_t*)src;
    bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)dst;

    bm::id64_t acc = 0;
    for (unsigned i = 0; i < arr_sz; i+=4)
    {
        acc |= (dst_u->w64[i] &= ~src_u->w64[i])     |
               (dst_u->w64[i+1] &= ~src_u->w64[i+1]) |
               (dst_u->w64[i+2] &= ~src_u->w64[i+2]) |
               (dst_u->w64[i+3] &= ~src_u->w64[i+3]);
    }
    return acc;
#endif
}

/*!
   \brief digest based bitblock SUB (AND NOT) operation

   \param dst - destination block.
   \param src - source block.
   \param digest - known digest of dst block
 
   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_sub(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src,
                         bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src);
    BM_ASSERT(dst != src);

    const bm::id64_t mask(1ull);

    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_SUB_DIGEST)
            bool all_zero = VECT_SUB_DIGEST(&dst[off], &src[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u = (const bm::bit_block_t::bunion_t*)(&src[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)(&dst[off]);

            bm::id64_t acc = 0;
            unsigned j = 0;
            do
            {
                acc |= dst_u->w64[j+0] &= ~src_u->w64[j+0];
                acc |= dst_u->w64[j+1] &= ~src_u->w64[j+1];
                acc |= dst_u->w64[j+2] &= ~src_u->w64[j+2];
                acc |= dst_u->w64[j+3] &= ~src_u->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);
        
            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif

    } // while
    
    return digest;
}

/*!
   \brief digest based bitblock SUB (AND NOT) operation (3 operand)

   \param dst - destination block.
   \param src1 - source block 1
   \param src2 - source block 2
   \param digest - known digest of dst block
 
   \return new digest

   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_sub_2way(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src1,
                         const bm::word_t* BMRESTRICT src2,
                         bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src1 && src2);
    BM_ASSERT(dst != src1 && dst != src2);

    const bm::id64_t mask(1ull);

    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

        #if defined(VECT_SUB_DIGEST_2WAY)
            bool all_zero = VECT_SUB_DIGEST_2WAY(&dst[off], &src1[off], &src2[off]);
            if (all_zero)
                digest &= ~(mask << wave);
        #else
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 =
                            (const bm::bit_block_t::bunion_t*)(&src1[off]);
            const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 =
                            (const bm::bit_block_t::bunion_t*)(&src2[off]);
            bm::bit_block_t::bunion_t* BMRESTRICT dst_u =
                            (bm::bit_block_t::bunion_t*)(&dst[off]);

            bm::id64_t acc = 0;
            unsigned j = 0;
            do
            {
                acc |= dst_u->w64[j+0] = src_u1->w64[j+0] & ~src_u2->w64[j+0];
                acc |= dst_u->w64[j+1] = src_u1->w64[j+1] & ~src_u2->w64[j+1];
                acc |= dst_u->w64[j+2] = src_u1->w64[j+2] & ~src_u2->w64[j+2];
                acc |= dst_u->w64[j+3] = src_u1->w64[j+3] & ~src_u2->w64[j+3];
                j+=4;
            } while (j < bm::set_block_digest_wave_size/2);
        
            if (!acc) // all zero
                digest &= ~(mask  << wave);
        #endif
    } // while
    
    return digest;
}


/*!
   \brief digest based bit-block SUB 5-way
   \return new digest
   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_sub_5way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src0,
                              const bm::word_t* BMRESTRICT src1,
                              const bm::word_t* BMRESTRICT src2,
                              const bm::word_t* BMRESTRICT src3,
                              bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src0 && src1 && src2 && src3);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

#if defined(VECT_SUB_DIGEST_5WAY)
        bool all_zero = VECT_SUB_DIGEST_5WAY(&dst[off], &src0[off], &src1[off], &src2[off], &src3[off]);
        if (all_zero)
            digest &= ~(mask << wave);
#else
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u0 = (const bm::bit_block_t::bunion_t*)(&src0[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 = (const bm::bit_block_t::bunion_t*)(&src1[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u2 = (const bm::bit_block_t::bunion_t*)(&src2[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u3 = (const bm::bit_block_t::bunion_t*)(&src3[off]);
        bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)(&dst[off]);

        bm::id64_t acc = 0;
        unsigned j = 0;
        do
        {
            acc |= dst_u->w64[j + 0] &= ~src_u0->w64[j + 0] & ~src_u1->w64[j + 0] & ~src_u2->w64[j + 0] & ~src_u3->w64[j + 0];
            acc |= dst_u->w64[j + 1] &= ~src_u0->w64[j + 1] & ~src_u1->w64[j + 1] & ~src_u2->w64[j + 1] & ~src_u3->w64[j + 1];
            acc |= dst_u->w64[j + 2] &= ~src_u0->w64[j + 2] & ~src_u1->w64[j + 2] & ~src_u2->w64[j + 2] & ~src_u3->w64[j + 2];
            acc |= dst_u->w64[j + 3] &= ~src_u0->w64[j + 3] & ~src_u1->w64[j + 3] & ~src_u2->w64[j + 3] & ~src_u3->w64[j + 3];
            j += 4;
        } while (j < bm::set_block_digest_wave_size / 2);

        if (!acc) // all zero
            digest &= ~(mask << wave);
#endif

    } // while

    return digest;
}

/*!
   \brief digest based bit-block SUB 3-way
   \return new digest
   @ingroup bitfunc
*/
inline
bm::id64_t bit_block_sub_3way(bm::word_t* BMRESTRICT dst,
                              const bm::word_t* BMRESTRICT src0,
                              const bm::word_t* BMRESTRICT src1,
                              bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src0 && src1);

    const bm::id64_t mask(1ull);
    bm::id64_t d = digest;
    while (d)
    {
        bm::id64_t t = bm::bmi_blsi_u64(d); // d & -d;

        unsigned wave = bm::word_bitcount64(t - 1);
        unsigned off = wave * bm::set_block_digest_wave_size;
        d = bm::bmi_bslr_u64(d); // d &= d - 1;

#if defined(VECT_SUB_DIGEST_3WAY)
        bool all_zero = VECT_SUB_DIGEST_3WAY(&dst[off], &src0[off], &src1[off]);
        if (all_zero)
            digest &= ~(mask << wave);
#else
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u0 = (const bm::bit_block_t::bunion_t*)(&src0[off]);
        const bm::bit_block_t::bunion_t* BMRESTRICT src_u1 = (const bm::bit_block_t::bunion_t*)(&src1[off]);
        bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)(&dst[off]);

        bm::id64_t acc = 0;
        unsigned j = 0;
        do
        {
            acc |= dst_u->w64[j + 0] &= ~src_u0->w64[j + 0] & ~src_u1->w64[j + 0];
            acc |= dst_u->w64[j + 1] &= ~src_u0->w64[j + 1] & ~src_u1->w64[j + 1];
            acc |= dst_u->w64[j + 2] &= ~src_u0->w64[j + 2] & ~src_u1->w64[j + 2];
            acc |= dst_u->w64[j + 3] &= ~src_u0->w64[j + 3] & ~src_u1->w64[j + 3];
            j += 4;
        } while (j < bm::set_block_digest_wave_size / 2);

        if (!acc) // all zero
            digest &= ~(mask << wave);
#endif
    } // while
    return digest;
}



/*!
   \brief bitblock SUB operation. 

   \param dst - destination block.
   \param src - source block.

   \returns pointer on destination block. 
    If returned value  equal to src means that block mutation requested. 
    NULL is valid return value.

   @ingroup bitfunc
*/
inline 
bm::word_t* bit_operation_sub(bm::word_t* BMRESTRICT dst, 
                              const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst || src);

    bm::word_t* ret = dst;
    if (IS_VALID_ADDR(dst))  //  The destination block already exists
    {
        if (!IS_VALID_ADDR(src))
        {
            if (IS_FULL_BLOCK(src))
            {
                // If the source block is all set
                // just clean the destination block
                return 0;
            }
        }
        else
        {
            auto any = bm::bit_block_sub(dst, src);
            if (!any)
                ret = 0;
        }
    }
    else // The destination block does not exist yet
    {
        if (!IS_VALID_ADDR(src))
        {
            if (IS_FULL_BLOCK(src)) 
            {
                // The source block is full
                return 0;
            }
        }
        else
        {
            if (IS_FULL_BLOCK(dst))
            {
                // The only case when we have to allocate the new block:
                // dst is all set and src exists
                return const_cast<bm::word_t*>(src);                  
            }
        }
    }
    return ret;
}


/*!
   \brief Plain bitblock XOR operation. 
   Function does not analyse availability of source and destination blocks.

   \param dst - destination block.
   \param src - source block.

   @ingroup bitfunc
*/
inline 
bm::id64_t bit_block_xor(bm::word_t* BMRESTRICT dst,
                         const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst);
    BM_ASSERT(src);
    BM_ASSERT(dst != src);

#ifdef BMVECTOPT
    bm::id64_t acc = VECT_XOR_BLOCK(dst, src);
#else
    unsigned arr_sz = bm::set_block_size / 2;

    const bm::bit_block_t::bunion_t* BMRESTRICT src_u = (const bm::bit_block_t::bunion_t*)src;
    bm::bit_block_t::bunion_t* BMRESTRICT dst_u = (bm::bit_block_t::bunion_t*)dst;

    bm::id64_t acc = 0;
    for (unsigned i = 0; i < arr_sz; i+=4)
    {
        acc |= dst_u->w64[i]   ^= src_u->w64[i];
        acc |= dst_u->w64[i+1] ^= src_u->w64[i+1];
        acc |= dst_u->w64[i+2] ^= src_u->w64[i+2];
        acc |= dst_u->w64[i+3] ^= src_u->w64[i+3];
    }
#endif
    return acc;
}

/*!
   \brief bitblock AND NOT with constant ~0 mask operation.

   \param dst - destination block.
   \param src - source block.

   @ingroup bitfunc
*/
inline
void bit_andnot_arr_ffmask(bm::word_t* BMRESTRICT dst,
                           const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    const bm::word_t* BMRESTRICT src_end = src + bm::set_block_size;
#ifdef BMVECTOPT
    VECT_ANDNOT_ARR_2_MASK(dst, src, src_end, ~0u);
#else
    bm::wordop_t* dst_ptr = (wordop_t*)dst;
    const bm::wordop_t* wrd_ptr = (wordop_t*) src;
    const bm::wordop_t* wrd_end = (wordop_t*) src_end;

    do
    {
        dst_ptr[0] = bm::all_bits_mask & ~wrd_ptr[0];
        dst_ptr[1] = bm::all_bits_mask & ~wrd_ptr[1];
        dst_ptr[2] = bm::all_bits_mask & ~wrd_ptr[2];
        dst_ptr[3] = bm::all_bits_mask & ~wrd_ptr[3];
        dst_ptr+=4; wrd_ptr+=4;
    } while (wrd_ptr < wrd_end);
#endif
}

/*!
   \brief bitblock XOR operation. 

   \param dst - destination block.
   \param src - source block.

   \returns pointer on destination block. 
    If returned value  equal to src means that block mutation requested. 
    NULL is valid return value.

   @ingroup bitfunc
*/
inline 
bm::word_t* bit_operation_xor(bm::word_t* BMRESTRICT dst, 
                              const bm::word_t* BMRESTRICT src) BMNOEXCEPT
{
    BM_ASSERT(dst || src);
    if (src == dst) return 0;  // XOR rule  

    bm::word_t* ret = dst;

    if (IS_VALID_ADDR(dst))  //  The destination block already exists
    {           
        if (!src) return dst;
        
        bit_block_xor(dst, src);
    }
    else // The destination block does not exist yet
    {
        if (!src) return dst;      // 1 xor 0 = 1

        // Here we have two cases:
        // if dest block is full or zero if zero we need to copy the source
        // otherwise XOR loop against 0xFF...
        //BM_ASSERT(dst == 0);
        return const_cast<bm::word_t*>(src);  // src is the final result               
    }
    return ret;
}

/*!
   \brief Performs bitblock XOR operation and calculates bitcount of the result. 

   \param src1 - bit block start ptr
   \param src2 - second bit block

   \returns bitcount value 

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_xor_count(const bm::word_t* BMRESTRICT src1,
                                 const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (src1 == src2)
        return 0;
    
    if (IS_EMPTY_BLOCK(src1) || IS_EMPTY_BLOCK(src2))
    {
        const bm::word_t* block = IS_EMPTY_BLOCK(src1) ? src2 : src1;
        if (IS_FULL_BLOCK(block))
            return bm::gap_max_bits;
        return bm::bit_block_count(block);
    }
    if (src1 == FULL_BLOCK_FAKE_ADDR)
        src1 = FULL_BLOCK_REAL_ADDR;
    if (src2 == FULL_BLOCK_FAKE_ADDR)
        src2 = FULL_BLOCK_REAL_ADDR;

    return bit_block_xor_count(src1, src2);
}

/*!
   \brief Performs bitblock XOR operation test. 

   \param src1 - bit block start ptr
   \param src2 - second bit block ptr

   \returns non zero value if there are bits

   @ingroup bitfunc
*/
inline 
bm::id_t bit_operation_xor_any(const bm::word_t* BMRESTRICT src1,
                               const bm::word_t* BMRESTRICT src2) BMNOEXCEPT
{
    if (src1 == src2)
        return 0;
    if (IS_EMPTY_BLOCK(src1) || IS_EMPTY_BLOCK(src2))
    {
        const bm::word_t* block = IS_EMPTY_BLOCK(src1) ? src2 : src1;
        return !bit_is_all_zero(block);
    }
    return bm::bit_block_xor_any(src1, src2);
}



/**
    \brief Inspects block for full zero words 

    \param blk - bit block pointer
    \param data_size - data size

    \return size of all non-zero words

    @ingroup bitfunc
*/
template<class T>
unsigned bit_count_nonzero_size(const T* blk, unsigned  data_size) BMNOEXCEPT
{
    BM_ASSERT(blk && data_size);
    unsigned count = 0;
    const T* blk_end = blk + data_size - 2;
    do
    {
        if (*blk == 0) // scan fwd to find 0 island length
        {
            const T* blk_j = blk + 1;
            for (; blk_j < blk_end; ++blk_j)
            {
                if (*blk_j != 0)
                    break;
            } // for
            blk = blk_j-1;
            count += (unsigned)sizeof(gap_word_t);
        }
        else
        {
            const T* blk_j = blk + 1; // scan fwd to find non-0 island len
            for ( ; blk_j < blk_end; ++blk_j)
            {
                if (*blk_j == 0)
                {
                    if (blk_j[1] | blk_j[2]) // look ahead, ignore short 0-run
                    {
                        ++blk_j; // skip zero word
                        continue;
                    }
                    break;
                }
            } // for
            count += unsigned(sizeof(gap_word_t));
            // count all bit-words now
            count += unsigned(blk_j - blk) * unsigned(sizeof(T));
            blk = blk_j;
        }
        ++blk;
    }
    while(blk < blk_end);
    return count + unsigned(2 * sizeof(T));
}


/**
    \brief Searches for the next 1 bit in the BIT block
    \param block - BIT buffer
    \param nbit - bit index to start checking from
    \param pos - [out] found value

    \return 0 if not found

    @ingroup bitfunc
*/
inline
unsigned bit_block_find(const bm::word_t* BMRESTRICT block,
                        unsigned nbit, unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(pos);
    
    unsigned nword  = unsigned(nbit >> bm::set_word_shift);
    unsigned bit_pos = (nbit & bm::set_word_mask);
    
    bm::word_t w = block[nword];
    w &= (1u << bit_pos);
    if (w)
    {
        *pos = nbit;
        return 1;
    }
    w = block[nword] >> bit_pos;
    w <<= bit_pos; // clear the trailing bits
    if (w)
    {
        bit_pos = bm::bit_scan_forward32(w); // trailing zeros
        *pos = unsigned(bit_pos + (nword * 8u * unsigned(sizeof(bm::word_t))));
        return 1;
    }
    
    for (unsigned i = nword+1; i < bm::set_block_size; ++i)
    {
        w = block[i];
        if (w)
        {
            bit_pos = bm::bit_scan_forward32(w); // trailing zeros
            *pos = unsigned(bit_pos + (i * 8u * unsigned(sizeof(bm::word_t))));
            return w;
        }
    } // for i
    return 0u;
}




/*!
    \brief BIT block find the last set bit (backward search)

    \param block - bit block buffer pointer
    \param last - index of the last 1 bit (out)
    \return true if found

    @ingroup bitfunc
*/
inline
unsigned bit_find_last(const bm::word_t* BMRESTRICT block,
                       unsigned* BMRESTRICT last) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(last);

    // TODO: SIMD version

    for (unsigned i = bm::set_block_size-1; true; --i)
    {
        bm::word_t w = block[i];
        if (w)
        {
            unsigned idx = bm::bit_scan_reverse(w);
            *last = unsigned(idx + (i * 8u * unsigned(sizeof(bm::word_t))));
            return w;
        }
        if (i == 0)
            break;
    } // for i
    return 0u;
}

/*!
    \brief BIT block find the first set bit

    \param block - bit block buffer pointer
    \param pos - index of the first 1 bit (out)
    \return 0 if not found

    @ingroup bitfunc
    @internal
*/
inline
bool bit_find_first(const bm::word_t* BMRESTRICT block,
                    unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(pos);

#ifdef VECT_BIT_FIND_FIRST
    return VECT_BIT_FIND_FIRST(block, 0, pos);
#else
    for (unsigned i = 0; i < bm::set_block_size; ++i)
    {
        bm::word_t w = block[i];
        if (w)
        {
            unsigned idx = bm::bit_scan_forward32(w); // trailing zeros
            *pos = unsigned(idx + (i * 8u * unsigned(sizeof(bm::word_t))));
            return w;
        }
    } // for i
    return false;
#endif
}

/*!
    \brief BIT block find the first set bit

    \param block - bit block buffer pointer
    \param first - index of the first 1 bit (out)
    \param digest - known digest of dst block

    \return 0 if not found

    @ingroup bitfunc
*/
inline
unsigned bit_find_first(const bm::word_t* BMRESTRICT block,
                        unsigned*         BMRESTRICT first,
                        bm::id64_t        digest) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(first);
    BM_ASSERT(digest);
    
    bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
    unsigned wave = bm::word_bitcount64(t - 1);
    unsigned i = wave * bm::set_block_digest_wave_size;

#ifdef VECT_BIT_FIND_FIRST
    return VECT_BIT_FIND_FIRST(block, i, first);
#else
    do
    {
        bm::id64_t w64 = block[i] | block[i+1];
        if (w64)
        {
            unsigned base = i * 8u * unsigned(sizeof(bm::word_t));
            if (bm::word_t w0 = block[i])
            {
                *first = bm::bit_scan_forward32(w0) + base;
                return w0;
            }
            BM_ASSERT(block[i+1]);
            return *first = bm::bit_scan_forward32(block[i+1]) + base + 32;
        }
        i+=2;
        w64 = block[i] | block[i+1];
        if (w64)
        {
            unsigned base = i * 8u * unsigned(sizeof(bm::word_t));
            if (bm::word_t w0 = block[i])
            {
                *first = bm::bit_scan_forward32(w0) + base;
                return w0;
            }
            BM_ASSERT(block[i+1]);
            return *first = bm::bit_scan_forward32(block[i+1]) + base + 32;
        }
        i+=2;
    } while (i < bm::set_block_size);
    return 0u;
#endif
}


/*!
    \brief BIT block find the first set bit if only 1 bit is set

    \param block - bit block buffer pointer
    \param first - index of the first 1 bit (out)
    \param digest - known digest of dst block

    \return 0 if not found

    @ingroup bitfunc
*/
inline
bool bit_find_first_if_1(const bm::word_t* BMRESTRICT block,
                         unsigned*         BMRESTRICT first,
                         bm::id64_t        digest) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(first);
    BM_ASSERT(bm::word_bitcount64(digest)==1);

    const bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
    const unsigned wave = bm::word_bitcount64(t - 1);
    const unsigned off = wave * bm::set_block_digest_wave_size;

#if defined(BMSSE42OPT) || defined(BMAVX2OPT) || defined(BMAVX512OPT)
    for (unsigned i = off; i < off + bm::set_block_digest_wave_size; i+=4)
    {
        __m128i wA = _mm_load_si128((__m128i*)&block[i]);
        if (_mm_test_all_zeros(wA, wA))
            continue;
        const unsigned cnt = i + 4;
        do
        {
            const bm::word_t w = block[i];
            switch (bm::word_bitcount(w))
            {
            case 0: break;
            case 1:
                *first = (i * 32) + bm::bit_scan_forward32(w);
                for (++i; i < cnt; ++i) // check the rest of the SSE lane
                    if (block[i])
                        return false;
                goto check_tail; // break out of switch-while
            default:
                return false;
            } // switch
        } while (++i < cnt);
        check_tail:
        for (; i < off + bm::set_block_digest_wave_size; i+=4)
        {
            wA = _mm_load_si128((__m128i*)&block[i]);
            if (!_mm_test_all_zeros(wA, wA)) // another != 0 found
                return false;
        } // for i
        return true;
    } // for i
#else
    for (auto i = off; i < off + bm::set_block_digest_wave_size; ++i)
    {
        if (auto w = block[i])
        {
            if (bm::word_bitcount(w) != 1)
                return false;
            const unsigned idx = (i * 32) + bm::bit_scan_forward32(w); // tzero
            for (++i; i < off + bm::set_block_digest_wave_size; ++i)
                if (block[i])
                    return false;
            *first = idx;
            return true;
        }
    } // for i
#endif
    return false;
}


/*!
    \brief BIT block find position for the rank

    \param block - bit block buffer pointer
    \param rank - rank to find (must be > 0)
    \param nbit_from - start bit position in block
    \param nbit_pos - (out)found position
 
    \return 0 if position with rank was found, or
              the remaining rank (rank - population count)

    @ingroup bitfunc
*/
template<typename SIZE_TYPE>
SIZE_TYPE bit_find_rank(const bm::word_t* const block,
                        SIZE_TYPE               rank,
                        unsigned                nbit_from,
                        unsigned&               nbit_pos) BMNOEXCEPT
{
    BM_ASSERT(block);
    BM_ASSERT(rank);
    
    unsigned nword  = nbit_from >> bm::set_word_shift;
    BM_ASSERT(nword < bm::set_block_size);

    unsigned pos = nbit_from;
    bm::id_t nbit = (nbit_from & bm::set_word_mask);
    
    if (nbit)
    {
        bm::id_t w = block[nword];
        w >>= nbit;
        unsigned bc = bm::word_bitcount(w);
        if (bc < rank) // skip this
        {
            rank -= bc; pos += unsigned(32u - nbit);
            ++nword;
        }
        else // target word
        {
            unsigned idx = bm::word_select32(w, unsigned(rank));
            nbit_pos = pos + idx;
            return 0;
        }
    }

    #if defined(BM64OPT) || defined(BM64_SSE4) || defined(BMAVX2OPT) || defined(BMAVX512OPT)
    {
        for (; nword < bm::set_block_size-1; nword+=2)
        {
            bm::id64_t w =
                (bm::id64_t(block[nword+1]) << 32) | bm::id64_t(block[nword]);
            bm::id_t bc = bm::word_bitcount64(w);
            if (bc >= rank) // target
            {
                unsigned idx = bm::word_select64(w, unsigned(rank));
                nbit_pos = pos + idx;
                return 0;
            }
            rank -= bc;
            pos += 64u;
        }
    }
    #endif

    for (; nword < bm::set_block_size; ++nword)
    {
        bm::id_t w = block[nword];
        bm::id_t bc = bm::word_bitcount(w);
        if (rank > bc)
        {
            rank -= bc; pos += 32u;
            continue;
        }
        unsigned idx = bm::word_select32(w, unsigned(rank));
        nbit_pos = pos + idx;
        return 0;
    } // for nword
    return rank;
}

/**
    \brief Find rank in block (GAP or BIT)
 
    \param block - bit block buffer pointer
    \param rank - rank to find (must be > 0)
    \param nbit_from - start bit position in block
    \param nbit_pos - found position
 
    \return 0 if position with rank was found, or
              the remaining rank (rank - population count)

    @internal
*/
template<typename SIZE_TYPE>
SIZE_TYPE block_find_rank(const bm::word_t* const block,
                          SIZE_TYPE               rank,
                          unsigned                nbit_from,
                          unsigned&               nbit_pos) BMNOEXCEPT
{
    if (BM_IS_GAP(block))
    {
        const bm::gap_word_t* const gap_block = BMGAP_PTR(block);
        rank = bm::gap_find_rank(gap_block, rank, nbit_from, nbit_pos);
    }
    else
    {
        rank = bm::bit_find_rank(block, rank, nbit_from, nbit_pos);
    }
    return rank;
}



/*!
    @brief Choose best representation for a bit-block
    @ingroup bitfunc 
*/
inline
bm::set_representation best_representation(unsigned bit_count,
                                           unsigned total_possible_bitcount,
                                           unsigned gap_count,
                                           unsigned block_size) BMNOEXCEPT
{
    unsigned arr_size = unsigned(sizeof(bm::gap_word_t) * bit_count + sizeof(bm::gap_word_t));
    unsigned gap_size = unsigned(sizeof(bm::gap_word_t) * gap_count + sizeof(bm::gap_word_t));
    unsigned inv_arr_size = unsigned(sizeof(bm::gap_word_t) * (total_possible_bitcount - bit_count) + sizeof(bm::gap_word_t));

    if ((gap_size < block_size) && (gap_size < arr_size) && (gap_size < inv_arr_size))
    {
        return bm::set_gap;
    }

    if (arr_size < inv_arr_size)
    {
        if ((arr_size < block_size) && (arr_size < gap_size))
        {
            return bm::set_array1;
        }
    }
    else
    {
        if ((inv_arr_size < block_size) && (inv_arr_size < gap_size))
        {
            return bm::set_array0;
        }
    }
    return bm::set_bitset;
}

/*!
    @brief Convert bit block into an array of ints corresponding to 1 bits
    @return destination size as a result of block decoding
    @ingroup bitfunc 
*/

/*!
    @brief Convert bit block into an array of ints corresponding to 1 bits
    @return destination size as a result of block decoding
    @ingroup bitfunc
*/
template<typename T>
unsigned bit_block_convert_to_arr(T* BMRESTRICT dest,
    const unsigned* BMRESTRICT src,
    bool inverted) BMNOEXCEPT
{
    bm::id64_t imask64 = inverted ? ~0ull : 0;
    T* BMRESTRICT pcurr = dest;
    for (unsigned bit_idx = 0; bit_idx < bm::gap_max_bits;
        src+=2, bit_idx += unsigned(sizeof(*src) * 8 * 2))
    {
        bm::id64_t w = 
            (bm::id64_t(src[0]) | (bm::id64_t(src[1]) << 32u)) ^ imask64;
        while (w)
        {
            bm::id64_t t = bmi_blsi_u64(w); // w & -w;
            *pcurr++ = (T)(bm::word_bitcount64(t - 1) + bit_idx);
            w = bmi_bslr_u64(w); // w &= w - 1;
        }
    } // for
    return (unsigned)(pcurr - dest);
}


/**
    \brief Checks all conditions and returns true if block consists of only 0 bits
    \param blk - Blocks's pointer
    \param deep_scan - flag to do full bit block verification (scan)
                       when deep scan is not requested result can be approximate
    \returns true if all bits are in the block are 0

    @internal
*/
inline
bool check_block_zero(const bm::word_t* blk, bool  deep_scan) BMNOEXCEPT
{
    if (!blk) return true;
    if (IS_FULL_BLOCK(blk)) return false;

    bool ret;
    if (BM_IS_GAP(blk))
        ret = gap_is_all_zero(BMGAP_PTR(blk));
    else
        ret = deep_scan ? bm::bit_is_all_zero(blk) : 0;
    return ret;
}


/**
    \brief Checks if block has only 1 bits
    \param blk - Block's pointer
    \param deep_scan - flag to do full bit block verification (scan)
                       when deep scan is not requested result can be approximate
    \return true if block consists of 1 bits.

    @internal
*/
inline
bool check_block_one(const bm::word_t* blk, bool deep_scan) BMNOEXCEPT
{
    if (blk == 0) return false;

    if (BM_IS_GAP(blk))
        return bm::gap_is_all_one(BMGAP_PTR(blk));

    if (IS_FULL_BLOCK(blk))
        return true;
    
    if (!deep_scan)
        return false; // block exists - presume it has 0 bits

    return bm::is_bits_one((wordop_t*)blk);
}



/*! @brief Calculates memory overhead for number of gap blocks sharing 
           the same memory allocation table (level lengths table).
    @ingroup gapfunc
*/
template<typename T> 
unsigned gap_overhead(const T* length, 
                      const T* length_end, 
                      const T* glevel_len) BMNOEXCEPT
{
    BM_ASSERT(length && length_end && glevel_len);

    unsigned overhead = 0;
    for (;length < length_end; ++length)
    {
        unsigned len = *length;
        int level = gap_calc_level(len, glevel_len);
        BM_ASSERT(level >= 0 && level < (int)bm::gap_levels);
        unsigned capacity = glevel_len[level];
        BM_ASSERT(capacity >= len);
        overhead += capacity - len;
    }
    return overhead;
}


/*! @brief Finds optimal gap blocks lengths.
    @param length - first element of GAP lengths array
    @param length_end - end of the GAP lengths array
    @param glevel_len - destination GAP lengths array
    @ingroup gapfunc
*/
template<typename T>
bool improve_gap_levels(const T* length,
                        const T* length_end,
                        T*       glevel_len) BMNOEXCEPT
{
    BM_ASSERT(length && length_end && glevel_len);

    size_t lsize = size_t(length_end - length);

    BM_ASSERT(lsize);
    
    gap_word_t max_len = 0;
    unsigned i;
    for (i = 0; i < lsize; ++i)
    {
        if (length[i] > max_len)
            max_len = length[i];
    }
    if (max_len < 5 || lsize <= bm::gap_levels)
    {
        glevel_len[0] = T(max_len + 4);
        for (i = 1; i < bm::gap_levels; ++i)
        {
            glevel_len[i] = bm::gap_max_buff_len;
        }
        return true;
    }

    glevel_len[bm::gap_levels-1] = T(max_len + 5);

    unsigned min_overhead = gap_overhead(length, length_end, glevel_len);
    bool is_improved = false;

    // main problem solving loop
    //
    for (i = bm::gap_levels-2; ; --i)
    {
        unsigned opt_len = 0;
        unsigned j;
        bool imp_flag = false;
        gap_word_t gap_saved_value = glevel_len[i];
        for (j = 0; j < lsize; ++j)
        {
            glevel_len[i] = T(length[j] + 4);
            unsigned ov = gap_overhead(length, length_end, glevel_len);
            if (ov <= min_overhead)
            {
                min_overhead = ov;                
                opt_len = length[j]+4;
                imp_flag = true;
            }
        }
        if (imp_flag) 
        {
            glevel_len[i] = (T)opt_len; // length[opt_idx]+4;
            is_improved = true;
        }
        else 
        {
            glevel_len[i] = gap_saved_value;
        }
        if (i == 0) 
            break;
    }
    
    // Remove duplicates
    //
    T val = *glevel_len;
    T* gp = glevel_len;
    T* res = glevel_len;
    for (i = 0; i < bm::gap_levels; ++i)
    {
        if (val != *gp)
        {
            val = *gp;
            *++res = val;
        }
        ++gp;
    }

    // Filling the "unused" part with max. possible value
    while (++res < (glevel_len + bm::gap_levels)) 
    {
        *res = bm::gap_max_buff_len;
    }

    return is_improved;

}

/*!
   \brief Find first bit which is different between two blocks (GAP or bit)
   \param blk - block 1
   \param arg_blk - block 2
   \param pos - out - position of difference (undefined if blocks are equal)
   \return  true if difference was found
   @internal
*/
inline
bool block_find_first_diff(const bm::word_t* BMRESTRICT blk,
                           const bm::word_t* BMRESTRICT arg_blk,
                           unsigned* BMRESTRICT pos) BMNOEXCEPT
{
    // If one block is zero we check if the other one has at least
    // one bit ON

    if (!blk || !arg_blk)
    {
        const bm::word_t* pblk; bool is_gap;
        if (blk)
        {
            pblk = blk;
            is_gap = BM_IS_GAP(blk);
        }
        else
        {
            pblk = arg_blk;
            is_gap = BM_IS_GAP(arg_blk);
        }

        if (is_gap)
        {
            unsigned found = bm::gap_find_first(BMGAP_PTR(pblk), pos);
            if (found)
                return true;
        }
        else
        {
            unsigned found = bm::bit_find_first(pblk, pos);
            if (found)
                return true;
        }
        return false;
    }

    bool arg_gap = BM_IS_GAP(arg_blk);
    bool gap = BM_IS_GAP(blk);

    if (arg_gap != gap)
    {
        //BM_DECLARE_TEMP_BLOCK(temp_blk)
        bm::bit_block_t temp_blk;
        bm::word_t* blk1; bm::word_t* blk2;

        if (gap)
        {
            bm::gap_convert_to_bitset((bm::word_t*)temp_blk,
                                    BMGAP_PTR(blk));
            blk1 = (bm::word_t*)temp_blk;
            blk2 = (bm::word_t*)arg_blk;
        }
        else
        {
            bm::gap_convert_to_bitset((bm::word_t*)temp_blk,
                                      BMGAP_PTR(arg_blk));
            blk1 = (bm::word_t*)blk;
            blk2 = (bm::word_t*)temp_blk;
        }
        bool found = bm::bit_find_first_diff(blk1, blk2, pos);
        if (found)
            return true;
    }
    else
    {
        if (gap)
        {
            bool found =
                bm::gap_find_first_diff(BMGAP_PTR(blk),
                                        BMGAP_PTR(arg_blk), pos);
            if (found)
                return true;
        }
        else
        {
            bool found = bm::bit_find_first_diff(blk, arg_blk, pos);
            if (found)
                return true;
        }
    }
    return false;
}




/**
    Bit-block get adapter, takes bitblock and represents it as a 
    get_32() accessor function
    \internal
*/
class bitblock_get_adapter
{
public:
    bitblock_get_adapter(const bm::word_t* bit_block) : b_(bit_block) {}
    
    BMFORCEINLINE
    bm::word_t get_32() BMNOEXCEPT { return *b_++; }
private:
    const bm::word_t*  b_;
};


/**
    Bit-block store adapter, takes bitblock and saves results into it
    \internal
*/
class bitblock_store_adapter
{
public:
    bitblock_store_adapter(bm::word_t* bit_block) : b_(bit_block) {}
    BMFORCEINLINE
    void push_back(bm::word_t w) { *b_++ = w; }
private:
    bm::word_t* b_;
};

/**
    Bit-block sum adapter, takes values and sums it
    /internal
*/
class bitblock_sum_adapter
{
public:
    bitblock_sum_adapter() : sum_(0) {}
    BMFORCEINLINE
    void push_back(bm::word_t w) BMNOEXCEPT { this->sum_+= w; }
    /// Get accumulated sum
    bm::word_t sum() const BMNOEXCEPT { return this->sum_; }
private:
    bm::word_t sum_;
};

/**
    Adapter to get words from a range stream 
    (see range serialized bit-block)
    \internal
*/
template<class DEC> class decoder_range_adapter
{
public: 
    decoder_range_adapter(DEC& dec, unsigned from_idx, unsigned to_idx)
    : decoder_(dec),
      from_(from_idx),
      to_(to_idx),
      cnt_(0)
    {}

    bm::word_t get_32() BMNOEXCEPT
    {
        if (cnt_ < from_ || cnt_ > to_)
        {    
            ++cnt_; return 0;
        }
        ++cnt_;
        return decoder_.get_32();
    }

private:
    DEC&     decoder_;
    unsigned from_;
    unsigned to_;
    unsigned cnt_;
};


/*!
    Abstract recombination algorithm for two bit-blocks
    Bit blocks can come as dserialization decoders or bit-streams
*/
template<class It1, class It2, class BinaryOp, class Encoder>
void bit_recomb(It1& it1, It2& it2, 
                BinaryOp& op, 
                Encoder& enc, 
                unsigned block_size = bm::set_block_size) BMNOEXCEPT
{
    for (unsigned i = 0; i < block_size; ++i)
    {
        bm::word_t w1 = it1.get_32();
        bm::word_t w2 = it2.get_32();
        bm::word_t w = op(w1, w2);
        enc.push_back( w );
    } // for
}

/// Bit AND functor
template<typename W> struct bit_AND
{
    W operator()(W w1, W w2) BMNOEXCEPT { return w1 & w2; }
};

/// Bit OR functor
template<typename W> struct bit_OR
{
    W operator()(W w1, W w2) BMNOEXCEPT { return w1 | w2; }
};

/// Bit SUB functor
template<typename W> struct bit_SUB
{
     W operator()(W w1, W w2) BMNOEXCEPT { return w1 & ~w2; }
};

/// Bit XOR functor
template<typename W> struct bit_XOR
{
     W operator()(W w1, W w2) BMNOEXCEPT { return w1 ^ w2; }
};

/// Bit ASSIGN functor
template<typename W> struct bit_ASSIGN
{
     W operator()(W, W w2) BMNOEXCEPT { return w2; }
};

/// Bit COUNT functor
template<typename W> struct bit_COUNT
{
    W operator()(W w1, W w2) BMNOEXCEPT
    {
        w1 = 0; w1 += bm::word_bitcount(w2);
        return w1;
    }
};

/// Bit COUNT AND functor
template<typename W> struct bit_COUNT_AND
{
    W operator()(W w1, W w2) BMNOEXCEPT { return bm::word_bitcount(w1 & w2); }
};

/// Bit COUNT XOR functor
template<typename W> struct bit_COUNT_XOR
{
    W operator()(W w1, W w2) BMNOEXCEPT { return bm::word_bitcount(w1 ^ w2); }
};

/// Bit COUNT OR functor
template<typename W> struct bit_COUNT_OR
{
    W operator()(W w1, W w2) BMNOEXCEPT { return bm::word_bitcount(w1 | w2); }
};


/// Bit COUNT SUB AB functor
template<typename W> struct bit_COUNT_SUB_AB
{
    W operator()(W w1, W w2) BMNOEXCEPT { return bm::word_bitcount(w1 & (~w2)); }
};

/// Bit SUB BA functor
template<typename W> struct bit_COUNT_SUB_BA
{
    W operator()(W w1, W w2) BMNOEXCEPT { return bm::word_bitcount(w2 & (~w1)); }
};

/// Bit COUNT A functor
template<typename W> struct bit_COUNT_A
{
    W operator()(W w1, W ) BMNOEXCEPT { return bm::word_bitcount(w1); }
};

/// Bit COUNT B functor
template<typename W> struct bit_COUNT_B
{
    W operator()(W, W w2) BMNOEXCEPT { return bm::word_bitcount(w2); }
};

typedef 
void (*gap_operation_to_bitset_func_type)(unsigned*, 
                                          const gap_word_t*);

typedef 
gap_word_t* (*gap_operation_func_type)(const gap_word_t* BMRESTRICT,
                                       const gap_word_t* BMRESTRICT,
                                       gap_word_t*       BMRESTRICT,
                                       unsigned& );

typedef
bm::id_t (*bit_operation_count_func_type)(const bm::word_t* BMRESTRICT,
                                          const bm::word_t* BMRESTRICT);


template<bool T> 
struct operation_functions
{
    static 
        gap_operation_to_bitset_func_type gap2bit_table_[bm::set_END];
    static 
        gap_operation_func_type gapop_table_[bm::set_END];
    static
        bit_operation_count_func_type bit_op_count_table_[bm::set_END];

    static
    gap_operation_to_bitset_func_type gap_op_to_bit(unsigned i)
    {
        return gap2bit_table_[i];
    }

    static
    gap_operation_func_type gap_operation(unsigned i)
    {
        return gapop_table_[i];
    }

    static
    bit_operation_count_func_type bit_operation_count(unsigned i)
    {
        return bit_op_count_table_[i];
    }
};

template<bool T>
gap_operation_to_bitset_func_type 
operation_functions<T>::gap2bit_table_[bm::set_END] = {
    &gap_and_to_bitset<bm::gap_word_t>,    // set_AND
    &gap_add_to_bitset<bm::gap_word_t>,    // set_OR
    &gap_sub_to_bitset<bm::gap_word_t>,    // set_SUB
    &gap_xor_to_bitset<bm::gap_word_t>,    // set_XOR
    0
};

template<bool T>
gap_operation_func_type 
operation_functions<T>::gapop_table_[bm::set_END] = {
    &gap_operation_and,    // set_AND
    &gap_operation_or,     // set_OR
    &gap_operation_sub,    // set_SUB
    &gap_operation_xor,    // set_XOR
    0
};


template<bool T>
bit_operation_count_func_type 
operation_functions<T>::bit_op_count_table_[bm::set_END] = {
    0,                            // set_AND
    0,                            // set_OR
    0,                            // set_SUB
    0,                            // set_XOR
    0,                            // set_ASSIGN
    0,                            // set_COUNT
    &bit_operation_and_count,     // set_COUNT_AND
    &bit_operation_xor_count,     // set_COUNT_XOR
    &bit_operation_or_count,      // set_COUNT_OR
    &bit_operation_sub_count,     // set_COUNT_SUB_AB
    &bit_operation_sub_count_inv, // set_COUNT_SUB_BA
    0,                            // set_COUNT_A
    0,                            // set_COUNT_B
};

/**
    Size of bit decode wave in words
    @internal
 */
const unsigned short set_bitscan_wave_size = 4;
/*!
    \brief Unpacks word wave (Nx 32-bit words)
    \param w_ptr - pointer on wave start
    \param bits - pointer on the result array
    \return number of bits in the list

    @ingroup bitfunc
    @internal
*/
inline
unsigned short
bitscan_wave(const bm::word_t* BMRESTRICT w_ptr,
             unsigned char* BMRESTRICT bits) BMNOEXCEPT
{
    bm::word_t w0, w1;
    unsigned int cnt0;

    w0 = w_ptr[0]; w1 = w_ptr[1];

#if defined(BMAVX512OPT) || defined(BMAVX2OPT) || defined(BM64OPT) || defined(BM64_SSE4)
    // combine into 64-bit word and scan (when HW popcnt64 is available)
    bm::id64_t w = (bm::id64_t(w1) << 32) | w0;
    cnt0 = bm::bitscan_popcnt64(w, bits);

    w0 = w_ptr[2]; w1 = w_ptr[3];
    w = (bm::id64_t(w1) << 32) | w0;
    cnt0 += bm::bitscan_popcnt64(w, bits + cnt0, 64);
#else
    #if (defined(__arm__) || defined(__aarch64__))
        cnt0 = bm::bitscan_bsf(w0, bits, (unsigned short)0);
        cnt0 += bm::bitscan_bsf(w1, bits + cnt0, (unsigned short)32);

        w0 = w_ptr[2]; w1 = w_ptr[3];
        cnt0 += bm::bitscan_bsf(w0, bits + cnt0, (unsigned short)64);
        cnt0 += bm::bitscan_bsf(w1, bits + cnt0, (unsigned short)(64+32));        
    #else    
        // decode wave as two 32-bit bitscan decodes
        cnt0 = bm::bitscan_popcnt(w0, bits);
        cnt0 += bm::bitscan_popcnt(w1, bits + cnt0, 32);

        w0 = w_ptr[2]; w1 = w_ptr[3];
        cnt0 += bm::bitscan_popcnt(w0, bits + cnt0, 64);
        cnt0 += bm::bitscan_popcnt(w1, bits + cnt0, 64+32);    
    #endif
#endif
    return static_cast<unsigned short>(cnt0);
}

#if defined (BM64_SSE4) || defined(BM64_AVX2) || defined(BM64_AVX512)
/**
    bit index to word gather-scatter algorithm (SIMD)
    @ingroup bitfunc
    @internal
*/
inline
void bit_block_gather_scatter(unsigned* BMRESTRICT arr,
                              const bm::word_t* BMRESTRICT blk,
                              const unsigned* BMRESTRICT idx,
                              unsigned size, unsigned start,
                              unsigned bit_idx) BMNOEXCEPT
{
typedef unsigned TRGW;
typedef unsigned IDX;
#if defined(BM64_SSE4)
    // TODO: provide 16 and 64-bit optimized implementations
    // optimized for unsigned
    if constexpr (sizeof(TRGW)==4 && sizeof(IDX)==4)
    {
        sse4_bit_block_gather_scatter(arr, blk, idx, size, start, bit_idx);
        return;
    }
#elif defined(BM64_AVX2) || defined(BM64_AVX512)
    if constexpr (sizeof(TRGW)==4 && sizeof(IDX)==4)
    {
        avx2_bit_block_gather_scatter(arr, blk, idx, size, start, bit_idx);
        return;
    }
#else
    BM_ASSERT(0);
#endif
}
#endif

/**
    bit index to word gather-scatter algorithm
    @ingroup bitfunc
    @internal
*/
template<typename TRGW, typename IDX, typename SZ>
void bit_block_gather_scatter(TRGW* BMRESTRICT arr,
                              const bm::word_t* BMRESTRICT blk,
                              const IDX* BMRESTRICT idx,
                              SZ size, SZ start, unsigned bit_idx) BMNOEXCEPT
{
    // TODO: SIMD for 64-bit index sizes and 64-bit target value size
    //
    TRGW mask1 = 1;
    const SZ len = (size - start);
    const SZ len_unr = len - (len % 2);
    SZ k = 0;
    for (; k < len_unr; k+=2)
    {
        const SZ base = start + k;
        const unsigned nbitA = unsigned(idx[base] & bm::set_block_mask);
        arr[base]  |= (TRGW(bool(blk[nbitA >> bm::set_word_shift] &
                       (mask1 << (nbitA & bm::set_word_mask)))) << bit_idx);
        const unsigned nbitB = unsigned(idx[base + 1] & bm::set_block_mask);
        arr[base+1] |= (TRGW(bool(blk[nbitB >> bm::set_word_shift] &
                        (mask1 << (nbitB & bm::set_word_mask)))) << bit_idx);
    } // for k
    for (; k < len; ++k)
    {
        unsigned nbit = unsigned(idx[start + k] & bm::set_block_mask);
        arr[start + k] |= (TRGW(bool(blk[nbit >> bm::set_word_shift] &
                         (mask1 << (nbit & bm::set_word_mask)))) << bit_idx);
    } // for k
}

/**
    @brief block boundaries look ahead U32
 
    @param idx - array to look into
    @param size - array size
    @param nb - block number to look ahead
    @param start - start offset in idx
 
    @return block boundary offset end (no more match at the returned offset)
 
    @internal
*/
inline
bm::id64_t idx_arr_block_lookup_u64(const bm::id64_t* idx,
                bm::id64_t size, bm::id64_t nb, bm::id64_t start) BMNOEXCEPT
{
    BM_ASSERT(idx);
    BM_ASSERT(start < size);

    // TODO: SIMD for 64-bit index vector
    for (;(start < size) &&
          (nb == (idx[start] >> bm::set_block_shift)); ++start)
    {}
    return start;
}

/**
    @brief block boundaries look ahead U32
 
    @param idx - array to look into
    @param size - array size
    @param nb - block number to look ahead
    @param start - start offset in idx
 
    @return block boundary offset end (no more match at the returned offset)
 
    @internal
*/
inline
unsigned idx_arr_block_lookup_u32(const unsigned* idx,
                unsigned size, unsigned nb, unsigned start) BMNOEXCEPT
{
    BM_ASSERT(idx);
    BM_ASSERT(start < size);
    
#if defined(VECT_ARR_BLOCK_LOOKUP)
    return VECT_ARR_BLOCK_LOOKUP(idx, size, nb, start);
#else
    for (;(start < size) &&
          (nb == unsigned(idx[start] >> bm::set_block_shift)); ++start)
    {}
    return start;
#endif
}

// --------------------------------------------------------------


/**
    @brief set bits in a bit-block using global index
 
    @param idx - array to look into
    @param block - block pointer to set bits
    @param start - index array start
    @param stop  - index array stop in a range [start..stop)

    @return block boundary offset end (no more match at the returned offset)
 
    @internal
    @ingroup bitfunc
*/
inline
void set_block_bits_u64(bm::word_t* BMRESTRICT block,
                        const bm::id64_t* BMRESTRICT idx,
                        bm::id64_t start, bm::id64_t stop) BMNOEXCEPT
{
    // TODO: SIMD for 64-bit mode
    for (bm::id64_t i = start; i < stop; ++i)
    {
        bm::id64_t n = idx[i];
        unsigned nbit = unsigned(n & bm::set_block_mask);
        unsigned nword  = nbit >> bm::set_word_shift;
        nbit &= bm::set_word_mask;
        block[nword] |= (1u << nbit);
    } // for i
}


/**
    @brief set bits in a bit-block using global index
 
    @param idx - array to look into
    @param block - block pointer to set bits
    @param start - index array start
    @param stop  - index array stop in a range [start..stop)

    @return block boundary offset end (no more match at the returned offset)
 
    @internal
    @ingroup bitfunc
*/
inline
void set_block_bits_u32(bm::word_t* BMRESTRICT block,
                        const unsigned* BMRESTRICT idx,
                        unsigned start, unsigned stop ) BMNOEXCEPT
{
    BM_ASSERT(start < stop);
#if defined(VECT_SET_BLOCK_BITS)
    VECT_SET_BLOCK_BITS(block, idx, start, stop);
#else
    do
    {
        unsigned n = idx[start++];
        unsigned nbit = unsigned(n & bm::set_block_mask);
        unsigned nword  = nbit >> bm::set_word_shift;
        nbit &= bm::set_word_mask;
        block[nword] |= (1u << nbit);
    } while (start < stop);
#endif
}



// --------------------------------------------------------------

/**
    @brief array range detector
    @internal
*/
inline
bool block_ptr_array_range(bm::word_t** arr,
                           unsigned& left, unsigned& right) BMNOEXCEPT
{
    BM_ASSERT(arr);
    
    unsigned i, j;
    for (i = 0; i < bm::set_sub_array_size; ++i)
    {
        if (arr[i])
        {
            left = i;
            break;
        }
    }
    if (i == bm::set_sub_array_size)
    {
        left = right = 0;
        return false; // nothing here
    }
    for (j = bm::set_sub_array_size-1; j != i; --j)
        if (arr[j])
            break;
    right = j;
    return true;
}

// --------------------------------------------------------------

/**
    Linear lower bound search in unsigned array
    @internal
*/
inline
unsigned lower_bound_linear_u32(const unsigned* arr,  unsigned target,
                                unsigned        from, unsigned to) BMNOEXCEPT
{
    BM_ASSERT(arr);
    BM_ASSERT(from <= to);
    
#if defined(VECT_LOWER_BOUND_SCAN_U32)
    return VECT_LOWER_BOUND_SCAN_U32(arr, target, from, to);
#else
    for (; from <= to; ++from)
    {
        if (arr[from] >= target)
            break;
    }
    return from;
#endif
}

/**
    Linear lower bound search in unsigned LONG array
    @internal
*/
inline
unsigned lower_bound_linear_u64(const unsigned long long* arr,
                                unsigned long long target,
                                unsigned from, unsigned to) BMNOEXCEPT
{
    BM_ASSERT(arr);
    BM_ASSERT(from <= to);

    // TODO: implement vectorized scan on u64 ints
    for (; from <= to; ++from)
    {
        if (arr[from] >= target)
            break;
    }
    return from;
}



// --------------------------------------------------------------

/**
    Hybrid, binary-linear lower bound search in unsigned array
    @internal
*/
inline
unsigned lower_bound_u32(const unsigned* arr,  unsigned target,
                         unsigned        from, unsigned to) BMNOEXCEPT
{
    BM_ASSERT(arr);
    BM_ASSERT(from <= to);
    const unsigned linear_cutoff = 32;
    
    unsigned l = from; unsigned r = to;
    unsigned dist = r - l;
    if (dist < linear_cutoff)
    {
        return bm::lower_bound_linear_u32(arr, target, l, r);
    }

    while (l <= r)
    {
        unsigned mid = (r-l)/2+l;
        if (arr[mid] < target)
            l = mid+1;
        else
            r = mid-1;
        dist = r - l;
        if (dist < linear_cutoff)
        {
            return bm::lower_bound_linear_u32(arr, target, l, r);
        }
    }
    return l;
}

/**
    Hybrid, binary-linear lower bound search in unsigned LONG array
    @internal
*/
inline
unsigned lower_bound_u64(const unsigned long long* arr,
                         unsigned long long target,
                         unsigned from, unsigned to) BMNOEXCEPT
{
    BM_ASSERT(arr);
    BM_ASSERT(from <= to);
    const unsigned linear_cutoff = 32;

    unsigned l = from; unsigned r = to;
    unsigned dist = r - l;
    if (dist < linear_cutoff)
    {
        return bm::lower_bound_linear_u64(arr, target, l, r);
    }

    while (l <= r)
    {
        unsigned mid = (r - l) / 2 + l;
        if (arr[mid] < target)
            l = mid + 1;
        else
            r = mid - 1;
        dist = r - l;
        if (dist < linear_cutoff)
        {
            return bm::lower_bound_linear_u64(arr, target, l, r);
        }
    }
    return l;
}

/**
    Scan search for pointer value in unordered array
    @return found flag and  index
    @internal
 */
inline
bool find_ptr(const void* const * p_arr, size_t arr_size,
              const void* ptr, size_t *idx) BMNOEXCEPT
{
    // TODO: SIMD?
    for (size_t i = 0; i < arr_size; ++i)
        if (ptr == p_arr[i])
        {
            *idx = i; return true;
        }
    return false;
}


/**
    calculate bvector<> global bit-index from block-local coords
    @return bit index in linear bit-vector coordinates
    @internal
*/
#ifdef BM64ADDR
inline
bm::id64_t block_to_global_index(unsigned i, unsigned j,
                                 unsigned block_idx) BMNOEXCEPT
{
    bm::id64_t base_idx = bm::id64_t(i) * bm::set_sub_array_size * bm::gap_max_bits;
    base_idx += j * bm::gap_max_bits;
    return block_idx + base_idx;
}
#else
inline
bm::id_t block_to_global_index(unsigned i, unsigned j,
                               unsigned block_idx) BMNOEXCEPT
{
    unsigned base_idx = i * bm::set_sub_array_size * bm::gap_max_bits;
    base_idx += j * bm::gap_max_bits;
    return block_idx + base_idx;
}
#endif

/**
    Calculate minimal delta between elements of sorted array
    @internal
 */
inline
unsigned min_delta_u32(const unsigned* arr, size_t arr_size)
{
    if (arr_size < 2)
        return 0;

    unsigned md = arr[1] - arr[0]; // initial delta value
    for (size_t i = 1; i < arr_size; ++i)
    {
        unsigned prev = arr[i-1];
        unsigned curr = arr[i];
        BM_ASSERT(prev <= curr - 1);
        unsigned delta = curr - prev;
        if (delta < md)
        {
            md = delta;
        }
    } // for
    return md;
}

/**
    Recalculate the array to decrement delta as
    arr[i] = arr[i] - delta + 1
    so that array remains monotonically growing (fit
    for interpolative compression)

    @internal
 */
inline
void min_delta_apply(unsigned* arr, size_t arr_size, unsigned delta) BMNOEXCEPT
{
    BM_ASSERT(delta > 0);
    --delta;
    // TODO: SIMD and unroll
    for (size_t i = 1; i < arr_size; ++i)
    {
        arr[i] -= delta;
        BM_ASSERT(arr[i] > arr[i-1]);
    } // for
}

/**
    Find max non-zero value in an array
    @internal
 */
template<typename VT, typename SZ>
bool find_max_nz(const VT* arr, SZ arr_size, SZ* found_idx) BMNOEXCEPT
{
    bool found = false;
    VT max_v = 0;
    for (SZ i = 0; i < arr_size; ++i)
    {
        VT v = arr[i];
        if (v > max_v)
        {
            max_v = v; *found_idx = i;
            found = true;
        }
    } // for i
    return found;
}

/**
    Find max non-zero value in an array
    @internal
 */
template<typename VT, typename SZ>
bool find_first_nz(const VT* arr, SZ arr_size, SZ* found_idx) BMNOEXCEPT
{
    for (SZ i = 0; i < arr_size; ++i)
    {
        VT v = arr[i];
        if (v)
        {
            *found_idx = i;
            return true;
        }
    } // for i
    return false;
}

/**
    Find count of non-zero elements in the array
    @internal
 */
template<typename VT, typename SZ>
SZ count_nz(const VT* arr, SZ arr_size) BMNOEXCEPT
{
    SZ cnt = 0;
    for (SZ i = 0; i < arr_size; ++i)
        cnt += (arr[i] != 0);
    return cnt;
}

// --------------------------------------------------------------
// Best representation
// --------------------------------------------------------------

/// Possible representations for bit sets
///
/// @internal
enum bit_representation
{
    e_bit_GAP = 0, ///< GAPs
    e_bit_INT,     ///< Int list
    e_bit_IINT,    ///< Inverted int list
    e_bit_1,       ///< all 1s
    e_bit_0,       ///< all 0s (empty block)
    e_bit_bit,   ///< uncompressed bit-block
    e_bit_end
};

/**
    Detect best representation for serialization for a block or sub-block
    @param gc - gap count
    @param bc - bit count
    @param max_bits - total number of bits in block
    @param bie_bits_per_int - number of bits per int in the compression scheme
    @param best_metric - [out] - best metric (number of bits or gaps)

    @return representation metric
 */
inline
bm::bit_representation best_representation(unsigned gc,
                                           unsigned bc,
                                           unsigned max_bits,
                                           float bie_bits_per_int,
                                           unsigned* best_metric) BMNOEXCEPT
{
    if (!bc)
    {
        *best_metric = bc;
        return e_bit_0;
    }
    if (bc == max_bits)
    {
        *best_metric = 0;
        return e_bit_1;
    }

    unsigned ibc = max_bits - bc;
    if (gc < bc) // GC < BC
    {
        if (gc <= ibc)
        {
            *best_metric = gc;
            float cost_in_bits = float(gc) * bie_bits_per_int;
            if (cost_in_bits >= float(max_bits))
                return e_bit_bit;
            return e_bit_GAP;
        }
    }
    else // GC >= BC
    {
        if (bc <= ibc)
        {
            *best_metric = bc;
            float cost_in_bits = float(bc) * bie_bits_per_int;
            if (cost_in_bits >= float(max_bits))
                return e_bit_bit;
            return e_bit_INT;
        }
    }
    *best_metric = ibc;
    float cost_in_bits = float(ibc) * bie_bits_per_int;
    if (cost_in_bits >= float(max_bits))
        return e_bit_bit;
    return e_bit_IINT;
}

// --------------------------------------------------------------
// Nibble array functions
// --------------------------------------------------------------

/**
    @brief set nibble in the array
    @param arr - base array of characters
    @param idx - nibble index
    @param v - value to set

    @internal
 */
inline
void set_nibble(unsigned char* arr, unsigned idx, unsigned char v) BMNOEXCEPT
{
    BM_ASSERT(arr);
    BM_ASSERT(v <= 0xF);

    unsigned arr_idx = idx >> 1;
    if (idx & 1)
    {
        unsigned char old_val = arr[arr_idx];
        old_val &= 0x0F; // clear the upper bits
        arr[arr_idx] = (unsigned char)(old_val | (v << 4));
    }
    else
    {
        unsigned char old_val = arr[arr_idx];
        old_val &= 0xF0; // clear the lower bits
        arr[arr_idx] = (unsigned char)(old_val | (v & 0xF));
    }
}

/**
    @brief get nibble from the array
    @param arr - base array of characters
    @param idx - nibble index
    @return value

    @internal
 */
inline
unsigned char get_nibble(const unsigned char* arr, unsigned idx) BMNOEXCEPT
{
    BM_ASSERT(arr);
    unsigned char v = arr[idx >> 1];
    v >>= (idx & 1) * 4;
    v &= 0xF;
    return v;
}


// --------------------------------------------------------------
// Functions to work with int values stored in 64-bit pointers
// --------------------------------------------------------------

/*!
    \brief helper union to interpret pointer as integers 
    @internal
*/
union ptr_payload_t
{
    bm::word_t* blk;
    bm::id64_t  i64;
    unsigned short i16[4];
};

/*!
    Test presense of value in payload pointer
    @internal
*/
inline
bm::id64_t ptrp_test(ptr_payload_t ptr, bm::gap_word_t v) BMNOEXCEPT
{
    if (v == 0)
    {
        return (ptr.i16[1] == 0);
    }
    bm::id64_t r = (ptr.i16[1] == v) | (ptr.i16[2] == v) | (ptr.i16[3] == v);
    return r;
}

// --------------------------------------------------------------------------

} // namespace bm

#endif
