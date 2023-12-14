#ifndef BMXORFUNC__H__INCLUDED__
#define BMXORFUNC__H__INCLUDED__
/*
Copyright(c) 2002-2019 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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

/*! \file bmxor.h
    \brief Functions and utilities for XOR filters (internal)
*/

#include "bmdef.h"
#include "bmutil.h"


namespace bm
{

/**
    Parameters for XOR similarity search.
    Tuneup params allows to reduce the search space to better
    balance compression rate vs speed.

    min_lookup_depth - default: 0,  minimal search depth to still try to improve
    max_lookup_depth - default: (2billion) maximum scan search
        Set a smaller number to improve search time

    Example:
    min_lookup_depth = 64; max_lookup_depth = 1024;

    stop_gain - default: 65533 - cutoff to stop search when serch found gain
                better than "stop_gain" absolute value
    Example:
    stop_gain = 10000;

    target_gain_ratio - default: 0.89 - cut off ratio between original block cost
                        metric and XOR basedd metric to stop searching.
                        (90% better than the original block is "good enough")
    Example:
    target_gain_ratio = 0.50; // 50% improvement is good enough

    min_gaps - default: 3 - minimal size of GAP block to be considered for
                            XOR search candidate
 */
struct xor_sim_params
{
    unsigned min_lookup_depth;
    unsigned max_lookup_depth;
    unsigned stop_gain;
    float    target_gain_ratio;
    unsigned min_gaps;

    xor_sim_params()
    : min_lookup_depth(0),
      max_lookup_depth(~0u/2),
      stop_gain(bm::gap_max_bits-3),
      target_gain_ratio(0.89f),
      min_gaps(3)
    {}
};


/**
    XOR complementarity type between 2 blocks
    @internal
 */
enum xor_complement_match
{
    e_no_xor_match = 0,
    e_xor_match_GC,
    e_xor_match_BC,
    e_xor_match_iBC,
    e_xor_match_EQ
};

/*!
    Function (32-bit) calculates basic complexity statistics on XOR product of
    two blocks (b1 XOR b2)
    @ingroup bitfunc
    @internal
*/
inline
void bit_block_xor_change32(const bm::word_t* BMRESTRICT block,
                            const bm::word_t* BMRESTRICT xor_block,
                            unsigned size,
                            unsigned* BMRESTRICT gc,
                            unsigned* BMRESTRICT bc) BMNOEXCEPT
{
    BM_ASSERT(gc && bc);

    unsigned gap_count = 1;
    unsigned bit_count = 0;

    bm::word_t  w, w0, w_prev, w_l;
    w = w0 = *block ^ *xor_block;
    bit_count += word_bitcount(w);

    const int w_shift = int(sizeof(w) * 8 - 1);
    w ^= (w >> 1);
    gap_count += bm::word_bitcount(w);
    gap_count -= (w_prev = (w0 >> w_shift)); // negative value correction

    const bm::word_t* block_end = block + size;
    for (++block, ++xor_block; block < block_end; ++block, ++xor_block)
    {
        w = w0 = *block ^ *xor_block;
        bit_count += bm::word_bitcount(w);
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

    *gc = gap_count;
    *bc = bit_count;
}

/*!
    Function (64-bit) calculates basic complexity statistics on XOR product of
    two blocks (b1 XOR b2)
    @ingroup bitfunc
    @internal
*/
inline
void bit_block_xor_change64(const bm::word_t* BMRESTRICT s_block,
                            const bm::word_t* BMRESTRICT ref_block,
                            unsigned size,
                            unsigned* BMRESTRICT gc,
                            unsigned* BMRESTRICT bc) BMNOEXCEPT
{
    BM_ASSERT(gc && bc);

    unsigned gap_count = 1;
    unsigned bit_count = 0;

    const bm::id64_t* BMRESTRICT block =   (const bm::id64_t*) s_block;
    const bm::id64_t* BMRESTRICT xor_block =  (const bm::id64_t*) ref_block;

    bm::id64_t  w, w0, w_prev, w_l;
    w = w0 = *block ^ *xor_block;
    bit_count += word_bitcount64(w);

    const int w_shift = int(sizeof(w) * 8 - 1);
    w ^= (w >> 1);
    gap_count += bm::word_bitcount64(w);
    gap_count -= unsigned(w_prev = (w0 >> w_shift)); // negative value correction

    const bm::id64_t* block_end = block + (size/2);
    for (++block, ++xor_block; block < block_end; ++block, ++xor_block)
    {
        w = w0 = *block ^ *xor_block;
        bit_count += bm::word_bitcount64(w);
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

    *gc = gap_count;
    *bc = bit_count;
}



/*!
    Function calculates number of times when bit value changed
    @internal
*/
inline
void bit_block_xor_change(const bm::word_t* BMRESTRICT block,
                              const bm::word_t* BMRESTRICT xor_block,
                              unsigned size,
                              unsigned* BMRESTRICT gc,
                              unsigned* BMRESTRICT bc) BMNOEXCEPT
{
#ifdef VECT_BLOCK_XOR_CHANGE
    VECT_BLOCK_XOR_CHANGE(block, xor_block, size, gc, bc);
#else
    #ifdef BM64OPT
        bm::bit_block_xor_change64(block, xor_block, size, gc, bc);
    #else
        bm::bit_block_xor_change32(block, xor_block, size, gc, bc);
    #endif
#endif
}

/**
    Structure to compute XOR gap-count profile by sub-block waves
    @ingroup bitfunc
    @internal
*/
struct block_waves_xor_descr
{
    // stats for s-block
    unsigned short sb_gc[bm::block_waves]; ///< GAP counts
    unsigned short sb_bc[bm::block_waves]; ///< BIT counts

    // stats ref.block XOR mask
    unsigned short sb_xor_gc[bm::block_waves]; ///< XOR-mask GAP count
    unsigned short sb_xor_bc[bm::block_waves]; ///< XOR-mask GAP count
};

/**
    Capture the XOR filter results (xor block against ref.block)
    @internal
 */
struct block_xor_match_descr
{
    typedef bvector_size_type   size_type;

    bm::xor_complement_match  match_type; ///< match type
    unsigned                  block_gain; ///< XOR filter improvement (best)
    size_type                 ref_idx;    ///< reference vector index
    bm::id64_t                xor_d64;    ///< recorded digest

    // sub-block waves masks for metrics
    bm::id64_t   gc_d64;
    bm::id64_t   bc_d64;
    bm::id64_t   ibc_d64;

    // recorded gains for metrics
    unsigned     gc_gain;
    unsigned     bc_gain;
    unsigned     ibc_gain;

    unsigned xor_gc;
    unsigned xor_bc;

    block_xor_match_descr() : match_type(e_no_xor_match) {}
};

/**
    XOR match pair
    @internal
 */
struct match_pair
{
    bvector_size_type         ref_idx;    ///< reference vector index
    bm::id64_t                xor_d64;    ///< recorded digest

    match_pair(){}
    match_pair(bvector_size_type idx, bm::id64_t d64)
    : ref_idx(idx), xor_d64(d64)
    {}

};

/**
    XOR match chain
    @internal
 */
template<typename BLOCK_IDX> struct block_match_chain
{
    BLOCK_IDX                nb;
    unsigned                 chain_size;
    unsigned                 ref_idx[64];
    bm::id64_t               xor_d64[64];
    bm::xor_complement_match match;

    bool operator==(const block_match_chain& bmc) const BMNOEXCEPT
    {
        if (nb != bmc.nb || chain_size != bmc.chain_size || match != bmc.match)
        {
            return false;
        }
        for (unsigned i = 0; i < chain_size; ++i)
        {
            if (ref_idx[i] != bmc.ref_idx[i])
                return false;
            if (match == e_xor_match_EQ)
                continue;
            if (xor_d64[i] != bmc.xor_d64[i])
                return false;
        }
        return true;
    }
};

/**
    Greedy algorithm to find additional matches
    improving the inital best match block on its match type

    @param match_pairs_vect - [out] target vector of best match pairs
    @param match_vect - [in/out] vector of all found match descriptors

    @return number of new finds (if any)
 */
template<typename PVT, typename VT>
typename VT::size_type
greedy_refine_match_vector(PVT&                      match_pairs_vect,
                           VT&                       match_vect,
                           typename VT::size_type    best_ref_idx,
                           bm::id64_t                d64,
                           bm::xor_complement_match  match_type)
{
    BM_ASSERT(match_type && d64);
    match_pairs_vect.resize(0);

    bm::id64_t  d64_acc(d64);

    // pass 1 (exact match)
    //
    typename VT::size_type sz = match_vect.size();
    for (typename VT::size_type i = 0; (i < sz) && (d64_acc != ~0ull); ++i)
    {
        block_xor_match_descr& xmd = match_vect[i];
        if (xmd.ref_idx == best_ref_idx) // self hit
            continue;
        if (xmd.match_type == match_type) // best compatible match types
        {
            bm::id64_t d64_new = ~d64_acc & xmd.xor_d64;
            if (d64_new)
            {
                d64_acc |= d64_new;    // add mask to accum
                match_pairs_vect.push_back(bm::match_pair(xmd.ref_idx, d64_new));
            }
            xmd.match_type = e_no_xor_match; // mark it to exclude from pass 2
        }
    } // for i

    // pass 2 (extended match)
    //
    const unsigned min_gain_cut_off = 50;
    for (typename VT::size_type i = 0; (i < sz) && (d64_acc != ~0ull); ++i)
    {
        block_xor_match_descr& xmd = match_vect[i];
        if (xmd.ref_idx == best_ref_idx || !xmd.match_type) // self hit or none
            continue;
        BM_ASSERT(xmd.match_type != match_type);

        bm::id64_t d64_new = 0;
        switch (match_type)
        {
        case e_xor_match_GC:
            if (xmd.gc_gain > min_gain_cut_off)
                d64_new = ~d64_acc & xmd.gc_d64;
            break;
        case e_xor_match_BC:
            if (xmd.bc_gain > min_gain_cut_off)
                d64_new = ~d64_acc & xmd.bc_d64;
            break;
        case e_xor_match_iBC:
            if (xmd.ibc_gain > min_gain_cut_off)
                d64_new = ~d64_acc & xmd.ibc_d64;
            break;
        default:
            break;
        } // switch

        if (d64_new) // some improvement found
        {
            d64_acc |= d64_new;    // add mask to accum
            match_pairs_vect.push_back(bm::match_pair(xmd.ref_idx, d64_new));
            xmd.match_type = e_no_xor_match;
        }
    } // for

    return match_pairs_vect.size();
}

/**
    Check effective bit-rate for the XOR encode vector
    @return 1 - < 256 (8bit), 2 - < 65536 (16-bit) or 0 - 32-bit
    @internal
 */
template<typename BMChain, typename RVect>
unsigned char check_pair_vect_vbr(const BMChain& mchain, const RVect& ref_vect)
{
    size_t max_idx = 0;
    for (size_t i = 0; i < mchain.chain_size; ++i)
    {
        bvector_size_type ridx = mchain.ref_idx[i];
        ridx = ref_vect.get_row_idx(ridx);
        if (ridx > max_idx)
            max_idx = ridx;
    } // for i
    if (max_idx < 256)
        return 1;
    if (max_idx < 65536)
        return 2;
    return 0;
}


/**
    Compute reference (non-XOR) 64-dim complexity descriptor for the
    s-block.
    Phase 1 of the XOR filtering process is to establish the base metric

    @internal
*/
inline
void compute_s_block_descr(const bm::word_t* BMRESTRICT block,
                        block_waves_xor_descr& BMRESTRICT x_descr,
                        unsigned* BMRESTRICT s_gc,
                        unsigned* BMRESTRICT s_bc) BMNOEXCEPT
{
    *s_gc = *s_bc = 0;
    // TODO: SIMD (for loop can go inside VECT to minimize LUT re-inits)
    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        unsigned off = (i * bm::set_block_digest_wave_size);
        const bm::word_t* sub_block = block + off;
        unsigned gc, bc;
        // TODO: optimize to compute GC and BC in a single pass
        #if defined(VECT_BLOCK_CHANGE)
            gc = VECT_BLOCK_CHANGE(sub_block, bm::set_block_digest_wave_size);
        #else
            #ifdef BM64OPT
                gc = bm::bit_block_change64(sub_block, bm::set_block_digest_wave_size);
                BM_ASSERT(gc == bm::bit_block_change32(sub_block, bm::set_block_digest_wave_size));
            #else
                gc = bm::bit_block_change32(sub_block, bm::set_block_digest_wave_size);
            #endif
        #endif
        bc = bm::bit_count_min_unroll(
                    sub_block, sub_block + bm::set_block_digest_wave_size);
        x_descr.sb_bc[i] = (unsigned short) bc;
        *s_bc += bc;
        if (i) // wave border correction
        {
            bm::word_t w_l = sub_block[-1];
            bm::word_t w_r = sub_block[0] & 1;
            w_l >>= 31;
            gc -= (w_l == w_r);
        }
        x_descr.sb_gc[i] = (unsigned short) gc;
        *s_gc += gc;

    } // for i
    // TODO: compute and return d64 - use it later
}


/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target := block ^ xor_block
    @param block - arg1
    @param xor_block - arg2
    @param digest - mask for each block wave to XOR (1) or just copy (0)

    @internal
*/
inline
void bit_block_xor(bm::word_t*         target_block,
                   const bm::word_t*   block,
                   const bm::word_t*   xor_block,
                   bm::id64_t          digest) BMNOEXCEPT
{
    BM_ASSERT(target_block);
    BM_ASSERT(block);
    BM_ASSERT(xor_block);
    BM_ASSERT(digest);

#ifdef VECT_BIT_BLOCK_XOR
    VECT_BIT_BLOCK_XOR(target_block, block, xor_block, digest);
#else
    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        const bm::id64_t mask = (1ull << i);
        unsigned off = (i * bm::set_block_digest_wave_size);

    #ifdef BM64OPT
        const bm::id64_t* sub_block = (const bm::id64_t*)(block + off);
        bm::id64_t* t_sub_block = (bm::id64_t*)(target_block + off);
        const bm::id64_t* sub_block_end = sub_block + bm::set_block_digest_wave_size/2;
        if (digest & mask) // XOR filtered sub-block
        {
            const bm::id64_t* xor_sub_block = (const bm::id64_t*)(xor_block + off);
            for ( ;sub_block < sub_block_end; )
            {
                t_sub_block[0] = sub_block[0] ^ xor_sub_block[0];
                t_sub_block[1] = sub_block[1] ^ xor_sub_block[1];
                t_sub_block[2] = sub_block[2] ^ xor_sub_block[2];
                t_sub_block[3] = sub_block[3] ^ xor_sub_block[3];
                t_sub_block+=4; sub_block+=4; xor_sub_block+=4;
            } // for
        }
        else // just copy the source
        {
            for (; sub_block < sub_block_end; t_sub_block+=4, sub_block+=4)
            {
                t_sub_block[0] = sub_block[0];
                t_sub_block[1] = sub_block[1];
                t_sub_block[2] = sub_block[2];
                t_sub_block[3] = sub_block[3];
            } // for
        }
    #else
        const bm::word_t* sub_block = block + off;
        bm::word_t* t_sub_block = target_block + off;
        const bm::word_t* sub_block_end = sub_block + bm::set_block_digest_wave_size;
        if (digest & mask) // XOR filtered sub-block
        {
            const bm::word_t* xor_sub_block = xor_block + off;
            for (; sub_block < sub_block_end; )
            {
                t_sub_block[0] = sub_block[0] ^ xor_sub_block[0];
                t_sub_block[1] = sub_block[1] ^ xor_sub_block[1];
                t_sub_block[2] = sub_block[2] ^ xor_sub_block[2];
                t_sub_block[3] = sub_block[3] ^ xor_sub_block[3];
                t_sub_block+=4; sub_block+=4; xor_sub_block+=4;
            } // for
        }
        else // just copy the source
        {
            for (; sub_block < sub_block_end; t_sub_block+=4, sub_block+=4)
            {
                t_sub_block[0] = sub_block[0];
                t_sub_block[1] = sub_block[1];
                t_sub_block[2] = sub_block[2];
                t_sub_block[3] = sub_block[3];
            } // for
        }
    #endif
    } // for i
#endif
}


/**
    Build partial XOR product of 2 bit-blocks using digest mask

    @param target_block - target := target ^ xor_block
    @param xor_block - arg
    @param digest - mask for each block wave to XOR (1)

    @internal
*/
inline
void bit_block_xor(bm::word_t* target_block, const bm::word_t*  xor_block,
                   bm::id64_t digest) BMNOEXCEPT
{
    BM_ASSERT(target_block);
    BM_ASSERT(xor_block);
    BM_ASSERT(digest);

    #ifdef VECT_BIT_BLOCK_XOR_2WAY
        VECT_BIT_BLOCK_XOR_2WAY(target_block, xor_block, digest);
    #else
        while (digest)
        {
            bm::id64_t t = bm::bmi_blsi_u64(digest); // d & -d;
            unsigned wave = bm::word_bitcount64(t - 1);
            unsigned off = wave * bm::set_block_digest_wave_size;

        #ifdef BM64OPT
            const bm::id64_t* xor_sub_block = (const bm::id64_t*)(xor_block + off);
            bm::id64_t* t_sub_block = (bm::id64_t*)(target_block + off);
            const bm::id64_t* t_sub_block_end = (t_sub_block + bm::set_block_digest_wave_size/2);
            for (; t_sub_block < t_sub_block_end; t_sub_block+=4, xor_sub_block+=4)
            {
                t_sub_block[0] ^= xor_sub_block[0];
                t_sub_block[1] ^= xor_sub_block[1];
                t_sub_block[2] ^= xor_sub_block[2];
                t_sub_block[3] ^= xor_sub_block[3];
            } // for
        #else
            const bm::word_t* xor_sub_block = xor_block + off;
            bm::word_t* t_sub_block = target_block + off;
            const bm::word_t* t_sub_block_end = t_sub_block + bm::set_block_digest_wave_size;
            for (; t_sub_block < t_sub_block_end; t_sub_block+=4, xor_sub_block+=4)
            {
                t_sub_block[0] ^= xor_sub_block[0];
                t_sub_block[1] ^= xor_sub_block[1];
                t_sub_block[2] ^= xor_sub_block[2];
                t_sub_block[3] ^= xor_sub_block[3];
            } // for
        #endif
            digest = bm::bmi_bslr_u64(digest); // d &= d - 1;
        } // while
    #endif
}

/**
    List of reference bit-vectors with their true index associations

    Each referece vector would have two alternative indexes:
     - index(position) in the reference list
     - index(row) in the external bit-matrix (plane index)

    @internal
*/
template<typename BV>
class bv_ref_vector
{
public:
    typedef BV                                          bvector_type;
    typedef typename bvector_type::size_type            size_type;
    typedef bvector_type*                               bvector_type_ptr;
    typedef const bvector_type*                         bvector_type_const_ptr;
    typedef typename bvector_type::allocator_type       bv_allocator_type;


    typedef bm::block_match_chain<size_type>            block_match_chain_type;
    typedef
    bm::dynamic_heap_matrix<block_match_chain_type, bv_allocator_type>
                                                             matrix_chain_type;

public:

    /// reset the collection (resize(0))
    void reset()
    {
        rows_acc_ = 0;
        ref_bvects_.resize(0); ref_bvects_rows_.resize(0);
    }

    /**
        Add reference vector
        @param bv - bvector pointer
        @param ref_idx - reference (row) index
    */
    void add(const bvector_type* bv, size_type ref_idx)
    {
        BM_ASSERT(bv);
        ref_bvects_.push_back(bv);
        ref_bvects_rows_.push_back(ref_idx);
    }

    /// Get reference list size
    size_type size() const BMNOEXCEPT { return (size_type)ref_bvects_.size(); }

    /// Get reference vector by the index in this ref-vector
    const bvector_type* get_bv(size_type idx) const BMNOEXCEPT
                                        { return ref_bvects_[idx]; }

    /// Get reference row index by the index in this ref-vector
    size_type get_row_idx(size_type idx) const BMNOEXCEPT
                        { return (size_type)ref_bvects_rows_[idx]; }

    /// not-found value for find methods
    static
    size_type not_found() BMNOEXCEPT { return ~(size_type(0)); }

    /// Find vector index by the reference index
    /// @return ~0 if not found
    size_type find(std::size_t ref_idx) const BMNOEXCEPT
    {
        size_type sz = size();
        for (size_type i = 0; i < sz; ++i) // TODO: optimization
            if (ref_idx == ref_bvects_rows_[i])
                return i;
        return not_found();
    }

    /// Find vector index by the pointer
    /// @return ~0 if not found
    size_type find_bv(const bvector_type* bv) const BMNOEXCEPT
    {
        size_type sz = size();
        for (size_type i = 0; i < sz; ++i)
            if (bv == ref_bvects_[i])
                return i;
        return not_found();
    }

    /// Fill block allocation digest for all vectors in the reference collection
    ///   @param bv_blocks - [out] bvector of blocks statistics
    ///
    void fill_alloc_digest(bvector_type& bv_blocks) const
    {
        size_type sz = size();
        if (sz)
        {
            for (size_type i = 0; i < sz; ++i)
                ref_bvects_[i]->fill_alloc_digest(bv_blocks);
            BM_DECLARE_TEMP_BLOCK(tb)
            bv_blocks.optimize(tb);
        }
    }

    /// Reset and build vector of references from a basic bit-matrix
    ///  all NULL rows are skipped, not added to the ref.vector
    /// @sa add_vectors
    ///
    template<class BMATR>
    void build(const BMATR& bmatr)
    {
        reset();
        add_vectors(bmatr);
    }

    /// Append basic bit-matrix to the list of reference vectors
    /// @sa build
    /// @sa add_sparse_vector
    template<typename BMATR>
    void add_vectors(const BMATR& bmatr)
    {
        size_type rows = bmatr.rows();
        for (size_type r = 0; r < rows; ++r)
            if (bvector_type_const_ptr bv = bmatr.get_row(r))
                add(bv, rows_acc_ + r);
        rows_acc_ += unsigned(rows);
    }

    /// Add bit-transposed sparse vector as a bit-matrix
    /// @sa add_vectors
    ///
    template<class SV>
    void add_sparse_vector(const SV& sv)
    {
        add_vectors(sv.get_bmatrix());
    }

    /** Utility function to resize matrix based on number of vectors and blocks
    */
    void resize_xor_matrix(matrix_chain_type& matr,
                           size_type total_blocks) const
    {
        if (total_blocks)
            matr.resize(ref_bvects_.size(), total_blocks, false /*no-copy*/);
        else
            matr.resize(0, 0);
    }

    /** Calculate blocks digest and resize XOR distance matrix
        based on total number of available blocks
        @return true if created ok (false if no blocks found)
     */
    bool build_nb_digest_and_xor_matrix(matrix_chain_type& matr,
                                        bvector_type& bv_blocks) const
    {
        fill_alloc_digest(bv_blocks);
        size_type cnt = bv_blocks.count();
        if (!cnt)
            return false;
        resize_xor_matrix(matr, cnt);
        return true;
    }

protected:
    typedef bm::heap_vector<bvector_type_const_ptr, bv_allocator_type, true> bvptr_vector_type;
    typedef bm::heap_vector<std::size_t, bv_allocator_type, true> bv_plane_vector_type;

protected:
    unsigned                 rows_acc_ = 0;     ///< total rows accumulator
    bvptr_vector_type        ref_bvects_;       ///< reference vector pointers
    bv_plane_vector_type     ref_bvects_rows_;  ///< reference vector row idxs
};

// --------------------------------------------------------------------------
//
// --------------------------------------------------------------------------

/**
    XOR similarity model

    @internal
*/
template<typename BV>
struct xor_sim_model
{
public:
    typedef BV                                          bvector_type;
    typedef typename bvector_type::size_type            size_type;
    typedef typename bvector_type::allocator_type       bv_allocator_type;


    typedef bm::block_match_chain<size_type>            block_match_chain_type;
    typedef
    bm::dynamic_heap_matrix<block_match_chain_type, bv_allocator_type>
                                                             matrix_chain_type;

public:
    matrix_chain_type  matr; ///< model matrix
    bvector_type       bv_blocks;  ///< blocks digest
};

// --------------------------------------------------------------------------
//
// --------------------------------------------------------------------------

/**
    XOR scanner to search for complement-similarities in
    collections of bit-vectors

    @internal
*/
template<typename BV>
class xor_scanner
{
public:
    typedef bm::bv_ref_vector<BV>                 bv_ref_vector_type;
    typedef BV                                    bvector_type;
    typedef typename bvector_type::allocator_type bv_allocator_type;
    typedef typename bvector_type::size_type      size_type;

    typedef bm::heap_vector<bm::block_xor_match_descr, bv_allocator_type, true>
            xor_matches_vector_type;
    typedef bm::heap_vector<bm::match_pair, bv_allocator_type, true>
            match_pairs_vector_type;
    typedef typename bv_ref_vector_type::matrix_chain_type
                                                    matrix_chain_type;
    typedef bm::heap_vector<bm::word_t*, bv_allocator_type, true>
                                                    bv_blocks_vector_type;
    typedef bm::heap_vector<unsigned, bv_allocator_type, true>
                                                    bv_bcgc_vector_type;

    typedef bm::heap_vector<bm::block_waves_xor_descr, bv_allocator_type, true>
                                                    bv_xdescr_vector_type;

public:

    xor_scanner() {}
    ~xor_scanner()
    {
        free_blocks();
    }


    void set_ref_vector(const bv_ref_vector_type* ref_vect) BMNOEXCEPT
    { ref_vect_ = ref_vect; }

    const bv_ref_vector_type& get_ref_vector() const BMNOEXCEPT
    { return *ref_vect_; }

    /** Get statistics for the r-(or s-) block
        @param ri - nb cache index
    */
    void get_s_block_stats(size_type ri) BMNOEXCEPT;

    /** Compute statistics for the r-(or s-) block
        @param block - bit-block target
    */
    void compute_s_block_stats(const bm::word_t* block) BMNOEXCEPT;

    /** Scan for all candidate bit-blocks to find mask or match
        @return XOR referenece match type
    */
    bm::xor_complement_match
    search_best_xor_mask(const bm::word_t* s_block,
                              size_type ri,
                              size_type ridx_from,
                              size_type ridx_to,
                              unsigned i, unsigned j,
                              bm::word_t* tx_block,
                              const bm::xor_sim_params& params);
    /**
        Run a search to add possible XOR match chain additions
     */
    size_type refine_match_chain();


    /**
        XOR all match blocks to target using their digest masks
     */
    void apply_xor_match_vector(
                       bm::word_t* target_xor_block,
                       const bm::word_t* s_block,
                       size_type s_ri,
                       const match_pairs_vector_type& pm_vect,
                       unsigned i, unsigned j) const BMNOEXCEPT;

    /**
        Calculate matrix of best XOR match metrics per block
        for the attached collection of bit-vectors
        @return true if computed successfully
     */
    bool compute_sim_model(xor_sim_model<BV>&        sim_model,
                           const bv_ref_vector_type& ref_vect,
                           const bm::xor_sim_params& params);

    /**
        Calculate matrix of best XOR match metrics per block
        for the attached collection of bit-vectors
        @return true if computed successfully
     */
    bool compute_sim_model(xor_sim_model<BV> &sim_model,
                           const bm::xor_sim_params& params);

    /**
        Compute similarity model for block
     */
    void compute_sim_model(
        typename xor_sim_model<BV>::matrix_chain_type &sim_model_matr,
                           size_type                 nb,
                           size_type                 nb_rank,
                           const bm::xor_sim_params& params);
    /**
        Compute reference complexity descriptor based on XOR vector.
        Returns the digest of sub-blocks where XOR filtering improved the metric
        (function needs reference to estimate the improvement).

        part of Phase 2 of the XOR filtering process

        @sa compute_sub_block_complexity_descr

        @internal
    */
    void compute_xor_complexity_descr(
                const bm::word_t* BMRESTRICT          block,
                bm::id64_t                            block_d64,
                const bm::word_t* BMRESTRICT          xor_block,
                bm::block_waves_xor_descr& BMRESTRICT x_descr,
                bm::block_xor_match_descr& BMRESTRICT xmd) const BMNOEXCEPT;

    /**
        Check if XOR transform simplified block enough for
        compressibility objective
     */
    bool validate_xor(const bm::word_t* xor_block) const BMNOEXCEPT;

    size_type found_ridx() const BMNOEXCEPT { return found_ridx_; }

    /// Return best match type of a found block
    ///
    bm::xor_complement_match get_best_match_type() const BMNOEXCEPT
        {   return x_block_mtype_; }

    const bm::word_t* get_found_block() const BMNOEXCEPT
        { return found_block_xor_; }
    unsigned get_x_best_metric() const BMNOEXCEPT { return x_best_metric_; }
    bm::id64_t get_xor_digest() const BMNOEXCEPT { return x_d64_; }

    unsigned get_s_bc() const BMNOEXCEPT { return s_bc_; }
    unsigned get_s_gc() const BMNOEXCEPT { return s_gc_; }
    unsigned get_s_block_best() const BMNOEXCEPT
                    { return s_block_best_metric_; }


    bm::block_waves_xor_descr& get_descr() BMNOEXCEPT { return x_descr_; }

    static
    bm::xor_complement_match best_metric(unsigned bc, unsigned gc,
                                        unsigned* best_metric) BMNOEXCEPT;

    xor_matches_vector_type& get_match_vector() BMNOEXCEPT
        { return match_vect_; }

    match_pairs_vector_type& get_match_pairs() BMNOEXCEPT
        { return chain_match_vect_; }

    /// Return block from the reference vector [vect_idx, block_i, block_j]
    ///
    const bm::word_t* get_ref_block(size_type ri,
                                    unsigned i, unsigned j) const BMNOEXCEPT
    { return ref_vect_->get_bv(ri)->get_blocks_manager().get_block_ptr(i, j); }

    /// Sync TEMP vector size
    /// @internal
    void sync_nb_vect();

protected:

    /// Deoptimize vertical slice of GAP blocks
    /// @param nb - block number
    ///
    void deoptimize_gap_blocks(size_type nb,
                               const xor_sim_params& params);

    /// Free the collection of temp blocks
    void free_blocks() BMNOEXCEPT;


private:
    xor_scanner(const xor_scanner&) = delete;
    xor_scanner& operator=(const xor_scanner&) = delete;

private:
    BM_DECLARE_TEMP_BLOCK(xor_tmp_block_)

    const bv_ref_vector_type*   ref_vect_ = 0; ///< ref.vect for XOR filter
    bv_blocks_vector_type       nb_blocks_vect_;   ///< pointers to temp blocks
    bv_bcgc_vector_type         nb_gc_vect_;
    bv_bcgc_vector_type         nb_bc_vect_;
    bv_xdescr_vector_type       nb_xdescr_vect_;

    bv_allocator_type           alloc_;            ///< allocator to produce blocks

    bm::block_waves_xor_descr        x_descr_;  ///< XOR desriptor

    // S-block statistics
    //
    unsigned                      s_bc_;     ///< bitcount
    unsigned                      s_gc_;     ///< gap count
    unsigned                      s_block_best_metric_; ///< s-block orig.metric

    unsigned                      x_best_metric_; ///< min(gc, bc, ibc)
    bm::xor_complement_match      x_block_mtype_; ///< metric type

    // scan related metrics
    bm::id64_t                    x_d64_;        ///< search digest
    size_type                     found_ridx_;   ///< match vector (in references)
    const bm::word_t*             found_block_xor_;

    // match chain members:
    //
    xor_matches_vector_type       match_vect_; ///< vector of match descr
    match_pairs_vector_type       chain_match_vect_; ///< refined match pairs
};

// --------------------------------------------------------------------------
//
// --------------------------------------------------------------------------
//
//  Naming conventions and glossary:
//
//  s_block - source serialization block to be XOR filtered (anchor block)
//  best_ref_block - best reference block picked for XOR transform
//  xt_block - s_block XOR best_ref_block
//



template<typename BV>
void xor_scanner<BV>::get_s_block_stats(size_type ri) BMNOEXCEPT
{
    x_descr_ = nb_xdescr_vect_[ri];
    s_gc_ = nb_gc_vect_[ri];
    s_bc_ = nb_bc_vect_[ri];

    x_block_mtype_ = best_metric(s_bc_, s_gc_, &s_block_best_metric_);
    x_best_metric_ = s_block_best_metric_;
}


template<typename BV>
void xor_scanner<BV>::compute_s_block_stats(const bm::word_t* block) BMNOEXCEPT
{
    BM_ASSERT(IS_VALID_ADDR(block));
    BM_ASSERT(!BM_IS_GAP(block));

    bm::compute_s_block_descr(block, x_descr_, &s_gc_, &s_bc_);

    x_block_mtype_ = best_metric(s_bc_, s_gc_, &s_block_best_metric_);
    x_best_metric_ = s_block_best_metric_;
}
// --------------------------------------------------------------------------

template<typename BV>
void xor_scanner<BV>::compute_xor_complexity_descr(
                const bm::word_t* BMRESTRICT block,
                bm::id64_t                   block_d64,
                const bm::word_t* BMRESTRICT xor_block,
                bm::block_waves_xor_descr& BMRESTRICT x_descr,
                bm::block_xor_match_descr& BMRESTRICT xmd) const  BMNOEXCEPT
{
    bm::id64_t d0 = ~block_d64;

    xmd.xor_gc = xmd.xor_bc = 0;

    // Pass 1: compute XOR descriptors
    //
    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        unsigned off = (i * bm::set_block_digest_wave_size);
        const bm::word_t* sub_block = block + off;
        const bm::word_t* xor_sub_block = xor_block + off;

        unsigned xor_gc, xor_bc;
        bm::bit_block_xor_change(sub_block, xor_sub_block,
                                 bm::set_block_digest_wave_size,
                                 &xor_gc, &xor_bc);
        x_descr.sb_xor_bc[i] = (unsigned short)xor_bc;
        BM_ASSERT(xor_gc);
        if (i) // wave border correction
        {
            bm::word_t w_l = (sub_block[-1] ^ xor_sub_block[-1]);
            bm::word_t w_r = (sub_block[0] ^ xor_sub_block[0]) & 1;
            w_l >>= 31;
            xor_gc -= (w_l == w_r);
        }
        x_descr.sb_xor_gc[i] = (unsigned short)xor_gc;

        xmd.xor_bc += xor_bc;
        xmd.xor_gc += xor_gc;

    } // for i

    // Pass 2: find the best match
    //
    unsigned block_gc_gain(0), block_bc_gain(0), block_ibc_gain(0);
    bm::id64_t gc_digest(0), bc_digest(0), ibc_digest(0);
    const unsigned wave_max_bits = bm::set_block_digest_wave_size * 32;

    for (unsigned i = 0; i < bm::block_waves; ++i)
    {
        bm::id64_t dmask = (1ull << i);
        if (d0 & dmask)
            continue;

        unsigned xor_gc = x_descr.sb_xor_gc[i];
        if (xor_gc <= 1)
        {
            gc_digest |= dmask;
            block_gc_gain += x_descr.sb_gc[i]; // all previous GAPs are gone
        }
        else if (xor_gc < x_descr.sb_gc[i]) // some improvement in GAPs
        {
            gc_digest |= dmask;
            block_gc_gain += (x_descr.sb_gc[i] - xor_gc);
        }
        unsigned xor_bc = x_descr.sb_xor_bc[i];
        if (xor_bc < x_descr.sb_bc[i]) // improvement in BITS
        {
            bc_digest |= dmask;
            block_bc_gain += (x_descr.sb_bc[i] - xor_bc);
        }
        unsigned xor_ibc = wave_max_bits - xor_bc;
        unsigned wave_ibc = wave_max_bits - x_descr.sb_bc[i];
        if (xor_ibc < wave_ibc) // improvement in 0 BITS
        {
            ibc_digest |= dmask;
            block_ibc_gain += (wave_ibc - xor_ibc);
        }

    } // for i

    // Save metric results into XOR match descriptor
    //

    xmd.gc_d64 = gc_digest;
    xmd.bc_d64 = bc_digest;
    xmd.ibc_d64 = ibc_digest;

    xmd.gc_gain = block_gc_gain;
    xmd.bc_gain = block_bc_gain;
    xmd.ibc_gain = block_ibc_gain;


    // Find the winning metric and its digest mask
    //
    if (!(block_gc_gain | block_bc_gain | block_ibc_gain)) // match not found
    {
        // this is to check if xor filter has
        // canceled out a whole sub-block wave
        //
        bm::id64_t d0_x = ~bm::calc_block_digest0(xor_block);
        if (d0 == d0_x)
        {
            xmd.match_type = bm::e_xor_match_GC;
            xmd.block_gain = bm::block_waves;
            xmd.xor_d64 = d0;
            return;
        }
        xmd.match_type = bm::e_no_xor_match;
        xmd.block_gain = 0; xmd.xor_d64 = 0;
        return;
    }

    int new_gc = int(s_gc_) - int(block_gc_gain);
    if (new_gc < 0)
        new_gc = 0;
    int new_bc = int(s_bc_) - int(block_bc_gain);
    if (new_bc < 0)
        new_bc = 0;
    int new_ibc =  int(bm::gap_max_bits) - int(s_bc_) - int(block_ibc_gain);
    if (new_ibc < 0)
        new_ibc = 0;


    unsigned best_m;
    xmd.match_type = best_metric(unsigned(new_bc), unsigned(new_gc), &best_m);
    switch (xmd.match_type)
    {
    case e_xor_match_GC:
        if (new_ibc < new_gc)
        {
            xmd.block_gain = block_ibc_gain; xmd.xor_d64 = ibc_digest;
        }
        else
        {
            xmd.block_gain = block_gc_gain; xmd.xor_d64 = gc_digest;
        }
        break;
    case e_xor_match_BC:
        if (new_ibc < new_bc)
        {
            xmd.block_gain = block_ibc_gain; xmd.xor_d64 = ibc_digest;
        }
        else
        {
            xmd.block_gain = block_bc_gain; xmd.xor_d64 = bc_digest;
        }
        break;
    case e_xor_match_iBC:
        xmd.block_gain = block_ibc_gain; xmd.xor_d64 = ibc_digest;
        break;
    default:
        break;
    } // switch
#if 0
    // Disabled as cases compression ratio degradation in some tests
    if (!xmd.xor_d64) // best metric choice did not work try best gain
    {
        if (block_gc_gain >= block_bc_gain && block_gc_gain >= block_ibc_gain)
        {
            xmd.block_gain = block_gc_gain; xmd.xor_d64 = gc_digest;
        }
        else
        if (block_bc_gain > block_gc_gain && block_bc_gain > block_ibc_gain)
        {
            xmd.block_gain = block_bc_gain; xmd.xor_d64 = bc_digest;
        }
        else
        {
            xmd.block_gain = block_ibc_gain; xmd.xor_d64 = ibc_digest;
        }
    }
#endif
}

// --------------------------------------------------------------------------

template<typename BV>
bm::xor_complement_match
xor_scanner<BV>::search_best_xor_mask(const bm::word_t* s_block,
                                       size_type s_ri,
                                       size_type ridx_from,
                                       size_type ridx_to,
                                       unsigned i, unsigned j,
                                       bm::word_t* tx_block,
                                       const bm::xor_sim_params& params)
{
    BM_ASSERT(ridx_from <= ridx_to);
    BM_ASSERT(IS_VALID_ADDR(s_block));
    BM_ASSERT(tx_block);

    if (ridx_to > ref_vect_->size())
        ridx_to = ref_vect_->size();

    bm::xor_complement_match rb_found = e_no_xor_match;
    bm::id64_t d64 = 0;
    found_block_xor_ = 0;

    unsigned best_block_gain = 0;
    int best_ri = -1;

    match_vect_.resize(0);

    unsigned s_gc(0);
    bool s_gap = BM_IS_GAP(s_block);
    if (s_gap)
    {
        const bm::gap_word_t* gap_s_block = BMGAP_PTR(s_block);
        s_gc = bm::gap_length(gap_s_block);
        if (s_gc <= 3)
            return e_no_xor_match;
        s_block = nb_blocks_vect_.at(s_ri);
        BM_ASSERT(s_block);
    }
    bm::id64_t s_block_d64 = bm::calc_block_digest0(s_block);

    // scan pass: over reference vectors
    //
    if (ridx_to > ridx_from + params.max_lookup_depth)
        ridx_to = ridx_from + params.max_lookup_depth;

    size_type depth = 0;
    for (size_type ri = ridx_from; ri < ridx_to; ++ri, ++depth)
    {
        const bm::word_t* ref_block = get_ref_block(ri, i, j);
        if (BM_IS_GAP(ref_block))
        {
            const bm::gap_word_t* gap_ref_block = BMGAP_PTR(ref_block);
            unsigned r_gc = bm::gap_length(gap_ref_block);
            if (r_gc <= 3)
                continue;

            if (s_gap) // both blocks GAPs - check if XOR does not make sense
            {
                if (s_gc < r_gc) // S-GAP block is shorter than Ref BLOCK
                {
                    unsigned gc_diff = r_gc - s_gc;
                    if (gc_diff >= s_gc) // cannot improve anything
                        continue;
                }
            }

            if (nb_blocks_vect_.size() > ri)
                ref_block = nb_blocks_vect_[ri];
        }
        if (!IS_VALID_ADDR(ref_block))
            continue;

        BM_ASSERT(s_block != ref_block);

        bm::block_xor_match_descr xmd;
        compute_xor_complexity_descr(s_block, s_block_d64, ref_block, x_descr_, xmd);
        if (xmd.xor_d64) // candidate XOR block found
        {
            xmd.ref_idx = ri;
            BM_ASSERT(xmd.match_type);
            if (xmd.block_gain > best_block_gain)
            {
                // check if "it is good enough"
                BM_ASSERT(x_best_metric_ <= s_block_best_metric_);

                best_block_gain = xmd.block_gain;
                best_ri = int(ri);
                d64 = xmd.xor_d64;
                if (xmd.block_gain >= (bm::gap_max_bits-3))
                    break;
                unsigned curr_best;
                //bm::xor_complement_match match =
                best_metric(xmd.xor_bc, xmd.xor_gc, &curr_best);
                float gain_ratio =
                    float(xmd.block_gain) / float(s_block_best_metric_);

                if (depth >= params.min_lookup_depth &&
                    x_block_mtype_ == xmd.match_type)
                {
                    if (xmd.block_gain >= params.stop_gain)
                        break;
                    if (gain_ratio > params.target_gain_ratio)
                        break;
                }
            }
            match_vect_.push_back(xmd); // place into vector of matches
        }
    } // for ri

    found_ridx_ = size_type(best_ri);
    x_d64_ = d64;

    if (best_ri != -1) // found some gain, validate it now
    {
        // assumed that if XOR compression c_level is at the highest
        const float bie_bits_per_int = 3.0f; // c_level_ < 6 ? 3.75f : 3.0f;
        const unsigned bie_limit =
                unsigned(float(bm::gap_max_bits) / bie_bits_per_int);

        unsigned xor_bc, xor_gc;
        const bm::word_t* ref_block = get_ref_block(size_type(best_ri), i, j);
        bool r_gap = BM_IS_GAP(ref_block);
        if (r_gap)
            ref_block = nb_blocks_vect_[size_type(best_ri)];
        found_block_xor_ = ref_block;

        // TODO: one pass operation?
        bm::bit_block_xor(tx_block, s_block, ref_block, d64);
        bm::bit_block_change_bc(tx_block, &xor_gc, &xor_bc);

        if (!xor_bc) // check if completely identical block?
        {
            x_best_metric_ = xor_bc;
            found_ridx_ = size_type(best_ri);
            x_block_mtype_ = rb_found = e_xor_match_BC;

            unsigned block_pos;
            bool found = bm::block_find_first_diff(s_block, ref_block, &block_pos);
            if (!found)
            {
                x_block_mtype_ = rb_found = e_xor_match_EQ; x_d64_ = 0;
            }
        }
        else // find the best matching metric (GC, BC, iBC, ..)
        {
            rb_found = best_metric(xor_bc, xor_gc, &x_best_metric_);

            // double check if XOR improves compression
            // with accounted serialization overhead
            //
            if (rb_found)
            {
                if (x_best_metric_ > bie_limit ||
                    get_s_block_best() < x_best_metric_)
                    return e_no_xor_match;

                unsigned gain = get_s_block_best() - x_best_metric_;
                gain *= 3; // use bit estimate (speculative)
                // gain should be greater than overhead for storing
                // reference data: xor token, digest-64, block idx
                unsigned gain_min = unsigned(sizeof(char) + sizeof(unsigned));
                if (d64 != ~0ull) // if mask is all 1s - it is not used
                    gain_min += (unsigned)sizeof(bm::id64_t);
                else
                {
                    if (x_best_metric_ <= 1)
                        return rb_found;
                }
                gain_min *= 8; // in bits
                if (gain > gain_min)
                    return rb_found;

                return e_no_xor_match;
            }
        }
    }
    return rb_found;
}

// --------------------------------------------------------------------------

template<typename BV>
typename xor_scanner<BV>::size_type xor_scanner<BV>::refine_match_chain()
{
    size_type match_size = 0;
    if (x_d64_ == ~0ull || !x_d64_)
        return match_size;
    bm::xor_complement_match mtype = get_best_match_type();
    match_size = (size_type)
        bm::greedy_refine_match_vector(
            chain_match_vect_, match_vect_, found_ridx_, x_d64_, mtype);
    return match_size;
}

// --------------------------------------------------------------------------

template<typename BV>
bool xor_scanner<BV>::compute_sim_model(xor_sim_model<BV> &sim_model,
                                        const bv_ref_vector_type& ref_vect,
                                        const bm::xor_sim_params& params)
{
    const bv_ref_vector_type* ref_vect_curr = this->ref_vect_; // save ref-vect

    ref_vect_ = &ref_vect;
    bool sim_ok = compute_sim_model(sim_model, params);

    ref_vect_ = ref_vect_curr; // restore state
    return sim_ok;
}

template<typename BV>
bool xor_scanner<BV>::compute_sim_model(bm::xor_sim_model<BV>& sim_model,
                                        const bm::xor_sim_params& params)
{
    BM_ASSERT(ref_vect_);

    sim_model.bv_blocks.clear(false);
    bool ret = ref_vect_->build_nb_digest_and_xor_matrix(sim_model.matr,
                                                         sim_model.bv_blocks);
    if (!ret)
        return ret;

    sync_nb_vect();

    typename bvector_type::enumerator en(sim_model.bv_blocks);
    for (size_type col = 0; en.valid(); ++en, ++col)
    {
        size_type nb = *en;
        compute_sim_model(sim_model.matr, nb, col, params);
    } // for en
    return true;
}

// --------------------------------------------------------------------------

template<typename BV>
void xor_scanner<BV>::compute_sim_model(
                typename xor_sim_model<BV>::matrix_chain_type &sim_model_matr,
                        size_type                 nb,
                        size_type                 nb_rank,
                        const bm::xor_sim_params& params)
{
    BM_ASSERT(nb_rank < sim_model_matr.cols());

    const float bie_bits_per_int = 3.0f; // c_level_ < 6 ? 3.75f : 3.0f;
    const unsigned bie_limit =
            unsigned(float(bm::gap_max_bits) / bie_bits_per_int);

    deoptimize_gap_blocks(nb, params);

    size_type rsize = ref_vect_->size();

    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);

    for (size_type ri=0; true; ++ri)
    {
        bm::block_match_chain<size_type>* m_row = sim_model_matr.row(ri);
        bm::block_match_chain<size_type>& bmc = m_row[nb_rank];
        bmc.nb = nb;
        bmc.chain_size = 0;
        bmc.match = e_no_xor_match;

        if (ri == rsize-1)
            break;

        const bm::word_t* s_block = get_ref_block(ri, i0, j0);
        if (!IS_VALID_ADDR(s_block))
            continue;

        if (BM_IS_GAP(s_block))
        {
            const bm::gap_word_t* gap_s_block = BMGAP_PTR(s_block);
            unsigned gc = bm::gap_length(gap_s_block);
            if (gc <= params.min_gaps)
                continue;
        }

        // compute s-block best metrics
        get_s_block_stats(ri);
        if (s_block_best_metric_ < 3)
            continue;

        // scan ref-vector plains for best similarity
        //
        bmc.match = search_best_xor_mask(s_block, ri, ri+1, rsize,
                                          i0, j0, xor_tmp_block_, params);

        // take index in the ref-vector (not translated to a plain number)
        //
        size_type ridx = found_ridx();
        bm::id64_t d64 = get_xor_digest();

        size_type chain_size = 0;
        if (d64 && d64 != ~0ULL) // something found
        {
            chain_size = refine_match_chain();
        }
        switch (bmc.match)
        {
        case e_no_xor_match:
            if (chain_size) // try chain compression for improve
            {
                match_pairs_vector_type& pm_vect = get_match_pairs();
                BM_ASSERT(chain_size == pm_vect.size());

                apply_xor_match_vector(xor_tmp_block_,
                                       s_block, ri,
                                       pm_vect, i0, j0);
                unsigned xor_bc, xor_gc;
                bm::bit_block_change_bc(xor_tmp_block_, &xor_gc, &xor_bc);

                bmc.match = best_metric(xor_bc, xor_gc, &x_best_metric_);
                if (bmc.match)
                {
                    if ((x_best_metric_ > bie_limit) ||
                        (get_s_block_best() < x_best_metric_))
                    {
                        bmc.match = e_no_xor_match;
                        continue;
                    }
                    for (size_type k = 0; k < chain_size; ++k)
                    {
                        const bm::match_pair& mp = pm_vect[k];
                        bmc.ref_idx[k] = (unsigned)mp.ref_idx;
                        bmc.xor_d64[k] = mp.xor_d64;
                        bmc.chain_size++;
                    } // for k
                }
            }
            break;
        case e_xor_match_EQ:
            bmc.chain_size++;
            bmc.ref_idx[0] = unsigned(ridx);
            break;
        default: // improving match found
            bmc.chain_size++;
            bmc.ref_idx[0] = unsigned(ridx);
            bmc.xor_d64[0] = d64;

            if (chain_size) // match chain needs no verification
            {
                match_pairs_vector_type& pm_vect = get_match_pairs();
                BM_ASSERT(chain_size == pm_vect.size());
                auto sz = pm_vect.size();
                for (size_type k = 0; k < sz; ++k)
                {
                    BM_ASSERT(k < 64);
                    const bm::match_pair& mp = pm_vect[k];
                    bmc.ref_idx[k+1] = (unsigned)mp.ref_idx;
                    bmc.xor_d64[k+1] = mp.xor_d64;
                    bmc.chain_size++;
                } // for k
            }

        } // switch

    } // for ri
}


// --------------------------------------------------------------------------

template<typename BV>
void xor_scanner<BV>::apply_xor_match_vector(
                       bm::word_t* target_xor_block,
                       const bm::word_t* s_block,
                       size_type s_ri,
                       const match_pairs_vector_type& pm_vect,
                       unsigned i, unsigned j) const BMNOEXCEPT
{
    bool s_gap = BM_IS_GAP(s_block);
    if (s_gap)
    {
        s_block = nb_blocks_vect_.at(s_ri);
        BM_ASSERT(s_block);
    }
    auto sz = pm_vect.size();
    for (typename match_pairs_vector_type::size_type k = 0; k < sz; ++k)
    {
        const bm::match_pair& mp = pm_vect[k];
        const bm::word_t* ref_block = get_ref_block(mp.ref_idx, i, j);
        if (BM_IS_GAP(ref_block))
        {
            ref_block = nb_blocks_vect_[mp.ref_idx];
            BM_ASSERT(ref_block);
        }
        if (!k)
            bm::bit_block_xor(target_xor_block, s_block, ref_block, mp.xor_d64);
        else
            bm::bit_block_xor(target_xor_block, ref_block, mp.xor_d64);
    } // for k
}

// --------------------------------------------------------------------------

template<typename BV>
bm::xor_complement_match
xor_scanner<BV>::best_metric(unsigned bc, unsigned gc,
                             unsigned* best_metric) BMNOEXCEPT
{
    BM_ASSERT(best_metric);
    unsigned ibc = bm::gap_max_bits - bc;
    if (!ibc)
    {
        *best_metric = gc;
        return e_xor_match_GC;
    }
    if (gc < bc) // GC < BC
    {
        if (gc <= ibc)
        {
            *best_metric = gc;
            return e_xor_match_GC;
        }
    }
    else // GC >= BC
    {
        if (bc <= ibc)
        {
            *best_metric = bc;
            return e_xor_match_BC;
        }
    }
    *best_metric = ibc;
    return e_xor_match_iBC;
}

// --------------------------------------------------------------------------


template<typename BV>
void xor_scanner<BV>::free_blocks() BMNOEXCEPT
{
    size_t sz = nb_blocks_vect_.size();
    for (size_t i = 0; i < sz; ++i)
    {
        bm::word_t* blk = nb_blocks_vect_[i];
        if (blk)
            alloc_.free_bit_block(blk);
    }
    nb_blocks_vect_.resize(0);
}

// --------------------------------------------------------------------------

template<typename BV>
void xor_scanner<BV>::deoptimize_gap_blocks(size_type nb,
                                    const xor_sim_params& params)
{
    size_type rsize = ref_vect_->size();
    BM_ASSERT(nb_blocks_vect_.size() == rsize);
    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);

    for (size_type ri=0; ri < rsize; ++ri)
    {
        const bm::word_t* block = get_ref_block(ri, i0, j0);
        if (BM_IS_GAP(block))
        {
            const bm::gap_word_t* gap_block = BMGAP_PTR(block);
            unsigned gc = bm::gap_length(gap_block);
            if (gc <= params.min_gaps)
                continue;

            bm::word_t* t_block = nb_blocks_vect_.at(ri);
            if (!t_block)
            {
                t_block = alloc_.alloc_bit_block();
                nb_blocks_vect_[ri] = t_block;
            }
            bm::gap_convert_to_bitset(t_block, BMGAP_PTR(block));
            block = t_block;
        }
        if (!IS_VALID_ADDR(block))
            continue;

        block_waves_xor_descr& x_descr = nb_xdescr_vect_[ri];
        unsigned gc, bc;
        bm::compute_s_block_descr(block, x_descr, &gc, &bc);
        nb_gc_vect_[ri] = gc;
        nb_bc_vect_[ri] = bc;
    } // for ri

}

// --------------------------------------------------------------------------

template<typename BV>
void xor_scanner<BV>::sync_nb_vect()
{
    size_type rsize = ref_vect_->size();
    if (nb_blocks_vect_.size() == rsize)
        return;
    free_blocks();
    nb_blocks_vect_.resize(rsize);
    bm::word_t** vect_data = nb_blocks_vect_.data();
    for (size_type i = 0; i < rsize; ++i)
        vect_data[i] = 0;
    nb_gc_vect_.resize(rsize);
    nb_bc_vect_.resize(rsize);
    nb_xdescr_vect_.resize(rsize);
}

// --------------------------------------------------------------------------

} // namespace bm

#endif
