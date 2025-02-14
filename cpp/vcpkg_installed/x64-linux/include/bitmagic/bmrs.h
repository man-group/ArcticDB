#ifndef BMRS__H__INCLUDED__
#define BMRS__H__INCLUDED__
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

/*! \file bmrs.h
    \brief Rank-Select index structures
*/

namespace bm
{

/**
    @brief Rank-Select acceleration index
 
    Index uses two-level acceleration structure:
    bcount - running total popcount for all (possible) blocks
    (missing blocks give duplicate counts as POPCNT(N-1) + 0).
    subcount - sub-count inside blocks
 
    @ingroup bvector
    @internal
*/
template<typename BVAlloc>
class rs_index
{
public:
    typedef BVAlloc        bv_allocator_type;
    
#ifdef BM64ADDR
    typedef bm::id64_t                                   size_type;
    typedef bm::id64_t                                   block_idx_type;
    typedef bm::id64_t                                   sblock_count_type;
#else
    typedef bm::id_t                                     size_type;
    typedef bm::id_t                                     block_idx_type;
    typedef bm::id_t                                     sblock_count_type;
#endif

    typedef bm::pair<bm::gap_word_t, bm::gap_word_t> sb_pair_type;

public:
    rs_index() : total_blocks_(0), max_sblock_(0) {}
    rs_index(const rs_index& rsi);
    
    /// init arrays to zeros
    void init() BMNOEXCEPT;

    /// copy rs index
    void copy_from(const rs_index& rsi);

    // -----------------------------------------------------------------

    /// set empty (no bits set super-block)
    void set_null_super_block(unsigned i) BMNOEXCEPT;
    
    /// set FULL (all bits set super-block)
    void set_full_super_block(unsigned i) BMNOEXCEPT;
    
    /// set super block count
    void set_super_block(unsigned i, size_type bcount) BMNOEXCEPT;
    
    /// return bit-count in super-block
    unsigned get_super_block_count(unsigned i) const BMNOEXCEPT;

    /// return running bit-count in super-block
    size_type get_super_block_rcount(unsigned i) const BMNOEXCEPT;
    
    /// return number of super-blocks
    size_type super_block_size() const BMNOEXCEPT;
    
    /// Find super-block with specified rank
    unsigned find_super_block(size_type rank) const BMNOEXCEPT;
    
    
    /// set size of true super-blocks (not NULL and not FFFFF)
    void resize_effective_super_blocks(size_type sb_eff);
    
    /// Add block list belonging to one super block
    void register_super_block(unsigned i,
                              const unsigned* bcount,
                              const bm::id64_t* sub_count);

    /// find block position and sub-range for the specified rank
    bool find(size_type* rank,
              block_idx_type* nb, bm::gap_word_t* sub_range) const;
    
    /// return sub-clock counts (64-bit for additional information)
    bm::id64_t sub_count(block_idx_type nb) const BMNOEXCEPT;


    // -----------------------------------------------------------------
    
    /// reserve the capacity for block count
    void resize(block_idx_type new_size);
    
    /// set total blocks
    void set_total(size_type t) { total_blocks_ = t; }
    
    /// get total blocks
    size_type get_total() const { return total_blocks_; }
    
    /// return bit-count for specified block
    unsigned count(block_idx_type nb) const;
    
    /// return total bit-count for the index
    size_type count() const;

    /// return running bit-count for specified block
    size_type rcount(block_idx_type nb) const;
    
    /// determine the sub-range within a bit-block
    static
    unsigned find_sub_range(unsigned block_bit_pos) BMNOEXCEPT;
    
    /// determine block sub-range for rank search
    bm::gap_word_t select_sub_range(block_idx_type nb,
                                    size_type& rank) const BMNOEXCEPT;
    
    /// find block position for the specified rank
    block_idx_type find(size_type rank) const BMNOEXCEPT;
    
private:
    typedef bm::heap_vector<sblock_count_type, bv_allocator_type, false>
                                                    sblock_count_vector_type;
    typedef bm::heap_vector<unsigned, bv_allocator_type, false>
                                                    sblock_row_vector_type;
    typedef bm::dynamic_heap_matrix<unsigned, bv_allocator_type>  blocks_matrix_type;

    typedef bm::dynamic_heap_matrix<bm::id64_t, bv_allocator_type>  blocks_matrix64_type;

private:
    unsigned                  sblock_rows_;
    sblock_count_vector_type  sblock_count_;   ///< super-block accumulated counts
    sblock_row_vector_type    sblock_row_idx_; ///< super-block row numbers
    blocks_matrix_type        block_matr_;     ///< blocks within super-blocks (matrix)
    blocks_matrix64_type      sub_block_matr_; ///< sub-block counts
    size_type                 total_blocks_;   ///< total bit-blocks in the index
    unsigned                  max_sblock_;     ///< max. superblock index
};

/**
    Precalculated decision table fdr interval selection
    @internal
 */
template<bool T> struct rs_intervals
{
    struct codes
    {
        const unsigned char _lut[65536] = {0, }; // nibble lookup table

        codes()
        {
            #if defined(BM64_SSE4) || defined(BM64_AVX2) || defined(BM64_AVX512)
            const unsigned cutoff_bias = rs3_half_span/8;
            #else
            const unsigned cutoff_bias = 0;
            #endif

            for (unsigned nbit_right = 0; nbit_right < 65536; ++nbit_right)
            {
                unsigned sub = (nbit_right <= rs3_border0) ? 0 :
                               (nbit_right > rs3_border1) ? 2 : 1;
                switch(sub)  // sub-range rank calc
                {
                case 0:
                    // |--x-----[0]-----------[1]----------|
                    if (nbit_right <= rs3_border0/2 + cutoff_bias)
                        sub = 0;
                    else // |--------[x]-----------[1]----------|
                        if (nbit_right == rs3_border0)
                            sub = 1;
                        else // |------x-[0]-----------[1]----------|
                            sub = 2;
                break;

                case 1:
                    // |--------[0]-x---------[1]----------|
                    if (nbit_right <= rs3_border0_1 + cutoff_bias)
                        sub = 3;
                    else
                    {
                        // |--------[0]-----------[x]----------|
                        if (nbit_right == rs3_border1)
                            sub = 4;
                        else
                        {
                            // |--------[0]--------x--[1]----------|
                            unsigned d1 = nbit_right - bm::rs3_border0_1;
                            unsigned d2 = rs3_border1 - nbit_right;
                            if (d2 < d1) // |--------[0]----|----x-[1]--------|
                                sub = 5;
                            else // |--------[0]----|-x----[1]----------|
                                sub = 6;
                        }
                    }
                break;

                case 2:
                // |--------[0]-----------[1]-x---*----|
                if (nbit_right <= rs3_border1_1)
                {
                    unsigned d1 = nbit_right - bm::rs3_border1;
                    unsigned d2 = (rs3_border1_1 + cutoff_bias) - nbit_right;
                    if (d1 < d2) // |--------[0]-----------[1]-x---*----|
                        sub = 7;
                    else // |--------[0]-----------[1]---x-*----|
                    {
                        if (nbit_right == rs3_border1_1)
                            sub = 8;
                        else
                            sub = 9;
                    }
                }
                else
                {
                    // |--------[0]-----------[1]----------x
                    if (nbit_right == bm::gap_max_bits-1)
                        sub = 10;
                    else
                    {
                        // |--------[0]-----------[1]-------x--|
                        BM_ASSERT(nbit_right > bm::rs3_border1_1);
                        unsigned d1 = nbit_right - bm::rs3_border1_1;
                        unsigned d2 = bm::gap_max_bits - nbit_right;
                        if (d2 < d1) // |--------[0]----------[1]----*---x-|
                            sub = 11;
                        else // |--------[0]----------[1]----*-x---|
                            sub = 12;
                    }
                }
                break;

                default:
                    BM_ASSERT(0);
                    sub = 0;
                } // switch

                bm::set_nibble(
                    const_cast<unsigned char*>(_lut),
                    nbit_right, (unsigned char)sub);

                BM_ASSERT((unsigned char)sub ==
                 bm::get_nibble(const_cast<unsigned char*>(_lut), nbit_right));

            } // for
        }
    };
    static codes _c;
};
template<bool T> typename rs_intervals<T>::codes rs_intervals<T>::_c;


//---------------------------------------------------------------------
//
//---------------------------------------------------------------------

template<typename BVAlloc>
rs_index<BVAlloc>::rs_index(const rs_index<BVAlloc>& rsi)
{
    copy_from(rsi);
}

//---------------------------------------------------------------------


template<typename BVAlloc>
void rs_index<BVAlloc>::init() BMNOEXCEPT
{
    sblock_count_.resize(0);
    sblock_row_idx_.resize(0);
    block_matr_.resize(0, 0, false);
    sub_block_matr_.resize(0, 0, false);
    
    total_blocks_ = sblock_rows_ = max_sblock_ = 0;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::resize(block_idx_type new_size)
{
    sblock_rows_ = 0;
    
    block_idx_type sblock_count = (new_size >> bm::set_array_shift) + 3;
    BM_ASSERT(sblock_count == (new_size / 256) + 3);
    sblock_count_.resize(sblock_count);
    sblock_row_idx_.resize(sblock_count);
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::copy_from(const rs_index& rsi)
{
    sblock_rows_ = rsi.sblock_rows_;
    sblock_count_ = rsi.sblock_count_;
    sblock_row_idx_ = rsi.sblock_row_idx_;
    block_matr_ = rsi.block_matr_;
    sub_block_matr_ = rsi.sub_block_matr_;

    total_blocks_ = rsi.total_blocks_;
    max_sblock_ = rsi.max_sblock_;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
unsigned rs_index<BVAlloc>::count(block_idx_type nb) const
{
    if (nb >= total_blocks_)
        return 0;
    unsigned i = unsigned(nb >> bm::set_array_shift);
    size_type sb_count = get_super_block_count(i);
    
    if (!sb_count)
        return 0;
    if (sb_count == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
        return bm::gap_max_bits;

    unsigned j = unsigned(nb &  bm::set_array_mask);

    unsigned row_idx = sblock_row_idx_[i+1];
    const unsigned* row = block_matr_.row(row_idx);
    unsigned bc;
    
    bc = (!j) ? row[j] : row[j] - row[j-1];
    return bc;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
typename rs_index<BVAlloc>::size_type
rs_index<BVAlloc>::count() const
{
    if (!total_blocks_)
        return 0;
    return sblock_count_[max_sblock_ + 1];
}

//---------------------------------------------------------------------

template<typename BVAlloc>
typename rs_index<BVAlloc>::size_type
rs_index<BVAlloc>::rcount(block_idx_type nb) const
{
    unsigned i = unsigned(nb >> bm::set_array_shift);
    size_type sb_rcount = i ? get_super_block_rcount(i-1) : i;

    unsigned sb_count = get_super_block_count(i);
    if (!sb_count)
        return sb_rcount;
    
    unsigned j = unsigned(nb &  bm::set_array_mask);
    if (sb_count == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
    {
        sb_count = (j+1) * bm::gap_max_bits;
        sb_rcount += sb_count;
        return sb_rcount;
    }
    
    unsigned row_idx = sblock_row_idx_[i+1];
    const unsigned* row = block_matr_.row(row_idx);
    unsigned bc = row[j];
    sb_rcount += bc;
    return sb_rcount;

}

//---------------------------------------------------------------------

template<typename BVAlloc>
unsigned rs_index<BVAlloc>::find_sub_range(unsigned block_bit_pos)  BMNOEXCEPT
{
    return (block_bit_pos <= rs3_border0) ? 0 :
            (block_bit_pos > rs3_border1) ? 2 : 1;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
bm::gap_word_t rs_index<BVAlloc>::select_sub_range(
                                            block_idx_type nb,
                                            size_type& rank) const BMNOEXCEPT
{
    bm::id64_t sub = sub_count(nb);
    unsigned sub_cnt = unsigned(sub);
    unsigned first = sub_cnt & 0xFFFF;
    unsigned second = sub_cnt >> 16;

    if (rank > first)
    {
        rank -= first;
        if (rank > second)
        {
            rank -= second;
            return rs3_border1 + 1;
        }
        else
            return rs3_border0 + 1;
    }
    return 0;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
typename rs_index<BVAlloc>::block_idx_type
rs_index<BVAlloc>::find(size_type rank) const BMNOEXCEPT
{
    BM_ASSERT(rank);

    unsigned i = find_super_block(rank);
    BM_ASSERT(i < super_block_size());
    
    size_type prev_rc = sblock_count_[i];
    size_type curr_rc = sblock_count_[i+1];
    size_type bc = curr_rc - prev_rc;
    
    BM_ASSERT(bc);

    unsigned j;
    rank -= prev_rc;
    if (bc == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
    {
        for (j = 0; j < bm::set_sub_array_size; ++j)
        {
            if (rank <= bm::gap_max_bits)
                break;
            rank -= bm::gap_max_bits;
        } // for j
    }
    else
    {
        unsigned row_idx = sblock_row_idx_[i+1];
        const unsigned* row = block_matr_.row(row_idx);
        BM_ASSERT(rank <= (bm::set_sub_array_size * bm::gap_max_bits));
        j = bm::lower_bound_u32(row, unsigned(rank), 0, bm::set_sub_array_size-1);
    }
    BM_ASSERT(j < bm::set_sub_array_size);

    block_idx_type nb = (i * bm::set_sub_array_size) + j;
    return nb;
}

//---------------------------------------------------------------------
template<typename BVAlloc>
bm::id64_t rs_index<BVAlloc>::sub_count(block_idx_type nb) const BMNOEXCEPT
{
    if (nb >= total_blocks_)
        return 0;
    
    unsigned i = unsigned(nb >> bm::set_array_shift);
    size_type sb_count = get_super_block_count(i);
    if (!sb_count)
        return 0;

    if (sb_count == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
    {
        unsigned first = rs3_border0 + 1;
        unsigned second = rs3_border1 - first + 1;
        bm::id64_t aux0 = bm::rs3_border0_1 + 1;
        bm::id64_t aux1 = bm::rs3_border1_1 + 1;
        return (aux0 << 32) | (aux1 << 48) | (second << 16) | first;
    }
    
    unsigned row_idx = sblock_row_idx_[i+1];
    const bm::id64_t* sub_row = sub_block_matr_.row(row_idx);
    return sub_row[nb &  bm::set_array_mask]; // j - coordinate element
}


//---------------------------------------------------------------------

template<typename BVAlloc>
bool rs_index<BVAlloc>::find(size_type* rank,
                             block_idx_type* nb,
                             bm::gap_word_t* sub_range) const
{
    BM_ASSERT(rank);
    BM_ASSERT(nb);
    BM_ASSERT(sub_range);

    unsigned i = find_super_block(*rank);
    if (i > max_sblock_)
        return false;
    
    size_type prev_rc = sblock_count_[i];
    size_type curr_rc = sblock_count_[i+1];
    size_type bc = curr_rc - prev_rc;
    
    BM_ASSERT(bc);

    unsigned j;
    *rank -= prev_rc;
    if (bc == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
    {
        // TODO: optimize
        size_type r = *rank;
        for (j = 0; j < bm::set_sub_array_size; ++j)
        {
            if (r <= bm::gap_max_bits)
                break;
            r -= bm::gap_max_bits;
        } // for j
        *rank = r;

        unsigned first = rs3_border0 + 1;
        unsigned second = rs3_border1 - first + 1;
        if (*rank > first)
        {
            *rank -= first;
            if (*rank > second)
            {
                *rank -= second;
                *sub_range = bm::rs3_border1 + 1;
            }
            else
            {
                *sub_range = bm::rs3_border0 + 1;
            }
        }
        else
        {
            *sub_range = 0;
        }
    }
    else
    {
        unsigned row_idx = sblock_row_idx_[i+1];
        const unsigned* row = block_matr_.row(row_idx);

        BM_ASSERT(*rank <= (bm::set_sub_array_size * bm::gap_max_bits));
        j = bm::lower_bound_u32(row, unsigned(*rank), 0, bm::set_sub_array_size-1);
        BM_ASSERT(j < bm::set_sub_array_size);
        if (j)
            *rank -= row[j-1];
        
        const bm::id64_t* sub_row = sub_block_matr_.row(row_idx);
        unsigned first = sub_row[j] & 0xFFFF;
        unsigned second = (sub_row[j] >> 16) & 0xFFFF;
        if (*rank > first)
        {
            *rank -= first;
            if (*rank > second)
            {
                *rank -= second;
                *sub_range = bm::rs3_border1 + 1;
            }
            else
            {
                *sub_range = bm::rs3_border0 + 1;
            }
        }
        else
        {
            *sub_range = 0;
        }
    }
    BM_ASSERT(j < bm::set_sub_array_size);
    *nb = (i * bm::set_sub_array_size) + j;
    
    return true;

}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::set_null_super_block(unsigned i) BMNOEXCEPT
{
    if (i > max_sblock_)
        max_sblock_ = i;
    sblock_count_[i+1] = sblock_count_[i];
    sblock_row_idx_[i+1] = 0U;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::set_full_super_block(unsigned i) BMNOEXCEPT
{
    set_super_block(i, bm::set_sub_array_size * bm::gap_max_bits);
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::set_super_block(unsigned i, size_type bcount) BMNOEXCEPT
{
    if (i > max_sblock_)
        max_sblock_ = i;

    sblock_count_[i+1] = sblock_count_[i] + bcount;
    if (bcount == (bm::set_sub_array_size * bm::gap_max_bits)) // FULL BLOCK
    {
        sblock_row_idx_[i+1] = 0;
    }
    else
    {
        sblock_row_idx_[i+1] = sblock_rows_;
        ++sblock_rows_;
    }
}

//---------------------------------------------------------------------

template<typename BVAlloc>
unsigned rs_index<BVAlloc>::get_super_block_count(unsigned i) const BMNOEXCEPT
{
    if (i > max_sblock_)
        return 0;
    size_type prev = sblock_count_[i];
    size_type curr = sblock_count_[i + 1];
    size_type cnt = curr - prev;
    BM_ASSERT(cnt <= (bm::set_sub_array_size * bm::gap_max_bits));
    return unsigned(cnt);
}

//---------------------------------------------------------------------

template<typename BVAlloc>
typename rs_index<BVAlloc>::size_type
rs_index<BVAlloc>::get_super_block_rcount(unsigned i) const BMNOEXCEPT
{
    if (i > max_sblock_)
        i = max_sblock_;
    return sblock_count_[i+1];
}

//---------------------------------------------------------------------

template<typename BVAlloc>
unsigned rs_index<BVAlloc>::find_super_block(size_type rank) const BMNOEXCEPT
{
    const sblock_count_type* bcount_arr = sblock_count_.begin();
    unsigned i;

    #ifdef BM64ADDR
        i = bm::lower_bound_u64(bcount_arr, rank, 1, max_sblock_+1);
    #else
        i = bm::lower_bound_u32(bcount_arr, rank, 1, max_sblock_+1);
    #endif
    return i-1;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
typename rs_index<BVAlloc>::size_type
rs_index<BVAlloc>::super_block_size() const BMNOEXCEPT
{

    size_type total_sblocks = (size_type)sblock_count_.size();
    if (total_sblocks)
        return max_sblock_ + 1;
    return 0;
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::resize_effective_super_blocks(size_type sb_eff)
{
    block_matr_.resize(sb_eff+1,     bm::set_sub_array_size);
    sub_block_matr_.resize(sb_eff+1, bm::set_sub_array_size);
}

//---------------------------------------------------------------------

template<typename BVAlloc>
void rs_index<BVAlloc>::register_super_block(unsigned i,
                                             const unsigned* bcount,
                                             const bm::id64_t* sub_count_in)
{
    BM_ASSERT(bcount);
    BM_ASSERT(sub_count_in);

    if (i > max_sblock_)
        max_sblock_ = i;

    
    sblock_row_idx_[i+1] = sblock_rows_;
    unsigned* row = block_matr_.row(sblock_rows_);
    bm::id64_t* sub_row = sub_block_matr_.row(sblock_rows_);
    ++sblock_rows_;

    unsigned bc = 0;
    for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
    {
        bc += bcount[j];
        row[j] = bc;
        sub_row[j] = sub_count_in[j];
        BM_ASSERT(bcount[j] >=
                     ((sub_count_in[j] >> 16) & bm::set_block_mask) +
                     (sub_count_in[j] & bm::set_block_mask));
    } // for j
    sblock_count_[i+1] = sblock_count_[i] + bc;
}

//---------------------------------------------------------------------

}
#endif
