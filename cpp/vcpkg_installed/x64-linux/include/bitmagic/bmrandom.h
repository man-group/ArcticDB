#ifndef BMRANDOM__H__INCLUDED__
#define BMRANDOM__H__INCLUDED__
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

/*! \file bmrandom.h
    \brief Generation of random subset 
*/

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif


#include "bmfunc.h"
#include "bmdef.h"

#include <stdlib.h>
#include <random>
#include <algorithm>

namespace bm
{


/*!
    Class implements algorithm for random subset generation.

    Implemented method tries to be fair, but doesn't guarantee
    true randomeness or absense of bias.

    Performace note: 
    Class holds temporary buffers and variables, so it is recommended to
    re-use instances over multiple calls.

    \ingroup setalgo
*/
template<class BV>
class random_subset
{
public:
    typedef BV                      bvector_type;
    typedef typename BV::size_type  size_type;
public:
    random_subset();
    ~random_subset();

    /// Get random subset of input vector
    ///
    /// @param bv_out - destination vector
    /// @param bv_in  - input vector
    /// @param sample_count  - number of bits to pick
    ///
    void sample(BV& bv_out, const BV& bv_in, size_type sample_count);
    

private:
    typedef 
        typename BV::blocks_manager_type  blocks_manager_type;

private:
    /// simple picking algorithm for small number of items
    /// in [first,last] range
    ///
    void simple_pick(BV& bv_out,
                      const BV&  bv_in,
                      size_type sample_count,
                      size_type first,
                      size_type last);

    void get_subset(BV& bv_out, 
                    const BV&  bv_in,
                    size_type  in_count,
                    size_type  sample_count);

    void get_block_subset(bm::word_t*       blk_out,
                          const bm::word_t* blk_src,
                          unsigned          count);
    static 
    unsigned process_word(bm::word_t*       blk_out, 
                          const bm::word_t* blk_src,
                          unsigned          nword,
                          unsigned          take_count) BMNOEXCEPT;

    static
    void get_random_array(bm::word_t*       blk_out, 
                          bm::gap_word_t*   bit_list,
                          unsigned          bit_list_size,
                          unsigned          count);
    static
    unsigned compute_take_count(unsigned bc,
                size_type in_count, size_type sample_count) BMNOEXCEPT;


private:
    random_subset(const random_subset&);
    random_subset& operator=(const random_subset&);
private:
    typename bvector_type::rs_index_type   rsi_; ///< RS index (block counts)
    bvector_type                           bv_nb_; ///< blocks vector
    bm::gap_word_t                         bit_list_[bm::gap_max_bits];
    bm::word_t*                            sub_block_;
};


///////////////////////////////////////////////////////////////////



template<class BV>
random_subset<BV>::random_subset()
{
    sub_block_ = new bm::word_t[bm::set_block_size];
}

template<class BV>
random_subset<BV>::~random_subset()
{
    delete [] sub_block_;
}

template<class BV>
void random_subset<BV>::sample(BV&       bv_out, 
                               const BV& bv_in, 
                               size_type sample_count)
{
    if (sample_count == 0)
    {
        bv_out.clear(true);
        return;
    }
    
    rsi_.init();
    bv_in.build_rs_index(&rsi_, &bv_nb_);
    
    size_type in_count = rsi_.count();
    if (in_count <= sample_count)
    {
        bv_out = bv_in;
        return;
    }
    
    float pick_ratio = float(sample_count) / float(in_count);
    if (pick_ratio < 0.054f)
    {
        size_type first, last;
        bool b = bv_in.find_range(first, last);
        if (!b)
            return;
        simple_pick(bv_out, bv_in, sample_count, first, last);
        return;
    }

    if (sample_count > in_count/2)
    {
        // build the complement vector and subtract it
        BV tmp_bv;
        size_type delta_count = in_count - sample_count;

        get_subset(tmp_bv, bv_in, in_count, delta_count);
        bv_out.bit_sub(bv_in, tmp_bv);
        return;
    }
    get_subset(bv_out, bv_in, in_count, sample_count);
}

template<class BV>
void random_subset<BV>::simple_pick(BV&        bv_out,
                                    const BV&  bv_in,
                                    size_type sample_count,
                                    size_type first,
                                    size_type last)
{
    bv_out.clear(true);

    std::random_device rd;
    #ifdef BM64ADDR
        std::mt19937_64 mt_rand(rd());
    #else
        std::mt19937 mt_rand(rd());
    #endif
    std::uniform_int_distribution<size_type> dist(first, last);

    while (sample_count)
    {
        size_type fidx;
        size_type ridx = dist(mt_rand); // generate random position
        
        BM_ASSERT(ridx >= first && ridx <= last);
        
        bool b = bv_in.find(ridx, fidx); // find next valid bit after random
        BM_ASSERT(b);
        if (b)
        {
            // set true if was false
            bool is_set = bv_out.set_bit_conditional(fidx, true, false);
            sample_count -= is_set;
            while (!is_set) // find next valid (and not set) bit
            {
                ++fidx;
                // searching always left to right may create a bias...
                b = bv_in.find(fidx, fidx);
                if (!b)
                    break;
                if (fidx > last)
                    break;
                is_set = bv_out.set_bit_conditional(fidx, true, false);
                sample_count -= is_set;
            } // while
        }
    } // while
}


template<class BV>
void random_subset<BV>::get_subset(BV&        bv_out,
                                   const BV&  bv_in,
                                   size_type  in_count,
                                   size_type  sample_count)
{
    bv_out.clear(true);
    bv_out.resize(bv_in.size());

    const blocks_manager_type& bman_in = bv_in.get_blocks_manager();
    blocks_manager_type& bman_out = bv_out.get_blocks_manager();

    bm::word_t* tmp_block = bman_out.check_allocate_tempblock();

    size_type first_nb, last_nb;
    bool b = bv_nb_.find_range(first_nb, last_nb);
    BM_ASSERT(b);
    if (!b)
        return;

    std::random_device rd;
    #ifdef BM64ADDR
        std::mt19937_64 mt_rand(rd());
    #else
        std::mt19937 mt_rand(rd());
    #endif
    std::uniform_int_distribution<size_type> dist_nb(first_nb, last_nb);

    size_type curr_sample_count = sample_count;
    for (unsigned take_count = 0; curr_sample_count; curr_sample_count -= take_count)
    {
        // pick block at random
        //
        size_type nb;
        size_type ridx = dist_nb(mt_rand); // generate random block idx
        BM_ASSERT(ridx >= first_nb && ridx <= last_nb);
        
        b = bv_nb_.find(ridx, nb); // find next valid nb
        if (!b)
        {
            b = bv_nb_.find(first_nb, nb);
            if (!b)
            {
                //b = bv_nb_.find(first_nb, nb);
                BM_ASSERT(!bv_nb_.any());
                //BM_ASSERT(b);
                // TODO: seems to be some rounding error, needs to be addressed
                return; // cannot find block
            }
        }
        bv_nb_.clear_bit_no_check(nb); // remove from blocks list

        // calculate proportinal sample count
        //
        unsigned bc = rsi_.count(nb);
        BM_ASSERT(bc && (bc <= 65536));
        take_count = compute_take_count(bc, in_count, sample_count);
        if (take_count > curr_sample_count)
            take_count = unsigned(curr_sample_count);
        BM_ASSERT(take_count);
        if (!take_count)
            continue;
        {
            unsigned i0, j0;
            bm::get_block_coord(nb, i0, j0);
            const bm::word_t* blk_src = bman_in.get_block(i0, j0);
            BM_ASSERT(blk_src);

            // allocate target block
            bm::word_t* blk_out = bman_out.get_block_ptr(i0, j0);
            BM_ASSERT(!blk_out);
            if (blk_out)
            {
                blk_out = bman_out.deoptimize_block(nb);
            }
            else
            {
                blk_out = bman_out.get_allocator().alloc_bit_block();
                bman_out.set_block(nb, blk_out);
            }
            if (take_count == bc) // whole block take (strange)
            {
                // copy the whole src block
                if (BM_IS_GAP(blk_src))
                    bm::gap_convert_to_bitset(blk_out, BMGAP_PTR(blk_src));
                else
                    bm::bit_block_copy(blk_out, blk_src);
                continue;
            }
            bm::bit_block_set(blk_out, 0);
            if (bc < 4096) // use array shuffle
            {
                unsigned arr_len;
                // convert source block to bit-block
                if (BM_IS_GAP(blk_src))
                {
                    arr_len = bm::gap_convert_to_arr(bit_list_,
                                                 BMGAP_PTR(blk_src),
                                                 bm::gap_max_bits);
                }
                else // bit-block
                {
                    arr_len = bm::bit_block_convert_to_arr(bit_list_, blk_src, 0);
                }
                BM_ASSERT(arr_len);
                get_random_array(blk_out, bit_list_, arr_len, take_count);
            }
            else // dense block
            {
                // convert source block to bit-block
                if (BM_IS_GAP(blk_src))
                {
                    bm::gap_convert_to_bitset(tmp_block, BMGAP_PTR(blk_src));
                    blk_src = tmp_block;
                }
                // pick random bits source block to target
                get_block_subset(blk_out, blk_src, take_count);
            }
        }
    } // for
}

template<class BV>
unsigned random_subset<BV>::compute_take_count(
                                    unsigned bc,
                                    size_type in_count,
                                    size_type sample_count) BMNOEXCEPT
{
    BM_ASSERT(sample_count);
    float block_percent = float(bc) / float(in_count);
    float bits_to_take = float(sample_count) * block_percent;
    bits_to_take += 0.99f;
    unsigned to_take = unsigned(bits_to_take);
    if (to_take > bc)
        to_take = bc;
    if (!to_take)
        to_take = unsigned(sample_count);

    return to_take;
}


template<class BV>
void random_subset<BV>::get_block_subset(bm::word_t*       blk_out,
                                          const bm::word_t* blk_src,
                                          unsigned          take_count)
{
    for (unsigned rounds = 0; take_count && rounds < 10; ++rounds)
    {
        // pick random scan start and scan direction
        //
        unsigned i = unsigned(rand()) % bm::set_block_size;
        unsigned new_count;

        for (; i < bm::set_block_size && take_count; ++i)
        {
            if (blk_src[i] && (blk_out[i] != blk_src[i]))
            {
                new_count = process_word(blk_out, blk_src, i, take_count);
                take_count -= new_count;
            }
        } // for i

    } // for
    // if masked scan did not produce enough results..
    //
    if (take_count)
    {
        // Find all vacant bits: do logical (src SUB out)
        for (unsigned i = 0; i < bm::set_block_size; ++i)
        {
            sub_block_[i] = blk_src[i] & ~blk_out[i];
        }
        // now transform vacant bits to array, then pick random elements
        //
        unsigned arr_len = 
            bm::bit_block_convert_to_arr(bit_list_, sub_block_, 0);
                           //                   bm::gap_max_bits, 
                           //                   bm::gap_max_bits,
                           //                   0);
        BM_ASSERT(arr_len);
        get_random_array(blk_out, bit_list_, arr_len, take_count);        
    }
}

template<class BV>
unsigned random_subset<BV>::process_word(bm::word_t*       blk_out, 
                                         const bm::word_t* blk_src,
                                         unsigned          nword,
                                         unsigned          take_count) BMNOEXCEPT
{
    unsigned new_bits, mask;
    do 
    {    
        mask = unsigned(rand());
        mask ^= mask << 16;
    } while (mask == 0);

    std::random_device rd;
    #ifdef BM64ADDR
        std::mt19937_64 mt_rand(rd());
    #else
        std::mt19937 mt_rand(rd());
    #endif
    unsigned src_rand = blk_src[nword] & mask;
    new_bits = src_rand & ~blk_out[nword];
    if (new_bits)
    {
        unsigned new_count = bm::word_bitcount(new_bits);

        // check if we accidentally picked more bits than needed
        if (new_count > take_count)
        {
            BM_ASSERT(take_count);

            unsigned char blist[64];
            unsigned arr_size = bm::bitscan(new_bits, blist);
            BM_ASSERT(arr_size == new_count);
            std::shuffle(blist, blist + arr_size, mt_rand);
            unsigned value = 0;
            for (unsigned j = 0; j < take_count; ++j)
            {
                value |= (1u << blist[j]);
            }
            new_bits = value;
            new_count = take_count;

            BM_ASSERT(bm::word_bitcount(new_bits) == take_count);
            BM_ASSERT((new_bits & ~blk_src[nword]) == 0);
        }

        blk_out[nword] |= new_bits;
        return new_count;
    }
    return 0;    
}


template<class BV>
void random_subset<BV>::get_random_array(bm::word_t*       blk_out, 
                                         bm::gap_word_t*   bit_list,
                                         unsigned          bit_list_size,
                                         unsigned          count)
{
    std::random_device rd;
    #ifdef BM64ADDR
        std::mt19937_64 mt_rand(rd());
    #else
        std::mt19937 mt_rand(rd());
    #endif
    std::shuffle(bit_list, bit_list + bit_list_size, mt_rand);
    for (unsigned i = 0; i < count; ++i)
    {
        bm::set_bit(blk_out, bit_list[i]);
    }
}

} // namespace


#endif
