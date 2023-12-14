#ifndef BM_BLOCKS__H__INCLUDED__
#define BM_BLOCKS__H__INCLUDED__
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


#include "bmfwd.h"

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4100)
#endif


namespace bm
{

/*!
   @brief bitvector blocks manager
        Embedded class managing bit-blocks on very low level.
        Includes number of functor classes used in different bitset algorithms. 
   @ingroup bvector
   @internal
*/
template<class Alloc>
class blocks_manager
{
public:
    template<typename TAlloc> friend class bvector;

    typedef Alloc allocator_type;
#ifdef BM64ADDR
    typedef bm::id64_t   id_type;
    typedef bm::id64_t   block_idx_type;
#else
    typedef bm::id_t     id_type;
    typedef bm::id_t     block_idx_type;
#endif
    typedef id_type size_type;


    /// Allocation arena for ReadOnly vectors
    ///
    /// @internal
    struct arena
    {
        void*                    a_ptr_;       ///< main allocated pointer
        bm::word_t***            top_blocks_;  ///< top descriptor
        bm::word_t*              blocks_;      ///< bit-blocks area
        bm::gap_word_t*          gap_blocks_;  ///< GAP blocks area
        bm::word_t**             blk_blks_;    ///< PTR sub-blocks area
        bm::bv_arena_statistics  st_;          ///< statistics and sizes

        /// Set all arena fields to zero
        void reset()
            { a_ptr_ = 0; top_blocks_ = 0; blocks_ = 0; gap_blocks_ = 0; blk_blks_ = 0;
              st_.reset(); }
    };


    /** Base functor class (block visitor)*/
    class bm_func_base
    {
    public:
        typedef id_type size_type;
        
        bm_func_base(blocks_manager& bman) BMNOEXCEPT : bm_(bman) {}

        void on_empty_top(unsigned /* top_block_idx*/ ) BMNOEXCEPT {}
        void on_empty_block(block_idx_type /* block_idx*/ )BMNOEXCEPT {}
    private:
        bm_func_base(const bm_func_base&);
        bm_func_base& operator=(const bm_func_base&);
    protected:
        blocks_manager&  bm_;
    };


    /** Base functor class connected for "constant" functors*/
    class bm_func_base_const
    {
    public:
        typedef id_type size_type;
        bm_func_base_const(const blocks_manager& bman) BMNOEXCEPT : bm_(bman) {}

        void on_empty_top(unsigned /* top_block_idx*/ ) BMNOEXCEPT {}
        void on_empty_block(block_idx_type /* block_idx*/ ) BMNOEXCEPT {}
    private:
        bm_func_base_const(const bm_func_base_const&) BMNOEXCEPT;
        bm_func_base_const& operator=(const bm_func_base_const&) BMNOEXCEPT;
    protected:
        const blocks_manager&  bm_;
    };


    /** Base class for bitcounting functors */
    class block_count_base : public bm_func_base_const
    {
    protected:
        block_count_base(const blocks_manager& bm) BMNOEXCEPT
            : bm_func_base_const(bm) {}

        bm::id_t block_count(const bm::word_t* block) const BMNOEXCEPT
        {
            return this->bm_.block_bitcount(block);
        }
    };


    /** Bitcounting functor */
    class block_count_func : public block_count_base
    {
    public:
        typedef id_type size_type;

        block_count_func(const blocks_manager& bm) BMNOEXCEPT
            : block_count_base(bm), count_(0) {}

        id_type count() const BMNOEXCEPT { return count_; }

        void operator()(const bm::word_t* block) BMNOEXCEPT
        {
            count_ += this->block_count(block);
        }
        void add_full(id_type c) BMNOEXCEPT { count_ += c; }
        void reset() BMNOEXCEPT { count_ = 0; }

    private:
        id_type count_;
    };
    

    /** Bitcounting functor filling the block counts array*/
    class block_count_arr_func : public block_count_base
    {
    public:
        typedef id_type size_type;

        block_count_arr_func(const blocks_manager& bm, unsigned* arr) BMNOEXCEPT
            : block_count_base(bm), arr_(arr), last_idx_(0) 
        {
            arr_[0] = 0;
        }

        void operator()(const bm::word_t* block, id_type idx) BMNOEXCEPT
        {
            while (++last_idx_ < idx)
                arr_[last_idx_] = 0;
            arr_[idx] = this->block_count(block);
            last_idx_ = idx;
        }

        id_type last_block() const BMNOEXCEPT { return last_idx_; }
        void on_non_empty_top(unsigned) BMNOEXCEPT {}

    private:
        unsigned*  arr_;
        id_type   last_idx_;
    };

    /** bit value change counting functor */
    class block_count_change_func : public bm_func_base_const
    {
    public:
        typedef id_type size_type;

        block_count_change_func(const blocks_manager& bm) BMNOEXCEPT
            : bm_func_base_const(bm),
                count_(0),
                prev_block_border_bit_(0)
        {}

        block_idx_type block_count(const bm::word_t* block,
                                   block_idx_type idx) BMNOEXCEPT
        {
            block_idx_type cnt = 0;
            id_type first_bit;
            
            if (IS_FULL_BLOCK(block) || (block == 0))
            {
                cnt = 1;
                if (idx)
                {
                    first_bit = block ? 1 : 0;
                    cnt -= !(prev_block_border_bit_ ^ first_bit);
                }
                prev_block_border_bit_ = block ? 1 : 0;
            }
            else
            {
                if (BM_IS_GAP(block))
                {
                    gap_word_t* gap_block = BMGAP_PTR(block);
                    cnt = bm::gap_length(gap_block) - 1;
                    if (idx)
                    {
                        first_bit = bm::gap_test_unr(gap_block, 0);
                        cnt -= !(prev_block_border_bit_ ^ first_bit);
                    }
                        
                    prev_block_border_bit_ = 
                        bm::gap_test_unr(gap_block, gap_max_bits-1);
                }
                else // bitset
                {
                    cnt = bm::bit_block_calc_change(block);
                    if (idx)
                    {
                        first_bit = block[0] & 1;
                        cnt -= !(prev_block_border_bit_ ^ first_bit);
                    }
                    prev_block_border_bit_ = 
                        block[set_block_size-1] >> ((sizeof(block[0]) * 8) - 1);
                    
                }
            }
            return cnt;
        }
        
        id_type count() const BMNOEXCEPT { return count_; }

        void operator()(const bm::word_t* block, block_idx_type idx) BMNOEXCEPT
        {
            count_ += block_count(block, idx);
        }

    private:
        id_type   count_;
        bm::id_t   prev_block_border_bit_;
    };


    /** Functor detects if any bit set*/
    class block_any_func : public bm_func_base_const
    {
    public:
        typedef id_type size_type;

        block_any_func(const blocks_manager& bm) BMNOEXCEPT
            : bm_func_base_const(bm) 
        {}

        bool operator()
                (const bm::word_t* block, block_idx_type /*idx*/) BMNOEXCEPT
        {
            if (BM_IS_GAP(block)) // gap block
                return (!gap_is_all_zero(BMGAP_PTR(block)));
            if (IS_FULL_BLOCK(block)) 
                return true;
            return (!bit_is_all_zero(block));
        }
    };

    /*! Change GAP level lengths functor */
    class gap_level_func : public bm_func_base
    {
    public:
        gap_level_func(blocks_manager& bm,
                       const gap_word_t* glevel_len) BMNOEXCEPT
            : bm_func_base(bm), glevel_len_(glevel_len)
        {
            BM_ASSERT(glevel_len);
        }

        void operator()(bm::word_t* block, block_idx_type idx)
        {
            blocks_manager& bman = this->bm_;
            
            if (!BM_IS_GAP(block))
                return;

            gap_word_t* gap_blk = BMGAP_PTR(block);

            // TODO: Use the same code as in the optimize functor
            if (gap_is_all_zero(gap_blk))
            {
                bman.set_block_ptr(idx, 0);
                bman.get_allocator().free_gap_block(gap_blk,
                                                    bman.glen());
            }
            else 
            if (gap_is_all_one(gap_blk))
            {
                bman.set_block_ptr(idx, FULL_BLOCK_FAKE_ADDR);
                bman.get_allocator().free_gap_block(gap_blk,
                                                    bman.glen());
                return;
            }

            unsigned len = bm::gap_length(gap_blk);
            int level = bm::gap_calc_level(len, glevel_len_);
            if (level == -1)
            {
                bm::word_t* blk = bman.get_allocator().alloc_bit_block();
                bman.set_block_ptr(idx, blk);
                bm::gap_convert_to_bitset(blk, gap_blk);
            }
            else
            {
                gap_word_t* gap_blk_new = 
                bman.allocate_gap_block(unsigned(level), gap_blk, glevel_len_);

                bm::word_t* p = (bm::word_t*) gap_blk_new;
                BMSET_PTRGAP(p);
                bman.set_block_ptr(idx, p);
            }
            bman.get_allocator().free_gap_block(gap_blk, bman.glen());
        }
        void on_non_empty_top(unsigned) {}

    private:
        const gap_word_t* glevel_len_;
    };

    /** Fill block with all-one bits functor */
    class block_one_func : public bm_func_base
    {
    public:
        block_one_func(blocks_manager& bm) BMNOEXCEPT : bm_func_base(bm) {}

        void operator()(bm::word_t* block, block_idx_type idx)
        {
            if (!IS_FULL_BLOCK(block))
                this->bm_.set_block_all_set(idx);
        }
    };

public:
    blocks_manager()
    : alloc_(Alloc())
    {
        ::memcpy(glevel_len_, bm::gap_len_table<true>::_len, sizeof(glevel_len_));
        top_block_size_ = 1;
    }

    blocks_manager(const gap_word_t* glevel_len, 
                   id_type           max_bits,
                   const Alloc&      alloc = Alloc())
        : max_bits_(max_bits),
          alloc_(alloc)
    {
        ::memcpy(glevel_len_, glevel_len, sizeof(glevel_len_));
        top_block_size_ = 1;
    }

    blocks_manager(const blocks_manager& blockman)
        : max_bits_(blockman.max_bits_),
          top_block_size_(blockman.top_block_size_),
          alloc_(blockman.alloc_)
    {
        ::memcpy(glevel_len_, blockman.glevel_len_, sizeof(glevel_len_));
        if (blockman.is_init())
        {
            if (blockman.arena_)
                this->copy_to_arena(blockman);
            else
                this->copy(blockman);
        }
    }
    
#ifndef BM_NO_CXX11
    blocks_manager(blocks_manager&& blockman) BMNOEXCEPT
        : max_bits_(blockman.max_bits_),
          top_block_size_(blockman.top_block_size_),
          alloc_(blockman.alloc_)
    {
        ::memcpy(glevel_len_, blockman.glevel_len_, sizeof(glevel_len_));
        move_from(blockman);
    }
#endif

    ~blocks_manager() BMNOEXCEPT
    {
        if (temp_block_)
            alloc_.free_bit_block(temp_block_);
        deinit_tree();
    }
    
    /*! \brief Swaps content 
        \param bm  another blocks manager
    */
    void swap(blocks_manager& bm) BMNOEXCEPT
    {
        BM_ASSERT(this != &bm);

        word_t*** btmp = top_blocks_;
        top_blocks_ = bm.top_blocks_;
        bm.top_blocks_ = btmp;

        bm::xor_swap(this->max_bits_, bm.max_bits_);
        bm::xor_swap(this->top_block_size_, bm.top_block_size_);

        arena* ar = arena_; arena_ = bm.arena_; bm.arena_ = ar;

        BM_ASSERT(sizeof(glevel_len_) / sizeof(glevel_len_[0]) == bm::gap_levels); // paranoiya check
        for (unsigned i = 0; i < bm::gap_levels; ++i)
            bm::xor_swap(glevel_len_[i], bm.glevel_len_[i]);
    }
    
    /*! \brief implementation of moving semantics
    */
    void move_from(blocks_manager& bm) BMNOEXCEPT
    {
        deinit_tree();
        swap(bm);
        alloc_ = bm.alloc_;
        if (!temp_block_)  // this does not have temp_block, borrow it from the donor
        {
            temp_block_ = bm.temp_block_;
            bm.temp_block_ = 0;
        }
    }
    

    void free_ptr(bm::word_t** ptr) BMNOEXCEPT
    {
        alloc_.free_ptr(ptr);
    }

    /**
        \brief Compute size of the block array needed to store bits
        \param bits_to_store - supposed capacity (number of bits)
        \return size of the top level block
    */
    unsigned compute_top_block_size(id_type bits_to_store) const BMNOEXCEPT
    {
        if (bits_to_store >= bm::id_max)  // working in full-range mode
            return bm::set_top_array_size;

        unsigned top_block_sz = (unsigned)
            (bits_to_store / (bm::set_block_size * sizeof(bm::word_t) *
                                                bm::set_sub_array_size * 8));
        if (top_block_sz < bm::set_sub_array_size) ++top_block_sz;
        return top_block_sz;
    }

    /**
        Returns current capacity (bits)
    */
    /*
    bm::id_t capacity() const
    {
        // arithmetic overflow protection...
        return top_block_size_ == bm::set_array_size ? bm::id_max :
            top_block_size_ * bm::set_array_size * bm::bits_in_block;
    }
    */


    /**
        \brief Finds block in 2-level blocks array  
        Specilized version of get_block(unsigned), returns an additional
        condition when there are no more blocks

        \param nb - Index of block (logical linear number)
        \param no_more_blocks - 1 if there are no more blocks at all
        \return block adress or NULL if not yet allocated
    */
    const bm::word_t*
    get_block(block_idx_type nb, int* no_more_blocks) const BMNOEXCEPT
    {
        BM_ASSERT(top_blocks_);
        unsigned i = unsigned(nb >> bm::set_array_shift);
        if (i >= top_block_size_)
        {
            *no_more_blocks = 1;
            return 0;
        }
        *no_more_blocks = 0;
        bm::word_t** blk_blk = top_blocks_[i];
        bm::word_t* ret;
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            ret = FULL_BLOCK_REAL_ADDR;
        }
        else
        {
            ret = blk_blk ? blk_blk[nb & bm::set_array_mask] : 0;
            if (ret == FULL_BLOCK_FAKE_ADDR)
                ret = FULL_BLOCK_REAL_ADDR;
        }
        return ret;
    }


    /**
    Find the next non-zero block starting from nb
    \param nb - block index
    \param deep_scan - flag to perform detailed bit-block analysis
    @return bm::set_total_blocks - no more blocks
    */
    block_idx_type
    find_next_nz_block(block_idx_type nb, bool deep_scan=true) const BMNOEXCEPT
    {
        if (is_init())
        {
            unsigned i,j;
            get_block_coord(nb, i, j);
            for (;i < top_block_size_; ++i)
            { 
                bm::word_t** blk_blk = top_blocks_[i];
                if (!blk_blk)
                { 
                    nb += bm::set_sub_array_size - j;
                }
                else
                   for (;j < bm::set_sub_array_size; ++j, ++nb)
                   {
                       bm::word_t* blk = blk_blk[j];
                       if (blk && !bm::check_block_zero(blk, deep_scan))
                           return nb;
                   } // for j
                j = 0;
            } // for i
        } // is_init()
        return bm::set_total_blocks;
    }

    /**
        \brief Finds block in 2-level blocks array
        \param i - top level block index
        \param j - second level block index
        \return block adress or NULL if not yet allocated
    */
    const bm::word_t* get_block(unsigned i, unsigned j) const BMNOEXCEPT
    {
        if (!top_blocks_ || i >= top_block_size_) return 0;
        const bm::word_t* const* blk_blk = top_blocks_[i];
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            return FULL_BLOCK_REAL_ADDR;
        const bm::word_t* ret = (blk_blk == 0) ? 0 : blk_blk[j];
        return (ret == FULL_BLOCK_FAKE_ADDR) ? FULL_BLOCK_REAL_ADDR : ret;
    }

    /**
        \brief Finds block in 2-level blocks array (unsinitized)
        \param i - top level block index
        \param j - second level block index
        \return block adress or NULL if not yet allocated
    */
    const bm::word_t* get_block_ptr(unsigned i, unsigned j) const BMNOEXCEPT
    {
        if (!top_blocks_ || i >= top_block_size_) return 0;

        const bm::word_t* const* blk_blk = top_blocks_[i];
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            return (bm::word_t*)blk_blk;
        return (blk_blk) ? blk_blk[j] : 0;
    }
    /**
        \brief Finds block in 2-level blocks array (unsinitized)
        \param i - top level block index
        \param j - second level block index
        \return block adress or NULL if not yet allocated
    */
    bm::word_t* get_block_ptr(unsigned i, unsigned j) BMNOEXCEPT
    {
        if (!top_blocks_ || i >= top_block_size_)
            return 0;
        bm::word_t* const* blk_blk = top_blocks_[i];
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            return (bm::word_t*)blk_blk;
        return (blk_blk) ? blk_blk[j] : 0;
    }


    /**
        \brief Function returns top-level block in 2-level blocks array
        \param i - top level block index
        \return block adress or NULL if not yet allocated
    */
    const bm::word_t* const * get_topblock(unsigned i) const BMNOEXCEPT
    {
        return (!top_blocks_ || i >= top_block_size_) ? 0 : top_blocks_[i];
    }

    /** 
        \brief Returns root block in the tree.
    */
    bm::word_t*** top_blocks_root() const BMNOEXCEPT
    {
        blocks_manager* bm = 
            const_cast<blocks_manager*>(this);
        return bm->top_blocks_root();
    }

    void set_block_all_set(block_idx_type nb)
    {
        unsigned i, j;
        get_block_coord(nb, i, j);
        set_block_all_set(i, j);
    }
    
    void set_block_all_set(unsigned i, unsigned j)
    {
        reserve_top_blocks(i+1);
        set_block_all_set_no_check(i, j);
    }

    void set_block_all_set_no_check(unsigned i, unsigned j)
    {
        bm::word_t* block = this->get_block_ptr(i, j);
        if (IS_VALID_ADDR(block))
        {
            if (BM_IS_GAP(block))
                alloc_.free_gap_block(BMGAP_PTR(block), glevel_len_);
            else
                alloc_.free_bit_block(block);
        }
        set_block_all_set_ptr(i, j);
    }
    
    /**
        Places new block into blocks table.
    */
    void set_block_all_set_ptr(unsigned i, unsigned j)
    {
        if (top_blocks_[i] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            return;
        if (!top_blocks_[i])
            alloc_top_subblock(i, 0);
        top_blocks_[i][j] = FULL_BLOCK_FAKE_ADDR;
    }


    /**
        set all-set block pointers for [start..end]
    */
    void set_all_set(block_idx_type nb, block_idx_type nb_to)
    {
        BM_ASSERT(nb <= nb_to);
        
        unsigned i, j, i_from, j_from, i_to, j_to;
        get_block_coord(nb, i_from, j_from);
        get_block_coord(nb_to, i_to, j_to);
        
        reserve_top_blocks(i_to+1);
        
        bm::word_t*** blk_root = top_blocks_root();

        if (i_from == i_to)  // same subblock
        {
            bm::word_t** blk_blk = blk_root[i_from];
            if (blk_blk != (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            {
                for (j = j_from; j <= j_to; ++j)
                    set_block_all_set_no_check(i_from, j);
            }
            return;
        }
        if (j_from > 0) // process first sub
        {
            bm::word_t** blk_blk = blk_root[i_from];
            if (blk_blk != (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            {
                for (j = j_from; j < bm::set_sub_array_size; ++j)
                    set_block_all_set_no_check(i_from, j); // TODO: optimize
            }
            ++i_from;
        }
        if (j_to < bm::set_sub_array_size-1) // process last sub
        {
            bm::word_t** blk_blk = blk_root[i_to];
            if (blk_blk != (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            {
                for (j = 0; j <= j_to; ++j)
                    set_block_all_set_no_check(i_to, j);
            }
            --i_to; // safe because (i_from == i_to) case is covered
        }

        // process all full sub-lanes
        //
        for (i = i_from; i <= i_to; ++i)
        {
            bm::word_t** blk_blk = blk_root[i];
            if (!blk_blk || blk_blk == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            {
                blk_root[i] = (bm::word_t**)FULL_BLOCK_FAKE_ADDR;
                continue;
            }
            j = 0;
            do
            {
                if (blk_blk[j] != FULL_BLOCK_FAKE_ADDR)
                    set_block_all_set_no_check(i, j);
            } while (++j < bm::set_sub_array_size);
        } // for i
    }

    /**
        set all-Zero block pointers for [start..end]
    */
    void set_all_zero(block_idx_type nb, block_idx_type nb_to) BMNOEXCEPT
    {
        BM_ASSERT(nb <= nb_to);
        
        unsigned i, j, i_from, j_from, i_to, j_to;
        get_block_coord(nb, i_from, j_from);

        if (i_from >= top_block_size_) // nothing to do
            return;

        get_block_coord(nb_to, i_to, j_to);

        if (i_to >= top_block_size_)
        {
            i_to = top_block_size_-1;
            j_to = bm::set_sub_array_size;
        }
                
        bm::word_t*** blk_root = top_blocks_root();
        
        if (i_from == i_to)  // same subblock
        {
            bm::word_t** blk_blk = blk_root[i_from];
            if (blk_blk)
            {
                j_to -= (j_to == bm::set_sub_array_size);
                for (j = j_from; j <= j_to; ++j)
                    zero_block(i_from, j); // TODO: optimization (lots of ifs in this loop)
            }
            return;
        }
        if (j_from > 0) // process first sub
        {
            bm::word_t** blk_blk = blk_root[i_from];
            if (blk_blk)
            {
                for (j = j_from; j < bm::set_sub_array_size; ++j)
                    zero_block(i_from, j); // TODO: optimize (zero_block is slo)
            }
            ++i_from;
        }
        if (j_to < bm::set_sub_array_size-1) // process last sub
        {
            bm::word_t** blk_blk = blk_root[i_to];
            if (blk_blk)
            {
                for (j = 0; j <= j_to; ++j)
                    zero_block(i_to, j);
            }
            --i_to; // safe because (i_from == i_to) case is covered
        }
        // process all full sub-lanes
        // TODO: loop unroll /SIMD
        for (i = i_from; i <= i_to; ++i)
        {
            bm::word_t** blk_blk = blk_root[i];
            if (!blk_blk)
                continue;
            if (blk_blk == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            {
                blk_root[i] = 0;
                continue;
            }
            j = 0;
            do
            {
                if (blk_blk[j])
                    zero_block(i, j);
            } while (++j < bm::set_sub_array_size);
        } // for i
    }


    /**
        Create(allocate) bit block. Old block (if exists) gets deleted.
    */
    bm::word_t* alloc_bit_block(block_idx_type nb)
    {
        bm::word_t* block = this->get_allocator().alloc_bit_block();
        bm::word_t* old_block = set_block(nb, block);
        if (IS_VALID_ADDR(old_block))
        {
            if (BM_IS_GAP(old_block))
                alloc_.free_gap_block(BMGAP_PTR(old_block), glen());
            else
                alloc_.free_bit_block(old_block);
        }
        return block;
    }

    /**
        Create all-zeros bit block. Old block (if exists) gets deleted.
    */
    bm::word_t* make_bit_block(block_idx_type nb)
    {
        bm::word_t* block = this->alloc_bit_block(nb);
        bit_block_set(block, 0);
        return block;
    }

    /**
        Create bit block as a copy of source block (bit or gap).
        Old block (if exists) gets deleted.
    */
    bm::word_t* copy_bit_block(block_idx_type nb,
                               const bm::word_t* block_src, int is_src_gap)
    {
        if (block_src == 0)
        {
            zero_block(nb);
            return 0;
        }
        bm::word_t* block = alloc_bit_block(nb);
        if (is_src_gap)
        {
            gap_word_t* gap_block = BMGAP_PTR(block_src);
            bm::gap_convert_to_bitset(block, gap_block);
        }
        else
        {            
            bm::bit_block_copy(block, block_src);
        }
        return block;
    }

    /**
        Clone GAP block from another GAP
        It can mutate into a bit-block if does not fit
    */
    bm::word_t* clone_gap_block(const bm::gap_word_t* gap_block, bool& gap_res)
    {
        BM_ASSERT(gap_block);
        
        bm::word_t* new_block;
        unsigned len = bm::gap_length(gap_block);
        int new_level = bm::gap_calc_level(len, glen());
        if (new_level < 0) // mutation
        {
            gap_res = false;
            new_block = alloc_.alloc_bit_block();
            bm::gap_convert_to_bitset(new_block, gap_block);
        }
        else
        {
            gap_res = true;
            new_block = (bm::word_t*)
                get_allocator().alloc_gap_block(unsigned(new_level), glen());
            ::memcpy(new_block, gap_block, len * sizeof(bm::gap_word_t));
            bm::set_gap_level(new_block, new_level);
        }
        return new_block;
    }
    
    /** Clone static known block, assign to i-j position. Old block must be NULL.
        @return new block address
    */
    void clone_gap_block(unsigned  i,
                         unsigned  j,
                         const bm::gap_word_t* gap_block,
                         unsigned len)
    {
        BM_ASSERT(get_block(i, j) == 0);

        int new_level = bm::gap_calc_level(len, this->glen());
        if (!new_level) // level 0, check if it is empty
        {
            if (bm::gap_is_all_zero(gap_block))
                return;
        }

        if (new_level < 0)
        {
            convert_gap2bitset(i, j, gap_block, len);
            return;
        }
        bm::gap_word_t* new_blk = allocate_gap_block(unsigned(new_level), gap_block);
        bm::set_gap_level(new_blk, new_level);
        bm::word_t* p = (bm::word_t*)new_blk;
        BMSET_PTRGAP(p);
        set_block(i, j, p, true); // set GAP block
    }
    
    /** Clone block, assign to i-j position
        @return new block address
    */
    bm::word_t* clone_assign_block(unsigned i, unsigned j,
                                   const bm::word_t* src_block,
                                   bool invert = false)
    {
        BM_ASSERT(src_block);
        bm::word_t* block = 0;
        if (BM_IS_GAP(src_block))
        {
            const bm::gap_word_t* gap_block = BMGAP_PTR(src_block);
            if (bm::gap_is_all_zero(gap_block))
                block = invert ? FULL_BLOCK_FAKE_ADDR : 0;
            else
            if (bm::gap_is_all_one(gap_block))
                block = invert ? 0 : FULL_BLOCK_FAKE_ADDR;
            else
            {
                bool is_gap;
                block = clone_gap_block(gap_block, is_gap);
                if (is_gap)
                {
                    if (invert)
                        bm::gap_invert(block);
                    BMSET_PTRGAP(block);
                }
                else
                {
                    if (invert) // TODO: implement inverted copy
                        bm::bit_invert((wordop_t*)block);
                }
            }
        }
        else // bit-block
        {
            if (src_block == FULL_BLOCK_FAKE_ADDR /*IS_FULL_BLOCK(blk_arg)*/)
                block = invert ? 0 : FULL_BLOCK_FAKE_ADDR;
            else
            {
                bm::bit_block_copy(block = alloc_.alloc_bit_block(), src_block);
                if (invert) // TODO: implement inverted copy
                    bm::bit_invert((wordop_t*) block);
            }
        }
        BM_ASSERT(top_blocks_[i][j] == 0);
        top_blocks_[i][j] = block;
        return block;
    }
    
    /**
    Attach the result of a GAP logical operation
    */
    void assign_gap(block_idx_type        nb,
                    const bm::gap_word_t* res,
                    unsigned              res_len,
                    bm::word_t*           blk,
                    gap_word_t*           tmp_buf)
    {
        unsigned i, j;
        get_block_coord(nb, i, j);
        assign_gap(i, j, res, res_len, blk, tmp_buf);
    }
    
    /** Attach the result of a GAP logical operation
        but check if it is all 000.. or FF..
    */
    void assign_gap_check(unsigned              i,
                          unsigned              j,
                        const bm::gap_word_t* res,
                        unsigned              res_len,
                        bm::word_t*           blk,
                        gap_word_t*           tmp_buf)
    {
        if (bm::gap_is_all_one(res))
        {
            zero_gap_block_ptr(i, j);
            set_block_ptr(i, j, FULL_BLOCK_FAKE_ADDR);
        }
        else
        {
            if (bm::gap_is_all_zero(res))
                zero_gap_block_ptr(i, j);
            else
                assign_gap(i, j, res, ++res_len, blk, tmp_buf);
        }
    }

    /** Attach the result of a GAP logical operation
    */
    void assign_gap(unsigned              i,
                    unsigned              j,
                    const bm::gap_word_t* res,
                    unsigned              res_len,
                    bm::word_t*           blk,
                    gap_word_t*           tmp_buf)
    {
        int level = bm::gap_level(BMGAP_PTR(blk));
        BM_ASSERT(level >= 0);
        unsigned threshold = unsigned(this->glen(unsigned(level)) - 4u);
        int new_level = bm::gap_calc_level(res_len, this->glen());
        if (new_level < 0)
        {
            convert_gap2bitset(i, j, res);
            return;
        }
        if (res_len > threshold) // GAP block needs next level up extension
        {
            BM_ASSERT(new_level >= 0);
            gap_word_t* new_blk = allocate_gap_block(unsigned(new_level), res);
            bm::set_gap_level(new_blk, new_level);
            bm::word_t* p = (bm::word_t*)new_blk;
            BMSET_PTRGAP(p);
            if (blk)
            {
                set_block_ptr(i, j, p);
                alloc_.free_gap_block(BMGAP_PTR(blk), this->glen());
            }
            else
            {
                set_block(i, j, p, true); // set GAP block
            }
            return;
        }
        // gap operation result is in the temporary buffer
        // we copy it back to the gap_block (target size/level - fits)
        BM_ASSERT(blk);
        bm::set_gap_level(tmp_buf, level);
        ::memcpy(BMGAP_PTR(blk), tmp_buf, res_len * sizeof(gap_word_t));
    }
    
    
    /**
        Copy block from another vector.
        Note:Target block is always replaced through re-allocation.
    */
    bm::word_t* copy_block(block_idx_type idx, const blocks_manager& bm_src)
    {
        const bm::word_t* block = bm_src.get_block(idx);
        if (block == 0)
        {
            zero_block(idx);
            return 0;
        }
        bm::word_t* new_blk = 0;
        bool is_gap = BM_IS_GAP(block);

        if (is_gap)
        {
            bm::gap_word_t* gap_block = BMGAP_PTR(block);
            new_blk = clone_gap_block(gap_block, is_gap); // is_gap - output
        }
        else
        {
            if (IS_FULL_BLOCK(block))
            {
                new_blk = FULL_BLOCK_FAKE_ADDR;
            }
            else
            {
                new_blk = alloc_.alloc_bit_block();
                bm::bit_block_copy(new_blk, block);
            }
        }
        set_block(idx, new_blk, is_gap);
        return new_blk;
    }


    /** 
        Function checks if block is not yet allocated, allocates it and sets to
        all-zero or all-one bits. 

        If content_flag == 1 (ALLSET block) requested and taken block is already ALLSET,
        function will return NULL

        initial_block_type and actual_block_type : 0 - bitset, 1 - gap
    */
    bm::word_t* check_allocate_block(block_idx_type nb,
                                     unsigned content_flag,
                                     int      initial_block_type,
                                     int*     actual_block_type,
                                     bool     allow_null_ret=true)
    {
        unsigned i, j;
        bm::get_block_coord(nb, i, j);
        bm::word_t*  block = this->get_block_ptr(i, j);

        if (!IS_VALID_ADDR(block)) // NULL block or ALLSET
        {
            // if we wanted ALLSET and requested block is ALLSET return NULL
            unsigned block_flag = IS_FULL_BLOCK(block);
            
            *actual_block_type = initial_block_type;
            if (block_flag == content_flag && allow_null_ret)
            {
                if (block_flag)
                    return FULL_BLOCK_FAKE_ADDR;
                return 0; // it means nothing to do for the caller
            }

            reserve_top_blocks(i + 1);
            if (initial_block_type == 0) // bitset requested
            {
                block = alloc_.alloc_bit_block();
                // initialize block depending on its previous status
                bm::bit_block_set(block, block_flag ? ~0u : 0u);
                set_block(i, j, block, false/*bit*/);
            }
            else // gap block requested
            {
                bm::gap_word_t* gap_block = allocate_gap_block(0);
                bm::gap_set_all(gap_block, bm::gap_max_bits, block_flag);
                set_block(i, j, (bm::word_t*)gap_block, true/*gap*/);
                return (bm::word_t*)gap_block;
            }
        }
        else // block already exists
        {
            *actual_block_type = BM_IS_GAP(block);
        }
        return block;
    }

    /**
        Function checks if block is not yet allocated, allocates and returns
    */
    bm::word_t* check_allocate_block(block_idx_type nb, int initial_block_type)
    {
        unsigned i, j;
        bm::get_block_coord(nb, i, j);
        bm::word_t* block = this->get_block_ptr(i, j);
        if (!IS_VALID_ADDR(block)) // NULL block or ALLSET
        {
            // if we wanted ALLSET and requested block is ALLSET return NULL
            unsigned block_flag = IS_FULL_BLOCK(block);
            if (initial_block_type == 0) // bitset requested
            {
                block = alloc_.alloc_bit_block();
                // initialize block depending on its previous status
                bit_block_set(block, block_flag ? 0xFF : 0);
                set_block(nb, block);
            }
            else // gap block requested
            {
                bm::gap_word_t* gap_block = allocate_gap_block(0);
                gap_set_all(gap_block, bm::gap_max_bits, block_flag);
                block = (bm::word_t*)gap_block;
                set_block(nb, block, true/*gap*/);
                BMSET_PTRGAP(block);
            }
        }
        return block;
    }


    /*! @brief Fills all blocks with 0.
        @param free_mem - if true function frees the resources (obsolete)
    */
    void set_all_zero(bool /*free_mem*/) BMNOEXCEPT
    {
        if (!is_init()) return;
        deinit_tree(); // TODO: optimization of top-level realloc
    }

    /*! Replaces all blocks with ALL_ONE block.
    */
    void set_all_one()
    {
        if (!is_init())
            init_tree();
        block_one_func func(*this);
        for_each_block(top_blocks_, top_block_size_,
                                bm::set_sub_array_size, func);
    }
    
    void free_top_subblock(unsigned nblk_blk) BMNOEXCEPT
    {
        BM_ASSERT(top_blocks_[nblk_blk]);
        if ((bm::word_t*)top_blocks_[nblk_blk] != FULL_BLOCK_FAKE_ADDR)
            alloc_.free_ptr(top_blocks_[nblk_blk], bm::set_sub_array_size);
        top_blocks_[nblk_blk] = 0;
    }


    bm::word_t** alloc_top_subblock(unsigned nblk_blk)
    {
        BM_ASSERT(!top_blocks_[nblk_blk] || top_blocks_[nblk_blk] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR);

        bm::word_t** p = (bm::word_t**)alloc_.alloc_ptr(bm::set_sub_array_size);
        ::memset(top_blocks_[nblk_blk] = p, 0,
                 bm::set_sub_array_size * sizeof(bm::word_t*));
        return p;
    }

    /**
        Allocate top sub-block if not allocated. If FULLBLOCK - deoptimize
    */
    bm::word_t** check_alloc_top_subblock(unsigned nblk_blk)
    {
        if (!top_blocks_[nblk_blk])
            return alloc_top_subblock(nblk_blk);
        if (top_blocks_[nblk_blk] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
            return alloc_top_subblock(nblk_blk, FULL_BLOCK_FAKE_ADDR);
        return top_blocks_[nblk_blk];
    }

    /**
        Places new block into descriptors table, returns old block's address.
        Old block is NOT deleted.
    */
    bm::word_t* set_block(block_idx_type nb, bm::word_t* block)
    {
        bm::word_t* old_block;
        
        if (!is_init())
            init_tree();

        // never use real full block adress, it may be instantiated in another DLL 
        // (unsafe static instantiation on windows)
        if (block == FULL_BLOCK_REAL_ADDR)
            block = FULL_BLOCK_FAKE_ADDR;

        unsigned nblk_blk = unsigned(nb >> bm::set_array_shift);
        reserve_top_blocks(nblk_blk+1);
        
        if (!top_blocks_[nblk_blk])
        {
            alloc_top_subblock(nblk_blk);
            old_block = 0;
        }
        else
        {
            if (top_blocks_[nblk_blk] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
                alloc_top_subblock(nblk_blk, FULL_BLOCK_FAKE_ADDR);
            old_block = top_blocks_[nblk_blk][nb & bm::set_array_mask];
        }

        // NOTE: block will be replaced without freeing, potential memory leak?
        top_blocks_[nblk_blk][nb & bm::set_array_mask] = block;
        return old_block;
    }

    /**
    Allocate an place new GAP block (copy of provided block)
    */
    bm::word_t* set_gap_block(block_idx_type      nb,
                          const gap_word_t* gap_block_src,
                          int               level)
    {
        if (level < 0)
        {
            bm::word_t* blk = alloc_.alloc_bit_block();
            set_block(nb, blk);
            gap_convert_to_bitset(blk, gap_block_src);
            return blk;
        }
        else
        {
            gap_word_t* gap_blk = alloc_.alloc_gap_block(
                                                unsigned(level), this->glen());
            gap_word_t* gap_blk_ptr = BMGAP_PTR(gap_blk);
            ::memcpy(gap_blk_ptr, gap_block_src, 
                                  gap_length(gap_block_src) * sizeof(gap_word_t));
            set_gap_level(gap_blk_ptr, level);
            set_block(nb, (bm::word_t*)gap_blk, true /*GAP*/);
            return (bm::word_t*)gap_blk;
        }
    }

    /**
        Places new block into descriptors table, returns old block's address.
        Old block is not deleted.
    */
    bm::word_t* set_block(block_idx_type nb, bm::word_t* block, bool gap)
    {
        unsigned i, j;
        get_block_coord(nb, i, j);
        reserve_top_blocks(i + 1);
        
        return set_block(i, j, block, gap);
    }


    /**
        Places new block into descriptors table, returns old block's address.
        Old block is not deleted.
    */
    bm::word_t* set_block(unsigned i, unsigned j, bm::word_t* block, bool gap)
    {
        BM_ASSERT(i < top_block_size_);
        bm::word_t* old_block;
        if (block)
        {
            if (block == FULL_BLOCK_REAL_ADDR)
                block = FULL_BLOCK_FAKE_ADDR;
            else
                block =
                (bm::word_t*) (gap ? BMPTR_SETBIT0(block) : BMPTR_CLEARBIT0(block));
        }

        // If first level array not yet allocated, allocate it and
        // assign block to it
        if (!top_blocks_[i])
        {
            top_blocks_[i] = (bm::word_t**)alloc_.alloc_ptr(bm::set_sub_array_size);
            ::memset(top_blocks_[i], 0, bm::set_sub_array_size * sizeof(void*));
            old_block = 0;
        }
        else
        {
            if (top_blocks_[i] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
                alloc_top_subblock(i, FULL_BLOCK_FAKE_ADDR);
            old_block = top_blocks_[i][j];
        }

        // NOTE: block will be replaced without freeing, potential memory leak?
        top_blocks_[i][j] = block;
        return old_block;
    }
    
    /**
        Allocate subblock and fill based on
    */
    bm::word_t** alloc_top_subblock(unsigned i, bm::word_t* addr)
    {
        BM_ASSERT(addr == 0 || addr == FULL_BLOCK_FAKE_ADDR);
        BM_ASSERT(top_blocks_[i] == 0 ||
                  top_blocks_[i] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR);
        bm::word_t** blk_blk =
            (bm::word_t**)alloc_.alloc_ptr(bm::set_sub_array_size);
        top_blocks_[i] = blk_blk;
        for (unsigned j = 0; j < bm::set_sub_array_size; j+=4)
            blk_blk[j+0] = blk_blk[j+1] = blk_blk[j+2] = blk_blk[j+3] = addr;
        return blk_blk;
    }
    
    /**
        Allocate and copy block.
        (no checks, no validation, may cause a memory leak if not used carefully)
    */
    void copy_bit_block(unsigned i, unsigned j, const bm::word_t* src_block)
    {
        BM_ASSERT(src_block);
        BM_ASSERT(src_block != FULL_BLOCK_FAKE_ADDR);
        
        reserve_top_blocks(i + 1);
        check_alloc_top_subblock(i);
        BM_ASSERT(top_blocks_[i][j]==0);
        bm::word_t* blk = top_blocks_[i][j] = alloc_.alloc_bit_block();
        bm::bit_block_stream(blk, src_block);
    }
    
    /**
        Optimize and copy bit-block
    */
    void opt_copy_bit_block(unsigned i, unsigned j,
                            const bm::word_t* src_block,
                            int               opt_mode,
                            bm::word_t*       tmp_block)
    {
        if (!opt_mode)
        {
            copy_bit_block(i, j, src_block);
            return;
        }
        
        reserve_top_blocks(i + 1);
        check_alloc_top_subblock(i);

        unsigned gap_count = bm::bit_block_calc_change(src_block);
        if (gap_count == 1) // solid block
        {
            if (*src_block) // 0xFFF...
            {
                BM_ASSERT(bm::is_bits_one((bm::wordop_t*)src_block));
                top_blocks_[i][j] = FULL_BLOCK_FAKE_ADDR;
                if (++j == bm::set_sub_array_size)
                {
                    validate_top_full(i);
                }
            }
            else
            {
                BM_ASSERT(bm::bit_is_all_zero(src_block));
                top_blocks_[i][j] = 0;
                validate_top_zero(i);
            }
            return;
        }
        // try to compress
        //
        unsigned threashold = this->glen(bm::gap_max_level)-4;
        if (gap_count < threashold) // compressable
        {
            BM_ASSERT(tmp_block);
            bm::gap_word_t* tmp_gap_blk = (gap_word_t*)tmp_block;
            
            unsigned len = bm::bit_to_gap(tmp_gap_blk, src_block, threashold);
            BM_ASSERT(len);
            int level = bm::gap_calc_level(len, this->glen());
            BM_ASSERT(level >= 0);
            bm::gap_word_t* gap_blk =
                        allocate_gap_block(unsigned(level), tmp_gap_blk);
            BM_ASSERT(top_blocks_[i][j]==0);
            top_blocks_[i][j] = (bm::word_t*)BMPTR_SETBIT0(gap_blk);
            return;
        }
        // non-compressable bit-block
        copy_bit_block(i, j, src_block);
    }

    /**
        Optimize bit-block at i-j position
    */
    void optimize_bit_block(unsigned i, unsigned j, int opt_mode)
    {
        bm::word_t* block = get_block_ptr(i, j);
        if (IS_VALID_ADDR(block))
        {
            if (BM_IS_GAP(block))
                return;
            
            unsigned gap_count = bm::bit_block_calc_change(block);
            if (gap_count == 1) // solid block
            {
                top_blocks_[i][j] = (*block) ? FULL_BLOCK_FAKE_ADDR : 0;
                return_tempblock(block);
                return;
            }
            if (opt_mode < 3) // less than opt_compress
                return;

            unsigned threashold = this->glen(bm::gap_max_level)-4;
            if (gap_count < threashold) // compressable
            {
                unsigned len;
                bm::gap_word_t tmp_gap_buf[bm::gap_equiv_len * 2];
                
                len = bm::bit_to_gap(tmp_gap_buf, block, threashold);
                BM_ASSERT(len);
                int level = bm::gap_calc_level(len, this->glen());
                BM_ASSERT(level >= 0);
                bm::gap_word_t* gap_blk =
                         allocate_gap_block(unsigned(level), tmp_gap_buf);
                top_blocks_[i][j] = (bm::word_t*)BMPTR_SETBIT0(gap_blk);
                return_tempblock(block);
            }
        }
    }
    


    /**
        Places new block into blocks table.
    */
    inline
    void set_block_ptr(block_idx_type nb, bm::word_t* block)
    {
        BM_ASSERT((nb >> bm::set_array_shift) < top_block_size_);
        BM_ASSERT(is_init());
        BM_ASSERT(top_blocks_[nb >> bm::set_array_shift]);
        
        unsigned i = unsigned(nb >> bm::set_array_shift);
        if (top_blocks_[i] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
        {
            if (block == FULL_BLOCK_FAKE_ADDR)
                return;
            alloc_top_subblock(i, FULL_BLOCK_FAKE_ADDR);
        }

        top_blocks_[i][nb & bm::set_array_mask] =
            (block == FULL_BLOCK_REAL_ADDR) ? FULL_BLOCK_FAKE_ADDR : block;
    }

    /**
        Places new block into blocks table.
    */
    BMFORCEINLINE
    void set_block_ptr(unsigned i, unsigned j, bm::word_t* block) BMNOEXCEPT
    {
        BM_ASSERT(is_init());
        BM_ASSERT(i < top_block_size_);
        BM_ASSERT(top_blocks_[i]);

        top_blocks_[i][j] =
            (block == FULL_BLOCK_REAL_ADDR) ? FULL_BLOCK_FAKE_ADDR : block;
    }


    /** 
        \brief Converts block from type gap to conventional bitset block.
        \param nb - Block's index. 
        \param gap_block - Pointer to the gap block, if NULL block nb is taken
        \return new bit block's memory
    */
    bm::word_t* convert_gap2bitset(block_idx_type nb, const gap_word_t* gap_block=0)
    {
        BM_ASSERT(is_init());
        
        unsigned i, j;
        get_block_coord(nb, i, j);
        reserve_top_blocks(i);
        
        return convert_gap2bitset(i, j, gap_block);
    }

    /**
        \brief Converts block from type gap to conventional bitset block.
        \param i - top index.
        \param j - secondary index.
        \param gap_block - Pointer to the gap block, if NULL block [i,j] is taken
        \param len - gap length
        \return new bit block's memory
    */
    bm::word_t* convert_gap2bitset(unsigned i, unsigned j,
                                   const gap_word_t* gap_block=0,
                                   unsigned len = 0)
    {
        BM_ASSERT(is_init());
        
        if (!top_blocks_[i])
            alloc_top_subblock(i);
        bm::word_t* block = top_blocks_[i][j];
        gap_block = gap_block ? gap_block : BMGAP_PTR(block);

        BM_ASSERT(IS_VALID_ADDR((bm::word_t*)gap_block));

        bm::word_t* new_block = alloc_.alloc_bit_block();
        bm::gap_convert_to_bitset(new_block, gap_block, len);
        
        top_blocks_[i][j] = new_block;

        // new block will replace the old one(no deletion)
        if (block)
            alloc_.free_gap_block(BMGAP_PTR(block), glen());

        return new_block;
    }


    /**
        Make sure block turns into true bit-block if it is GAP or a full block
        @return bit-block pointer
    */
    bm::word_t* deoptimize_block(block_idx_type nb)
    {
        unsigned i, j;
        bm::get_block_coord(nb, i, j);
        return deoptimize_block(i, j, false);
    }

    /** deoptimize block and return bit-block ptr
        can return NULL if block does not exists or allocate (if requested)
    */
    bm::word_t* deoptimize_block(unsigned i, unsigned j, bool alloc)
    {
        bm::word_t* block = this->get_block_ptr(i, j);
        if (!block && alloc)
        {
            reserve_top_blocks(i+1);
            if (!top_blocks_[i])
            {
                alloc_top_subblock(i, 0);
            }
            bm::word_t* new_block = alloc_.alloc_bit_block();
            bm::bit_block_set(new_block, 0u);
            set_block_ptr(i, j, new_block);
            return new_block;
        }
        return deoptimize_block_no_check(block, i, j);
    }

    bm::word_t* deoptimize_block_no_check(bm::word_t* block,
                                         unsigned i, unsigned j)
    {
        BM_ASSERT(block);
        if (BM_IS_GAP(block))
        {
            gap_word_t* gap_block = BMGAP_PTR(block);
            bm::word_t* new_block = alloc_.alloc_bit_block();
            bm::gap_convert_to_bitset(new_block, gap_block);
            alloc_.free_gap_block(gap_block, this->glen());

            set_block_ptr(i, j, new_block);
            return new_block;
        }
        if (IS_FULL_BLOCK(block))
        {
            if (top_blocks_[i] == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
                alloc_top_subblock(i, FULL_BLOCK_FAKE_ADDR);

            bm::word_t* new_block = alloc_.alloc_bit_block();
            bm::bit_block_set(new_block, ~0u);
            set_block_ptr(i, j, new_block);
            return new_block;
        }
        return block;
    }



    /**
        Free block, make it zero pointer in the tree
    */
    void zero_block(block_idx_type nb) BMNOEXCEPT
    {
        unsigned i, j;
        get_block_coord(nb, i, j);
        if (!top_blocks_ || i >= top_block_size_)
            return;
        zero_block(i, j);
    }
    

    /**
    Free block, make it zero pointer in the tree
    */
    void zero_block(unsigned i, unsigned j) BMNOEXCEPT
    {
        BM_ASSERT(top_blocks_ && i < top_block_size_);
        
        bm::word_t** blk_blk = top_blocks_[i];
        if (blk_blk)
        {
            if (blk_blk == (bm::word_t**)FULL_BLOCK_FAKE_ADDR)
                blk_blk = alloc_top_subblock(i, FULL_BLOCK_FAKE_ADDR);
            
            bm::word_t* block = blk_blk[j];
            blk_blk[j] = 0;
            if (IS_VALID_ADDR(block))
            {
                if (BM_IS_GAP(block))
                {
                    alloc_.free_gap_block(BMGAP_PTR(block), glen());
                }
                else
                {
                    alloc_.free_bit_block(block);
                }
            }

            if (j == bm::set_sub_array_size-1)
            {
                // back scan if top sub-block can also be dropped
                do {
                    if (blk_blk[--j])
                        return;
                    if (!j)
                    {
                        alloc_.free_ptr(top_blocks_[i], bm::set_sub_array_size);
                        top_blocks_[i] = 0;
                        return;
                    }
                } while (1);
            }
        }
    }
    
    /**
    Free block, make it zero pointer in the tree
    */
    void zero_gap_block_ptr(unsigned i, unsigned j) BMNOEXCEPT
    {
        BM_ASSERT(top_blocks_ && i < top_block_size_);
        
        bm::word_t** blk_blk = top_blocks_[i];
        bm::word_t* block = blk_blk[j];

        BM_ASSERT(blk_blk);
        BM_ASSERT(BM_IS_GAP(block));

        blk_blk[j] = 0;
        alloc_.free_gap_block(BMGAP_PTR(block), glen());
    }


    /**
        Count number of bits ON in the block
    */
    static
    bm::id_t block_bitcount(const bm::word_t* block) BMNOEXCEPT
    {
        BM_ASSERT(block);
        id_t count;
        if (BM_IS_GAP(block))
            count = bm::gap_bit_count_unr(BMGAP_PTR(block));
        else // bitset
            count = (IS_FULL_BLOCK(block)) ? bm::bits_in_block
                                           : bm::bit_block_count(block);
        return count;
    }

    /**
        Bit count all blocks to determine if it is very sparse
    */
    bool is_sparse_sblock(unsigned i, unsigned sparse_cut_off) const BMNOEXCEPT
    {
        if (!sparse_cut_off)
            return false;
        const unsigned non_sparse_cut_off = sparse_cut_off * bm::set_sub_array_size;

        BM_ASSERT(i < top_block_size());
        word_t*** blk_root = top_blocks_root();
        if (!blk_root)
            return false;
        bm::word_t** blk_blk = blk_root[i];
        if (!blk_blk || (bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            return false;
        bm::id_t cnt_sum(0), effective_blocks(0), gap_len_sum(0);
        for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
        {
            const bm::word_t* blk = blk_blk[j]; //  this->get_block(i, j);
            if (blk == FULL_BLOCK_FAKE_ADDR)
                return false;
            if (blk)
            {
                bm::id_t cnt;
                if (BM_IS_GAP(blk))
                {
                    const bm::gap_word_t* gp = BMGAP_PTR(blk);
                    cnt = bm::gap_bit_count_unr(gp);
                    gap_len_sum += bm::gap_length(gp);
                }
                else // bitset
                {
                    cnt = bm::bit_block_count(blk);
                }
                if (cnt)
                {
                    ++effective_blocks;
                    cnt_sum += cnt;
                    if (cnt_sum > non_sparse_cut_off) // too many bits set
                        return false;
                }
            }
        } // for j

        BM_ASSERT(effective_blocks <= bm::set_sub_array_size);
        if (effective_blocks > 1)
        {
            if (cnt_sum < 5) // super-duper sparse ...
                return false;
            
            bm::id_t blk_avg = cnt_sum / effective_blocks;
            if (blk_avg <= sparse_cut_off)
            {
                if (gap_len_sum)
                {
                    gap_len_sum += effective_blocks * 3;
                    if (gap_len_sum < cnt_sum)
                        return false;
                }
                return true;
            }
        }
        return false;
    }

    /**
        \brief Extends GAP block to the next level or converts it to bit block.
        \param nb - Block's linear index.
        \param blk - Blocks's pointer 

        \return new GAP block pointer or NULL if block type mutated into NULL
    */
    bm::gap_word_t* extend_gap_block(block_idx_type nb, gap_word_t* blk)
    {
        unsigned level = bm::gap_level(blk);
        unsigned len = bm::gap_length(blk);
        if (level == bm::gap_max_level || len >= gap_max_buff_len)
        {
            deoptimize_block(nb);
        }
        else
        {
            bm::gap_word_t* new_gap_blk = allocate_gap_block(++level, blk);
            bm::word_t* new_blk = (bm::word_t*)new_gap_blk;
            BMSET_PTRGAP(new_blk);

            set_block_ptr(nb, new_blk);
            alloc_.free_gap_block(blk, glen());

            return new_gap_blk;
        }
        return 0;
    }
    /**
        Mark pointer as GAP and assign to the blocks tree
    */
    void set_block_gap_ptr(block_idx_type nb, gap_word_t* gap_blk)
    {
        bm::word_t* block = (bm::word_t*)BMPTR_SETBIT0(gap_blk);
        set_block_ptr(nb, block);
    }


    /*! Returns temporary block, allocates if needed. */
    bm::word_t* check_allocate_tempblock()
    {
        return temp_block_ ? temp_block_ 
                            : (temp_block_ = alloc_.alloc_bit_block());
    }
    
    /*! deallocate temp block */
    void free_temp_block() BMNOEXCEPT
    {
        if (temp_block_)
        {
            alloc_.free_bit_block(temp_block_);
            temp_block_ = 0;
        }
    }

    /*! Detach and return temp block.
        if temp block is NULL allocates a bit-block
        caller is responsible for returning
        @sa return_tempblock
    */
    bm::word_t* borrow_tempblock()
    {
        if (temp_block_)
        {
            bm::word_t* ret = temp_block_; temp_block_ = 0;
            return ret;
        }
        return alloc_.alloc_bit_block();
    }
    
    /*! Return temp block
        if temp block already exists - block gets deallocated
    */
    void return_tempblock(bm::word_t* block) BMNOEXCEPT
    {
        BM_ASSERT(block != temp_block_);
        BM_ASSERT(IS_VALID_ADDR(block));
        
        if (temp_block_)
            alloc_.free_bit_block(block);
        else
            temp_block_ = block;
    }

    /*! Assigns new GAP lengths vector */
    void set_glen(const gap_word_t* glevel_len) BMNOEXCEPT
    {
        ::memcpy(glevel_len_, glevel_len, sizeof(glevel_len_));
    }

    bm::gap_word_t* allocate_gap_block(unsigned level, 
                                       const gap_word_t* src = 0,
                                       const gap_word_t* glevel_len = 0)
    {
        if (!glevel_len)
            glevel_len = glevel_len_;
        gap_word_t* ptr = alloc_.alloc_gap_block(level, glevel_len);
        if (src)
        {
            unsigned len = gap_length(src);
            ::memcpy(ptr, src, len * sizeof(gap_word_t));
            // Reconstruct the mask word using the new level code.
            *ptr = (gap_word_t)(((len-1) << 3) | (level << 1) | (*src & 1));
        }
        else
        {
            *ptr = (gap_word_t)(level << 1);
        }
        return ptr;
    }
    
    /** Returns true if second level block pointer is 0.
    */
    bool is_subblock_null(unsigned nsub) const BMNOEXCEPT
    {
        BM_ASSERT(top_blocks_);
        if (nsub >= top_block_size_)
            return true;
        return top_blocks_[nsub] == NULL;
    }

    bm::word_t*** top_blocks_root() BMNOEXCEPT { return top_blocks_; }

    /*! \brief Returns current GAP level vector
    */
    const gap_word_t* glen() const BMNOEXCEPT
    {
        return glevel_len_;
    }

    /*! \brief Returns GAP level length for specified level
        \param level - level number
    */
    unsigned glen(unsigned level) const BMNOEXCEPT
    {
        return glevel_len_[level];
    }
    
    /*! \brief Returns size of the top block array in the tree 
    */
    unsigned top_block_size() const BMNOEXCEPT
    {
        return top_block_size_;
    }

    /**
        \brief reserve capacity for specified number of bits
    */
    void reserve(id_type max_bits)
    {
        if (max_bits) 
        {
            unsigned bc = compute_top_block_size(max_bits);
            reserve_top_blocks(bc);
        }
    }

    /*!
        \brief Make sure blocks manager has enough blocks capacity
    */
    unsigned reserve_top_blocks(unsigned top_blocks)
    {
        if ((top_blocks_ && top_blocks <= top_block_size_) || !top_blocks )
            return top_block_size_; // nothing to do
        
        bm::word_t*** new_blocks = 
            (bm::word_t***)alloc_.alloc_ptr(top_blocks);

        unsigned i = 0;
        if (top_blocks_)
        {
            if (i < top_block_size_)
            {
                ::memcpy(&new_blocks[0], &top_blocks_[0],
                            top_block_size_ * sizeof(top_blocks_[0]));
                i = top_block_size_;
            }
            alloc_.free_ptr(top_blocks_, top_block_size_);
        }
        if (i < top_blocks)
            ::memset(&new_blocks[i], 0, sizeof(void*) * (top_blocks-i));
        top_blocks_ = new_blocks;
        top_block_size_ = top_blocks;
        return top_block_size_;
    }
    
    /** \brief Returns reference on the allocator
    */
    allocator_type& get_allocator() BMNOEXCEPT { return alloc_; }

    /** \brief Returns allocator
    */
    allocator_type get_allocator() const BMNOEXCEPT { return alloc_; }
    
    
    /// if tree of blocks already up
    bool is_init() const BMNOEXCEPT { return top_blocks_ != 0; }
    
    /// allocate first level of descr. of blocks 
    void init_tree()
    {
        BM_ASSERT(top_blocks_ == 0);
        if (top_block_size_)
        {
            top_blocks_ = (bm::word_t***) alloc_.alloc_ptr(top_block_size_);
            ::memset(top_blocks_, 0, top_block_size_ * sizeof(bm::word_t**));
            return;
        }
        top_blocks_ = 0;
    }

    /// allocate first level of descr. of blocks
    void init_tree(unsigned top_size)
    {
        BM_ASSERT(top_blocks_ == 0);
        if (top_size > top_block_size_)
            top_block_size_ = top_size;
        init_tree();
    }

    // ----------------------------------------------------------------
    #define BM_FREE_OP(x) blk = blk_blk[j + x]; \
        if (IS_VALID_ADDR(blk)) \
        { \
            if (BM_IS_GAP(blk)) \
                alloc_.free_gap_block(BMGAP_PTR(blk), glen()); \
            else \
                alloc_.free_bit_block(blk); \
        }
    
    void deallocate_top_subblock(unsigned nblk_blk) BMNOEXCEPT
    {
        if (!top_blocks_[nblk_blk])
            return;
        bm::word_t** blk_blk = top_blocks_[nblk_blk];
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            top_blocks_[nblk_blk] = 0;
            return;
        }
        unsigned j = 0; bm::word_t* blk;
        do
        {
        #if defined(BM64_AVX2) || defined(BM64_AVX512)
            if (!avx2_test_all_zero_wave(blk_blk + j))
            {
                BM_FREE_OP(0)
                BM_FREE_OP(1)
                BM_FREE_OP(2)
                BM_FREE_OP(3)
            }
            j += 4;
        #elif defined(BM64_SSE4)
            if (!sse42_test_all_zero_wave(blk_blk + j))
            {
                BM_FREE_OP(0)
                BM_FREE_OP(1)
            }
            j += 2;
        #else
            BM_FREE_OP(0)
            ++j;
        #endif
        } while (j < bm::set_sub_array_size);

        alloc_.free_ptr(top_blocks_[nblk_blk], bm::set_sub_array_size);
        top_blocks_[nblk_blk] = 0;
    }

    /** destroy tree, free memory in all blocks and control structures
        Note: pointers are NOT assigned to zero(!)
    */
    void destroy_tree() BMNOEXCEPT
    {
        BM_ASSERT(!arena_); // arena must be NULL here

        if (!top_blocks_)
            return;

        unsigned top_blocks = top_block_size();
        for (unsigned i = 0; i < top_blocks; )
        {
            bm::word_t** blk_blk = top_blocks_[i];
            if (!blk_blk)
            {
                ++i; // look ahead
                bool found = bm::find_not_null_ptr(top_blocks_, i, top_blocks, &i);
                if (!found) // nothing to do
                    break;
                blk_blk = top_blocks_[i];
            }
            if ((bm::word_t*)blk_blk != FULL_BLOCK_FAKE_ADDR)
                deallocate_top_subblock(i);
            ++i;
        } // for i

        alloc_.free_ptr(top_blocks_, top_block_size_); // free the top
    }
    #undef BM_FREE_OP

    void deinit_tree() BMNOEXCEPT
    {
        if (arena_)
            destroy_arena();
        else
            destroy_tree();
        top_blocks_ = 0; top_block_size_ = 0;
    }

    // ----------------------------------------------------------------
    
    /// calculate top blocks which are not NULL and not FULL
    unsigned find_real_top_blocks() const BMNOEXCEPT
    {
        unsigned cnt = 0;
        unsigned top_blocks = top_block_size();
        // TODO: SIMD
        for (unsigned i = 0; i < top_blocks; ++i)
        {
            bm::word_t** blk_blk = top_blocks_[i];
            if (!blk_blk || ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR))
                continue;
            ++cnt;
        } // for i
        return cnt;
    }

    // ----------------------------------------------------------------

    /// calculate max top blocks size whithout NULL-tail
    unsigned find_max_top_blocks() const BMNOEXCEPT
    {
        unsigned top_blocks = top_block_size();
        if (!top_blocks)
            return 0;
        unsigned i = top_blocks - 1;
        for (; i > 0; --i)
        {
            bm::word_t** blk_blk = top_blocks_[i];
            if (blk_blk)
                break;
        } // for i
        return i+1;
    }

    // ----------------------------------------------------------------

    void validate_top_zero(unsigned i) BMNOEXCEPT
    {
        BM_ASSERT(i < top_block_size());
        bm::word_t** blk_blk = top_blocks_[i];
        // TODO: SIMD or unroll
        for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
        {
            if (blk_blk[j])
                return;
        } // for j
        alloc_.free_ptr(blk_blk, bm::set_sub_array_size);
        top_blocks_[i] = 0;
    }

    // ----------------------------------------------------------------

    void validate_top_full(unsigned i) BMNOEXCEPT
    {
        BM_ASSERT(i < top_block_size());
        bm::word_t** blk_blk = top_blocks_[i];
        // TODO: SIMD
        for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
        {
            if (blk_blk[j] != FULL_BLOCK_FAKE_ADDR)
                return;
        } // for j
        alloc_.free_ptr(blk_blk, bm::set_sub_array_size);
        top_blocks_[i] = (bm::word_t**)FULL_BLOCK_FAKE_ADDR;
    }

    /**
        Calculate approximate memory needed to serialize big runs
        of 0000s and 111s (as blocks)
    */
    size_t calc_serialization_null_full() const BMNOEXCEPT
    {
        size_t s_size = sizeof(unsigned);
        if (!top_blocks_)
            return s_size;
        block_idx_type nb_empty = 0;
        block_idx_type nb_full = 0;
        unsigned top_blocks = top_block_size();
        for (unsigned i = 0; i < top_blocks; )
        {
            bm::word_t** blk_blk = top_blocks_[i];
            if (!blk_blk)
            {
                s_size += nb_full ? 1+sizeof(block_idx_type) : 0; nb_full = 0;
                nb_empty += bm::set_sub_array_size;

                unsigned nb_prev = i++;
                bool found = bm::find_not_null_ptr(top_blocks_, i, top_blocks, &i);
                BM_ASSERT(i >= nb_prev);
                if (!found)
                {
                    nb_empty = 0;
                    break;
                }
                nb_empty += (i - nb_prev) * bm::set_sub_array_size;
                blk_blk = top_blocks_[i];
                BM_ASSERT(blk_blk);
                if (!blk_blk)
                    break;
            }
            if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            {
                s_size += nb_empty ? 1+sizeof(block_idx_type) : 0; nb_empty = 0;
                nb_full += bm::set_sub_array_size;
                ++i;
                continue;
            }
            unsigned j = 0; bm::word_t* blk;
            do
            {
                blk = blk_blk[j];
                if (!blk)
                {
                    s_size += nb_full ? 1+sizeof(block_idx_type) : 0; nb_full = 0;
                    ++nb_empty;
                }
                else
                if (blk == FULL_BLOCK_FAKE_ADDR)
                {
                    s_size += nb_empty ? 1+sizeof(block_idx_type) : 0; nb_empty = 0;
                    ++nb_full;
                }
                else // real block (not 000 and not 0xFFF)
                {
                    s_size += nb_empty ? 1+sizeof(block_idx_type) : 0; nb_empty = 0;
                    s_size += nb_full  ? 1+sizeof(block_idx_type) : 0; nb_full = 0;
                }
            } while (++j < bm::set_sub_array_size);

            ++i;
        } // for i
        s_size += nb_empty ? 1+sizeof(block_idx_type) : 0;
        s_size += nb_full  ? 1+sizeof(block_idx_type) : 0;
        return s_size;
    }

    // ----------------------------------------------------------------

    void optimize_block(unsigned i, unsigned j,
                        bm::word_t*    block,
                        bm::word_t*    temp_block,
                        int            opt_mode,
                        bv_statistics* bv_stat)
    {
        if (!IS_VALID_ADDR(block))
            return;

        if (BM_IS_GAP(block)) // gap block
        {
            gap_word_t* gap_blk = BMGAP_PTR(block);
            if (bm::gap_is_all_zero(gap_blk))
            {
                set_block_ptr(i, j, 0);
                alloc_.free_gap_block(gap_blk, glen());
            }
            else
            if (bm::gap_is_all_one(gap_blk))
            {
                set_block_ptr(i, j, FULL_BLOCK_FAKE_ADDR);
                alloc_.free_gap_block(gap_blk, glen());
            }
            else
            {
                // try to shrink the GAP block
                //
                unsigned len = bm::gap_length(gap_blk);
                int old_level = bm::gap_level(gap_blk);
                int new_level = bm::gap_calc_level(len, glen());
                if (new_level >= 0 && new_level < old_level) // can shrink
                {
                    gap_word_t* new_gap_blk =
                        allocate_gap_block(unsigned(new_level), gap_blk);
                    bm::word_t* blk = (bm::word_t*)BMPTR_SETBIT0(new_gap_blk);
                    set_block_ptr(i, j, blk);
                    alloc_.free_gap_block(gap_blk, glen());
                    gap_blk = new_gap_blk;
                }

                if (bv_stat)
                {
                    unsigned level = bm::gap_level(gap_blk);
                    bv_stat->add_gap_block(
                        bm::gap_capacity(gap_blk, glen()), len, level);
                }
            }
        }
        else // bit-block
        {
            // evaluate bit-block complexity
            unsigned gap_count = bm::bit_block_calc_change(block);
            if (gap_count == 1) // all-zero OR all-one block
            {
                if ((block[0] & 1u)) // all-ONE
                {
                    BM_ASSERT(bm::is_bits_one((wordop_t*)block));
                    get_allocator().free_bit_block(block);
                    block = FULL_BLOCK_FAKE_ADDR;
                }
                else // empty block
                {
                    BM_ASSERT(bm::bit_is_all_zero(block));
                    get_allocator().free_bit_block(block);
                    block = 0;
                }
                set_block_ptr(i, j, block);
                return;
            }
            if (opt_mode < 3) // free_01 optimization
            {
                if (bv_stat)
                    bv_stat->add_bit_block();
                return;
            }
            // try to compress
            //
            gap_word_t* tmp_gap_blk = (bm::gap_word_t*)temp_block;
            *tmp_gap_blk = bm::gap_max_level << 1;
            unsigned threashold = glen(bm::gap_max_level)-4;
            unsigned len = 0;
            if (gap_count < threashold)
            {
                len = bm::bit_to_gap(tmp_gap_blk, block, threashold);
                BM_ASSERT(len);
            }
            if (len)  // GAP compression successful
            {
                get_allocator().free_bit_block(block);
                // check if new gap block can be eliminated
                BM_ASSERT(!(bm::gap_is_all_zero(tmp_gap_blk)
                           || bm::gap_is_all_one(tmp_gap_blk)));

                int level = bm::gap_calc_level(len, glen());
                BM_ASSERT(level >= 0);
                gap_word_t* gap_blk =
                    allocate_gap_block(unsigned(level), tmp_gap_blk);
                bm::word_t* blk = (bm::word_t*)BMPTR_SETBIT0(gap_blk);
                set_block_ptr(i, j, blk);
                if (bv_stat)
                {
                    level = bm::gap_level(gap_blk);
                    bv_stat->add_gap_block(
                            bm::gap_capacity(gap_blk, glen()),
                            bm::gap_length(gap_blk), unsigned(level));
                }
            }
            else  // non-compressable bit-block
            {
                if (bv_stat)
                    bv_stat->add_bit_block();
            }
        } // bit-block
    }

    // ----------------------------------------------------------------
    
    void optimize_tree(bm::word_t*  temp_block, int opt_mode,
                       bv_statistics* bv_stat)
    {
        if (!top_blocks_)
            return;
        
        unsigned top_blocks = top_block_size();
        for (unsigned i = 0; i < top_blocks; ++i)
        {
            bm::word_t** blk_blk = top_blocks_[i];
            if (!blk_blk)
            {
                ++i;
                bool found = bm::find_not_null_ptr(top_blocks_, i, top_blocks, &i);
                if (!found)
                    break;
                blk_blk = top_blocks_[i];
            }
            if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
                continue;
            
            for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
            {
                optimize_block(i, j, blk_blk[j], temp_block, opt_mode, bv_stat);
            }
            
            // optimize the top level
            //
            if (blk_blk[0] == FULL_BLOCK_FAKE_ADDR)
                validate_top_full(i);
            else
                if (!blk_blk[0])
                    validate_top_zero(i);
            
            blk_blk = top_blocks_[i];
            if (!blk_blk || (bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            {}
            else
            {
                if (bv_stat)
                    bv_stat->ptr_sub_blocks++;
            }
        } // for i
        
        if (bv_stat)
        {
            size_t full_null_size = calc_serialization_null_full();
            bv_stat->max_serialize_mem += full_null_size;
        }
    }

    // ----------------------------------------------------------------
    
    void stat_correction(bv_statistics* stat) noexcept
    {
        BM_ASSERT(stat);
        
        size_t safe_inc = stat->max_serialize_mem / 10; // 10% increment
        if (!safe_inc) safe_inc = 256;
        stat->max_serialize_mem += safe_inc;

        unsigned top_size = top_block_size();
        size_t blocks_mem = sizeof(*this);
        blocks_mem += sizeof(bm::word_t**) * top_size;
        blocks_mem += stat->ptr_sub_blocks * (sizeof(void*) * bm::set_sub_array_size);
        stat->memory_used += blocks_mem;
        stat->bv_count = 1;
    }



    // -----------------------------------------------------------------------

    /*!
       @brief Calculates bitvector arena statistics.
    */
    void calc_arena_stat(bm::bv_arena_statistics* st) const BMNOEXCEPT
    {
        BM_ASSERT(st);

        st->reset();
        const bm::word_t* const * const* blk_root = top_blocks_root();

        if (!blk_root)
            return;
        unsigned top_size = st->top_block_size = top_block_size();
        for (unsigned i = 0; i < top_size; ++i)
        {
            const bm::word_t* const* blk_blk = blk_root[i];
            if (!blk_blk)
            {
                ++i;
                bool found = bm::find_not_null_ptr(blk_root, i, top_size, &i);
                if (!found)
                    break;
                blk_blk = blk_root[i];
                BM_ASSERT(blk_blk);
                if (!blk_blk)
                    break;
            }
            if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
                continue;
            st->ptr_sub_blocks_sz += bm::set_sub_array_size;
            for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
            {
                const bm::word_t* blk = blk_blk[j];
                if (IS_VALID_ADDR(blk))
                {
                    if (BM_IS_GAP(blk))
                    {
                        const bm::gap_word_t* gap_blk = BMGAP_PTR(blk);
                        unsigned len = bm::gap_length(gap_blk);
                        BM_ASSERT(gap_blk[len-1] == 65535);
                        st->gap_blocks_sz += len;
                    }
                    else // bit block
                        st->bit_blocks_sz += bm::set_block_size;
                }
            } // for j
        } // for i

    }

    /**
        Arena allocation memory guard
        @internal
     */
    struct arena_guard
    {
        arena_guard(arena* ar, blocks_manager& bman) noexcept
            : ar_(ar), bman_(bman)
        {}
        ~arena_guard() noexcept
        {
            if (ar_)
            {
                blocks_manager::free_arena(ar_, bman_.alloc_);
                ::free(ar_);
            }
        }
        void release() noexcept { ar_ = 0; }

        arena* ar_;
        blocks_manager& bman_;
    };

    /// calculate arena statistics, calculate and copy all blocks there
    ///
    void copy_to_arena(const blocks_manager& bman)
    {
        BM_ASSERT(arena_ == 0 && top_blocks_ == 0);

        bm::bv_arena_statistics src_arena_st;
        if (bman.arena_)
            src_arena_st = bman.arena_->st_;
        else
            bman.calc_arena_stat(&src_arena_st);

        arena* ar = (arena*)::malloc(sizeof(arena));
        if (!ar)
        {
        #ifndef BM_NO_STL
            throw std::bad_alloc();
        #else
            BM_THROW(BM_ERR_BADALLOC);
        #endif
        }
        ar->reset();
        arena_guard aguard(ar, *this); // alloc_arena can throw an exception..

        alloc_arena(ar, src_arena_st, get_allocator());
        bman.copy_to_arena(ar);
        arena_ = ar;
        aguard.release(); // ownership of arena is transfered

        // reset the top tree link to work with the arena
        top_blocks_ = ar->top_blocks_;
        top_block_size_ = ar->st_.top_block_size;
    }


    // ----------------------------------------------------------------

    /// Allocate arena (content memory) based on arena statistics
    ///
    /// @param ar - arena pointer to allocate its buffers
    /// @param st - arena statistics (should be calculated for the same vector
    /// @para alloc - allocator
    ///
    static
    void alloc_arena(arena* ar, const bm::bv_arena_statistics& st,
                     allocator_type& alloc)
    {
        BM_ASSERT(ar);
        ar->st_ = st;

        // compute total allocation size in bytes
        size_t alloc_sz = st.get_alloc_size();
        // size to alloc in pointers
        size_t alloc_sz_v = (alloc_sz + (sizeof(void*)-1)) / sizeof(void*);

        char* arena_mem_ptr = (char*) alloc.alloc_ptr(alloc_sz_v);
        ar->a_ptr_ = arena_mem_ptr;

        if (st.bit_blocks_sz)
        {
            ar->blocks_ = (bm::word_t*)arena_mem_ptr;
            BM_ASSERT(bm::is_aligned(ar->blocks_));
            arena_mem_ptr += st.bit_blocks_sz * sizeof(bm::word_t);
        }
        else
            ar->blocks_ = 0;

        ar->top_blocks_ = (bm::word_t***) arena_mem_ptr;
        for (unsigned i = 0; i < ar->st_.top_block_size; ++i) // init as NULLs
            ar->top_blocks_[i] = 0;
        arena_mem_ptr += st.top_block_size * sizeof(void*);

        if (st.ptr_sub_blocks_sz)
        {
            ar->blk_blks_ = (bm::word_t**) arena_mem_ptr;
            arena_mem_ptr += st.ptr_sub_blocks_sz * sizeof(void*);
        }
        else
            ar->blk_blks_ = 0;

        if (st.gap_blocks_sz)
            ar->gap_blocks_ = (bm::gap_word_t*)arena_mem_ptr;
        else
            ar->gap_blocks_ = 0;
    }

    // ----------------------------------------------------------------

    /// Free arena (content memory) based on arena statistics
    /// @param ar - arena pointer to free its buffers
    /// @param alloc - allocator
    ///
    static
    void free_arena(arena* ar, allocator_type& alloc) BMNOEXCEPT
    {
        BM_ASSERT(ar);
        if (ar->a_ptr_)
        {
            size_t alloc_sz = ar->st_.get_alloc_size();
            // size to alloc in pointers
            size_t alloc_sz_v = (alloc_sz + (sizeof(void*)-1)) / sizeof(void*);
            alloc.free_ptr(ar->a_ptr_, alloc_sz_v);
        }
    }

    // ----------------------------------------------------------------
    /// free all arena memory
    ///
    void destroy_arena() BMNOEXCEPT
    {
        free_arena(arena_, alloc_);
        ::free(arena_); arena_ = 0; top_blocks_ = 0; top_block_size_ = 0;
    }

    // ----------------------------------------------------------------

    /**
        Copy blocks into arena allocated memory
        @param ar - target allocated arena
        @param arena_st - arena allocation statistics
        @sa alloc_arena
     */
    void copy_to_arena(arena* ar) const BMNOEXCEPT
    {
        BM_ASSERT(ar);

        bm::bv_arena_statistics& st = ar->st_; (void) st;
        bm::bv_arena_statistics arena_st;
        arena_st.reset();

        bm::word_t*** blk_root = ar->top_blocks_;
        const bm::word_t* const * const * blk_root_arg = top_blocks_root();
        BM_ASSERT (blk_root_arg);

        // arena target pointers
        bm::word_t**    t_blk_blk = ar->blk_blks_;
        bm::word_t*     t_block   = ar->blocks_;
        bm::gap_word_t* t_gap_block = ar->gap_blocks_;

        unsigned top_size = top_block_size();
        for (unsigned i = 0; i < top_size; ++i)
        {
            const bm::word_t* const* blk_blk_arg = blk_root_arg[i];
            if (!blk_blk_arg)
            {
                ++i;
                bool found = bm::find_not_null_ptr(blk_root_arg, i, top_size, &i);
                if (!found)
                    break;
                blk_blk_arg = blk_root_arg[i];
                BM_ASSERT(blk_blk_arg);
                if (!blk_blk_arg)
                    break;
            }
            if ((bm::word_t*)blk_blk_arg == FULL_BLOCK_FAKE_ADDR)
            {
                blk_root[i] = (bm::word_t**)FULL_BLOCK_FAKE_ADDR;
                continue;
            }

            blk_root[i] = t_blk_blk;
            for (unsigned j = 0; j < bm::set_sub_array_size; ++j)
            {
                const bm::word_t* blk = blk_blk_arg[j];
                t_blk_blk[j] = (bm::word_t*)blk; // copy FULL and NULL blocks
                if (!IS_VALID_ADDR(blk))
                    continue;

                if (BM_IS_GAP(blk))
                {
                    const bm::gap_word_t* gap_blk = BMGAP_PTR(blk);
                    unsigned len = bm::gap_length(gap_blk);
                    BM_ASSERT(gap_blk[len-1] == 65535);

                    ::memcpy(t_gap_block, gap_blk, len * sizeof(bm::gap_word_t));
                    bm::word_t* blk_p = (bm::word_t*) t_gap_block;
                    BMSET_PTRGAP(blk_p);
                    t_blk_blk[j] = blk_p;
                    t_gap_block += len;

                    arena_st.gap_blocks_sz += len;
                    BM_ASSERT(st.gap_blocks_sz >= arena_st.gap_blocks_sz);
                }
                else // bit block
                {
                    bm::bit_block_copy(t_block, blk);
                    t_blk_blk[j] = t_block;
                    t_block += bm::set_block_size;

                    arena_st.bit_blocks_sz += bm::set_block_size;
                    BM_ASSERT(st.bit_blocks_sz >= arena_st.bit_blocks_sz);
                }

            } // for j

            t_blk_blk += bm::set_sub_array_size;
            arena_st.ptr_sub_blocks_sz += bm::set_sub_array_size;
            BM_ASSERT(st.ptr_sub_blocks_sz >= arena_st.ptr_sub_blocks_sz);

        } // for i

    }


private:

    void operator =(const blocks_manager&);

    void copy(const blocks_manager& blockman,
              block_idx_type block_from = 0,
              block_idx_type block_to = bm::set_total_blocks)
    {
        BM_ASSERT(!arena_);
        bm::word_t*** blk_root_arg = blockman.top_blocks_root();
        if (!blk_root_arg)
            return;

        unsigned arg_top_blocks = blockman.top_block_size();
        {
            block_idx_type need_top_blocks = 1 + (block_to / 256);
            if (need_top_blocks < arg_top_blocks)
                arg_top_blocks = unsigned(need_top_blocks);
        }

        this->reserve_top_blocks(arg_top_blocks);
        bm::word_t*** blk_root = top_blocks_root();

        
        unsigned i_from, j_from, i_to, j_to;
        get_block_coord(block_from, i_from, j_from);
        get_block_coord(block_to, i_to, j_to);
        
        if (i_to >= arg_top_blocks-1)
        {
            i_to = arg_top_blocks-1;
            j_to = bm::set_sub_array_size-1;
        }

        for (unsigned i = i_from; i <= i_to; ++i)
        {
            bm::word_t** blk_blk_arg = blk_root_arg[i];
            if (!blk_blk_arg)
                continue;
            if ((bm::word_t*)blk_blk_arg == FULL_BLOCK_FAKE_ADDR)
            {
                BM_ASSERT(!blk_root[i]);
                if (i < i_to)
                {
                    unsigned j = (i == i_from) ? j_from : 0;
                    if (!j)
                    {
                        blk_root[i] = blk_blk_arg;
                        continue;
                    }
                }
                blk_blk_arg = FULL_SUB_BLOCK_REAL_ADDR;
            }
            
            BM_ASSERT(blk_root[i] == 0);

            bm::word_t** blk_blk = blk_root[i] = (bm::word_t**)alloc_.alloc_ptr(bm::set_sub_array_size);
            ::memset(blk_blk, 0, bm::set_sub_array_size * sizeof(bm::word_t*));
            
            unsigned j = (i == i_from) ? j_from : 0;
            unsigned j_limit = (i == i_to) ? j_to+1 : bm::set_sub_array_size;
            bm::word_t* blk;
            const bm::word_t* blk_arg;
            do
            {
                blk = blk_blk[j]; blk_arg = blk_blk_arg[j];
                if (blk_arg)
                {
                    bool is_gap = BM_IS_GAP(blk_arg);
                    if (is_gap)
                    {
                        blk = clone_gap_block(BMGAP_PTR(blk_arg), is_gap);
                        if (is_gap)
                            BMSET_PTRGAP(blk);
                    }
                    else
                    {
                        if (blk_arg == FULL_BLOCK_FAKE_ADDR)
                            blk = FULL_BLOCK_FAKE_ADDR;
                        else
                        {
                            BM_ASSERT(!IS_FULL_BLOCK(blk_arg));
                            blk = alloc_.alloc_bit_block();
                            bm::bit_block_copy(blk, blk_arg);
                        }
                    }
                    blk_blk[j] = blk;
                }
                ++j;
            } while (j < j_limit);
        } // for i
    }

private:
    /// maximum addresable bits
    id_type                                max_bits_ = bm::id_max;
    /// Tree of blocks.
    bm::word_t***                          top_blocks_ = 0;
    /// Size of the top level block array in blocks_ tree
    unsigned                               top_block_size_;
    /// Temp block.
    bm::word_t*                            temp_block_ = 0;
    /// vector defines gap block lengths for different levels 
    gap_word_t                             glevel_len_[bm::gap_levels];
    /// allocator
    allocator_type                         alloc_;
    /// memory arena pointer
    arena*                                 arena_ = 0;
};

/**
    Bit block buffer guard
    \internal
*/
template<class BlocksManager>
class bit_block_guard
{
public:
    bit_block_guard(BlocksManager& bman, bm::word_t* blk=0) BMNOEXCEPT
        : bman_(bman), 
          block_(blk)
    {}
    ~bit_block_guard()
    {
        if (IS_VALID_ADDR(block_))
            bman_.get_allocator().free_bit_block(block_, 3);
    }

    void attach(bm::word_t* blk) BMNOEXCEPT
    {
        if (IS_VALID_ADDR(block_))
            bman_.get_allocator().free_bit_block(block_);
        block_ = blk;
    }

    bm::word_t* allocate()
    {
        attach(bman_.get_allocator().alloc_bit_block(3));
        return block_;
    }
    bm::word_t* get() BMNOEXCEPT { return block_; }

private:
    bit_block_guard(const bit_block_guard&);
    bit_block_guard& operator=(const bit_block_guard&);
private:
    BlocksManager& bman_;
    bm::word_t*    block_;
};

/*!
    Resource guard for PCLASS::set_allocator_pool()
    @ingroup bvector
    @internal
*/
template<typename POOL, typename PCLASS>
class alloc_pool_guard
{
public:
    alloc_pool_guard() BMNOEXCEPT : optr_(0)
    {}

    alloc_pool_guard(POOL& pool, PCLASS& obj) BMNOEXCEPT
        : optr_(&obj)
    {
        obj.set_allocator_pool(&pool);
    }
    ~alloc_pool_guard() BMNOEXCEPT
    {
        if (optr_)
            optr_->set_allocator_pool(0);
    }

    /// check if vector has no assigned allocator and set one
    void assign_if_not_set(POOL& pool,
                           PCLASS& obj) BMNOEXCEPT
    {
        if (!obj.get_allocator_pool()) // alloc pool not set yet
        {
            BM_ASSERT(!optr_);
            optr_ = &obj;
            optr_->set_allocator_pool(&pool);
        }
    }

private:
    alloc_pool_guard(const alloc_pool_guard&) = delete;
    void operator=(const alloc_pool_guard&) = delete;
private:
    PCLASS*  optr_; ///< garded object
};



}

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif

