#ifndef BMINTERVALS__H__INCLUDED__
#define BMINTERVALS__H__INCLUDED__

/*
Copyright(c) 2002-2020 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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
/*! \file bmintervals.h
    \brief Algorithms for bit ranges and intervals
*/

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration
// #include "bm.h" or "bm64.h" explicitly
# error missing include (bm.h or bm64.h)
#endif

#include "bmdef.h"

/** \defgroup bvintervals Algorithms for bit intervals
    Algorithms and iterators for bit ranges and intervals
    @ingroup bvector
 */


namespace bm
{

/*!
    \brief forward iterator class to traverse bit-vector as ranges

    Traverse enumerator for forward walking bit-vector as intervals:
    series of consequtive 1111s flanked with zeroes.
    Enumerator can traverse the whole bit-vector or jump(go_to) to position.

   \ingroup bvintervals
*/
template<typename BV>
class interval_enumerator
{
public:
#ifndef BM_NO_STL
        typedef std::input_iterator_tag  iterator_category;
#endif
        typedef BV                                         bvector_type;
        typedef typename bvector_type::size_type           size_type;
        typedef typename bvector_type::allocator_type      allocator_type;
        typedef bm::byte_buffer<allocator_type>            buffer_type;
        typedef bm::pair<size_type, size_type>             pair_type;

public:
    /*! @name Construction and assignment */
    //@{

    interval_enumerator()
        : bv_(0), interval_(bm::id_max, bm::id_max), gap_ptr_(0)
    {}

    /**
        Construct enumerator for the bit-vector
    */
    interval_enumerator(const BV& bv)
        : bv_(&bv), interval_(bm::id_max, bm::id_max), gap_ptr_(0)
    {
        go_to_impl(0, false);
    }

    /**
        Construct enumerator for the specified position
        @param bv - source bit-vector
        @param start_pos - position on bit-vector to search for interval
        @param extend_start - flag to extend interval start to the start if
            true start happenes to be less than start_pos
        @sa go_to
    */
    interval_enumerator(const BV& bv, size_type start_pos, bool extend_start)
        : bv_(&bv), interval_(bm::id_max, bm::id_max), gap_ptr_(0)
    {
        go_to_impl(start_pos, extend_start);
    }

    /**
        Copy constructor
    */
    interval_enumerator(const interval_enumerator<BV>& ien)
        : bv_(ien.bv_), interval_(bm::id_max, bm::id_max), gap_ptr_(0)
    {
        go_to_impl(ien.start(), false);
    }

    /**
        Assignment operator
    */
    interval_enumerator& operator=(const interval_enumerator<BV>& ien)
    {
        bv_ = ien.bv_; gap_ptr_ = 0;
        go_to_impl(ien.start(), false);
    }

#ifndef BM_NO_CXX11
    /** move-ctor */
    interval_enumerator(interval_enumerator<BV>&& ien) BMNOEXCEPT
        : bv_(0), interval_(bm::id_max, bm::id_max), gap_ptr_(0)
    {
        this->swap(ien);
    }

    /** move assignmment operator */
    interval_enumerator<BV>& operator=(interval_enumerator<BV>&& ien) BMNOEXCEPT
    {
        if (this != &ien)
            this->swap(ien);
        return *this;
    }
#endif

    //@}


    // -----------------------------------------------------------------

    /*! @name Comparison methods all use start position to compare  */
    //@{

    bool operator==(const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() == ien.start()); }
    bool operator!=(const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() != ien.start()); }
    bool operator < (const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() < ien.start()); }
    bool operator <= (const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() <= ien.start()); }
    bool operator > (const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() > ien.start()); }
    bool operator >= (const interval_enumerator<BV>& ien) const BMNOEXCEPT
                            { return (start() >= ien.start()); }
    //@}


    /// Return interval start/left as bit-vector coordinate 011110 [left..right]
    size_type start() const BMNOEXCEPT;
    /// Return interval end/right as bit-vector coordinate 011110 [left..right]
    size_type end() const BMNOEXCEPT;

    const pair_type& operator*() const BMNOEXCEPT { return interval_; }

    /// Get interval pair
    const pair_type& get() const BMNOEXCEPT { return interval_; }

    /// Returns true if enumerator is valid (false if traversal is done)
    bool valid() const BMNOEXCEPT;

    // -----------------------------------------------------------------

    /*! @name enumerator positioning  */
    //@{

    /*!
        @brief Go to inetrval at specified position
        Jump to position with interval. If interval is not available at
        the specified position (o bit) enumerator will find the next interval.
        If interval is present we have an option to find interval start [left..]
        and set enumerator from the effective start coodrinate

        @param pos - position on bit-vector
        @param extend_start - find effective start if it is less than the
                              go to position
        @return true if enumerator remains valid after the jump
    */
    bool go_to(size_type pos, bool extend_start = true);

    /*! Advance to the next interval
        @return true if interval is available
        @sa valid
    */
    bool advance();

    /*! \brief Advance enumerator forward to the next available bit */
    interval_enumerator<BV>& operator++() BMNOEXCEPT
        { advance(); return *this; }

    /*! \brief Advance enumerator forward to the next available bit */
    interval_enumerator<BV> operator++(int) BMNOEXCEPT
    {
        interval_enumerator<BV> tmp = *this;
        advance();
        return tmp;
    }
    //@}

    /**
        swap enumerator with another one
    */
    void swap(interval_enumerator<BV>& ien) BMNOEXCEPT;

protected:
    typedef typename bvector_type::block_idx_type       block_idx_type;
    typedef typename bvector_type::allocator_type       bv_allocator_type;
    typedef bm::heap_vector<unsigned short, bv_allocator_type, true>
                                                    gap_vector_type;


    bool go_to_impl(size_type pos, bool extend_start);

    /// Turn FSM into invalid state (out of range)
    void invalidate() BMNOEXCEPT;

private:
    const BV*                  bv_;      ///!< bit-vector for traversal
    gap_vector_type            gap_buf_; ///!< GAP buf.vector for bit-block
    pair_type                  interval_; ///! current inetrval
    const bm::gap_word_t*      gap_ptr_; ///!< current pointer in GAP block
};

//----------------------------------------------------------------------------

/*!
    \brief Returns true if range is all 1s flanked with 0s
    Function performs the test on a closed range [left, right]
    true interval is all 1s AND test(left-1)==false AND test(right+1)==false
    Examples:
        01110 [1,3] - true
        11110 [0,3] - true
        11110 [1,3] - false
    \param bv   - bit-vector for check
   \param left - index of first bit start checking
   \param right - index of last bit
   \return true/false

   \ingroup bvintervals

   @sa is_all_one_range
*/
template<class BV>
bool is_interval(const BV& bv,
                 typename BV::size_type left,
                 typename BV::size_type right) BMNOEXCEPT
{
    typedef typename BV::block_idx_type block_idx_type;

    const typename BV::blocks_manager_type& bman = bv.get_blocks_manager();

    if (!bman.is_init())
        return false; // nothing to do

    if (right < left)
        bm::xor_swap(left, right);
    if (left == bm::id_max) // out of range
        return false;
    if (right == bm::id_max)
        --right;

    block_idx_type nblock_left = (left >> bm::set_block_shift);
    block_idx_type nblock_right = (right >> bm::set_block_shift);

    if (nblock_left == nblock_right) // same block (fast case)
    {
        unsigned nbit_left = unsigned(left  & bm::set_block_mask);
        unsigned nbit_right = unsigned(right  & bm::set_block_mask);
        if ((nbit_left > 0) && (nbit_right < bm::gap_max_bits-1))
        {
            unsigned i0, j0;
            bm::get_block_coord(nblock_left, i0, j0);
            const bm::word_t* block = bman.get_block_ptr(i0, j0);
            bool b = bm::block_is_interval(block, nbit_left, nbit_right);
            return b;
        }
    }
    bool is_left, is_right, is_all_one;
    is_left = left > 0 ? bv.test(left-1) : false;
    if (is_left == false)
    {
        is_right = (right < (bm::id_max - 1)) ? bv.test(right + 1) : false;
        if (is_left == false && is_right == false)
        {
            is_all_one = bv.is_all_one_range(left, right);
            return is_all_one;
        }
    }
    return false;
}


//----------------------------------------------------------------------------

/*!

    \brief Reverse find index of first 1 bit gap (01110) starting from position
    Reverse scan for the first 1 in a block of continious 1s.
    Method employs closed interval semantics: 0[pos..from]

    \param bv   - bit-vector for search
    \param from - position to start reverse search from
    \param pos - [out] index of the found first 1 bit in a gap of bits
    \return true if search returned result, false if not found
           (start point is zero)

    \sa is_interval, find_interval_end
    \ingroup bvintervals
*/
template<class BV>
bool find_interval_start(const BV& bv,
                         typename BV::size_type from,
                         typename BV::size_type& pos)  BMNOEXCEPT
{
    typedef typename BV::size_type size_type;
    typedef typename BV::block_idx_type block_idx_type;

    const typename BV::blocks_manager_type& bman = bv.get_blocks_manager();

    if (!bman.is_init())
        return false; // nothing to do
    if (!from)
    {
        pos = from;
        return bv.test(from);
    }

    block_idx_type nb = (from >> bm::set_block_shift);
    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);

    size_type base_idx;
    unsigned found_nbit;

    const bm::word_t* block = bman.get_block_ptr(i0, j0);
    if (!block)
        return false;
    unsigned nbit = unsigned(from & bm::set_block_mask);
    unsigned res = bm::block_find_interval_start(block, nbit, &found_nbit);

    switch (res)
    {
    case 0: // not interval
        return false;
    case 1: // interval found
        pos = found_nbit + (nb * bm::gap_max_bits);
        return true;
    case 2: // keep scanning
        base_idx = bm::get_block_start<size_type>(i0, j0);
        pos = base_idx + found_nbit;
        if (!nb)
            return true;
        break;
    default:
        BM_ASSERT(0);
    } // switch

    --nb;
    bm::get_block_coord(nb, i0, j0);
    bm::word_t*** blk_root = bman.top_blocks_root();

    for (unsigned i = i0; true; --i)
    {
        bm::word_t** blk_blk = blk_root[i];
        if (!blk_blk)
            return true;
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            pos = bm::get_super_block_start<size_type>(i);
            if (!i)
                break;
            continue;
        }
        unsigned j = (i == i0) ? j0 : 255;
        for (; true; --j)
        {
            if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            {
                pos = bm::get_block_start<size_type>(i, j);
                goto loop_j_end; // continue
            }

            block = blk_blk[j];
            if (!block)
                return true;

            res = bm::block_find_interval_start(block,
                                            bm::gap_max_bits-1, &found_nbit);
            switch (res)
            {
            case 0: // not interval (but it was the interval, so last result
                return true;
            case 1: // interval found
                base_idx = bm::get_block_start<size_type>(i, j);
                pos = base_idx + found_nbit;
                return true;
            case 2: // keep scanning
                pos = bm::get_block_start<size_type>(i, j);
                break;
            default:
                BM_ASSERT(0);
            } // switch

            loop_j_end: // continue point
            if (!j)
                break;
        } // for j

        if (!i)
            break;
    } // for i

    return true;
}


//----------------------------------------------------------------------------

/*!
   \brief Reverse find index of first 1 bit gap (01110) starting from position
   Reverse scan for the first 1 in a block of continious 1s.
   Method employs closed interval semantics: 0[pos..from]

   \param bv   - bit-vector for search
   \param from - position to start reverse search from
   \param pos - [out] index of the found first 1 bit in a gap of bits
   \return true if search returned result, false if not found
           (start point is zero)

   \sa is_interval, find_interval_end
    \ingroup bvintervals
*/
template <typename BV>
bool find_interval_end(const BV& bv,
                       typename BV::size_type from,
                       typename BV::size_type & pos)  BMNOEXCEPT
{
    typedef typename BV::block_idx_type block_idx_type;

    if (from == bm::id_max)
        return false;
    const typename BV::blocks_manager_type& bman = bv.get_blocks_manager();

    if (!bman.is_init())
        return false; // nothing to do
    if (from == bm::id_max-1)
    {
        pos = from;
        return bv.test(from);
    }

    block_idx_type nb = (from >> bm::set_block_shift);
    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);

    unsigned found_nbit;

    const bm::word_t* block = bman.get_block_ptr(i0, j0);
    if (!block)
        return false;
    unsigned nbit = unsigned(from & bm::set_block_mask);
    unsigned res = bm::block_find_interval_end(block, nbit, &found_nbit);
    switch (res)
    {
    case 0: // not interval
        return false;
    case 1: // interval found
        pos = found_nbit + (nb * bm::gap_max_bits);
        return true;
    case 2: // keep scanning
        pos = found_nbit + (nb * bm::gap_max_bits);
        break;
    default:
        BM_ASSERT(0);
    } // switch

    block_idx_type nblock_right = (bm::id_max >> bm::set_block_shift);
    unsigned i_from, j_from, i_to, j_to;
    bm::get_block_coord(nblock_right, i_to, j_to);
    block_idx_type top_size = bman.top_block_size();
    if (i_to >= top_size)
        i_to = unsigned(top_size-1);

    ++nb;
    bm::word_t*** blk_root = bman.top_blocks_root();
    bm::get_block_coord(nb, i_from, j_from);

    for (unsigned i = i_from; i <= i_to; ++i)
    {
        bm::word_t** blk_blk = blk_root[i];
        if (!blk_blk)
            return true;
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            if (i > i_from)
            {
                pos += bm::gap_max_bits * bm::set_sub_array_size;
                continue;
            }
            else
            {
                // TODO: optimization to avoid scanning rest of the super block
            }
        }

        unsigned j = (i == i_from) ? j_from : 0;
        do
        {
            if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            {
                pos += bm::gap_max_bits;
                continue;
            }

            block = blk_blk[j];
            if (!block)
                return true;

            res = bm::block_find_interval_end(block, 0, &found_nbit);
            switch (res)
            {
            case 0: // not interval (but it was the interval, so last result
                return true;
            case 1: // interval found
                pos += found_nbit+1;
                return true;
            case 2: // keep scanning
                pos += bm::gap_max_bits;
                break;
            default:
                BM_ASSERT(0);
            } // switch
        } while (++j < bm::set_sub_array_size);
    } // for i

    return true;
}



//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------

template<typename BV>
typename interval_enumerator<BV>::size_type
interval_enumerator<BV>::start() const BMNOEXCEPT
{
    return interval_.first;
}

//----------------------------------------------------------------------------

template<typename BV>
typename interval_enumerator<BV>::size_type
interval_enumerator<BV>::end() const BMNOEXCEPT
{
    return interval_.second;
}

//----------------------------------------------------------------------------

template<typename BV>
bool interval_enumerator<BV>::valid() const BMNOEXCEPT
{
    return (interval_.first != bm::id_max);
}

//----------------------------------------------------------------------------

template<typename BV>
void interval_enumerator<BV>::invalidate() BMNOEXCEPT
{
    interval_.first = interval_.second = bm::id_max;
}

//----------------------------------------------------------------------------

template<typename BV>
bool interval_enumerator<BV>::go_to(size_type pos, bool extend_start)
{
    return go_to_impl(pos, extend_start);
}

//----------------------------------------------------------------------------

template<typename BV>
bool interval_enumerator<BV>::go_to_impl(size_type pos, bool extend_start)
{
    if (!bv_ || !bv_->is_init() || (pos >= bm::id_max))
    {
        invalidate();
        return false;
    }

    bool found;
    size_type start_pos;

    // go to prolog: identify the true interval start position
    //
    if (extend_start)
    {
        found = bm::find_interval_start(*bv_, pos, start_pos);
        if (!found)
        {
            found = bv_->find(pos, start_pos);
            if (!found)
            {
                invalidate();
                return false;
            }
        }
    }
    else
    {
        found = bv_->find(pos, start_pos);
        if (!found)
        {
            invalidate();
            return false;
        }
    }

    // start position established, start decoding from it
    interval_.first = pos = start_pos;

    block_idx_type nb = (pos >> bm::set_block_shift);
    const typename BV::blocks_manager_type& bman = bv_->get_blocks_manager();
    unsigned i0, j0;
    bm::get_block_coord(nb, i0, j0);
    const bm::word_t* block = bman.get_block_ptr(i0, j0);
    BM_ASSERT(block);

    if (block == FULL_BLOCK_FAKE_ADDR)
    {
        // super-long interval, find the end of it
        found = bm::find_interval_end(*bv_, pos, interval_.second);
        BM_ASSERT(found);
        gap_ptr_ = 0;
        return true;
    }

    if (BM_IS_GAP(block))
    {
        const bm::gap_word_t* BMRESTRICT gap_block = BMGAP_PTR(block);
        unsigned nbit = unsigned(pos  & bm::set_block_mask);

        unsigned is_set;
        unsigned gap_pos = bm::gap_bfind(gap_block, nbit, &is_set);
        BM_ASSERT(is_set);

        interval_.second = (nb * bm::gap_max_bits) + gap_block[gap_pos];
        if (gap_block[gap_pos] == bm::gap_max_bits-1)
        {
            // it is the end of the GAP block - run search
            //
            if (interval_.second == bm::id_max-1)
            {
                gap_ptr_ = 0;
                return true;
            }
            found = bm::find_interval_end(*bv_, interval_.second + 1, start_pos);
            if (found)
                interval_.second = start_pos;
            gap_ptr_ = 0;
            return true;
        }
        gap_ptr_ = gap_block + gap_pos;
        return true;
    }

    // bit-block: turn to GAP and position there
    //
    if (gap_buf_.size() == 0)
    {
        gap_buf_.resize(bm::gap_max_bits+64);
    }
    bm::gap_word_t* gap_tmp = gap_buf_.data();
    unsigned len = bm::bit_to_gap(gap_tmp, block, bm::gap_max_bits+64);
    BM_ASSERT(len);


    size_type base_idx = (nb * bm::gap_max_bits);
    for (unsigned i = 1; i <= len; ++i)
    {
        size_type gap_pos = base_idx + gap_tmp[i];
        if (gap_pos >= pos)
        {
            if (gap_tmp[i] == bm::gap_max_bits - 1)
            {
                found = bm::find_interval_end(*bv_, gap_pos, interval_.second);
                BM_ASSERT(found);
                gap_ptr_ = 0;
                return true;
            }

            gap_ptr_ = &gap_tmp[i];
            interval_.second = gap_pos;
            return true;
        }
        if (gap_tmp[i] == bm::gap_max_bits - 1)
            break;
    } // for

    BM_ASSERT(0);

    return false;
}

//----------------------------------------------------------------------------

template<typename BV>
bool interval_enumerator<BV>::advance()
{
    BM_ASSERT(valid());

    if (interval_.second == bm::id_max-1)
    {
        invalidate();
        return false;
    }
    block_idx_type nb = (interval_.first >> bm::set_block_shift);

    bool found;
    if (gap_ptr_) // in GAP block
    {
        ++gap_ptr_; // 0 - GAP
        if (*gap_ptr_ == bm::gap_max_bits-1) // GAP block end
        {
            return go_to_impl(((nb+1) * bm::gap_max_bits), false);
        }
        unsigned prev = *gap_ptr_;

        ++gap_ptr_; // 1 - GAP
        BM_ASSERT(*gap_ptr_ > prev);
        interval_.first = (nb * bm::gap_max_bits) + prev + 1;
        if (*gap_ptr_ == bm::gap_max_bits-1) // GAP block end
        {
            found = bm::find_interval_end(*bv_, interval_.first, interval_.second);
            BM_ASSERT(found); (void)found;
            gap_ptr_ = 0;
            return true;
        }
        interval_.second = (nb * bm::gap_max_bits) + *gap_ptr_;
        return true;
    }
    return go_to_impl(interval_.second + 1, false);
}

//----------------------------------------------------------------------------

template<typename BV>
void interval_enumerator<BV>::swap(interval_enumerator<BV>& ien) BMNOEXCEPT
{
    const BV* bv_tmp = bv_;
    bv_ = ien.bv_;
    ien.bv_ = bv_tmp;

    gap_buf_.swap(ien.gap_buf_);
    bm::xor_swap(interval_.first, ien.interval_.first);
    bm::xor_swap(interval_.second, ien.interval_.second);

    const bm::gap_word_t* gap_tmp = gap_ptr_;
    gap_ptr_ = ien.gap_ptr_;
    ien.gap_ptr_ = gap_tmp;
}

//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------


} // namespace bm


#endif
