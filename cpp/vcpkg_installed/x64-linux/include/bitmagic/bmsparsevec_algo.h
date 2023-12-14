#ifndef BMSPARSEVEC_ALGO__H__INCLUDED__
#define BMSPARSEVEC_ALGO__H__INCLUDED__
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
/*! \file bmsparsevec_algo.h
    \brief Algorithms for bm::sparse_vector
*/

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration 
// #include "bm.h" or "bm64.h" explicitly 
# error missing include (bm.h or bm64.h)
#endif

#include <limits>
#include <string.h>

#include "bmdef.h"
#include "bmsparsevec.h"
#include "bmaggregator.h"
#include "bmbuffer.h"
#include "bmalgo.h"
#include "bmdef.h"

#ifdef _MSC_VER
# pragma warning( disable: 4146 )
#endif



/** \defgroup svalgo Sparse vector algorithms
    Sparse vector algorithms
    \ingroup svector
 */


namespace bm
{


/*!
    \brief Clip dynamic range for signal higher than specified
    
    \param  svect - sparse vector to do clipping
    \param  high_bit - max bit (inclusive) allowed for this signal vector
    
    
    \ingroup svalgo
    \sa dynamic_range_clip_low
*/
template<typename SV>
void dynamic_range_clip_high(SV& svect, unsigned high_bit)
{
    unsigned sv_planes = svect.slices();
    
    BM_ASSERT(sv_planes > high_bit && high_bit > 0);
    
    typename SV::bvector_type bv_acc;
    unsigned i;
    
    // combine all the high bits into accumulator vector
    for (i = high_bit+1; i < sv_planes; ++i)
    {
        if (typename SV::bvector_type* bv = svect.slice(i))
        {
            bv_acc.bit_or(*bv);
            svect.free_slice(i);
        }
    } // for i
    
    // set all bits ON for all low vectors, which happen to be clipped
    for (i = high_bit; true; --i)
    {
        typename SV::bvector_type* bv = svect.get_create_slice(i);
        bv->bit_or(bv_acc);
        if (!i)
            break;
    } // for i
}


/*!
    \brief Clip dynamic range for signal lower than specified (boost)
    
    \param  svect - sparse vector to do clipping
    \param  low_bit - low bit (inclusive) allowed for this signal vector
    
    \ingroup svalgo
    \sa dynamic_range_clip_high 
*/
template<typename SV>
void dynamic_range_clip_low(SV& svect, unsigned low_bit)
{
    if (low_bit == 0) return; // nothing to do
    BM_ASSERT(svect.slices() > low_bit);
    
    unsigned sv_planes = svect.slices();
    typename SV::bvector_type bv_acc1;
    unsigned i;
    
    // combine all the high bits into accumulator vector
    for (i = low_bit+1; i < sv_planes; ++i)
    {
        if (typename SV::bvector_type* bv = svect.slice(i))
            bv_acc1.bit_or(*bv);
    } // for i
    
    // accumulate all vectors below the clipping point
    typename SV::bvector_type bv_acc2;
    typename SV::bvector_type* bv_low_plane = svect.get_create_slice(low_bit);
    
    for (i = low_bit-1; true; --i)
    {
        if (typename SV::bvector_type* bv = svect.slice(i))
        {
            bv_acc2.bit_or(*bv);
            svect.free_slice(i);
            if (i == 0)
                break;
        }
    } // for i
    
    // now we want to set 1 in the clipping low plane based on
    // exclusive or (XOR) between upper and lower parts)
    // as a result high signal (any bits in the upper planes) gets
    // slightly lower value (due to clipping) low signal gets amplified
    // (lower contrast algorithm)
    
    bv_acc1.bit_xor(bv_acc2);
    bv_low_plane->bit_or(bv_acc1);
}

/**
    Find first mismatch (element which is different) between two sparse vectors
    (uses linear scan in bit-vector planes)

    Function works with both NULL and NOT NULL vectors
    NULL means unassigned (uncertainty), so first mismatch NULL is a mismatch
    even if values in vectors can be formally the same (i.e. 0)

    @param sv1 - vector 1
    @param sv2 - vector 2
    @param midx - mismatch index
    @param null_proc - defines if we want to include (not) NULL
                  vector into comparison (bm::use_null) or not.
                  By default search takes NULL vector into account 

    @return true if mismatch found

    @sa sparse_vector_find_mismatch

    \ingroup svalgo
*/
template<typename SV>
bool sparse_vector_find_first_mismatch(const SV& sv1,
                                       const SV& sv2,
                                       typename SV::size_type& midx,
                                       bm::null_support  null_proc = bm::use_null)
{
    typename SV::size_type mismatch = bm::id_max;
    bool found = false;

    unsigned sv_idx = 0;

    unsigned planes1 = (unsigned)sv1.get_bmatrix().rows();
    BM_ASSERT(planes1);

    typename SV::bvector_type_const_ptr bv_null1 = sv1.get_null_bvector();
    typename SV::bvector_type_const_ptr bv_null2 = sv2.get_null_bvector();

    // for RSC vector do NOT compare NULL planes

    if (bm::conditional<SV::is_rsc_support::value>::test())
    {
        //--planes1;
    }
    else // regular sparse vector - may have NULL planes
    {
        if (null_proc == bm::use_null)
        {
            if (bv_null1 && bv_null2) // both (not) NULL vectors present
            {
                bool f = bv_null1->find_first_mismatch(*bv_null2, midx, mismatch);
                if (f && (midx < mismatch)) // better mismatch found
                {
                    found = f; mismatch = midx;
                }
            }
            else // one or both NULL vectors are not present
            {
                if (bv_null1)
                {
                    typename SV::bvector_type bv_tmp; // TODO: get rid of temp bv
                    bv_tmp.resize(sv2.size());
                    bv_tmp.invert(); // turn into true NULL vector

                    // find first NULL value (mismatch)
                    bool f = bv_null1->find_first_mismatch(bv_tmp, midx, mismatch);
                    if (f && (midx < mismatch)) // better mismatch found
                    {
                        found = f; mismatch = midx;
                    }
                }
                if (bv_null2)
                {
                    typename SV::bvector_type bv_tmp; // TODO: get rid of temp bv
                    bv_tmp.resize(sv1.size());
                    bv_tmp.invert();

                    bool f = bv_null2->find_first_mismatch(bv_tmp, midx, mismatch);
                    if (f && (midx < mismatch)) // better mismatch found
                    {
                        found = f; mismatch = midx;
                    }
                }
            }
        } // null_proc
    }

    for (unsigned i = 0; mismatch && (i < planes1); ++i)
    {
        typename SV::bvector_type_const_ptr bv1 = sv1.get_slice(i);
        if (bv1 == bv_null1)
            bv1 = 0;
        typename SV::bvector_type_const_ptr bv2 = sv2.get_slice(i);
        if (bv2 == bv_null2)
            bv2 = 0;

        if (!bv1)
        {
            if (!bv2)
                continue;
            bool f = bv2->find(midx);
            if (f && (midx < mismatch))
            {
                found = f; sv_idx = 2; mismatch = midx;
            }
            continue;
        }
        if (!bv2)
        {
            BM_ASSERT(bv1);
            bool f = bv1->find(midx);
            if (f && (midx < mismatch))
            {
                found = f; sv_idx = 1; mismatch = midx;
            }
            continue;
        }
        // both planes are not NULL
        //
        bool f = bv1->find_first_mismatch(*bv2, midx, mismatch);
        if (f && (midx < mismatch)) // better mismatch found
        {
            found = f; mismatch = midx;
            // which vector has '1' at mismatch position?
            if (bm::conditional<SV::is_rsc_support::value>::test())
                sv_idx = (bv1->test(mismatch)) ? 1 : 2;
        }

    } // for i

    // RSC address translation here
    //
    if (bm::conditional<SV::is_rsc_support::value>::test())
    {
        if (found) // RSC address translation
        {
            BM_ASSERT(sv1.is_compressed());
            BM_ASSERT(sv2.is_compressed());

            switch (sv_idx)
            {
            case 1:
                found = sv1.find_rank(midx + 1, mismatch);
                break;
            case 2:
                found = sv2.find_rank(midx + 1, mismatch);
                break;
            default: // unknown, try both
                BM_ASSERT(0);
            }
            BM_ASSERT(found);
            midx = mismatch;
        }
        if (null_proc == bm::use_null)
        {
            // no need for address translation in this case
            bool f = bv_null1->find_first_mismatch(*bv_null2, midx, mismatch);
            if (f && (midx < mismatch)) // better mismatch found
            {
                found = f; mismatch = midx;
            }
        }

/*
        else // search for mismatch in the NOT NULL vectors
        {
            if (null_proc == bm::use_null)
            {
                // no need for address translation in this case
                typename SV::bvector_type_const_ptr bv_null1 = sv1.get_null_bvector();
                typename SV::bvector_type_const_ptr bv_null2 = sv2.get_null_bvector();
                found = bv_null1->find_first_mismatch(*bv_null2, mismatch);
            }
        }
*/
    }

    midx = mismatch; // minimal mismatch
    return found;
}

/**
    Find mismatch vector, indicating positions of mismatch between two sparse vectors
    (uses linear scan in bit-vector planes)

    Function works with both NULL and NOT NULL vectors

    @param bv - [out] - bit-ector with mismatch positions indicated as 1s
    @param sv1 - vector 1
    @param sv2 - vector 2
    @param null_proc - rules of processing for (not) NULL plane
      bm::no_null - NULLs from both vectors are treated as uncertainty
                    and NOT included into final result
      bm::use_null - difference in NULLs accounted into the result

    @sa sparse_vector_find_first_mismatch

    \ingroup svalgo
*/
template<typename SV1, typename SV2>
void sparse_vector_find_mismatch(typename SV1::bvector_type& bv,
                                 const SV1&                  sv1,
                                 const SV2&                  sv2,
                                 bm::null_support            null_proc)
{
    typedef typename SV1::bvector_type       bvector_type;
    typedef typename bvector_type::allocator_type        allocator_type;
    typedef typename allocator_type::allocator_pool_type allocator_pool_type;

    allocator_pool_type  pool; // local pool for blocks
    typename bvector_type::mem_pool_guard mp_guard_bv;
    mp_guard_bv.assign_if_not_set(pool, bv);


    if (bm::conditional<SV1::is_rsc_support::value>::test())
    {
        BM_ASSERT(0); // TODO: fixme
    }
    if (bm::conditional<SV2::is_rsc_support::value>::test())
    {
        BM_ASSERT(0); // TODO: fixme
    }

    bv.clear();

    unsigned planes = sv1.effective_slices();; // slices();
    if (planes < sv2.slices())
        planes = sv2.slices();

    for (unsigned i = 0; i < planes; ++i)
    {
        typename SV1::bvector_type_const_ptr bv1 = sv1.get_slice(i);
        typename SV2::bvector_type_const_ptr bv2 = sv2.get_slice(i);

        if (!bv1)
        {
            if (!bv2)
                continue;
            bv |= *bv2;
            continue;
        }
        if (!bv2)
        {
            BM_ASSERT(bv1);
            bv |= *bv1;
            continue;
        }

        // both planes are not NULL, compute XOR diff
        //
        bvector_type bv_xor;
        typename bvector_type::mem_pool_guard mp_guard;
        mp_guard.assign_if_not_set(pool, bv_xor);

        bv_xor.bit_xor(*bv1, *bv2, SV1::bvector_type::opt_none);
        bv |= bv_xor;

    } // for i

    // size mismatch check
    {
        typename SV1::size_type sz1 = sv1.size();
        typename SV2::size_type sz2 = sv2.size();
        if (sz1 != sz2)
        {
            if (sz1 < sz2)
            {
            }
            else
            {
                bm::xor_swap(sz1, sz2);
            }
            bv.set_range(sz1, sz2-1);
        }
    }

    // NULL processings
    //
    typename SV1::bvector_type_const_ptr bv_null1 = sv1.get_null_bvector();
    typename SV2::bvector_type_const_ptr bv_null2 = sv2.get_null_bvector();

    switch (null_proc)
    {
    case bm::no_null:
        // NULL correction to exclude all NULL (unknown) values from the result set
        //  (AND with NOT NULL vector)
        if (bv_null1 && bv_null2)
        {
            bvector_type bv_or;
            typename bvector_type::mem_pool_guard mp_guard;
            mp_guard.assign_if_not_set(pool, bv_or);

            bv_or.bit_or(*bv_null1, *bv_null2, bvector_type::opt_none);
            bv &= bv_or;
        }
        else
        {
            if (bv_null1)
                bv &= *bv_null1;
            if (bv_null2)
                bv &= *bv_null2;
        }
    break;
    case bm::use_null:
        if (bv_null1 && bv_null2)
        {
            bvector_type bv_xor;
            typename bvector_type::mem_pool_guard mp_guard;
            mp_guard.assign_if_not_set(pool, bv_xor);

            bv_xor.bit_xor(*bv_null1, *bv_null2, bvector_type::opt_none);
            bv |= bv_xor;
        }
        else
        {
            bvector_type bv_null;
            typename bvector_type::mem_pool_guard mp_guard;
            mp_guard.assign_if_not_set(pool, bv_null);
            if (bv_null1)
            {
                bv_null = *bv_null1;
                bv_null.resize(sv1.size());
            }
            if (bv_null2)
            {
                bv_null = *bv_null2;
                bv_null.resize(sv2.size());
            }
            bv_null.invert();
            bv |= bv_null;
        }
    break;
    default:
        BM_ASSERT(0);
    }
}

/**
    \brief Index for SV sorted vectors for approximate range queries

    @internal
 */
template<typename SV>
class sv_sample_index
{
public:
    typedef typename SV::value_type                             value_type;
    typedef typename SV::size_type                              size_type;
    typedef typename SV::bvector_type                           bvector_type;
    typedef typename bvector_type::allocator_type               allocator_type;
    typedef bm::dynamic_heap_matrix<value_type, allocator_type> heap_matrix_type;

    sv_sample_index(){}
    sv_sample_index(const SV& sv, unsigned s_factor)
    {
        construct(sv, s_factor);
    }



    /**
        Build sampling index for the sorted sprase vector
        @param sv - string sparse vector to index
        @param s_factor - sampling factor
    */
    void construct(const SV& sv, unsigned s_factor);


    /// Original SV size
    size_type sv_size() const BMNOEXCEPT { return sv_size_; }

    /// Index size (number of sampled elements)
    size_type size() const BMNOEXCEPT { return idx_size_; }

    /// returns true if all index values are unique
    bool is_unique() const BMNOEXCEPT { return idx_unique_; }

    /// find range (binary)
    /// @internal
    bool bfind_range(const value_type* search_str,
                     size_t            in_len,
                     size_type&        l,
                     size_type&        r) const BMNOEXCEPT;

    /// find common prefix between index elements and search string
    ///
    size_type common_prefix_length(const value_type* search_str,
                                   size_t in_len,
                                   size_type l, size_type r) const BMNOEXCEPT;


    /**
        recalculate range into SV coordinates range [from..to)
    */
    void recalc_range(const value_type* search_str,
                      size_type&        l,
                      size_type&        r) const BMNOEXCEPT;

    /// Return length of minimal indexed string
    size_t get_min_len() const BMNOEXCEPT { return min_key_len_; }



private:
    heap_matrix_type      s_cache_; ///< cache for SV sampled elements
    unsigned              s_factor_ = 0;
    size_type             sv_size_ = 0;       ///< original sv size
    size_type             idx_size_ = 0;      ///< index size
    bool                  idx_unique_ = true; ///< inx value unique or there are dups?
    size_t                min_key_len_ = 0;   ///< minimal key size in index
};


/**
    \brief algorithms for sparse_vector scan/search
 
    Scanner uses properties of bit-vector planes to answer questions
    like "find all sparse vector elements equivalent to XYZ".

    Class uses fast algorithms based on properties of bit-planes.
    This is NOT a brute force, direct scan, scanner uses search space pruning and cache optimizations
    to run the search.

    S_FACTOR - Sampling factor for search. Can be: [ 4, 8, 16, 32, 64 ]. Default: 16.
      Lower sampling facor (4, 8) lowers memory footprint for the scanner class instance
      Higher - improves search performance (at the expense for memory for sampled elements)
      Sampling factor is used for binary search in bound string sparse vector, so memory consumption
      depends on sampling and max string length.
 
    @ingroup svalgo
    @ingroup setalgo
*/
template<typename SV, unsigned S_FACTOR>
class sparse_vector_scanner
{
public:
    typedef typename SV::bvector_type       bvector_type;
    typedef const bvector_type*             bvector_type_const_ptr;
    typedef bvector_type*                   bvector_type_ptr;
    typedef typename SV::value_type         value_type;
    typedef typename SV::size_type          size_type;
    
    typedef typename bvector_type::allocator_type        allocator_type;
    typedef typename allocator_type::allocator_pool_type allocator_pool_type;

    typedef bm::aggregator<bvector_type>                 aggregator_type;
    typedef
    bm::heap_vector<value_type, typename bvector_type::allocator_type, true>
                                                    remap_vector_type;
    typedef
    bm::heap_vector<bvector_type*, allocator_type, true> bvect_vector_type;
    typedef
    typename aggregator_type::bv_count_vector_type bv_count_vector_type;

    /**
       Scanner options to control execution
     */
    typedef
    typename aggregator_type::run_options run_options;

public:


    /**
        Pipeline to run multiple searches against a particular SV faster using cache optimizations.
        USAGE:
        - setup the pipeline (add searches)
        - run all searches at once
        - get the search results (in the order it was added)
     */
    template<class Opt = bm::agg_run_options<> >
    class pipeline
    {
    public:
        typedef
        typename aggregator_type::template pipeline<Opt> aggregator_pipeline_type;
    public:
        pipeline(const SV& sv)
            : sv_(sv), bv_and_mask_(0),
            eff_slices_(sv_.get_bmatrix().calc_effective_rows_not_null())
        {}

        /// Set pipeline run options
        run_options& options() BMNOEXCEPT
            { return agg_pipe_.options(); }

        /// Get pipeline run options
        const run_options& get_options() const BMNOEXCEPT
            { return agg_pipe_.options(); }

        /** Set pipeline mask bit-vector to restrict the search space
            @param bv_mask - pointer to bit-vector restricting search to vector indexes marked
            as 1s. Pointer ownership is not transferred, NULL value resets the mask.
            bv_mask defines the mask for all added searches.

        */
        void set_search_mask(const bvector_type* bv_mask) BMNOEXCEPT;

        /**
          Set search limit for results. Requires that bit-counting to be enabled in the template parameters.
          Warning: search limit is approximate (for performance reasons) so it can occasinally find more
          than requested. It cannot find less. Search limit works for individual results (not ORed aggregate)
          @param limit - search limit (target population count to search for)
         */
        void set_search_count_limit(size_type limit) BMNOEXCEPT
            { agg_pipe_.set_search_count_limit(limit); }

        /**
            Attach OR (aggregator vector).
            Pipeline results all will be OR-ed (UNION) into the OR target vector
           @param bv_or - OR target vector
         */
        void set_or_target(bvector_type* bv_or) BMNOEXCEPT
            { agg_pipe_.set_or_target(bv_or); }

        /*! @name pipeline fill-in methods */
        //@{

        /**
            Add new search string
            @param str - zero terminated string pointer to configure a search for
         */
        void add(const typename SV::value_type* str);

        /** Prepare pipeline for the execution (resize and init internal structures)
            Once complete, you cannot add() to it.
        */
        void complete() { agg_pipe_.complete(); }
        
        bool is_complete() const BMNOEXCEPT
            { return agg_pipe_.is_complete(); }

        size_type size() const BMNOEXCEPT { return agg_pipe_.size(); }

        //@}

        /** Return results vector used for pipeline execution */
        bvect_vector_type& get_bv_res_vector()  BMNOEXCEPT
            { return agg_pipe_.get_bv_res_vector(); }
        /** Return results vector count used for pipeline execution */

        bv_count_vector_type& get_bv_count_vector()  BMNOEXCEPT
            { return agg_pipe_.get_bv_count_vector(); }


        /**
            get aggregator pipeline (access to compute internals)
            @internal
         */
         aggregator_pipeline_type& get_aggregator_pipe() BMNOEXCEPT
            { return agg_pipe_; }

    protected:
        friend class sparse_vector_scanner;

        pipeline(const pipeline&) = delete;
        pipeline& operator=(const pipeline&) = delete;
    protected:
        const SV&                    sv_;               ///< target sparse vector ref
        aggregator_pipeline_type     agg_pipe_;         ///< traget aggregator pipeline
        remap_vector_type            remap_value_vect_; ///< remap buffer
        const bvector_type*          bv_and_mask_;
        size_t                       eff_slices_; ///< number of effectice NOT NULL value slices
    };

public:
    sparse_vector_scanner();

    /**
        \brief bind sparse vector for all searches
        
        \param sv - input sparse vector to bind for searches
        \param sorted - source index is sorted, build index for binary search
    */
    void bind(const SV& sv, bool sorted);

    /**
        \brief reset sparse vector binding
    */
    void reset_binding() BMNOEXCEPT;


    /*! @name Find in scalar vector */
    //@{

    /**
        \brief find all sparse vector elements EQ to search value

        Find all sparse vector elements equivalent to specified value

        \param sv - input sparse vector
        \param value - value to search for
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] == value)
    */
    void find_eq(const SV&  sv, value_type  value, bvector_type& bv_out);


    /**
        \brief find all sparse vector elements EQ to search value

        Find all sparse vector elements equivalent to specified value

        \param sv - input sparse vector
        \param value - value to search for
        \param bi - back insert iterator for the search results
    */
    template<typename BII>
    void find_eq(const SV&  sv, value_type  value, BII bi);


    /**
        \brief find first sparse vector element

        Find all sparse vector elements equivalent to specified value.
        Works well if sperse vector represents unordered set

        \param sv - input sparse vector
        \param value - value to search for
        \param pos - output found sparse vector element index
     
        \return true if found
    */
    bool find_eq(const SV&  sv, value_type value, size_type& pos);

    /**
        \brief binary search for position in the sorted sparse vector

        Method assumes the sparse array is sorted, if value is found pos returns its index,
        if not found, pos would contain index where it could be inserted to maintain the order

        \param sv - input sparse vector
        \param val - value to search for
        \param pos - output sparse vector element index (actual index if found or insertion
                    point if not found)

        \return true if value found
    */
    bool bfind(const SV&  sv, const value_type  val, size_type&  pos);

    /**
        \brief find all elements  sparse vector element greater (>) than value

        \param sv - input sparse vector
        \param value - value to search for
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] > value)
    */
    void find_gt(const SV& sv, value_type  val, bvector_type&  bv_out);

    /**
        \brief find all elements  sparse vector element greater or equal (>=) than value

        \param sv - input sparse vector
        \param value - value to search for
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] >= value)
    */
    void find_ge(const SV& sv, value_type  val, bvector_type&  bv_out);


    /**
        \brief find all elements  sparse vector element less  (<) than value

        \param sv - input sparse vector
        \param value - value to search for
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] < value)
    */
    void find_lt(const SV& sv, value_type  val, bvector_type&  bv_out);

    /**
        \brief find all elements  sparse vector element less  or equal (<=) than value

        \param sv - input sparse vector
        \param value - value to search for
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] <= value)
    */
    void find_le(const SV& sv, value_type  val, bvector_type&  bv_out);


    /**
        \brief find all elements  sparse vector element in closed range [left..right] interval

        \param sv - input sparse vector
        \param from - range from
        \param to - range to
        \param bv_out - search result bit-vector (search result  is a vector of 1s when sv[i] <= value)
    */
    void find_range(const SV&  sv,
                    value_type from, value_type to,
                    bvector_type&  bv_out);


    //@}


    // --------------------------------------------------
    //
    /*! @name Find in bit-transposed string vector */
    //@{


    /**
        \brief find sparse vector elements (string)
        @param sv  - sparse vector of strings to search
        @param str - string to search for
        @param bv_out - search result bit-vector (search result masks 1 elements)
    */
    bool find_eq_str(const SV& sv, const value_type* str, bvector_type& bv_out);

    /**
        \brief find sparse vector elementa (string) in the attached SV
        @param str - string to search for
        @param bv_out - search result bit-vector (search result masks 1 elements)
    */
    bool find_eq_str(const value_type* str, bvector_type& bv_out);

    /**
        \brief find first sparse vector element (string)
        @param sv  - sparse vector of strings to search
        @param str - string to search for
        @param pos - [out] index of the first found
    */
    bool find_eq_str(const SV& sv, const value_type* str, size_type&  pos);

    /**
        \brief binary find first sparse vector element (string)
        Sparse vector must be attached (bind())
        @param str - string to search for
        @param pos - [out] index of the first found
        @sa bind
    */
    bool find_eq_str(const value_type* str, size_type& pos);

    /**
        \brief find sparse vector elements  with a given prefix (string)
        @param sv  - sparse vector of strings to search
        @param str - string prefix to search for
                     "123" is a prefix for "1234567"
        @param bv_out - search result bit-vector (search result masks 1 elements)
    */
    bool find_eq_str_prefix(const SV& sv, const value_type* str,
                            bvector_type& bv_out);

    /**
        \brief find sparse vector elements using search pipeline
        @param pipe  - pipeline to run
     */
    template<class TPipe>
    void find_eq_str(TPipe& pipe);

    /**
        \brief binary find first sparse vector element (string). Sparse vector must be sorted.

        @param sv  - sparse vector of strings to search
        @param str - string prefix to search for
        @param pos - [out] first position found
    */
    bool bfind_eq_str(const SV& sv,
                      const value_type* str, size_type& pos);

    /**
        \brief lower bound search for an array position
     
        Method assumes the sparse array is sorted
     
        \param sv - input sparse vector
        \param str - value to search for
        \param pos - output sparse vector element index

        \return true if value found
    */
    bool lower_bound_str(const SV&            sv,
                         const value_type*    str,
                         size_type&           pos);

    /**
        \brief binary find first sparse vector element (string)
        Sparse vector must be sorted and attached (use method bind())

        @param str - string prefix to search for
        @param pos - [out] first position found

        @sa bind
    */
    bool bfind_eq_str(const value_type* str, size_type& pos);

    /**
        \brief binary find first sparse vector element (string)
        Sparse vector must be sorted and attached (use method bind())

        @param str - string prefix to search for
        @param len - string length
        @param pos - [out] first position found

        @sa bind
    */
    bool bfind_eq_str(const value_type* str, size_t len, size_type& pos);


    //@}

    /**
        \brief find all sparse vector elements EQ to 0
        \param sv - input sparse vector
        \param bv_out - output bit-vector (search result masks 1 elements)
        \param null_correct - flag to perform NULL correction
    */
    void find_zero(const SV& sv, bvector_type& bv_out, bool null_correct = true);

    /*!
        \brief Find non-zero elements
        Output vector is computed as a logical OR (join) of all planes

        \param  sv - input sparse vector
        \param  bv_out - output bit-bector of non-zero elements
    */
    void find_nonzero(const SV& sv, bvector_type& bv_out);

    /*!
        \brief Find positive (greter than zero elements)
        Output vector is computed as a logical OR (join) of all planes

        \param  sv - input sparse vector
        \param  bv_out - output bit-bector of non-zero elements
    */
    void find_positive(const SV& sv, bvector_type& bv_out);


    /**
        \brief invert search result ("EQ" to "not EQ")

        \param  sv - input sparse vector
        \param  bv_out - output bit-bector of non-zero elements
    */
    void invert(const SV& sv, bvector_type& bv_out);

    /**
        \brief find all values A IN (C, D, E, F)
        \param  sv - input sparse vector
        \param  start - start iterator (set to search) 
        \param  end   - end iterator (set to search)
        \param  bv_out - output bit-bector of non-zero elements
     */
    template<typename It>
    void find_eq(const SV&  sv,
                 It    start, 
                 It    end,
                 bvector_type& bv_out)
    {
        typename bvector_type::mem_pool_guard mp_guard;
        mp_guard.assign_if_not_set(pool_, bv_out); // set algorithm-local memory pool to avoid heap contention

        bvector_type bv1;
        typename bvector_type::mem_pool_guard mp_guard1(pool_, bv1);
        bool any_zero = false;
        for (; start < end; ++start)
        {
            value_type v = *start;
            any_zero |= (v == 0);
            bool found = find_eq_with_nulls(sv, v, bv1);
            if (found)
                bv_out.bit_or(bv1);
            else
            {
                BM_ASSERT(!bv1.any());
            }
        } // for
        if (any_zero)
            correct_nulls(sv, bv_out);
    }


    /// For testing purposes only
    ///
    /// @internal
    void find_eq_with_nulls_horizontal(const SV& sv, value_type value,
                                       bvector_type&  bv_out);

    /// For testing purposes only
    ///
    /// @internal
    void find_gt_horizontal(const SV&      sv,
                            value_type     value,
                            bvector_type&  bv_out,
                            bool null_correct = true);


    /** Exclude possible NULL values from the result vector
        \param  sv - input sparse vector
        \param  bv_out - output bit-bector of non-zero elements
    */
    void correct_nulls(const SV& sv, bvector_type& bv_out);

    /// Return allocator pool for blocks
    ///  (Can be used to improve performance of repeated searches with the same scanner)
    ///
    allocator_pool_type& get_bvector_alloc_pool() BMNOEXCEPT { return pool_; }
    
protected:

    template<bool BOUND>
    bool bfind_eq_str_impl(const SV& sv,
                           const value_type* str, size_t in_len,
                           bool remaped,
                           size_type& pos);


    /// Remap input value into SV char encodings
    static
    bool remap_tosv(remap_vector_type& remap_vect_target,
                    const value_type* str,
                    const SV& sv);

    /// set search boundaries (hint for the aggregator)
    void set_search_range(size_type from, size_type to) BMNOEXCEPT;
    
    /// reset (disable) search range
    void reset_search_range() BMNOEXCEPT;

    /// find value (may include NULL indexes)
    bool find_eq_with_nulls(const SV&   sv,
                            value_type         value,
                            bvector_type&      bv_out,
                            size_type          search_limit = 0);

    /// find first value (may include NULL indexes)
    bool find_first_eq(const SV&   sv,
                       value_type  value,
                       size_type&  idx);
    
    /// find first string value (may include NULL indexes)
    bool find_first_eq(const SV&          sv,
                       const value_type*  str,
                       size_t             in_len,
                       size_type&         idx,
                       bool               remaped,
                       unsigned           prefix_len);

    /// find EQ str / prefix impl
    bool find_eq_str_impl(const SV&                      sv,
                     const value_type* str,
                     bvector_type& bv_out,
                     bool prefix_sub);

    /// Prepare aggregator for AND-SUB (EQ) search
    bool prepare_and_sub_aggregator(const SV& sv, value_type value);

    /// Prepare aggregator for AND-SUB (EQ) search (string)
    bool prepare_and_sub_aggregator(const SV&  sv,
                                    const value_type*  str,
                                    unsigned octet_start,
                                    bool prefix_sub);

    /// Rank-Select decompression for RSC vectors
    void decompress(const SV&   sv, bvector_type& bv_out);

    /// compare sv[idx] with input str
    template <bool BOUND>
    int compare_str(const SV& sv, size_type idx,
                    const value_type* str) const BMNOEXCEPT;

    /// compare sv[idx] with input value
    int compare(const SV& sv, size_type idx, const value_type val) BMNOEXCEPT;

    ///  @internal
    void aggregate_OR_slices(bvector_type& bv_target, const SV& sv,
                unsigned from, unsigned total_planes);

    ///  @internal
    static
    void aggregate_AND_OR_slices(bvector_type& bv_target,
                const bvector_type& bv_mask,
                const SV& sv,
                unsigned from, unsigned total_planes);

    /// Return the slice limit for signed/unsigned vector value types
    /// @internal
    static constexpr int gt_slice_limit() noexcept
    {
        if constexpr (std::is_signed<value_type>::value)
            return 1; // last plane
        else
            return 0;
    }

    void resize_buffers()
    {
        value_vect_.resize_no_copy(effective_str_max_ * 2);
        remap_value_vect_.resize_no_copy(effective_str_max_ * 2);
        remap_prefix_vect_.resize_no_copy(effective_str_max_ * 2);
    }


protected:
    sparse_vector_scanner(const sparse_vector_scanner&) = delete;
    void operator=(const sparse_vector_scanner&) = delete;

protected:
    enum search_algo_params
    {
        linear_cutoff1 = 16,
        linear_cutoff2 = 128
    };

    typedef bm::dynamic_heap_matrix<value_type, allocator_type> heap_matrix_type;
    typedef bm::dynamic_heap_matrix<value_type, allocator_type> matrix_search_buf_type;

    typedef
    bm::heap_vector<bm::id64_t, typename bvector_type::allocator_type, true>
                                            mask_vector_type;

protected:

    /// Addd char  to aggregator (AND-SUB)
    template<typename AGG>
    static
    void add_agg_char(AGG& agg,
                      const SV& sv,
                      int octet_idx, bm::id64_t planes_mask,
                      unsigned value)
    {
        unsigned char bits[64];
        unsigned short bit_count_v = bm::bitscan(value, bits);
        for (unsigned i = 0; i < bit_count_v; ++i)
        {
            unsigned bit_idx = bits[i];
            BM_ASSERT(value & (value_type(1) << bit_idx));
            unsigned plane_idx = (unsigned(octet_idx) * 8) + bit_idx;
            const bvector_type* bv = sv.get_slice(plane_idx);
            BM_ASSERT(bv != sv.get_null_bvector());
            agg.add(bv, 0); // add to the AND group
        } // for i
        unsigned vinv = unsigned(value);
        if constexpr (sizeof(value_type) == 1)
            vinv ^= 0xFF;
        else // 2-byte char
        {
            BM_ASSERT(sizeof(value_type) == 2);
            vinv ^= 0xFFFF;
        }
        // exclude the octet bits which are all not set (no vectors)
        vinv &= unsigned(planes_mask);
        for (unsigned octet_plane = (unsigned(octet_idx) * 8); vinv; vinv &= vinv-1)
        {
            bm::id_t t = vinv & -vinv;
            unsigned bit_idx = bm::word_bitcount(t - 1);
            unsigned plane_idx = octet_plane + bit_idx;
            const bvector_type* bv = sv.get_slice(plane_idx);
            BM_ASSERT(bv);
            BM_ASSERT(bv != sv.get_null_bvector());
            agg.add(bv, 1); // add to SUB group
        } // for
    }

    enum code
    {
        sub_bfind_block_cnt = S_FACTOR,
        sub_block_l1_size = bm::gap_max_bits / S_FACTOR // size in bits/elements
    };

private:
    allocator_pool_type                pool_;
    bvector_type                       bv_tmp_;
    aggregator_type                    agg_;
    bm::rank_compressor<bvector_type>  rank_compr_;
    
    size_type                          mask_from_;
    size_type                          mask_to_;
    bool                               mask_set_;
    
    const SV*                          bound_sv_;

    bm::sv_sample_index<SV>            range_idx_;      ///< range index
/*
    heap_matrix_type                   block_l0_cache_; ///< cache for elements[0] of each block
    heap_matrix_type                   block_l1_cache_; ///< cache for elements[x]
*/
    size_type                          effective_str_max_;
    
    remap_vector_type                  value_vect_;        ///< value buffer
    remap_vector_type                  remap_value_vect_;  ///< remap buffer
    remap_vector_type                  remap_prefix_vect_; ///< common prefix buffer
    /// masks of allocated bit-planes (1 - means there is a bit-plane)
    mask_vector_type                   vector_plane_masks_;
    matrix_search_buf_type             hmatr_; ///< heap matrix for string search linear stage
};


/*!
    \brief Integer set to set transformation (functional image in groups theory)
    https://en.wikipedia.org/wiki/Image_(mathematics)
 
    Input sets gets translated through the function, which is defined as
    "one to one (or NULL)" binary relation object (sparse_vector).
    Also works for M:1 relationships.
 
    \ingroup svalgo
    \ingroup setalgo
*/
template<typename SV>
class set2set_11_transform
{
public:
    typedef typename SV::bvector_type       bvector_type;
    typedef typename SV::value_type         value_type;
    typedef typename SV::size_type          size_type;
    typedef typename bvector_type::allocator_type::allocator_pool_type allocator_pool_type;
public:

    set2set_11_transform();
    ~set2set_11_transform();


    /**  Get read access to zero-elements vector
         Zero vector gets populated after attach_sv() is called
         or as a side-effect of remap() with immediate sv param
    */
    const bvector_type& get_bv_zero() const { return bv_zero_; }

    /** Perform remapping (Image function) (based on attached translation table)

    \param bv_in  - input set, defined as a bit-vector
    \param bv_out - output set as a bit-vector

    @sa attach_sv
    */
    void remap(const bvector_type&        bv_in,
               bvector_type&              bv_out);

    /** Perform remapping (Image function)
   
     \param bv_in  - input set, defined as a bit-vector
     \param sv_brel   - binary relation (translation table) sparse vector
     \param bv_out - output set as a bit-vector
    */
    void remap(const bvector_type&        bv_in,
               const    SV&               sv_brel,
               bvector_type&              bv_out);
    
    /** Remap single set element
   
     \param id_from  - input value
     \param sv_brel  - translation table sparse vector
     \param id_to    - out value
     
     \return - true if value was found and remapped
    */
    bool remap(size_type id_from, const SV& sv_brel, size_type& id_to);


    /** Run remap transformation
   
     \param bv_in  - input set, defined as a bit-vector
     \param sv_brel   - translation table sparse vector
     \param bv_out - output set as a bit-vector
     
     @sa remap
    */
    void run(const bvector_type&        bv_in,
             const    SV&               sv_brel,
             bvector_type&              bv_out)
    {
        remap(bv_in, sv_brel, bv_out);
    }
    
    /** Attach a translation table vector for remapping (Image function)

        \param sv_brel   - binary relation sparse vector pointer
                          (pass NULL to detach)
        \param compute_stats - flag to perform computation of some statistics
                               later used in remapping. This only make sense
                               for series of remappings on the same translation
                               vector.
        @sa remap
    */
    void attach_sv(const SV* sv_brel, bool compute_stats = false);

    
protected:
    void one_pass_run(const bvector_type&        bv_in,
                      const    SV&               sv_brel,
                      bvector_type&              bv_out);
    
    /// @internal
    template<unsigned BSIZE>
    struct gather_buffer
    {
        size_type   BM_VECT_ALIGN gather_idx_[BSIZE] BM_VECT_ALIGN_ATTR;
        value_type  BM_VECT_ALIGN buffer_[BSIZE] BM_VECT_ALIGN_ATTR;
    };
    
    enum gather_window_size
    {
        sv_g_size = 1024 * 8
    };
    typedef gather_buffer<sv_g_size>  gather_buffer_type;
    

protected:
    set2set_11_transform(const set2set_11_transform&) = delete;
    void operator=(const set2set_11_transform&) = delete;
    
protected:
    const SV*              sv_ptr_;    ///< current translation table vector
    gather_buffer_type*    gb_;        ///< intermediate buffers
    bvector_type           bv_product_;///< temp vector
    
    bool                   have_stats_; ///< flag of statistics presense
    bvector_type           bv_zero_;   ///< bit-vector for zero elements
    
    allocator_pool_type    pool_;
};



//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------

template<typename SV>
set2set_11_transform<SV>::set2set_11_transform()
: sv_ptr_(0), gb_(0), have_stats_(false)
{
    // set2set_11_transform for signed int is not implemented yet
    static_assert(std::is_unsigned<value_type>::value,
                  "BM: unsigned sparse vector is required for transform");
    gb_ = (gather_buffer_type*)::malloc(sizeof(gather_buffer_type));
    if (!gb_)
    {
        SV::throw_bad_alloc();
    }
}

//----------------------------------------------------------------------------

template<typename SV>
set2set_11_transform<SV>::~set2set_11_transform()
{
    if (gb_)
        ::free(gb_);
}


//----------------------------------------------------------------------------

template<typename SV>
void set2set_11_transform<SV>::attach_sv(const SV*  sv_brel, bool compute_stats)
{
    sv_ptr_ = sv_brel;
    if (!sv_ptr_)
    {
        have_stats_ = false;
    }
    else
    {
        if (sv_brel->empty() || !compute_stats)
            return; // nothing to do
        const bvector_type* bv_non_null = sv_brel->get_null_bvector();
        if (bv_non_null)
            return; // already have 0 elements vector
        

        typename bvector_type::mem_pool_guard mp_g_z;
        mp_g_z.assign_if_not_set(pool_, bv_zero_);

        bm::sparse_vector_scanner<SV> scanner;
        scanner.find_zero(*sv_brel, bv_zero_);
        have_stats_ = true;
    }
}

//----------------------------------------------------------------------------

template<typename SV>
bool set2set_11_transform<SV>::remap(size_type  id_from,
                                     const SV&  sv_brel,
                                     size_type& id_to)
{
    if (sv_brel.empty())
        return false; // nothing to do

    const bvector_type* bv_non_null = sv_brel.get_null_bvector();
    if (bv_non_null)
    {
        if (!bv_non_null->test(id_from))
            return false;
    }
    size_type idx = sv_brel.translate_address(id_from);
    id_to = sv_brel.get(idx);
    return true;
}

//----------------------------------------------------------------------------

template<typename SV>
void set2set_11_transform<SV>::remap(const bvector_type&        bv_in,
                                     const    SV&               sv_brel,
                                     bvector_type&              bv_out)
{
    typename bvector_type::mem_pool_guard mp_g_out, mp_g_p, mp_g_z;
    mp_g_out.assign_if_not_set(pool_, bv_out);
    mp_g_p.assign_if_not_set(pool_, bv_product_);
    mp_g_z.assign_if_not_set(pool_, bv_zero_);

    attach_sv(&sv_brel);
    
    remap(bv_in, bv_out);

    attach_sv(0); // detach translation table
}

template<typename SV>
void set2set_11_transform<SV>::remap(const bvector_type&        bv_in,
                                     bvector_type&              bv_out)
{
    BM_ASSERT(sv_ptr_);

    bv_out.clear();

    if (sv_ptr_ == 0 || sv_ptr_->empty())
        return; // nothing to do

    bv_out.init(); // just in case to "fast set" later

    typename bvector_type::mem_pool_guard mp_g_out, mp_g_p, mp_g_z;
    mp_g_out.assign_if_not_set(pool_, bv_out);
    mp_g_p.assign_if_not_set(pool_, bv_product_);
    mp_g_z.assign_if_not_set(pool_, bv_zero_);


    const bvector_type* enum_bv;

    const bvector_type * bv_non_null = sv_ptr_->get_null_bvector();
    if (bv_non_null)
    {
        // TODO: optimize with 2-way ops
        bv_product_ = bv_in;
        bv_product_.bit_and(*bv_non_null);
        enum_bv = &bv_product_;
    }
    else // non-NULL vector is not available
    {
        enum_bv = &bv_in;
        // if we have any elements mapping into "0" on the other end
        // we map it once (chances are there are many duplicates)
        //
        
        if (have_stats_) // pre-attached translation statistics
        {
            bv_product_ = bv_in;
            size_type cnt1 = bv_product_.count();
            bv_product_.bit_sub(bv_zero_);
            size_type cnt2 = bv_product_.count();
            
            BM_ASSERT(cnt2 <= cnt1);
            
            if (cnt1 != cnt2) // mapping included 0 elements
                bv_out.set_bit_no_check(0);
            
            enum_bv = &bv_product_;
        }
    }

    

    size_type buf_cnt, nb_old, nb;
    buf_cnt = nb_old = 0;
    
    typename bvector_type::enumerator en(enum_bv->first());
    for (; en.valid(); ++en)
    {
        typename SV::size_type idx = *en;
        idx = sv_ptr_->translate_address(idx);
        
        nb = unsigned(idx >> bm::set_block_shift);
        if (nb != nb_old) // new blocks
        {
            if (buf_cnt)
            {
                sv_ptr_->gather(&gb_->buffer_[0], &gb_->gather_idx_[0], buf_cnt, BM_SORTED_UNIFORM);
                bv_out.set(&gb_->buffer_[0], buf_cnt, BM_SORTED);
                buf_cnt = 0;
            }
            nb_old = nb;
            gb_->gather_idx_[buf_cnt++] = idx;
        }
        else
        {
            gb_->gather_idx_[buf_cnt++] = idx;
        }
        
        if (buf_cnt == sv_g_size)
        {
            sv_ptr_->gather(&gb_->buffer_[0], &gb_->gather_idx_[0], buf_cnt, BM_SORTED_UNIFORM);
            bv_out.set(&gb_->buffer_[0], buf_cnt, bm::BM_SORTED);
            buf_cnt = 0;
        }
    } // for en
    if (buf_cnt)
    {
        sv_ptr_->gather(&gb_->buffer_[0], &gb_->gather_idx_[0], buf_cnt, BM_SORTED_UNIFORM);
        bv_out.set(&gb_->buffer_[0], buf_cnt, bm::BM_SORTED);
    }
}


//----------------------------------------------------------------------------

template<typename SV>
void set2set_11_transform<SV>::one_pass_run(const bvector_type&        bv_in,
                                            const    SV&               sv_brel,
                                            bvector_type&              bv_out)
{
    if (sv_brel.empty())
        return; // nothing to do

    bv_out.init();

    typename SV::const_iterator it = sv_brel.begin();
    for (; it.valid(); ++it)
    {
        typename SV::value_type t_id = *it;
        size_type idx = it.pos();
        if (bv_in.test(idx))
        {
            bv_out.set_bit_no_check(t_id);
        }
    } // for
}


//----------------------------------------------------------------------------
//
//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
sparse_vector_scanner<SV, S_FACTOR>::sparse_vector_scanner()
{
    mask_set_ = false;
    mask_from_ = mask_to_ = bm::id_max;

    bound_sv_ = 0;
    effective_str_max_ = 0;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::bind(const SV&  sv, bool sorted)
{
    static_assert(S_FACTOR == 4 || S_FACTOR == 8 || S_FACTOR == 16
                  || S_FACTOR == 32 || S_FACTOR == 64,
    "BM: sparse_vector_scanner<> incorrect sampling factor template parameter");

    (void)sorted; // MSVC warning over if constexpr variable "not-referenced"
    bound_sv_ = &sv;

    if constexpr (SV::is_str()) // bindings for the string sparse vector
    {
        effective_str_max_ = sv.effective_vector_max();
        resize_buffers();

        if (sorted)
        {
            range_idx_.construct(sv, S_FACTOR);
        }
        // pre-calculate vector plane masks
        //
        vector_plane_masks_.resize(effective_str_max_);
        for (unsigned i = 0; i < effective_str_max_; ++i)
        {
            vector_plane_masks_[i] = sv.get_slice_mask(i);
        } // for i
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::reset_binding() BMNOEXCEPT
{
    bound_sv_ = 0;
    effective_str_max_ = 0;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_zero(const SV&     sv,
                                          bvector_type& bv_out,
                                          bool null_correct)
{
    if (sv.size() == 0)
    {
        bv_out.clear();
        return;
    }
    find_nonzero(sv, bv_out);
    if constexpr (SV::is_compressed())
    {
        bv_out.invert();
        bv_out.set_range(sv.effective_size(), bm::id_max - 1, false);
        decompress(sv, bv_out);
    }
    else
    {
        invert(sv, bv_out);
    }
    if (null_correct)
        correct_nulls(sv, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::invert(const SV& sv, bvector_type& bv_out)
{
    if (sv.size() == 0)
    {
        bv_out.clear();
        return;
    }
    // TODO: find a better algorithm (NAND?)
    auto old_sz = bv_out.size();
    bv_out.resize(sv.size());
    bv_out.invert();
    bv_out.resize(old_sz);
    correct_nulls(sv, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::correct_nulls(const SV&   sv,
                                              bvector_type& bv_out)
{
    const bvector_type* bv_null = sv.get_null_bvector();
    if (bv_null) // correct result to only use not NULL elements
        bv_out.bit_and(*bv_null);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_with_nulls(const SV&    sv,
                                    value_type     value,
                                    bvector_type&  bv_out,
                                    size_type      search_limit)
{
    if (sv.empty())
        return false; // nothing to do

    if (!value)
    {
        find_zero(sv, bv_out);
        return bv_out.any();
    }
    agg_.reset();
    
    bool found = prepare_and_sub_aggregator(sv, value);
    if (!found)
    {
        bv_out.clear();
        return found;
    }

    bool any = (search_limit == 1);
    found = agg_.combine_and_sub(bv_out, any);
    agg_.reset();
    return found;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_first_eq(const SV&   sv,
                                            value_type    value,
                                            size_type&    idx)
{
    if (sv.empty())
        return false; // nothing to do
    
    BM_ASSERT(value); // cannot handle zero values
    if (!value)
        return false;

    agg_.reset();
    bool found = prepare_and_sub_aggregator(sv, value);
    if (!found)
        return found;
    found = agg_.find_first_and_sub(idx);
    agg_.reset();
    return found;
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_first_eq(
                                const SV&                       sv,
                                const value_type*               str,
                                size_t                          in_len,
                                size_type&                      idx,
                                bool                            remaped,
                                unsigned                        prefix_len)
{
    BM_ASSERT(*str && in_len);
    BM_ASSERT(in_len == ::strlen(str));

    if (!*str)
        return false;
    agg_.reset();
    unsigned common_prefix_len = 0;

    value_type* pref = remap_prefix_vect_.data();
    if (mask_set_) // it is assumed that the sv is SORTED so common prefix check
    {
        // if in range is exactly one block
        if (/*bool one_nb = */agg_.set_range_hint(mask_from_, mask_to_))
        {
            if (prefix_len == ~0u) // not valid (uncalculated) prefix len
            {
                common_prefix_len =
                    sv.template common_prefix_length<true>(mask_from_, mask_to_, pref);
                if (common_prefix_len)
                {
                    if (remaped)
                        str = remap_value_vect_.data();
                    // next comparison is in the remapped form
                    for (unsigned i = 0; i < common_prefix_len; ++i)
                        if (str[i] != pref[i])
                            return false;
                }
            }
            else
            {
                unsigned pl; (void)pl;
                BM_ASSERT(prefix_len <=
                                (pl=sv.template common_prefix_length<true>(
                                                mask_from_, mask_to_, pref)));
                common_prefix_len = prefix_len;
            }
        } // if one block hit
        else
        {
            if (prefix_len != ~0u) // not valid (uncalculated) prefix len
            {
                unsigned pl; (void)pl;
                BM_ASSERT(prefix_len <=
                                (pl=sv.template common_prefix_length<true>(
                                                mask_from_, mask_to_, pref)));
                common_prefix_len = prefix_len;
            }
        }
    }

    // prefix len checks
    if (common_prefix_len && (in_len <= common_prefix_len))
    {
        if (in_len == common_prefix_len)
            --common_prefix_len;
        else // (in_len < common_prefix_len)
            return false;
    }

    if (remaped)
        str = remap_value_vect_.data();
    else
    {
        if (sv.is_remap() && (str != remap_value_vect_.data()))
        {
            bool r = sv.remap_tosv(
                remap_value_vect_.data(), sv.effective_max_str(), str);
            if (!r)
                return r;
            str = remap_value_vect_.data();
        }
    }
    
    bool found = prepare_and_sub_aggregator(sv, str, common_prefix_len, true);
    if (found)
        found = agg_.find_first_and_sub(idx);

    agg_.reset();
    return found;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::prepare_and_sub_aggregator(
                                      const SV&  sv,
                                      const value_type*  str,
                                      unsigned           octet_start,
                                      bool               prefix_sub)
{
    int len = 0;
    for (; str[len] != 0; ++len)
    {}
    BM_ASSERT(len);

    // use reverse order (faster for sorted arrays)
    // octet_start is the common prefix length (end index)
    for (int octet_idx = len-1; octet_idx >= int(octet_start); --octet_idx)
    {
        unsigned value = unsigned(str[octet_idx]) & 0xFFu;
        BM_ASSERT(value != 0);
        bm::id64_t planes_mask;
        if (&sv == bound_sv_)
            planes_mask = vector_plane_masks_[unsigned(octet_idx)];
        else
            planes_mask = sv.get_slice_mask(unsigned(octet_idx));

        if ((value & planes_mask) != value) // pre-screen for impossible values
            return false; // found non-existing plane

        add_agg_char(agg_, sv, octet_idx, planes_mask, value); // AND-SUB aggregator
     } // for octet_idx
    
    // add all vectors above string len to the SUB aggregation group
    //
    if (prefix_sub)
    {
        typename SV::size_type planes = sv.get_bmatrix().rows_not_null();
        for (unsigned plane_idx = unsigned(len * 8);
                            plane_idx < planes; ++plane_idx)
        {
            if (bvector_type_const_ptr bv = sv.get_slice(plane_idx))
                agg_.add(bv, 1); // agg to SUB group
        } // for
    }
    return true;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::prepare_and_sub_aggregator(
                                                        const SV&   sv,
                                                        value_type   value)
{
    using unsigned_value_type = typename SV::unsigned_value_type;

    agg_.reset();

    unsigned char bits[sizeof(value) * 8];
    unsigned_value_type uv = sv.s2u(value);

    unsigned short bit_count_v = bm::bitscan(uv, bits);
    BM_ASSERT(bit_count_v);
    const unsigned_value_type mask1 = 1;

    // prep the lists for combined AND-SUB aggregator
    //   (backward order has better chance for bit reduction on AND)
    //
    for (unsigned i = bit_count_v; i > 0; --i)
    {
        unsigned bit_idx = bits[i-1];
        BM_ASSERT(uv & (mask1 << bit_idx));
        if (const bvector_type* bv = sv.get_slice(bit_idx))
            agg_.add(bv);
        else
            return false;
    }
    unsigned sv_planes = sv.effective_slices();
    for (unsigned i = 0; i < sv_planes; ++i)
    {
        unsigned_value_type mask = mask1 << i;
        if (bvector_type_const_ptr bv = sv.get_slice(i); bv && !(uv & mask))
            agg_.add(bv, 1); // agg to SUB group
    } // for i
    return true;
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_eq_with_nulls_horizontal(
                                  const SV&     sv,
                                  value_type    value,
                                  bvector_type& bv_out)
{
    bv_out.clear();
    if (sv.empty())
        return; // nothing to do

    if (!value)
    {
        find_zero(sv, bv_out);
        return;
    }

    unsigned char bits[sizeof(value) * 8];
    typename SV::unsigned_value_type uv = sv.s2u(value);
    unsigned short bit_count_v = bm::bitscan(uv, bits);
    BM_ASSERT(bit_count_v);

    // aggregate AND all matching vectors
    if (const bvector_type* bv_plane = sv.get_slice(bits[--bit_count_v]))
        bv_out = *bv_plane;
    else // plane not found
    {
        bv_out.clear();
        return;
    }
    for (unsigned i = 0; i < bit_count_v; ++i)
    {
        const bvector_type* bv_plane = sv.get_slice(bits[i]);
        if (bv_plane)
            bv_out &= *bv_plane;
        else // mandatory plane not found - empty result!
        {
            bv_out.clear();
            return;
        }
    } // for i

    // SUB all other planes
    unsigned sv_planes = sv.effective_slices();
    for (unsigned i = 0; (i < sv_planes) && uv; ++i)
    {
        if (const bvector_type* bv = sv.get_slice(i); bv && !(uv & (1u << i)))
            bv_out -= *bv;
    } // for i
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_gt(
                                        const SV&      sv,
                                        value_type     val,
                                        bvector_type&  bv_out)
{
    // this is not very optimal but should be good enough
    find_gt_horizontal(sv, val, bv_out, true /*NULL correction */);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_ge(
                                        const SV&      sv,
                                        value_type     val,
                                        bvector_type&  bv_out)
{
    if constexpr (std::is_signed<value_type>::value)
    {
        if (val == std::numeric_limits<int>::min())
        {
            bvector_type bv_min;
            find_eq(sv, val, bv_min);
            find_gt_horizontal(sv, val, bv_out, true /*NULL correction */);
            bv_out.merge(bv_min);
        }
        else
        {
            --val;
            find_gt_horizontal(sv, val, bv_out, true /*NULL correction */);
        }
    }
    else // unsigned
    {
        if (val)
        {
            --val;
            find_gt_horizontal(sv, val, bv_out, true /*NULL correction */);
        }
        else // val == 0
        {
            // result set is ALL elements minus possible NULL values
            bv_out.set_range(0, sv.size()-1);
            correct_nulls(sv, bv_out);
        }
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_lt(
                                        const SV&      sv,
                                        value_type     val,
                                        bvector_type&  bv_out)
{
    find_ge(sv, val, bv_out);
    invert(sv, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_le(const SV& sv,
                                                  value_type val,
                                                  bvector_type&  bv_out)
{
    find_gt(sv, val, bv_out);
    invert(sv, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_range(const SV&  sv,
                                           value_type from, value_type to,
                                           bvector_type&  bv_out)
{
    if (to < from)
        bm::xor_swap(from, to);

    find_ge(sv, from, bv_out);

    bvector_type bv_gt;
    find_gt(sv, to, bv_gt);
    bv_out.bit_sub(bv_gt);
}

//----------------------------------------------------------------------------


template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_gt_horizontal(const SV&   sv,
                                                   value_type     value,
                                                   bvector_type&  bv_out,
                                                   bool null_correct)
{
    unsigned char blist[64];
    unsigned bit_count_v;

    if (sv.empty())
    {
        bv_out.clear();
        return; // nothing to do
    }

    if (!value)
    {
        bvector_type bv_zero;
        find_zero(sv, bv_zero);
        auto sz = sv.size();
        bv_out.set_range(0, sz-1);
        bv_out.bit_sub(bv_zero);  // all but zero

        if constexpr (std::is_signed<value_type>::value)
        {
            if (const bvector_type* bv_sign = sv.get_slice(0)) // sign bvector
                bv_out.bit_sub(*bv_sign);  // all but negatives
        }
        if (null_correct)
            correct_nulls(sv, bv_out);
        return;
    }
    bv_out.clear();

    bvector_type gtz_bv; // all >= 0

    if constexpr (std::is_signed<value_type>::value)
    {
        find_positive(sv, gtz_bv); // all positives are greater than all negs
        if (value == -1)
        {
            bv_out.swap(gtz_bv);
            if (null_correct)
                correct_nulls(sv, bv_out);
            return;
        }
        if (value == -2)
        {
            find_eq(sv, -1, bv_out);
            bv_out.bit_or(gtz_bv);
            if (null_correct)
                correct_nulls(sv, bv_out);
            return;
        }

        auto uvalue = SV::s2u(value + bool(value < 0)); // turns it int LE expression
        uvalue &= ~1u; // drop the negative bit
        BM_ASSERT(uvalue);
        bit_count_v = bm::bitscan(uvalue, blist);
    }
    else
        bit_count_v = bm::bitscan(value, blist);

    BM_ASSERT(bit_count_v);


    // aggregate all top bit-slices (surely greater)
    // TODO: use aggregator function
    bvector_type top_or_bv;

    unsigned total_planes = sv.effective_slices();
    const bvector_type* bv_sign = sv.get_slice(0); (void)bv_sign;

    agg_.reset();
    if constexpr (std::is_signed<value_type>::value)
    {
        if (value < 0)
        {
            if (!bv_sign) // no negatives at all
            {
                bv_out.swap(gtz_bv); // return all >= 0
                if (null_correct)
                    correct_nulls(sv, bv_out);
                return;
            }
            aggregate_AND_OR_slices(top_or_bv, *bv_sign, sv, blist[bit_count_v-1]+1, total_planes);
        }
        else // value > 0
        {
            aggregate_OR_slices(top_or_bv, sv, blist[bit_count_v-1]+1, total_planes);
        }
    }
    else
    {
        if (total_planes < unsigned(blist[bit_count_v-1]+1))
            return; // number is greater than anything in this vector (empty set)
        aggregate_OR_slices(top_or_bv, sv, blist[bit_count_v-1]+1, total_planes);
    }
    agg_.reset();

    bv_out.merge(top_or_bv);

    // TODO: optimize FULL blocks

    bvector_type and_eq_bv; // AND accum
    bool first = true; // flag for initial assignment

    // GT search
    for (int i = int(bit_count_v)-1; i >= 0; --i)
    {
        int slice_idx = blist[i];
        if (slice_idx == gt_slice_limit()) // last plane
            break;
        const bvector_type* bv_base_plane = sv.get_slice(unsigned(slice_idx));
        if (!bv_base_plane)
            break;
        if (first)
        {
            first = false;
            if constexpr (std::is_signed<value_type>::value)
            {
                if (value < 0)
                    and_eq_bv.bit_and(*bv_base_plane, *bv_sign); // use negatives for AND mask
                else // value > 0
                    and_eq_bv.bit_and(*bv_base_plane, gtz_bv);
            }
            else // unsigned
                and_eq_bv = *bv_base_plane; // initial assignment
        }
        else
            and_eq_bv.bit_and(*bv_base_plane); // AND base to accumulator

        int next_slice_idx = 0;
        if (i)
        {
            next_slice_idx = blist[i-1];
            if (slice_idx-1 == next_slice_idx)
                continue;
            ++next_slice_idx;
        }

        // AND-OR the mid-planes
        //
        for (int j = slice_idx-1; j >= next_slice_idx; --j)
        {
            if constexpr (std::is_signed<value_type>::value)
                if (j == 0) // sign plane
                    break; // do not process the sign plane at all
            if (const bvector_type* bv_sub_plane = sv.get_slice(unsigned(j)))
                bv_out.bit_or_and(and_eq_bv, *bv_sub_plane);
        } // for j
    } // for i

    if constexpr (std::is_signed<value_type>::value)
    {
        if (value < 0)
        {
            // now we have negatives greter by abs value
            top_or_bv.set_range(0, sv.size()-1);
            top_or_bv.bit_sub(bv_out);
            bv_out.swap(top_or_bv);
        }
        else // value > 0
        {
            gtz_bv.resize(sv.size());
            gtz_bv.invert(); // now it is all < 0
            bv_out.bit_sub(gtz_bv); // exclude all negatives
        }
    }

    decompress(sv, bv_out);

    if (null_correct)
    {
        if constexpr (!std::is_signed<value_type>::value) // unsigned
            return; // NULL correction for positive values is not needed
        else // signed
        {
            if (value < 0)
                correct_nulls(sv, bv_out);
        }
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::aggregate_OR_slices(
                bvector_type& bv_target,
                const SV& sv,
                unsigned from, unsigned total_planes)
{
    for (unsigned i = from; i < total_planes; ++i)
    {
        if (const bvector_type* bv_slice = sv.get_slice(i))
        {
            BM_ASSERT(bv_slice != sv.get_null_bvector());
            agg_.add(bv_slice);
        }
    }
    agg_.combine_or(bv_target);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::aggregate_AND_OR_slices(bvector_type& bv_target,
            const bvector_type& bv_mask,
            const SV& sv,
            unsigned from, unsigned total_planes)
{
    for (unsigned i = from; i < total_planes; ++i)
    {
        if (const bvector_type* bv_slice = sv.get_slice(i))
        {
            BM_ASSERT(bv_slice != sv.get_null_bvector());
            bv_target.bit_or_and(bv_mask, *bv_slice);
        }
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str_prefix(const SV&   sv,
                                const typename SV::value_type* str,
                                typename SV::bvector_type&     bv_out)
{
    return find_eq_str_impl(sv, str, bv_out, false);
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str(
                                        const typename SV::value_type* str,
                                        typename SV::size_type&        pos)
{
    BM_ASSERT(bound_sv_);
    return this->find_eq_str(*bound_sv_, str, pos);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str(
                                const SV&                      sv,
                                const typename SV::value_type* str,
                                typename SV::size_type&        pos)
{
    bool found = false;
    if (sv.empty())
        return found;
    if (*str)
    {
        bool remaped = false;
        if constexpr (SV::is_remap_support::value) // test remapping trait
        {
            if (sv.is_remap() && str != remap_value_vect_.data())
            {
                auto str_len = sv.effective_vector_max();
                remap_value_vect_.resize(str_len);
                remaped = sv.remap_tosv(
                    remap_value_vect_.data(), str_len, str);
                if (!remaped)
                    return remaped;
                str = remap_value_vect_.data();
            }
        }

        size_t in_len = ::strlen(str);
        size_type found_pos;
        found = find_first_eq(sv, str, in_len, found_pos, remaped, ~0u);
        if (found)
        {
            pos = found_pos;
            if constexpr (SV::is_rsc_support::value) // test rank/select trait
            {
                if constexpr (sv.is_compressed()) // if compressed vector - need rank translation
                    found = sv.find_rank(found_pos + 1, pos);
            }
        }
    }
    else // search for zero(empty str) value
    {
        // TODO: implement optimized version which would work witout temp vector
        typename SV::bvector_type bv_zero;
        find_zero(sv, bv_zero);
        found = bv_zero.find(pos);
    }
    return found;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str(
                                        const typename SV::value_type* str,
                                        typename SV::bvector_type& bv_out)
{
    BM_ASSERT(bound_sv_);
    return find_eq_str(*bound_sv_, str, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str(
                                            const SV&                      sv,
                                            const typename SV::value_type* str,
                                            typename SV::bvector_type& bv_out)
{
    return find_eq_str_impl(sv, str, bv_out, true);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::remap_tosv(
                                remap_vector_type& remap_vect_target,
                                const typename SV::value_type* str,
                                const SV& sv)
{
    auto str_len = sv.effective_vector_max();
    remap_vect_target.resize(str_len);
    bool remaped = sv.remap_tosv(remap_vect_target.data(), str_len, str);
    return remaped;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq_str_impl(
                             const SV&  sv,
                             const typename SV::value_type* str,
                             typename SV::bvector_type& bv_out,
                             bool prefix_sub)
{
    bool found = false;
    bv_out.clear(true);
    if (sv.empty())
        return false; // nothing to do
    if constexpr (SV::is_rsc_support::value) // test rank/select trait
    {
        // search in RS compressed string vectors is not implemented
        BM_ASSERT(0);
    }

    if (*str)
    {
        if constexpr (SV::is_remap_support::value) // test remapping trait
        {
            if (sv.is_remap() && (str != remap_value_vect_.data()))
            {
                bool remaped = remap_tosv(remap_value_vect_, str, sv);
                if (!remaped)
                    return false; // not found because
                str = remap_value_vect_.data();
            }
        }
        /// aggregator search
        agg_.reset();

        const unsigned common_prefix_len = 0;
        found = prepare_and_sub_aggregator(sv, str, common_prefix_len,
                                           prefix_sub);
        if (!found)
            return found;
        found = agg_.combine_and_sub(bv_out);

        agg_.reset();
    }
    else // search for zero(empty str) value
    {
        find_zero(sv, bv_out);
        found = bv_out.any();
    }
    return found;

}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR> template<class TPipe>
void sparse_vector_scanner<SV, S_FACTOR>::find_eq_str(TPipe& pipe)
{
    if (pipe.bv_and_mask_)
    {
        size_type first, last;
        bool any = pipe.bv_and_mask_->find_range(first, last);
        if (!any)
            return;
        agg_.set_range_hint(first, last);
    }
    agg_.combine_and_sub(pipe.agg_pipe_);
    agg_.reset_range_hint();
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
template<bool BOUND>
bool sparse_vector_scanner<SV, S_FACTOR>::bfind_eq_str_impl(
                                    const SV&                      sv,
                                    const typename SV::value_type* str,
                                    size_t                         in_len,
                                    bool                           remaped,
                                    typename SV::size_type&        pos)
{
    bool found = false;
    if (sv.empty())
        return found;

    unsigned prefix_len = ~0u;

    if (in_len)
    {
        reset_search_range();
        
        size_type l, r;
        size_type found_pos;

        if constexpr (BOUND)
        {
            found = range_idx_.bfind_range(str, in_len, l, r);
            if (!found)
                return found;

            prefix_len =
                (unsigned) range_idx_.common_prefix_length(str, in_len, l, r);

            if ((l == r) && (in_len == prefix_len))
            {
                range_idx_.recalc_range(str, l, r);
                pos = l;
                return found;
            }

            range_idx_.recalc_range(str, l, r);
            set_search_range(l, r); // r := r-1 (may happen here) [l..r] interval

            BM_ASSERT(this->compare_str<false>(sv, l, str) <= 0);
            // bad assert, because of the r = r-1 correction in recalc_range()
            //BM_ASSERT(this->compare_str<false>(sv, r, str) >= 0);

        }
        else
        {
            // narrow down the search
            const unsigned min_distance_cutoff = bm::gap_max_bits + bm::gap_max_bits / 2;
            size_type dist;
            l = 0; r = sv.size()-1;

            // binary search to narrow down the search window
            while (l <= r)
            {
                dist = r - l;
                if (dist < min_distance_cutoff)
                {
                    // we are in an narrow <2 blocks window, but still may be in two
                    // different neighboring blocks, lets try to narrow
                    // to exactly one block

                    size_type nb_l = (l >> bm::set_block_shift);
                    size_type nb_r = (r >> bm::set_block_shift);
                    if (nb_l != nb_r)
                    {
                        size_type mid = nb_r * bm::gap_max_bits;
                        if (mid < r)
                        {
                            int cmp = this->compare_str<BOUND>(sv, mid, str);
                            if (cmp < 0) // mid < str
                                l = mid;
                            else
                                r = mid-(cmp!=0); // branchless if (cmp==0) r=mid;
                            BM_ASSERT(l < r);
                        }
                        nb_l = unsigned(l >> bm::set_block_shift);
                        nb_r = unsigned(r >> bm::set_block_shift);
                    }

                    if (nb_l == nb_r)
                    {
                        size_type max_nb = sv.size() >> bm::set_block_shift;
                        if (nb_l != max_nb)
                        {
                            // linear scan to identify the sub-range
                            size_type mid = nb_r * bm::gap_max_bits + sub_block_l1_size;
                            for (unsigned i = 0; i < (sub_bfind_block_cnt-1);
                                                 ++i, mid += sub_block_l1_size)
                            {
                                int cmp = this->compare_str<BOUND>(sv, mid, str);
                                if (cmp < 0)
                                    l = mid;
                                else
                                {
                                    r = mid;
                                    break;
                                }
                            } // for i
                        }
                        set_search_range(l, r);
                        break;
                    }
                }

                typename SV::size_type mid = dist/2+l;
                size_type nb = (mid >> bm::set_block_shift);
                mid = nb * bm::gap_max_bits;
                int cmp;
                if (mid <= l)
                {
                    if (nb == 0 && r > bm::gap_max_bits)
                        mid = bm::gap_max_bits;
                    else
                    {
                        mid = dist / 2 + l;
                        cmp = this->compare_str<false>(sv, mid, str);
                        goto l1;
                    }
                }
                BM_ASSERT(mid > l);
                cmp = this->compare_str<BOUND>(sv, mid, str);
                l1:
                if (cmp == 0)
                {
                    found_pos = mid;
                    //found = true;
                    set_search_range(l, mid);
                    break;
                }
                if (cmp < 0)
                    l = mid+1;
                else
                    r = mid-1;
            } // while
        }

        // use linear search (range is set)
        found = find_first_eq(sv, str, in_len, found_pos, remaped, prefix_len);
        if (found)
        {
            pos = found_pos;
            if constexpr (SV::is_compressed()) // if compressed vector - need rank translation
                found = sv.find_rank(found_pos + 1, pos);
        }
        reset_search_range();
    }
    else // search for zero value
    {
        // TODO: implement optimized version which would work without temp vector
        typename SV::bvector_type bv_zero;
        find_zero(sv, bv_zero);
        found = bv_zero.find(pos);
    }
    return found;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::bfind_eq_str(const SV& sv,
                                    const value_type* str, size_type& pos)
{
    size_t len = ::strlen(str);
    effective_str_max_ = sv.effective_max_str();
    if (len > effective_str_max_)
        return false; // impossible value

    resize_buffers();

    bool remaped = false;
    if constexpr (SV::is_remap_support::value)
    {
        if (sv.is_remap())
        {
            remap_value_vect_.resize_no_copy(len);
            remaped = sv.remap_tosv(remap_value_vect_.data(),
                                        effective_str_max_, str);
            if (!remaped)
                return remaped;
        }
    }
    return bfind_eq_str_impl<false>(sv, str, len, remaped, pos);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::bfind_eq_str(
                                            const typename SV::value_type* str,
                                            typename SV::size_type&        pos)
{
    BM_ASSERT(bound_sv_); // this function needs prior bind()
    size_t len = ::strlen(str);
    if (len > effective_str_max_)
        return false; // impossible value
    bool remaped = false;
    if constexpr (SV::is_remap_support::value)
    {
        if (bound_sv_->is_remap())
        {
            remaped = bound_sv_->remap_tosv(remap_value_vect_.data(),
                                                 effective_str_max_, str);
            if (!remaped)
                return remaped;
        }
    }
    return bfind_eq_str_impl<true>(*bound_sv_, str, len, remaped, pos);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::bfind_eq_str(
                        const value_type* str, size_t in_len, size_type& pos)
{
    BM_ASSERT(str);
    BM_ASSERT(bound_sv_);

    if (in_len > effective_str_max_)
        return false; // impossible value

    value_type* s = value_vect_.data(); // copy to temp buffer, put zero end

    bool remaped = false;
    // test search pre-condition based on remap tables
    if constexpr (SV::is_remap_support::value)
    {
        if (bound_sv_->is_remap())
        {
            remaped = bound_sv_->remap_n_tosv_2way(
                                            remap_value_vect_.data(),
                                            s,
                                            effective_str_max_,
                                            str,
                                            in_len);
            if (!remaped)
                return remaped;
        }
    }
    if (!remaped) // copy string, make sure it is zero terminated
    {
        for (size_t i = 0; i < in_len && *str; ++i)
            s[i] = str[i];
        s[in_len] = value_type(0);
    }
    return bfind_eq_str_impl<true>(*bound_sv_, s, in_len, remaped, pos);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::bfind(
                                      const SV&                      sv,
                                      const typename SV::value_type  val,
                                      typename SV::size_type&        pos)
{
    int cmp;
    size_type l = 0, r = sv.size();
    if (l == r) // empty vector
    {
        pos = 0;
        return false;
    }
    --r;
    
    // check initial boundary conditions if insert point is at tail/head
    cmp = this->compare(sv, l, val); // left (0) boundary check
    if (cmp > 0) // vect[x] > str
    {
        pos = 0;
        return false;
    }
    if (cmp == 0)
    {
        pos = 0;
        return true;
    }
    cmp = this->compare(sv, r, val); // right(size-1) boundary check
    if (cmp == 0)
    {
        pos = r;
        // back-scan to rewind all duplicates
        // TODO: adapt one-sided binary search to traverse large platos
        for (; r >= 0; --r)
        {
            cmp = this->compare(sv, r, val);
            if (cmp != 0)
                return true;
            pos = r;
        } // for i
        return true;
    }
    if (cmp < 0) // vect[x] < str
    {
        pos = r+1;
        return false;
    }
    
    size_type dist = r - l;
    if (dist < linear_cutoff1)
    {
        for (; l <= r; ++l)
        {
            cmp = this->compare(sv, l, val);
            if (cmp == 0)
            {
                pos = l;
                return true;
            }
            if (cmp > 0)
            {
                pos = l;
                return false;
            }
        } // for
    }
    
    while (l <= r)
    {
        size_type mid = (r-l)/2+l;
        cmp = this->compare(sv, mid, val);
        if (cmp == 0)
        {
            pos = mid;
            // back-scan to rewind all duplicates
            for (size_type i = mid-1; i >= 0; --i)
            {
                cmp = this->compare(sv, i, val);
                if (cmp != 0)
                    return true;
                pos = i;
            } // for i
            pos = 0;
            return true;
        }
        if (cmp < 0)
            l = mid+1;
        else
            r = mid-1;

        dist = r - l;
        if (dist < linear_cutoff2) // do linear scan here
        {
            typename SV::const_iterator it(&sv, l);
            for (; it.valid(); ++it, ++l)
            {
                typename SV::value_type sv_value = it.value();
                if (sv_value == val)
                {
                    pos = l;
                    return true;
                }
                if (sv_value > val) // vect[x] > val
                {
                    pos = l;
                    return false;
                }
            } // for it
            BM_ASSERT(0);
            pos = l;
            return false;
        }
    } // while
    
    BM_ASSERT(0);
    return false;
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::lower_bound_str(
                                        const SV&  sv,
                                        const typename SV::value_type* str,
                                        typename SV::size_type&        pos)
{
    int cmp;
    size_type l = 0, r = sv.size();
    
    if (l == r) // empty vector
    {
        pos = 0;
        return false;
    }
    --r;
    
    // check initial boundary conditions if insert point is at tail/head
    cmp = this->compare_str<false>(sv, l, str); // left (0) boundary check
    if (cmp > 0) // vect[x] > str
    {
        pos = 0;
        return false;
    }
    if (cmp == 0)
    {
        pos = 0;
        return true;
    }
    cmp = this->compare_str<false>(sv, r, str); // right(size-1) boundary check
    if (cmp == 0)
    {
        pos = r;
        // back-scan to rewind all duplicates
        // TODO: adapt one-sided binary search to traverse large platos
        for (; r >= 0; --r)
        {
            cmp = this->compare_str<false>(sv, r, str);
            if (cmp != 0)
                return true;
            pos = r;
        } // for i
        return true;
    }
    if (cmp < 0) // vect[x] < str
    {
        pos = r+1;
        return false;
    }
    
    size_type dist = r - l;
    if (dist < linear_cutoff1)
    {
        for (; l <= r; ++l)
        {
            cmp = this->compare_str<false>(sv, l, str);
            if (cmp == 0)
            {
                pos = l;
                return true;
            }
            if (cmp > 0)
            {
                pos = l;
                return false;
            }
        } // for
    }
    while (l <= r)
    {
        size_type mid = (r-l)/2+l;
        cmp = this->compare_str<false>(sv, mid, str);
        if (cmp == 0)
        {
            pos = mid;
            // back-scan to rewind all duplicates
            for (size_type i = mid-1; i >= 0; --i)
            {
                cmp = this->compare_str<false>(sv, i, str);
                if (cmp != 0)
                    return true;
                pos = i;
            } // for i
            pos = 0;
            return true;
        }
        if (cmp < 0)
            l = mid+1;
        else
            r = mid-1;

        dist = r - l;
        if (dist < linear_cutoff2) // do linear scan here
        {
            if (!hmatr_.is_init())
            {
                unsigned max_str = (unsigned)sv.effective_max_str();
                hmatr_.resize(linear_cutoff2, max_str+1, false);
            }

            dist = sv.decode(hmatr_, l, dist+1);
            for (unsigned i = 0; i < dist; ++i, ++l)
            {
                const typename SV::value_type* hm_str = hmatr_.row(i);
                cmp = ::strcmp(hm_str, str);
                if (cmp == 0)
                {
                    pos = l;
                    return true;
                }
                if (cmp > 0) // vect[x] > str
                {
                    pos = l;
                    return false;
                }
            }
            cmp = this->compare_str<false>(sv, l, str);
            if (cmp > 0) // vect[x] > str
            {
                pos = l;
                return false;
            }
            BM_ASSERT(0);
            pos = l;
            return false;
        }
    } // while
    
    BM_ASSERT(0);
    return false;
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
template <bool BOUND>
int sparse_vector_scanner<SV, S_FACTOR>::compare_str(
                                           const SV& sv,
                                           size_type idx,
                                           const value_type* BMRESTRICT str
                                           ) const BMNOEXCEPT
{
#if 0
    if constexpr (BOUND)
    {
        BM_ASSERT(bound_sv_ == &sv);

        size_type nb = (idx >> bm::set_block_shift);
        size_type nbit = (idx & bm::set_block_mask);
        int res = 0;
        const value_type* BMRESTRICT s0;
        /*
        if (!nbit) // access to sentinel, first block element
            s0 = block_l0_cache_.row(nb);
        else
        {
            BM_ASSERT(nbit % sub_block_l1_size == 0);
            size_type k =
              (nb * (sub_bfind_block_cnt-1)) + (nbit / sub_block_l1_size - 1);
            s0 = block_l1_cache_.row(k);
        }
        */
        // strcmp
        /*
        if constexpr (sizeof(void*) == 8) // TODO: improve for WASM
        {
            for (unsigned i = 0; true; i+=sizeof(bm::id64_t))
            {
                bm::id64_t o64, v64;
                ::memcpy(&o64, str+i, sizeof(o64));
                ::memcpy(&v64, s0+i, sizeof(v64));

                if (o64 != v64 || bm::has_zero_byte_u64(o64)
                               || bm::has_zero_byte_u64(v64))
                {
                    do
                    {
                        char octet = str[i]; char value = s0[i];
                        res = (value > octet) - (value < octet);
                        if (res || !octet)
                            return res;
                        ++i;
                    } while(1);
                }
            } // for i
        }
        else */
        {
            for (unsigned i = 0; true; ++i)
            {
                char octet = str[i]; char value = s0[i];
                res = (value > octet) - (value < octet);
                if (res || !octet)
                    break;
            } // for i
        }

        return res;
    }
    else
#endif
    {
        return sv.compare(idx, str);
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
int sparse_vector_scanner<SV, S_FACTOR>::compare(
                                        const SV& sv,
                                        size_type idx,
                                        const value_type val) BMNOEXCEPT
{
    // TODO: implement sentinel elements cache (similar to compare_str())
    return sv.compare(idx, val);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_eq(
                                        const SV&                  sv,
                                        typename SV::value_type    value,
                                        typename SV::bvector_type& bv_out)
{
    if (sv.empty())
    {
        bv_out.clear();
        return; // nothing to do
    }
    if (!value)
    {
        find_zero(sv, bv_out);
        return;
    }

    find_eq_with_nulls(sv, value, bv_out, 0);
    
    decompress(sv, bv_out);
    correct_nulls(sv, bv_out);
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR> template<typename BII>
void sparse_vector_scanner<SV, S_FACTOR>::find_eq(
                            const SV&  sv, value_type  value, BII bi)
{
    static_assert(!SV::is_compressed(), "BM:find_eq on RSC vector not implemented");

    if (sv.empty())
        return; // nothing to do
    if (!value)
    {
        // TODO: better implementation for 0 value seach
        typename SV::bvector_type bv_out;
        find_zero(sv, bv_out);
        typename SV::bvector_type::enumerator en = bv_out.get_enumerator(0);
        for (; en.valid(); ++en)
            *bi = *en;
        return;
    }

    // search for value with aggregator
    //
    agg_.reset();

    bool found = prepare_and_sub_aggregator(sv, value);
    if (!found)
        return; // impossible value

    found = agg_.combine_and_sub_bi(bi);
    agg_.reset();
}


//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
bool sparse_vector_scanner<SV, S_FACTOR>::find_eq(
                                        const SV&                  sv,
                                        typename SV::value_type    value,
                                        typename SV::size_type&    pos)
{
    if (!value) // zero value - special case
    {
        bvector_type bv_zero;
        find_eq(sv, value, bv_zero);
        bool found = bv_zero.find(pos);
        return found;
    }

    size_type found_pos;
    bool found = find_first_eq(sv, value, found_pos);
    if (found)
    {
        pos = found_pos;
        if (bm::conditional<SV::is_rsc_support::value>::test()) // test rank/select trait
        {
            if constexpr (SV::is_compressed()) // if compressed vector - need rank translation
                found = sv.find_rank(found_pos + 1, pos);
        }
    }
    return found;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_nonzero(
                                        const    SV&               sv,
                                        typename SV::bvector_type& bv_out)
{
    agg_.reset(); // in case if previous scan was interrupted
    auto sz = sv.effective_slices(); // sv.slices();
    for (unsigned i = 0; i < sz; ++i)
        agg_.add(sv.get_slice(i));
    agg_.combine_or(bv_out);
    agg_.reset();
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::find_positive(
                                     const SV&                  sv,
                                     typename SV::bvector_type& bv_out)
{
    BM_ASSERT(sv.size());
    bv_out.set_range(0, sv.size()-1); // select all elements
    if constexpr (std::is_signed<value_type>::value)
    {
        if (const bvector_type* bv_sign = sv.get_slice(0)) // sign bvector
            bv_out.bit_sub(*bv_sign);  // all MINUS negatives
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::decompress(
                                           const SV&   sv,
                                           typename SV::bvector_type& bv_out)
{
    if constexpr (SV::is_compressed())
    {
        const bvector_type* bv_non_null = sv.get_null_bvector();
        BM_ASSERT(bv_non_null);

        // TODO: implement faster decompressor for small result-sets
        rank_compr_.decompress(bv_tmp_, *bv_non_null, bv_out);
        bv_out.swap(bv_tmp_);
    }
    else
    {
        (void)sv; (void)bv_out;
    }
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::set_search_range(
                                size_type from, size_type to) BMNOEXCEPT
{
    BM_ASSERT(from <= to);
    mask_from_ = from; mask_to_ = to; mask_set_ = true;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR>
void sparse_vector_scanner<SV, S_FACTOR>::reset_search_range() BMNOEXCEPT
{
    mask_set_ = false;
}


//----------------------------------------------------------------------------
// sparse_vector_scanner<SV, S_FACTOR>::pipeline<Opt>
//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR> template<class Opt>
void
sparse_vector_scanner<SV, S_FACTOR>::pipeline<Opt>::set_search_mask(
                                const bvector_type* bv_mask) BMNOEXCEPT
{
    static_assert(Opt::is_masks(),
                  "BM: Search masking needs to be enabled in template parameter options before function call. see bm::agg_run_options<> ");
    bv_and_mask_ = bv_mask;
}

//----------------------------------------------------------------------------

template<typename SV, unsigned S_FACTOR> template<class Opt>
void
sparse_vector_scanner<SV, S_FACTOR>::pipeline<Opt>::add(
                                    const typename SV::value_type* str)
{
    BM_ASSERT(str);

    typename aggregator_type::arg_groups* arg = agg_pipe_.add();
    BM_ASSERT(arg);

    if constexpr (SV::is_remap_support::value) // test remapping trait
    {
        if (sv_.is_remap() && (str != remap_value_vect_.data()))
        {
            bool remaped = remap_tosv(remap_value_vect_, str, sv_);
            if (!remaped)
                return; // not found (leaves the arg(arg_group) empty
            str = remap_value_vect_.data();
        }
    }
    int len = 0;
    for (; str[len] != 0; ++len)
    {}
    BM_ASSERT(len);

    if constexpr(Opt::is_masks())
        arg->add(bv_and_mask_, 0);
    arg->add(sv_.get_null_bvector(), 0);

    // use reverse order (faster for sorted arrays)

    for (int octet_idx = len-1; octet_idx >= 0; --octet_idx)
    {
        //if (unsigned(octet_idx) < octet_start) // common prefix
        //    break;

        unsigned value = unsigned(str[octet_idx]) & 0xFF;
        BM_ASSERT(value != 0);

        bm::id64_t planes_mask;
        #if (0)
        if (&sv == bound_sv_)
            planes_mask = vector_plane_masks_[unsigned(octet_idx)];
        else
        #endif
        planes_mask = sv_.get_slice_mask(unsigned(octet_idx));

        if ((value & planes_mask) != value) // pre-screen for impossible values
        {
            arg->reset(); // reset is necessary to disable single plane AND
            return;       // found non-existing plane
        }

        sparse_vector_scanner<SV>::add_agg_char(
            *arg, sv_, octet_idx, planes_mask, value); // AND-SUB aggregator
     } // for octet_idx


    // add all vectors above string len to the SUB aggregation group
    //
    //if (prefix_sub)
    {
        unsigned plane_idx = unsigned(len * 8);
        // SUB group should not include not NULL bvector
        size_type planes = size_type(this->eff_slices_);
        for (; plane_idx < planes; ++plane_idx)
        {
            if (bvector_type_const_ptr bv = sv_.get_slice(plane_idx))
                arg->add(bv, 1); // agg to SUB group
        } // for
    }

}

//----------------------------------------------------------------------------
// sv_sample_index<SV>
//----------------------------------------------------------------------------

template<typename SV>
void sv_sample_index<SV>::construct(const SV& sv, unsigned s_factor)
{
    BM_ASSERT(SV::is_str());
    s_factor_ = s_factor;
    sv_size_ = sv.size();
    if (!sv_size_)
        return;

    // resize and init the cache matrix
    //
    auto effective_str_max = sv.effective_vector_max() + 1;
    size_type total_nb = (sv_size_ / bm::gap_max_bits) + 1;
    size_type idx_size = total_nb * s_factor + 1;
    s_cache_.init_resize(idx_size, effective_str_max);
    s_cache_.set_zero();

    // build the index
    const size_type cols = size_type(s_cache_.cols());
    const size_type s_step = bm::gap_max_bits / s_factor;
    idx_size_ = 0;
    for(size_type i = 0; true; )
    {
        value_type* s_str = s_cache_.row(idx_size_);
        ++idx_size_;
        sv.get(i, s_str, cols);
        i += s_step;
        if (i >= sv_size_) // add the last sampled element
        {
            i = sv_size_-1;
            if (i)
            {
                s_str = s_cache_.row(idx_size_);
                ++idx_size_;
                sv.get(i, s_str, cols);
            }
            break;
        }
    } // for i

    size_t min_len = 0;
    {
        const value_type* s = s_cache_.row(0);
        min_len = ::strlen(s);
    }

    // find index duplicates, minimum key size, ...
    //
    idx_unique_ = true;
    const value_type* str_prev = s_cache_.row(0);
    for(size_type i = 1; i < idx_size_; ++i)
    {
        const value_type* str_curr = s_cache_.row(i);
        size_t curr_len = ::strlen(str_curr);
        if (curr_len < min_len)
            min_len = curr_len;

        int cmp = SV::compare_str(str_prev, str_curr);
        BM_ASSERT(cmp <= 0);
        if (cmp == 0) // duplicate
        {
            idx_unique_ = false;
            break;
        }
        str_prev = str_curr;
    } // for i

    min_key_len_ = min_len;

}

//----------------------------------------------------------------------------

template<typename SV>
bool sv_sample_index<SV>::bfind_range(const value_type* search_str,
                                      size_t            in_len,
                                      size_type&        l,
                                      size_type&        r) const BMNOEXCEPT
{
    const size_type linear_cutoff = 4;
    if (!idx_size_)
        return false;
    l = 0; r = idx_size_ - 1;
    int cmp;

    size_t min_len = this->min_key_len_;
    if (in_len < min_len)
        min_len = in_len;

    // check the left-right boundaries
    {
        const value_type* str = s_cache_.row(l);
        cmp = SV::compare_str(search_str, str, min_len);
        if (cmp < 0)
            return false;

        str = s_cache_.row(r);
        cmp = SV::compare_str(search_str, str, min_len);
        if (cmp > 0)
            return false;
    }

    while (l < r)
    {
        size_type dist = r - l;
        if (dist < linear_cutoff) // do linear scan here
        {
            for (size_type i = l+1; i < r; ++i)
            {
                const value_type* str_i = s_cache_.row(i);
                cmp = SV::compare_str(search_str, str_i, min_len);
                if (cmp > 0) // |----i-*--|----|
                {            // |----*----|----|
                    l = i;
                    continue; // continue searching
                }
                /*
                if (cmp == 0)   // |----*----|----|
                {
                    l = r = i;
                    return true;
                }
                */
                // |--*-i----|----|
                BM_ASSERT(i);
                r = i;
                break;
            } // for i
            return true;
        } // if linear scan

        size_type mid = (r-l) / 2 + l;
        const value_type* str_m = s_cache_.row(mid);
        cmp = SV::compare_str(str_m, search_str, min_len);
/*
        if (cmp == 0)
        {
            l = r = mid;
            return true;
        } */
        if (cmp <= 0) // str_m < search_str
            l = mid;
        else         // str_m > search_str
            r = mid;

    } // while

    return true;
}

//----------------------------------------------------------------------------

template<typename SV>
typename sv_sample_index<SV>::size_type
sv_sample_index<SV>::common_prefix_length(const value_type* str_s,
                                          size_t            in_len,
                                          size_type l,
                                          size_type r) const BMNOEXCEPT
{
    const value_type* str_l = s_cache_.row(l);
    const value_type* str_r = s_cache_.row(r);

    size_t min_len = (in_len < min_key_len_) ? in_len : min_key_len_;
    size_type i = 0;
    if (min_len >= 4)
    {
        for (; i < min_len-3; i+=4)
        {
            unsigned i2, i1;
            ::memcpy(&i2, &str_l[i], sizeof(i2));
            ::memcpy(&i1, &str_r[i], sizeof(i1));
            BM_ASSERT(!bm::has_zero_byte_u64(
                                bm::id64_t(i2) | (bm::id64_t(i1) << 32)));
            if (i1 != i2)
                break;
            ::memcpy(&i2, &str_s[i], sizeof(i2));
            BM_ASSERT(!bm::has_zero_byte_u64(
                                bm::id64_t(i2) | (bm::id64_t(i1) << 32)));
            if (i1 != i2)
                break;
        } // for i
    }

    for (; true; ++i)
    {
        auto ch1 = str_l[i]; auto ch2 = str_r[i];
        if (ch1 != ch2 || (!(ch1|ch2))) // chars not the same or both zero
            break;
        auto chs = str_s[i];
        if (ch1 != chs)
            break;
    } // for i
    return i;
}


//----------------------------------------------------------------------------

template<typename SV>
void sv_sample_index<SV>::recalc_range(const value_type* search_str,
                                       size_type&      l,
                                       size_type&      r) const BMNOEXCEPT
{
    BM_ASSERT(l <= r);
    BM_ASSERT(r < idx_size_);

    // -1 correction here below is done to get it to the closed interval
    // [from..to] when possible, because it reduces search space
    // by one scan wave

    const size_type s_step = bm::gap_max_bits / s_factor_;
    if (r == idx_size_-1) // last element
    {
        l *= s_step;
        if (l == r)
        {
            r = sv_size_-1;
            BM_ASSERT(l <= r);
            return;
        }
        r = sv_size_-1;
        if (l > r)
            l = 0;
    }
    else
    {
        if (l == r)
        {
            l *= s_step;
            r = l + s_step-1;
            if (r >= sv_size_)
                r = sv_size_-1;
        }
        else
        {
            const value_type* str = s_cache_.row(r);
            l *= s_step;
            r *= s_step;
            int cmp = SV::compare_str(search_str, str);
            BM_ASSERT(cmp <= 0);
            if (cmp != 0)
                r -= (r && idx_unique_); // -1 correct
            else
                if (idx_unique_)
                {
                    l = r;
                }
        }
    }
    BM_ASSERT(r <= sv_size_);
    BM_ASSERT(l <= r);
}

//----------------------------------------------------------------------------

} // namespace bm


#endif
