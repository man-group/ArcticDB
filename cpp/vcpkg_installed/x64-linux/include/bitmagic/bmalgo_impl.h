#ifndef BMALGO_IMPL__H__INCLUDED__
#define BMALGO_IMPL__H__INCLUDED__
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

/*! \file bmalgo_impl.h
    \brief Algorithms for bvector<>
*/

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4311 4312 4127)
#endif

#include "bmdef.h"
#include "bmutil.h"

namespace bm
{

/*! 
   @defgroup setalgo bvector<> algorithms 
 
   Set algebra algorithms using bit-vectors, arrays.
   Binary metrics and distances. Random sub-sets.
 
   @ingroup bvector
 */

/*! 
    @defgroup distance Binary-distance metrics
 
    Distance metrics and algorithms to compute binary distances
    @ingroup setalgo
 */


/*! 
    \brief    Distance metrics codes defined for vectors A and B
    \ingroup  distance
*/
enum distance_metric
{
    COUNT_AND = set_COUNT_AND,          //!< (A & B).count()
    COUNT_XOR = set_COUNT_XOR,          //!< (A ^ B).count()
    COUNT_OR  = set_COUNT_OR,           //!< (A | B).count()
    COUNT_SUB_AB = set_COUNT_SUB_AB,    //!< (A - B).count()
    COUNT_SUB_BA = set_COUNT_SUB_BA,    //!< (B - A).count()
    COUNT_A      = set_COUNT_A,         //!< A.count()
    COUNT_B      = set_COUNT_B          //!< B.count()
};

/**
    Convert set operation into compatible distance metric
    \ingroup  distance
*/
inline
distance_metric operation2metric(set_operation op) BMNOEXCEPT
{
    BM_ASSERT(is_const_set_operation(op));
    if (op == set_COUNT) op = set_COUNT_B;
    // distance metric is created as a set operation sub-class
    // simple cast will work to convert
    return (distance_metric) op;
}

/*! 
    \brief Distance metric descriptor, holds metric code and result.
    \sa distance_operation
*/
struct distance_metric_descriptor
{
#ifdef BM64ADDR
    typedef bm::id64_t   size_type;
#else
    typedef bm::id_t     size_type;
#endif

     distance_metric   metric;
     size_type          result;
     
     distance_metric_descriptor(distance_metric m) BMNOEXCEPT
     : metric(m),
       result(0)
    {}
    distance_metric_descriptor() BMNOEXCEPT
    : metric(bm::COUNT_XOR),
      result(0)
    {}
    
    /*! 
        \brief Sets metric result to 0
    */
    void reset() BMNOEXCEPT
    {
        result = 0;
    }
};

/// functor-adaptor for C-style callbacks
///
/// @internal
///
template <class VCBT, class size_type>
struct bit_visitor_callback_adaptor
{
    typedef VCBT bit_visitor_callback_type;

    bit_visitor_callback_adaptor(void* h, bit_visitor_callback_type cb_func)
        : handle_(h), func_(cb_func)
    {}

    int add_bits(size_type offset, const unsigned char* bits, unsigned size)
    {
        for (unsigned i = 0; i < size; ++i)
        {
            int ret = func_(handle_, offset + bits[i]);
            if (ret < 0)
                return ret;
        }
        return 0;
    }
    int add_range(size_type offset, size_type size)
    {
        for (size_type i = 0; i < size; ++i)
        {
            int ret = func_(handle_, offset + i);
            if (ret < 0)
                return ret;
        }
        return 0;
    }

    void*                     handle_;
    bit_visitor_callback_type func_;
};

/// functor-adaptor for back-inserter
///
/// @internal
///
template <class BII, class size_type>
struct bit_visitor_back_inserter_adaptor
{

    bit_visitor_back_inserter_adaptor(BII bi)
        : bi_(bi)
    {}

    int add_bits(size_type offset, const unsigned char* bits, unsigned size)
    {
        for (unsigned i = 0; i < size; ++i)
            *bi_ = offset + bits[i];
        return 0;
    }
    int add_range(size_type offset, size_type size)
    {
        for (size_type i = 0; i < size; ++i)
            *bi_ = offset + i;
        return 0;
    }

    BII bi_;
};


/*!
    \brief Internal function computes different distance metrics.
    \internal 
    \ingroup  distance
     
*/
inline
void combine_count_operation_with_block(const bm::word_t*           blk,
                                        const bm::word_t*           arg_blk,
                                        distance_metric_descriptor* dmit,
                                        distance_metric_descriptor* dmit_end) BMNOEXCEPT
                                            
{     
     gap_word_t* g1 = BMGAP_PTR(blk);
     gap_word_t* g2 = BMGAP_PTR(arg_blk);

     unsigned gap = BM_IS_GAP(blk);
     unsigned arg_gap = BM_IS_GAP(arg_blk);
     
     if (gap) // first block GAP-type
     {
         if (arg_gap)  // both blocks GAP-type
         {
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     dmd.result += gap_count_and(g1, g2);
                     break;
                 case bm::COUNT_OR:
                     dmd.result += gap_count_or(g1, g2);
                     break;
                 case bm::COUNT_SUB_AB:
                     dmd.result += gap_count_sub(g1, g2);
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.result += gap_count_sub(g2, g1);
                     break;
                 case bm::COUNT_XOR:
                     dmd.result += gap_count_xor(g1, g2);
                    break;
                 case bm::COUNT_A:
                    dmd.result += gap_bit_count_unr(g1);
                    break;
                 case bm::COUNT_B:
                    dmd.result += gap_bit_count_unr(g2);
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                                     
             } // for it
             
             return;

         }
         else // first block - GAP, argument - BITSET
         {
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     if (arg_blk)
                        dmd.result += gap_bitset_and_count(arg_blk, g1);
                     break;
                 case bm::COUNT_OR:
                     if (!arg_blk)
                        dmd.result += gap_bit_count_unr(g1);
                     else
                        dmd.result += gap_bitset_or_count(arg_blk, g1); 
                     break;
                 case bm::COUNT_SUB_AB:
                     {
                     bm::word_t  BM_VECT_ALIGN temp_bit_blk[bm::set_block_size] BM_VECT_ALIGN_ATTR;

                     gap_convert_to_bitset((bm::word_t*) temp_bit_blk, g1);
                     dmd.result += 
                       bit_operation_sub_count((bm::word_t*)temp_bit_blk, arg_blk);
                     }
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.metric = bm::COUNT_SUB_AB; // recursive call to SUB_AB
                     combine_count_operation_with_block(arg_blk,
                                                        blk,
                                                        it, it+1);
                     dmd.metric = bm::COUNT_SUB_BA; // restore status quo
                     break;
                 case bm::COUNT_XOR:
                     if (!arg_blk)
                        dmd.result += gap_bit_count_unr(g1);
                     else
                        dmd.result += gap_bitset_xor_count(arg_blk, g1);
                     break;
                 case bm::COUNT_A:
                    if (g1)
                        dmd.result += gap_bit_count_unr(g1);
                    break;
                 case bm::COUNT_B:
                    if (arg_blk)
                    {
                        dmd.result += 
                          bit_block_count(arg_blk);
                    }
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                                     
             } // for it
             
             return;
         
         }
     } 
     else // first block is BITSET-type
     {     
         if (arg_gap) // second argument block is GAP-type
         {
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     if (blk) 
                        dmd.result += gap_bitset_and_count(blk, g2);                         
                     break;
                 case bm::COUNT_OR:
                     if (!blk)
                        dmd.result += gap_bit_count_unr(g2);
                     else
                        dmd.result += gap_bitset_or_count(blk, g2);
                     break;
                 case bm::COUNT_SUB_AB:
                     if (blk)
                        dmd.result += gap_bitset_sub_count(blk, g2);
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.metric = bm::COUNT_SUB_AB; // recursive call to SUB_AB
                     combine_count_operation_with_block(arg_blk,
                                                        //arg_gap,
                                                        blk,
                                                        //gap,
                                                        it, it+1);
                     dmd.metric = bm::COUNT_SUB_BA; // restore status quo
                     break;
                 case bm::COUNT_XOR:
                     if (!blk)
                        dmd.result += gap_bit_count_unr(g2);
                     else
                        dmd.result += gap_bitset_xor_count(blk, g2); 
                    break;
                 case bm::COUNT_A:
                    if (blk)
                    {
                        dmd.result += 
                            bit_block_count(blk);
                    }
                    break;
                 case bm::COUNT_B:
                    if (g2)
                        dmd.result += gap_bit_count_unr(g2);
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                                     
             } // for it
             
             return;
         }
     }

     // --------------------------------------------
     //
     // Here we combine two plain bitblocks 

     for (distance_metric_descriptor* it = dmit; it < dmit_end; ++it)
     {
         distance_metric_descriptor& dmd = *it;
        bit_operation_count_func_type gfunc = 
            operation_functions<true>::bit_operation_count(dmd.metric);
        if (gfunc)
        {
            dmd.result += (*gfunc)(blk, arg_blk);
        }
        else
        {
            switch (dmd.metric)
            {
            case bm::COUNT_A:
                if (blk)
                    dmd.result += bm::bit_block_count(blk);
                break;
            case bm::COUNT_B:
                if (arg_blk)
                    dmd.result += bm::bit_block_count(arg_blk);
                break;
            case bm::COUNT_AND:
            case bm::COUNT_XOR:
            case bm::COUNT_OR:
            case bm::COUNT_SUB_AB:
            case bm::COUNT_SUB_BA:
            default:
                BM_ASSERT(0);
            } // switch
        }

     } // for it
}

/*!
\brief Internal function computes AND distance.
\internal 
\ingroup  distance
*/
inline
unsigned combine_count_and_operation_with_block(const bm::word_t* blk,
                                                const bm::word_t* arg_blk) BMNOEXCEPT
{
    unsigned gap = BM_IS_GAP(blk);
    unsigned arg_gap = BM_IS_GAP(arg_blk);
    if (gap) // first block GAP-type
    {
        if (arg_gap)  // both blocks GAP-type
        {
            return gap_count_and(BMGAP_PTR(blk), BMGAP_PTR(arg_blk));
        }
        else // first block - GAP, argument - BITSET
        {
            return gap_bitset_and_count(arg_blk, BMGAP_PTR(blk));
        }
    } 
    else // first block is BITSET-type
    {     
        if (arg_gap) // second argument block is GAP-type
        {
            return gap_bitset_and_count(blk, BMGAP_PTR(arg_blk));
        }
    }

    // --------------------------------------------
    // Here we combine two plain bitblocks 

    return bit_operation_and_count(blk, arg_blk);
}


/*!
    \brief Internal function computes different existense of distance metric.
    \internal 
    \ingroup  distance
*/
inline
void combine_any_operation_with_block(const bm::word_t* blk,
                                      unsigned gap,
                                      const bm::word_t* arg_blk,
                                      unsigned arg_gap,
                                      distance_metric_descriptor* dmit,
                                      distance_metric_descriptor* dmit_end) BMNOEXCEPT
                                            
{
     gap_word_t* res=0;
     
     gap_word_t* g1 = BMGAP_PTR(blk);
     gap_word_t* g2 = BMGAP_PTR(arg_blk);
     
     if (gap) // first block GAP-type
     {
         if (arg_gap)  // both blocks GAP-type
         {
             gap_word_t tmp_buf[bm::gap_max_buff_len * 3]; // temporary result
             
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 if (dmd.result)
                 {
                     continue;
                 }
                 res = 0;
                 unsigned dsize = 0;
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     dmd.result += gap_operation_any_and(g1, g2);
                     break;
                 case bm::COUNT_OR:
                     res = gap_operation_or(g1, g2, tmp_buf, dsize);
                     break;
                 case bm::COUNT_SUB_AB:
                     dmd.result += gap_operation_any_sub(g1, g2); 
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.result += gap_operation_any_sub(g2, g1); 
                     break;
                 case bm::COUNT_XOR:
                    dmd.result += gap_operation_any_xor(g1, g2); 
                    break;
                 case bm::COUNT_A:
                    res = g1;
                    break;
                 case bm::COUNT_B:
                    res = g2;
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                if (res)
                    dmd.result += !gap_is_all_zero(res);
                                     
             } // for it
             
             return;

         }
         else // first block - GAP, argument - BITSET
         {
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 if (dmd.result)
                 {
                     continue;
                 }
                 
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     if (arg_blk)
                        dmd.result += gap_bitset_and_any(arg_blk, g1);
                     break;
                 case bm::COUNT_OR:
                     if (!arg_blk)
                        dmd.result += !gap_is_all_zero(g1);
                     else
                        dmd.result += gap_bitset_or_any(arg_blk, g1); 
                     break;
                 case bm::COUNT_SUB_AB:
                     {
                     bm::word_t  BM_VECT_ALIGN temp_blk[bm::set_block_size] BM_VECT_ALIGN_ATTR;
                     gap_convert_to_bitset((bm::word_t*) temp_blk, g1);
                     dmd.result += 
                       bit_operation_sub_any((bm::word_t*)temp_blk, arg_blk);
                     }
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.metric = bm::COUNT_SUB_AB; // recursive call to SUB_AB
                     combine_any_operation_with_block(arg_blk,
                                                      arg_gap,
                                                      blk,
                                                      gap,
                                                      it, it+1);
                     dmd.metric = bm::COUNT_SUB_BA; // restore status quo
                     break;
                 case bm::COUNT_XOR:
                     if (!arg_blk)
                        dmd.result += !gap_is_all_zero(g1);
                     else
                        dmd.result += gap_bitset_xor_any(arg_blk, g1);
                     break;
                 case bm::COUNT_A:
                    if (g1)
                        dmd.result += !gap_is_all_zero(g1);
                    break;
                 case bm::COUNT_B:
                    if (arg_blk)
                    {
                        dmd.result += 
                          !bit_is_all_zero(arg_blk);
                    }
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                                     
             } // for it
             
             return;
         
         }
     } 
     else // first block is BITSET-type
     {     
         if (arg_gap) // second argument block is GAP-type
         {
             for (distance_metric_descriptor* it = dmit;it < dmit_end; ++it)
             {
                 distance_metric_descriptor& dmd = *it;
                 if (dmd.result)
                 {
                     continue;
                 }
                 
                 switch (dmd.metric)
                 {
                 case bm::COUNT_AND:
                     if (blk) 
                        dmd.result += gap_bitset_and_any(blk, g2);                         
                     break;
                 case bm::COUNT_OR:
                     if (!blk)
                        dmd.result += !gap_is_all_zero(g2);
                     else
                        dmd.result += gap_bitset_or_any(blk, g2);
                     break;
                 case bm::COUNT_SUB_AB:
                     if (blk)
                        dmd.result += gap_bitset_sub_any(blk, g2);
                     break;
                 case bm::COUNT_SUB_BA:
                     dmd.metric = bm::COUNT_SUB_AB; // recursive call to SUB_AB
                     combine_any_operation_with_block(arg_blk,
                                                      arg_gap,
                                                      blk,
                                                      gap,
                                                      it, it+1);
                     dmd.metric = bm::COUNT_SUB_BA; // restore status quo
                     break;
                 case bm::COUNT_XOR:
                     if (!blk)
                        dmd.result += !gap_is_all_zero(g2);
                     else
                        dmd.result += gap_bitset_xor_any(blk, g2); 
                    break;
                 case bm::COUNT_A:
                    if (blk)
                    {
                        dmd.result+=
                            !bm::bit_is_all_zero(blk);
                    }
                    break;
                 case bm::COUNT_B:
                    if (g2)
                        dmd.result += !gap_is_all_zero(g2);
                    break;
                 default:
                     BM_ASSERT(0);
                 } // switch
                                     
             } // for it
             
             return;
         }
     }

     // --------------------------------------------
     //
     // Here we combine two plain bitblocks 

     for (distance_metric_descriptor* it = dmit; it < dmit_end; ++it)
     {
        distance_metric_descriptor& dmd = *it;
        if (dmd.result)
        {
            continue;
        }

        switch (dmd.metric)
        {
        case bm::COUNT_AND:
            dmd.result += 
            bit_operation_and_any(blk, arg_blk);
            break;
        case bm::COUNT_OR:
            dmd.result += 
            bit_operation_or_any(blk, arg_blk);
            break;
        case bm::COUNT_SUB_AB:
            dmd.result += 
            bit_operation_sub_any(blk, arg_blk);
            break;
        case bm::COUNT_SUB_BA:
            dmd.result += 
            bit_operation_sub_any(arg_blk, blk);
            break;
        case bm::COUNT_XOR:
            dmd.result += 
            bit_operation_xor_any(blk, arg_blk);
            break;
        case bm::COUNT_A:
            if (blk)
                dmd.result += !bit_is_all_zero(blk);
            break;
        case bm::COUNT_B:
            if (arg_blk)
                dmd.result += !bit_is_all_zero(arg_blk);
            break;
        default:
            BM_ASSERT(0);
        } // switch

     } // for it
}



/*!
    Convenience internal function to compute combine count for one metric
    \internal
    \ingroup  distance
*/
inline
unsigned
combine_count_operation_with_block(const bm::word_t* blk,
                                   const bm::word_t* arg_blk,
                                   distance_metric metric) BMNOEXCEPT
{
    distance_metric_descriptor dmd(metric);
    combine_count_operation_with_block(blk, //gap, 
                                       arg_blk, //arg_gap, 
                                       &dmd, &dmd+1);
    return unsigned(dmd.result);
}


/*!
    Convenience internal function to compute combine any for one metric
    \internal
    \ingroup  distance
*/
inline
bm::distance_metric_descriptor::size_type
combine_any_operation_with_block(const bm::word_t* blk,
                                          unsigned gap,
                                          const bm::word_t* arg_blk,
                                          unsigned arg_gap,
                                          distance_metric metric) BMNOEXCEPT
{
    distance_metric_descriptor dmd(metric);
    combine_any_operation_with_block(blk, gap, 
                                     arg_blk, arg_gap, 
                                     &dmd, &dmd+1);
    return dmd.result;
}

/*!
    \brief Staging function for distance operation

    \return temp block allocated (or NULL)

    \internal
*/
inline
void distance_stage(const distance_metric_descriptor* dmit,
                    const distance_metric_descriptor* dmit_end,
                    bool*                             is_all_and) BMNOEXCEPT
{
    for (const distance_metric_descriptor* it = dmit; it < dmit_end; ++it)
    {
        if (it->metric != bm::COUNT_AND)
        {
            *is_all_and = false;
        } 
    } // for
}

/*!
    \brief Distance computing template function.

    Function receives two bitvectors and an array of distance metrics
    (metrics pipeline). Function computes all metrics saves result into
    corresponding pipeline results (distance_metric_descriptor::result)
    An important detail is that function reuses metric descriptors, 
    incrementing received values. It allows you to accumulate results 
    from different calls in the pipeline.
    
    \param bv1      - argument bitvector 1 (A)
    \param bv2      - argument bitvector 2 (B)
    \param dmit     - pointer to first element of metric descriptors array
                      Input-Output parameter, receives metric code as input,
                      computation is added to "result" field
    \param dmit_end - pointer to (last+1) element of metric descriptors array
    \ingroup  distance
    
*/
template<class BV>
void distance_operation(const BV& bv1, 
                        const BV& bv2, 
                        distance_metric_descriptor* dmit,
                        distance_metric_descriptor* dmit_end) BMNOEXCEPT
{
    const typename BV::blocks_manager_type& bman1 = bv1.get_blocks_manager();
    const typename BV::blocks_manager_type& bman2 = bv2.get_blocks_manager();

    bool is_all_and = true; // flag is distance operation is just COUNT_AND
    distance_stage(dmit, dmit_end, &is_all_and);

    bm::word_t*** blk_root = bman1.top_blocks_root();
    typename BV::size_type block_idx = 0;
    unsigned i, j;
    
    const bm::word_t* blk;
    const bm::word_t* arg_blk;

    unsigned top_block_size = bman1.top_block_size();
    unsigned ebs2 = bman2.top_block_size();
    unsigned top_size;
    if (ebs2 > top_block_size)
        top_size = ebs2;
    else
        top_size = top_block_size;

    for (i = 0; i < top_size; ++i)
    {
        bm::word_t** blk_blk = (blk_root && (i < top_block_size)) ? blk_root[i] : 0;
        if (!blk_blk) 
        {
            // AND operation requested - we can skip this portion here 
            if (is_all_and)
            {
                block_idx += bm::set_sub_array_size;
                continue;
            }
            const bm::word_t* const* bvbb = bman2.get_topblock(i);
            if (bvbb == 0) 
            {
                block_idx += bm::set_sub_array_size;
                continue;
            }

            blk = 0;
            for (j = 0; j < bm::set_sub_array_size; ++j,++block_idx)
            {                
                arg_blk = bman2.get_block(i, j);
                if (!arg_blk) 
                    continue;
                combine_count_operation_with_block(blk, 
                                                   arg_blk, 
                                                   dmit, dmit_end);
            } // for j
            continue;
        }

        for (j = 0; j < bm::set_sub_array_size; ++j, ++block_idx)
        {
            blk = bman1.get_block(i, j);
            if (blk == 0 && is_all_and)
                continue;

            arg_blk = bman2.get_block(i, j);

            if (!blk & !arg_blk)
                continue;
                
            combine_count_operation_with_block(blk, 
                                               arg_blk, 
                                               dmit, dmit_end);
        } // for j

    } // for i
}


/*!
\brief Distance AND computing template function.


\param bv1      - argument bitvector 1 (A)
\param bv2      - argument bitvector 2 (B)
\ingroup  distance

*/
template<class BV>
typename BV::size_type distance_and_operation(const BV& bv1,
                                              const BV& bv2) BMNOEXCEPT
{
    const typename BV::blocks_manager_type& bman1 = bv1.get_blocks_manager();
    const typename BV::blocks_manager_type& bman2 = bv2.get_blocks_manager();
    
    if (!bman1.is_init() || !bman2.is_init())
        return 0;

    bm::word_t*** blk_root     = bman1.top_blocks_root();
    bm::word_t*** blk_root_arg = bman2.top_blocks_root();
    typename BV::size_type count = 0;

    unsigned top_block_size =
        bm::min_value(bman1.top_block_size(),bman2.top_block_size());

    for (unsigned i = 0; i < top_block_size; ++i)
    {
        bm::word_t** blk_blk;
        bm::word_t** blk_blk_arg;
        if ((blk_blk = blk_root[i]) == 0 || (blk_blk_arg= blk_root_arg[i]) == 0)
            continue;

        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
            blk_blk = FULL_SUB_BLOCK_REAL_ADDR;
        if ((bm::word_t*)blk_blk_arg == FULL_BLOCK_FAKE_ADDR)
            blk_blk_arg = FULL_SUB_BLOCK_REAL_ADDR;

        for (unsigned j = 0; j < bm::set_sub_array_size; j+=4)
        {
            (blk_blk[j] && blk_blk_arg[j]) ? 
                count += combine_count_and_operation_with_block(BLOCK_ADDR_SAN(blk_blk[j]), BLOCK_ADDR_SAN(blk_blk_arg[j]))
                :0;
            (blk_blk[j+1] && blk_blk_arg[j+1]) ? 
                count += combine_count_and_operation_with_block(BLOCK_ADDR_SAN(blk_blk[j+1]), BLOCK_ADDR_SAN(blk_blk_arg[j+1]))
                :0;
            (blk_blk[j+2] && blk_blk_arg[j+2]) ? 
                count += combine_count_and_operation_with_block(BLOCK_ADDR_SAN(blk_blk[j+2]), BLOCK_ADDR_SAN(blk_blk_arg[j+2]))
                :0;
            (blk_blk[j+3] && blk_blk_arg[j+3]) ? 
                count += combine_count_and_operation_with_block(BLOCK_ADDR_SAN(blk_blk[j+3]), BLOCK_ADDR_SAN(blk_blk_arg[j+3]))
                :0;
        } // for j

    } // for i
    return count;
}


/*!
    \brief Distance screening template function.

    Function receives two bitvectors and an array of distance metrics
    (metrics pipeline). Function computes possybility of a metric(any bit) 
    saves result into corresponding pipeline results 
    (distance_metric_descriptor::result)
    An important detail is that function reuses metric descriptors, 
    incrementing received values. It allows you to accumulate results 
    from different calls in the pipeline.
    
    \param bv1      - argument bitvector 1 (A)
    \param bv2      - argument bitvector 2 (B)
    \param dmit     - pointer to first element of metric descriptors array
                      Input-Output parameter, receives metric code as input,
                      computation is added to "result" field
    \param dmit_end - pointer to (last+1) element of metric descriptors array
    \ingroup  distance
*/
template<class BV>
void distance_operation_any(const BV& bv1, 
                            const BV& bv2, 
                            distance_metric_descriptor* dmit,
                            distance_metric_descriptor* dmit_end) BMNOEXCEPT
{
    const typename BV::blocks_manager_type& bman1 = bv1.get_blocks_manager();
    const typename BV::blocks_manager_type& bman2 = bv2.get_blocks_manager();
    
    bool is_all_and = true; // flag is distance operation is just COUNT_AND
    distance_stage(dmit, dmit_end, &is_all_and);
  
    bm::word_t*** blk_root = bman1.top_blocks_root();
    unsigned block_idx = 0;
    unsigned i, j;
    
    const bm::word_t* blk;
    const bm::word_t* arg_blk;
    bool  blk_gap;
    bool  arg_gap;

    unsigned top_block_size = bman1.top_block_size();
    unsigned ebs2 = bman2.top_block_size();
    unsigned top_size;
    if (ebs2 > top_block_size)
        top_size = ebs2;
    else
        top_size = top_block_size;

    for (i = 0; i < top_size; ++i)
    {
        bm::word_t** blk_blk = (blk_root && (i < top_block_size)) ? blk_root[i] : 0;
        if (blk_blk == 0) // not allocated
        {
            // AND operation requested - we can skip this portion here 
            if (is_all_and)
            {
                block_idx += bm::set_sub_array_size;
                continue;
            }

            const bm::word_t* const* bvbb = bman2.get_topblock(i);
            if (bvbb == 0) 
            {
                block_idx += bm::set_sub_array_size;
                continue;
            }

            blk = 0;
            blk_gap = false;

            for (j = 0; j < bm::set_sub_array_size; ++j,++block_idx)
            {                
                arg_blk = bman2.get_block(i, j);
                if (!arg_blk) 
                    continue;
                arg_gap = BM_IS_GAP(arg_blk);
                
                combine_any_operation_with_block(blk, blk_gap,
                                                 arg_blk, arg_gap,
                                                 dmit, dmit_end);

                // check if all distance requests alredy resolved
                bool all_resolved = false;
                distance_metric_descriptor* it=dmit;
                do
                {
                    if (!it->result)
                    {
                        all_resolved = false;
                        break;
                    }
                    ++it;
                } while (it < dmit_end);
                if (all_resolved)
                    return;
            } // for j

            continue;
        }

        for (j = 0; j < bm::set_sub_array_size; ++j, ++block_idx)
        {
            blk = bman1.get_block(i, j);
            if (blk == 0 && is_all_and)
                continue;

            arg_blk = bman2.get_block(i, j);

            if (blk == 0 && arg_blk == 0)
                continue;
                
            arg_gap = BM_IS_GAP(arg_blk);
            blk_gap = BM_IS_GAP(blk);
            
            combine_any_operation_with_block(blk, blk_gap,
                                             arg_blk, arg_gap,
                                             dmit, dmit_end);
            
            // check if all distance requests alredy resolved
            bool all_resolved = true;
            distance_metric_descriptor* it=dmit;
            do
            {
                if (!it->result)
                {
                    all_resolved = false;
                    break;
                }
                ++it;
            } while (it < dmit_end);
            if (all_resolved)
                return;

        } // for j

    } // for i
}



/**
    \brief Internal algorithms scans the input for the block range limit
    \internal
*/
template<typename It, typename SIZE_TYPE>
It block_range_scan(It  first, It last,
                    SIZE_TYPE nblock, SIZE_TYPE* max_id) BMNOEXCEPT
{
    SIZE_TYPE m = *max_id;
    It right;
    for (right = first; right != last; ++right)
    {
        SIZE_TYPE v = SIZE_TYPE(*right);
        BM_ASSERT(v < bm::id_max);
        if (v >= m)
            m = v;
        SIZE_TYPE nb = v >> bm::set_block_shift;
        if (nb != nblock)
            break;
    }
    *max_id = m;
    return right;
}

/**
    \brief OR Combine bitvector and the iterable sequence 

    Algorithm combines bvector with sequence of integers 
    (represents another set). When the agrument set is sorted 
    this method can give serious increase in performance.
    
    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence
    
    \ingroup setalgo
*/
template<class BV, class It>
void combine_or(BV& bv, It  first, It last)
{
    typedef typename BV::size_type size_type;
    typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();
    
    size_type max_id = 0;

    while (first < last)
    {
        typename BV::block_idx_type nblock = (*first) >> bm::set_block_shift;
        It right = bm::block_range_scan(first, last, nblock, &max_id);
        if (max_id >= bv.size())
        {
            BM_ASSERT(max_id < bm::id_max);
            bv.resize(max_id + 1);
        }

        // now we have one in-block array of bits to set
        
        label1:
        
        int block_type;
        bm::word_t* blk =
            bman.check_allocate_block(nblock, 
                                      true, 
                                      bv.get_new_blocks_strat(), 
                                      &block_type);
        if (!blk) 
            continue;
                        
        if (block_type == 1) // gap
        {            
            bm::gap_word_t* gap_blk = BMGAP_PTR(blk);
            unsigned threshold = bm::gap_limit(gap_blk, bman.glen());
            
            for (; first < right; ++first)
            {
                unsigned is_set;
                unsigned nbit   = (*first) & bm::set_block_mask; 
                
                unsigned new_block_len =
                    bm::gap_set_value(true, gap_blk, nbit, &is_set);
                if (new_block_len > threshold) 
                {
                    bman.extend_gap_block(nblock, gap_blk);
                    ++first;
                    goto label1;  // block pointer changed - goto reset
                }
            }
        }
        else // bit block
        {
            for (; first < right; ++first)
            {
                size_type pos = *first;
                unsigned nbit   = unsigned(pos & bm::set_block_mask);
                unsigned nword  = unsigned(nbit >> bm::set_word_shift); 
                nbit &= bm::set_word_mask;
                blk[nword] |= (1u << nbit);
            } // for
        }
    } // while
}


/**
    \brief XOR Combine bitvector and the iterable sequence 

    Algorithm combines bvector with sequence of integers 
    (represents another set). When the agrument set is sorted 
    this method can give serious increase in performance.
    
    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence
    
    \ingroup setalgo
*/
template<class BV, class It>
void combine_xor(BV& bv, It  first, It last)
{
    typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();
    
    typename BV::size_type max_id = 0;

    while (first < last)
    {
        typename BV::block_idx_type nblock = ((*first) >> bm::set_block_shift);
        It right = block_range_scan(first, last, nblock, &max_id);

        if (max_id >= bv.size())
        {
            BM_ASSERT(max_id < bm::id_max);
            bv.resize(max_id + 1);
        }

        // now we have one in-block array of bits to set
        
        label1:
        
        int block_type;
        bm::word_t* blk =
            bman.check_allocate_block(nblock, 
                                      true, 
                                      bv.get_new_blocks_strat(), 
                                      &block_type,
                                      false /* no null return */);
        BM_ASSERT(blk); 
                        
        if (block_type == 1) // gap
        {
            bm::gap_word_t* gap_blk = BMGAP_PTR(blk);
            unsigned threshold = bm::gap_limit(gap_blk, bman.glen());
            
            for (; first < right; ++first)
            {
                unsigned is_set;
                unsigned nbit   = (*first) & bm::set_block_mask; 
                
                is_set = bm::gap_test_unr(gap_blk, nbit);
                BM_ASSERT(is_set <= 1);
                is_set ^= 1; 
                
                unsigned new_block_len =
                    gap_set_value(is_set, gap_blk, nbit, &is_set);
                if (new_block_len > threshold) 
                {
                    bman.extend_gap_block(nblock, gap_blk);
                    ++first;
                    goto label1;  // block pointer changed - goto reset
                }
            }
        }
        else // bit block
        {
            for (; first < right; ++first)
            {
                unsigned nbit   = unsigned(*first & bm::set_block_mask); 
                unsigned nword  = unsigned(nbit >> bm::set_word_shift); 
                nbit &= bm::set_word_mask;
                blk[nword] ^= (1u << nbit);
            } // for
        }
    } // while
    
    bv.forget_count();
}



/**
    \brief SUB Combine bitvector and the iterable sequence 

    Algorithm combines bvector with sequence of integers 
    (represents another set). When the agrument set is sorted 
    this method can give serious increase in performance.
    
    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence
    
    \ingroup setalgo
*/
template<class BV, class It>
void combine_sub(BV& bv, It  first, It last)
{
    typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();
    
    typename BV::size_type max_id = 0;

    while (first < last)
    {
        typename BV::block_idx_type nblock = (*first) >> bm::set_block_shift;     
        It right = bm::block_range_scan(first, last, nblock, &max_id);

        if (max_id >= bv.size())
        {
            BM_ASSERT(max_id < bm::id_max);
            bv.resize(max_id + 1);
        }

        // now we have one in-block array of bits to set
        
        label1:
        
        int block_type;
        bm::word_t* blk =
            bman.check_allocate_block(nblock, 
                                      false, 
                                      bv.get_new_blocks_strat(), 
                                      &block_type);

        if (!blk)
            continue;
                        
        if (block_type == 1) // gap
        {
            bm::gap_word_t* gap_blk = BMGAP_PTR(blk);
            unsigned threshold = bm::gap_limit(gap_blk, bman.glen());
            
            for (; first < right; ++first)
            {
                unsigned is_set;
                unsigned nbit   = (*first) & bm::set_block_mask; 
                
                is_set = bm::gap_test_unr(gap_blk, nbit);
                if (!is_set)
                    continue;
                
                unsigned new_block_len =
                    gap_set_value(false, gap_blk, nbit, &is_set);
                if (new_block_len > threshold) 
                {
                    bman.extend_gap_block(nblock, gap_blk);
                    ++first;
                    goto label1;  // block pointer changed - goto reset
                }
            }
        }
        else // bit block
        {
            for (; first < right; ++first)
            {
                unsigned nbit   = unsigned(*first & bm::set_block_mask); 
                unsigned nword  = unsigned(nbit >> bm::set_word_shift); 
                nbit &= bm::set_word_mask;
                blk[nword] &= ~((bm::word_t)1 << nbit);
            } // for
        }
    } // while
    
    bv.forget_count();
}

/**
    \brief AND Combine bitvector and the iterable sequence 

    Algorithm combines bvector with sorted sequence of integers 
    (represents another set).
    
    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence
    
    \ingroup setalgo
*/
template<class BV, class It>
void combine_and_sorted(BV& bv, It  first, It last)
{
    typename BV::size_type prev = 0;
    for ( ;first < last; ++first)
    {
        typename BV::size_type id = *first;
        BM_ASSERT(id >= prev); // make sure it's sorted
        bv.set_bit_and(id, true);
        if (++prev < id) 
        {
            bv.set_range(prev, id-1, false);
        }
        prev = id;
    }
}


/**
    \brief AND Combine bitvector and the iterable sequence 

    Algorithm combines bvector with sequence of integers 
    (represents another set). When the agrument set is sorted 
    this method can give serious increase in performance.
    
    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence
    
    \ingroup setalgo
    \sa combine_and_sorted
*/
template<class BV, class It>
void combine_and(BV& bv, It  first, It last)
{
    BV bv_tmp;
    combine_or(bv_tmp, first, last);
    bv &= bv_tmp;
}

/*!
    \brief Compute number of bit intervals (GAPs) in the bitvector
    
    Algorithm traverses bit vector and count number of uninterrupted
    intervals of 1s and 0s.
    <pre>
    For example: 
      empty vector   - 1 interval
      00001111100000 - gives us 3 intervals
      10001111100000 - 4 intervals
      00000000000000 - 1 interval
      11111111111111 - 1 interval
    </pre>
    \return Number of intervals
    \ingroup setalgo
*/
template<class BV>
typename BV::size_type count_intervals(const BV& bv)
{
    const typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    
    if (!bman.is_init())
        return 1;

    bm::word_t*** blk_root = bman.top_blocks_root();
    typename BV::blocks_manager_type::block_count_change_func func(bman);
    typename BV::blocks_manager_type::block_idx_type st = 0;
    bm::for_each_block(blk_root, bman.top_block_size(), func, st);

    typename BV::size_type intervals = func.count();
    bool last_bit_set = bv.test(bm::id_max-1);

    intervals -= last_bit_set; // correct last (out of range) interval
    return intervals;
}

/*!
    \brief Export bitset from an array of binary data representing
    the bit vector. 

    Input array can be array of ints, chars or other native C types.
    Method works with iterators, which makes it compatible with 
    STL containers and C arrays

    \param bv     - destination bitvector
    \param first  - first element of the iterated sequence
    \param last   - last element of the iterated sequence

    \ingroup setalgo
*/
template<typename BV, class It>
void export_array(BV& bv, It first, It last)
{
    typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    if (!bman.is_init())
        bman.init_tree();
    
    unsigned inp_word_size = sizeof(*first);
    size_t array_size = size_t(last - first);
    size_t bit_cnt = array_size * inp_word_size * 8;
    int block_type;
    bm::word_t tmp;
    bm::word_t b1, b2, b3, b4;

    if (bit_cnt >= bv.size())
        bv.resize((bm::id_t)bit_cnt + 1);
    else
        bv.set_range((typename BV::size_type)bit_cnt, bv.size() - 1, false);
    switch (inp_word_size)
    {
    case 1:
        {
            size_t word_cnt = array_size / 4;
            for (typename BV::block_idx_type i = 0; i < bm::set_total_blocks; ++i)
            {
                bm::word_t* blk =
                    bman.check_allocate_block(i, 
                                              false, 
                                              BM_BIT, 
                                              &block_type,
                                              false /*no NULL ret*/);
                if (block_type == 1) // gap
                {
                    blk = bman.deoptimize_block(i);
                }
                
                bm::word_t* wrd_ptr = blk;
                if (word_cnt >= bm::set_block_size) {
                    bm::word_t* wrd_end = blk + bm::set_block_size;
                    do {
                        b1 = bm::word_t(*first++); b2 = bm::word_t(*first++);
                        b3 = bm::word_t(*first++); b4 = bm::word_t(*first++);
                        tmp = b1 | (b2 << 8u) | (b3 << 16u) | (b4 << 24u);
                        *wrd_ptr++ = tmp;
                    } while (wrd_ptr < wrd_end);
                    word_cnt -= bm::set_block_size;
                } 
                else 
                {
                    size_t to_convert = size_t(last - first);
                    for (size_t j = 0; j < to_convert / 4; ++j)
                    {
                        b1 = bm::word_t(*first++); b2 = bm::word_t(*first++);
                        b3 = bm::word_t(*first++); b4 = bm::word_t(*first++);
                        tmp = b1 | (b2 << 8u) | (b3 << 16u) | (b4 << 24u);
                        *wrd_ptr++ = tmp;
                    }
                    while (1)
                    {
                        if (first == last) break;
                        *wrd_ptr = bm::word_t(*first++);
                        if (first == last) break;
                        *wrd_ptr |= bm::word_t(*first++) << 8u;
                        if (first == last) break;
                        *wrd_ptr |= bm::word_t(*first++) << 16u;
                        if (first == last) break;
                        *wrd_ptr |= bm::word_t(*first++) << 24u;
                        ++wrd_ptr;
                    }
                }
                if (first == last) break;
            } // for
        }
        break;
    case 2:
        {
            size_t word_cnt = array_size / 2;
            for (typename BV::block_idx_type i = 0; i < bm::set_total_blocks; ++i)
            {
                bm::word_t* blk =
                    bman.check_allocate_block(i, 
                                              false, 
                                              BM_BIT, 
                                              &block_type,
                                              false /*no NULL ret*/);
                if (block_type) // gap
                    blk = bman.deoptimize_block(i);
                
                bm::word_t* wrd_ptr = blk;
                if (word_cnt >= bm::set_block_size) {
                    bm::word_t* wrd_end = blk + bm::set_block_size;
                    do {
                        b1 = bm::word_t(*first++); b2 = bm::word_t(*first++);
                        tmp = b1 | (b2 << 16);
                        *wrd_ptr++ = tmp;
                    } while (wrd_ptr < wrd_end);
                    word_cnt -= bm::set_block_size;
                } 
                else 
                {
                    size_t to_convert = size_t(last - first);
                    for (unsigned j = 0; j < to_convert / 2; ++j)
                    {
                        b1 = bm::word_t(*first++); b2 = bm::word_t(*first++);
                        tmp = b1 | (b2 << 16u);
                        *wrd_ptr++ = tmp;
                    }
                    while (1)
                    {
                        if (first == last) break;
                        *wrd_ptr = bm::word_t(*first++);
                        if (first == last) break;
                        *wrd_ptr |= bm::word_t((*first++) << 16u);
                        ++wrd_ptr;
                    }
                }
                if (first == last) break;
            } // for
        }
        break;
    case 4:
        {
            size_t word_cnt = array_size;
            for (typename BV::block_idx_type i = 0; i < bm::set_total_blocks; ++i)
            {
                bm::word_t* blk =
                    bman.check_allocate_block(i, 
                                              false, 
                                              BM_BIT, 
                                              &block_type,
                                              false /*no NULL ret*/);
                if (block_type == 1) // gap
                    blk = bman.deoptimize_block(i);
                
                bm::word_t* wrd_ptr = blk;
                if (word_cnt >= bm::set_block_size) {
                    bm::word_t* wrd_end = blk + bm::set_block_size;
                    do
                    {
                        *wrd_ptr++ = bm::word_t(*first++);
                    } while (wrd_ptr < wrd_end);
                    word_cnt -= bm::set_block_size;
                } 
                else 
                {
                    while (1)
                    {
                        if (first == last) break;
                        *wrd_ptr = bm::word_t(*first++);
                        ++wrd_ptr;
                    }
                }
                if (first == last) break;
            } // for
        }
        break;
    default:
        BM_ASSERT(0);
    } // switch

}


/*!
   \brief for-each visitor, calls a visitor functor for each 1 bit group
 
   \param block - bit block buffer pointer
   \param offset - global block offset (number of bits)
   \param bit_functor - functor must support .add_bits(offset, bits_ptr, size)
   \return - functor return code (< 0 - interrupt the processing)
 
   @ingroup bitfunc
   @internal
*/
template<typename Func, typename SIZE_TYPE>
int for_each_bit_blk(const bm::word_t* block, SIZE_TYPE offset,
                      Func&  bit_functor)
{
    BM_ASSERT(block);
    int ret;
    if (IS_FULL_BLOCK(block))
    {
        ret = bit_functor.add_range(offset, bm::gap_max_bits);
        return ret;
    }
    unsigned char bits[bm::set_bitscan_wave_size*32];
    SIZE_TYPE offs = offset;
    const word_t* block_end = block + bm::set_block_size;
    do
    {
        if (unsigned cnt = bm::bitscan_wave(block, bits))
        {
            ret = bit_functor.add_bits(offs, bits, cnt);
            if (ret < 0)
                return ret;
        }
        offs += bm::set_bitscan_wave_size * 32;
        block += bm::set_bitscan_wave_size;
    } while (block < block_end);
    return 0;
}

/*!
   \brief for-each range visitor, calls a visitor functor for each 1 bit group

   \param block - bit block buffer pointer
   \param offset - global block offset (number of bits)
   \param left - bit addredd in block from [from..to]
   \param right - bit addredd in block to [from..to]
   \param bit_functor - functor must support .add_bits(offset, bits_ptr, size)
   \return - functor return code (< 0 - interrupt the processing)
   @ingroup bitfunc
   @internal
*/
template<typename Func, typename SIZE_TYPE>
int for_each_bit_blk(const bm::word_t* block, SIZE_TYPE offset,
                      unsigned left, unsigned right,
                      Func&  bit_functor)
{
    BM_ASSERT(block);
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::bits_in_block);

    int ret = 0;
    if (IS_FULL_BLOCK(block))
    {
        unsigned sz = right - left + 1;
        ret = bit_functor.add_range(offset + left, sz);
        return ret;
    }
    unsigned char bits[bm::set_bitscan_wave_size*32];

    unsigned cnt, nword, nbit, bitcount, temp;
    nbit = left & bm::set_word_mask;
    const bm::word_t* word =
        block + (nword = unsigned(left >> bm::set_word_shift));
    if (left == right)  // special case (only 1 bit to check)
    {
        if ((*word >> nbit) & 1u)
        {
            bits[0] = (unsigned char)nbit;
            ret = bit_functor.add_bits(offset + (nword * 32), bits, 1);
        }
        return ret;
    }

    bitcount = right - left + 1u;
    if (nbit) // starting position is not aligned
    {
        unsigned right_margin = nbit + right - left;
        if (right_margin < 32)
        {
            unsigned mask_r = bm::mask_r_u32(nbit);
            unsigned mask_l = bm::mask_l_u32(right_margin);
            unsigned mask = mask_r & mask_l;

            temp = (*word & mask);
            cnt = bm::bitscan(temp, bits);
            if (cnt)
                ret = bit_functor.add_bits(offset + (nword * 32), bits, cnt);
            return ret;
        }
        unsigned mask_r = bm::mask_r_u32(nbit);
        temp = *word & mask_r;
        cnt = bm::bitscan(temp, bits);
        if (cnt)
        {
            ret = bit_functor.add_bits(offset + (nword * 32), bits, cnt);
            if (ret < 0)
                return ret;
        }
        bitcount -= 32 - nbit;
        ++word; ++nword;
    }
    else
    {
        bitcount = right - left + 1u;
    }

    // word aligned now - scan the bit-stream loop unrolled 4x words(wave)
    BM_ASSERT(bm::set_bitscan_wave_size == 4);
    for ( ;bitcount >= 128;
           bitcount-=128, word+=bm::set_bitscan_wave_size,
           nword += bm::set_bitscan_wave_size)
    {
        cnt = bm::bitscan_wave(word, bits);
        if (cnt)
        {
            ret = bit_functor.add_bits(offset + (nword * 32), bits, cnt);
            if (ret < 0)
                return ret;
        }
    } // for

    for ( ;bitcount >= 32; bitcount-=32, ++word)
    {
        temp = *word;
        cnt = bm::bitscan(temp, bits);
        if (cnt)
        {
            ret = bit_functor.add_bits(offset + (nword * 32), bits, cnt);
            if (ret < 0)
                return ret;
        }
        ++nword;
    } // for

    BM_ASSERT(bitcount < 32);

    if (bitcount)  // we have a tail to count
    {
        unsigned mask_l = bm::mask_l_u32(bitcount-1);
        temp = *word & mask_l;
        cnt = bm::bitscan(temp, bits);
        if (cnt)
        {
            ret = bit_functor.add_bits(offset + (nword * 32), bits, cnt);
            if (ret < 0)
                return ret;
        }
    }
    return 0;
}



/*!
   \brief for-each visitor, calls a special visitor functor for each 1 bit range
 
   \param buf - bit block buffer pointer
   \param offset - global block offset (number of bits)
   \param bit_functor - functor must support .add_range(offset, bits_ptr, size)
   \return - functor return code (< 0 - interrupt the processing)

   @ingroup gapfunc
   @internal
*/
template<typename T, typename Func, typename SIZE_TYPE>
int for_each_gap_blk(const T* buf, SIZE_TYPE offset,
                      Func&  bit_functor)
{
    const T* pcurr = buf + 1;
    const T* pend = buf + (*buf >> 3);
    int ret = 0;
    if (*buf & 1)
    {
        ret = bit_functor.add_range(offset, *pcurr + 1);
        if (ret < 0)
            return ret;
        ++pcurr;
    }
    for (++pcurr; (pcurr <= pend) && (ret >= 0); pcurr += 2)
    {
        T prev = *(pcurr-1);
        ret = bit_functor.add_range(offset + prev + 1, *pcurr - prev);
    }
    return ret;
}

/*!
   \brief for-each visitor, calls a special visitor functor for each 1 bit range

   \param buf - bit block buffer pointer
   \param offset - global block offset (number of bits)
   \param left - interval start [left..right]
   \param right - intreval end [left..right]
   \param bit_functor - functor must support .add_range(offset, bits_ptr, size)
   \return - functor return code (< 0 - interrupt the processing)

   @ingroup gapfunc
   @internal
*/
template<typename T, typename Func, typename SIZE_TYPE>
int for_each_gap_blk_range(const T* BMRESTRICT buf,
                            SIZE_TYPE offset,
                            unsigned left, unsigned right,
                            Func&  bit_functor)
{
    BM_ASSERT(left <= right);
    BM_ASSERT(right < bm::bits_in_block);

    unsigned is_set;
    unsigned start_pos = bm::gap_bfind(buf, left, &is_set);
    const T* BMRESTRICT pcurr = buf + start_pos;
    int ret = 0;
    if (is_set)
    {
        if (right <= *pcurr)
        {
            ret = bit_functor.add_range(offset + left, (right + 1)-left);
            return ret;
        }
        ret = bit_functor.add_range(offset + left, (*pcurr + 1)-left);
        if (ret < 0)
            return ret;
        ++pcurr;
    }

    const T* BMRESTRICT pend = buf + (*buf >> 3);
    for (++pcurr; pcurr <= pend; pcurr += 2)
    {
        T prev = *(pcurr-1);
        if (right <= *pcurr)
        {
            int sz = int(right) - int(prev);
            if (sz > 0)
                ret = bit_functor.add_range(offset + prev + 1, unsigned(sz));
            return ret;
        }
        ret = bit_functor.add_range(offset + prev + 1, *pcurr - prev);
        if (ret < 0)
            return ret;
    } // for
    return 0;
}



/*! For each non-zero block in [from, to] executes supplied functor
    \internal
*/
template<typename T, typename N, typename F>
int for_each_bit_block_range(T*** root,
                              N top_size, N nb_from, N nb_to, F& f)
{
    BM_ASSERT(top_size);
    if (nb_from > nb_to)
        return 0;
    unsigned i_from = unsigned(nb_from >> bm::set_array_shift);
    unsigned j_from = unsigned(nb_from &  bm::set_array_mask);
    unsigned i_to = unsigned(nb_to >> bm::set_array_shift);
    unsigned j_to = unsigned(nb_to &  bm::set_array_mask);

    if (i_from >= top_size)
        return 0;
    if (i_to >= top_size)
    {
        i_to = unsigned(top_size-1);
        j_to = bm::set_sub_array_size-1;
    }
    int ret;
    for (unsigned i = i_from; i <= i_to; ++i)
    {
        T** blk_blk = root[i];
        if (!blk_blk)
            continue;
        if ((bm::word_t*)blk_blk == FULL_BLOCK_FAKE_ADDR)
        {
            unsigned j = (i == i_from) ? j_from : 0;
            if (!j && (i != i_to)) // full sub-block
            {
                N base_idx = bm::get_super_block_start<N>(i);
                ret = f.add_range(base_idx, bm::set_sub_total_bits);
                if (ret < 0)
                    return ret;
            }
            else
            {
                do
                {
                    N base_idx = bm::get_block_start<N>(i, j);
                    ret = f.add_range(base_idx, bm::gap_max_bits);
                    if (ret < 0)
                        return ret;
                    if ((i == i_to) && (j == j_to))
                        return 0;
                } while (++j < bm::set_sub_array_size);
            }
        }
        else
        {
            unsigned j = (i == i_from) ? j_from : 0;
            do
            {
                const T* block;
                if (blk_blk[j])
                {
                    N base_idx = bm::get_block_start<N>(i, j);
                    if (0 != (block = blk_blk[j]))
                    {
                        if (BM_IS_GAP(block))
                            ret = bm::for_each_gap_blk(BMGAP_PTR(block), base_idx, f);
                        else
                            ret = bm::for_each_bit_blk(block, base_idx, f);
                        if (ret < 0)
                            return ret;
                    }
                }

                if ((i == i_to) && (j == j_to))
                    return 0;
            } while (++j < bm::set_sub_array_size);
        }
    } // for i
    return 0;
}


/**
    Implementation of for_each_bit_range without boilerplave checks
    @internal
*/
template<class BV, class Func>
int for_each_bit_range_no_check(const BV&             bv,
                       typename BV::size_type left,
                       typename BV::size_type right,
                       Func&                  bit_functor)
{
    typedef typename BV::size_type      size_type;
    typedef typename BV::block_idx_type block_idx_type;

    const typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    bm::word_t*** blk_root = bman.top_blocks_root();
    if (!blk_root)
        return 0;
        
    block_idx_type nblock_left  = (left  >> bm::set_block_shift);
    block_idx_type nblock_right = (right >> bm::set_block_shift);

    unsigned i0, j0;
    bm::get_block_coord(nblock_left, i0, j0);
    const bm::word_t* block = bman.get_block_ptr(i0, j0);
    unsigned nbit_left  = unsigned(left  & bm::set_block_mask);
    size_type offset = nblock_left * bm::bits_in_block;
    int ret = 0;
    if (nblock_left == nblock_right) // hit in the same block
    {
        if (!block)
            return ret;
        unsigned nbit_right = unsigned(right & bm::set_block_mask);
        if (BM_IS_GAP(block))
        {
            ret = bm::for_each_gap_blk_range(BMGAP_PTR(block), offset,
                                             nbit_left, nbit_right, bit_functor);
        }
        else
        {
            ret = bm::for_each_bit_blk(block, offset, nbit_left, nbit_right,
                                 bit_functor);
        }
        return ret;
    }
    // process left block
    if (nbit_left && block)
    {
        if (BM_IS_GAP(block))
        {
            ret = bm::for_each_gap_blk_range(BMGAP_PTR(block), offset,
                                nbit_left, bm::bits_in_block-1, bit_functor);
        }
        else
        {
            ret = bm::for_each_bit_blk(block, offset, nbit_left, bm::bits_in_block-1,
                                 bit_functor);
        }
        if (ret < 0)
            return ret;
        ++nblock_left;
    }

    // process all complete blocks in the middle
    {
        block_idx_type top_blocks_size = bman.top_block_size();
        ret = bm::for_each_bit_block_range(blk_root, top_blocks_size,
                                nblock_left, nblock_right-1, bit_functor);
        if (ret < 0)
            return ret;
    }

    unsigned nbit_right = unsigned(right & bm::set_block_mask);
    bm::get_block_coord(nblock_right, i0, j0);
    block = bman.get_block_ptr(i0, j0);

    if (block)
    {
        offset = nblock_right * bm::bits_in_block;
        if (BM_IS_GAP(block))
        {
            ret = bm::for_each_gap_blk_range(BMGAP_PTR(block), offset,
                                       0, nbit_right, bit_functor);
        }
        else
        {
            ret = bm::for_each_bit_blk(block, offset, 0, nbit_right, bit_functor);
        }
    }
    return ret;
}

/**
    convert sub-blocks to an array of set 1s (32-bit)
    @internal
 */
template<typename BV, typename VECT>
void convert_sub_to_arr(const BV& bv, unsigned sb, VECT& vect)
{
    vect.resize(0);

    typename BV::size_type from, to, idx;
    typename BV::size_type sb_max_bc = bm::set_sub_array_size * bm::gap_max_bits;
    from = sb * sb_max_bc;
    to = (sb+1) * sb_max_bc;
    if (!to || to > bm::id_max) // overflow check
        to = bm::id_max;

    typename BV::enumerator en = bv.get_enumerator(from);
    for (; en.valid(); ++en)
    {
        idx = *en;
        if (idx >= to)
            break;
        idx -= from;
        vect.push_back((typename VECT::value_type)idx);
    } // for en
}


} // namespace bm

#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif
