#ifndef BM3VL__H__INCLUDED__
#define BM3VL__H__INCLUDED__
/*
Copyright(c) 2021 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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

/*! \file bm3vl.h
    \brief Three-valued logic (3VL) operations
*/

#include "bm.h"

namespace bm
{

/** @defgroup bm3VL three-valued logic
    Functions for three-valued logic (Kleene)
    https://en.wikipedia.org/wiki/Three-valued_logic
    @ingroup bvector
 */


/**
    @brief Initialized the value bit-vector so that it always returns 0 (false)
    for the unknown.

    3-value logics in BitMagic is built on two bit-vectors:
    value-bit-vector and NULL bit-vector. Value vector contains '1s' for the
    true known elements. bm::init_3vl makes sure this is true by running
    bv_value = bv_value AND bv_null logical operation.
    NULL bit-vector represents NOT NULL (known) values as 1s

    @param bv_value - [in, out] values bit-vector
    @param bv_null -  [in] not NULL (known) bit-vector

    @ingroup bm3VL

 */
template<class BV>
void init_kleene(BV& bv_value, const BV& bv_null)
{
    bv_value.bit_and(bv_null);
}

/**
    @brief Return Kleene logic value based on value and known vectors

    @param bv_value - [in] values bit-vector
    @param bv_null - [in] knowns bit-vector
    @param idx     - [in] index of value to extract and return

    @return (−1: false; 0: unknown; +1: true)
    @ingroup bm3VL
*/
template<class BV>
int get_value_kleene(const BV& bv_value, const BV& bv_null,
                    typename BV::size_type idx) BMNOEXCEPT
{
    bool b_notnull = bv_null.test(idx);
    if (b_notnull)
    {
        // TODO: optimization may be needed to test 2 bvs in the same coord
        bool b_val = bv_value.test(idx);
        if (b_val)
            return 1;
        return -1;
    }
    return 0; // unknown
}

/**
    @brief Set Kleene logic value based on value and known vectors

    @param bv_value - [out] values bit-vector
    @param bv_null - [out] knowns bit-vector
    @param idx     - [in] index of value to extract and return
    @param val     - [in] value which can be: (−1: false; 0: unknown; +1: true)

    @ingroup bm3VL
*/
template<class BV>
void set_value_kleene(BV& bv_value, BV& bv_null,
                     typename BV::size_type idx, int val)
{
    bool is_set;
    switch (val)
    {
    case -1:
        bv_value.clear_bit(idx);
        bv_null.set(idx);
        break;
    case 0:
        is_set = bv_null.set_bit_and(idx, false);
        if (is_set)
            bv_value.clear_bit(idx);
        break;
    case 1:
        // TODO: optimization may be needed to set 2 bvs in the same coords
        bv_value.set(idx);
        bv_null.set(idx);
        break;
    default:
        BM_ASSERT(0); // unknown 3-value logic constant
    } // switch
}


/**
    @brief Kleene NEG operation

    True becomes false and vice verse, but unknowns remain unknown false
    if we look directly into bv_value vector. This oprtaion does NOT produce
    unknown true values.

    @param bv_value - [in, out] values bit-vector
    @param bv_null -  [in] not NULL (known) bit-vector

    @ingroup bm3VL
 */
template<class BV>
void invert_kleene(BV& bv_value, const BV& bv_null)
{
    bv_value.bit_xor(bv_null);
}

/**
    @brief Kleene OR(vect1, vect2) (vect1 |= vect2)
    1 OR Unk = 1 (known)
    @param bv_value1 - [in, out] values bit-vector
    @param bv_null1 -  [in, out] not NULL (known) bit-vector
    @param bv_value2 - [in] values bit-vector
    @param bv_null2 -  [in] not NULL (known) bit-vector

    @ingroup bm3VL
 */
template<class BV>
void or_kleene(BV& bv_value1, BV& bv_null1,
               const BV& bv_value2, const BV& bv_null2)
{
    BV bv_known_false1, bv_known_false2; // known but false
    bv_known_false1.bit_xor(bv_value1, bv_null1, BV::opt_none);
    bv_known_false2.bit_xor(bv_value2, bv_null2, BV::opt_none);

    bv_known_false1.bit_sub(bv_null2); // known false but unknown in 2
    bv_known_false2.bit_sub(bv_null1); // known false but unknown in 1

    bv_value1.bit_or(bv_value2);
    bv_null1.bit_or(bv_null2);

    bv_null1.bit_sub(bv_known_false1); // exclude FALSE-unknown combinations
    bv_null1.bit_sub(bv_known_false2);
}

/**
    @brief 3-way Kleene OR: target := OR(vect1, vect2) (target := vect1 | vect2)
    1 OR Unk = 1 (known)
    @param bv_value_target - [out] target values bit-vector
    @param bv_null_target -  [out] target not NULL (known) bit-vector
    @param bv_value1 - [in] values bit-vector
    @param bv_null1 -  [in] not NULL (known) bit-vector
    @param bv_value2 - [in] values bit-vector
    @param bv_null2 -  [in] not NULL (known) bit-vector

    @ingroup bm3VL
 */
template<class BV>
void or_kleene(BV& bv_value_target, BV& bv_null_target,
               const BV& bv_value1, const BV& bv_null1,
               const BV& bv_value2, const BV& bv_null2)
{
    BV bv_known_false1, bv_known_false2; // known but false
    bv_known_false1.bit_xor(bv_value1, bv_null1, BV::opt_none);
    bv_known_false2.bit_xor(bv_value2, bv_null2, BV::opt_none);

    bv_known_false1.bit_sub(bv_null2); // known false but unknown in 2
    bv_known_false2.bit_sub(bv_null1); // known false but unknown in 1

    bv_value_target.bit_or(bv_value1, bv_value2, BV::opt_none);
    bv_null_target.bit_or(bv_null1, bv_null2, BV::opt_none);

    // exclude FALSE-unknown combinations
    bv_null_target.bit_sub(bv_known_false1);
    bv_null_target.bit_sub(bv_known_false2);
}



/**
    @brief Kleene AND(vect1, vect2) (vect1 &= vect2)
    0 AND Unk = 0 (known)
    @param bv_value1 - [in, out] values bit-vector
    @param bv_null1 -  [in, out] not NULL (known) bit-vector
    @param bv_value2 - [in] values bit-vector
    @param bv_null2 -  [in] not NULL (known) bit-vector 

    @ingroup bm3VL
 */
template<class BV>
void and_kleene(BV& bv_value1, BV& bv_null1,
                const BV& bv_value2, const BV& bv_null2)
{
    BV bv_ambig_null1; // unknowns on just one of the two args
    bv_ambig_null1.bit_xor(bv_null1, bv_null2, BV::opt_none);

    BV bv_ambig_null2(bv_ambig_null1); // just a copy

    bv_ambig_null1.bit_and(bv_value1); // "unknowns 1"
    bv_ambig_null2.bit_and(bv_value2); // "unknowns 2"

    bv_value1.bit_and(bv_value2);

    bv_null1.bit_or(bv_null2); // merge all null2 knowns
    // minus all single-end TRUE unk
    bv_null1.bit_sub(bv_ambig_null1);
    bv_null1.bit_sub(bv_ambig_null2);
}

/**
    @brief 3-way Kleene target:=AND(vect1, vect2) (target:= vect1 & vect2)
    0 AND Unk = 0 (known)
    @param bv_value_target - [out] values bit-vector
    @param bv_null_target -  [out] not NULL (known) bit-vector
    @param bv_value1 - [in] values bit-vector
    @param bv_null1 -  [in] not NULL (known) bit-vector
    @param bv_value2 - [in] values bit-vector
    @param bv_null2 -  [in] not NULL (known) bit-vector

    @ingroup bm3VL
 */
template<class BV>
void and_kleene(BV& bv_value_target, BV& bv_null_target,
                const BV& bv_value1, const BV& bv_null1,
                const BV& bv_value2, const BV& bv_null2)
{
    BV bv_ambig_null1; // unknowns on just one of the two args
    bv_ambig_null1.bit_xor(bv_null1, bv_null2, BV::opt_none);

    BV bv_ambig_null2(bv_ambig_null1); // just a copy

    bv_ambig_null1.bit_and(bv_value1); // "unknowns 1"
    bv_ambig_null2.bit_and(bv_value2); // "unknowns 2"

    bv_value_target.bit_and(bv_value1, bv_value2, BV::opt_none);
    bv_null_target.bit_or(bv_null1, bv_null2, BV::opt_none);

    bv_null_target.bit_sub(bv_ambig_null1);
    bv_null_target.bit_sub(bv_ambig_null2);
}


/**
    Reference function for Kleene logic AND (for verification and testing)
    @ingroup bm3VL
    @internal
 */
inline
int and_values_kleene(int a, int b) BMNOEXCEPT
{
    switch (a)
    {
    case -1:
        return -1; // always false
    case 0:
        switch (b)
        {
        case -1: return -1;
        case 0:  return 0;
        case 1:  return 0;
        default:
            BM_ASSERT(0);
        }
        break;
    case 1:
        switch (b)
        {
        case -1: return -1;
        case 0:  return 0;
        case 1:  return 1;
        default:
            BM_ASSERT(0);
        }
        break;
    default:
        BM_ASSERT(0);
    }
    BM_ASSERT(0);
    return 0;
}


/**
    Reference function for Kleene logic OR (for verification and testing)
    @ingroup bm3VL
    @internal
 */
inline
int or_values_kleene(int a, int b) BMNOEXCEPT
{
    switch (a)
    {
    case -1:
        switch (b)
        {
        case -1: return -1;
        case 0:  return 0;
        case 1:  return 1;
        default:
            BM_ASSERT(0);
        }
        break;
    case 0:
        switch (b)
        {
        case -1: return 0;
        case 0:  return 0;
        case 1:  return 1;
        default:
            BM_ASSERT(0);
        }
        break;
    case 1:
        return 1;
    default:
        BM_ASSERT(0);
    }
    BM_ASSERT(0);
    return 0;
}


} // namespace bm

#endif
