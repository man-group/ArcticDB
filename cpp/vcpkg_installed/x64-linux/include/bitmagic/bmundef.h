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

/*! \file bmundef.h
    \brief pre-processor un-defines to avoid global space pollution (internal)
*/


#undef BMRESTRICT
#undef BMFORCEINLINE
#undef BMGAP_PTR
#undef BMSET_PTRGAP
#undef BM_IS_GAP
#undef BMPTR_SETBIT0
#undef BMPTR_CLEARBIT0
#undef BMPTR_TESTBIT0
#undef BM_SET_MMX_GUARD
#undef BM_SER_NEXT_GRP
#undef BM_SET_ONE_BLOCKS
#undef DECLARE_TEMP_BLOCK
#undef BM_MM_EMPTY
#undef BM_ASSERT
#undef FULL_BLOCK_ADDR
#undef IS_VALID_ADDR
#undef IS_FULL_BLOCK
#undef IS_EMPTY_BLOCK
#undef BM_INCWORD_BITCOUNT
#undef BM_MINISET_GAPLEN
#undef BM_MINISET_ARRSIZE
#undef BM_FALLTHROUGH

#undef BMVECTOPT
#undef VECT_XOR_ARR_2_MASK
#undef VECT_ANDNOT_ARR_2_MASK

#undef VECT_BITCOUNT
#undef VECT_BIT_COUNT_DIGEST

#undef VECT_BITCOUNT_AND
#undef VECT_BITCOUNT_OR
#undef VECT_BITCOUNT_XOR
#undef VECT_BITCOUNT_SUB
#undef VECT_INVERT_ARR
#undef VECT_AND_ARR
#undef VECT_OR_ARR
#undef VECT_OR_BLOCK_2WAY
#undef VECT_OR_BLOCK_3WAY
#undef VECT_OR_BLOCK_5WAY
#undef VECT_SUB_ARR
#undef VECT_XOR_BLOCK
#undef VECT_XOR_BLOCK_2WAY

#undef VECT_COPY_BLOCK
#undef VECT_SET_BLOCK
#undef VECT_IS_ZERO_BLOCK
#undef VECT_IS_ONE_BLOCK

#undef VECT_LOWER_BOUND_SCAN_U32
#undef VECT_SHIFT_R1
#undef VECT_SHIFT_R1_AND

#undef VECT_ARR_BLOCK_LOOKUP
#undef VECT_SET_BLOCK_BITS

#undef VECT_BLOCK_CHANGE
#undef VECT_BLOCK_CHANGE_BC

#undef VECT_BIT_TO_GAP

#undef VECT_AND_DIGEST
#undef VECT_AND_DIGEST_2WAY
#undef VECT_AND_OR_DIGEST_2WAY
#undef VECT_AND_DIGEST_5WAY
#undef VECT_AND_DIGEST_3WAY
#undef VECT_BLOCK_SET_DIGEST

#undef VECT_SUB_DIGEST
#undef VECT_SUB_DIGEST_5WAY
#undef VECT_SUB_DIGEST_2WAY
#undef VECT_SUB_BLOCK

#undef VECT_BLOCK_XOR_CHANGE
#undef VECT_BIT_BLOCK_XOR

#undef VECT_BIT_FIND_FIRST
#undef VECT_BIT_FIND_DIFF
#undef VECT_BIT_FIND_FIRST_IF_1

#undef VECT_GAP_BFIND
#undef VECT_GAP_TEST

#undef BMI1_SELECT64
#undef BMI2_SELECT64

#undef VECT_COPY_BLOCK_UNALIGN
#undef VECT_COPY_BLOCK
#undef VECT_STREAM_BLOCK_UNALIGN
#undef VECT_STREAM_BLOCK


#undef BM_UNALIGNED_ACCESS_OK
#undef BM_x86

#undef BM_ALLOC_ALIGN

