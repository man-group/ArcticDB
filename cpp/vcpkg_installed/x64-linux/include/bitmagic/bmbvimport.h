#ifndef BMBVIMPORT__H__INCLUDED__
#define BMBVIMPORT__H__INCLUDED__
/*
Copyright(c) 2002-2021 Anatoliy Kuznetsov(anatoliy_kuznetsov at yahoo.com)

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
/*! \file bmbvimport.h
    \brief Import of bvector<> from native type bit-arrays
*/

#ifndef BM__H__INCLUDED__
// BitMagic utility headers do not include main "bm.h" declaration
// #include "bm.h" or "bm64.h" explicitly
# error missing include (bm.h or bm64.h)
#endif

/** \defgroup bvimport Import bvector<> from native bit-arrays
    \ingroup bvector
 */
namespace bm
{

/**
    Import native stream of bits (represented as 32-bit unsigned ints)
    @param bv [out] - target bvector
    @param bit_arr [in] - source array
    @param bit_arr_size [in] - source array size in words (NOT in bits or bytes)
    @param optimize [in] - flag to optimize/compress target bvector on the fly

    @ingroup bvimport
 */
template<class BV>
void bit_import_u32(BV& bv,
                   const unsigned int* BMRESTRICT bit_arr,
                   typename BV::size_type         bit_arr_size,
                   bool                           optimize)
{
    BM_ASSERT(bit_arr);

    bv.clear(true);
    if (!bit_arr_size)
        return;

    using block_idx_type = typename BV::block_idx_type;
    using bv_size_type = typename BV::size_type;

    block_idx_type total_blocks = bit_arr_size / bm::set_block_size;
    unsigned top_blocks = unsigned(total_blocks / bm::set_sub_array_size) + 1;
    BM_ASSERT(top_blocks);

    typename BV::blocks_manager_type& bman = bv.get_blocks_manager();
    bman.reserve_top_blocks(top_blocks);

    for (unsigned i = 0; i < top_blocks; ++i)
        bman.check_alloc_top_subblock(i);

    typename BV::block_idx_type nb = 0;
    for (; nb < total_blocks; ++nb)
    {
        bm::word_t* block = bman.borrow_tempblock();
        bman.set_block(nb, block);
        const unsigned int* BMRESTRICT bit_arr_block_ptr =
                                        &bit_arr[nb*bm::set_block_size];
        bm::bit_block_copy_unalign(block, bit_arr_block_ptr);
        if (optimize)
        {
            unsigned i0, j0;
            bm::get_block_coord(nb, i0, j0);
            bman.optimize_bit_block(i0, j0, BV::opt_compress); // returns tem_block if needed
        }
    } // for nb

    // tail processing
    //
    typename BV::size_type tail_idx = nb * bm::set_block_size;
    if (tail_idx < bit_arr_size)
    {
        BM_ASSERT(bit_arr_size - tail_idx < bm::set_block_size);
        bm::word_t* block = bman.borrow_tempblock();
        bman.set_block(nb, block);
        bv_size_type i = tail_idx;
        unsigned k = 0;
        while (i < bit_arr_size) // copy the array's tail
            block[k++] = bit_arr[i++];
        while (k < bm::set_block_size) // zero the block's tail
            block[k++] = 0;
        if (optimize)
        {
            unsigned i0, j0;
            bm::get_block_coord(nb, i0, j0);
            bman.optimize_bit_block(i0, j0, BV::opt_compress); // returns tem_block if needed
        }
    }
    if (optimize)
        bman.free_temp_block();
}

} // namespace bm


#endif
