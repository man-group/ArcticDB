#ifndef BMFWD__H__INCLUDED__
#define BMFWD__H__INCLUDED__
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

/*! \file bmfwd.h
    \brief Forward declarations
*/

#include <stddef.h>
#include "bmconst.h"

namespace bm
{

class block_allocator;
class ptr_allocator;

template<class BA, class PA> class alloc_pool;
typedef bm::alloc_pool<block_allocator, ptr_allocator> standard_alloc_pool;

template<class BA = block_allocator, class PA = ptr_allocator, class APool = standard_alloc_pool > class mem_alloc;

template <class A, size_t N> class miniset;
template<size_t N> class bvmini;

typedef bm::mem_alloc<block_allocator, ptr_allocator, standard_alloc_pool> standard_allocator;

template<class A = bm::standard_allocator> class bvector;
template<class A = bm::standard_allocator> class rs_index;

template<typename Val, typename BVAlloc, bool trivial_type> class heap_vector;
template<typename Val, size_t ROWS, size_t COLS, typename BVAlloc> class heap_matrix;
template<typename Val, typename BVAlloc> class dynamic_heap_matrix;

template<typename BV> class aggregator;

template<typename BV> class rank_compressor;
template<typename BV> class basic_bmatrix;

template<typename BV, typename DEC> class deserializer;

template<class Val, class BV> class sparse_vector;
template<class Val, class SV> class rsc_sparse_vector;

template<class SVect, unsigned S_FACTOR = 16> class sparse_vector_scanner;
template<class SVect> class sparse_vector_serializer;
template<class SVect> class sparse_vector_deserializer;

struct block_waves_xor_descr;

} // namespace

#endif
