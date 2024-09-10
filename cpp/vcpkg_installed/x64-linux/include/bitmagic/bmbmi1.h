#ifndef BMBMI1__H__INCLUDED__
#define BMBMI1__H__INCLUDED__
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

/*! \file bmbmi1,h
    \brief Intel BMI1 Bit manipulation primitives (internal)
*/

#include <immintrin.h>
#include "bmdef.h"
#include "bmconst.h"

#define BMBMI1OPT

namespace bm
{

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param val - 64-bit work to search
    \param rank - rank to select (should be > 0)
 
    \return selected value (inxed of bit set)
*/
inline
unsigned bmi1_select64_lz(bm::id64_t w, unsigned rank)
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= _mm_popcnt_u64(w));
    
    unsigned bc, lz;
    unsigned val_lo32 = w & 0xFFFFFFFFull;
    
    unsigned bc_lo32 = unsigned(_mm_popcnt_u64(val_lo32));
    if (rank <= bc_lo32)
    {
        unsigned val_lo16 = w & 0xFFFFull;
        unsigned bc_lo16 = unsigned(_mm_popcnt_u64(val_lo16));
        w = (rank <= bc_lo16) ? val_lo16 : val_lo32;
    }
    bc = unsigned(_mm_popcnt_u64(w));

    unsigned diff = bc - rank;
    for (; diff; --diff)
    {
        lz = unsigned(_lzcnt_u64(w));
        w &= ~(1ull << (63 - lz));
    }
    BM_ASSERT(unsigned(_mm_popcnt_u64(w)) == rank);
    lz = unsigned(_lzcnt_u64(w));
    lz = 63 - lz;
    return lz;
}

/**
    \brief word find index of the rank-th bit set by bit-testing
    \param w - 64-bit work to search
    \param rank - rank to select (should be > 0)
 
    \return selected value (inxed of bit set)
*/
inline
unsigned bmi1_select64_tz(bm::id64_t w, unsigned rank)
{
    BM_ASSERT(w);
    BM_ASSERT(rank);
    BM_ASSERT(rank <= _mm_popcnt_u64(w));

    do
    {
        if ((--rank) == 0)
            break;
        w = _blsr_u64(w); // w &= w - 1;
    } while (1);
    bm::id64_t t = _blsi_u64(w); //w & -w;
    unsigned count = unsigned(_tzcnt_u64(t));
    return count;
}


#define BMI1_SELECT64 bmi1_select64_tz

} // bm

#endif
