#ifndef BMBMI2__H__INCLUDED__
#define BMBMI2__H__INCLUDED__
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

#include "bmdef.h"
#include "bmconst.h"

#include "bmbmi1.h"


namespace bm
{

#if defined(__INTEL_COMPILER) || defined(_MSC_VER)

#define BMBMI2OPT

inline
unsigned bmi2_select64_pdep(bm::id64_t val, unsigned rank)
{
    bm::id64_t i = 1ull << (rank-1);
    bm::id64_t d = _pdep_u64(i, val);
    i = _tzcnt_u64(d);

    return unsigned(i);
}

#define BMI2_SELECT64 bmi2_select64_pdep

#else // Intel and MSVC

#ifdef __GNUG__

#define BMBMI2OPT

inline
unsigned bmi2_select64_pdep(bm::id64_t val, unsigned rank)
{
    bm::id64_t i = 1ull << (rank-1);
    
    asm("pdep %[val], %[mask], %[val]"
            : [val] "+r" (val)
            : [mask] "r" (i));
    asm("tzcnt %[bit], %[index]"
            : [index] "=r" (i)
            : [bit] "g" (val)
            : "cc");
    
    return unsigned(i);
}

#define BMI2_SELECT64 bmi2_select64_pdep

#endif  // __GNUG__

#endif // compilers


} // bm

#endif
