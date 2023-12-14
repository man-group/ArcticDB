#ifndef BMSIMD__H__INCLUDED__
#define BMSIMD__H__INCLUDED__
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
/*! \file bmsimd.h
    \brief SIMD target version definitions
*/

#ifdef BMNEONOPT
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#include "sse2neon.h"
#pragma GCC diagnostic pop
#define BMSSE2OPT
#endif


#ifdef BMWASMSIMDOPT
#include <wasm_simd128.h>
#define BMSSE42OPT
#endif

#ifdef BMAVX512OPT
# undef BMAVX2OPT
# undef BMSSE42OPT
# undef BMSSE2OPT
# define BMVECTOPT
# include "bmavx512.h"
#endif


#ifdef BMAVX2OPT
# undef BMSSE42OPT
# undef BMSSE2OPT
# define BMVECTOPT
# include "bmavx2.h"
#endif


#ifdef BMSSE42OPT
# define BMVECTOPT
# include "bmsse4.h"
#endif

#ifdef BMSSE2OPT
# undef BM64OPT
# define BMVECTOPT
# include "bmsse2.h"
#endif

namespace bm
{

/**
    @brief return SIMD optimization used for building BitMagic
    @return SIMD code
 
    @ingroup bmagic
*/
inline int simd_version()
{
#if defined(BMWASMSIMDOPT)
    return bm::simd_wasm128;
#elif defined(BMNEONOPT)
    return bm::simd_neon;
#elif defined(BMAVX512OPT)
    return bm::simd_avx512;
#elif defined(BMAVX2OPT)
    return bm::simd_avx2;
#elif defined(BMSSE42OPT)
    return bm::simd_sse42;
#elif defined(BMSSE2OPT)
    return bm::simd_sse2;
#else
    return bm::simd_none;
#endif
}


} // namespace

#endif
