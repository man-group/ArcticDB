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

/*! \file bmdef.h
    \brief Definitions(internal)
*/

#include <climits>
#include <stdint.h>

// Incorporate appropriate tuneups when the NCBI C++ Toolkit's core
// headers have been included.
//
#ifdef NCBI_ASSERT
#  define BM_ASSERT _ASSERT

#  ifdef HAVE_RESTRICT_CXX
#    define BM_HASRESTRICT
#    define BMRESTRICT NCBI_RESTRICT
#  endif

#  if defined(NCBI_FORCEINLINE)  &&  \
    ( !defined(NCBI_COMPILER_GCC)  ||  NCBI_COMPILER_VERSION >= 400  || \
      defined(__OPTIMIZE__))
#    define BM_HASFORCEINLINE
#    define BMFORCEINLINE NCBI_FORCEINLINE
#  endif

#  ifdef NCBI_SSE
#    if NCBI_SSE >= 20
#      define BMSSE2OPT 1
#    endif
#    if NCBI_SSE >= 40
#      define BMSSE2OPT 1
#    endif
#    if NCBI_SSE >= 42
#      define BMSSE42OPT 1
#    endif
#  endif
#endif


// macro to define/undefine unaligned memory access (x86, PowerPC)
//
#if defined(__i386) || defined(__x86_64) || defined(__ppc__) || \
    defined(__ppc64__) || defined(_M_IX86) || defined(_M_AMD64) || \
    defined(_M_IX86) || defined(_M_AMD64) || defined(_M_X64) || \
    defined(_M_ARM) || defined(_M_ARM64) || \
    defined(__arm__) || defined(__aarch64__) || \
    (defined(_M_MPPC) && !defined(BM_FORBID_UNALIGNED_ACCESS))
#define BM_UNALIGNED_ACCESS_OK 1
#endif

#if defined(_M_IX86) || defined(_M_AMD64) || defined(_M_X64) || \
    defined(__i386) || defined(__x86_64) || defined(_M_AMD64) || \
    defined(BMSSE2OPT) || defined(BMSSE42OPT)
#define BM_x86
#endif

// cxx11 features
//
#if defined(BM_NO_CXX11) || (defined(_MSC_VER)  &&  _MSC_VER < 1900)
# define BMNOEXCEPT
# define BMNOEXCEPT2
#else
# ifndef BMNOEXCEPT
#  define BMNOEXCEPT noexcept
#if defined(__EMSCRIPTEN__)
#else
#  define BMNOEXCEPT2
#endif
# endif
#endif

// WebASM compilation settings
//
#if defined(__EMSCRIPTEN__)

#undef BM_x86

// EMSCRIPTEN specific tweaks
// WebAssemply compiles into 32-bit memory system but offers 64-bit wordsize
// WebASM also benefits from use GCC extensions (buildins like popcnt, lzcnt)
//
// BMNOEXCEPT2 is to declare "noexcept" for WebAsm only where needed
// and silence GCC warnings
//
# define BM64OPT
# define BM_USE_GCC_BUILD
# define BMNOEXCEPT2 noexcept

#else
#  define BMNOEXCEPT2
#endif


// Enable MSVC 8.0 (2005) specific optimization options
//
#if(_MSC_VER >= 1400)
#  define BM_HASFORCEINLINE
#  ifndef BMRESTRICT
#    define BMRESTRICT __restrict
#  endif
#endif 

#ifdef __GNUG__
#  ifndef BMRESTRICT
#    define BMRESTRICT __restrict__
#  endif

#  ifdef __OPTIMIZE__
#    define BM_NOASSERT
#  endif
#endif

# ifdef NDEBUG
#    define BM_NOASSERT
# endif


#ifndef BM_ASSERT
# ifndef BM_NOASSERT
#  include <cassert>
#  define BM_ASSERT assert
# else
#  ifndef BM_ASSERT
#    define BM_ASSERT(x)
#  endif
# endif
#endif


#if defined(__x86_64) || defined(_M_AMD64) || defined(_WIN64) || \
    defined(__LP64__) || defined(_LP64) || ( __WORDSIZE == 64 )
#ifndef BM64OPT
# define BM64OPT
#endif
#endif



#define FULL_BLOCK_REAL_ADDR bm::all_set<true>::_block._p
#define FULL_BLOCK_FAKE_ADDR bm::all_set<true>::_block._p_fullp
#define FULL_SUB_BLOCK_REAL_ADDR bm::all_set<true>::_block._s
#define BLOCK_ADDR_SAN(addr) (addr == FULL_BLOCK_FAKE_ADDR) ? FULL_BLOCK_REAL_ADDR : addr
#define IS_VALID_ADDR(addr) bm::all_set<true>::is_valid_block_addr(addr)
#define IS_FULL_BLOCK(addr) bm::all_set<true>::is_full_block(addr)
#define IS_EMPTY_BLOCK(addr) bool(addr == 0)

#define BM_BLOCK_TYPE(addr) bm::all_set<true>::block_type(addr)

// Macro definitions to manipulate bits in pointers
// This trick is based on the fact that pointers allocated by malloc are
// aligned and bit 0 is never set. It means we are safe to use it.
// BM library keeps GAP flag in pointer.


// TODO: consider UINTPTR_MAX == 0xFFFFFFFF
//
# if ULONG_MAX != 0xffffffff || defined(_WIN64)  // 64-bit

#  define BMPTR_SETBIT0(ptr)   ( ((bm::id64_t)ptr) | 1 )
#  define BMPTR_CLEARBIT0(ptr) ( ((bm::id64_t)ptr) & ~(bm::id64_t)1 )
#  define BMPTR_TESTBIT0(ptr)  ( ((bm::id64_t)ptr) & 1 )

# else // 32-bit

#  define BMPTR_SETBIT0(ptr)   ( ((bm::id_t)ptr) | 1 )
#  define BMPTR_CLEARBIT0(ptr) ( ((bm::id_t)ptr) & ~(bm::id_t)1 )
#  define BMPTR_TESTBIT0(ptr)  ( ((bm::id_t)ptr) & 1 )

# endif

# define BMGAP_PTR(ptr) ((bm::gap_word_t*)BMPTR_CLEARBIT0(ptr))
# define BMSET_PTRGAP(ptr) ptr = (bm::word_t*)BMPTR_SETBIT0(ptr)
# define BM_IS_GAP(ptr) (BMPTR_TESTBIT0(ptr))





#ifdef BM_HASRESTRICT
# ifndef BMRESTRICT
#  define BMRESTRICT restrict
# endif
#else
# ifndef BMRESTRICT
#   define BMRESTRICT
# endif
#endif

#ifndef BMFORCEINLINE
#ifdef BM_HASFORCEINLINE
# ifndef BMFORCEINLINE
#  define BMFORCEINLINE __forceinline
# endif
#else
# define BMFORCEINLINE inline
#endif
#endif


// --------------------------------
// SSE optmization macros
//

#ifdef BMSSE42OPT
# if defined(BM64OPT) || defined(__x86_64) || defined(_M_AMD64) || defined(_WIN64) || \
    defined(__LP64__) || defined(_LP64) || ( __WORDSIZE == 64 )
#   undef BM64OPT
#   define BM64_SSE4
# endif
# undef BMSSE2OPT
#endif

#ifdef BMAVX2OPT
# if defined(BM64OPT) || defined(__x86_64) || defined(_M_AMD64) || defined(_WIN64) || \
    defined(__LP64__) || defined(_LP64)
#   undef BM64OPT
#   undef BM64_SSE4
#   define BM64_AVX2
# endif
# undef BMSSE2OPT
# undef BMSSE42OPT
#endif

#ifdef BMAVX512OPT
# if defined(BM64OPT) || defined(__x86_64) || defined(_M_AMD64) || defined(_WIN64) || \
    defined(__LP64__) || defined(_LP64)
#   undef BM64OPT
#   undef BM64_SSE4
#   undef BM64_AVX2
#   define BM64_AVX512
# endif
# undef BMSSE2OPT
# undef BMSSE42OPT
#endif



# ifndef BM_SET_MMX_GUARD
#  define BM_SET_MMX_GUARD
# endif


#if (defined(BMSSE2OPT) || defined(BMSSE42OPT) || defined(BMAVX2OPT) || defined(BMAVX512OPT))

    # ifndef BM_SET_MMX_GUARD
    #  define BM_SET_MMX_GUARD  sse_empty_guard  bm_mmx_guard_;
    # endif

    #ifdef _MSC_VER

    #ifndef BM_ALIGN16
    #  define BM_ALIGN16 __declspec(align(16))
    #  define BM_ALIGN16ATTR
    #endif

    #ifndef BM_ALIGN32
    #  define BM_ALIGN32 __declspec(align(32))
    #  define BM_ALIGN32ATTR
    #endif

    #ifndef BM_ALIGN64
    #  define BM_ALIGN64 __declspec(align(64))
    #  define BM_ALIGN64ATTR
    #endif

    # else // GCC

    #ifndef BM_ALIGN16
    #  define BM_ALIGN16
    #  define BM_ALIGN16ATTR __attribute__((aligned(16)))
    #endif

    #ifndef BM_ALIGN32
    #  define BM_ALIGN32
    #  define BM_ALIGN32ATTR __attribute__((aligned(32)))
    #endif

    #ifndef BM_ALIGN64
    #  define BM_ALIGN64
    #  define BM_ALIGN64ATTR __attribute__((aligned(64)))
    #endif
    #endif

#else 

    #define BM_ALIGN16 
    #define BM_ALIGN16ATTR
    #define BM_ALIGN32
    #define BM_ALIGN32ATTR
    #define BM_ALIGN64
    #define BM_ALIGN64ATTR

#endif



#if (defined(BMSSE2OPT) || defined(BMSSE42OPT) || defined(BMWASMSIMDOPT) || defined(BMNEONOPT))
#   define BM_VECT_ALIGN BM_ALIGN16
#   define BM_VECT_ALIGN_ATTR BM_ALIGN16ATTR
#else
#   if defined(BMAVX2OPT)
#       define BM_VECT_ALIGN BM_ALIGN32
#       define BM_VECT_ALIGN_ATTR BM_ALIGN32ATTR
#   else
#       if defined(BMAVX512OPT)
#          define BM_VECT_ALIGN BM_ALIGN64
#          define BM_VECT_ALIGN_ATTR BM_ALIGN64ATTR
#       else
#          define BM_VECT_ALIGN
#          define BM_VECT_ALIGN_ATTR
#       endif
#   endif
#endif



// throw redefinintion for compatibility with language wrappers
//
#ifndef BM_ASSERT_THROW
#define BM_ASSERT_THROW(x, xerrcode)
#endif


#ifndef __has_cpp_attribute
#  define __has_cpp_attribute(x) 0
#endif
#ifndef __has_attribute
#  define __has_attribute(x) 0
#endif
#if __has_cpp_attribute(fallthrough)  &&  \
    (!defined(__clang__)  ||  (__clang_major__ > 7 && __cplusplus >= 201703L))
#  define BM_FALLTHROUGH [[fallthrough]]
#elif __has_cpp_attribute(gcc::fallthrough)
#  define BM_FALLTHROUGH [[gcc::fallthrough]]
#elif __has_cpp_attribute(clang::fallthrough)
#  define BM_FALLTHROUGH [[clang::fallthrough]]
#elif __has_attribute(fallthrough)
#  define BM_FALLTHROUGH __attribute__ ((fallthrough))
#else
#  define BM_FALLTHROUGH
#endif




