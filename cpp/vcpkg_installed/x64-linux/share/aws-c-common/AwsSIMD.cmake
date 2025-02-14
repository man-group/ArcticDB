# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

include(CheckCCompilerFlag)
include(CheckIncludeFile)

if (MSVC)
    set(AWS_AVX2_FLAG "/arch:AVX2")
    set(AWS_AVX512_FLAG "/arch:AVX512")
    set(AWS_AVX512vL_FLAG "")
    set(AWS_CLMUL_FLAG "")
    set(AWS_SSE4_2_FLAG "")
    set(AWS_ARMv8_1_FLAG "/arch:arm8.1")
    set(WERROR_FLAG "")
else()
    set(AWS_AVX2_FLAG "-mavx -mavx2")
    set(AWS_AVX512_FLAG "-mavx512f -mvpclmulqdq")
    set(AWS_AVX512vL_FLAG "-mavx512vl")
    set(AWS_CLMUL_FLAG "-mpclmul")
    set(AWS_SSE4_2_FLAG "-msse4.2")

    check_c_compiler_flag("-mtune=neoverse-v1" HAVE_MTUNE_NEOVERSE_V1)
    if (HAVE_MTUNE_NEOVERSE_V1)
        set(AWS_ARMv8_1_FLAG "-march=armv8-a+crc+crypto -mtune=neoverse-v1")
    else()
        set(AWS_ARMv8_1_FLAG "-march=armv8-a+crc+crypto")
    endif()
    set(WERROR_FLAG "-Werror")
endif()

if (USE_CPU_EXTENSIONS)
    set(AVX_CFLAGS ${AWS_SSE4_2_FLAG})

    check_c_compiler_flag(${AWS_AVX2_FLAG} HAVE_M_AVX2_FLAG)
    if (HAVE_M_AVX2_FLAG)
        set(AVX_CFLAGS "${AWS_AVX2_FLAG} ${AVX_CFLAGS}")
    endif()

    check_c_compiler_flag("${AWS_AVX512_FLAG} ${AWS_CLMUL_FLAG}" HAVE_M_AVX512_FLAG)
    if (HAVE_M_AVX512_FLAG)
        set(AVX_CFLAGS "${AWS_AVX512_FLAG} ${AWS_CLMUL_FLAG} ${AVX_CFLAGS}")
    endif()

    set(old_flags "${CMAKE_REQUIRED_FLAGS}")
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${AVX_CFLAGS} ${WERROR_FLAG}")

    check_c_source_compiles("
        #include <immintrin.h>
        #include <emmintrin.h>
        #include <string.h>

        int main() {
            __m256i vec;
            memset(&vec, 0, sizeof(vec));

            _mm256_shuffle_epi8(vec, vec);
            _mm256_set_epi32(1,2,3,4,5,6,7,8);
            _mm256_permutevar8x32_epi32(vec, vec);

            return 0;
        }"  AWS_HAVE_AVX2_INTRINSICS)          

    check_c_source_compiles("
        #include <immintrin.h>

        int main() {
            __m512 a = _mm512_setzero_ps();
            return 0;
        }" AWS_HAVE_AVX512_INTRINSICS)

    check_c_source_compiles("
        #include <immintrin.h>
        #include <string.h>

        int main() {
            __m256i vec;
            memset(&vec, 0, sizeof(vec));
            return (int)_mm256_extract_epi64(vec, 2);
        }" AWS_HAVE_MM256_EXTRACT_EPI64)

    check_c_source_compiles("
        #include <wmmintrin.h>
        #include <emmintrin.h>
        int main() {
            __m128i a = _mm_setzero_si128();
            __m128i b = _mm_setzero_si128();
            __m128i result = _mm_clmulepi64_si128(a, b, 0x00);
            (void)result;
            return 0;
        }" AWS_HAVE_CLMUL)

    set(CMAKE_REQUIRED_FLAGS "${old_flags} ${AWS_ARMv8_1_FLAG} ${WERROR_FLAG}")
    check_c_source_compiles("
            #include <arm_acle.h>
            int main() {
                int crc = __crc32d(0, 1);
                return 0;
            }" AWS_HAVE_ARM32_CRC)

    check_c_source_compiles("
        #include <stdatomic.h>
        int main() {
            _Atomic int var = 0;
            atomic_fetch_add_explicit(&var, 1, memory_order_relaxed);
            return 0;
    }" AWS_HAVE_ARMv8_1)

    set(CMAKE_REQUIRED_FLAGS "${old_flags}")

endif() # USE_CPU_EXTENSIONS

# The part where the definition is added to the compiler flags has been moved to config.h.in
# see git history for more details.

# Adds AVX flags, if any, that are supported. These files will be built with
# available avx intrinsics enabled.
# Usage: simd_add_source_avx(target file1.c file2.c ...)
function(simd_add_source_avx target)
    foreach(file ${ARGN})
        target_sources(${target} PRIVATE ${file})
        set_source_files_properties(${file} PROPERTIES COMPILE_FLAGS " ${AVX_CFLAGS}")
    endforeach()
endfunction(simd_add_source_avx)

# The part where the definition is added to the compiler flags has been moved to config.h.in
# see git history for more details.

# Adds compiler flags to the source and adds the source to target.
# Unfortunately the flags have to be passed as strings. Predefined flags are
# at the top of this file.
# Usage: simd_append_source_and_features(target file1.c ${AWS_AVX512_FLAG} ${AWS_AVX2_FLAG} ...)
function(simd_append_source_and_features target file)
    set(CC_FLAGS "")
    foreach(flag ${ARGN})
        set(CC_FLAGS "${CC_FLAGS} ${flag}")
    endforeach()

    target_sources(${target} PRIVATE ${file})
    set_source_files_properties(${file} PROPERTIES COMPILE_FLAGS " ${CC_FLAGS}")
endfunction(simd_append_source_and_features)
