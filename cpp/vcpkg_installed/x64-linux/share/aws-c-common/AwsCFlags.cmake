# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

include(CheckCCompilerFlag)
include(CheckIncludeFile)
include(CheckSymbolExists)
include(CMakeParseArguments) # needed for CMake v3.4 and lower

option(AWS_ENABLE_LTO "Enables LTO on libraries. Ensure this is set on all consumed targets, or linking will fail" OFF)
option(LEGACY_COMPILER_SUPPORT "This enables builds with compiler versions such as gcc 4.1.2. This is not a 'supported' feature; it's just a best effort." OFF)
option(AWS_SUPPORT_WIN7 "Restricts WINAPI calls to Win7 and older (This will have implications in downstream libraries that use TLS especially)" OFF)
option(AWS_WARNINGS_ARE_ERRORS "Compiler warning is treated as an error. Try turning this off when observing errors on a new or uncommon compiler" OFF)
option(AWS_ENABLE_TRACING "Enable tracing macros" OFF)
option(AWS_STATIC_MSVC_RUNTIME_LIBRARY "Windows-only. Turn ON to use the statically-linked MSVC runtime lib, instead of the DLL" OFF)
option(STATIC_CRT "Deprecated. Use AWS_STATIC_MSVC_RUNTIME_LIBRARY instead" OFF)

# Check for Posix Large Files Support (LFS).
# On most 64bit systems, LFS is enabled by default.
# On some 32bit systems, LFS must be enabled by via defines before headers are included.
# For more info, see docs:
# https://www.gnu.org/software/libc/manual/html_node/File-Position-Primitive.html
# https://www.gnu.org/software/libc/manual/html_node/Feature-Test-Macros.html
function(aws_check_posix_lfs extra_flags variable)
    list(APPEND CMAKE_REQUIRED_FLAGS ${extra_flags})
    check_c_source_compiles("
        #include <stdio.h>

        /* fails to compile if off_t smaller than 64bits */
        typedef char array[sizeof(off_t) >= 8 ? 1 : -1];

        int main() {
            return 0;
        }"
        HAS_64BIT_FILE_OFFSET_${variable})

    if (HAS_64BIT_FILE_OFFSET_${variable})
        # sometimes off_t is 64bit, but fseeko() is missing (ex: Android API < 24)
        check_symbol_exists(fseeko "stdio.h" HAS_FSEEKO_${variable})

        if (HAS_FSEEKO_${variable})
            set(${variable} 1 PARENT_SCOPE)
        endif()
    endif()
endfunction()

# This function will set all common flags on a target
# Options:
#  NO_WGNU: Disable -Wgnu
#  NO_WEXTRA: Disable -Wextra
#  NO_PEDANTIC: Disable -pedantic
function(aws_set_common_properties target)
    set(options NO_WGNU NO_WEXTRA NO_PEDANTIC NO_LTO)
    cmake_parse_arguments(SET_PROPERTIES "${options}" "" "" ${ARGN})

    if(MSVC)
        # Remove other /W flags
        if(CMAKE_C_FLAGS MATCHES "/W[0-4]")
            string(REGEX REPLACE "/W[0-4]" "" CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}" PARENT_SCOPE)
        endif()

        list(APPEND AWS_C_FLAGS /W4 /MP)

        if(AWS_WARNINGS_ARE_ERRORS)
            list(APPEND AWS_C_FLAGS /WX)
        endif()

        # /volatile:iso relaxes some implicit memory barriers that MSVC normally applies for volatile accesses
        # Since we want to be compatible with user builds using /volatile:iso, use it for the tests.
        list(APPEND AWS_C_FLAGS /volatile:iso)

        # disable non-constant initializer warning, it's not non-standard, just for Microsoft
        list(APPEND AWS_C_FLAGS /wd4204)
        # disable passing the address of a local warning. Again, not non-standard, just for Microsoft
        list(APPEND AWS_C_FLAGS /wd4221)

        if (AWS_SUPPORT_WIN7)
            # Use only APIs available in Win7 and later
            message(STATUS "Windows 7 support requested, forcing WINVER and _WIN32_WINNT to 0x0601")
            list(APPEND AWS_C_FLAGS /DWINVER=0x0601)
            list(APPEND AWS_C_FLAGS /D_WIN32_WINNT=0x0601)
            list(APPEND AWS_C_FLAGS /DAWS_SUPPORT_WIN7=1)
        endif()

    else()
        list(APPEND AWS_C_FLAGS -Wall -Wstrict-prototypes)

        list(APPEND AWS_C_FLAGS $<$<NOT:$<CONFIG:Release>>:-fno-omit-frame-pointer>)

        if(AWS_WARNINGS_ARE_ERRORS)
            list(APPEND AWS_C_FLAGS -Werror)
        endif()

        if(NOT SET_PROPERTIES_NO_WEXTRA)
            list(APPEND AWS_C_FLAGS -Wextra)
        endif()

        if(NOT SET_PROPERTIES_NO_PEDANTIC)
            list(APPEND AWS_C_FLAGS -pedantic)
        endif()

        # Warning disables always go last to avoid future flags re-enabling them
        list(APPEND AWS_C_FLAGS -Wno-long-long)

        # Always enable position independent code, since this code will always end up in a shared lib
        check_c_compiler_flag("-fPIC -Werror" HAS_FPIC_FLAG)
        if (HAS_FPIC_FLAG)
            list(APPEND AWS_C_FLAGS -fPIC)
        endif()

        if (LEGACY_COMPILER_SUPPORT)
            list(APPEND AWS_C_FLAGS -Wno-strict-aliasing)
        endif()

        # -moutline-atomics generates code for both older load/store exclusive atomics and also
        # Arm's Large System Extensions (LSE) which scale substantially better on large core count systems.
        #
        # Test by compiling a program that actually uses atomics.
        # Previously we'd simply used check_c_compiler_flag() but that wasn't detecting
        # some real-world problems (see https://github.com/awslabs/aws-c-common/issues/902).
        if (AWS_ARCH_ARM64)
            set(old_flags "${CMAKE_REQUIRED_FLAGS}")
            set(CMAKE_REQUIRED_FLAGS "-moutline-atomics -Werror")
            check_c_source_compiles("
                int main() {
                    int x = 1;
                    __atomic_fetch_add(&x, -1, __ATOMIC_SEQ_CST);
                    return x;
                }" HAS_MOUTLINE_ATOMICS)
            set(CMAKE_REQUIRED_FLAGS "${old_flags}")

            if (HAS_MOUTLINE_ATOMICS)
                list(APPEND AWS_C_FLAGS -moutline-atomics)
            endif()
        endif()

        # Check for Posix Large File Support (LFS).
        # Doing this check here, instead of AwsFeatureTests.cmake,
        # because we might need to modify AWS_C_FLAGS to enable it.
        set(HAS_LFS FALSE)
        aws_check_posix_lfs("" BY_DEFAULT)
        if (BY_DEFAULT)
            set(HAS_LFS TRUE)
        else()
            aws_check_posix_lfs("-D_FILE_OFFSET_BITS=64" VIA_DEFINES)
            if (VIA_DEFINES)
                list(APPEND AWS_C_FLAGS "-D_FILE_OFFSET_BITS=64")
                set(HAS_LFS TRUE)
            endif()
        endif()
        # This becomes a define in config.h
        set(AWS_HAVE_POSIX_LARGE_FILE_SUPPORT ${HAS_LFS} CACHE BOOL "Posix Large File Support")

        # Hide symbols from libcrypto.a
        # This avoids problems when an application ends up using both libcrypto.a and libcrypto.so.
        #
        # An example of this happening is the aws-c-io tests.
        # All the C libs are compiled statically, but then a PKCS#11 library is
        # loaded at runtime which happens to use libcrypto.so from OpenSSL.
        # If the symbols from libcrypto.a aren't hidden, then SOME function calls use the libcrypto.a implementation
        # and SOME function calls use the libcrypto.so implementation, and this mismatch leads to weird crashes.
        if (UNIX AND NOT APPLE)
            # If we used target_link_options() (CMake 3.13+) we could make these flags PUBLIC
            set_property(TARGET ${target} APPEND_STRING PROPERTY LINK_FLAGS " -Wl,--exclude-libs,libcrypto.a")
        endif()

    endif()

    check_include_file(stdint.h HAS_STDINT)
    check_include_file(stdbool.h HAS_STDBOOL)

    if (NOT HAS_STDINT)
        list(APPEND AWS_C_FLAGS -DNO_STDINT)
    endif()

    if (NOT HAS_STDBOOL)
        list(APPEND AWS_C_FLAGS -DNO_STDBOOL)
    endif()

    if(NOT SET_PROPERTIES_NO_WGNU)
        check_c_compiler_flag("-Wgnu -Werror" HAS_WGNU)
        if(HAS_WGNU)
            # -Wgnu-zero-variadic-macro-arguments results in a lot of false positives
            list(APPEND AWS_C_FLAGS -Wgnu -Wno-gnu-zero-variadic-macro-arguments)

            # some platforms implement htonl family of functions via GNU statement expressions (https://gcc.gnu.org/onlinedocs/gcc/Statement-Exprs.html)
            # which generates -Wgnu-statement-expression warning.
            set(old_flags "${CMAKE_REQUIRED_FLAGS}")
            set(CMAKE_REQUIRED_FLAGS "-Wgnu -Werror")
            check_c_source_compiles("
            #include <netinet/in.h>

            int main() {
            uint32_t x = 0;
            x = htonl(x);
            return (int)x;
            }"  NO_GNU_EXPR)
            set(CMAKE_REQUIRED_FLAGS "${old_flags}")

            if (NOT NO_GNU_EXPR)
                list(APPEND AWS_C_FLAGS -Wno-gnu-statement-expression)
            endif()
        endif()
    endif()

    # some platforms (especially when cross-compiling) do not have the sysconf API in their toolchain files.
    check_c_source_compiles("
    #include <unistd.h>
    int main() {
      sysconf(_SC_NPROCESSORS_ONLN);
    }"  HAVE_SYSCONF)

    if (HAVE_SYSCONF)
        list(APPEND AWS_C_DEFINES_PRIVATE -DHAVE_SYSCONF)
    endif()

    list(APPEND AWS_C_DEFINES_PRIVATE $<$<CONFIG:Debug>:DEBUG_BUILD>)

    if ((NOT SET_PROPERTIES_NO_LTO) AND AWS_ENABLE_LTO)
        # enable except in Debug builds
        set(_ENABLE_LTO_EXPR $<NOT:$<CONFIG:Debug>>)

        # try to check whether compiler supports LTO/IPO
        if (POLICY CMP0069)
            cmake_policy(SET CMP0069 NEW)
            include(CheckIPOSupported OPTIONAL RESULT_VARIABLE ipo_check_exists)
            if (ipo_check_exists)
                check_ipo_supported(RESULT ipo_supported)
                if (ipo_supported)
                    message(STATUS "Enabling IPO/LTO for Release builds")
                else()
                    message(STATUS "AWS_ENABLE_LTO is enabled, but cmake/compiler does not support it, disabling")
                    set(_ENABLE_LTO_EXPR OFF)
                endif()
            endif()
        endif()
    else()
        set(_ENABLE_LTO_EXPR OFF)
    endif()

    if(BUILD_SHARED_LIBS)
        if (NOT MSVC)
            # this should only be set when building shared libs.
            list(APPEND AWS_C_FLAGS "-fvisibility=hidden")
        endif()
    endif()

    if(AWS_ENABLE_TRACING)
        target_link_libraries(${target} PRIVATE ittnotify)
    else()
        # Disable intel notify api if tracing is not enabled
        list(APPEND AWS_C_DEFINES_PRIVATE -DINTEL_NO_ITTNOTIFY_API)
    endif()
    target_compile_options(${target} PRIVATE ${AWS_C_FLAGS})
    target_compile_definitions(${target} PRIVATE ${AWS_C_DEFINES_PRIVATE} PUBLIC ${AWS_C_DEFINES_PUBLIC})
    set_target_properties(${target} PROPERTIES LINKER_LANGUAGE C C_STANDARD 99 C_STANDARD_REQUIRED ON)
    set_target_properties(${target} PROPERTIES INTERPROCEDURAL_OPTIMIZATION ${_ENABLE_LTO_EXPR}>)
endfunction()
