
macro(verify_tools)
    # TODO: don't do this if the user is supplying their own curl/openssl/zlib
    # minimum version of cmake that
    #   (1) supports ExternalProject_Add URL_HASH
    #   (2) correctly extracts OPENSSL's version number from openssl/opensslv.h in version 1.0.2d
    cmake_minimum_required (VERSION 3.1.2)

    # TODO: don't do this if the user is supplying their own curl/openssl/zlib
    find_package(Git)
    if(NOT GIT_FOUND)
        find_package(Git)
        message(FATAL_ERROR "Unable to find git; git is required in order to build for Android")
    endif()
endmacro()

macro(determine_stdlib_and_api)
    if(ANDROID_STL MATCHES "c\\+\\+" OR NOT ANDROID_STL)
        if(FORCE_SHARED_CRT OR BUILD_SHARED_LIBS)
            SET(ANDROID_STL "c++_shared" CACHE STRING "" FORCE)
        else()
            SET(ANDROID_STL "c++_static" CACHE STRING "" FORCE)
        endif()

        if(NOT ANDROID_NATIVE_API_LEVEL)
            set(ANDROID_NATIVE_API_LEVEL "android-21")
        endif()

        # API levels below 9 will not build with libc++
        string(REGEX REPLACE "android-(..?)" "\\1" EXTRACTED_API_LEVEL "${ANDROID_NATIVE_API_LEVEL}")
        if(EXTRACTED_API_LEVEL LESS "9")
            message(STATUS "Libc++ requires setting API level to at least 9")
            set(ANDROID_NATIVE_API_LEVEL "android-9" CACHE STRING "" FORCE)
        endif()

    elseif(ANDROID_STL MATCHES "gnustl")
        if(FORCE_SHARED_CRT OR BUILD_SHARED_LIBS)
            SET(ANDROID_STL "gnustl_shared" CACHE STRING "" FORCE)
        else()
            SET(ANDROID_STL "gnustl_static" CACHE STRING "" FORCE)
        endif()

        # With gnustl, API level can go as low as 3, but let's make a reasonably modern default
        if(NOT ANDROID_NATIVE_API_LEVEL)
            set(ANDROID_NATIVE_API_LEVEL "android-19")
        endif()

    else()
        message(FATAL_ERROR "Invalid value for ANDROID_STL: ${ANDROID_STL}")
    endif()

    string(REGEX REPLACE "android-(..?)" "\\1" ANDROID_NATIVE_API_LEVEL_NUM "${ANDROID_NATIVE_API_LEVEL}")

endmacro()

macro(apply_pre_project_platform_settings)
    verify_tools()

    if(NOT NDK_DIR)
        set(NDK_DIR $ENV{ANDROID_NDK})
    endif()
    if(NOT IS_DIRECTORY "${NDK_DIR}")
        message(FATAL_ERROR "Could not find Android NDK (${NDK_DIR}); either set the ANDROID_NDK environment variable or pass the path in via -DNDK_DIR=..." )
    endif()

    if(NOT CMAKE_TOOLCHAIN_FILE)
        set(CMAKE_TOOLCHAIN_FILE "${NDK_DIR}/build/cmake/android.toolchain.cmake")
    endif()
    if(NOT EXISTS "${CMAKE_TOOLCHAIN_FILE}")
        message(FATAL_ERROR "Could not find android.toolchain.cmake, please use an Android NDK (e.g. r19c) with a built-in cmake toolchain file or provide one by specifying CMAKE_TOOLCHAIN_FILE.")
    endif()

    # android-specific required overrrides
    if (NOT DEFINED CUSTOM_MEMORY_MANAGEMENT)
        set(CUSTOM_MEMORY_MANAGEMENT ON)
    endif()

    set(ANDROID_STL_FORCE_FEATURES "OFF")

    # android-specific parameter defaults
    if(NOT ANDROID_ABI)
        set(ANDROID_ABI "armeabi-v7a")
        message(STATUS "Android ABI: none specified, defaulting to ${ANDROID_ABI}")
    else()
        message(STATUS "Android ABI: ${ANDROID_ABI}")
    endif()

    if(NOT ANDROID_TOOLCHAIN)
        set(ANDROID_TOOLCHAIN "clang")
        message(STATUS "Android toolchain unspecified, defaulting to ${ANDROID_TOOLCHAIN}")
    endif()
    # CMake 3.7+ compatibility for Android NDK 13, 14
    if(ANDROID_TOOLCHAIN STREQUAL "clang")
        set(CMAKE_ANDROID_NDK_TOOLCHAIN_VERSION "clang")
    endif()

    determine_stdlib_and_api()

    message(STATUS "Android std lib: ${ANDROID_STL}")
    message(STATUS "Android API level: ${ANDROID_NATIVE_API_LEVEL}")

    add_definitions("-DDISABLE_HOME_DIR_REDIRECT")

endmacro()

macro(apply_post_project_platform_settings)
    set(SDK_INSTALL_BINARY_PREFIX "${SDK_INSTALL_BINARY_PREFIX}/${ANDROID_ABI}-api-${ANDROID_NATIVE_API_LEVEL}")

    set(PLATFORM_DEP_LIBS log atomic)
    set(PLATFORM_DEP_LIBS_ABSTRACT_NAME log atomic)

    # Workaround for problem when ndk 13, gcc, and libc++ are used together.
    # See https://www.bountysource.com/issues/38341727-stddef-h-no-such-file-or-directory
    if(NOT NDK_RELEASE_NUMBER LESS "13000")
        if(ANDROID_TOOLCHAIN_NAME STREQUAL "standalone" AND ANDROID_STL MATCHES "libc" AND ANDROID_STANDALONE_TOOLCHAIN)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isystem ${ANDROID_STANDALONE_TOOLCHAIN}/include/c++/4.9.x")
        endif()
    endif()
    if(ANDROID_STL MATCHES "libc" AND ANDROID_NATIVE_API_LEVEL_NUM LESS "21")
        include_directories("${NDK_DIR}/sources/android/support/include")
    endif()

endmacro()

