# The NDK does not provide any http or crypto functionality out of the box; we build versions of zlib, openssl, and curl to account for this.
if(BUILD_CURL OR BUILD_OPENSSL OR BUILD_ZLIB)
    set(EXTERNAL_CXX_FLAGS "-Wno-unused-private-field")
    set(EXTERNAL_C_FLAGS "")

    set(BASE_SDK_DIR ${CMAKE_BINARY_DIR} CACHE STRING "Android build" FORCE)

    # we patch the install process for each dependency to match what we need for 3rd party installation
    if (DEFINED CMAKE_INSTALL_PREFIX)
        set(EXTERNAL_INSTALL_DIR ${CMAKE_INSTALL_PREFIX}/external-install CACHE STRING "A string describes the path where external dependencies will be installed")
    else()
        set(EXTERNAL_INSTALL_DIR ${CMAKE_BINARY_DIR}/external-install CACHE STRING "A string describes the path where external dependencies will be installed")
    endif()
    set(EXTERNAL_BUILD_DIR ${CMAKE_BINARY_DIR}/external-build)
    file(MAKE_DIRECTORY ${EXTERNAL_BUILD_DIR})

    # zlib
    if(BUILD_ZLIB)
        set(ZLIB_SOURCE_DIR ${CMAKE_BINARY_DIR}/zlib CACHE INTERNAL "zlib source dir")
        set(ZLIB_INSTALL_DIR ${EXTERNAL_INSTALL_DIR}/zlib CACHE INTERNAL "zlib install dir")
        set(ZLIB_INCLUDE_DIR ${ZLIB_INSTALL_DIR}/include/zlib CACHE INTERNAL "zlib include dir")
        set(ZLIB_LIBRARY_DIR ${ZLIB_INSTALL_DIR}/lib CACHE INTERNAL "zlib library dir")

        set( ZLIB_INCLUDE_FLAGS "-isystem ${ZLIB_INCLUDE_DIR}" CACHE INTERNAL "compiler flags to find zlib includes")
        set( ZLIB_LINKER_FLAGS "-L${ZLIB_LIBRARY_DIR}" CACHE INTERNAL "linker flags to find zlib")

        if(UNIX)
            set(ZLIB_NAME libz)
        else()
            set(ZLIB_NAME zlib)
        endif()

        add_library(zlib UNKNOWN IMPORTED)
        set_property(TARGET zlib PROPERTY IMPORTED_LOCATION ${ZLIB_LIBRARY_DIR}/${ZLIB_NAME}.a)
        set(ZLIB_LIBRARIES "${ZLIB_LIBRARY_DIR}/${ZLIB_NAME}.a")
        set(CURL_ZLIB_DEPENDENCY "ZLIB")
    endif()

    # OpenSSL
    if(BUILD_OPENSSL)
        set(OPENSSL_SOURCE_DIR ${CMAKE_BINARY_DIR}/openssl-src CACHE INTERNAL "openssl source dir")
        set(OPENSSL_INSTALL_DIR ${EXTERNAL_INSTALL_DIR}/openssl CACHE INTERNAL "openssl install dir")
        set(OPENSSL_INCLUDE_DIR ${OPENSSL_INSTALL_DIR}/include CACHE INTERNAL "openssl include dir")
        set(OPENSSL_LIBRARY_DIR ${OPENSSL_INSTALL_DIR}/lib CACHE INTERNAL "openssl library dir")
        set(OPENSSL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS} -fPIE" CACHE INTERNAL "openssl")
        set(OPENSSL_C_FLAGS "${EXTERNAL_C_FLAGS} -fPIE" CACHE INTERNAL "openssl")
        if(ANDROID_ABI STREQUAL "x86_64")
            set(OPENSSL_C_FLAGS "${OPENSSL_C_FLAGS} -DOPENSSL_NO_INLINE_ASM" CACHE INTERNAL "openssl")
        endif()
        set(OPENSSL_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fPIE -pie" CACHE INTERNAL "openssl")

        set(OPENSSL_INCLUDE_FLAGS "-isystem ${OPENSSL_INCLUDE_DIR} -isystem ${OPENSSL_INCLUDE_DIR}/openssl" CACHE INTERNAL "compiler flags to find openssl includes")
        set(OPENSSL_LINKER_FLAGS "-L${OPENSSL_LIBRARY_DIR}" CACHE INTERNAL "linker flags to find openssl")

        add_library(ssl UNKNOWN IMPORTED)
        set_property(TARGET ssl PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARY_DIR}/libssl.a)
        add_library(crypto UNKNOWN IMPORTED)
        set_property(TARGET crypto PROPERTY IMPORTED_LOCATION ${OPENSSL_LIBRARY_DIR}/libcrypto.a)
        set_target_properties(crypto PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}"
            IMPORTED_LINK_INTERFACE_LANGUAGES "C")
        set(OPENSSL_LIBRARIES "${OPENSSL_LIBRARY_DIR}/libssl.a;${OPENSSL_LIBRARY_DIR}/libcrypto.a")
        set(OPENSSL_CRYPTO_LIBRARY "${OPENSSL_LIBRARY_DIR}/libcrypto.a")
        set(CURL_OPENSSL_DEPENDENCY "OPENSSL")

        set(LibCrypto_INCLUDE_DIR "${OPENSSL_INCLUDE_DIR}" CACHE INTERNAL "crypto include dir")
        set(LibCrypto_STATIC_LIBRARY "${OPENSSL_LIBRARY_DIR}/libcrypto.a" CACHE INTERNAL "crypto static library")
    endif()

    # curl
    if(BUILD_CURL)
        set(CURL_SOURCE_DIR ${CMAKE_BINARY_DIR}/curl CACHE INTERNAL "libcurl source dir")
        set(CURL_INSTALL_DIR ${EXTERNAL_INSTALL_DIR}/curl CACHE INTERNAL "libcurl install dir")
        set(CURL_INCLUDE_DIR ${CURL_INSTALL_DIR}/include CACHE INTERNAL "libcurl include dir")
        set(CURL_LIBRARY_DIR ${CURL_INSTALL_DIR}/lib CACHE INTERNAL "libcurl library dir")

        set( CURL_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS}" CACHE INTERNAL "" )
        set( CURL_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}" CACHE INTERNAL "" )

        set( CURL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS} ${OPENSSL_INCLUDE_FLAGS} ${ZLIB_INCLUDE_FLAGS} -Wno-unused-value -fPIE ${ZLIB_LINKER_FLAGS} ${OPENSSL_LINKER_FLAGS}" CACHE INTERNAL "")
        set( CURL_C_FLAGS "${EXTERNAL_C_FLAGS}  ${OPENSSL_INCLUDE_FLAGS} ${ZLIB_INCLUDE_FLAGS} -Wno-unused-value -fPIE ${ZLIB_LINKER_FLAGS} ${OPENSSL_LINKER_FLAGS}" CACHE INTERNAL "")
        set( CURL_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fPIE -pie ${ZLIB_LINKER_FLAGS} ${OPENSSL_LINKER_FLAGS}" CACHE INTERNAL "" )

        if(ZLIB_LIBRARIES)
            set(CURL_USE_ZLIB "ON")
        else()
            set(CURL_USE_ZLIB "OFF")
        endif()

        add_library(curl UNKNOWN IMPORTED)
        set_property(TARGET curl PROPERTY IMPORTED_LOCATION ${CURL_LIBRARY_DIR}/libcurl.a)

        set(CURL_LIBRARIES "${CURL_LIBRARY_DIR}/libcurl.a")
    endif()

    execute_process(
        COMMAND ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR}
        -DAWS_NATIVE_SDK_ROOT=${AWS_NATIVE_SDK_ROOT}

        -DBUILD_CURL=${BUILD_CURL}
        -DCURL_SOURCE_DIR=${CURL_SOURCE_DIR}
        -DCURL_INSTALL_DIR=${CURL_INSTALL_DIR}
        -DCURL_OPENSSL_DEPENDENCY=${CURL_OPENSSL_DEPENDENCY}
        -DCURL_ZLIB_DEPENDENCY=${CURL_ZLIB_DEPENDENCY}
        -DCURL_CXX_FLAGS=${CURL_CXX_FLAGS}
        -DCURL_C_FLAGS=${CURL_C_FLAGS}
        -DCURL_STATIC_LINKER_FLAGS=${CURL_STATIC_LINKER_FLAGS}
        -DCURL_SHARED_LINKER_FLAGS=${CURL_SHARED_LINKER_FLAGS}
        -DCURL_EXE_LINKER_FLAGS=${CURL_EXE_LINKER_FLAGS}
        -DCURL_USE_ZLIB=${CURL_USE_ZLIB}

        -DBUILD_OPENSSL=${BUILD_OPENSSL}
        -DOPENSSL_SOURCE_DIR=${OPENSSL_SOURCE_DIR}
        -DOPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}
        -DOPENSSL_INSTALL_DIR=${OPENSSL_INSTALL_DIR}
        -DOPENSSL_CXX_FLAGS=${OPENSSL_CXX_FLAGS}
        -DOPENSSL_C_FLAGS=${OPENSSL_C_FLAGS}
        -DOPENSSL_EXE_LINKER_FLAGS=${OPENSSL_EXE_LINKER_FLAGS}

        -DBUILD_ZLIB=${BUILD_ZLIB}
        -DZLIB_SOURCE_DIR=${ZLIB_SOURCE_DIR}
        -DZLIB_INSTALL_DIR=${ZLIB_INSTALL_DIR}

        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DCMAKE_ANDROID_NDK_TOOLCHAIN_VERSION=${CMAKE_ANDROID_NDK_TOOLCHAIN_VERSION}
        -DANDROID_NATIVE_API_LEVEL=${ANDROID_NATIVE_API_LEVEL}
        -DANDROID_ABI=${ANDROID_ABI}
        -DANDROID_TOOLCHAIN=${ANDROID_TOOLCHAIN}
        -DANDROID_STL=${ANDROID_STL}

        -DEXTERNAL_CXX_FLAGS=${EXTERNAL_CXX_FLAGS}
        -DEXTERNAL_C_FLAGS=${EXTERNAL_C_FLAGS}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/android-build
        WORKING_DIRECTORY ${EXTERNAL_BUILD_DIR}
        RESULT_VARIABLE CONFIGURE_DEPS_EXIT_CODE)

    if (NOT ${CONFIGURE_DEPS_EXIT_CODE} EQUAL 0)
        message(FATAL_ERROR "Failed to configure dependency libraries.")
    endif()

    include(ProcessorCount)
    ProcessorCount(NUM_JOBS)
    execute_process(COMMAND ${CMAKE_COMMAND} --build ${EXTERNAL_BUILD_DIR} --config ${CMAKE_BUILD_TYPE} -- -j ${NUM_JOBS}
    RESULT_VARIABLE BUILD_DEPS_EXIT_CODE)

    if (NOT ${BUILD_DEPS_EXIT_CODE} EQUAL 0)
        message(FATAL_ERROR "Failed to build dependency libraries.")
    endif()

endif()
