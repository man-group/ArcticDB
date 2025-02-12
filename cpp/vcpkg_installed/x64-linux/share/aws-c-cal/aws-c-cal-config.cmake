include(CMakeFindDependencyMacro)

find_dependency(aws-c-common)

if (NOT OFF AND NOT WIN32 AND NOT APPLE) # if NOT BYO_CRYPTO AND NOT WIN32 AND NOT APPLE
    if (OFF AND NOT ANDROID) # if USE_OPENSSL AND NOT ANDROID
        # aws-c-cal has been built with a dependency on OpenSSL::Crypto,
        # therefore consumers of this library have a dependency on OpenSSL and must have it found
        find_dependency(OpenSSL REQUIRED)
        find_dependency(Threads REQUIRED)
    else()
        list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/modules")
        find_dependency(crypto)
    endif()
endif()

macro(aws_load_targets type)
    include(${CMAKE_CURRENT_LIST_DIR}/aws-c-cal-targets.cmake)
endmacro()

# try to load the lib follow BUILD_SHARED_LIBS. Fall back if not exist.
if (BUILD_SHARED_LIBS)
    if (EXISTS "${CMAKE_CURRENT_LIST_DIR}/shared")
        aws_load_targets(shared)
    else()
        aws_load_targets(static)
    endif()
else()
    if (EXISTS "${CMAKE_CURRENT_LIST_DIR}/static")
        aws_load_targets(static)
    else()
        aws_load_targets(shared)
    endif()
endif()
