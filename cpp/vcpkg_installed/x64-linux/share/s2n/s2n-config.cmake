include(CMakeFindDependencyMacro)

if (NOT MSVC)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
endif()

find_dependency(OpenSSL COMPONENTS Crypto)

include(${CMAKE_CURRENT_LIST_DIR}/s2n-targets.cmake)

