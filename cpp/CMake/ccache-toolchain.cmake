# Chainload toolchain for vcpkg package builds on Linux.
# Includes the default vcpkg Linux toolchain (which sets -fPIC etc.), then
# layers ccache on top if available and no launcher is already configured.

include("${CMAKE_CURRENT_LIST_DIR}/../vcpkg/scripts/toolchains/linux.cmake")

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM AND NOT CMAKE_C_COMPILER_LAUNCHER AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE STRING "")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}" CACHE STRING "")
endif()
