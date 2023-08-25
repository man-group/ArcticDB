cmake_minimum_required(VERSION 3.12) # FindPython

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
cmake_policy(SET CMP0048 NEW)
project(arcticdb VERSION 0.0.1)

enable_testing()


set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(
    ARCTICDB_USING_CONDA
    "Whether ArcticDB's build system relies on conda for resolving its dependencies."
    OFF
)


if(WIN32)
    add_compile_definitions(NOGDI)
    add_compile_definitions(WIN32_LEAN_AND_MEAN)
    add_compile_definitions(HAVE_SNPRINTF)
    add_compile_definitions(NOMINMAX)

    # We always generate the debug info on Windows since those are not in the final .dll.
    # sccache needs the /Z7 format, but the linker will still produce .pdb files.
    # Guide to MSVC compilation warnings https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warnings-c4000-through-c4199?view=msvc-170
    set(CMAKE_CXX_FLAGS "/DWIN32 /D_WINDOWS /GR /EHsc /bigobj /Z7 /w /wd4244 /wd4267")
    set(CMAKE_CXX_FLAGS_RELEASE "/MT /DNDEBUG /O2")
    set(CMAKE_CXX_FLAGS_DEBUG "/MTd /RTC1 /Od")
else()
    if(${ARCTICDB_USING_CONDA})
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    endif()
    set(CMAKE_CXX_FLAGS_DEBUG "-ggdb")
    if(DEFINED ENV{ARCTICDB_CODE_COVERAGE_BUILD})
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage -g -O0")
    endif()
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -ggdb")
endif()

IF(DEFINED ENV{CMAKE_BUILD_TYPE})
    message(STATUS "Setting CMAKE_BUILD_TYPE to $ENV{CMAKE_BUILD_TYPE}")
    set(CMAKE_BUILD_TYPE $ENV{CMAKE_BUILD_TYPE})
ENDIF()

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting CMAKE_BUILD_TYPE to Release")
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(FIND_LIBRARY_USE_LIB64_PATHS ON)
include(PythonUtils) # Must be called before Pybind (third_party) to override its finding mechanism

add_subdirectory(third_party)
add_subdirectory(proto)
if(NOT ${ARCTICDB_USING_CONDA})
    include(AzureVcpkg) #AzureVcpkg.cmake is from https://github.com/Azure/azure-sdk-for-cpp/blob/main/cmake-modules/AzureVcpkg.cmake, commit ada77b3
endif()

python_utils_dump_vars_if_enabled("After Pybind")
python_utils_check_include_dirs("accepted by pybind")
python_utils_check_version_is_as_expected()

#proto files are generated there so it's necessary to include them
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/proto/arcticc/pb2/proto/)
# for IDE resolution otherwise it is lost
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if("${BUILD_CYTHON}")
    add_subdirectory(${BUILD_CYTHON_PATH})
endif()

add_subdirectory(arcticdb)
