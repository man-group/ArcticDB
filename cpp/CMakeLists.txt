cmake_minimum_required(VERSION 3.21) # TARGET_RUNTIME_DLLS

# Make the `project` command handle the version of the project.
cmake_policy(SET CMP0048 NEW)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# We do not need any compilers' extensions, so we disable them.
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
project(arcticdb VERSION 0.0.1)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(ARCTICDB_MSVC_OMIT_RUNTIME_CHECKS OFF CACHE BOOL "")
set(ARCTICDB_USE_PCH OFF CACHE BOOL "Whether to use precompiled headers")
option(
    ARCTICDB_USING_CONDA
    "Whether ArcticDB's build system relies on conda for resolving its dependencies."
    OFF
)
option(ARCTICDB_USING_ADDRESS_SANITIZER "Enable address sanitizer." OFF)
option(ARCTICDB_USING_THREAD_SANITIZER "Enable thread sanitizer." OFF)
option(ARCTICDB_USING_UB_SANITIZER "Enable undefined behavior sanitizer." OFF)
option(ARCTICDB_LOG_PERFORMANCE "Whether to log performance timings." OFF)

set(ARCTICDB_SANITIZER_FLAGS)
if(${ARCTICDB_USING_ADDRESS_SANITIZER})
    list(APPEND ARCTICDB_SANITIZER_FLAGS "-fsanitize=address;-shared-libasan")
    link_libraries(atomic)
    message(STATUS "Building ArcticDB with Address Sanitizer")
endif()
if(${ARCTICDB_USING_THREAD_SANITIZER})
    list(APPEND ARCTICDB_SANITIZER_FLAGS "-fsanitize=thread")
    link_libraries(atomic)
    message(STATUS "Building ArcticDB with Thread Sanitizer")
endif()
if(${ARCTICDB_USING_UB_SANITIZER})
    list(APPEND ARCTICDB_SANITIZER_FLAGS "-fsanitize=undefined")
    message(STATUS "Building ArcticDB with UB Sanitizer")
endif()

if(${ARCTICDB_LOG_PERFORMANCE})
    add_compile_definitions(ARCTICDB_LOG_PERFORMANCE)
endif()

if(ARCTICDB_SANITIZER_FLAGS)
    list(APPEND ARCTICDB_SANITIZER_FLAGS "-g;-fno-omit-frame-pointer;-fno-optimize-sibling-calls;")
    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        add_link_options(-fuse-ld=lld)
        # According to clang's docs only LLD can be used for lining https://clang.llvm.org/docs/AddressSanitizer.html#usage
        message(STATUS "Clang compiler detected. Enforcing lld compiler.")
    else()
        message(WARNING "Sanitizers are tested only under Clang compiler and LLD linker.")
    endif()
    # Note: Sanitizer flags are set globally for all targets created after
    add_compile_options("${ARCTICDB_SANITIZER_FLAGS}")
    add_link_options(${ARCTICDB_SANITIZER_FLAGS})
    message(STATUS "Building ArcticDB with sanitizers. Compiler flags: ${ARCTICDB_SANITIZER_FLAGS}")
endif()

if(WIN32)
    add_compile_definitions(
        NOGDI WIN32_LEAN_AND_MEAN HAVE_SNPRINTF NOMINMAX
        "$<$<CONFIG:Debug>:DEBUG_BUILD>"
    )

    # We always generate the debug info on Windows since those are not in the final .dll.
    # sccache needs the /Z7 format, but the linker will still produce .pdb files.
    # Guide to MSVC compilation warnings https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warnings-c4000-through-c4199?view=msvc-170
    add_compile_options(
        /DWIN32 /D_WINDOWS /GR /EHsc /bigobj /Z7 /wd4244 /wd4267
        "$<$<CONFIG:Debug>:/Od;/MTd>"
        "$<$<CONFIG:Release>:/MT;/Ox>"
    )
    if(${ARCTICDB_MSVC_OMIT_RUNTIME_CHECKS})
        message(STATUS "Removing MSVC runtime checks")
        foreach(flag_var CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
            STRING (REGEX REPLACE "/RTC[^ ]*" "" ${flag_var} "${${flag_var}}")
        endforeach()
    endif()
else()
    if(${ARCTICDB_USING_CONDA})
        # Required to be able to include headers from glog since glog 0.7
        # See: https://github.com/google/glog/pull/1030
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wextra -DGLOG_USE_GLOG_EXPORT")
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

if(DEFINED ENV{CMAKE_BUILD_TYPE})
    message(STATUS "Setting CMAKE_BUILD_TYPE to $ENV{CMAKE_BUILD_TYPE}")
    set(CMAKE_BUILD_TYPE $ENV{CMAKE_BUILD_TYPE})
endif()

if(NOT CMAKE_BUILD_TYPE)
    message(STATUS "Setting CMAKE_BUILD_TYPE to Release")
    set(CMAKE_BUILD_TYPE Release)
endif()


set(FIND_LIBRARY_USE_LIB64_PATHS ON)
include(PythonUtils) # Must be called before Pybind (third_party) to override its finding mechanism

add_subdirectory(third_party)
add_subdirectory(proto)


python_utils_dump_vars_if_enabled("After Pybind")
python_utils_check_include_dirs("accepted by pybind")
python_utils_check_version_is_as_expected()

#proto files are generated there so it's necessary to include them
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/proto/arcticc/pb2/proto/)
# for IDE resolution otherwise it is lost
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(arcticdb)

# arcticdb_core_static is used by other projects so we expose the headers via `arcticdb/.../abc.hpp`.
# We plan to introduce a new target that exposes only a "public" subset of headers from this project
# soon instead, as exposing all headers is fragile for consumers.
target_include_directories(arcticdb_core_static PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/proto/arcticc/pb2/proto/>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
