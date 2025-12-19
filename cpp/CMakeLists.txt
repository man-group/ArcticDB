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
option(ARCTICDB_COUNT_ALLOCATION "Override new and delete to count allocations." OFF)

set(ARCTICDB_SANITIZER_FLAGS)
if(${ARCTICDB_USING_ADDRESS_SANITIZER})
    # Note: -shared-libasan is not needed for sanitizers to work correctly.
    list(APPEND ARCTICDB_SANITIZER_FLAGS "-fsanitize=address")
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

if(${ARCTICDB_COUNT_ALLOCATIONS})
    add_compile_definitions(ARCTICDB_COUNT_ALLOCATIONS)
    include(FetchContent)
    FetchContent_Declare(
            cpptrace
            GIT_REPOSITORY https://github.com/jeremy-rifkin/cpptrace.git
            GIT_TAG        v0.7.3
    )
    FetchContent_MakeAvailable(cpptrace)
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

if(${ARCTICDB_USING_CONDA})
    # Required to be able to include headers from glog since glog 0.7
    # See: https://github.com/google/glog/pull/1030
    add_compile_definitions(GLOG_USE_GLOG_EXPORT)
endif()

if(WIN32)
    add_compile_definitions(
        NOGDI WIN32_LEAN_AND_MEAN HAVE_SNPRINTF NOMINMAX
        "$<$<CONFIG:Debug>:DEBUG_BUILD>"
    )
    if(${ARCTICDB_USING_CONDA})
        # Some packages on conda-forge would need to be rebuilt for Windows with a patch on their feedstock to perform work
        # which have done been performed upstrem (adding those compile definitions with CMake config or etc.).
        #
        # The maintenance cost of all those patch and the complexity involved to pick the resulting new builds would be
        # much higher that setting those compile definitions here.
        add_compile_definitions(
            # Required to link against zstd.dll
            # See: https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L47-L53
            ZSTD_DLL_IMPORT=1

            # Required to link against lz4.dll
            # See: https://github.com/lz4/lz4/blob/dev/lib/lz4frame.h#L79-L85
            LZ4_DLL_IMPORT=1

            # Required to link against protobuf.dll
            # See: https://github.com/protocolbuffers/protobuf/blob/384fabf35a9b5d1f1502edaf9a139b1f51551a01/cmake/README.md?plain=1#L296-L319
            PROTOBUF_USE_DLLS=1

            # Required to link against fmt.dll
            FMT_SHARED=1

            # Required to link against spdlog.dll
            # See: https://github.com/gabime/spdlog/blob/faa0a7a9c5a3550ed5461fab7d8e31c37fd1a2ef/include/spdlog/common.h#L28-L48
            SPDLOG_COMPILED_LIB=1
            SPDLOG_SHARED_LIB=1
            SPDLOG_HEADER_ONLY=0

            # Indicate that we are building using fmt's external DLL instead of spdlog's internal fmt
            SPDLOG_FMT_EXTERNAL=1

            # Required to link against azure-core.dll
            # See: https://github.com/Azure/azure-sdk-for-cpp/blob/c914c0933fa4835c6618a42bb276d6077423b170/sdk/storage/azure-storage-common/inc/azure/storage/common/dll_import_export.hpp#L19
            AZ_CORE_DLL=1

            # Required to link against azure-identity.dll
            # See: https://github.com/Azure/azure-sdk-for-cpp/blob/bf36db827f5ea1ef2e38953875c6c335b4242b2e/sdk/identity/azure-identity/inc/azure/identity/dll_import_export.hpp
            AZ_IDENTITY_DLL=1

            # Required to link against azure-storage-common.dll
            # See: https://github.com/Azure/azure-sdk-for-cpp/blob/c914c0933fa4835c6618a42bb276d6077423b170/sdk/storage/azure-storage-common/inc/azure/storage/common/dll_import_export.hpp#L19
            AZ_STORAGE_COMMON_DLL=1

            # Required to link against azure-storage-blobs.dll
            # See: https://github.com/Azure/azure-sdk-for-cpp/blob/bf36db827f5ea1ef2e38953875c6c335b4242b2e/sdk/storage/azure-storage-blobs/inc/azure/storage/blobs/dll_import_export.hpp
            AZ_STORAGE_BLOBS_DLL=1
        )
    endif()

    # We always generate the debug info on Windows since those are not in the final .dll.
    # sccache needs the /Z7 format, but the linker will still produce .pdb files.
    # Guide to MSVC compilation warnings https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warnings-c4000-through-c4199?view=msvc-170
    add_compile_options(
        /DWIN32 /D_WINDOWS /GR /EHsc /bigobj /Z7 /wd4244 /wd4267
    )

    if(${ARCTICDB_USING_CONDA})
        # Conda builds use dynamic linkage
        add_compile_options(
            "$<$<CONFIG:Debug>:/MDd>"
            "$<$<CONFIG:Release>:/MD;/Ox>"
        )
    else()
        # PyPI builds use static linkage
        add_compile_options(
            "$<$<CONFIG:Debug>:/Od;/MTd>"
            "$<$<CONFIG:Release>:/MT;/Ox>"
        )
    endif()

    if(${ARCTICDB_MSVC_OMIT_RUNTIME_CHECKS})
        message(STATUS "Removing MSVC runtime checks")
        foreach(flag_var CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
            STRING (REGEX REPLACE "/RTC[^ ]*" "" ${flag_var} "${${flag_var}}")
        endforeach()
    endif()
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

# Add `third_party` include directories (only used for vcpkg builds) so that headers can
# be included without a compile-time check on `ARCTICDB_USING_CONDA` in the source files.
if(NOT ${ARCTICDB_USING_CONDA})
    # `semimap.h` is at `third_party/semimap/semimap.h`, so we need `third_party` in the path
    # to allow <semimap/semimap.h> to resolve correctly
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/lmdbcxx)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/recycle/src)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/Remotery/lib)
endif()

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
