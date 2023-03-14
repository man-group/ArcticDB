cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
cmake_policy(SET CMP0048 NEW)
project(arcticdb VERSION 0.0.1)

enable_testing()

message(ENV_BASE_DIR="${ENV_BASE_DIR}")
message(Python_ROOT_DIR="${Python_ROOT_DIR}")
message(PYTHON_LIBRARIES="${PYTHON_LIBRARIES}")
message(PYTHON_INCLUDE_DIRS="${PYTHON_INCLUDE_DIRS}")
message(BUILD_PYTHON_VERSION="${BUILD_PYTHON_VERSION}")
message(PYTHON_LIBRARY_SO="${PYTHON_LIBRARY_SO}")

if(WIN32)
    add_compile_definitions(NOGDI)
    add_compile_definitions(WIN32_LEAN_AND_MEAN)
    add_compile_definitions(HAVE_SNPRINTF)
    add_compile_definitions(NOMINMAX)

    # Guide to MSVC compilation warnings https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warnings-c4000-through-c4199?view=msvc-170
    set(CMAKE_CXX_FLAGS "/DWIN32 /D_WINDOWS /w /GR /EHsc /w /wd4244 /wd4267")
    set(CMAKE_CXX_FLAGS_RELEASE "/MT /DNDEBUG")
    set(CMAKE_CXX_FLAGS_DEBUG "/MTd /Zi /RTC1")
else()
    set(CMAKE_CXX_FLAGS "-Wall -Wextra")
    set(CMAKE_CXX_FLAGS_DEBUG "-ggdb")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3")
    set(STATIC_SYS_LIBS  -static-libgcc -static-libstdc++)
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

add_subdirectory(third_party)
add_subdirectory(proto)

#proto files are generated there so it's necessary to include them
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/proto/arcticc/pb2/proto/)
# for IDE resolution otherwise it is lost
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

if("${BUILD_CYTHON}")
    add_subdirectory(${BUILD_CYTHON_PATH})
endif()

add_subdirectory(arcticdb)
