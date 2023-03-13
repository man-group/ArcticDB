cmake_minimum_required(VERSION 3.12) # FindPython

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
cmake_policy(SET CMP0048 NEW)
project(arcticdb VERSION 0.0.1)

enable_testing()

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


if (${CMAKE_TOOLCHAIN_FILE} MATCHES "vcpkg.cmake$")
    message(STATUS "CMAKE_TOOLCHAIN_FILE is set to vcpkg, so will use find_package to find the libraries it build")
    set(ARCTICDB_USING_VCPKG ON)
endif()

# Must be called before Pybind to override its outdated finding mechanism
if (ARCTICDB_USING_VCPKG)
    cmake_minimum_required(VERSION 3.18) # For finding Development.Module below
    find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
    set(ARCTICDB_PYTHON_MODULE Python3::Module)

elseif(NOT BUILD_PYTHON_VERSION)
# BUILD_PYTHON_VERSION is set by the Python build hooks regardless of whether building an internal egg or an external
# wheel. If invoking CMake directly however (which would normally be the case in an IDE) BUILD_PYTHON_VERSION will not
# be set. In this case. we'll set BUILD_PYTHON_VERSION as well as 4 other required build variables to sane default
# values.
    message("Resetting Python include locations...")
    set(ENV_BASE_DIR "/default-medusa-venv")

    set(BUILD_PYTHON_VERSION 3.6)

    set(PYTHON_LIBRARIES "${ENV_BASE_DIR}/lib")
    set(PYTHON_EXECUTABLE "${ENV_BASE_DIR}/bin/python")
    set(PYTHON_INCLUDE_DIRS "${ENV_BASE_DIR}/include/python${BUILD_PYTHON_VERSION}m")
    set(PYTHON_LIBRARY_SO "${ENV_BASE_DIR}/lib/libpython${BUILD_PYTHON_VERSION}m.so")
    set(ARCTICDB_PYTHON_MODULE ${PYTHON_LIBRARY_SO})
endif()

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
