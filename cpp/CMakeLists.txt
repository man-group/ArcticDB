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

if(${CMAKE_TOOLCHAIN_FILE} MATCHES "vcpkg.cmake$")
    message(STATUS "CMAKE_TOOLCHAIN_FILE is set to vcpkg, so will use find_package to find the libraries it build")
    set(ARCTICDB_USING_VCPKG ON)
endif()

if(${CMAKE_VERSION} VERSION_LESS 3.15)
    message(WARNING "Your cmake version is older than 3.15, which doesn't support the more granular FindPython \
Development.Module component. We will fallback to not requiring it, but the object may fail to link.")
    set(ARCTICDB_FIND_PYTHON_DEV_MODE ""                 CACHE STRING "FindPython Development arguments")
else()
    set(ARCTICDB_FIND_PYTHON_DEV_MODE Development.Module CACHE STRING "FindPython Development arguments")
endif()

foreach(var_name PYTHON_EXECUTABLE PYTHON_LIBRARIES PYTHON_LIBRARY PYTHON_INCLUDE_DIRS)
    if(DEFINED ${var_name})
        message("${var_name} is set (to ${${var_name}}), which can influence FindPython and Pybind.")
    endif()
endforeach ()

set(Python_USE_STATIC_LIBS OFF)
# Must be called before Pybind (third_party) to override its outdated finding mechanism
find_package(Python 3 COMPONENTS Interpreter ${ARCTICDB_FIND_PYTHON_DEV_MODE} REQUIRED)

if(${Python_Development.Module_FOUND})
    set(PYTHON_LIBRARY_SO Python::Module CACHE STRING "Python::Module link target name for override")
elseif(${Python_Development_FOUND})
    set(PYTHON_LIBRARY_SO Python::Python CACHE STRING "Python::Module link target name for override")
elseif(NOT ${PYTHON_INCLUDE_DIRS} OR NOT EXISTS ${PYTHON_INCLUDE_DIRS})
    # Only possible due to CMake < 3.15
    message(WARNING "Did not find Python::Module or Develop and no valid (override) path is supplied. Will ask python:")
    execute_process (COMMAND "${Python_EXECUTABLE}" -c "import sysconfig; print(sysconfig.get_path('include'))"
            OUTPUT_VARIABLE PYTHON_INCLUDE_DIRS
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    get_filename_component(PYTHON_INCLUDE_DIRS_EXTRA ${Python_EXECUTABLE} DIRECTORY) # pyconfig.h is sometimes here
    list(APPEND PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIRS_EXTRA})
    message(${PYTHON_INCLUDE_DIRS})
endif()

if(${BUILD_PYTHON_VERSION} AND NOT ${BUILD_PYTHON_VERSION} VERSION_EQUAL "${Python_VERSION_MAJOR}.${Python_VERSION_MINOR}")
    message(FATAL_ERROR "You specified BUILD_PYTHON_VERSION=${BUILD_PYTHON_VERSION}, but FindPython found ${Python_VERSION}. \
Use the official Python_ROOT_DIR and Python_EXECUTABLE hints to properly specify alternative versions, or run cmake \
from within a virtualenv.")
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
