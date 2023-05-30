if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})    
    cmake_minimum_required(VERSION 3.12) 
    set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../CMake" ${CMAKE_MODULE_PATH})
    cmake_policy(SET CMP0048 NEW)
    project(arcticdb-python VERSION 1.2.1)

    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    set(FIND_LIBRARY_USE_LIB64_PATHS ON)

    option(ARCTICDB_USING_CONDA "Are we building with conda?" OFF)
    find_package(arcticdb REQUIRED)
    if(ARCTICDB_USING_CONDA)
    SET(ARCTICDB_CORE_LIBRARY arcticdb::arcticdb_core)
    else()
        SET(ARCTICDB_CORE_LIBRARY arcticdb::arcticdb_core_static)
    endif()


    


else()
    SET(ARCTICDB_CORE_LIBRARY arcticdb_core_static)
endif()

find_package(pybind11 REQUIRED)
    
## Core python bindings, private only ##
set(arcticdb_python_srcs
        ../async/python_bindings.cpp
        ../codec/python_bindings.cpp
        ../column_store/python_bindings.cpp
        ../storage/python_bindings.cpp
        ../stream/python_bindings.cpp
        ../toolbox/python_bindings.cpp
        ../version/python_bindings.cpp)

add_library(arcticdb_python STATIC ${arcticdb_python_srcs})

target_link_libraries(arcticdb_python
        PUBLIC
        #tsan
        ${ARCTICDB_CORE_LIBRARY}
        ${AWSSDK_LINK_LIBRARIES}
        pybind11::module
        )

target_include_directories(arcticdb_python PRIVATE
        ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
        ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
        ${BITMAGIC_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/../..
        )


if (NOT WIN32)
    add_compile_options("-ftemplate-depth=1000")
    if(ARCTICDB_USING_CONDA)
        # Compilers on conda-forge are relatively strict.
        # For now, we do not want to treat warnings as errors for those builds.
        add_compile_options("-Werror")
    endif()
endif ()

## arcticdb_ext python module
# This configures: linking to Python::Module + pybind, output pre-/suf-fix, -fvisibility=hidden, strip release build
pybind11_add_module(arcticdb_ext MODULE
       python_module.cpp
       arctic_version.cpp
       arctic_version.hpp
       reader.hpp
       adapt_read_dataframe.hpp
        )


set (additional_link_flags "")
if (HIDE_LINKED_SYMBOLS AND (NOT WIN32))

    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        list(APPEND additional_link_flags
            "-Wl,--exclude-libs,ALL"
            "-Wl,--gc-sections")
    endif()
endif()

target_link_libraries(arcticdb_ext
        PUBLIC
        #tsan
        PRIVATE
        arcticdb_python
        ${additional_link_flags}
        )


if (WIN32)
    target_link_options(arcticdb_ext
            PRIVATE $<$<CONFIG:Release>:/DEBUG> $<$<CONFIG:Release>:/OPT:REF>)
endif()


target_include_directories(arcticdb_ext
        PRIVATE
        ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
        ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
        ${BITMAGIC_INCLUDE_DIRS}
        ${PYTHON_NUMPY_INCLUDE_DIR}
        ${AWSSDK_INCLUDE_DIRS}
        )

install(TARGETS arcticdb_ext
        LIBRARY DESTINATION . COMPONENT Python_Lib
        RUNTIME DESTINATION . COMPONENT Python_Lib)

# Improves portability (in setup.py) by creating an explicit target that manually installs the library
if(NOT WIN32 AND "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo" AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_custom_command(OUTPUT arcticdb_ext.gz
            COMMAND bash -c "gzip -c $<TARGET_FILE:arcticdb_ext> >arcticdb_ext.gz"
            DEPENDS arcticdb_ext)
    add_custom_target(install_arcticdb_ext
            ${CMAKE_STRIP} --strip-unneeded $<TARGET_FILE:arcticdb_ext>
                           -o ${CMAKE_INSTALL_PREFIX}/$<TARGET_FILE_NAME:arcticdb_ext>
            DEPENDS arcticdb_ext.gz)
else()
    add_custom_target(install_arcticdb_ext
            ${CMAKE_COMMAND} -E copy $<TARGET_FILE:arcticdb_ext> ${CMAKE_INSTALL_PREFIX})
endif()
