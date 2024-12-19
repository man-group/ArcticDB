# Helpers
function(_python_utils_dump_vars_targets _target _props)
    if(TARGET ${_target})
        foreach(prop ${_props})
            get_property(prop_set TARGET ${_target} PROPERTY ${prop} SET)
            if(prop_set)
                get_target_property(val ${_target} ${prop})
                message("${_target} ${prop}=${val}")
            endif()
        endforeach()
    endif()
endfunction()

function(python_utils_dump_vars _message)
    message("${_message}")

    if(NOT CMAKE_PROPERTY_LIST)
        execute_process(COMMAND cmake --help-property-list OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)
        string(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
        set(CMAKE_INTERFACE_PROPERTY_LIST ${CMAKE_PROPERTY_LIST})
        list(FILTER CMAKE_INTERFACE_PROPERTY_LIST INCLUDE REGEX "^((COMPATIBLE_)?INTERFACE|IMPORTED_LIBNAME_)_.*")
    endif()
    foreach(target Python::Module Python::Python Python3::Module Python3::Python)
        _python_utils_dump_vars_targets(${target} "${CMAKE_PROPERTY_LIST}")
    endforeach()
    foreach(target headers pybind11 module embed python_link_helper python_headers)
        _python_utils_dump_vars_targets("pybind11::${target}" "${CMAKE_INTERFACE_PROPERTY_LIST}")
    endforeach()

    get_cmake_property(vars VARIABLES)
    foreach(var ${vars})
        if (var MATCHES ".*(PYTHON|Python|PYBIND|Pybind).*" AND NOT var MATCHES ".*_COPYRIGHT$")
            message("${var}=${${var}}")
        endif ()
    endforeach()
endfunction()

function(python_utils_dump_vars_if_enabled _message)
    if($ENV{ARCTICDB_DEBUG_FIND_PYTHON})
        python_utils_dump_vars(${_message})
    endif()
endfunction()


# Checks
function(python_utils_check_include_dirs _var_source)
    set(var_name "${ARCTICDB_PYTHON_PREFIX}_INCLUDE_DIRS")
    if(DEFINED CACHE{${var_name}})
        foreach(header_name Python.h pyconfig.h)
            find_file(valid_python_dir ${header_name} PATHS ${${var_name}} NO_DEFAULT_PATH)
            if(${valid_python_dir} STREQUAL "valid_python_dir-NOTFOUND")
                message(WARNING "${var_name} is ${_var_source} ('${${var_name}}'), but is invalid or\
                incomplete (missing ${header_name}). It will be ignored")
                unset(${var_name} CACHE)
                break()
            endif()
        endforeach()
    endif()
endfunction()

function(_python_utils_check_vars) # Arguments are the bad prefixes
    function(_body)
        set(var_name "${ARCTICDB_PYTHON_PREFIX}_${suffix}")
        set(good_prefix_val "${${var_name}}")
        if(DEFINED ${var_name})
            message("${var_name} is set (to '${${var_name}}'), which can influence FindPython and pybind11.")
        endif()

        foreach(bad_prefix ${ARGV})
            set(var_name "${bad_prefix}_${suffix}")
            if(DEFINED ${var_name} AND NOT "${${var_name}}" STREQUAL "${good_prefix_val}")
                message(WARNING "${var_name} is set (to '${${var_name}}'). It won't be used to build ArcticDB but may\
                    cause third-party packages to find a different Python and break the build.")
            endif()
        endforeach()
    endfunction()

    foreach(suffix EXECUTABLE LIBRARIES LIBRARY INCLUDE_DIR INCLUDE_DIRS FIND_ABI)
        _body(${ARGV})
    endforeach()

    set(suffix ROOT_DIR)
    if("${ARCTICDB_PYTHON_PREFIX}" STREQUAL "PYTHON")
        # pybind11 don't use PYTHON_ROOT_DIR, but we use Python_ROOT_DIR to resolve the EXECUTABLE
        set(ARCTICDB_PYTHON_PREFIX "Python")
        _body(PYTHON ${ARGV1})
    endif()
endfunction()

function(python_utils_check_version_is_as_expected)
    # Then check
    if(${BUILD_PYTHON_VERSION})
        if(NOT "${BUILD_PYTHON_VERSION}" VERSION_EQUAL
                "${${ARCTICDB_PYTHON_PREFIX}_VERSION_MAJOR}.${${ARCTICDB_PYTHON_PREFIX}_VERSION_MINOR}")
            message(FATAL_ERROR "You specified BUILD_PYTHON_VERSION=${BUILD_PYTHON_VERSION}, but FindPython found \
${${ARCTICDB_PYTHON_PREFIX}_VERSION}. Use the official Python_ROOT_DIR and Python_EXECUTABLE hints to properly override\
the default Python, or run cmake from within a virtualenv.")
        endif()
    endif()
endfunction()

# Enhanced FindPython
if(DEFINED ARCTICDB_FIND_PYTHON_DEV_MODE)
    message(STATUS "Using supplied ARCTICDB_FIND_PYTHON_DEV_MODE=${ARCTICDB_FIND_PYTHON_DEV_MODE}.")
    if("${ARCTICDB_FIND_PYTHON_DEV_MODE}" STREQUAL "pybind11")
        set(ARCTICDB_FIND_PYTHON_DEV_MODE "")
    endif()
elseif(WIN32)
    # Static linking is not necessary on Windows and Development.Module needs the runtime lib anyway:
    set(ARCTICDB_FIND_PYTHON_DEV_MODE Development)
elseif(${CMAKE_VERSION} VERSION_LESS 3.15)
    message(WARNING "Your cmake version is older than 3.15, which doesn't support the more granular FindPython \
            Development.Module component. Will try to use pybind11's old detection.")
else()
    set(Python_USE_STATIC_LIBS ON)
    set(ARCTICDB_FIND_PYTHON_DEV_MODE Development.Module)
endif()

if(ARCTICDB_FIND_PYTHON_DEV_MODE)
    set(ARCTICDB_PYTHON_PREFIX Python)
    python_utils_check_include_dirs("supplied")
    _python_utils_check_vars(PYTHON Python3)

    if(NOT Python_EXECUTABLE AND NOT Python_ROOT_DIR AND NOT Python_LIBRARY)
        # FindPython searches the PATH environment last, but that's arguably the only correct place it should look
        find_program(Python_EXECUTABLE NAMES python3 python NAMES_PER_DIR REQUIRED NO_CMAKE_SYSTEM_PATH)
        set(PYTHON_EXECUTABLE ${Python_EXECUTABLE} CACHE FILEPATH "Python executable found by FindPython")
    else()
        set(Python_FIND_STRATEGY LOCATION)
    endif()

    # Let CMake find Python without telling it the BUILD_PYTHON_VERSION we wanted. This way we know third-party stuff that
    # is not aware of BUILD_PYTHON_VERSION is going to find the same thing
    find_package(Python 3 COMPONENTS Interpreter ${ARCTICDB_FIND_PYTHON_DEV_MODE} REQUIRED)
    set(PYTHON_INCLUDE_DIRS ${Python_INCLUDE_DIRS})
   
    python_utils_dump_vars_if_enabled("After our FindPython before any third-party:")

    if(DEFINED Python_FIND_ABI)
        list(GET Python_FIND_ABI 0 _find_debug_python)
        if(${_find_debug_python})
            # Disables https://github.com/pybind/pybind11/blame/01f938e79938fd5b9bb6df2daeff95a625f6211f/include/pybind11/detail/common.h#L148-L160
            add_compile_definitions(Py_DEBUG)
        endif()
    else()
        set_target_properties(Python::Module PROPERTIES
                MAP_IMPORTED_CONFIG_DEBUG ";RELEASE"
                MAP_IMPORTED_CONFIG_RELWITHDEBINFO ";RELEASE"
            )
    endif()
    set(PYBIND11_FINDPYTHON OFF)
else()
    set(ARCTICDB_PYTHON_PREFIX PYTHON)
    python_utils_check_include_dirs("supplied")
    _python_utils_check_vars(Python Python3)

    if(NOT PYTHON_EXECUTABLE)
        find_program(PYTHON_EXECUTABLE python HINTS ${Python_ROOT_DIR} PATH_SUFFIXES bin NO_CMAKE_SYSTEM_PATH)
    endif()

    set(PYBIND11_FINDPYTHON OFF)
endif()

