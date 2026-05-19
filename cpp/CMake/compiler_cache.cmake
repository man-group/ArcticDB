# Sets compiler cache by prepending to CMAKE_CXX_COMPILER_LAUNCHER and CMAKE_C_COMPILER_LAUNCHER based on the value of
# the ARCTICDB_COMPILER_CACHE cache variable. Allowed values for the variable are AUTO|SCCACHE|CCACHE|OFF|CUSTOM
# * OFF does noting
# * SCCACHE|CCACHE the executables for the specified caches are sought. If they are not found the script fails.
# * CUSOM prepends CMAKE_C(XX)_COMPILER_LAUNCHER with the value of ARCTICDB_COMPILER_CACHE_PATH. If
#   ARCTICDB_COMPILER_CACHE_PATH is not set the script fails
# * AUTO tries to find a compiler cache in the order sccache|ccache. If nothing is found the script exits (but does
#   not fail). If the executable is found but CMAKE_C(XX)_COMPILER_LAUNCHER the script will not change anything.
function(set_arcticdb_compiler_cache)
    
    validate_compiler_cache_value()
    if("${ARCTICDB_COMPILER_CACHE}" STREQUAL "OFF")
        return()
    endif()

    if("${ARCTICDB_COMPILER_CACHE}" STREQUAL "CUSTOM")
        set(CACHE_PROGRAM_PATH ${ARCTICDB_COMPILER_CACHE_PATH})
        get_filename_component(CACHE_PROGRAM_NAME ${ARCTICDB_COMPILER_CACHE_PATH} NAME_WE)
    elseif("${ARCTICDB_COMPILER_CACHE}" STREQUAL "AUTO")
        set(CACHES_TO_TRY sccache ccache)
        foreach(CACHE_CANDIDATE ${CACHES_TO_TRY})
            find_program(CACHE_PROGRAM_PATH ${CACHE_CANDIDATE})
            if(CACHE_PROGRAM_PATH)
                set(CACHE_PROGRAM_NAME ${CACHE_CANDIDATE})
                break()
            else()
                unset(CACHE_PROGRAM_PATH CACHE)
            endif()
        endforeach()

        if(NOT CACHE_PROGRAM_PATH)
            message("ArcticDB could not find any compiler cache as a result of setting ARCTICDB_COMPILER_CACHE to AUTO")
            return()
        endif()
    else()
        string(TOLOWER ${ARCTICDB_COMPILER_CACHE} CACHE_PROGRAM_NAME)
        find_program(CACHE_PROGRAM_PATH ${CACHE_PROGRAM_NAME})
        if(NOT CACHE_PROGRAM_PATH)
            message(FATAL_ERROR
                "ARCTICDB_COMPILER_CACHE set to ${ARCTICDB_COMPILER_CACHE} but ${ARCTICDB_COMPILER_CACHE} cannot be "
                "found. Consider setting ARCTICDB_COMPILER_CACHE to CUSTOM and using ARCTICDB_COMPILER_CACHE_PATH"
            )
        endif()
    endif()

    if(MSVC AND CACHE_PROGRAM_PATH AND NOT ${ARCTICDB_PDB_GENERATION_MODE} STREQUAL "TU")
        message(FATAL_ERROR
            "In order to use a compiler cache with MSVC a .pdb must be generated per each TU. Consider setting "
            "ARCTICDB_PDB_GENERATION_MODE to TU."
        )
    endif()
    
    prepend_cache_if_not_existing("${CACHE_PROGRAM_PATH}" "${CACHE_PROGRAM_NAME}")
endfunction()

# Check if the value of the cache variable ARCTICDB_COMPILER_CACHE is correct. Fail if not.
function(validate_compiler_cache_value)
    string(TOUPPER ${ARCTICDB_COMPILER_CACHE} CACHE_UPPER)
    set(ALLOWED_VALUES "CCACHE" "SCCACHE" "OFF" "CUSTOM" "AUTO")
    if(NOT CACHE_UPPER IN_LIST ALLOWED_VALUES)
        message(FATAL_ERROR "Unknown value of the ARCTICDB_COMPILER_CACHE variable: ${ARCTICDB_COMPILER_CACHE}")
    endif()

    if("${ARCTICDB_COMPILER_CACHE}" STREQUAL "CUSTOM" AND NOT ARCTICDB_COMPILER_CACHE_PATH)
        message(FATAL_ERROR
            "ARCTICDB_COMPILER_CACHE set to ${ARCTICDB_COMPILER_CACHE} but ARCTICDB_COMPILER_CACHE_PATH was not set"
        )
    endif()
endfunction()

# Set the compiler cache by prepending to CMAKE_CXX_COMPILER_LAUNCHER and CMAKE_C_COMPILER_LAUNCHER
# CACHE_PROGRAM_PATH - full path to the compiler cache executable
# CACHE_NAME - short name of the executable (filename with not extension)
# * If the CMAKE_C(XX)_COMPILER_LAUNCHER already contains CACHE_PROGRAM_PATH does prints a status and does nothing
# * If the CMAKE_C(XX)_COMPILER_LAUNCHER already contains CACHE_NAME it replaces the existing entry with CACHE_PROGRAM_PATH
# * Otherwise prepends the compiler cache to CMAKE_C(XX)_COMPILER_LAUNCHER
function(prepend_cache_if_not_existing CACHE_PROGRAM_PATH CACHE_NAME)
    set(LAUNCHER_LISTS CMAKE_CXX_COMPILER_LAUNCHER CMAKE_C_COMPILER_LAUNCHER)
    foreach(LAUNCHER_LIST ${LAUNCHER_LISTS})
        set(CACHE_ALREADY_SET FALSE)
        foreach(LAUNCHER_PATH ${${LAUNCHER_LIST}})
            get_filename_component(LAUNCHER ${LAUNCHER_PATH} NAME_WE)
            if("${CACHE_PROGRAM_PATH}" STREQUAL "${LAUNCHER_PATH}")
                message(STATUS "Compiler cache ${CACHE_NAME}: ${CACHE_PROGRAM_PATH} already set.")
                set(CACHE_ALREADY_SET TRUE)
                break()
            elseif("${LAUNCHER}" STREQUAL "${CACHE_NAME}")
                if(NOT "${ARCTICDB_COMPILER_CACHE}" STREQUAL "AUTO")
                    message(
                        "Compiler cache ${CACHE_NAME} already set in ${LAUNCHER_LIST} with different path. Existing path: "
                        "${LAUNCHER}, required path: ${CACHE_PROGRAM_PATH}. Replacing the existing path with required path."
                    )
                    list(FIND ${LAUNCHER_LIST} "${LAUNCHER_PATH}" idx)
                    list(REMOVE_AT ${LAUNCHER_LIST} ${idx})
                    list(INSERT ${LAUNCHER_LIST} ${idx} "${CACHE_PROGRAM_PATH}")
                    set(${LAUNCHER_LIST} ${${LAUNCHER_LIST}} CACHE STRING "" FORCE)
                endif()
                set(CACHE_ALREADY_SET TRUE)
                break()
            endif()
        endforeach()
        if(NOT CACHE_ALREADY_SET)
            if(${LAUNCHER_LIST})
                message(
                    WARNING
                    "Setting compiler cache ${CACHE_PROGRAM_PATH} on top of non-empty ${LAUNCHER_LIST}:"
                    "${${LAUNCHER_LIST}}"
                )
            else()
                message("ArcticDB setting compiler cache ${CACHE_NAME}: ${CACHE_PROGRAM_PATH}")
            endif()
            list(PREPEND ${LAUNCHER_LIST} ${CACHE_PROGRAM_PATH})
            set(${LAUNCHER_LIST} ${${LAUNCHER_LIST}} CACHE STRING "" FORCE)
        endif()
    endforeach()
endfunction()