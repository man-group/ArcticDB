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
    elseif("${ARCTICDB_COMPILER_CACHE}" STREQUAL "AUTO")
        set(CACHES_TO_TRY sccache ccache)
        foreach(CACHE_PROGRAM_NAME ${CACHES_TO_TRY})
            find_program(CACHE_PROGRAM_PATH ${CACHE_PROGRAM_NAME})
            if(CACHE_PROGRAM_PATH)
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
                "ARCTICDB_COMPILER_CACHE set to ${ARCTICDB_COMPILER_CACHE} but ${ARCTICDB_COMPILER_CACHE} cannot be
                found. Consider setting ARCTICDB_COMPILER_CACHE to CUSTOM and using ARCTICDB_COMPILER_CACHE_PATH"
            )
        endif()
    endif()

    if(MSVC AND CACHE_PROGRAM_PATH AND NOT ${ARCTICDB_PDB_GENERATION_MODE} STREQUAL "TU")
        message(FATAL_ERROR
            "In order to use a compiler cache with MSVC a .pdb must be generated per each TU. Consider setting
            ARCTICDB_PDB_GENERATION_MODE to TU."
        )
    endif()

    
    if(NOT "${ARCTICDB_COMPILER_CACHE}" STREQUAL "AUTO" OR NOT CMAKE_C_COMPILER_LAUNCHER)
        if(CMAKE_CXX_COMPILER_LAUNCHER)
            message(
                WARNING
                "Setting C compiler cache ${CACHE_PROGRAM_PATH} on top of non-empty CMAKE_C_COMPILER_LAUNCHER: \
                ${CMAKE_CXX_COMPILER_LAUNCHER}"
            )
        else()
            message("ArcticDB using C compiler cache ${CACHE_PROGRAM_PATH}. ARCTICDB_COMPILER_CACHE: ${ARCTICDB_COMPILER_CACHE}")
        endif()
        list(PREPEND CMAKE_C_COMPILER_LAUNCHER ${CACHE_PROGRAM_PATH})
        set(CMAKE_C_COMPILER_LAUNCHER ${CMAKE_C_COMPILER_LAUNCHER} PARENT_SCOPE)
    endif()

    if(NOT "${ARCTICDB_COMPILER_CACHE}" STREQUAL "AUTO" OR NOT CMAKE_CXX_COMPILER_LAUNCHER)
        message("ArcticDB using CXX compiler cache ${CACHE_PROGRAM_PATH}. ARCTICDB_COMPILER_CACHE: ${ARCTICDB_COMPILER_CACHE}")
        if(CMAKE_CXX_COMPILER_LAUNCHER)
            message(
                WARNING
                "Setting CXX compiler cache ${CACHE_PROGRAM_PATH} on top of non-empty CMAKE_CXX_COMPILER_LAUNCHER: \
                ${CMAKE_CXX_COMPILER_LAUNCHER}"
            )
        else()
            message("ArcticDB using CXX compiler cache ${CACHE_PROGRAM_PATH}. ARCTICDB_COMPILER_CACHE: ${ARCTICDB_COMPILER_CACHE}")
        endif()
        list(PREPEND CMAKE_CXX_COMPILER_LAUNCHER ${CACHE_PROGRAM_PATH})
        set(CMAKE_CXX_COMPILER_LAUNCHER ${CMAKE_CXX_COMPILER_LAUNCHER} PARENT_SCOPE)
    endif()
endfunction()

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
