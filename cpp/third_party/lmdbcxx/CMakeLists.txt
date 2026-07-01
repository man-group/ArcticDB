if (WIN32)
    #TODO: Fix this replacement in lmdb++.h
    #Windows doesnt define __cplusplus properly. There are workarounds but folly fails then
    FILE(READ "${CMAKE_CURRENT_SOURCE_DIR}/lmdb++.h" infile)
    STRING(REPLACE "#if __cplusplus < 201103L" "#ifndef __cplusplus" infile_mod "${infile}")
    FILE(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/lmdb++.h" "${infile_mod}")
endif ()

# Locate the patch executable.
# On Linux/macOS it is on PATH.  On Windows, Git for Windows ships patch.exe
# at a well-known path that is not on the system PATH by default.
find_program(LMDB_PATCH_EXECUTABLE
    NAMES patch patch.exe
    PATHS
        "C:/Program Files/Git/usr/bin"
        "C:/msys64/usr/bin"
    DOC "GNU patch used to apply LMDB CVE security fixes at configure time")

if(NOT LMDB_PATCH_EXECUTABLE)
    message(FATAL_ERROR
        "Cannot find the 'patch' command required to apply LMDB CVE security patches "
        "(CVE-2019-16224..16228).\n"
        "  Linux/macOS : install the system patch package (e.g. 'apt install patch').\n"
        "  Windows     : install Git for Windows (https://gitforwindows.org); patch.exe "
        "is included at 'C:/Program Files/Git/usr/bin/patch.exe'.")
endif()

# Copy LMDB sources to the build directory and apply CVE security patches.
# The submodule is kept unmodified; patched copies live in the build tree.
# To upgrade: update the submodule, then verify each patch still applies cleanly.
set(LMDB_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../lmdb/libraries/liblmdb")
set(LMDB_PATCHED_DIR "${CMAKE_CURRENT_BINARY_DIR}/lmdb_patched")
set(LMDB_PATCH_STAMP "${LMDB_PATCHED_DIR}/.cve_patched")

set(LMDB_PATCH_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/cve-2019-16224-validate-db-flags.patch"
    "${CMAKE_CURRENT_SOURCE_DIR}/cve-2019-16225-reject-dirty-pages.patch"
    "${CMAKE_CURRENT_SOURCE_DIR}/cve-2019-16226-validate-node-del-size.patch"
    "${CMAKE_CURRENT_SOURCE_DIR}/cve-2019-16227-guard-xcursor-null.patch"
    "${CMAKE_CURRENT_SOURCE_DIR}/cve-2019-16228-validate-psize.patch")

file(MAKE_DIRECTORY "${LMDB_PATCHED_DIR}")

set(_LMDB_NEEDS_REPATCH FALSE)
if(NOT EXISTS "${LMDB_PATCH_STAMP}")
    set(_LMDB_NEEDS_REPATCH TRUE)
elseif("${LMDB_SRC_DIR}/mdb.c" IS_NEWER_THAN "${LMDB_PATCH_STAMP}")
    set(_LMDB_NEEDS_REPATCH TRUE)
else()
    foreach(_patch ${LMDB_PATCH_FILES})
        if("${_patch}" IS_NEWER_THAN "${LMDB_PATCH_STAMP}")
            set(_LMDB_NEEDS_REPATCH TRUE)
        endif()
    endforeach()
endif()

if(_LMDB_NEEDS_REPATCH)
    file(COPY
        "${LMDB_SRC_DIR}/mdb.c"
        "${LMDB_SRC_DIR}/midl.c"
        DESTINATION "${LMDB_PATCHED_DIR}")
    # Normalise to LF so that patch(1) context lines match on Windows,
    # where git may check out CRLF if core.autocrlf=true.
    file(READ "${LMDB_PATCHED_DIR}/mdb.c" _mdb_content)
    string(REPLACE "\r\n" "\n" _mdb_content "${_mdb_content}")
    file(WRITE "${LMDB_PATCHED_DIR}/mdb.c" "${_mdb_content}")
    foreach(_patch ${LMDB_PATCH_FILES})
        execute_process(
            COMMAND "${LMDB_PATCH_EXECUTABLE}" -p0 -i "${_patch}" "${LMDB_PATCHED_DIR}/mdb.c"
            WORKING_DIRECTORY "${LMDB_PATCHED_DIR}"
            RESULT_VARIABLE LMDB_PATCH_RESULT
            OUTPUT_VARIABLE LMDB_PATCH_OUTPUT
            ERROR_VARIABLE  LMDB_PATCH_ERROR)
        if(NOT LMDB_PATCH_RESULT EQUAL 0)
            message(FATAL_ERROR "Failed to apply ${_patch}:\n${LMDB_PATCH_OUTPUT}\n${LMDB_PATCH_ERROR}")
        endif()
    endforeach()
    file(TOUCH "${LMDB_PATCH_STAMP}")
endif()

add_library(lmdb STATIC
    "${LMDB_PATCHED_DIR}/mdb.c"
    "${LMDB_PATCHED_DIR}/midl.c")

if (NOT WIN32)
    set_source_files_properties(
        "${LMDB_PATCHED_DIR}/mdb.c"
        "${LMDB_PATCHED_DIR}/midl.c"
        PROPERTIES COMPILE_FLAGS
        "-W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized -O2 -g")
endif ()

target_include_directories(lmdb PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${LMDB_PATCHED_DIR}>)
