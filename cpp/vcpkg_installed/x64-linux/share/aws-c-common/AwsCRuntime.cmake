# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

# This function detects the local host's c runtime and writes a tag into the supplied output variable
# Output is "cruntime" on non-Linux platforms.  Output is "glibc" or "musl" on Linux platforms.
#
# Intended usage is for managed language CRTs to use this function to build native artifact paths, facilitating
# support for alternative C runtimes like Musl.
#
function(aws_determine_local_c_runtime target)
    if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
        execute_process(COMMAND "ldd" "--version" OUTPUT_VARIABLE AWS_LDD_OUTPUT ERROR_VARIABLE AWS_LDD_OUTPUT)
        string(TOLOWER "${AWS_LDD_OUTPUT}" AWS_LDD_OUTPUT_LOWER)
        message(STATUS "ldd output lower: ${AWS_LDD_OUTPUT_LOWER}")
        string(FIND "${AWS_LDD_OUTPUT_LOWER}" "musl" AWS_MUSL_INDEX)
        string(FIND "${AWS_LDD_OUTPUT_LOWER}" "glibc" AWS_GLIBC_INDEX)
        string(FIND "${AWS_LDD_OUTPUT_LOWER}" "gnu" AWS_GNU_INDEX)
        if (NOT(${AWS_MUSL_INDEX} EQUAL -1))
            message(STATUS "MUSL libc detected")
            set(${target} "musl" PARENT_SCOPE)
        else()
            if ((NOT(${AWS_GLIBC_INDEX} EQUAL -1)) OR (NOT(${AWS_GNU_INDEX} EQUAL -1)))
                message(STATUS "Gnu libc detected")
            else()
                message(STATUS "Could not determine C runtime, defaulting to gnu libc")
            endif()
            set(${target} "glibc" PARENT_SCOPE)
        endif()
    else()
        set(${target} "cruntime" PARENT_SCOPE)
    endif()
endfunction()
