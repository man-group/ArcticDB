# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

set(LIBRARY_DIRECTORY lib)
set(RUNTIME_DIRECTORY bin)
# Set the default lib installation path on GNU systems with GNUInstallDirs
if (UNIX AND NOT APPLE)
    include(GNUInstallDirs)
    set(LIBRARY_DIRECTORY ${CMAKE_INSTALL_LIBDIR})
    set(RUNTIME_DIRECTORY ${CMAKE_INSTALL_BINDIR})

    # this is the absolute dumbest thing in the world, but find_package won't work without it
    # also I verified this is correctly NOT "lib64" when CMAKE_C_FLAGS includes "-m32"
    if (${LIBRARY_DIRECTORY} STREQUAL "lib64")
        set(FIND_LIBRARY_USE_LIB64_PATHS true)
    endif()
endif()


function(aws_prepare_shared_lib_exports target)
    if (BUILD_SHARED_LIBS)
        install(TARGETS ${target}
                EXPORT ${target}-targets
                ARCHIVE
                DESTINATION ${LIBRARY_DIRECTORY}
                COMPONENT Development
                LIBRARY
                DESTINATION ${LIBRARY_DIRECTORY}
                NAMELINK_SKIP
                COMPONENT Runtime
                RUNTIME
                DESTINATION ${RUNTIME_DIRECTORY}
                COMPONENT Runtime)
        install(TARGETS ${target}
                EXPORT ${target}-targets
                LIBRARY
                DESTINATION ${LIBRARY_DIRECTORY}
                NAMELINK_ONLY
                COMPONENT Development)
    else()
        install(TARGETS ${target}
                EXPORT ${target}-targets
                ARCHIVE DESTINATION ${LIBRARY_DIRECTORY}
                COMPONENT Development)
    endif()
endfunction()

function(aws_prepare_symbol_visibility_args target lib_prefix)
    if (BUILD_SHARED_LIBS)
        target_compile_definitions(${target} PUBLIC "-D${lib_prefix}_USE_IMPORT_EXPORT")
        target_compile_definitions(${target} PRIVATE "-D${lib_prefix}_EXPORTS")
    endif()
endfunction()

# Strips debug info from the target shared library or executable, and puts it in a $<TARGET_FILE:${target}>.dbg
# archive, then links the original binary to the dbg archive so gdb will find it
# This only applies to Unix shared libs and executables, windows has pdbs.
# This is only done on Release and RelWithDebInfo build types
function(aws_split_debug_info target)
    if (UNIX AND CMAKE_BUILD_TYPE MATCHES Rel AND CMAKE_STRIP AND CMAKE_OBJCOPY)
        get_target_property(target_type ${target} TYPE)
        if (target_type STREQUAL "SHARED_LIBRARY" OR target_type STREQUAL "EXECUTABLE")
            add_custom_command(TARGET ${target} POST_BUILD
                COMMAND ${CMAKE_OBJCOPY} --only-keep-debug $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.dbg
                COMMAND ${CMAKE_STRIP} --strip-debug --strip-unneeded $<TARGET_FILE:${target}>
                COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE:${target}>.dbg $<TARGET_FILE:${target}>)
        endif()
    endif()
endfunction()
