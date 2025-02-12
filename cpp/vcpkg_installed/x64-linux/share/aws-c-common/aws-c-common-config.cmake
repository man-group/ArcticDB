set(THREADS_PREFER_PTHREAD_FLAG ON)

if(WIN32 OR UNIX OR APPLE)
    find_package(Threads REQUIRED)
endif()

macro(aws_load_targets type)
    include(${CMAKE_CURRENT_LIST_DIR}/aws-c-common-targets.cmake)
endmacro()

# try to load the lib follow BUILD_SHARED_LIBS. Fall back if not exist.
if (BUILD_SHARED_LIBS)
    if (EXISTS "${CMAKE_CURRENT_LIST_DIR}/shared")
        aws_load_targets(shared)
    else()
        aws_load_targets(static)
    endif()
else()
    if (EXISTS "${CMAKE_CURRENT_LIST_DIR}/static")
        aws_load_targets(static)
    else()
        aws_load_targets(shared)
    endif()
endif()
