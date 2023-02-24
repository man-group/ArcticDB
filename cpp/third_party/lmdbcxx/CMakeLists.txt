if (NOT WIN32)
    set_source_files_properties(mdb.c midl.c
        PROPERTIES COMPILE_FLAGS
        "-W -Wall -Wno-unused-parameter -Wbad-function-cast -Wuninitialized -O2 -g")
else ()
    #TODO: Fix this replacement in lmdb++.h
    #Windows doesnt define __cplusplus properly. There are workarounds but folly fails then
    FILE(READ "${CMAKE_CURRENT_SOURCE_DIR}/lmdb++.h" infile)
    STRING(REPLACE "#if __cplusplus < 201103L" "#ifndef __cplusplus" infile_mod "${infile}")
    FILE(WRITE "${CMAKE_CURRENT_SOURCE_DIR}/lmdb++.h" "${infile_mod}")
endif ()


add_library(lmdb STATIC mdb.c midl.c)

target_include_directories(lmdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

