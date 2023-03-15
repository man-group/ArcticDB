INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)

if (NOT WIN32)
    find_program (SED_TOOL NAMES sed)
    if (NOT SED_TOOL)
        message (FATAL_ERROR "Unable to find sed")
    endif (NOT SED_TOOL)
    message(${SED_TOOL})
endif ()

SET(PROTO_IN_FILES
		utils.proto
        descriptors.proto
        encoding.proto
        storage.proto
        config.proto
        lmdb_storage.proto
        mongo_storage.proto
        in_memory_storage.proto
        s3_storage.proto
        nfs_backed_storage.proto
        logger.proto
        )

# Slightly alter the original proto files to cope with grpc incompat with py3+
foreach(filename ${PROTO_IN_FILES})
    list (APPEND PROTO_ALTERED_FILES "${CMAKE_CURRENT_BINARY_DIR}/${filename}")
    list (APPEND PROTO_ABS_FILE  "${PROJECT_SOURCE_DIR}/proto/arcticc/pb2/${filename}")
endforeach(filename)

if (NOT WIN32)
# This runs alter_proto.sh systematically, but it only alters the underlying
# files when their content changes by manually keeping track of input hash.
    add_custom_command(OUTPUT ${PROTO_ALTERED_FILES}
            COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/alter_proto.sh ARGS ${PROTO_ABS_FILE}
            DEPENDS ${PROTO_ABS_FILE}
            )
else ()
    foreach(filename ${PROTO_IN_FILES})
        FILE(READ "${PROJECT_SOURCE_DIR}/proto/arcticc/pb2/${filename}" infile)
        STRING(REPLACE "import \"arcticc/" "import \"" infile_mod "${infile}")
        FILE(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${filename}" "${infile_mod}")
    endforeach(filename)
endif ()

# Generate c++ files from altered proto
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})
SET(Protobuf_IMPORT_DIRS ${Protobuf_INCLUDE_DIRS})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER
        ${PROTO_ALTERED_FILES}
        )

ADD_LIBRARY(arcticdb_proto ${PROTO_HEADER} ${PROTO_SRC})