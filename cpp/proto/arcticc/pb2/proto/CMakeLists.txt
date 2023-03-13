INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)

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
    FILE(READ "../${filename}" infile)
    STRING(REPLACE "import \"arcticc/pb2/" "import \"" infile_mod "${infile}")
    FILE(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${filename}" "${infile_mod}")
    list (APPEND PROTO_ALTERED_FILES "${CMAKE_CURRENT_BINARY_DIR}/${filename}")
endforeach(filename)

add_custom_command(OUTPUT ${PROTO_ALTERED_FILES}
        COMMAND echo
        SOURCES ${PROTO_IN_FILES})

# Generate c++ files from altered proto
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})
SET(Protobuf_IMPORT_DIRS ${Protobuf_INCLUDE_DIRS})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER
        ${PROTO_ALTERED_FILES}
        )

ADD_LIBRARY(arcticdb_proto ${PROTO_HEADER} ${PROTO_SRC})