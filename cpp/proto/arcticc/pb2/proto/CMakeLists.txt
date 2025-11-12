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
        gcp_storage.proto
        azure_storage.proto
        nfs_backed_storage.proto
        mapped_file_storage.proto
        logger.proto
        )

set(proto_import_regex "import \"arcticc/pb2/([^\"]+\\.proto)\"")

# Slightly alter the original proto files to cope with grpc incompat with py3+
foreach(filename ${PROTO_IN_FILES})
    # Parse import statements
    set(in_file "../${filename}")
    file(READ "${in_file}" in_file_str)
    string(REGEX MATCHALL ${proto_import_regex} proto_imports "${in_file_str}")
    string(REGEX REPLACE ${proto_import_regex} "${CMAKE_CURRENT_BINARY_DIR}/\\1" in_file_depends "${proto_imports}")

    # Wrap the transform logic in a cmake script to allow tracking of file change relationships
    set(out_file "${CMAKE_CURRENT_BINARY_DIR}/${filename}")
    add_custom_command(OUTPUT "${out_file}"
            COMMAND ${CMAKE_COMMAND} "-DIN_FILE=${in_file}" "-DOUT_FILE=${out_file}" -P transform_proto.cmake
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
            DEPENDS ${in_file} ${in_file_depends})
    list(APPEND PROTO_ALTERED_FILES "${out_file}")
endforeach(filename)

# Generate c++ files from altered proto
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
INCLUDE_DIRECTORIES(${Protobuf_INCLUDE_DIRS})
SET(Protobuf_IMPORT_DIRS ${Protobuf_INCLUDE_DIRS})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER
        ${PROTO_ALTERED_FILES}
        )

ADD_LIBRARY(arcticdb_proto ${PROTO_HEADER} ${PROTO_SRC})

# Link against protobuf. Try modern target first, fallback to old-style variables.
if(TARGET protobuf::libprotobuf)
    target_link_libraries(arcticdb_proto PRIVATE protobuf::libprotobuf)
    # On Windows, when protobuf is built as a DLL, the generated code needs PROTOBUF_USE_DLLS
    # to properly import symbols from the DLL.
    if(WIN32)
        get_target_property(PROTOBUF_LIB_TYPE protobuf::libprotobuf TYPE)
        if(PROTOBUF_LIB_TYPE STREQUAL "SHARED_LIBRARY")
            target_compile_definitions(arcticdb_proto PRIVATE PROTOBUF_USE_DLLS=1)
        endif()
    endif()
else()
    # Fallback to old-style FindProtobuf variables
    target_link_libraries(arcticdb_proto PRIVATE ${Protobuf_LIBRARIES})
    # On Windows with conda, protobuf is typically a DLL
    if(WIN32 AND DEFINED ARCTICDB_USING_CONDA AND ARCTICDB_USING_CONDA)
        target_compile_definitions(arcticdb_proto PRIVATE PROTOBUF_USE_DLLS=1)
    endif()
endif()