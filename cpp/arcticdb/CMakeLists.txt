option(TEST "Build all tests." ON)
option(BUILD_WITH_REMOTERY "Build ArcticDB with Remotery (used for performance analysis)" OFF)
option(SSL_LINK "Link against SSL libraries." ON)
option(HIDE_LINKED_SYMBOLS "Passed `exclude-libs` through to linker, ensuring linked library symbols are not exported. Shrinks binary size." OFF)
option(STATIC_LINK_STD_LIB "Statically link the C++ standard libraries" OFF)

find_package(GTest CONFIG REQUIRED)
include(CTest)
include(GoogleTest)

include(GenerateExportHeader)

# System and third-party libraries with built-in find_package Config
find_package(AWSSDK REQUIRED COMPONENTS s3)

find_package(Boost REQUIRED)
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(Folly REQUIRED)
find_package(Gflags REQUIRED)
find_package(LZ4 REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(double-conversion REQUIRED)

if(NOT WIN32 AND ${SSL_LINK})
    find_package(Kerberos REQUIRED)
endif()

find_package(Protobuf REQUIRED)
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
find_package(zstd CONFIG REQUIRED)  # "CONFIG" bypasses our cpp/CMake/FindZstd.cmake module
find_package(unofficial-pcre REQUIRED)
find_package(Libevent CONFIG REQUIRED)

find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

set(ARCTICDB_MONGO_LIBS $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>)
set(Zstd_LIBRARY $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
set(ARCTICDB_INCLUDE_ROCKSDB 0)

find_library(Pcre_LIBRARY NAMES pcre PATHS /usr/local/lib/libpcre.a REQUIRED)
find_library(Sasl2_LIBRARY NAMES sasl2 libsasl2 PATHS /usr/local/lib/libsasl2.a REQUIRED)

find_path(BITMAGIC_INCLUDE_DIRS "bitmagic/bm.h")

set(FIND_LIBRARY_USE_LIB64_PATHS ON)

set(TEST_DISCOVERY_TIMEOUT, 60)

set(CMAKE_INSTALL_RPATH "\\$ORIGIN/lib")

## Core library without python bindings ##
set(arcticdb_srcs
        # header files
        async/async_store.hpp
        async/batch_read_args.hpp
        async/task_scheduler.hpp
        async/tasks.hpp
        codec/codec.hpp
        codec/encode_common.hpp
        codec/codec-inl.hpp
        codec/core.hpp
        codec/lz4.hpp
        codec/magic_words.hpp
        codec/passthrough.hpp
        codec/slice_data_sink.hpp
        codec/typed_block_encoder_impl.hpp
        codec/zstd.hpp
        column_store/block.hpp
        column_store/chunked_buffer.hpp
        column_store/column_data.hpp
        column_store/column.hpp
        column_store/column_utils.hpp
        column_store/memory_segment.hpp
        column_store/memory_segment_impl.hpp
        column_store/row_ref.hpp
        column_store/string_pool.hpp
        entity/atom_key.hpp
        entity/frame_and_descriptor.hpp
        entity/index_range.hpp
        entity/key.hpp
        entity/metrics.hpp
        entity/data_error.hpp
        entity/native_tensor.hpp
        entity/performance_tracing.hpp
        entity/protobuf_mappings.hpp
        entity/read_result.hpp
        entity/ref_key.hpp
        entity/serialized_key.hpp
        entity/type_conversion.hpp
        entity/types.hpp
        entity/types-inl.hpp
        entity/variant_key.hpp
        entity/versioned_item.hpp
        entity/descriptor_item.hpp
        log/log.hpp
        log/trace.hpp
        pipeline/column_mapping.hpp
        pipeline/column_stats.hpp
        pipeline/frame_data_wrapper.hpp
        pipeline/frame_slice.hpp
        pipeline/frame_utils.hpp
        pipeline/index_fields.hpp
        pipeline/index_segment_reader.hpp
        pipeline/index_utils.hpp
        pipeline/index_writer.hpp
        pipeline/input_tensor_frame.hpp
        pipeline/pipeline_common.hpp
        pipeline/pipeline_utils.hpp
        pipeline/python_output_frame.hpp
        pipeline/query.hpp
        pipeline/read_options.hpp
        pipeline/read_options.hpp
        pipeline/read_pipeline.hpp
        pipeline/slicing.hpp
        pipeline/string_pool_utils.hpp
        pipeline/string_pool_utils.hpp
        pipeline/value.hpp
        pipeline/value_set.hpp
        pipeline/write_frame.hpp
        pipeline/write_options.hpp
        processing/aggregation.hpp
        processing/component_manager.hpp
        processing/operation_dispatch.hpp
        processing/operation_dispatch_binary.hpp
        processing/operation_dispatch_unary.hpp
        processing/operation_types.hpp
        processing/signed_unsigned_comparison.hpp
        processing/processing_unit.hpp
        processing/bucketizer.hpp
        processing/clause.hpp
        processing/expression_context.hpp
        processing/expression_node.hpp
        storage/constants.hpp
        storage/common.hpp
        storage/config_resolvers.hpp
        storage/failure_simulation.hpp
        storage/library.hpp
        storage/library_index.hpp
        storage/library_manager.hpp
        storage/azure/azure_storage.hpp
        storage/lmdb/lmdb_storage.hpp
        storage/memory/memory_storage.hpp
        storage/mongo/mongo_client.hpp
        storage/mongo/mongo_instance.hpp
        storage/mongo/mongo_storage.hpp
        storage/object_store_utils.hpp
        storage/s3/nfs_backed_storage.hpp
        storage/s3/s3_client_accessor.hpp
        storage/s3/s3_storage_tool.hpp
        storage/storage_factory.hpp
        storage/storage_options.hpp
        storage/storage.hpp
        storage/storage_override.hpp
        storage/store.hpp
        stream/aggregator.hpp
        stream/aggregator-inl.hpp
        stream/append_map.hpp
        stream/merge_utils.hpp
        stream/index_aggregator.hpp
        stream/index.hpp
        stream/merge.hpp
        stream/merge.hpp
        stream/merge.hpp util/ref_counted_map.hpp
        stream/protobuf_mappings.hpp
        stream/row_builder.hpp
        stream/schema.hpp
        stream/segment_aggregator.hpp
        stream/stream_reader.hpp
        stream/stream_sink.hpp
        stream/stream_source.hpp
        stream/stream_utils.hpp
        stream/stream_writer.hpp
        toolbox/library_tool.hpp
        util/allocator.hpp
        util/bitset.hpp
        util/buffer.hpp
        util/buffer_pool.hpp
        util/clock.hpp
        util/configs_map.hpp
        util/constants.hpp
        util/constructors.hpp
        util/container_filter_wrapper.hpp
        util/cursored_buffer.hpp
        util/cursor.hpp
        util/decimal.hpp
        util/encoding_conversion.hpp
        util/exponential_backoff.hpp
        util/flatten_utils.hpp
        util/format_bytes.hpp
        util/format_date.hpp
        util/hash.hpp
        util/key_utils.hpp
        util/lock_table.hpp
        util/magic_num.hpp
        util/movable_priority_queue.hpp
        util/movable_priority_queue.hpp
        util/offset_string.hpp
        util/offset_string.hpp
        util/optional_defaults.hpp
        util/pb_util.hpp
        util/preconditions.hpp
        util/preprocess.hpp
        util/ranges_from_future.hpp
        util/ref_counted_map.hpp
        util/ref_counted_map.hpp
        util/regex_filter.hpp
        util/simple_string_hash.hpp
        util/slab_allocator.hpp
        util/sparse_utils.hpp
        util/storage_lock.hpp
        util/string_utils.hpp
        util/timeouts.hpp
        util/timer.hpp
        util/trace.hpp
        util/type_traits.hpp
        util/variant.hpp
        version/de_dup_map.hpp
        version/op_log.hpp
        version/schema_checks.hpp
        version/snapshot.hpp
        version/version_constants.hpp
        version/version_core.hpp
        version/version_core-inl.hpp
        version/versioned_engine.hpp
        version/version_functions.hpp
        version/version_log.hpp
        version/version_map_batch_methods.hpp
        version/version_map_entry.hpp
        version/version_map_entry.hpp
        version/version_map.hpp
        version/version_store_api.hpp
        version/version_store_objects.hpp
        version/version_utils.hpp
        # CPP files
        storage/memory/memory_storage.cpp
        async/async_store.cpp
        async/task_scheduler.cpp
        async/tasks.cpp
        codec/codec.cpp
        codec/encode_v1.cpp
        codec/encode_v2.cpp
        codec/encoding_sizes.cpp
        codec/segment.cpp
        codec/variant_encoded_field_collection.cpp
        column_store/chunked_buffer.cpp
        column_store/column.cpp
        column_store/column_data.cpp
        column_store/memory_segment_impl.cpp
        column_store/memory_segment_impl.cpp
        column_store/string_pool.cpp
        entity/data_error.cpp
        entity/field_collection.cpp
        entity/key.cpp
        entity/merge_descriptors.cpp
        entity/metrics.cpp
        entity/performance_tracing.cpp
        entity/types.cpp
        log/log.cpp
        pipeline/column_stats.cpp
        pipeline/frame_slice.cpp
        pipeline/frame_utils.cpp
        pipeline/index_segment_reader.cpp
        pipeline/index_utils.cpp
        pipeline/pipeline_context.cpp
        pipeline/python_output_frame.cpp
        pipeline/query.cpp
        pipeline/read_frame.cpp
        pipeline/slicing.cpp
        pipeline/string_pool_utils.cpp
        pipeline/value_set.cpp
        pipeline/write_frame.cpp
        python/normalization_checks.cpp
        processing/processing_unit.cpp
        processing/aggregation.cpp
        processing/clause.cpp
        processing/component_manager.cpp
        processing/expression_node.cpp
        processing/operation_dispatch.cpp
        processing/operation_dispatch_unary.cpp
        processing/operation_dispatch_binary.cpp
        processing/operation_dispatch_binary_eq.cpp
        processing/operation_dispatch_binary_gt.cpp
        processing/operation_dispatch_binary_lt.cpp
        processing/operation_dispatch_binary_operator.cpp
        python/python_to_tensor_frame.cpp
        storage/config_resolvers.cpp
        storage/failure_simulation.cpp
        storage/library_manager.cpp
        storage/azure/azure_storage.cpp
        storage/lmdb/lmdb_storage.cpp
        storage/mongo/mongo_client.cpp
        storage/mongo/mongo_instance.cpp
        storage/mongo/mongo_storage.cpp
        storage/s3/nfs_backed_storage.cpp
        storage/s3/s3_api.cpp
        storage/s3/s3_storage.cpp
        storage/s3/s3_storage_tool.cpp
        storage/storage_factory.cpp
        stream/aggregator.cpp
        stream/append_map.cpp
        toolbox/library_tool.cpp
        util/allocator.cpp
        util/buffer_pool.cpp
        util/configs_map.cpp
        util/decimal.cpp
        util/error_code.cpp
        util/global_lifetimes.cpp
        util/offset_string.cpp
        util/offset_string.cpp
        util/sparse_utils.cpp
        util/string_utils.cpp
        util/trace.cpp
        util/type_handler.cpp
        version/local_versioned_engine.cpp
        version/op_log.cpp
        version/snapshot.cpp
        version/symbol_list.cpp
        version/version_core.cpp
        version/version_store_api.cpp
        version/version_utils.cpp
        version/symbol_list.cpp
        version/version_map_batch_methods.cpp
        util/third_party/robin_hood.hpp
        )

add_library(arcticdb_core_object OBJECT ${arcticdb_srcs})

if (STATIC_LINK_STD_LIB)
    message("Statically linking standard libraries")
    set(standard_libraries
        -static-libgcc
        -static-libstdc++
    )
else()
    message("Dynamically linking standard libraries")
    set(standard_libraries
        stdc++
        gcc
    )
endif()

set (arcticdb_core_libraries
        pybind11::module # Transitively includes Python::Module or Python::Python as appropriate
        arcticdb_proto
        prometheus-cpp::push
        prometheus-cpp::pull
        ${standard_libraries}
        fmt::fmt
        protobuf::libprotobuf
        ${LZ4_LIBRARY}
        ${Zstd_LIBRARY}
        ${ARCTICDB_MONGO_LIBS}
        spdlog::spdlog
        Folly::folly # Transitively includes: double-conversion, gflags, glog, libevent, libssl, libcrypto, libiberty, libsodium
        Azure::azure-identity
        Azure::azure-storage-blobs
        xxhash_static
        unofficial::pcre::pcre
        msgpackc-cxx
        lmdb
        stdc++fs
        dl
        ${Sasl2_LIBRARY}
        Threads::Threads
)

add_compile_definitions(ARCTICDB_USING_STATIC_XXHASH)

set(arcticdb_core_includes
    ${BITMAGIC_INCLUDE_DIRS}
)

if (SSL_LINK)
    message("Linking against SSL")
    find_package(OpenSSL REQUIRED)
    list(APPEND arcticdb_core_libraries OpenSSL::SSL)
    list(APPEND arcticdb_core_libraries ${KERBEROS_LIBRARY})
    list(APPEND arcticdb_core_includes  ${KERBEROS_INCLUDE_DIR})
endif ()

target_link_libraries(arcticdb_core_object PUBLIC ${arcticdb_core_libraries})

target_include_directories(arcticdb_core_object
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
        ${arcticdb_core_includes}
        )

add_library(arcticdb_core_static STATIC $<TARGET_OBJECTS:arcticdb_core_object>)

target_include_directories(arcticdb_core_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
        )

target_link_libraries(arcticdb_core_static PUBLIC ${arcticdb_core_libraries})

add_library(arcticdb_core SHARED $<TARGET_OBJECTS:arcticdb_core_object>)

target_link_libraries(arcticdb_core PRIVATE
        ${arcticdb_core_libraries}
        ${AWSSDK_LINK_LIBRARIES}
        )

GENERATE_EXPORT_HEADER(arcticdb_core)

## Unit Tests ##
unset(Python_USE_STATIC_LIBS)
find_package(Python 3 COMPONENTS Interpreter Development REQUIRED)

set(unit_test_srcs
        util/test/test_storage_lock.cpp
        )

set(EXECUTABLE_PERMS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE) # 755

add_executable(test_unit_arcticdb ${unit_test_srcs})

install(TARGETS test_unit_arcticdb RUNTIME
            DESTINATION .
            PERMISSIONS ${EXECUTABLE_PERMS}
            COMPONENT Tests)

set(COMMON_PUBLIC_TEST_LIBRARIES
        arcticdb_core_static
        GTest::gtest
        GTest::gmock
        Python::Python
        curl
        krb5support
        k5crypto
        util
        atomic
        ${AWSSDK_LINK_LIBRARIES}
        arcticdb_core_static
        GTest::gtest_main
        )

target_link_libraries(test_unit_arcticdb
    PUBLIC
    ${COMMON_PUBLIC_TEST_LIBRARIES}
    )

target_include_directories(test_unit_arcticdb PRIVATE
        ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
        ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
        ${BITMAGIC_INCLUDE_DIRS}
        )

gtest_discover_tests(test_unit_arcticdb PROPERTIES DISCOVERY_TIMEOUT 60)
