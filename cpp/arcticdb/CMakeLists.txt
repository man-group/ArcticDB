option(TEST "Build all tests." ON)
option(BUILD_WITH_REMOTERY "Build ArcticDB with Remotery (used for performance analysis)" OFF)
option(SSL_LINK "Link against SSL libraries." ON)
option(HIDE_LINKED_SYMBOLS "Passed `exclude-libs` through to linker, ensuring linked library symbols are not exported. Shrinks binary size." ON)

find_package(GTest CONFIG REQUIRED)
include(CTest)
include(GoogleTest)

include(GenerateExportHeader)

# System and third-party libraries with built-in find_package Config
cmake_policy(PUSH)
    if (EXISTS "/usr/local/lib64/aws-c-cal/cmake/modules/FindLibCrypto.cmake") # Workaround old AWS SDK bug
        cmake_policy(SET CMP0045 OLD)
    endif()
    find_package(AWSSDK REQUIRED COMPONENTS s3 identity-management)
cmake_policy(POP)

find_package(Boost REQUIRED)
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(Folly REQUIRED)
find_package(Gflags REQUIRED)
find_package(LZ4 REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(Threads REQUIRED)
find_package(double-conversion REQUIRED)
find_package(unordered_dense CONFIG REQUIRED)

if(NOT WIN32 AND ${SSL_LINK})
    find_package(Kerberos REQUIRED)
endif()

# Required to allow protobuf to use the correct version of absl:
# https://github.com/protocolbuffers/protobuf/issues/12292#issuecomment-1529680040
find_package(protobuf REQUIRED CONFIG)
find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
find_package(xxHash REQUIRED)
find_package(zstd CONFIG REQUIRED)  # "CONFIG" bypasses our cpp/CMake/FindZstd.cmake module

find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

if(${BUILD_WITH_REMOTERY})
    add_compile_definitions(USE_REMOTERY)
endif()

if(NOT ${ARCTICDB_USING_CONDA})
    find_package(unofficial-pcre REQUIRED)
    find_package(Libevent CONFIG REQUIRED)
    set(ARCTICDB_MONGO_LIBS $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>)
    set(Zstd_LIBRARY $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
else()

    # ARCTICDB_USING_CONDA is used in header of the files including vendored
    # sources to be able to use dependencies via conda at compile time.
    # TODO: rely on CMake directives instead of `ARCTICDB_USING_CONDA`.
    add_compile_definitions(ARCTICDB_USING_CONDA)
    # https://conda-forge.org/docs/maintainer/knowledge_base.html#newer-c-features-with-old-sdk
    add_compile_definitions(_LIBCPP_DISABLE_AVAILABILITY)

    SET(HIDE_LINKED_SYMBOLS OFF)
    
    find_package(pybind11 REQUIRED)
    find_package(PCRE REQUIRED)
    find_package(Libevent  REQUIRED)
    find_package(semimap REQUIRED)

    find_package(recycle REQUIRED)
    find_package(msgpack-c REQUIRED)
    find_package(LMDB REQUIRED)
    find_package(LMDBXX REQUIRED)

    find_package(EnTT REQUIRED)

    if(${BUILD_WITH_REMOTERY})
        find_package(Remotery REQUIRED)
    endif()

    set(ARCTICDB_MONGO_LIBS mongo::mongocxx_shared mongo::bsoncxx_shared)

    set(Zstd_LIBRARY zstd::libzstd_shared)

    get_target_property(NEW_FOLLY_DEPS Folly::folly_deps INTERFACE_INCLUDE_DIRECTORIES)
    message(STATUS "NEW_FOLLY_DEPS" ${NEW_FOLLY_DEPS})
    list(REMOVE_ITEM NEW_FOLLY_DEPS "/usr/local/opt/openssl/include" )
    set_target_properties(Folly::folly_deps PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${NEW_FOLLY_DEPS}")
endif()

# Have to statically link xxhash as inlining causes non-deterministic hashing, and cannot dynamically link
# # since `XXH_state_t` which we rely on is only available when statically linking.
add_library(xxHash STATIC IMPORTED)
set_target_properties(xxHash PROPERTIES IMPORTED_LOCATION ${xxHash_LIBRARY})
set_target_properties(xxHash PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${xxHash_INCLUDE_DIR})

# Libraries baked into the internal and external Linux images are found by searching various paths
# On Windows, vcpkg will provide a "Config" which takes precedence
find_library(Pcre_LIBRARY NAMES pcre PATHS /usr/local/lib/libpcre.a REQUIRED)
if (WIN32)
    find_package(Iconv)
else ()
    find_library(Sasl2_LIBRARY NAMES sasl2 libsasl2 PATHS /usr/local/lib/libsasl2.a REQUIRED)
endif ()


find_path(BITMAGIC_INCLUDE_DIRS "bitmagic/bm.h")

set(FIND_LIBRARY_USE_LIB64_PATHS ON)
include(PythonUtils) # Must be called before Pybind (third_party) to override its finding mechanism

python_utils_dump_vars_if_enabled("After Pybind")
python_utils_check_include_dirs("accepted by pybind")
python_utils_check_version_is_as_expected()
################################################################################
##            Interlibrary dependencies
################################################################################
##            +--------------+
##            |arcticdb_core<--------------+
##            +--------------+              |
##                    ^                     |
##                    |                     |
##            +----------------+            |
##       +---->arcticdb_python|            |
##       |    +-------------^--+     +-------------------+
##       |                           |test_unit_arcticdb|
##       |                           +-------------------+
##       |
##  +------------+
##  |arcticdb_ext|
##  +------------+
##
################################################################################
set(TEST_DISCOVERY_TIMEOUT, 60)

IF("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND NOT DEFINED ENV{DEBUG_BUILD})
    set(ENV{DEBUG_BUILD} TRUE)
ENDIF()

IF($ENV{DEBUG_BUILD})
    # Add macros definition here
    message("Setting debug flags")
    ADD_DEFINITIONS(-DDEBUG_BUILD)
ELSE()
    message(STATUS "Not setting debug flags")
ENDIF()

IF(DEFINED ENV{USE_SLAB_ALLOCATOR})
    message(STATUS "Setting USE_SLAB_ALLOCATOR")
    ADD_DEFINITIONS(-DUSE_SLAB_ALLOCATOR)
ENDIF()

set(CMAKE_INSTALL_RPATH "\\$ORIGIN/lib")

if (NOT MSVC)
    if(NOT ${ARCTICDB_USING_CONDA})
        # Compilers on conda-forge are relatively strict.
        # For now, we do not want to treat warnings as errors for those builds.
        add_compile_options("-Werror")
    endif()
    add_compile_options("-ggdb")
    add_compile_options("-fvisibility=hidden")
    add_compile_options("-fno-omit-frame-pointer")
else()
    add_compile_options(
        /WX
        /wd4250 # Suppress inherits via dominance warning
        /we4189 # Unused local variable is an error
        /we4100 # Unused function parameter is an error
    )
endif ()

## Core library without python bindings ##
set(arcticdb_srcs
        storage/memory_layout.hpp
        # header files
        async/async_store.hpp
        async/batch_read_args.hpp
        async/bit_rate_stats.hpp
        async/task_scheduler.hpp
        async/tasks.hpp
        codec/codec.hpp
        codec/encode_common.hpp
        codec/codec-inl.hpp
        codec/core.hpp
        codec/lz4.hpp
        codec/magic_words.hpp
        codec/passthrough.hpp
        codec/protobuf_mappings.hpp
        codec/slice_data_sink.hpp
        codec/segment_header.hpp
        codec/segment_identifier.hpp
        codec/typed_block_encoder_impl.hpp
        codec/zstd.hpp
        column_store/block.hpp
        column_store/chunked_buffer.hpp
        column_store/column_data.hpp
        column_store/column_data_random_accessor.hpp
        column_store/column.hpp
        column_store/column_utils.hpp
        column_store/key_segment.hpp
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
        entity/descriptors.hpp
        entity/native_tensor.hpp
        entity/output_type.hpp
        entity/performance_tracing.hpp
        entity/protobuf_mappings.hpp
        entity/read_result.hpp
        entity/ref_key.hpp
        entity/serialized_key.hpp
        entity/type_conversion.hpp
        entity/types.hpp
        entity/type_utils.hpp
        entity/types_proto.hpp
        entity/types-inl.hpp
        entity/variant_key.hpp
        entity/versioned_item.hpp
        entity/descriptor_item.hpp
        entity/field_collection.hpp
        entity/field_collection_proto.hpp
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
        pipeline/value.hpp
        pipeline/value_set.hpp
        pipeline/write_frame.hpp
        pipeline/write_options.hpp
        python/python_strings.hpp
        python/python_handler_data.hpp
        pipeline/string_reducers.hpp
        processing/aggregation_utils.hpp
        processing/component_manager.hpp
        processing/operation_dispatch.hpp
        processing/operation_dispatch_binary.hpp
        processing/operation_dispatch_unary.hpp
        processing/operation_types.hpp
        processing/signed_unsigned_comparison.hpp
        processing/processing_unit.hpp
        processing/bucketizer.hpp
        processing/clause.hpp
        processing/clause_utils.hpp
        processing/expression_context.hpp
        processing/expression_node.hpp
        processing/query_planner.hpp
        processing/sorted_aggregation.hpp
        processing/unsorted_aggregation.hpp
        storage/constants.hpp
        storage/common.hpp
        storage/config_resolvers.hpp
        storage/coalesced/multi_segment_header.hpp
        storage/coalesced/multi_segment_utils.hpp
        storage/failure_simulation.hpp
        storage/library.hpp
        storage/library_index.hpp
        storage/library_manager.hpp
        storage/storage_mock_client.hpp
        storage/azure/azure_client_wrapper.hpp
        storage/azure/azure_mock_client.hpp
        storage/azure/azure_real_client.hpp
        storage/azure/azure_storage.hpp
        storage/lmdb/lmdb.hpp
        storage/lmdb/lmdb_client_wrapper.hpp
        storage/lmdb/lmdb_mock_client.hpp
        storage/lmdb/lmdb_real_client.hpp
        storage/lmdb/lmdb_storage.hpp
        storage/memory/memory_storage.hpp
        storage/memory/memory_storage.cpp
        storage/mongo/mongo_client.hpp
        storage/mongo/mongo_instance.hpp
        storage/mongo/mongo_client_wrapper.hpp
        storage/mongo/mongo_mock_client.hpp
        storage/mongo/mongo_storage.hpp
        storage/object_store_utils.hpp
        storage/file/file_store.hpp
        storage/file/mapped_file_storage.hpp
        storage/file/file_store.hpp
        storage/single_file_storage.hpp
        storage/s3/nfs_backed_storage.hpp
        storage/s3/s3_client_wrapper.hpp
        storage/s3/s3_storage_tool.hpp
        storage/s3/s3_settings.hpp
        storage/storage_factory.hpp
        storage/storage_options.hpp
        storage/storage.hpp
        storage/storage_override.hpp
        storage/store.hpp
        stream/aggregator.hpp
        stream/aggregator-inl.hpp
        stream/append_map.hpp
        stream/merge_utils.hpp
        stream/piloted_clock.hpp
        stream/index_aggregator.hpp
        stream/index.hpp
        stream/merge.hpp
        stream/merge.hpp
        stream/merge.hpp
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
        util/spinlock.hpp
        util/key_utils.hpp
        util/lock_table.hpp
        util/lru_cache.hpp
        util/magic_num.hpp
        util/movable_priority_queue.hpp
        util/movable_priority_queue.hpp
        util/memory_mapped_file.hpp
        util/name_validation.hpp
        util/native_handler.hpp
        util/offset_string.hpp
        util/optional_defaults.hpp
        util/pb_util.hpp
        util/preconditions.hpp
        util/preprocess.hpp
        util/ranges_from_future.hpp
        util/regex_filter.hpp
        util/simple_string_hash.hpp
        util/slab_allocator.hpp
        util/sparse_utils.hpp
        util/storage_lock.hpp
        util/reliable_storage_lock.hpp
        util/reliable_storage_lock-inl.hpp
        util/string_utils.hpp
        util/thread_cached_int.hpp
        util/timeouts.hpp
        util/timer.hpp
        util/trace.hpp
        util/lazy.hpp
        util/type_traits.hpp
        util/variant.hpp
        util/gil_safe_py_none.hpp
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
        async/async_store.cpp
        async/bit_rate_stats.cpp
        async/task_scheduler.cpp
        async/tasks.cpp
        codec/codec.cpp
        codec/encode_v1.cpp
        codec/encode_v2.cpp
        codec/encoded_field.cpp
        codec/protobuf_mappings.cpp
        codec/segment.cpp
        codec/segment_header.cpp
        column_store/chunked_buffer.cpp
        column_store/column.cpp
        column_store/column_data.cpp
        column_store/key_segment.cpp
        column_store/memory_segment_impl.cpp
        column_store/memory_segment_impl.cpp
        column_store/string_pool.cpp
        entity/data_error.cpp
        entity/field_collection.cpp
        entity/field_collection_proto.cpp
        entity/key.cpp
        entity/merge_descriptors.cpp
        entity/metrics.cpp
        entity/performance_tracing.cpp
        entity/protobuf_mappings.cpp
        entity/types.cpp
        entity/type_utils.cpp
        entity/types_proto.cpp
        log/log.cpp
        pipeline/column_mapping.cpp
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
        python/python_strings.cpp
        processing/processing_unit.cpp
        processing/aggregation_utils.cpp
        processing/clause.cpp
        processing/clause_utils.cpp
        processing/component_manager.cpp
        processing/expression_node.cpp
        processing/operation_dispatch.cpp
        processing/operation_dispatch_unary.cpp
        processing/operation_dispatch_binary.cpp
        processing/operation_dispatch_binary_eq.cpp
        processing/operation_dispatch_binary_neq.cpp
        processing/operation_dispatch_binary_gt.cpp
        processing/operation_dispatch_binary_gte.cpp
        processing/operation_dispatch_binary_lt.cpp
        processing/operation_dispatch_binary_lte.cpp
        processing/operation_dispatch_binary_operator_plus.cpp
        processing/operation_dispatch_binary_operator_minus.cpp
        processing/operation_dispatch_binary_operator_times.cpp
        processing/operation_dispatch_binary_operator_divide.cpp
        processing/query_planner.cpp
        processing/sorted_aggregation.cpp
        processing/unsorted_aggregation.cpp
        python/python_to_tensor_frame.cpp
        storage/config_resolvers.cpp
        storage/failure_simulation.cpp
        storage/library_manager.cpp
        storage/azure/azure_storage.cpp
        storage/azure/azure_real_client.cpp
        storage/azure/azure_mock_client.cpp
        storage/lmdb/lmdb_mock_client.cpp
        storage/lmdb/lmdb_real_client.cpp
        storage/lmdb/lmdb_storage.cpp
        storage/file/mapped_file_storage.cpp
        storage/mongo/mongo_client.cpp
        storage/mongo/mongo_instance.cpp
        storage/mongo/mongo_mock_client.cpp
        storage/mongo/mongo_storage.cpp
        storage/s3/nfs_backed_storage.cpp
        storage/s3/ec2_utils.cpp
        storage/s3/s3_api.cpp
        storage/s3/s3_real_client.cpp
        storage/s3/s3_mock_client.cpp
        storage/s3/s3_storage.cpp
        storage/s3/s3_storage_tool.cpp
        storage/storage_factory.cpp
        stream/aggregator.cpp
        stream/append_map.cpp
        stream/index.cpp
        stream/piloted_clock.cpp
        stream/protobuf_mappings.cpp
        toolbox/library_tool.cpp
        util/allocator.cpp
        util/buffer_holder.cpp
        util/buffer_pool.cpp
        util/configs_map.cpp
        util/decimal.cpp
        util/error_code.cpp
        util/global_lifetimes.cpp
        util/memory_mapped_file.hpp
        util/name_validation.cpp
        util/offset_string.cpp
        util/sparse_utils.cpp
        util/string_utils.cpp
        util/timer.cpp
        util/trace.cpp
        util/type_handler.cpp
        version/key_block.hpp
        version/key_block.cpp
        util/gil_safe_py_none.cpp
        version/local_versioned_engine.cpp
        version/op_log.cpp
        version/snapshot.cpp
        version/symbol_list.cpp
        version/version_core.cpp
        version/version_store_api.cpp
        version/version_utils.cpp
        version/version_map_batch_methods.cpp)

add_library(arcticdb_core_object OBJECT ${arcticdb_srcs})

if(${ARCTICDB_USE_PCH})
    message(STATUS "Using precompiled headers for target arcticdb_core_object")
    set(BASE_PCH
        # STL
        <memory>
        <cstdint>
        <unordered_map>
        <vector>
        <stdexcept>
        <string>
        <array>
        <variant>
        <optional>
        <type_traits>
        <cstddef>
        <climits>
        <mutex>
        <string_view>
        <chrono>
        <atomic>
        <limits>
        <cstdlib>
        <utility>
        <iomanip>
        <sstream>
        <ctime>
        <map>
        <unordered_set>
        <iostream>
        <cassert>
        <functional>
        <list>
        <random>
        <fstream>
        <any>
        <cstdio>
        <algorithm>
        <queue>
        <exception>
        <deque>
        <ranges>
        <shared_mutex>
        <set>
        <thread>
        <filesystem>
        <typeinfo>
        <concepts>
        <numeric>

        # Boost
        <boost/iterator_adaptors.hpp>
        <boost/circular_buffer.hpp>
        <boost/core/noncopyable.hpp>
        <boost/algorithm/string.hpp>
        <boost/container/small_vector.hpp>
        <boost/iterator/iterator_facade.hpp>
        <boost/noncopyable.hpp>

        # Python
        # TODO: Won't be needed when we move all python conde to handlers outside of the core
        <Python.h>
        <pybind11/stl.h>
        <pybind11/pybind11.h>
        <pybind11/numpy.h>

        # Proto
        <google/protobuf/io/zero_copy_stream_impl.h>
        <google/protobuf/text_format.h>
        <google/protobuf/any.pb.h>
        <google/protobuf/any.h>
        <google/protobuf/util/message_differencer.h>
        <google/protobuf/arena.h>

        # Proto generated
        <descriptors.pb.h>
        <storage.pb.h>
        <lmdb_storage.pb.h>
        <mapped_file_storage.pb.h>
        <encoding.pb.h>
        <in_memory_storage.pb.h>
        <mongo_storage.pb.h>
        <s3_storage.pb.h>
        <utils.pb.h>
        <config.pb.h>
        <azure_storage.pb.h>
        <nfs_backed_storage.pb.h>

        # Folly
        <folly/Function.h>
        <folly/hash/Hash.h>
        <folly/Range.h>
        <folly/container/Enumerate.h>
        <folly/Poly.h>
        <folly/ScopeGuard.h>
        <folly/portability/Time.h>
        <folly/gen/Base.h>


        # FMT
        <fmt/core.h>
        <fmt/format.h>
        <fmt/compile.h>
        <fmt/std.h>

        # SPD Log
        <spdlog/spdlog.h>

        # Bit Magic
        <bitmagic/bm.h>
        <bitmagic/bmserial.h>

        # Misc
        <ankerl/unordered_dense.h>
        <third_party/recycle/src/recycle/shared_pool.hpp>
        <xxhash.h>
        <pcre.h>
    )
    target_precompile_headers(arcticdb_core_object PRIVATE ${BASE_PCH})
endif()

if (WIN32)
    target_compile_options(
        arcticdb_core_object
        PRIVATE
            /W4
            /external:I${CMAKE_SOURCE_DIR}/vcpkg
            /external:I${CMAKE_SOURCE_DIR}/third_party
            /Zc:__cplusplus #needed to get the correct __cplusplus macro value
    )
endif()

add_compile_definitions(ENTT_ID_TYPE=std::uint64_t)

set (arcticdb_core_libraries
        pybind11::module # Transitively includes Python::Module or Python::Python as appropriate
        arcticdb_proto
        xxHash::xxHash
        prometheus-cpp::push
        prometheus-cpp::pull
        unordered_dense::unordered_dense
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
)

if(${ARCTICDB_PYTHON_EXPLICIT_LINK})
    # Even though python is transitive dependency MSVS builds fail if we don't link against python explicitly
    # TODO: Figure out why
    list(APPEND arcticdb_core_libraries
        Python::Python
    )
endif()

# Dependencies expose different names of their libraries depending
# on their origin (i.e. vendored source or packages on conda-forge).
if(${ARCTICDB_USING_CONDA})

    list (APPEND arcticdb_core_libraries
        pcre
        msgpack-c
        ${LMDB_LIBRARIES}
    )
    if(${BUILD_WITH_REMOTERY})
        list (APPEND arcticdb_core_libraries
            ${remotery_LIBRARY}
        )
    endif()
else()

    list (APPEND arcticdb_core_libraries
        unofficial::pcre::pcre
        msgpackc-cxx
        EnTT::EnTT
        lmdb
    )
    if(${BUILD_WITH_REMOTERY})
        list (APPEND arcticdb_core_libraries
            remotery_static
        )
    endif()
endif()

if (WIN32)
    list (APPEND arcticdb_core_libraries
            Iconv::Iconv
            )
else ()

    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        list (APPEND arcticdb_core_libraries stdc++fs)
    endif()


    list (APPEND arcticdb_core_libraries
            dl
            ${Sasl2_LIBRARY}
            )
endif ()

list (APPEND arcticdb_core_libraries Threads::Threads) # comes last for weakreference resolution pthread_atexit in folly

set(arcticdb_core_includes
    ${BITMAGIC_INCLUDE_DIRS}
)

if (SSL_LINK)
    message("Linking against SSL")
    find_package(OpenSSL REQUIRED)
    list(APPEND arcticdb_core_libraries OpenSSL::SSL)
    if (NOT WIN32)
        list(APPEND arcticdb_core_libraries ${KERBEROS_LIBRARY})
        list(APPEND arcticdb_core_includes  ${KERBEROS_INCLUDE_DIR})
    endif()
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

## Core python bindings, private only ##
set(arcticdb_python_srcs
        async/python_bindings.cpp
        codec/python_bindings.cpp
        column_store/python_bindings.cpp
        storage/python_bindings.cpp
        stream/python_bindings.cpp
        toolbox/python_bindings.cpp
        version/python_bindings.cpp
        python/reader.hpp
        python/adapt_read_dataframe.hpp
        python/python_handlers.hpp
        python/python_handlers.cpp)

add_library(arcticdb_python STATIC ${arcticdb_python_srcs})

if(${ARCTICDB_USE_PCH})
    message(STATUS "Using precompiled headers for target arcticdb_python")
    target_precompile_headers(
        arcticdb_python
        PRIVATE
            <Python.h>
            <pybind11/stl.h>
            <pybind11/pybind11.h>
            <pybind11/numpy.h>
    )
endif()

target_link_libraries(arcticdb_python
        PUBLIC
        arcticdb_core_static
        ${AWSSDK_LINK_LIBRARIES}
        )

target_include_directories(arcticdb_python PRIVATE
        ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
        ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
        ${BITMAGIC_INCLUDE_DIRS}
        ${LMDB_LIBRARIES}
        )


if (NOT WIN32)
    add_compile_options("-ftemplate-depth=1000")
    if(NOT ${ARCTICDB_USING_CONDA})
        # Compilers on conda-forge are relatively strict.
        # For now, we do not want to treat warnings as errors for those builds.
        add_compile_options("-Werror")
    endif()
endif ()

## arcticdb_ext python module
# This configures: linking to Python::Module + pybind, output pre-/suf-fix, -fvisibility=hidden, strip release build
pybind11_add_module(arcticdb_ext MODULE
        python/python_module.cpp
        python/arctic_version.cpp
        python/arctic_version.hpp
        )

set (additional_link_flags "")
if (HIDE_LINKED_SYMBOLS AND (NOT WIN32))

    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        list(APPEND additional_link_flags
            "-Wl,--exclude-libs,ALL"
            "-Wl,--gc-sections")
    endif()
endif()

target_link_libraries(arcticdb_ext
        PUBLIC
        PRIVATE
        arcticdb_python
        ${additional_link_flags}
        )

if (WIN32)
    target_link_options(arcticdb_ext
            PRIVATE $<$<CONFIG:Release>:/DEBUG> $<$<CONFIG:Release>:/OPT:REF>)
endif()

target_include_directories(arcticdb_ext
        PRIVATE
        ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
        ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
        ${BITMAGIC_INCLUDE_DIRS}
        ${PYTHON_NUMPY_INCLUDE_DIR}
        ${AWSSDK_INCLUDE_DIRS}
        )

install(TARGETS arcticdb_ext
        LIBRARY DESTINATION . COMPONENT Python_Lib
        RUNTIME DESTINATION . COMPONENT Python_Lib)

# Improves portability (in setup.py) by creating an explicit target that manually installs the library
if(NOT WIN32 AND "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo" AND NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_custom_command(OUTPUT arcticdb_ext.gz
            COMMAND bash -c "gzip -c $<TARGET_FILE:arcticdb_ext> >arcticdb_ext.gz"
            DEPENDS arcticdb_ext)
    add_custom_target(install_arcticdb_ext
            ${CMAKE_STRIP} --strip-unneeded $<TARGET_FILE:arcticdb_ext>
                           -o ${CMAKE_INSTALL_PREFIX}/$<TARGET_FILE_NAME:arcticdb_ext>
            DEPENDS arcticdb_ext.gz)
else()
    add_custom_target(install_arcticdb_ext
            ${CMAKE_COMMAND} -E copy $<TARGET_FILE:arcticdb_ext> ${CMAKE_INSTALL_PREFIX})
endif()

## Unit Tests ##
if(${TEST})
    unset(Python_USE_STATIC_LIBS)
    find_package(Python 3 COMPONENTS Interpreter Development REQUIRED)
    find_package(Arrow REQUIRED)
    find_package(rapidcheck REQUIRED)
    find_package(benchmark REQUIRED)

    python_utils_dump_vars_if_enabled("Python for test compilation")

    set(unit_test_srcs
            async/test/test_async.cpp
            codec/test/test_codec.cpp
            codec/test/test_encode_field_collection.cpp
            codec/test/test_segment_header.cpp
            codec/test/test_encoded_field.cpp
            column_store/test/ingestion_stress_test.cpp
            column_store/test/test_column.cpp
            column_store/test/test_column_data_random_accessor.cpp
            column_store/test/test_index_filtering.cpp
            column_store/test/test_memory_segment.cpp
            entity/test/test_atom_key.cpp
            entity/test/test_key_serialization.cpp
            entity/test/test_metrics.cpp
            entity/test/test_ref_key.cpp
            entity/test/test_tensor.cpp
            log/test/test_log.cpp
            pipeline/test/test_container.hpp
            pipeline/test/test_pipeline.cpp
            pipeline/test/test_query.cpp
            util/test/test_regex.cpp
            processing/test/test_arithmetic_type_promotion.cpp
            processing/test/test_clause.cpp
            processing/test/test_component_manager.cpp
            processing/test/test_expression.cpp
            processing/test/test_filter_and_project_sparse.cpp
            processing/test/test_has_valid_type_promotion.cpp
            processing/test/test_operation_dispatch.cpp
            processing/test/test_parallel_processing.cpp
            processing/test/test_resample.cpp
            processing/test/test_set_membership.cpp
            processing/test/test_signed_unsigned_comparison.cpp
            processing/test/test_type_comparison.cpp
            storage/test/test_local_storages.cpp
            storage/test/test_memory_storage.cpp
            storage/test/test_s3_storage.cpp
            storage/test/test_storage_factory.cpp
            storage/test/test_storage_exceptions.cpp
            storage/test/test_azure_storage.cpp
            storage/test/test_storage_operations.cpp
            stream/test/stream_test_common.cpp
            stream/test/test_aggregator.cpp
            stream/test/test_append_map.cpp
            stream/test/test_row_builder.cpp
            stream/test/test_segment_aggregator.cpp
            stream/test/test_types.cpp
            util/memory_tracing.hpp
            util/test/gtest_main.cpp
            util/test/random_throw.hpp
            util/test/test_bitmagic.cpp
            util/test/test_buffer_pool.cpp
            util/test/test_composite.cpp
            util/test/test_cursor.cpp
            util/test/test_decimal.cpp
            util/test/test_exponential_backoff.cpp
            util/test/test_format_date.cpp
            util/test/test_hash.cpp
            util/test/test_id_transformation.cpp
            util/test/test_key_utils.cpp
            util/test/test_ranges_from_future.cpp
            util/test/test_reliable_storage_lock.cpp
            util/test/test_slab_allocator.cpp
            util/test/test_storage_lock.cpp
            util/test/test_string_pool.cpp
            util/test/test_string_utils.cpp
            util/test/test_tracing_allocator.cpp
            version/test/test_append.cpp
            version/test/test_key_block.cpp
            version/test/test_sort_index.cpp
            version/test/test_sorting_info_state_machine.cpp
            version/test/test_sparse.cpp
            version/test/test_stream_version_data.cpp
            version/test/test_symbol_list.cpp
            version/test/test_version_map.cpp
            version/test/test_version_map_batch.cpp
            version/test/test_version_store.cpp
            version/test/version_map_model.hpp
            python/python_handlers.cpp
            storage/test/common.hpp)

    set(EXECUTABLE_PERMS OWNER_WRITE OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE) # 755

    add_executable(test_unit_arcticdb ${unit_test_srcs})

    install(TARGETS test_unit_arcticdb RUNTIME
                DESTINATION .
                PERMISSIONS ${EXECUTABLE_PERMS}
                COMPONENT Tests)
                
    target_compile_definitions(test_unit_arcticdb PRIVATE PYBIND11_NO_ASSERT_GIL_HELD_INCREF_DECREF)


    if (WIN32)
        target_link_libraries(
            test_unit_arcticdb
            PRIVATE
                ${AWSSDK_LINK_LIBRARIES}
                GTest::gtest
                GTest::gmock
                arcticdb_core_static
        )

        # Fix gtest/folly unistd conflict until one of them fixes it. There's another copy for rapidcheck
        target_compile_options(test_unit_arcticdb PRIVATE /FI "${CMAKE_CURRENT_SOURCE_DIR}/util/test/gtest.hpp")

        # gtest_discover_tests runs the exe in order to dynamically list all tests. In case the executable depends on a DLL
        # which is not in PATH gtest_discover_tests will return a cryptic error. The tests depend on a few DLLs one of which
        # is python<version>.dll. On Windows there is no RPATH and in addition to that gtest_discover_tests cannot take/modify
        # environment variables (known issue: https://gitlab.kitware.com/cmake/cmake/-/issues/21453). Thus we are left with two options:
        # 1. Instruct each developer to modify their path variable (gets annoying when different venvs are used)
        # 2. Copy all required DLLs to the folder with the exe
        # Option 2 is used. Although it defeats the purpose of a DLL it works without extra configuration steps
        # The same approach is used for rapidcheck tests
        add_custom_command(
	        TARGET test_unit_arcticdb POST_BUILD
	        COMMAND ${CMAKE_COMMAND} -E echo "Copy required DLLs: $<TARGET_RUNTIME_DLLS:test_unit_arcticdb>"
	        COMMAND ${CMAKE_COMMAND} -E $<$<NOT:$<BOOL:$<TARGET_RUNTIME_DLLS:test_unit_arcticdb>>>:true> copy_if_different $<TARGET_RUNTIME_DLLS:test_unit_arcticdb> $<TARGET_FILE_DIR:test_unit_arcticdb>
	        COMMAND_EXPAND_LISTS
        )
    else()
        set(COMMON_PUBLIC_TEST_LIBRARIES
                arcticdb_core_static
                GTest::gtest
                GTest::gmock
                Python::Python # + pybind11::pybind11 (transitively included) = pybind11::embed, but latter is sometimes not found...
                util # TODO: find out where we lost the transitive link to it
                )
        if(NOT APPLE)
            list(APPEND COMMON_PUBLIC_TEST_LIBRARIES
                    atomic # Otherwise gtest_discover_tests cannot run the test if libatomic is not in a standard location
                )
        endif()

        find_library(Gcov_LIBRARY gcov)
        if (NOT(${Gcov_LIBRARY} EQUAL "Gcov_LIBRARY-NOTFOUND"))
            list(APPEND common_public_test_libraries ${Gcov_LIBRARY})
        endif()

        target_link_libraries(test_unit_arcticdb
            PUBLIC
            ${COMMON_PUBLIC_TEST_LIBRARIES}
            PRIVATE
            ${AWSSDK_LINK_LIBRARIES}
            )

        target_include_directories(test_unit_arcticdb PRIVATE
                ${LIBMONGOCXX_STATIC_INCLUDE_DIRS}
                ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
                ${BITMAGIC_INCLUDE_DIRS}
                )
    endif()

    gtest_discover_tests(test_unit_arcticdb PROPERTIES DISCOVERY_TIMEOUT 60)

    set(benchmark_srcs
            stream/test/stream_test_common.cpp
            column_store/test/benchmark_memory_segment.cpp
            processing/test/benchmark_clause.cpp)

    add_executable(benchmarks ${benchmark_srcs})

    target_link_libraries(benchmarks
            PUBLIC
            benchmark::benchmark
            benchmark::benchmark_main
            ${COMMON_PUBLIC_TEST_LIBRARIES}
            PRIVATE
            ${AWSSDK_LINK_LIBRARIES}
            arcticdb_core_static
            )

    set(rapidcheck_srcs
            column_store/test/rapidcheck_column_store.cpp
            column_store/test/rapidcheck_chunked_buffer.cpp
            column_store/test/rapidcheck_column.cpp
            column_store/test/rapidcheck_column_data_random_accessor.cpp
            column_store/test/rapidcheck_column_map.cpp
            column_store/test/test_chunked_buffer.cpp
            processing/test/rapidcheck_resample.cpp
            stream/test/stream_test_common.cpp
            util/test/rapidcheck_decimal.cpp
            util/test/rapidcheck_generators.cpp
            util/test/rapidcheck_string_pool.cpp
            util/test/rapidcheck_main.cpp
            util/test/rapidcheck_lru_cache.cpp
            version/test/rapidcheck_version_map.cpp)

    add_executable(arcticdb_rapidcheck_tests ${rapidcheck_srcs})
    install(TARGETS arcticdb_rapidcheck_tests RUNTIME
                DESTINATION .
                PERMISSIONS ${EXECUTABLE_PERMS}
                COMPONENT Tests)

    if(ARCTICDB_USING_CONDA)
        set(RAPIDCHECK_PRIVATE_LIBRARIES
            ${AWSSDK_LINK_LIBRARIES}
            Arrow::arrow_shared)
    else()
        set(RAPIDCHECK_PRIVATE_LIBRARIES
            ${AWSSDK_LINK_LIBRARIES}
            "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
    endif()

    if(WIN32)
        target_link_libraries(
            arcticdb_rapidcheck_tests
            PRIVATE
                arcticdb_core_static
                GTest::gtest
                GTest::gmock
                rapidcheck
                rapidcheck_gtest
                ${RAPIDCHECK_PRIVATE_LIBRARIES}
        )
        # gtest_discover_tests runs the exe in order to dynamically list all tests. In case the executable depends on a DLL
        # which is not in PATH gtest_discover_tests will return a cryptic error. The tests depend on a few DLLs one of which
        # is python<version>.dll. On Windows there is no RPATH and in addition to that gtest_discover_tests cannot take/modify
        # environment variables (known issue: https://gitlab.kitware.com/cmake/cmake/-/issues/21453). Thus we are left with two options:
        # 1. Instruct each developer to modify their path variable (gets annoying when different venvs are used)
        # 2. Copy all required DLLs to the folder with the exe
        # Option 2 is used. Although it defeats the purpose of a DLL it works without extra configuration steps
        # The same approach is used for unit tests
        add_custom_command(
	        TARGET arcticdb_rapidcheck_tests POST_BUILD
	        COMMAND ${CMAKE_COMMAND} -E echo "Copy required DLLs: $<TARGET_RUNTIME_DLLS:arcticdb_rapidcheck_tests>"
	        COMMAND ${CMAKE_COMMAND} -E $<$<NOT:$<BOOL:$<TARGET_RUNTIME_DLLS:arcticdb_rapidcheck_tests>>>:true> copy_if_different $<TARGET_RUNTIME_DLLS:arcticdb_rapidcheck_tests> $<TARGET_FILE_DIR:arcticdb_rapidcheck_tests>
	        COMMAND_EXPAND_LISTS
        )
        target_compile_options(arcticdb_rapidcheck_tests PRIVATE /FI "util/test/gtest.hpp")
    else()
        target_link_libraries(arcticdb_rapidcheck_tests
            PUBLIC
                ${COMMON_PUBLIC_TEST_LIBRARIES}
                rapidcheck
                rapidcheck_gtest
            PRIVATE
                ${RAPIDCHECK_PRIVATE_LIBRARIES}
            )
    endif()

    gtest_discover_tests(arcticdb_rapidcheck_tests PROPERTIES DISCOVERY_TIMEOUT 60)

    if(${ARCTICDB_USE_PCH})
        message(STATUS "Using precompiled headers for target test_unit_arcticdb")
        target_precompile_headers(
            test_unit_arcticdb
            PRIVATE
                # GTest
                <gtest/gtest.h>
                <gtest/gtest_prod.h>
                ${BASE_PCH}
        )
        message(STATUS "Using precompiled headers for target target_precompile_headers")
        target_precompile_headers(
            arcticdb_rapidcheck_tests
            PRIVATE
                <rapidcheck/state.h>
                <rapidcheck.h>
                ${BASE_PCH}
        )
    endif()
endif()
