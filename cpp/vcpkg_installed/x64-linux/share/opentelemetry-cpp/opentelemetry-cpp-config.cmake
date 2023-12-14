# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

#.rst:
# opentelemetry-cpp-config.cmake
# --------
#
# Find the native opentelemetry-cpp includes and library.
#
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This module defines the following variables:
#
# ::
#
#   OPENTELEMETRY_CPP_INCLUDE_DIRS  - Include directories of opentelemetry-cpp.
#   OPENTELEMETRY_CPP_LIBRARY_DIRS  - Link directories of opentelemetry-cpp.
#   OPENTELEMETRY_CPP_LIBRARIES     - List of libraries when using opentelemetry-cpp.
#   OPENTELEMETRY_CPP_FOUND         - True if opentelemetry-cpp found.
#   OPENTELEMETRY_ABI_VERSION_NO    - ABI version of opentelemetry-cpp.
#   OPENTELEMETRY_VERSION           - Version of opentelemetry-cpp.
#
# ::
#   opentelemetry-cpp::api                               - Imported target of opentelemetry-cpp::api
#   opentelemetry-cpp::sdk                               - Imported target of opentelemetry-cpp::sdk
#   opentelemetry-cpp::ext                               - Imported target of opentelemetry-cpp::ext
#   opentelemetry-cpp::version                           - Imported target of opentelemetry-cpp::version
#   opentelemetry-cpp::common                            - Imported target of opentelemetry-cpp::common
#   opentelemetry-cpp::trace                             - Imported target of opentelemetry-cpp::trace
#   opentelemetry-cpp::metrics                           - Imported target of opentelemetry-cpp::metrics
#   opentelemetry-cpp::logs                              - Imported target of opentelemetry-cpp::logs
#   opentelemetry-cpp::in_memory_span_exporter           - Imported target of opentelemetry-cpp::in_memory_span_exporter
#   opentelemetry-cpp::otlp_grpc_client                  - Imported target of opentelemetry-cpp::otlp_grpc_client
#   opentelemetry-cpp::otlp_recordable                   - Imported target of opentelemetry-cpp::otlp_recordable
#   opentelemetry-cpp::otlp_grpc_exporter                - Imported target of opentelemetry-cpp::otlp_grpc_exporter
#   opentelemetry-cpp::otlp_grpc_log_record_exporter     - Imported target of opentelemetry-cpp::otlp_grpc_log_record_exporter
#   opentelemetry-cpp::otlp_grpc_metrics_exporter        - Imported target of opentelemetry-cpp::otlp_grpc_metrics_exporter
#   opentelemetry-cpp::otlp_http_client                  - Imported target of opentelemetry-cpp::otlp_http_client
#   opentelemetry-cpp::otlp_http_exporter                - Imported target of opentelemetry-cpp::otlp_http_exporter
#   opentelemetry-cpp::otlp_http_log_record_exporter     - Imported target of opentelemetry-cpp::otlp_http_log_record_exporter
#   opentelemetry-cpp::otlp_http_metric_exporter         - Imported target of opentelemetry-cpp::otlp_http_metric_exporter
#   opentelemetry-cpp::ostream_log_record_exporter       - Imported target of opentelemetry-cpp::ostream_log_record_exporter
#   opentelemetry-cpp::ostream_metrics_exporter          - Imported target of opentelemetry-cpp::ostream_metrics_exporter
#   opentelemetry-cpp::ostream_span_exporter             - Imported target of opentelemetry-cpp::ostream_span_exporter
#   opentelemetry-cpp::elasticsearch_log_record_exporter - Imported target of opentelemetry-cpp::elasticsearch_log_record_exporter
#   opentelemetry-cpp::etw_exporter                      - Imported target of opentelemetry-cpp::etw_exporter
#   opentelemetry-cpp::zpages                            - Imported target of opentelemetry-cpp::zpages
#   opentelemetry-cpp::http_client_curl                  - Imported target of opentelemetry-cpp::http_client_curl
#   opentelemetry-cpp::opentracing_shim                  - Imported target of opentelemetry-cpp::opentracing_shim
#

# =============================================================================
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
# =============================================================================

set(OPENTELEMETRY_ABI_VERSION_NO
    "1"
    CACHE STRING "opentelemetry-cpp ABI version" FORCE)
set(OPENTELEMETRY_VERSION
    "1.12.0"
    CACHE STRING "opentelemetry-cpp version" FORCE)


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was opentelemetry-cpp-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

####################################################################################

# ##############################################################################

find_package(Threads)
include(CMakeFindDependencyMacro)
find_dependency(absl)

set_and_check(OPENTELEMETRY_CPP_INCLUDE_DIRS "${PACKAGE_PREFIX_DIR}/include")
set_and_check(OPENTELEMETRY_CPP_LIBRARY_DIRS "${PACKAGE_PREFIX_DIR}/lib")

include("${CMAKE_CURRENT_LIST_DIR}/opentelemetry-cpp-target.cmake")

set(OPENTELEMETRY_CPP_LIBRARIES)
set(_OPENTELEMETRY_CPP_LIBRARIES_TEST_TARGETS
    api
    sdk
    ext
    version
    common
    trace
    metrics
    logs
    in_memory_span_exporter
    otlp_recordable
    otlp_grpc_client
    otlp_grpc_exporter
    otlp_grpc_log_record_exporter
    otlp_grpc_metrics_exporter
    otlp_http_client
    otlp_http_exporter
    otlp_http_log_record_exporter
    otlp_http_metric_exporter
    ostream_log_record_exporter
    ostream_metrics_exporter
    ostream_span_exporter
    prometheus_exporter
    elasticsearch_log_record_exporter
    etw_exporter
    zpages
    http_client_curl
    opentracing_shim)
foreach(_TEST_TARGET IN LISTS _OPENTELEMETRY_CPP_LIBRARIES_TEST_TARGETS)
  if(TARGET opentelemetry-cpp::${_TEST_TARGET})
    list(APPEND OPENTELEMETRY_CPP_LIBRARIES opentelemetry-cpp::${_TEST_TARGET})
  endif()
endforeach()

# handle the QUIETLY and REQUIRED arguments and set opentelemetry-cpp_FOUND to
# TRUE if all variables listed contain valid results, e.g. valid file paths.
include("FindPackageHandleStandardArgs")
find_package_handle_standard_args(
  ${CMAKE_FIND_PACKAGE_NAME}
  FOUND_VAR ${CMAKE_FIND_PACKAGE_NAME}_FOUND
  REQUIRED_VARS OPENTELEMETRY_CPP_INCLUDE_DIRS OPENTELEMETRY_CPP_LIBRARIES)

if(${CMAKE_FIND_PACKAGE_NAME}_FOUND)
  set(OPENTELEMETRY_CPP_FOUND
      ${${CMAKE_FIND_PACKAGE_NAME}_FOUND}
      CACHE BOOL "whether opentelemetry-cpp is found" FORCE)
else()
  unset(OPENTELEMETRY_CPP_FOUND)
  unset(OPENTELEMETRY_CPP_FOUND CACHE)
endif()
