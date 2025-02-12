
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was prometheus-cpp-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

####################################################################################
include(CMakeFindDependencyMacro)

set_and_check(prometheus-cpp_INCLUDE_DIR "${PACKAGE_PREFIX_DIR}/include")

set(PROMETHEUS_CPP_ENABLE_PULL ON)
set(PROMETHEUS_CPP_ENABLE_PUSH ON)
set(PROMETHEUS_CPP_USE_COMPRESSION OFF)
set(PROMETHEUS_CPP_USE_THIRDPARTY_LIBRARIES OFF)
set(PROMETHEUS_CPP_THIRDPARTY_CIVETWEB_WITH_SSL OFF)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
find_dependency(Threads)
unset(CMAKE_THREAD_PREFER_PTHREAD)

if(PROMETHEUS_CPP_ENABLE_PULL)
  if(PROMETHEUS_CPP_USE_THIRDPARTY_LIBRARIES)
    if(PROMETHEUS_CPP_THIRDPARTY_CIVETWEB_WITH_SSL)
        find_dependency(OpenSSL)
    endif()
  else()
    find_dependency(civetweb)
  endif()
endif()

if(PROMETHEUS_CPP_ENABLE_PULL AND PROMETHEUS_CPP_USE_COMPRESSION)
  find_dependency(ZLIB)
endif()

if(PROMETHEUS_CPP_ENABLE_PUSH)
  find_dependency(CURL)
endif()

include("${CMAKE_CURRENT_LIST_DIR}/prometheus-cpp-targets.cmake")
