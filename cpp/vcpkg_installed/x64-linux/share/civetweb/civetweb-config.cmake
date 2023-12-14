
####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was civetweb-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

####################################################################################
include(CMakeFindDependencyMacro)

set_and_check(civetweb_INCLUDE_DIR "${PACKAGE_PREFIX_DIR}/include")

find_dependency(Threads)

set(CIVETWEB_SAVED_MODULE_PATH ${CMAKE_MODULE_PATH})
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR})

if(FALSE)
  find_dependency(LibDl)
endif()

if(TRUE)
  find_dependency(LibRt)
endif()

if(FALSE)
  find_dependency(WinSock)
endif()

set(CMAKE_MODULE_PATH ${CIVETWEB_SAVED_MODULE_PATH})
unset(CIVETWEB_SAVED_MODULE_PATH)

include("${CMAKE_CURRENT_LIST_DIR}/civetweb-targets.cmake")
