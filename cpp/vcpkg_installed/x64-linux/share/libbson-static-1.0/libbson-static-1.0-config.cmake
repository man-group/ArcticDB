# Copyright 2017 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

message(WARNING "This CMake target is deprecated.  Use 'mongo::bson_static' instead.  Consult the example projects for further details.")

set (BSON_STATIC_MAJOR_VERSION 1)
set (BSON_STATIC_MINOR_VERSION 22)
set (BSON_STATIC_MICRO_VERSION 2)
set (BSON_STATIC_VERSION 1.22.2)


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was libbson-static-1.0-config.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

set_and_check (BSON_STATIC_INCLUDE_DIRS "${PACKAGE_PREFIX_DIR}/include")

# We want to provide an absolute path to the library and we know the
# directory and the base name, but not the suffix, so we use CMake's
# find_library () to pick that up.  Users can override this by configuring
# BSON_STATIC_LIBRARY themselves.
find_library (BSON_STATIC_LIBRARY bson-static-1.0 PATHS "${PACKAGE_PREFIX_DIR}/lib" NO_DEFAULT_PATH)

set (BSON_STATIC_LIBRARIES ${BSON_STATIC_LIBRARY})

foreach (LIB  /usr/lib/x86_64-linux-gnu/librt.a)
    list (APPEND BSON_STATIC_LIBRARIES ${LIB})
endforeach ()

set (BSON_STATIC_DEFINITIONS BSON_STATIC)
