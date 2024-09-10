include(CMakeFindDependencyMacro)
find_dependency(bson-1.0 1.28.0)
if("OFF")
    find_dependency(Snappy CONFIG)
endif()

# If we need to import a TLS package for our imported targets, do that now:
set(MONGOC_TLS_BACKEND [[OpenSSL]])
set(_tls_package [[OpenSSL]])
if(_tls_package)
  # We bring our own FindLibreSSL, since most systems do not have one yet. The system's version
  # will be preferred, if possible.
  set(_prev_path "${CMAKE_MODULE_PATH}")
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/3rdParty")
  find_dependency("${_tls_package}")
  set(CMAKE_MODULE_PATH "${_prev_path}")
endif()

unset(_required)
unset(_quiet)
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED)
  set(_required REQUIRED)
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_QUIETLY)
  set(_quiet QUIET)
endif()

set(_mongoc_built_with_bundled_utf8proc "OFF")
if(NOT _mongoc_built_with_bundled_utf8proc AND NOT TARGET PkgConfig::PC_UTF8PROC)
  # libmongoc was compiled against an external utf8proc and links against a
  # FindPkgConfig-generated IMPORTED target. Find that package and generate that
  # imported target here:
endif()
find_dependency(unofficial-utf8proc CONFIG)

# Find dependencies for SASL
set(_sasl_backend [[OFF]])
if(_sasl_backend STREQUAL "Cyrus")
  # We need libsasl2. The find-module should be installed within this package.
  # temporarily place it on the module search path:
  set(_prev_path "${CMAKE_MODULE_PATH}")
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/3rdParty")
  find_dependency(SASL2 2.0)
  set(CMAKE_MODULE_PATH "${_prev_path}")
endif()

include("${CMAKE_CURRENT_LIST_DIR}/mongoc-targets.cmake")
