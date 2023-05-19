# Try to find the lmdb++ headers
#  LMDBXX_FOUND - system has lmdb++ lib
#  LMDBXX_INCLUDE_DIR - the lmdb++ include directory

find_path(LMDBXX_INCLUDE_DIR NAMES  lmdb++.h PATHS "$ENV{LMDB_DIRXX}/include")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LMDBXX DEFAULT_MSG LMDBXX_INCLUDE_DIR)

if(LMDBXX_FOUND)
    message(STATUS "Found lmdbxx    (include: ${LMDBXX_INCLUDE_DIR}, library: ${LMDBXX_LIBRARIES})")

    mark_as_advanced(LMDBXX_INCLUDE_DIR)
endif()
