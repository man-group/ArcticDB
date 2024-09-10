#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "civetweb::civetweb" for configuration "Release"
set_property(TARGET civetweb::civetweb APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(civetweb::civetweb PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libcivetweb.a"
  )

list(APPEND _cmake_import_check_targets civetweb::civetweb )
list(APPEND _cmake_import_check_files_for_civetweb::civetweb "${_IMPORT_PREFIX}/lib/libcivetweb.a" )

# Import target "civetweb::civetweb-cpp" for configuration "Release"
set_property(TARGET civetweb::civetweb-cpp APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(civetweb::civetweb-cpp PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libcivetweb-cpp.a"
  )

list(APPEND _cmake_import_check_targets civetweb::civetweb-cpp )
list(APPEND _cmake_import_check_files_for_civetweb::civetweb-cpp "${_IMPORT_PREFIX}/lib/libcivetweb-cpp.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
