#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "rapidcheck" for configuration "Debug"
set_property(TARGET rapidcheck APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(rapidcheck PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/librapidcheck.a"
  )

list(APPEND _cmake_import_check_targets rapidcheck )
list(APPEND _cmake_import_check_files_for_rapidcheck "${_IMPORT_PREFIX}/debug/lib/librapidcheck.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
