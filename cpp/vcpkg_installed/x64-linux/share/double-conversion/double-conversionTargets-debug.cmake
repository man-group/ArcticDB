#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "double-conversion::double-conversion" for configuration "Debug"
set_property(TARGET double-conversion::double-conversion APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(double-conversion::double-conversion PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libdouble-conversion.a"
  )

list(APPEND _cmake_import_check_targets double-conversion::double-conversion )
list(APPEND _cmake_import_check_files_for_double-conversion::double-conversion "${_IMPORT_PREFIX}/debug/lib/libdouble-conversion.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
