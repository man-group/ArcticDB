#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Azure::azure-identity" for configuration "Debug"
set_property(TARGET Azure::azure-identity APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(Azure::azure-identity PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libazure-identity.a"
  )

list(APPEND _cmake_import_check_targets Azure::azure-identity )
list(APPEND _cmake_import_check_files_for_Azure::azure-identity "${_IMPORT_PREFIX}/debug/lib/libazure-identity.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
