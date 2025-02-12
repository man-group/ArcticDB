#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Azure::azure-storage-common" for configuration "Debug"
set_property(TARGET Azure::azure-storage-common APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(Azure::azure-storage-common PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libazure-storage-common.a"
  )

list(APPEND _cmake_import_check_targets Azure::azure-storage-common )
list(APPEND _cmake_import_check_files_for_Azure::azure-storage-common "${_IMPORT_PREFIX}/debug/lib/libazure-storage-common.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
