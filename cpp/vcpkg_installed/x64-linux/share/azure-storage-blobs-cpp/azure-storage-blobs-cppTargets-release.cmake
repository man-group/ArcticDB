#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Azure::azure-storage-blobs" for configuration "Release"
set_property(TARGET Azure::azure-storage-blobs APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Azure::azure-storage-blobs PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libazure-storage-blobs.a"
  )

list(APPEND _cmake_import_check_targets Azure::azure-storage-blobs )
list(APPEND _cmake_import_check_files_for_Azure::azure-storage-blobs "${_IMPORT_PREFIX}/lib/libazure-storage-blobs.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
