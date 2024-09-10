#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "liblzma::liblzma" for configuration "Debug"
set_property(TARGET liblzma::liblzma APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(liblzma::liblzma PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/liblzma.a"
  )

list(APPEND _cmake_import_check_targets liblzma::liblzma )
list(APPEND _cmake_import_check_files_for_liblzma::liblzma "${_IMPORT_PREFIX}/debug/lib/liblzma.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
