get_filename_component(VCPKG_IMPORT_PREFIX "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)
#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "utf8proc" for configuration "Debug"
set_property(TARGET utf8proc APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(utf8proc PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${VCPKG_IMPORT_PREFIX}/debug/lib/libutf8proc.a"
  )

list(APPEND _cmake_import_check_targets utf8proc )
list(APPEND _cmake_import_check_files_for_utf8proc "${VCPKG_IMPORT_PREFIX}/debug/lib/libutf8proc.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
