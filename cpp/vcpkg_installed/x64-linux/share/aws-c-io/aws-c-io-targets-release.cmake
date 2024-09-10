#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "AWS::aws-c-io" for configuration "Release"
set_property(TARGET AWS::aws-c-io APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(AWS::aws-c-io PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libaws-c-io.a"
  )

list(APPEND _cmake_import_check_targets AWS::aws-c-io )
list(APPEND _cmake_import_check_files_for_AWS::aws-c-io "${_IMPORT_PREFIX}/lib/libaws-c-io.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
