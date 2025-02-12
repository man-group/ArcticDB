#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "AWS::aws-crt-cpp" for configuration "Release"
set_property(TARGET AWS::aws-crt-cpp APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(AWS::aws-crt-cpp PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libaws-crt-cpp.a"
  )

list(APPEND _cmake_import_check_targets AWS::aws-crt-cpp )
list(APPEND _cmake_import_check_files_for_AWS::aws-crt-cpp "${_IMPORT_PREFIX}/lib/libaws-crt-cpp.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
