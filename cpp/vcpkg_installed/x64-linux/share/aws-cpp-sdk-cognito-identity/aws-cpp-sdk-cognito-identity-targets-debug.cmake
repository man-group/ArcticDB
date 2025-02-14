#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "aws-cpp-sdk-cognito-identity" for configuration "Debug"
set_property(TARGET aws-cpp-sdk-cognito-identity APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(aws-cpp-sdk-cognito-identity PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libaws-cpp-sdk-cognito-identity.a"
  )

list(APPEND _cmake_import_check_targets aws-cpp-sdk-cognito-identity )
list(APPEND _cmake_import_check_files_for_aws-cpp-sdk-cognito-identity "${_IMPORT_PREFIX}/debug/lib/libaws-cpp-sdk-cognito-identity.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
