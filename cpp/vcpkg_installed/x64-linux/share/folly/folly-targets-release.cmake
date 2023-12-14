#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Folly::folly" for configuration "Release"
set_property(TARGET Folly::folly APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Folly::folly PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "ASM;CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libfolly.a"
  )

list(APPEND _cmake_import_check_targets Folly::folly )
list(APPEND _cmake_import_check_files_for_Folly::folly "${_IMPORT_PREFIX}/lib/libfolly.a" )

# Import target "Folly::folly_test_util" for configuration "Release"
set_property(TARGET Folly::folly_test_util APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Folly::folly_test_util PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libfolly_test_util.a"
  )

list(APPEND _cmake_import_check_targets Folly::folly_test_util )
list(APPEND _cmake_import_check_files_for_Folly::folly_test_util "${_IMPORT_PREFIX}/lib/libfolly_test_util.a" )

# Import target "Folly::follybenchmark" for configuration "Release"
set_property(TARGET Folly::follybenchmark APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(Folly::follybenchmark PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libfollybenchmark.a"
  )

list(APPEND _cmake_import_check_targets Folly::follybenchmark )
list(APPEND _cmake_import_check_files_for_Folly::follybenchmark "${_IMPORT_PREFIX}/lib/libfollybenchmark.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
