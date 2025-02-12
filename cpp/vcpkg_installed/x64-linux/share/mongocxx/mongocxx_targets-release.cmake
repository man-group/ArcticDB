#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "mongo::mongocxx_static" for configuration "Release"
set_property(TARGET mongo::mongocxx_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(mongo::mongocxx_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libmongocxx-static.a"
  )

list(APPEND _cmake_import_check_targets mongo::mongocxx_static )
list(APPEND _cmake_import_check_files_for_mongo::mongocxx_static "${_IMPORT_PREFIX}/lib/libmongocxx-static.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
