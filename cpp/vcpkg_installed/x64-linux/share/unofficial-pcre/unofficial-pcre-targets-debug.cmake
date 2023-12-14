#----------------------------------------------------------------
# Generated CMake target import file for configuration "Debug".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "unofficial::pcre::pcre" for configuration "Debug"
set_property(TARGET unofficial::pcre::pcre APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(unofficial::pcre::pcre PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libpcre.a"
  )

list(APPEND _cmake_import_check_targets unofficial::pcre::pcre )
list(APPEND _cmake_import_check_files_for_unofficial::pcre::pcre "${_IMPORT_PREFIX}/debug/lib/libpcre.a" )

# Import target "unofficial::pcre::pcreposix" for configuration "Debug"
set_property(TARGET unofficial::pcre::pcreposix APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(unofficial::pcre::pcreposix PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LINK_INTERFACE_LIBRARIES_DEBUG "unofficial::pcre::pcre"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libpcreposix.a"
  )

list(APPEND _cmake_import_check_targets unofficial::pcre::pcreposix )
list(APPEND _cmake_import_check_files_for_unofficial::pcre::pcreposix "${_IMPORT_PREFIX}/debug/lib/libpcreposix.a" )

# Import target "unofficial::pcre::pcre16" for configuration "Debug"
set_property(TARGET unofficial::pcre::pcre16 APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(unofficial::pcre::pcre16 PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libpcre16.a"
  )

list(APPEND _cmake_import_check_targets unofficial::pcre::pcre16 )
list(APPEND _cmake_import_check_files_for_unofficial::pcre::pcre16 "${_IMPORT_PREFIX}/debug/lib/libpcre16.a" )

# Import target "unofficial::pcre::pcre32" for configuration "Debug"
set_property(TARGET unofficial::pcre::pcre32 APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(unofficial::pcre::pcre32 PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "C"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libpcre32.a"
  )

list(APPEND _cmake_import_check_targets unofficial::pcre::pcre32 )
list(APPEND _cmake_import_check_files_for_unofficial::pcre::pcre32 "${_IMPORT_PREFIX}/debug/lib/libpcre32.a" )

# Import target "unofficial::pcre::pcrecpp" for configuration "Debug"
set_property(TARGET unofficial::pcre::pcrecpp APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(unofficial::pcre::pcrecpp PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LINK_INTERFACE_LIBRARIES_DEBUG "unofficial::pcre::pcre"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/debug/lib/libpcrecpp.a"
  )

list(APPEND _cmake_import_check_targets unofficial::pcre::pcrecpp )
list(APPEND _cmake_import_check_files_for_unofficial::pcre::pcrecpp "${_IMPORT_PREFIX}/debug/lib/libpcrecpp.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
