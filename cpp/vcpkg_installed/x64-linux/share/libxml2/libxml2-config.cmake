# libxml2-config.cmake
# --------------------
#
# Libxml2 cmake module.
# This module sets the following variables:
#
# ::
#
#   LIBXML2_INCLUDE_DIR        - Directory where LibXml2 headers are located.
#   LIBXML2_INCLUDE_DIRS       - list of the include directories needed to use LibXml2.
#   LIBXML2_LIBRARY            - path to the LibXml2 library.
#   LIBXML2_LIBRARIES          - xml2 libraries to link against.
#   LIBXML2_DEFINITIONS        - the compiler switches required for using LibXml2.
#   LIBXML2_VERSION_MAJOR      - The major version of libxml2.
#   LIBXML2_VERSION_MINOR      - The minor version of libxml2.
#   LIBXML2_VERSION_PATCH      - The patch version of libxml2.
#   LIBXML2_VERSION_STRING     - version number as a string (ex: "2.3.4")
#   LIBXML2_MODULES            - whether libxml2 has dso support
#   LIBXML2_XMLLINT_EXECUTABLE - path to the XML checking tool xmllint coming with LibXml2

include("${CMAKE_CURRENT_LIST_DIR}/libxml2-export.cmake")


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was libxml2-config.cmake.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

set(LIBXML2_VERSION_MAJOR  2)
set(LIBXML2_VERSION_MINOR  11)
set(LIBXML2_VERSION_PATCH  5)
set(LIBXML2_VERSION_STRING "2.11.5")
set(LIBXML2_INSTALL_PREFIX ${PACKAGE_PREFIX_DIR})
set(LIBXML2_INCLUDE_DIR    ${PACKAGE_PREFIX_DIR}/include/libxml2)
set(LIBXML2_LIBRARY_DIR    ${PACKAGE_PREFIX_DIR}/lib)

macro(select_library_location target basename)
  if(TARGET ${target})
    foreach(property IN ITEMS IMPORTED_LOCATION IMPORTED_IMPLIB)
      get_target_property(${basename}_${property}_DEBUG ${target} ${property}_DEBUG)
      get_target_property(${basename}_${property}_MINSIZEREL ${target} ${property}_MINSIZEREL)
      get_target_property(${basename}_${property}_RELEASE ${target} ${property}_RELEASE)
      get_target_property(${basename}_${property}_RELWITHDEBINFO ${target} ${property}_RELWITHDEBINFO)

      if(${basename}_${property}_DEBUG AND ${basename}_${property}_RELEASE)
        set(${basename}_LIBRARY debug ${${basename}_${property}_DEBUG} optimized ${${basename}_${property}_RELEASE})
      elseif(${basename}_${property}_DEBUG AND ${basename}_${property}_RELWITHDEBINFO)
        set(${basename}_LIBRARY debug ${${basename}_${property}_DEBUG} optimized ${${basename}_${property}_RELWITHDEBINFO})
      elseif(${basename}_${property}_DEBUG AND ${basename}_${property}_MINSIZEREL)
        set(${basename}_LIBRARY debug ${${basename}_${property}_DEBUG} optimized ${${basename}_${property}_MINSIZEREL})
      elseif(${basename}_${property}_RELEASE)
        set(${basename}_LIBRARY ${${basename}_${property}_RELEASE})
      elseif(${basename}_${property}_RELWITHDEBINFO)
        set(${basename}_LIBRARY ${${basename}_${property}_RELWITHDEBINFO})
      elseif(${basename}_${property}_MINSIZEREL)
        set(${basename}_LIBRARY ${${basename}_${property}_MINSIZEREL})
      elseif(${basename}_${property}_DEBUG)
        set(${basename}_LIBRARY ${${basename}_${property}_DEBUG})
      endif()
    endforeach()
  endif()
endmacro()

macro(select_executable_location target basename)
  if(TARGET ${target})
    get_target_property(${basename}_IMPORTED_LOCATION_DEBUG ${target} IMPORTED_LOCATION_DEBUG)
    get_target_property(${basename}_IMPORTED_LOCATION_MINSIZEREL ${target} IMPORTED_LOCATION_MINSIZEREL)
    get_target_property(${basename}_IMPORTED_LOCATION_RELEASE ${target} IMPORTED_LOCATION_RELEASE)
    get_target_property(${basename}_IMPORTED_LOCATION_RELWITHDEBINFO ${target} IMPORTED_LOCATION_RELWITHDEBINFO)

    if(${basename}_IMPORTED_LOCATION_RELEASE)
      set(${basename}_EXECUTABLE ${${basename}_IMPORTED_LOCATION_RELEASE})
    elseif(${basename}_IMPORTED_LOCATION_RELWITHDEBINFO)
      set(${basename}_EXECUTABLE ${${basename}_IMPORTED_LOCATION_RELWITHDEBINFO})
    elseif(${basename}_IMPORTED_LOCATION_MINSIZEREL)
      set(${basename}_EXECUTABLE ${${basename}_IMPORTED_LOCATION_MINSIZEREL})
    elseif(${basename}_IMPORTED_LOCATION_DEBUG)
      set(${basename}_EXECUTABLE ${${basename}_IMPORTED_LOCATION_DEBUG})
    endif()
  endif()
endmacro()

select_library_location(LibXml2::LibXml2 LIBXML2)
select_executable_location(LibXml2::xmlcatalog LIBXML2_XMLCATALOG)
select_executable_location(LibXml2::xmllint LIBXML2_XMLLINT)

set(LIBXML2_LIBRARIES ${LIBXML2_LIBRARY})
set(LIBXML2_INCLUDE_DIRS ${LIBXML2_INCLUDE_DIR})

include(CMakeFindDependencyMacro)

set(LIBXML2_SHARED OFF)
set(LIBXML2_WITH_ICONV ON)
set(LIBXML2_WITH_THREADS ON)
set(LIBXML2_WITH_ICU OFF)
set(LIBXML2_WITH_LZMA ON)
set(LIBXML2_WITH_ZLIB ON)

if(LIBXML2_WITH_ICONV)
  find_dependency(Iconv)
  list(APPEND LIBXML2_LIBRARIES    ${Iconv_LIBRARIES})
  list(APPEND LIBXML2_INCLUDE_DIRS ${Iconv_INCLUDE_DIRS})
endif()

if(NOT LIBXML2_SHARED)
  set(LIBXML2_DEFINITIONS -DLIBXML_STATIC)

  if(LIBXML2_WITH_THREADS)
    find_dependency(Threads)
    list(APPEND LIBXML2_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
  endif()

  if(LIBXML2_WITH_ICU)
    find_dependency(ICU COMPONENTS data i18n uc)
    list(APPEND LIBXML2_LIBRARIES    ${ICU_LIBRARIES})
  endif()

  if(LIBXML2_WITH_LZMA)
    find_dependency(LibLZMA)
    list(APPEND LIBXML2_LIBRARIES    ${LIBLZMA_LIBRARIES})
  endif()

  if(LIBXML2_WITH_ZLIB)
    find_dependency(ZLIB)
    list(APPEND LIBXML2_LIBRARIES    ${ZLIB_LIBRARIES})
  endif()

  if(UNIX)
    list(APPEND LIBXML2_LIBRARIES m)
  endif()

  if(WIN32)
    list(APPEND LIBXML2_LIBRARIES ws2_32)
  endif()
endif()

# whether libxml2 has dso support
set(LIBXML2_MODULES ON)

mark_as_advanced(LIBXML2_LIBRARY LIBXML2_XMLCATALOG_EXECUTABLE LIBXML2_XMLLINT_EXECUTABLE)
