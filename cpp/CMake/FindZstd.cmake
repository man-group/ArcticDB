#
# - Try to find Facebook zstd library
# This will define
# Zstd_FOUND
# Zstd_INCLUDE_DIR
# Zstd_LIBRARY
#

find_path(Zstd_INCLUDE_DIR NAMES zstd.h)

find_library(Zstd_LIBRARY_DEBUG NAMES zstdd)
find_library(Zstd_LIBRARY_RELEASE NAMES zstd)

include(SelectLibraryConfigurations)
SELECT_LIBRARY_CONFIGURATIONS(Zstd)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    Zstd DEFAULT_MSG
    Zstd_LIBRARY Zstd_INCLUDE_DIR
)

if (Zstd_FOUND)
    message(STATUS "Found Zstd: ${Zstd_LIBRARY}")
endif()

mark_as_advanced(Zstd_INCLUDE_DIR Zstd_LIBRARY)
