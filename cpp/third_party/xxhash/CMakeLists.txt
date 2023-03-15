add_library(xxhash_static STATIC xxhash.c)
target_include_directories(xxhash_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

