add_library(remotery_static STATIC lib/Remotery.c)
target_include_directories(remotery_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lib>)
