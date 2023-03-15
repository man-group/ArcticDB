add_subdirectory(pybind11)
add_subdirectory(xxhash)
add_subdirectory(lmdbcxx)
add_subdirectory(Remotery)

set(SPDLOG_FMT_EXTERNAL ON)
add_subdirectory(spdlog EXCLUDE_FROM_ALL)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/Remotery/lib)
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)
add_subdirectory(rapidcheck/extras/gtest EXCLUDE_FROM_ALL)
add_subdirectory(semimap EXCLUDE_FROM_ALL)
add_subdirectory(recycle EXCLUDE_FROM_ALL)
add_subdirectory(msgpack-c EXCLUDE_FROM_ALL)
