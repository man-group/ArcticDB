if(NOT ${ARCTICDB_USING_CONDA})
    add_subdirectory(pybind11)
    add_subdirectory(xxhash)
    add_subdirectory(recycle)
    add_subdirectory(semimap EXCLUDE_FROM_ALL)
    add_subdirectory(msgpack-c)
    add_subdirectory(rapidcheck)
    add_subdirectory(rapidcheck/extras/gtest)
    add_subdirectory(Remotery)
    add_subdirectory(lmdbcxx)

    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/Remotery/lib)
endif()

