if(NOT ${ARCTICDB_USING_CONDA})
    add_subdirectory(pybind11)
    add_subdirectory(xxhash)
    add_subdirectory(rapidcheck)
    add_subdirectory(rapidcheck/extras/gtest)
endif()
add_subdirectory(lmdbcxx)
add_subdirectory(Remotery)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/Remotery/lib)
<<<<<<< HEAD
if(NOT ${ARCTICDB_USING_CONDA})
    add_subdirectory(semimap EXCLUDE_FROM_ALL)
endif()

add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)
add_subdirectory(rapidcheck/extras/gtest EXCLUDE_FROM_ALL)
=======
add_subdirectory(semimap EXCLUDE_FROM_ALL)
>>>>>>> 51f4336 (Support rapidcheck as a dependency from conda-forge)
add_subdirectory(recycle EXCLUDE_FROM_ALL)
add_subdirectory(msgpack-c EXCLUDE_FROM_ALL)
