if(NOT ${ARCTICDB_USING_CONDA})
    add_subdirectory(pybind11)
    add_subdirectory(recycle)
    add_subdirectory(semimap EXCLUDE_FROM_ALL)
    add_subdirectory(msgpack-c)
    add_subdirectory(Remotery)
    add_subdirectory(entt)
endif()

# lmdbcxx is built unconditionally: it applies CVE-2019-16224..16228 security
# patches that were never upstreamed to the OpenLDAP LMDB project and therefore
# are absent from the conda-forge lmdb package even at 0.9.35.
# The resulting 'lmdb' STATIC target is linked on both vcpkg and conda paths.
add_subdirectory(lmdbcxx)
