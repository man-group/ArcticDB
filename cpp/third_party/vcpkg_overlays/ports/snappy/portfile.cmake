vcpkg_from_github(
    fix_clang-cl_build.patch
    no-werror.patch
    pkgconfig.diff
)

file(COPY "${CURRENT_PORT_DIR}/snappy.pc.in" DESTINATION "${SOURCE_PATH}")

vcpkg_cmake_configure() 