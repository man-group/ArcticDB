set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# Build dependencies without LTO so a clang/lld toolchain can link them.
# The default x64-linux cached artifacts carry GCC fat-LTO objects whose
# embedded .gnu.lto sections clang's linkers (lld/mold) cannot consume.
set(VCPKG_C_FLAGS "-fno-lto")
set(VCPKG_CXX_FLAGS "-fno-lto")
