set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# Release-only: CI never opens the debug dependency libraries, and they are more than half of vcpkg_installed.
set(VCPKG_BUILD_TYPE release)
