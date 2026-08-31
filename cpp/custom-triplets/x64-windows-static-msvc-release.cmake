set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_PLATFORM_TOOLSET_VERSION 14.41)
# Release-only: CI never opens the debug dependency libraries, and they are more than half of vcpkg_installed.
set(VCPKG_BUILD_TYPE release)
