set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
# Unpinned so the runner image's MSVC is used. See "Install Required MSVC" in .github/workflows/build_steps.yml.
# set(VCPKG_PLATFORM_TOOLSET_VERSION 14.41)
