# Overlay triplet for macOS arm64.
#
# Identical to the stock vcpkg triplets/arm64-osx.cmake, but pins
# VCPKG_OSX_DEPLOYMENT_TARGET so every dependency (folly, fmt, spdlog, ...) is
# compiled with -mmacosx-version-min=15.0, matching CMAKE_OSX_DEPLOYMENT_TARGET
# in the macos-* presets.
#
# Why this matters: the wheel is built on the macos-26 runner with Xcode 26's
# libc++. If a dependency is compiled at the host's default deployment target,
# libc++'s availability gating lets it emit a *strong* reference to newer dylib
# symbols (e.g. std::__1::__hash_memory) that do not exist in the macOS 15 system
# libc++. arcticdb_ext then links fine on macos-26 but fails to dlopen at runtime
# on macOS 15 with "Symbol not found: __ZNSt3__113__hash_memoryEPKvm".
# Building the deps at 15.0 makes libc++ take the back-deployable path instead.
#
# Keeping the target in the triplet (rather than only in the MACOSX_DEPLOYMENT_TARGET
# env var) also folds it into vcpkg's ABI hash, so the binary cache cannot serve
# stale dependencies that were built against a newer target.
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)
set(VCPKG_OSX_DEPLOYMENT_TARGET "15.0")
