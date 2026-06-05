# Chainload toolchain: build selected vcpkg ports with clang-23 so their
# -fsanitize=thread instrumentation shares the same TSan runtime as the
# clang-built ArcticDB. Used by x64-linux-tsan.cmake for threading-relevant ports.
#
# Set the compiler, then include vcpkg's standard Linux toolchain so all the
# normal setup (CMAKE_SYSTEM_PROCESSOR, fPIC, VCPKG_*_FLAGS handling) is kept.
# linux.cmake only sets the compiler for cross-compiles, so our choice stands.
set(CMAKE_C_COMPILER /users/is/aseaton/bin/clang)
set(CMAKE_CXX_COMPILER /users/is/aseaton/bin/clang++)
include("/users/is/aseaton/source/ArcticDB-worktree/tree-one/cpp/vcpkg/scripts/toolchains/linux.cmake")
