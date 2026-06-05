set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# Threading-relevant ports built with clang-23 + ThreadSanitizer so TSan can see
# their synchronization. folly: clears the futures/ThreadLocal false positives.
# AWS SDK/curl stack: makes the async S3 transfer (the suspected corruptor) visible.
# All other ports stay gcc + -fno-lto (clang-linkable, uninstrumented).
set(_adb_tsan_ports
    folly
    aws-sdk-cpp aws-crt-cpp aws-checksums
    aws-c-common aws-c-cal aws-c-io aws-c-http aws-c-auth aws-c-s3
    aws-c-event-stream aws-c-mqtt aws-c-compression aws-c-sdkutils
    s2n curl)

list(FIND _adb_tsan_ports "${PORT}" _adb_tsan_idx)
if(NOT _adb_tsan_idx EQUAL -1)
    set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE
        "/users/is/aseaton/source/ArcticDB-worktree/tree-one/cpp/custom-triplets/clang-tsan-toolchain.cmake")
    set(VCPKG_C_FLAGS "-fsanitize=thread -fno-lto -Wno-error")
    set(VCPKG_CXX_FLAGS "-fsanitize=thread -fno-lto -Wno-error -DFMT_CONSTEVAL=")
    set(VCPKG_LINKER_FLAGS "-fsanitize=thread -fuse-ld=lld")
else()
    set(VCPKG_C_FLAGS "-fno-lto")
    set(VCPKG_CXX_FLAGS "-fno-lto")
endif()
