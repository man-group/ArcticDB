# Differs from stock triplets/x64-linux.cmake by building dependencies as shared libraries
# so relinking arcticdb_ext after a source edit stays cheap. The overall I/O is lower,
# since the target libraries don't have to copy all of the .text. This in turn lowers
# pressure on OS file caches.
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# xxhash has some scary warning about needing to know internal struct layouts
# I didn't researhc it enough, and the library isn't that large.
if(PORT MATCHES "^xxhash$")
    set(VCPKG_LIBRARY_LINKAGE static)
endif()