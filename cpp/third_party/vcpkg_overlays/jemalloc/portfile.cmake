# ArcticDB: derived from the vcpkg jemalloc port for linking into a dlopen'ed Python extension next to the system
# malloc. Uses the release tarball, whose pre-generated configure script needs no autotools on the build machine.
# - je_ prefix: do not define malloc/free, so C code keeps the system allocator.
# - no C++ operators: ArcticDB defines its own operator new/delete.
# - no initial-exec TLS: it can exhaust the static TLS block of a dlopen'ed module.
# - no macOS zone: it would replace the default malloc zone for the whole process.
vcpkg_download_distfile(ARCHIVE
    URLS "https://github.com/jemalloc/jemalloc/releases/download/${VERSION}/jemalloc-${VERSION}.tar.bz2"
    FILENAME "jemalloc-${VERSION}.tar.bz2"
    SHA512 46e4829c88078618860cb75aff074f281306eeb9fa624b7457dd9464d811723f80c27af1c3648e04c9fa74df5c087ae4e415c3b611602fd849f83f37f8cd4f9d
)
vcpkg_extract_source_archive(SOURCE_PATH ARCHIVE "${ARCHIVE}")

set(opts "--with-jemalloc-prefix=je_" "--disable-cxx" "--disable-initial-exec-tls")
if(VCPKG_TARGET_IS_OSX)
    list(APPEND opts "--disable-zone-allocator")
endif()

vcpkg_make_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS ${opts}
)

vcpkg_make_install()

vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/tools")

file(INSTALL "${SOURCE_PATH}/COPYING" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}" RENAME copyright)
