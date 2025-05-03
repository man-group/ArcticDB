#!/bin/bash

set -euo pipefail

GCC_VERSION=$1
GCC_INSTALL_DIR="/opt/gcc/$GCC_VERSION"

curl "https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz" -o "/tmp/gcc-$GCC_VERSION.tar.gz"

mkdir -p "$GCC_INSTALL_DIR"
pushd /tmp
tar xvf "gcc-$GCC_VERSION.tar.gz"

mkdir "gcc-$GCC_VERSION-build"
pushd "gcc-$GCC_VERSION-build"
"../gcc-$GCC_VERSION/configure" --enable-languages=c,c++ --disable-multilib "--prefix=$GCC_INSTALL_DIR"
make -j$(nproc) && make install
if [ ! -f "$GCC_INSTALL_DIR/bin/cc" ]; then
  # For some reason some versions of gcc don't have a `cc` link to gcc. If missing we add one.
  ln -sf "$GCC_INSTALL_DIR/bin/gcc" "$GCC_INSTALL_DIR/bin/cc"
fi
popd # gcc-$GCC_VERSION-build

popd # /tmp

# Cleanup after building
rm -rf "/tmp/gcc-$GCC_VERSION-build" "/tmp/gcc-$GCC_VERSION" "/tmp/gcc-$GCC_VERSION-build" "/tmp/gcc-$GCC_VERSION.tar.gz"