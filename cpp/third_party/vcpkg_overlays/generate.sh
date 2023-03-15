#!/bin/bash
# Keep a complete copy of the modified vcpkg ports to avoid surprises
set -e

overlays=$(realpath $(dirname $BASH_SOURCE))

cd "${overlays}/../../" # cpp folder

echo "For the contents of this folder:" > $overlays/LICENSE
cat vcpkg/LICENSE.txt >> $overlays/LICENSE

cp vcpkg/ports/folly/* $overlays/folly
sed -i -e 's/PATCHES$/&\n        no_exception_tracer.patch/' \
       -e '/^vcpkg_cmake_configure/,+10 s/OPTIONS$/&\n        -DFOLLY_NO_EXCEPTION_TRACER=ON/' \
       $overlays/folly/portfile.cmake
