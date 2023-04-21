#!/bin/bash
set -e
tooling_dir="$(dirname $BASH_SOURCE)"
SOURCE_DIR=$tooling_dir/../cpp

# make abolute path from relative path
SOURCE_DIR=$(realpath $SOURCE_DIR)
echo $SOURCE_DIR

CMAKE=$CONDA_PREFIX/bin/cmake

mkdir -p build

# this changes once we merge the PR
BITMAGIC_INCLUDE_DIRS=$HOME/src/BitMagic/src
 
$CMAKE \
    -S $SOURCE_DIR  \
    -B build \
    -DTEST=OFF \
    -DARCTICDB_USING_CONDA=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    -DBITMAGIC_INCLUDE_DIRS=$BITMAGIC_INCLUDE_DIRS \
    -DSTATIC_LINK_STD_LIB=OFF \



