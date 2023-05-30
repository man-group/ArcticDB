#!/bin/bash
set -e
TOOLING_DIR="$(dirname $BASH_SOURCE)"
PYTHON=$(which python)
PYTHON_VERSION=$($PYTHON -c "import sys; print('{0[0]}.{0[1]}'.format(sys.version_info))")
echo "PYTHON_VERSION:" $PYTHON_VERSION

unameOut="$(uname -s)"
case "${unameOut}" in
    Linux*)     OS=linux;;
    Darwin*)    OS=darwin;;
    *)          OS="UNKNOWN:${unameOut}"
esac
echo ${OS}

CPP_SOURCE_DIR=$TOOLING_DIR/../cpp
PRESET_NAME="$OS-conda-release"


echo "CONDA" $CONDA_PREFIX


echo "CPP"

# do an if false to disable the block

if true; then


pushd $CPP_SOURCE_DIR

ARCTICDB_USING_CONDA=1 cmake \
    -DTEST=NO \
    -DARCTICDB_BUILD_PYTHON_EXTENSION=OFF \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    --preset $PRESET_NAME


ARCTICDB_USING_CONDA=1 cmake \
    --build \
    "out/$PRESET_NAME-build"  \
    --target install
popd

fi


PYWTHON_CPP_SOURCE_DIR=$TOOLING_DIR/../cpp/arcticdb/python
pushd $PYWTHON_CPP_SOURCE_DIR

ARCTICDB_USING_CONDA=1 cmake \
    -DTEST=NO \
    -DARCTICDB_BUILD_PYTHON_EXTENSION=OFF \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    --preset $PRESET_NAME

ARCTICDB_USING_CONDA=1 cmake \
    --build \
    "out/$PRESET_NAME-build"  \
    --target install


popd