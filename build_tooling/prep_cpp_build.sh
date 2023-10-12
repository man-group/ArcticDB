#!/bin/bash

pushd $(realpath $(dirname $BASH_SOURCE))/../cpp/vcpkg

if [[ -e "$VCPKG_INSTALLATION_ROOT" ]] ; then
    git fetch --unshallow file://$VCPKG_INSTALLATION_ROOT
elif [[ -n "$VCPKG_INSTALLATION_ROOT" && -e "/host$VCPKG_INSTALLATION_ROOT" ]] ; then
    git fetch --unshallow file:///host$VCPKG_INSTALLATION_ROOT
else
    git fetch --unshallow origin master
fi

cd ..

case `uname -a` in
*Microsoft*|MINGW*)
    if [[ -n "$GITHUB_ACTION" ]] ; then
        # Redirect the build directory to the more spacious C:
        if [[ "$MSYSTEM" != MINGW* ]] ; then echo "Must run $0 with git/MINGW bash" >&2
        elif [[ -e out ]] ; then echo "out directory cannot exist at this point" >&2
        else
            mkdir "${ARCTICDB_BUILD_DIR:?environment variable is not set}"
            MSYS=winsymlinks:nativestrict ln -s "$ARCTICDB_BUILD_DIR" out
        fi
        export ARCTICDB_VCPKG_INSTALL_DIR="C:\test_arcticdb_install_dir"  # TODO: should configure in build.yml
        mkdir ${ARCTICDB_VCPKG_INSTALL_DIR}
    fi

    mkdir vcpkg/buildtrees vcpkg/packages out || true
    MSYS_NO_PATHCONV=1 compact.exe /C vcpkg\\buildtrees vcpkg\\packages out
    ;;
*)
    if [[ -n "$ARCTICDB_BUILD_DIR" ]] ; then
        if [[ "$CIBUILDWHEEL" == "1" ]] ; then
            ARCTICDB_BUILD_DIR="/host$ARCTICDB_BUILD_DIR"
            rm -rf "$ARCTICDB_BUILD_DIR/*" || true
        fi
        [[ -d "$ARCTICDB_BUILD_DIR" ]] || mkdir -p "$ARCTICDB_BUILD_DIR"
        [[ ! -e out ]] || rm -rf out
        ln -s "$ARCTICDB_BUILD_DIR" out
    fi
esac

popd
