#!/bin/bash

export VCPKG_ROOT=`realpath $(dirname $BASH_SOURCE)/../cpp/vcpkg`

pushd $VCPKG_ROOT
PLATFORM_VCPKG_ROOT=`cygpath -wa . 2>/dev/null || pwd`

[[ -x vcpkg ]] || ./bootstrap-vcpkg.sh -disableMetrics
nuget="`which mono 2>/dev/null` `./vcpkg fetch nuget | tail -n 1`"  # which mono will return empty on windows
echo "Using nuget=$nuget"

VCPKG_BINARY_SOURCES="clear;nugettimeout,1200;nuget,github,readwrite"
url="https://nuget.pkg.github.com/$VCPKG_NUGET_USER/index.json"
$nuget sources add -source $url -storepasswordincleartext -name github \
    -username "${VCPKG_NUGET_USER:?environment variable is not set}" \
    -password "${VCPKG_NUGET_TOKEN:?environment variable is not set}"
$nuget setapikey "$VCPKG_NUGET_TOKEN" -source $url

if [[ -n "$VCPKG_MAN_NUGET_TOKEN" ]] ; then
    $nuget sources add -source "https://nuget.pkg.github.com/man-group/index.json" -storepasswordincleartext -name man \
    -username "${VCPKG_MAN_NUGET_USER:-$VCPKG_NUGET_USER}" \
    -password "$VCPKG_MAN_NUGET_TOKEN"
    VCPKG_BINARY_SOURCES="$VCPKG_BINARY_SOURCES;nuget,man,read"
fi

popd
