#!/bin/bash

cd $(realpath $(dirname $BASH_SOURCE))/../cpp/vcpkg

[[ -x vcpkg ]] || ./bootstrap-vcpkg.sh -disableMetrics
nuget="`which mono` `./vcpkg fetch nuget | tail -n 1`"  # which mono will return empty on windows
echo "Using nuget=$nuget"
url="https://nuget.pkg.github.com/$nuget_user/index.json"
$nuget sources add -source $url -storepasswordincleartext -name github \
    -username "${nuget_user:?nuget_user environment variable is not set}" \
    -password "${nuget_token:?nuget_token environment variable is not set}"
$nuget setapikey "$nuget_token" -source $url

case `uname -a` in
MINGW*|*WSL*)
    mkdir buildtrees packages ../out
    cmd.exe /C 'compact.exe /C buildtrees packages "..\\out"'
esac
