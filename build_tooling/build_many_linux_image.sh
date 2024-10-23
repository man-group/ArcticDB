#!/bin/bash

set -e

if [[ -z "$manylinux_image" ]] ; then
    echo Resolving pinned image for cibuildwheel == \
        v${cibuildwheel_ver:?'Must set either manylinux_image or cibuildwheel_ver environment variable'}

    url="https://github.com/pypa/cibuildwheel/raw/v${cibuildwheel_ver}/cibuildwheel/resources/pinned_docker_images.cfg"
    manylinux_image=$(curl -sL "$url" | awk "/${image_grep:-manylinux_2_28_x86_64}/ { print \$3 ; exit }" )
    if [[ -z "$manylinux_image" ]] ; then
        echo "Failed to parse source image from cibuildwheel repo: ${url}" >&2
        exit 1
    fi
fi

if [[ -z "$sccache_ver" ]] ; then
    sccache_ver=`curl -sL -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" \
        https://api.github.com/repos/mozilla/sccache/releases/latest | jq -r ".tag_name"`
fi

echo "Building:
* From: ${manylinux_image}
* To: ${output_tag:="local_manylinux:latest"}
* sccache_ver=${sccache_ver}
"

cd `mktemp -d`
trap "rm -rf $PWD" EXIT

wget -nv https://github.com/mozilla/sccache/releases/download/$sccache_ver/sccache-$sccache_ver-x86_64-unknown-linux-musl.tar.gz
tar xvf sccache*.tar.gz
mv sccache-*/sccache .
chmod 555 sccache



echo "
FROM $manylinux_image

RUN dnf update -y
RUN dnf install -y python3.11 python3.11-devel python3.11-pip curl wget zip unzip tar perl-IPC-Cmd \
flex krb5-devel cyrus-sasl-devel epel-release libcurl-devel

RUN rpm --import https://download.mono-project.com/repo/xamarin.gpg
RUN dnf config-manager --add-repo https://download.mono-project.com/repo/centos8-stable.repo
RUN dnf install -y mono-complete

RUN dnf clean all

RUN export CMAKE_C_COMPILER=/opt/rh/gcc-toolset-12/root/bin/gcc
RUN export CMAKE_CXX_COMPILER=/opt/rh/gcc-toolset-12/root/bin/g++
RUN export LD_LIBRARY_PATH=/opt/rh/gcc-toolset-12/root/usr/lib64:/opt/rh/gcc-toolset-12/root/usr/lib:/opt/rh/gcc-toolset-12/root/usr/lib64/dyninst:${LD_LIBRARY_PATH}
RUN export PATH=/opt/rh/gcc-toolset-12/root/usr/bin:${PATH}

ADD sccache /usr/local/bin/

LABEL io.arcticdb.cibw_ver=\"${cibuildwheel_ver}\" io.arcticdb.base=\"${manylinux_image}\"
" > Dockerfile

docker build -t $output_tag .
