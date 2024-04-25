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
RUN rpm --import https://repo.almalinux.org/almalinux/RPM-GPG-KEY-AlmaLinux
RUN yum update -y && \
    yum install -y zip jq less gcc-toolset-13-gdb \
      openssl-devel cyrus-sasl-devel gcc-toolset-13-libatomic-devel libcurl-devel python3-devel flex && \
    yum clean all && touch /etc/arcticdb_deps_installed

ENV CMAKE_C_COMPILER=/opt/rh/gcc-toolset-13/root/usr/bin/gcc
ENV CMAKE_CXX_COMPILER=/opt/rh/gcc-toolset-13/root/usr/bin/g++
ENV CC=/opt/rh/gcc-toolset-13/root/usr/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-13/root/usr/bin/g++
ENV LD_LIBRARY_PATH=/opt/rh/gcc-toolset-13/root/usr/lib64:/opt/rh/gcc-toolset-13/root/usr/lib:$LD_LIBRARY_PATH
ENV PATH=/opt/rh/gcc-toolset-13/root/usr/bin:$PATH

LABEL io.arcticdb.cibw_ver=\"${cibuildwheel_ver}\" io.arcticdb.base=\"${manylinux_image}\"
" > Dockerfile

docker build -t $output_tag .
