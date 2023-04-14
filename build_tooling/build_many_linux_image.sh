#!/bin/bash

set -e

if [[ -z "$manylinux_image" ]] ; then
    echo Resolving pinned image for cibuildwheel == \
        v${cibuildwheel_ver:?'Must set either manylinux_image or cibuildwheel_ver environment variable'}

    url="https://github.com/pypa/cibuildwheel/raw/v${cibuildwheel_ver}/cibuildwheel/resources/pinned_docker_images.cfg"
    manylinux_image=$(curl -sL "$url" | awk "/${image_grep:-manylinux2014_x86_64}/ { print \$3 ; exit }" )
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
RUN cp \$(which bash) /usr/local/bin/suid_bash && chown 0:0 /usr/local/bin/suid_bash && chmod u+s /usr/local/bin/suid_bash
RUN rpmkeys --import 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF' && \
    curl https://download.mono-project.com/repo/centos7-stable.repo > /etc/yum.repos.d/mono-centos7-stable.repo && \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://olcentgbl.trafficmanager.net|' /etc/yum.repos.d/CentOS-Base.repo && \
    sed -ir 's/socket_timeout=3/socket_timeout=1/ ; s/maxhostfileage.*/maxhostfileage=1/ ;\
            s/#?exclude.*/exclude=.edu/' /etc/yum/pluginconf.d/fastestmirror.conf
ADD sccache /usr/local/bin/
RUN yum update -y && \
    yum install -y zip jq openssl-devel cyrus-sasl-devel devtoolset-10-libatomic-devel libcurl-devel python3-devel && \
    rpm -Uvh --nodeps \$(repoquery --location mono-{core,web,devel,data,wcf,winfx}) && \
    yum clean all && touch /etc/arcticdb_deps_installed
LABEL io.arcticdb.cibw_ver=\"${cibuildwheel_ver}\" io.arcticdb.base=\"${manylinux_image}\"
" > Dockerfile

docker build -t $output_tag .
