FROM quay.io/pypa/manylinux_2_28_x86_64

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

RUN wget -nv https://github.com/mozilla/sccache/releases/download/v0.8.2/sccache-v0.8.2-x86_64-unknown-linux-musl.tar.gz
RUN tar xvf sccache*.tar.gz
RUN mv sccache-*/sccache .
RUN chmod 555 sccache

RUN cp sccache /usr/local/bin/

WORKDIR /root
