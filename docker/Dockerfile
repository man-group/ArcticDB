FROM quay.io/pypa/manylinux_2_28_x86_64

RUN dnf update -y
RUN dnf remove -y 'gcc-toolset-*'
RUN dnf install -y zip flex bison gcc-toolset-11 gcc-toolset-11-gdb gcc-toolset-11-libatomic-devel krb5-devel cyrus-sasl-devel openssl-devel \
unzip tar epel-release jq wget libcurl-devel git-lfs cmake3 \
python3.11-devel python3.11-pip perl-IPC-Cmd

RUN dnf groupinstall -y 'Development Tools'
RUN dnf install -y mono-complete

RUN dnf clean all

RUN wget -nv https://github.com/mozilla/sccache/releases/download/v0.8.2/sccache-v0.8.2-x86_64-unknown-linux-musl.tar.gz
RUN tar xvf sccache*.tar.gz
RUN mv sccache-*/sccache .
RUN chmod 555 sccache

RUN cp sccache /usr/local/bin/

# Test-time services baked in so the pytest CI jobs don't have to download them on every run
RUN wget -nv https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel80-7.0.0.tgz && \
    tar -xzf mongodb-linux-x86_64-rhel80-7.0.0.tgz && \
    cp mongodb-linux-x86_64-rhel80-7.0.0/bin/* /usr/local/bin/ && \
    rm -rf mongodb-linux-x86_64-rhel80-7.0.0* && \
    mongod --version

RUN wget -nv https://nodejs.org/dist/v24.11.0/node-v24.11.0-linux-x64.tar.xz && \
    tar -xJf node-v24.11.0-linux-x64.tar.xz -C /usr/local --strip-components=1 && \
    rm node-v24.11.0-linux-x64.tar.xz && \
    node --version

RUN npm install -g azurite@3.36.0 && azurite --version

ENV CC=/opt/rh/gcc-toolset-11/root/bin/gcc
ENV CMAKE_C_COMPILER=/opt/rh/gcc-toolset-11/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-11/root/bin/g++
ENV CMAKE_CXX_COMPILER=/opt/rh/gcc-toolset-11/root/bin/g++
ENV LD_LIBRARY_PATH=/opt/rh/gcc-toolset-11/root/usr/lib64:/opt/rh/gcc-toolset-11/root/usr/lib:/opt/rh/gcc-toolset-11/root/usr/lib64/dyninst
ENV PATH=/opt/rh/gcc-toolset-11/root/usr/bin:/opt/python/cp311-cp311/bin:/usr/bin:${PATH}
