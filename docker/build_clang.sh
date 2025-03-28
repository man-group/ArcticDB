#!/bin/bash
set -eux

pushd /opt
    git clone --depth=1 https://github.com/llvm/llvm-project.git
    cd llvm-project
    mkdir build_clang
    cd build_clang
    cmake -DCMAKE_BUILD_TYPE=Release -DLLVM_ENABLE_PROJECTS="clang;lld" -DLLVM_ENABLE_RUNTIMES="compiler-rt" ../llvm
    make -j 60
    make install
    cd /opt
    rm -rf llvm-project
popd


