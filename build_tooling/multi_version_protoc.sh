#!/bin/bash
set -e -o pipefail

proto_ver=$1
case $proto_ver in
3)
    grpc_ver="<=1.48.*"
    ;;
4)
    grpc_ver=">=1.49"
    ;;
*)
    echo "Usage: $0 {3 or 4 for the protobuf Python package version} [optional output dir]"
    exit 1
    ;;
esac

# virtualenv-lite to avoid hard-coding Man internal locations
PYTHONPATH=/tmp/arcticdb_protoc
rm -rf $PYTHONPATH ; mkdir $PYTHONPATH
dir=${2:-/tmp/proto}/$proto_ver
[[ -e $dir ]] || mkdir -p $dir
py="${3:-`which python || which python3`}"

"$py" -m pip install --disable-pip-version-check --target=$PYTHONPATH grpcio-tools${grpc_ver}

export PYTHONPATH
set -x

# Check the transitively install version:
"$py" -c "import google.protobuf ; print(google.protobuf.__version__)" | grep "^${proto_ver}\."

"$py" -m grpc_tools.protoc -Icpp/proto --python_out=$dir cpp/proto/arcticc/pb2/*.proto
touch $dir/arcticc/__init__.py
touch $dir/arcticc/pb2/__init__.py

rm -rf $PYTHONPATH
