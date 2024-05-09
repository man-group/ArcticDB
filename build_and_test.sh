#!/bin/bash

set -e

git submodule sync --recursive

rm -rf dist/ wheelhouse/

source /root/pyenvs/dev/bin/activate

python setup.py clean bdist_wheel

auditwheel repair dist/*.whl

python -m pip install --force-reinstall wheelhouse/*.whl "protobuf<5"
python -m pip install ray

catchsegv python -c "import ray;import arcticdb;print(arcticdb.__version__)" || exit 1

