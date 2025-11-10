export ARCTICDB_PERSISTENT_STORAGE_TESTS=1
export ARCTICDB_PERSISTENT_STORAGE_STRATEGY_BRANCH=blah
export ARCTICDB_PERSISTENT_STORAGE_SHARED_PATH_PREFIX=blah
export ARCTICDB_PERSISTENT_STORAGE_UNIQUE_PATH_PREFIX=blah
export ARCTICDB_REAL_S3_BUCKET=blah
export ARCTICDB_REAL_S3_ENDPOINT=https://s3.eu-west-1.amazonaws.com/
export ARCTICDB_REAL_S3_REGION=eu-west-1
export ARCTICDB_REAL_S3_CLEAR=0
export ARCTICDB_REAL_S3_ACCESS_KEY=blah
export ARCTICDB_REAL_S3_SECRET_KEY=blah

export ARCTICDB_REAL_AZURE_CONNECTION_STRING=blah
export ARCTICDB_REAL_AZURE_CONTAINER=blah


python -m asv run -v --show-stderr master^! --bench .*Resample.peakmem.*mean.*
python -m asv run -v --show-stderr HEAD^! --bench .*Resample.peakmem.*mean.*
python -m asv compare master HEAD
