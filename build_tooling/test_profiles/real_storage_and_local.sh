# In  this case the simulated real storages will be skipped,
# because even they are enabled, the real storages are also enabled
# and then simulated will not be executed
export ARCTICDB_LOCAL_STORAGE_TESTS_ENABLED=1
export ARCTICDB_STORAGE_LMDB=
export ARCTICDB_STORAGE_AZURITE=
export ARCTICDB_STORAGE_MONGO=
export ARCTICDB_STORAGE_MEM=
export ARCTICDB_STORAGE_NFS=
export ARCTICDB_STORAGE_SIM_S3=
export ARCTICDB_STORAGE_SIM_GCP=

export ARCTICDB_TEST_ENCODING_V1=1
export ARCTICDB_TEST_ENCODING_V2=0

export PERSISTENT_STORAGE_TESTS_ENABLED=1
export ARCTICDB_STORAGE_AWS_S3=1
export ARCTICDB_STORAGE_GCP=1