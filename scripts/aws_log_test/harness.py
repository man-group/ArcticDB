"""One-off harness: run a moto S3 round trip so AWS SDK logging (if enabled
via ARCTICDB_AWS_LogLevel_int in the environment) is exercised. Parent process
sets the env vars and captures fd-level stdout/stderr separately."""

import pandas as pd

# Importing arcticdb runs set_config_from_env_vars(os.environ), which maps
# ARCTICDB_AWS_LogLevel_int -> AWS.LogLevel etc.
from arcticdb import Arctic
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory


def main():
    with MotoS3StorageFixtureFactory(
        use_ssl=False, ssl_test_support=False, bucket_versioning=False
    ) as factory:
        bucket = factory.create_fixture()
        ac = Arctic(bucket.arctic_uri)
        ac.create_library("lib")
        lib = ac["lib"]
        df = pd.DataFrame({"a": [1, 2, 3]})
        lib.write("sym", df)
        result = lib.read("sym").data
        assert len(result) == 3
    print("HARNESS_DONE", flush=True)


if __name__ == "__main__":
    main()
