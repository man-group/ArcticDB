import requests
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory


def test_rate_limit(s3_storage_factory: MotoS3StorageFixtureFactory):  # Don't need to create buckets
    # Given a working Moto server
    s3 = s3_storage_factory
    requests.head(s3.endpoint).raise_for_status()

    # When request limiting is enabled
    requests.post(s3.endpoint + "/rate_limit", b"2").raise_for_status()

    # Then the specified number of requests work
    requests.head(s3.endpoint).raise_for_status()
    requests.head(s3.endpoint).raise_for_status()

    # Then rate limit how many times you call
    for _ in range(3):
        resp = requests.head(s3.endpoint)
        assert resp.status_code == 503
        assert resp.reason == "Slow down"

    # When we then reset
    requests.post(s3.endpoint + "/rate_limit", b"-1").raise_for_status()

    # Then working again
    requests.head(s3.endpoint).raise_for_status()
