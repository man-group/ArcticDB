import http.client
import ssl
import urllib.parse

import botocore.exceptions
import pytest
import requests
from arcticdb.storage_fixtures.s3 import MotoS3StorageFixtureFactory, MotoGcpS3StorageFixtureFactory

from tests.util.mark import SKIP_CONDA_MARK


def test_rate_limit(s3_storage_factory: MotoS3StorageFixtureFactory):  # Don't need to create buckets
    # Given a working Moto server
    s3 = s3_storage_factory
    requests.head(s3.endpoint, verify=s3.client_cert_file).raise_for_status()

    # When request limiting is enabled
    requests.post(s3.endpoint + "/rate_limit", b"2", verify=s3.client_cert_file).raise_for_status()

    # Then the specified number of requests work
    requests.head(s3.endpoint, verify=s3.client_cert_file).raise_for_status()
    requests.head(s3.endpoint, verify=s3.client_cert_file).raise_for_status()

    # Then rate limit how many times you call
    for _ in range(3):
        resp = requests.head(s3.endpoint, verify=s3.client_cert_file)
        assert resp.status_code == 503
        assert resp.reason == "Slow Down"

    # TODO: If this test fails before this point, rate limit doesn't get reset and other tests which
    # share the same session will fail as well
    # When we then reset
    requests.post(s3.endpoint + "/rate_limit", b"-1", verify=s3.client_cert_file).raise_for_status()

    # Then working again
    requests.head(s3.endpoint, verify=s3.client_cert_file).raise_for_status()


@SKIP_CONDA_MARK
def test_gcp_no_batch_delete(gcp_storage_factory: MotoGcpS3StorageFixtureFactory):
    # Given
    gcp = gcp_storage_factory
    bucket = gcp.create_fixture()
    boto_bucket = bucket.get_boto_bucket()
    boto_bucket.put_object(Key="key1", Body=b"contents1")
    boto_bucket.put_object(Key="key2", Body=b"contents2")
    assert [k.key for k in boto_bucket.objects.all()] == ["key1", "key2"]

    # When
    with pytest.raises(botocore.exceptions.ClientError):
        boto_bucket.delete_objects(
            Delete={
                "Objects": [
                    {"Key": "key1"},
                    {"Key": "key2"},
                ]
            }
        )

    # Then
    # We're checking that our simulator doesn't handle batch deletes (like GCP does not)
    assert [k.key for k in boto_bucket.objects.all()] == ["key1", "key2"]


def test_s3_has_batch_delete(s3_storage_factory: MotoS3StorageFixtureFactory):
    # Given
    s3 = s3_storage_factory
    bucket = s3.create_fixture()
    boto_bucket = bucket.get_boto_bucket()
    boto_bucket.put_object(Key="key1", Body=b"contents1")
    boto_bucket.put_object(Key="key2", Body=b"contents2")
    assert [k.key for k in boto_bucket.objects.all()] == ["key1", "key2"]

    # When
    boto_bucket.delete_objects(
        Delete={
            "Objects": [
                {"Key": "key1"},
                {"Key": "key2"},
            ]
        }
    )

    # Then
    # We're checking that our simulator does handle batch deletes (like AWS does)
    assert [k.key for k in boto_bucket.objects.all()] == []


@pytest.mark.parametrize(
    "factory_name", ["s3_storage_factory", "s3_ssl_disabled_storage_factory", "gcp_storage_factory"]
)
def test_mock_server_keeps_connections_alive(factory_name, request):
    # A server that closes the connection after every response forces a new TCP connection per S3 request. Under
    # xdist that piles up TIME_WAIT sockets and exhausts the loopback ports on Windows.
    factory = request.getfixturevalue(factory_name)
    url = urllib.parse.urlparse(factory.endpoint)
    if url.scheme == "https":
        conn = http.client.HTTPSConnection(
            url.hostname, url.port, context=ssl.create_default_context(cafile=factory.client_cert_file)
        )
    else:
        conn = http.client.HTTPConnection(url.hostname, url.port)
    try:
        conn.connect()
        sock = conn.sock
        for _ in range(50):
            conn.request("GET", "/whoami")
            response = conn.getresponse()
            response.read()
            assert response.status == 200
            assert response.getheader("Connection", "").lower() != "close"
            assert not response.will_close
            assert conn.sock is sock
    finally:
        conn.close()
