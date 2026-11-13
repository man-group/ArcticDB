import http.client
import ssl
import time
from urllib.parse import urlsplit

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


def _connect(endpoint_url):
    endpoint = urlsplit(endpoint_url)
    if endpoint.scheme == "https":
        return http.client.HTTPSConnection(endpoint.hostname, endpoint.port, context=ssl._create_unverified_context())
    return http.client.HTTPConnection(endpoint.hostname, endpoint.port)


def test_mock_server_keeps_connections_alive(s3_storage_factory: MotoS3StorageFixtureFactory):
    # A connection per request leaves a TIME_WAIT entry per request, which exhausts loopback ports on Windows CI
    conn = _connect(s3_storage_factory.endpoint)
    local_addresses = []

    def send(method, path, body=None, headers={}, **kwargs):
        conn.request(method, path, body, headers, **kwargs)
        local_addresses.append(conn.sock.getsockname())
        response = conn.getresponse()
        return response.status, response.read()

    try:
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
        # Rejected before the body is read, so the server must skip the body to find the next request
        status, _ = send("PUT", "/bucket/key", b"x" * 100_000, {"x-amz-checksum-mode": "enabled"})
        assert status == 411
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
        status, _ = send(
            "PUT", "/bucket/key", iter([b"x" * 50_000] * 2), {"x-amz-checksum-mode": "enabled"}, encode_chunked=True
        )
        assert status == 411
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
    finally:
        conn.close()
    assert len(set(local_addresses)) == 1, local_addresses


def test_mock_server_answers_next_request_on_connection_promptly(s3_storage_factory: MotoS3StorageFixtureFactory):
    # werkzeug waits 10ms for more request data after every response, which would delay each request on a kept-alive
    # connection by that much
    conn = _connect(s3_storage_factory.endpoint)
    round_trips = []
    try:
        for _ in range(20):
            start = time.perf_counter()
            conn.request("GET", "/whoami")
            assert conn.getresponse().read() == b"Moto AWS S3"
            round_trips.append(time.perf_counter() - start)
    finally:
        conn.close()
    # The first request opens the connection, so it is not delayed by a previous response
    assert min(round_trips[1:]) < 0.005, round_trips


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
