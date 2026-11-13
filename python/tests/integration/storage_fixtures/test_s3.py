import contextlib
import http.client
import selectors
import socket
import ssl
import threading
from urllib.parse import urlsplit

import botocore.exceptions
import pytest
import requests
from arcticdb.storage_fixtures.s3 import (
    KeepAliveRequestHandler,
    MotoS3StorageFixtureFactory,
    MotoGcpS3StorageFixtureFactory,
)
import werkzeug.serving
from werkzeug.serving import make_server

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

    def send(method, path, body=None, headers=None, **kwargs):
        conn.request(method, path, body, headers or {}, **kwargs)
        local_addresses.append(conn.sock.getsockname())
        response = conn.getresponse()
        return response.status, response.read()

    try:
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
        # Rejected before the body is read, so the server must skip the body to find the next request
        status, _ = send("PUT", "/bucket/key", b"x" * 100_000, {"x-amz-checksum-mode": "enabled"})
        assert status == 411
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
        # The same with a chunked body
        status, _ = send(
            "PUT", "/bucket/key", iter([b"x" * 50_000] * 2), {"x-amz-checksum-mode": "enabled"}, encode_chunked=True
        )
        assert status == 411
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
        # The mock's own responses have a body even for HEAD, which must not be sent ahead of the next response
        assert send("HEAD", "/whoami") == (200, b"")
        assert send("HEAD", "/bucket/key", headers={"x-amz-checksum-mode": "enabled"}) == (411, b"")
        assert send("GET", "/whoami") == (200, b"Moto AWS S3")
    finally:
        conn.close()
    assert len(set(local_addresses)) == 1, local_addresses


@contextlib.contextmanager
def _serve_with_keep_alive_handler(app):
    server = make_server("127.0.0.1", 0, app, threaded=True, request_handler=KeepAliveRequestHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    conn = http.client.HTTPConnection("127.0.0.1", server.server_port)
    try:
        yield conn
    finally:
        conn.close()
        server.shutdown()
        server.server_close()


def _hello_app(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"hello"]


def test_mock_server_disables_nagle():
    # A response goes out in two sends, headers then body. With Nagle on, a kept-alive connection holds the body back
    # until the client ACKs the headers, which Linux delays by 40ms, so every request would take 40ms
    def nodelay_app(environ, start_response):
        nodelay = environ["werkzeug.socket"].getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"on" if nodelay else b"off"]

    with _serve_with_keep_alive_handler(nodelay_app) as conn:
        conn.request("GET", "/")
        assert conn.getresponse().read() == b"on"


def test_mock_server_does_not_wait_for_more_request_data(monkeypatch):
    # After each response werkzeug waits up to 10ms for more request data to discard. On a kept-alive connection the
    # next request is only read after that wait, so every request would take 10ms longer
    timed_out_selects = []

    class RecordingSelector(selectors.DefaultSelector):
        def select(self, timeout=None):
            events = super().select(timeout)
            if not events:
                timed_out_selects.append(timeout)
            return events

    monkeypatch.setattr(werkzeug.serving.selectors, "DefaultSelector", RecordingSelector)
    with _serve_with_keep_alive_handler(_hello_app) as conn:
        for _ in range(3):
            conn.request("GET", "/")
            assert conn.getresponse().read() == b"hello"
    assert timed_out_selects == []


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
