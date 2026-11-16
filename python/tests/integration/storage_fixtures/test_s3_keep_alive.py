import contextlib
import http.client
import selectors
import socket
import ssl
import threading
from urllib.parse import urlsplit

from arcticdb.storage_fixtures.s3 import KeepAliveRequestHandler, MotoS3StorageFixtureFactory
from werkzeug.serving import make_server


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
    # A swallowed request would otherwise hang until the server times the connection out
    conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=10)
    try:
        yield conn
    finally:
        conn.close()
        server.shutdown()
        server.server_close()


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
    def hello_app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"hello"]

    timed_out_selects = []

    class RecordingSelector(selectors.DefaultSelector):
        def select(self, timeout=None):
            events = super().select(timeout)
            if not events:
                timed_out_selects.append(timeout)
            return events

    # Process-wide, but in this process only werkzeug's drain creates selectors while the test runs
    monkeypatch.setattr(selectors, "DefaultSelector", RecordingSelector)
    with _serve_with_keep_alive_handler(hello_app) as conn:
        for _ in range(3):
            conn.request("GET", "/")
            assert conn.getresponse().read() == b"hello"
    assert timed_out_selects == []
