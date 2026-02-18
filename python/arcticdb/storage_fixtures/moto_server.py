import argparse

import werkzeug
from moto.server import DomainDispatcherApplication, create_backend_app


class HostDispatcherApplication(DomainDispatcherApplication):
    _reqs_till_rate_limit = -1

    def get_backend_for_host(self, host):
        """The stand-alone server needs a way to distinguish between S3 and IAM. We use the host for that"""
        if host is None:
            return None
        if "s3" in host or host == "localhost":
            return "s3"
        elif host == "127.0.0.1":
            return "iam"
        elif host == "moto_api":
            return "moto_api"
        else:
            raise RuntimeError(f"Unknown host {host}")

    def __call__(self, environ, start_response):
        path_info: bytes = environ.get("PATH_INFO", "")

        with self.lock:
            # Check for x-amz-checksum-mode header
            if environ.get("HTTP_X_AMZ_CHECKSUM_MODE") == "enabled":
                response_body = (
                    b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                    b"<Error><Code>MissingContentLength</Code>"
                    b"<Message>You must provide the Content-Length HTTP header.</Message></Error>"
                )
                start_response(
                    "411 Length Required", [("Content-Type", "text/xml"), ("Content-Length", str(len(response_body)))]
                )
                return [response_body]
            # Mock ec2 imds responses for testing
            if path_info in (
                "/latest/dynamic/instance-identity/document",
                b"/latest/dynamic/instance-identity/document",
            ):
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"Something to prove imds is reachable"]

            # Allow setting up a rate limit
            if path_info in ("/rate_limit", b"/rate_limit"):
                length = int(environ["CONTENT_LENGTH"])
                body = environ["wsgi.input"].read(length).decode("ascii")
                self._reqs_till_rate_limit = int(body)
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"Limit accepted"]

            if self._reqs_till_rate_limit == 0:
                response_body = (
                    b'<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message>'
                    b"<RequestId>176C22715A856A29</RequestId><HostId>9Gjjt1m+cjU4OPvX9O9/8RuvnG41MRb/18Oux2o5H5MY7ISNTlXN+Dz9IG62/ILVxhAGI0qyPfg=</HostId></Error>"
                )
                start_response(
                    "503 Slow Down", [("Content-Type", "text/xml"), ("Content-Length", str(len(response_body)))]
                )
                return [response_body]
            else:
                self._reqs_till_rate_limit -= 1

            # Lets add ability to identify type as S3
            if "/whoami" in path_info:
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"Moto AWS S3"]

        return super().__call__(environ, start_response)


class GcpHostDispatcherApplication(HostDispatcherApplication):
    """GCP's S3 implementation does not have batch delete."""

    def __call__(self, environ, start_response):
        path_info: bytes = environ.get("PATH_INFO", "")

        if environ["REQUEST_METHOD"] == "POST" and environ["QUERY_STRING"] == "delete":
            response_body = (
                b'<?xml version="1.0" encoding="UTF-8"?>'
                b"<Error>"
                b"<Code>NotImplemented</Code>"
                b"<Message>A header or query you provided requested a function that is not implemented.</Message>"
                b"<Details>POST ?delete is not implemented for objects.</Details>"
                b"</Error>"
            )
            start_response(
                "501 Not Implemented", [("Content-Type", "text/xml"), ("Content-Length", str(len(response_body)))]
            )
            return [response_body]

        # Lets add ability to identify type as GCP
        if "/whoami" in path_info:
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"Moto GCP"]

        return super().__call__(environ, start_response)


def run_s3_server(port, key_file, cert_file):
    werkzeug.run_simple(
        "0.0.0.0",
        port,
        HostDispatcherApplication(create_backend_app),
        threaded=True,
        ssl_context=(cert_file, key_file) if cert_file and key_file else None,
    )


def run_gcp_server(port, key_file, cert_file):
    werkzeug.run_simple(
        "0.0.0.0",
        port,
        GcpHostDispatcherApplication(create_backend_app),
        threaded=True,
        ssl_context=(cert_file, key_file) if cert_file and key_file else None,
    )


def main():
    parser = argparse.ArgumentParser(description="Run a mock S3 or GCP server")
    parser.add_argument("--port", type=int, required=True, help="Port to run the server on")
    parser.add_argument("--key-file", type=str, default=None, help="Path to SSL key file")
    parser.add_argument("--cert-file", type=str, default=None, help="Path to SSL certificate file")
    parser.add_argument("--server-type", type=str, choices=["s3", "gcp"], required=True, help="Server type: s3 or gcp")

    args = parser.parse_args()

    if args.server_type == "s3":
        run_s3_server(args.port, args.key_file, args.cert_file)
    else:
        run_gcp_server(args.port, args.key_file, args.cert_file)


if __name__ == "__main__":
    main()
