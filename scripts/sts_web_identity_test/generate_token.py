#!/usr/bin/env python3
"""Generate a signed JWT token for STS AssumeRoleWithWebIdentity testing."""

import argparse
import time

import jwt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", required=True, help="Path to RSA private key PEM file")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (used as OIDC issuer)")
    parser.add_argument("--output", required=True, help="Path to write the token file")
    parser.add_argument("--ttl", type=int, default=3600, help="Token TTL in seconds (default: 3600)")
    args = parser.parse_args()

    with open(args.key) as f:
        private_key = f.read()

    now = int(time.time())
    payload = {
        "iss": f"https://{args.bucket}.s3.amazonaws.com",
        "sub": "test-subject",
        "aud": "sts.amazonaws.com",
        "iat": now,
        "exp": now + args.ttl,
    }

    token = jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": "test-key-1"})

    with open(args.output, "w") as f:
        f.write(token)

    print(f"Token written to {args.output} (expires in {args.ttl}s)")


if __name__ == "__main__":
    main()
