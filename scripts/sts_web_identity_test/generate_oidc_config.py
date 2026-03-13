#!/usr/bin/env python3
"""Generate OIDC discovery documents (openid-configuration and JWKS) from an RSA public key."""

import argparse
import base64
import json
import os

from cryptography.hazmat.primitives.serialization import load_pem_public_key


def b64url_uint(n: int, length: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes(length, "big")).rstrip(b"=").decode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pubkey", required=True, help="Path to RSA public key PEM file")
    parser.add_argument("--bucket", required=True, help="S3 bucket name for OIDC issuer URL")
    parser.add_argument("--output-dir", required=True, help="Directory to write output files")
    args = parser.parse_args()

    with open(args.pubkey, "rb") as f:
        pub = load_pem_public_key(f.read())

    nums = pub.public_numbers()
    n_bytes = (nums.n.bit_length() + 7) // 8

    jwks = {
        "keys": [
            {
                "kty": "RSA",
                "kid": "test-key-1",
                "use": "sig",
                "alg": "RS256",
                "n": b64url_uint(nums.n, n_bytes),
                "e": b64url_uint(nums.e, 3),
            }
        ]
    }

    issuer = f"https://{args.bucket}.s3.amazonaws.com"
    discovery = {
        "issuer": issuer,
        "jwks_uri": f"{issuer}/keys.json",
        "authorization_endpoint": "urn:kubernetes:programmatic_authorization",
        "response_types_supported": ["id_token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }

    os.makedirs(args.output_dir, exist_ok=True)

    with open(os.path.join(args.output_dir, "openid-configuration"), "w") as f:
        json.dump(discovery, f, indent=2)

    with open(os.path.join(args.output_dir, "keys.json"), "w") as f:
        json.dump(jwks, f, indent=2)

    print(f"Wrote openid-configuration and keys.json to {args.output_dir}")


if __name__ == "__main__":
    main()
