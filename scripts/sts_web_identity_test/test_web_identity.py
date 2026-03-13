#!/usr/bin/env python3
"""
Test that SafeSTSWebIdentityCredentialsProvider works end-to-end.

Expects these env vars to be set (run_test.sh handles this):
    AWS_WEB_IDENTITY_TOKEN_FILE - path to JWT token file
    AWS_ROLE_ARN                - IAM role ARN to assume
    AWS_DEFAULT_REGION          - AWS region

All other AWS credential env vars (AWS_ACCESS_KEY_ID, etc.) must be UNSET
so that earlier providers in MyAWSCredentialsProviderChain fail and the
chain reaches SafeSTSWebIdentityCredentialsProvider.
"""

import argparse
import multiprocessing
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

import arcticdb as adb
from arcticdb.util.test import assert_frame_equal
from arcticdb.arctic import Arctic


def _delete_symbol(args):
    """Top-level function so it can be pickled by ProcessPoolExecutor."""
    uri, lib_name, symbol = args
    ac = Arctic(uri)
    lib = ac.get_library(lib_name)
    lib.delete(symbol)
    print(f"Deleted {symbol} in subprocess", flush=True)


def check_env():
    """Verify the environment is set up correctly."""
    required = ["AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_DEFAULT_REGION"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    # These should NOT be set - they'd let earlier providers succeed
    should_be_unset = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"]
    leaking = [v for v in should_be_unset if os.environ.get(v)]
    if leaking:
        print(f"WARNING: These env vars are set and may bypass the web identity path: {', '.join(leaking)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket for test data")
    parser.add_argument("--region", required=True, help="AWS region")
    args = parser.parse_args()

    check_env()

    lib_name = f"web_identity_test_{int(time.time())}"
    uri = f"s3://s3.{args.region}.amazonaws.com:{args.bucket}?aws_auth=true&path_prefix=sts_web_identity_test"

    print(f"Connecting to: {uri}")
    start = time.monotonic()

    ac = adb.Arctic(uri)

    try:
        print(f"Creating library: {lib_name}")
        lib = ac.create_library(lib_name)

        # Test write
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        lib.write("test_symbol", df)
        print("Write succeeded")

        # Test read
        result = lib.read("test_symbol").data
        assert_frame_equal(result, df)
        print("Read succeeded, data matches")

        # Test list
        symbols = lib.list_symbols()
        assert "test_symbol" in symbols
        print("List symbols succeeded")

        # Test multiprocess delete (exercises credential provider in forked processes)
        symbols = [f"mp_test_{i}" for i in range(4)]
        for sym in symbols:
            lib.write(sym, df)
        print(f"Wrote {len(symbols)} symbols for multiprocess delete test")

        with ProcessPoolExecutor(max_workers=len(symbols), mp_context=multiprocessing.get_context("fork")) as executor:
            list(executor.map(_delete_symbol, [(uri, lib_name, sym) for sym in symbols]))

        remaining = lib.list_symbols()
        for sym in symbols:
            assert not lib.has_symbol(sym), f"Symbol {sym} was not deleted"
            assert sym not in remaining, f"Symbol {sym} still appears in list_symbols after deletion"
        print(f"Multiprocess delete succeeded ({len(symbols)} symbols across {len(symbols)} workers)")

        elapsed = time.monotonic() - start
        print(f"\nAll operations completed in {elapsed:.1f}s")

        if elapsed > 10:
            print(f"WARNING: Took {elapsed:.1f}s - this might indicate the CRT-based provider was hit")
        else:
            print("Timing looks good (< 10s) - SafeSTSWebIdentityCredentialsProvider is working correctly")

        print("\n=== PASS ===")

    except Exception as e:
        elapsed = time.monotonic() - start
        print(f"\n=== FAIL ({elapsed:.1f}s) ===")
        print(f"Error: {e}")
        sys.exit(1)

    finally:
        try:
            ac.delete_library(lib_name)
            print(f"Cleaned up library: {lib_name}")
        except Exception:
            print(f"WARNING: Failed to clean up library: {lib_name}")


if __name__ == "__main__":
    main()
