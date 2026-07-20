"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.

Utilities for ASV benchmarks that store data on SeaweedFS.

A single SeaweedFS server (started by build_tooling/start_seaweed.sh) is shared by every
benchmark in the ASV run. The server disables automatic vacuuming, so reclaiming space is
the benchmarks' responsibility: hard-delete a whole bucket (SeaweedFS backs each bucket
with a collection of the same name, and dropping the collection removes its volume files
wholesale, no vacuum needed).

Benchmark classes share the ARCTICDB_CACHE_BUCKET bucket for data written by setup_cache().
Every such class must call reset_arcticdb_cache_bucket() at the start of its setup_cache()
so it starts from an empty bucket and the previous class's data is reclaimed.

This module has no notion of any other bucket a benchmark class might use (e.g. a per-measurement
work bucket): creating, naming, and deleting those is entirely the calling class's responsibility.
"""

import asyncio
import atexit
import os
from typing import Iterable, List, Optional
from urllib.parse import urlsplit

import aioboto3
import boto3
import requests
from arcticdb_ext.cpp_async import io_thread_count
from botocore.config import Config as BotoConfig

SEAWEED_MASTER_URL = os.getenv("ARCTICDB_SEAWEED_MASTER_URL", "http://127.0.0.1:9333")
SEAWEED_FILER_URL = os.getenv("ARCTICDB_SEAWEED_FILER_URL", "http://127.0.0.1:8888")
SEAWEED_S3_ENDPOINT = os.getenv("ARCTICDB_SEAWEED_S3_ENDPOINT", "http://127.0.0.1:8333")
# The benchmark server runs without an S3 identity file, so any credentials are accepted
SEAWEED_S3_ACCESS_KEY = os.getenv("ARCTICDB_SEAWEED_S3_ACCESS_KEY", "seaweed")
SEAWEED_S3_SECRET_KEY = os.getenv("ARCTICDB_SEAWEED_S3_SECRET_KEY", "seaweed")

# Underscores are not allowed in bucket names, hence the dashes
ARCTICDB_CACHE_BUCKET = "arcticdb-cache"

# (connect, read): connection failures surface quickly, but dropping collections with many
# volumes can take a while
_HTTP_TIMEOUT = (5, 600)

# Reused across grow_bucket_volumes()/delete_bucket() calls so requests to the master/filer
# keep-alive their TCP connections instead of reconnecting every time
_http_session = requests.Session()


_s3_client_singleton = None


def s3_client():
    """Sync S3 client for the benchmark server; a singleton so repeated calls reuse the same
    client and connection pool instead of reconnecting every time."""
    global _s3_client_singleton
    if _s3_client_singleton is None:
        _s3_client_singleton = boto3.client(
            "s3",
            endpoint_url=SEAWEED_S3_ENDPOINT,
            aws_access_key_id=SEAWEED_S3_ACCESS_KEY,
            aws_secret_access_key=SEAWEED_S3_SECRET_KEY,
            region_name="us-east-1",
            config=BotoConfig(s3={"addressing_style": "path"}, max_pool_connections=64),
        )
    return _s3_client_singleton


_async_s3_loop = None
_async_s3_client_cm = None
_async_s3_client_singleton = None


def _async_s3_loop_get():
    global _async_s3_loop
    if _async_s3_loop is None:
        _async_s3_loop = asyncio.new_event_loop()
    return _async_s3_loop


async def async_s3_client():
    """Async S3 client for the benchmark server; a singleton, entered once and kept open for
    the life of the process. Only worth the asyncio overhead where calls need real concurrency
    (e.g. copy_bucket's per-key copies); everything else should use the plain sync s3_client().

    aioboto3/aiohttp connectors are bound to the event loop they were created on, so this must
    only be awaited on the dedicated _async_s3_loop_get() loop. It is awaitable (rather than a
    sync accessor) because the loop is already running when the cold-start __aenter__ happens,
    and run_until_complete cannot be nested."""
    global _async_s3_client_cm, _async_s3_client_singleton
    if _async_s3_client_singleton is None:
        _async_s3_client_cm = aioboto3.Session().client(
            "s3",
            endpoint_url=SEAWEED_S3_ENDPOINT,
            aws_access_key_id=SEAWEED_S3_ACCESS_KEY,
            aws_secret_access_key=SEAWEED_S3_SECRET_KEY,
            region_name="us-east-1",
            config=BotoConfig(s3={"addressing_style": "path"}, max_pool_connections=64),
        )
        _async_s3_client_singleton = await _async_s3_client_cm.__aenter__()
    return _async_s3_client_singleton


@atexit.register
def _close_async_s3_client():
    """Close the async client and its loop so aiohttp does not warn about unclosed sessions at
    interpreter exit."""
    if _async_s3_client_singleton is not None:
        _async_s3_loop.run_until_complete(_async_s3_client_cm.__aexit__(None, None, None))
    if _async_s3_loop is not None:
        _async_s3_loop.close()


def arctic_uri(bucket: str) -> str:
    """Arctic connection string for a bucket on the benchmark SeaweedFS server."""
    parsed = urlsplit(SEAWEED_S3_ENDPOINT)
    scheme = "s3s" if parsed.scheme == "https" else "s3"
    uri = f"{scheme}://{parsed.hostname}:{bucket}?access={SEAWEED_S3_ACCESS_KEY}&secret={SEAWEED_S3_SECRET_KEY}"
    if parsed.port:
        uri += f"&port={parsed.port}"
    return uri


def grow_bucket_volumes(bucket: str, count: Optional[int] = None) -> None:
    """
    Pre-create `count` volumes (default: ArcticDB's actual IO thread count) in the bucket's collection.

    Volumes serialise appends, so a bucket needs at least as many volumes as ArcticDB has IO
    threads for concurrent writes not to contend on volume-level locks. /vol/grow is additive,
    so only call this on a freshly created bucket.
    """
    count = count if count is not None else io_thread_count()
    response = _http_session.get(
        f"{SEAWEED_MASTER_URL}/vol/grow", params={"collection": bucket, "count": count}, timeout=_HTTP_TIMEOUT
    )
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = {}
    error = body.get("error") if isinstance(body, dict) else None
    if error:
        raise RuntimeError(f"SeaweedFS master /vol/grow failed: {error}")
    response.raise_for_status()


def create_bucket(bucket: str, volume_count: Optional[int] = None) -> None:
    """Create the bucket and pre-grow its volumes (see grow_bucket_volumes)."""
    s3_client().create_bucket(Bucket=bucket)
    grow_bucket_volumes(bucket, volume_count)


def list_buckets() -> List[str]:
    return [b["Name"] for b in s3_client().list_buckets()["Buckets"]]


def delete_bucket(bucket: str) -> None:
    """
    Hard-delete a bucket without per-object deletes.

    Dropping the bucket's backing collection removes its volume files wholesale and reclaims
    the space immediately, no vacuum needed. The filer metadata is removed afterwards (chunk
    deletion skipped: the data is already gone with the collection).
    """
    response = _http_session.get(
        f"{SEAWEED_MASTER_URL}/col/delete", params={"collection": bucket}, timeout=_HTTP_TIMEOUT
    )
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = {}
    error = body.get("error") if isinstance(body, dict) else None
    # A bucket that never had data has no backing collection yet, which is fine to "delete"
    if error and not ("not exist" in error or "not found" in error):
        raise RuntimeError(f"SeaweedFS master /col/delete failed: {error}")
    if not error:
        response.raise_for_status()
    response = _http_session.delete(
        f"{SEAWEED_FILER_URL}/buckets/{bucket}",
        params={"recursive": "true", "ignoreRecursiveError": "true", "skipChunkDeletion": "true"},
        timeout=_HTTP_TIMEOUT,
    )
    if response.status_code not in (200, 204, 404):
        response.raise_for_status()


def reset_arcticdb_cache_bucket() -> str:
    """
    Hard-delete and recreate the shared setup_cache bucket.

    Must be called at the start of every setup_cache() that stores data on SeaweedFS.
    """
    try:
        delete_bucket(ARCTICDB_CACHE_BUCKET)
    except requests.ConnectionError as e:
        raise RuntimeError(
            f"Cannot reach the SeaweedFS benchmark server at {SEAWEED_MASTER_URL}; "
            "start it with: build_tooling/start_seaweed.sh start"
        ) from e
    create_bucket(ARCTICDB_CACHE_BUCKET)
    return ARCTICDB_CACHE_BUCKET


async def _gather_all(coros):
    """gather() that waits for every task even when one fails (a fail-fast gather would strand
    the rest as pending tasks on the shared loop, where they would resume mid-flight during an
    unrelated later run_until_complete), then raises the first error."""
    results = await asyncio.gather(*coros, return_exceptions=True)
    errors = [result for result in results if isinstance(result, BaseException)]
    if errors:
        raise errors[0]
    return results


def copy_bucket(
    source: str, destination: str, prefixes: Optional[Iterable[str]] = None, max_concurrency: int = 64
) -> int:
    """
    Server-side copy of `source` into the existing bucket `destination`, optionally restricted
    to the given key prefixes. Returns the number of objects copied.

    max_concurrency bounds how many copies are in flight at once; the connection pool is fixed
    at 64 by the async_s3_client() singleton.
    """

    async def _copy():
        client = await async_s3_client()

        async def list_prefix(prefix):
            prefix_keys = []
            async for page in client.get_paginator("list_objects_v2").paginate(Bucket=source, Prefix=prefix):
                prefix_keys.extend(obj["Key"] for obj in page.get("Contents", []))
            return prefix_keys

        listed = await _gather_all(list_prefix(p) for p in (prefixes if prefixes is not None else [""]))
        # Deduplicated so overlapping prefixes cannot copy the same key twice
        keys = list(dict.fromkeys(key for prefix_keys in listed for key in prefix_keys))

        in_flight = asyncio.Semaphore(max_concurrency)

        async def copy_one(key):
            async with in_flight:
                await client.copy_object(Bucket=destination, Key=key, CopySource={"Bucket": source, "Key": key})

        await _gather_all(copy_one(key) for key in keys)
        return len(keys)

    return _async_s3_loop_get().run_until_complete(_copy())
