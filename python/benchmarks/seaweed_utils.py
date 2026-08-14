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

Benchmark classes create their own SeaweedClient and use it to reset whatever cache bucket they
own at the start of setup_cache() (delete + recreate the bucket) so each run starts empty and the
previous run's data is reclaimed.

This module has no notion of any other bucket a benchmark class might use (e.g. a per-measurement
work bucket): creating, naming, and deleting those is entirely the calling class's responsibility.
"""

import asyncio
from typing import Iterable, List, Optional
from urllib.parse import urlsplit

import aioboto3
import boto3
import botocore
import requests
from arcticdb_ext.cpp_async import io_thread_count
from botocore.config import Config as BotoConfig


class SeaweedClient:
    """Client for the shared benchmark SeaweedFS server.

    The connection options default to the local benchmark server. The sync S3 client is created
    lazily on first use and then reused so repeated calls share its connection pool; copy_bucket
    spins up its own short-lived async client and event loop for the duration of the copy.
    """

    def __init__(
        self,
        master_url: str = "http://127.0.0.1:9333",
        s3_endpoint: str = "http://127.0.0.1:8333",
        # The benchmark server runs without an S3 identity file, so any credentials are accepted
        s3_access_key: str = "seaweed",
        s3_secret_key: str = "seaweed",
        # (connect, read): connection failures surface quickly, but dropping collections with many
        # volumes can take a while
        http_timeout=(5, 600),
    ):
        self.master_url = master_url
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.http_timeout = http_timeout

        # The sync client starts uninitialised and is created on the first call that needs it
        self.sync_client = None

    def _get_sync_client(self):
        """Sync S3 client for the benchmark server; created once and reused so repeated calls
        share the same client and connection pool instead of reconnecting every time."""
        if self.sync_client is None:
            self.sync_client = boto3.client(
                "s3",
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                region_name="us-east-1",
                config=BotoConfig(s3={"addressing_style": "path"}, max_pool_connections=64),
            )
        return self.sync_client

    def arctic_uri(self, bucket: str, path_prefix: Optional[str] = None) -> str:
        """Arctic connection string for a bucket on the benchmark SeaweedFS server, optionally scoped
        to a path_prefix within the bucket."""
        parsed = urlsplit(self.s3_endpoint)
        scheme = "s3s" if parsed.scheme == "https" else "s3"
        uri = f"{scheme}://{parsed.hostname}:{bucket}?access={self.s3_access_key}&secret={self.s3_secret_key}"
        if parsed.port:
            uri += f"&port={parsed.port}"
        if path_prefix is not None:
            uri += f"&path_prefix={path_prefix}"
        return uri

    def grow_bucket_volumes(self, bucket: str, count: Optional[int] = None) -> None:
        """
        Pre-create `count` volumes (default: ArcticDB's actual IO thread count) in the bucket's collection.

        Volumes serialise appends, so a bucket needs at least as many volumes as ArcticDB has IO
        threads for concurrent writes not to contend on volume-level locks. /vol/grow is additive,
        so only call this on a freshly created bucket.
        """
        count = count if count is not None else io_thread_count()
        response = requests.get(
            f"{self.master_url}/vol/grow", params={"collection": bucket, "count": count}, timeout=self.http_timeout
        )
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}
        error = body.get("error") if isinstance(body, dict) else None
        if error:
            raise RuntimeError(f"SeaweedFS master /vol/grow failed: {error}")
        response.raise_for_status()

    def create_bucket(self, bucket: str, volume_count: Optional[int] = None) -> None:
        """Create the bucket and pre-grow its volumes (see grow_bucket_volumes)."""
        self._get_sync_client().create_bucket(Bucket=bucket)
        self.grow_bucket_volumes(bucket, volume_count)

    def delete_bucket(self, bucket: str) -> None:
        try:
            self._get_sync_client().delete_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                return
            raise

    def _async_client(self, max_concurrency: int = 64):
        return aioboto3.Session().client(
            "s3",
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            region_name="us-east-1",
            config=BotoConfig(s3={"addressing_style": "path"}, max_pool_connections=max_concurrency),
        )

    @staticmethod
    async def _list_prefix(client, bucket: str, prefix: str) -> List[str]:
        keys = []
        async for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    async def _copy_bucket_async(
        self, source: str, destination: str, prefixes: Optional[Iterable[str]], max_concurrency: int
    ) -> int:
        async with self._async_client(max_concurrency) as client:
            # A TaskGroup runs the children concurrently and, if any raises, cancels the rest and
            # awaits them before propagating (no stranded tasks left pending on loop teardown).
            async with asyncio.TaskGroup() as tg:
                listings = [
                    tg.create_task(self._list_prefix(client, source, p))
                    for p in (prefixes if prefixes is not None else [""])
                ]
            # Deduplicated so overlapping prefixes cannot copy the same key twice
            keys = list(dict.fromkeys(key for listing in listings for key in listing.result()))
            # Every copy is issued at once; the client's connection pool caps how many actually run
            async with asyncio.TaskGroup() as tg:
                for key in keys:
                    tg.create_task(
                        client.copy_object(Bucket=destination, Key=key, CopySource={"Bucket": source, "Key": key})
                    )
            return len(keys)

    def copy_bucket(
        self, source: str, destination: str, prefixes: Optional[Iterable[str]] = None, max_concurrency: int = 64
    ) -> int:
        """
        Server-side copy of `source` into the existing bucket `destination`, optionally restricted
        to the given key prefixes. Returns the number of objects copied.

        Spins up a fresh event loop and async S3 client for the duration of the copy (async is only
        worth its overhead here, where the per-key copies run concurrently). Every copy is handed to
        the client at once; max_concurrency sizes its connection pool, which bounds how many are
        actually in flight.
        """
        return asyncio.run(self._copy_bucket_async(source, destination, prefixes, max_concurrency))
