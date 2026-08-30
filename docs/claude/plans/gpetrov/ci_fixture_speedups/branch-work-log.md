# Branch work log: gpetrov/ci_fixture_speedups

Three unrelated stalls found while chasing the integration jobs, which were 45-47 min and the critical path once
Windows was fixed.

## localhost resolves to ::1 first

On Windows the moto, GCP and azurite fixtures bind IPv4 only, so every connection through the name wastes about
2 s failing over: 100 connects take 205 s via `localhost` and 0.5 s via `127.0.0.1`. The fixtures now use the
literal and the test CA covers it.

This needs the moto change in the same commit. moto keys one Flask app, and one set of buckets, per value
returned by `get_backend_for_host`, so once the S3 and IAM endpoints stopped agreeing on a host string they got
separate backends and every bucket created through one was invisible to the other (`NoSuchBucket`).

## test_mongo_retryable_network_error, ~1,000 s in every run

The test kills mongod and runs six operations, each waiting the default 120 s `SelectionTimeoutMs` per backoff
attempt; teardown then spent 72 s sending `shutdown` to a server that was already dead. Scoped the timeouts to
the test and skipped the shutdown when the process has exited. Not runnable locally (no mongod); verified on CI.

## Six azurite tests, ~123 s or ~186 s each, but under 1 s alone

Debug-level timing of the `Submitting/Submitted DeleteBlob batch` pairs showed 967 batch deletes in a run, 966
instant and exactly one taking 122.4 s. It is not azurite: 60 concurrent batch deletes driven from Python peak at
0.06 s. It is the Azure C++ SDK's curl pool reusing a connection azurite (node, 5 s `keepAliveTimeout`) has
already closed. Deterministic repro: write, idle 6 s, write with prune - 5-6 s instead of 0.08 s.

Added `AzureStorage.HttpKeepAlive` (default 1, so unchanged for users) wired to
`CurlTransportOptions::HttpKeepAlive`. With it off the local xdist repro goes from "1 of 967 batches takes 122.4 s"
to "1002 batches, slowest 0.0 s". CI test jobs set it to 0. Worth reporting upstream to azure-sdk-for-cpp.
