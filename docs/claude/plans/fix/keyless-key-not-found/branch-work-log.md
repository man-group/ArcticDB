# fix/keyless-key-not-found

- With `S3.Async=1`, a missing key made `do_async_read_impl` call `raise_s3_exception`, which throws a `KeyNotFoundException` built from a message only, so `keys()` dereferenced a null `shared_ptr`. `recurse_index_keys` (ignores missing keys during prune/delete) calls `e.keys()` and segfaulted.
- Fix at the source: the async read now mirrors the sync read (`raise_if_unexpected_error`, then `throw KeyNotFoundException(unencoded_key)`), so every read-path `KeyNotFoundException` carries its key again. Guarding the callers was rejected: `iterate_snapshots` needs the key to decide whether to swallow the error, so a keyless exception cannot be handled correctly there.
- Tests: gtest `S3StorageFixture.test_async_read_missing_key_in_exception` (mock S3 client) and pytest `test_delete_with_missing_index_key_s3_async` (moto S3, prune and delete_version). Both segfaulted before the fix.
