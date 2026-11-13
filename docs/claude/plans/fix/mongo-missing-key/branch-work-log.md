# fix/mongo-missing-key

- Salvaged from closed #3565 without its async S3 changes.
- Mongo reads now carry the missing key on the NoSuchKey/KeyNotFound server-error path, like the other backends (gtest `MongoMockStorageTest.TestReadNoSuchKeyCarriesKey`; failed before with an empty key list).
- `KeyNotFoundException::keys()` no longer assumes the exception carries keys: one built from a message has an empty list (gtest `KeyNotFoundException.keys_is_empty_when_built_from_message`; segfaulted before). `storage_exceptions.hpp` now includes what it uses.
- `iterate_snapshots` rethrows a `KeyNotFoundException` with no keys or naming another key, instead of treating it as its snapshot being deleted. Without this, the `keys()` change made master silently skip a keyless error (gtests `IterateSnapshots.RethrowsKeyNotFoundWithoutKeys`, `IterateSnapshots.IgnoresKeyNotFoundForVisitedSnapshot`).
