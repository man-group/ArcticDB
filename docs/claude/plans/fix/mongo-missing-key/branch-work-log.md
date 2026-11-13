# fix/mongo-missing-key

- Salvaged from closed #3565 without its async S3 changes.
- Mongo reads now carry the missing key on the NoSuchKey/KeyNotFound server-error path, like the other backends (gtest `MongoMockStorageTest.TestReadNoSuchKeyCarriesKey`; failed before with an empty key list).
- `KeyNotFoundException::keys()` no longer assumes the exception carries keys: one built from a message has an empty list (gtest `KeyNotFoundException.keys_is_empty_when_built_from_message`; segfaulted before). `storage_exceptions.hpp` now includes what it uses.
- `iterate_snapshots` rethrows a `KeyNotFoundException` with no keys or naming another key, instead of treating it as its snapshot being deleted. Without this, the `keys()` change made master silently skip a keyless error (gtests `IterateSnapshots.RethrowsKeyNotFoundWithoutKeys`, `IterateSnapshots.IgnoresKeyNotFoundForVisitedSnapshot`).
- Review follow-up: `generate_segments_from_keys` rethrows the original exception (`throw;`, unnamed catch for MSVC) so its message is kept; `storage_exceptions.hpp` qualifies `entity::` names instead of a namespace-scope `using namespace entity;`, and `keys_` defaults to an empty list for every constructor; `iterate_snapshots` names its skip condition and logs the snapshot key; `IgnoresKeyNotFoundForVisitedSnapshot` now checks that a second snapshot is still visited after the first is skipped.
