# Branch work log: gpetrov/fix-slice-added-race

## Fix the data race on the `slice_added` guard (#3381)

- Replaced the `std::vector<bool>` + per-position `std::vector<std::mutex>` guard in
  `schedule_first_iteration` with `util::ClaimFlags`, one `std::atomic_flag` per segment. The old guard
  raced because `std::vector<bool>` is bit-packed, so neighbouring positions shared a word and lost each
  other's writes even under a per-position lock. A lost `true` made a second unit re-add an entity that
  already had its components and abort the worker on an EnTT assertion.
- Added `cpp/arcticdb/util/claim_flags.hpp` and a regression test
  `cpp/arcticdb/util/test/test_claim_flags.cpp` that claims neighbouring positions from 8 threads and
  asserts no claim is lost. The old implementation fails this test on essentially every iteration.
- `ComponentManager::add_components` now raises an `InternalException` when an entity already has the
  component, instead of tripping the plain `assert()` inside EnTT that aborts the process.
- Left `CMAKE_CXX_FLAGS_RELWITHDEBINFO` without `-DNDEBUG`; the trade-off is described in the PR body.
