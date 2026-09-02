# Branch work log: gpetrov/fix-slice-added-race

## Fix the data race on the `slice_added` guard (#3381)

- Replaced the `std::vector<bool>` guard in `schedule_first_iteration` with `util::OnceFlags`
  (`cpp/arcticdb/util/once_flags.hpp`): per-position mutex plus a `uint8_t` flag, with the mutex held
  across the add. The old guard raced because `std::vector<bool>` is bit-packed, so neighbouring
  positions shared a word and lost each other's writes even under a per-position lock. A lost `true`
  made a second unit re-add an entity that already had its components and abort the worker on an EnTT
  assertion.
- The per-position mutex is kept deliberately. It is what orders the losing unit after the add: the
  loser goes straight on to `MemSegmentProcessingTask`, which gathers the components, so releasing it
  as soon as the flag is claimed (a plain `atomic_flag`) would let it read components that do not exist
  yet. An earlier revision of this branch made that mistake.
- `cpp/arcticdb/util/test/test_once_flags.cpp` covers both properties: no flag is lost when
  neighbouring positions are used concurrently, and no caller returns from `call_once` before the first
  caller's callable has finished.
- `ComponentManager::add_components` now raises an `InternalException` when an entity already has the
  component, instead of tripping the plain `assert()` inside EnTT that aborts the process.
- Left `CMAKE_CXX_FLAGS_RELWITHDEBINFO` without `-DNDEBUG`; the trade-off is described in the PR body.
