# Branch work log: gpetrov/ci_build_speedups

Sits on top of the whole CI stack (`ci_stress_nightly_only` + `build_core_objects` + `ci_publish_and_critical_path`).
Where that stack attacked test jobs, this attacks compile, which now gates everything: every test job starts within
0.0 min of its own compile finishing, so there is no runner queueing left to hide behind.

## The framing that matters: warm is not cold

Reference run 33316030365 is a **100% sccache-hit run** (`Compile requests 197 / Cache hits 197`), so its 250
compile job-min contain almost no compilation. Run 33179259321, with genuinely new C++, missed independently in all
six Linux jobs (27.9-38.1% hit) and each `Build wheel` took 34-39 min instead of 4-5. Different problem, different
fixes. What follows targets the warm run, which is what sets today's 40 min wall.

Warm budget inside the slowest compile job (Windows wheel 3.12, 860 s): vcpkg restore 262 s, cmake configure 276 s,
**ninja compile 113 s**, **link 378 s**. Compiling ArcticDB is 13% of it; linking is 44%.

## What is here

- **sccache stats on every platform.** `setup.py` only printed them when `AUDITWHEEL_PLAT` was set, i.e. only
  inside the manylinux container, so Windows - the critical path - was entirely unmeasured. Plus a step after the
  C++ compiles. This is first because it is what makes everything else measurable.
- **Nothing archived on the wheel path.** See below.
- **Windows fixed costs**: skip the ~2 min build-tools install when the image already has the pinned 14.41 toolset,
  and stop shipping 868 MB of per-TU PDBs on pull requests (kept on master and the nightly).

## The archive bug this fixes

`45ba80f7d` ("Link arcticdb_ext against the core objects") was supposed to remove a ~1 GB archive step from the
wheel path. It did not. `arcticdb_python` is a STATIC library, and INTERFACE sources propagate into the consumer,
so `target_link_libraries(arcticdb_python PUBLIC arcticdb_core_objects)` archived all 157 core objects into
`arcticdb_python.lib` instead - measured at **260 s** of the 378 s link in the reference run, against ~23 s for the
same step on Linux, which has thin archives (`ARCTICDB_THIN_ARCHIVES=ON`; `lib.exe` has no equivalent).

Fixed by splitting the INTERFACE target in two:

- `arcticdb_core_usage` - include dirs, compile definitions and link libraries, **no sources**. For intermediate
  targets that must compile against the core without carrying a copy of it.
- `arcticdb_core_objects` - the above plus `$<TARGET_OBJECTS:arcticdb_core_object>`. Link from the final binary
  only; linking it from a static library is exactly what caused the bug.

and making `arcticdb_python` an OBJECT library. Verified locally on the generated link line: **167 object files,
167 distinct, no arcticdb archive**, 157 from the core and 8 from the bindings.

### Measured on CI (run 33331318539, dispatch on this branch, against baseline 33316030365)

`arcticdb_python.lib` no longer appears in the Windows ninja output at all; the only static archives left are
third-party (`lmdb.lib`, `arcticdb_proto.lib`, 0-1 s each).

| Windows 3.12 | before | after |
|---|---:|---:|
| `arcticdb_python.lib` archive | 260s | gone |
| `arcticdb_ext.pyd` link | 118s | 59s |
| ninja phase | 491s | 203s |
| `Build wheel` step | 860s | 460s |

Across all six Windows compile jobs: **16.0 -> 12.2 min mean**, i.e. ~3.8 min per job and ~23 job-min, and 3.8 min
off the front of every Windows test chain. Linux and macOS are unchanged within noise, as expected - Linux already
had thin archives.

The risk worth recording: handing `arcticdb_ext.pyd` 165 loose objects instead of one library was expected to make
*its* link slower and possibly eat the saving. It did the opposite, 118 s -> 59 s; the linker had been re-reading
members out of the archive.

Windows sccache stats, visible for the first time thanks to the change above: `Compile requests 192 / Cache hits
183 / Cache misses 0 / hit rate 100.00 %`. So Windows is as warm as Linux and the Linux-only picture held.

The MSVC toolset check behaved as predicted: it logged the check, found no 14.41, and installed (108 s). No saving
today - it is correctness for when the image and the pin agree, nothing more.

## Deliberately not here

- **Release-only vcpkg triplets** (~2 min wall, ~25 job-min; Windows restores 3.6 GB of debug dependency libs it
  never opens). `VCPKG_BUILD_TYPE` is part of the ABI hash, so a new triplet name invalidates the whole NuGet
  binary cache and the first run rebuilds all ~135 packages from source. Worth doing, but it would swamp the
  measurements here, so it goes in its own run.
- **macOS `-j1`** (`RAM/5000` in `cpp/CMake/CpuCount.cmake` single-threads a 3-core runner) and Linux `-j3`->`-j4`.
  Job-minutes only - macOS is not on the critical path - and both need a peak-RSS measurement first.
- **Build-once-per-OS for the core.** Real (113 of 157 core TUs transitively include pybind11 or `Python.h`,
  through ~7 shallow leak points such as `column.hpp:30` and `native_tensor.hpp:20`) but a job-minutes play, not a
  wall play: the six compile jobs all start in the same second and miss independently, so deduplication only helps
  the next run. Capturing it within one run needs a build-once job feeding six link-only jobs, which serialises,
  and at zero queueing that trades wall for cost.
- **Precompiled headers.** Measured, same flags and machine: ~3 s/TU, ~6% overall - loading the 487 MB `.gch` eats
  most of the parse saving. And `cpp/CMake/pdb_generation.cmake:35` makes PCH a `FATAL_ERROR` alongside per-TU
  PDBs, so Windows cannot have both it and sccache.
- **Unity builds / splitting a monster TU.** All 165 core+bindings TUs timed; the worst is ~60 s
  (`version_core.cpp`) and the tail is flat, not spiky. Nothing to target.
