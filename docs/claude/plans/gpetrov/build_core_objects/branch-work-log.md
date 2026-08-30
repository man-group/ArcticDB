# Branch work log: gpetrov/build_core_objects

Independent of the CI stack; branched straight off master.

`arcticdb_ext` linked `arcticdb_core_static`, so the archive was built and then largely re-read at link time. It
now consumes `$<TARGET_OBJECTS:arcticdb_core_object>` through an INTERFACE target that forwards the usage
requirements off `arcticdb_core_static` with `$<TARGET_PROPERTY:...>`, which keeps include dirs, compile
definitions and link libraries identical while skipping the archive step.
