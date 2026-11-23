from collections import namedtuple

import pandas as pd
import pytest

from arcticdb import LibraryOptions
from arcticdb.options import EnterpriseLibraryOptions
from arcticdb.version_store.library import StagedDataFinalizeMethod, UpdatePayload, WriteMetadataPayload, WritePayload
from arcticdb_ext.storage import ModifiableLibraryOption

SYM = "sym"
DF_1 = pd.DataFrame({"col": [1, 2]}, index=pd.date_range("2024-01-01", periods=2))
DF_2 = pd.DataFrame({"col": [3, 4]}, index=pd.date_range("2024-01-03", periods=2))
DF_UPDATE = pd.DataFrame({"col": [10]}, index=pd.date_range("2024-01-01", periods=1))
MERGE_SOURCE = pd.DataFrame({"col": [99]}, index=pd.date_range("2024-01-02", periods=1))


def setup_one_write(lib):
    lib.write(SYM, DF_1, prune_previous_versions=False)


def setup_write_then_append(lib):
    lib.write(SYM, DF_1, prune_previous_versions=False)
    lib.append(SYM, DF_2, prune_previous_versions=False)


def setup_write_then_stage(lib):
    lib.write(SYM, DF_1, prune_previous_versions=False)
    lib.stage(SYM, DF_2)


def list_versions(lib):
    return sorted(v.version for v in lib.list_versions(SYM))


Operation = namedtuple("Operation", ["setup", "run", "created_versions"])

OPERATIONS = [
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write(SYM, DF_2, **kwargs),
            created_versions=(0, 1),
        ),
        id="write",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write_pickle(SYM, {"a": 1}, **kwargs),
            created_versions=(0, 1),
        ),
        id="write_pickle",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write_batch([WritePayload(SYM, DF_2)], **kwargs),
            created_versions=(0, 1),
        ),
        id="write_batch",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write_pickle_batch([WritePayload(SYM, {"a": 1})], **kwargs),
            created_versions=(0, 1),
        ),
        id="write_pickle_batch",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.append(SYM, DF_2, **kwargs),
            created_versions=(0, 1),
        ),
        id="append",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.append_batch([WritePayload(SYM, DF_2)], **kwargs),
            created_versions=(0, 1),
        ),
        id="append_batch",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.update(SYM, DF_UPDATE, **kwargs),
            created_versions=(0, 1),
        ),
        id="update",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.update_batch([UpdatePayload(SYM, DF_UPDATE)], **kwargs),
            created_versions=(0, 1),
        ),
        id="update_batch",
    ),
    pytest.param(
        Operation(
            setup=setup_write_then_stage,
            run=lambda lib, kwargs: lib.finalize_staged_data(SYM, StagedDataFinalizeMethod.WRITE, **kwargs),
            created_versions=(0, 1),
        ),
        id="finalize_staged_data",
    ),
    pytest.param(
        Operation(
            setup=setup_write_then_stage,
            run=lambda lib, kwargs: lib.sort_and_finalize_staged_data(SYM, StagedDataFinalizeMethod.WRITE, **kwargs),
            created_versions=(0, 1),
        ),
        id="sort_and_finalize_staged_data",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write_metadata(SYM, {"m": 1}, **kwargs),
            created_versions=(0, 1),
        ),
        id="write_metadata",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.write_metadata_batch([WriteMetadataPayload(SYM, {"m": 1})], **kwargs),
            created_versions=(0, 1),
        ),
        id="write_metadata_batch",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.delete_data_in_range(
                SYM, (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")), **kwargs
            ),
            created_versions=(0, 1),
        ),
        id="delete_data_in_range",
    ),
    pytest.param(
        Operation(
            setup=setup_one_write,
            run=lambda lib, kwargs: lib.merge(SYM, MERGE_SOURCE, **kwargs),
            created_versions=(0, 1),
        ),
        id="merge",
    ),
    pytest.param(
        Operation(
            setup=setup_write_then_append,
            run=lambda lib, kwargs: lib.compact_data(SYM, **kwargs),
            created_versions=(0, 1, 2),
        ),
        id="compact_data",
    ),
    pytest.param(
        Operation(
            setup=setup_write_then_append,
            run=lambda lib, kwargs: lib.compact_data_batch([SYM], **kwargs),
            created_versions=(0, 1, 2),
        ),
        id="compact_data_batch",
    ),
]


@pytest.mark.parametrize("operation", OPERATIONS)
def test_library_option_true_prunes_when_argument_omitted(lmdb_library_factory, operation):
    lib = lmdb_library_factory(LibraryOptions(prune_previous_versions=True))
    operation.setup(lib)
    operation.run(lib, {})
    assert list_versions(lib) == [max(operation.created_versions)]


@pytest.mark.parametrize("operation", OPERATIONS)
def test_library_option_defaults_to_false(lmdb_library_factory, operation):
    lib = lmdb_library_factory()
    operation.setup(lib)
    operation.run(lib, {})
    assert list_versions(lib) == sorted(operation.created_versions)


@pytest.mark.parametrize("operation", OPERATIONS)
def test_explicit_false_argument_overrides_library_option(lmdb_library_factory, operation):
    lib = lmdb_library_factory(LibraryOptions(prune_previous_versions=True))
    operation.setup(lib)
    operation.run(lib, {"prune_previous_versions": False})
    assert list_versions(lib) == sorted(operation.created_versions)


def test_modify_library_option_to_true_prunes_when_argument_omitted(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)
    lib.write(SYM, DF_1, prune_previous_versions=False)
    ac.modify_library_option(lib, ModifiableLibraryOption.PRUNE_PREVIOUS_VERSIONS, True)
    assert lib._nvs.lib_cfg().lib_desc.version.write_options.prune_previous_version
    lib.write(SYM, DF_2)
    assert list_versions(lib) == [1]


def test_modify_library_option_to_false_stops_pruning(lmdb_storage, lib_name):
    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name, library_options=LibraryOptions(prune_previous_versions=True))
    ac.modify_library_option(lib, ModifiableLibraryOption.PRUNE_PREVIOUS_VERSIONS, False)
    assert not lib._nvs.lib_cfg().lib_desc.version.write_options.prune_previous_version
    lib.write(SYM, DF_1)
    lib.write(SYM, DF_2)
    assert list_versions(lib) == [0, 1]


@pytest.mark.parametrize("options_class", [LibraryOptions, EnterpriseLibraryOptions])
@pytest.mark.parametrize("other", [None, "a string", 1])
def test_options_equality_with_other_type_is_false(options_class, other):
    options = options_class()
    assert options != other
    assert not (options == other)
    assert options in [other, options]


@pytest.mark.parametrize("options_class", [LibraryOptions, EnterpriseLibraryOptions])
def test_options_equality_with_same_type(options_class):
    assert options_class() == options_class()


def test_modify_enterprise_sequencer_library_options(lmdb_storage, lib_name):
    from arcticdb.options import ModifiableEnterpriseLibraryOption
    from arcticdb_ext.storage import UnsupportedLibraryOptionValue

    ac = lmdb_storage.create_arctic()
    lib = ac.create_library(lib_name)

    # Valid updates persisted into _arctic_cfg
    ac.modify_library_option(
        lib,
        ModifiableEnterpriseLibraryOption.SEQUENCER_URL,
        "https://seq-0.svc.cluster.local:9100, https://seq-1.svc.cluster.local:9100",
    )
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_CONFLICT_POLICY, "keep_both")
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_INLINE_THRESHOLD_BYTES, 2 * 1024 * 1024)

    wo = lib._nvs.lib_cfg().lib_desc.version.write_options
    assert wo.sequencer_url == "https://seq-0.svc.cluster.local:9100,https://seq-1.svc.cluster.local:9100"
    assert wo.sequencer_conflict_policy == 4
    assert wo.sequencer_inline_threshold_bytes == 2 * 1024 * 1024

    # Invalid inputs rejected without mutating _arctic_cfg
    with pytest.raises(UnsupportedLibraryOptionValue):
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_URL, "not-a-url")
    with pytest.raises(UnsupportedLibraryOptionValue):
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_CONFLICT_POLICY, "invalid_policy")
    with pytest.raises(UnsupportedLibraryOptionValue):
        ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_INLINE_THRESHOLD_BYTES, -1)
    with pytest.raises(UnsupportedLibraryOptionValue):
        ac.modify_library_option(
            lib, ModifiableEnterpriseLibraryOption.SEQUENCER_INLINE_THRESHOLD_BYTES, 32 * 1024 * 1024
        )

    # Clearing via None or "" disables cleanly
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_URL, None)
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_CONFLICT_POLICY, "")
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_INLINE_THRESHOLD_BYTES, 0)
    wo2 = lib._nvs.lib_cfg().lib_desc.version.write_options
    assert wo2.sequencer_url == ""
    assert wo2.sequencer_conflict_policy == 0
    assert wo2.sequencer_inline_threshold_bytes == 0

    # P0-25: Unsafe URLs (userinfo, query strings, control chars, empty comma lists) rejected
    for bad_url in (
        "https://user:secret@seq.local:9100",
        "https://seq.local:9100?token=leak",
        "https://seq.local:9100\r\nX-Injected: 1",
        ",,,",
    ):
        with pytest.raises(UnsupportedLibraryOptionValue):
            ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_URL, bad_url)

    # P1-5: Library.enterprise_options() exposes all 4 sequencer fields
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_URL, "https://seq-0.local:9100")
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_CONFLICT_POLICY, "overwrite_lww")
    ac.modify_library_option(lib, ModifiableEnterpriseLibraryOption.SEQUENCER_INLINE_THRESHOLD_BYTES, 131072)
    ent_opts = lib.enterprise_options()
    assert ent_opts.sequencer_url == "https://seq-0.local:9100"
    assert ent_opts.sequencer_conflict_policy == "overwrite_lww"
    assert ent_opts.sequencer_inline_threshold_bytes == 131072

    # P1-11: Exception hierarchy for DataConflictException, SliceConflictException, QuotaExceededException
    from arcticdb.exceptions import (
        ArcticException,
        DataConflictException,
        QuotaExceededException,
        SliceConflictException,
        StorageException,
    )

    assert issubclass(DataConflictException, StorageException)
    assert issubclass(SliceConflictException, StorageException)
    assert issubclass(QuotaExceededException, StorageException)
    assert issubclass(QuotaExceededException, ArcticException)
