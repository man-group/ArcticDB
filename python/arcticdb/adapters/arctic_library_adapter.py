"""
Copyright 2026 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from abc import ABC, abstractmethod
from typing import Iterable, List

from arcticc.pb2.storage_pb2 import EnvironmentConfigsMap, LibraryConfig, LibraryDescriptor
from arcticdb.config import _DEFAULT_ENV
from arcticdb.version_store._store import NativeVersionStore
from arcticdb.options import DEFAULT_ENCODING_VERSION, LibraryOptions, EnterpriseLibraryOptions
from arcticc.pb2.storage_pb2 import LibraryConfig
from arcticdb_ext.storage import Library, StorageOverride, CONFIG_LIBRARY_NAME
from arcticdb.encoding_version import EncodingVersion


def set_library_options(
    lib_desc: "LibraryConfig", options: LibraryOptions, enterprise_library_options: EnterpriseLibraryOptions
):
    write_options = lib_desc.version.write_options

    write_options.dynamic_strings = True
    write_options.use_tombstones = True
    write_options.fast_tombstone_all = True
    lib_desc.version.symbol_list = True

    write_options.pickle_on_failure = False
    write_options.snapshot_dedup = False
    write_options.delayed_deletes = False

    write_options.dynamic_schema = options.dynamic_schema
    write_options.de_duplication = options.dedup
    write_options.segment_row_size = options.rows_per_segment
    write_options.column_group_size = options.columns_per_segment
    write_options.recursive_normalizers = options.recursive_normalizers
    write_options.prune_previous_version = options.prune_previous_versions

    lib_desc.version.encoding_version = (
        options.encoding_version if options.encoding_version is not None else DEFAULT_ENCODING_VERSION
    )

    write_options.sync_passive.enabled = enterprise_library_options.replication
    write_options.delayed_deletes = enterprise_library_options.background_deletion
    if enterprise_library_options.sequencer_url:
        raw_urls = str(enterprise_library_options.sequencer_url)
        tokens = [t.strip() for t in raw_urls.split(",") if t.strip()]
        if not tokens:
            raise ValueError("sequencer_url must contain at least one http:// or https:// endpoint")
        for tok in tokens:
            low = tok.lower()
            if not (low.startswith("http://") or low.startswith("https://")):
                raise ValueError(f"sequencer_url entries must start with http:// or https://, got: {tok!r}")
            if any(c in tok for c in ("@", "?", "#", "\\", "\r", "\n", "\x00")) or any(ord(c) < 0x20 for c in tok):
                raise ValueError("sequencer_url must not contain userinfo, query, fragment, backslash, or control chars")
        write_options.sequencer_url = ",".join(tokens)
    if enterprise_library_options.sequencer_inline_threshold_bytes is not None:
        thresh = int(enterprise_library_options.sequencer_inline_threshold_bytes)
        if thresh < 0 or thresh > 16 * 1024 * 1024:
            raise ValueError(f"sequencer_inline_threshold_bytes must be in [0, 16777216], got {thresh}")
        write_options.sequencer_inline_threshold_bytes = thresh
    if enterprise_library_options.sequencer_conflict_policy is not None and enterprise_library_options.sequencer_conflict_policy != "":
        policy_map = {
            "unspecified": 0,
            "strict_append": 1,
            "strict": 1,
            "reject": 2,
            "overwrite_lww": 3,
            "lww": 3,
            "overwrite": 3,
            "keep_both": 4,
        }
        p = enterprise_library_options.sequencer_conflict_policy
        if isinstance(p, int) and not isinstance(p, bool):
            if p < 0 or p > 4:
                raise ValueError(f"sequencer_conflict_policy integer must be in [0, 4], got {p}")
            write_options.sequencer_conflict_policy = p
        else:
            key = str(p).strip().lower()
            if key not in policy_map:
                raise ValueError(
                    f"Invalid sequencer_conflict_policy {p!r}; expected one of {sorted(policy_map.keys())}"
                )
            write_options.sequencer_conflict_policy = policy_map[key]
    if enterprise_library_options.sequencer_conflict_key_columns:
        write_options.sequencer_conflict_key_columns.extend(enterprise_library_options.sequencer_conflict_key_columns)


class ArcticLibraryAdapter(ABC):
    @abstractmethod
    def __init__(self, uri: str, encoding_version: EncodingVersion): ...

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def supports_uri(uri: str) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def config_library(self) -> Library:
        raise NotImplementedError

    def native_config(self):
        return None

    def get_sequencer_config(self):
        return None

    def get_library_config(
        self, name: str, library_options: LibraryOptions, enterprise_library_options: EnterpriseLibraryOptions
    ):
        env_cfg = EnvironmentConfigsMap()

        self.add_library_to_env(env_cfg, name)

        library_options.encoding_version = (
            library_options.encoding_version if library_options.encoding_version is not None else self._encoding_version
        )
        set_library_options(
            env_cfg.env_by_id[_DEFAULT_ENV].lib_by_path[name], library_options, enterprise_library_options
        )

        return NativeVersionStore.create_library_config(
            env_cfg, _DEFAULT_ENV, name, encoding_version=library_options.encoding_version
        )

    @abstractmethod
    def add_library_to_env(self, env_cfg: EnvironmentConfigsMap, name: str):
        raise NotImplementedError

    def get_storage_override(self) -> StorageOverride:
        return StorageOverride()

    def get_masking_override(self) -> StorageOverride:
        """Override that clears any storage config that should not be persisted."""
        return StorageOverride()

    def get_name_for_library_manager(self, user_facing_name: str) -> str:
        """Can override to translate user-supplied library names to a format more stuiable to the Storage."""
        return user_facing_name

    def library_manager_names_to_user_facing(self, names: Iterable[str]) -> List[str]:
        """The inverse of `get_name_for_library_manager`."""
        return names
