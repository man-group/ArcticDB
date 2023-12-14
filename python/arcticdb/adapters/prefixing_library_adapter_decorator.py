"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import inspect
import re
from typing import Optional, Tuple, Type, Callable, TYPE_CHECKING, Iterable, List

from arcticdb import LibraryOptions, Arctic
from arcticdb.adapters.arctic_library_adapter import ArcticLibraryAdapter
from arcticdb.encoding_version import EncodingVersion


class PrefixingLibraryAdapterDecorator(ArcticLibraryAdapter if TYPE_CHECKING else object):
    # Can't inherit ArcticLibraryAdapter at run-time as it would interfere with __getattr__
    URI_SCHEME = "prefix:"
    _URI_REGEX = re.compile(URI_SCHEME + r"([^?]+)\?(.+)")

    @staticmethod
    def ensure_registered():
        if PrefixingLibraryAdapterDecorator not in Arctic._LIBRARY_ADAPTERS:
            Arctic._LIBRARY_ADAPTERS.append(PrefixingLibraryAdapterDecorator)

    @staticmethod
    def prefix_uri(prefix: str, inner_uri: str) -> str:
        assert "?" not in prefix, "prefix cannot contain '?', but got: " + prefix
        return f"{PrefixingLibraryAdapterDecorator.URI_SCHEME}{prefix}?{inner_uri}"

    @staticmethod
    def _parse(uri: str) -> Optional[Tuple[str, Type[ArcticLibraryAdapter], str]]:
        match = PrefixingLibraryAdapterDecorator._URI_REGEX.match(uri)
        if match:
            prefix, uri = match.groups()
            for adapter_cls in Arctic._LIBRARY_ADAPTERS:
                if adapter_cls.supports_uri(uri):
                    return prefix, adapter_cls, uri
        return None

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return bool(PrefixingLibraryAdapterDecorator._parse(uri))

    def __init__(self, uri: str, encoding_version: EncodingVersion):
        parsed = PrefixingLibraryAdapterDecorator._parse(uri)
        assert parsed, f"Invalid uri for {type(self).__name__}: {uri}"
        self._prefix = parsed[0]
        self._inner = parsed[1](parsed[2], encoding_version)

    def get_name_for_library_manager(self, user_facing_name: str) -> str:
        return self._prefix + user_facing_name

    def library_manager_names_to_user_facing(self, names: Iterable[str]) -> List[str]:
        return [name[len(self._prefix) :] for name in names if name.startswith(self._prefix)]

    def get_library_config(self, name: str, library_options: LibraryOptions):
        lib_mgr_name = self.get_name_for_library_manager(name)
        return self._inner.get_library_config(lib_mgr_name, library_options)

    def cleanup_library(self, library_name: str):
        lib_mgr_name = self.get_name_for_library_manager(library_name)
        return self._inner.cleanup_library(lib_mgr_name)

    def __repr__(self):
        return repr(self._inner)

    def __getattr__(self, name: str):
        """Pass-through all other inner attrs, except method(s) with a "name" parameter"""
        inner_attr = getattr(self._inner, name)
        marker = "PrefixingLibraryAdapterDecoratorChecked"
        if not name.startswith("__") and isinstance(inner_attr, Callable) and not getattr(inner_attr, marker, False):
            sig = inspect.signature(inner_attr)
            for param in sig.parameters:
                if "name" in param:
                    raise NotImplementedError(f"{type(self).__name__} need to be updated to wrap {inner_attr}")
            try:
                setattr(inner_attr, marker, True)
            except:
                pass
        return inner_attr
