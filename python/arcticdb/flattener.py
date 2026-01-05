"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import collections
import hashlib
import msgpack
import sys

from arcticdb.exceptions import DataTooNestedException, UnsupportedKeyInDictionary

try:
    from msgpack.fallback import DEFAULT_RECURSE_LIMIT
except ImportError:
    # The default as of msgpack 1.1.0 - handle the import error in case the msgpack wheel stops exporting this constant.
    # Want to keep compatibility with a wide range of msgpack versions.
    DEFAULT_RECURSE_LIMIT = 511

from arcticdb import _msgpack_compat
from arcticdb.log import version as log
from arcticdb.version_store._custom_normalizers import get_custom_normalizer
from arcticdb.version_store._normalization import MsgPackNormalizer, CompositeNormalizer
from arcticdb.preconditions import check
from arcticdb_ext import get_config_int


class Flattener:
    # Probably a bad idea given the dict key could have this, fine for now as write does not allow this symbol anyways.
    SEPARATOR = "__"
    MAX_KEY_LENGTH = 100

    def __init__(self):
        self.custom_normalizer = get_custom_normalizer(False)
        self.meta_structure_v2 = get_config_int("VersionStore.RecursiveNormalizerMetastructure") == 2
        self.meta_structure_v1_deprecation_warning = (
            get_config_int("VersionStore.RecursiveNormalizerMetastructureV1DeprecationWarning") != 0
        )

    @staticmethod
    def is_named_tuple(obj):
        # Sadly it's non trivial to check if an object is of type named tuple: https://stackoverflow.com/a/2166841
        t = type(obj)
        b = t.__bases__
        if len(b) != 1 or b[0] != tuple:
            return False
        f = getattr(t, "_fields", None)
        if not isinstance(f, tuple):
            return False
        return all(type(n) == str for n in f)

    def compact_v1(self, symbol):
        hash_length = 12
        if len(symbol) < self.MAX_KEY_LENGTH:
            return symbol

        try:
            convert = symbol.encode("utf-8")
        except AttributeError:
            convert = symbol

        tokens = symbol.split(self.SEPARATOR)
        vaguely_readable_name = "_".join([token[-3:] for token in tokens])[: (self.MAX_KEY_LENGTH - hash_length)]

        shortened_hash = str(int(hashlib.sha256(convert).hexdigest(), 16) % 10**hash_length)
        return "{}_{}".format(vaguely_readable_name, shortened_hash)

    def can_flatten(self, item):
        if self.is_named_tuple(item):
            # Tricky to unwrap namedtuples with our current method. Just pickle this.
            return False
        return self.is_sequence_like(item) or self.is_dict_like(item) or self.is_normalizable_to_nested_structure(item)

    def get_normalizer_for_item(self, item):
        return self.custom_normalizer.get_normalizer_for_item(item)

    def is_normalizable_to_nested_structure(self, item):
        normalizer = self.get_normalizer_for_item(item)
        if normalizer and hasattr(normalizer, "NESTED_STRUCTURE") and normalizer.NESTED_STRUCTURE:
            return True

        return False

    @staticmethod
    def is_sequence_like(item):
        return isinstance(item, collections.abc.Sequence) and not isinstance(item, str)

    @staticmethod
    def is_dict_like(item):
        return isinstance(item, collections.abc.MutableMapping)

    def _encode_metastruct_item_type_v2(self, obj_type):
        return None if obj_type == type(None) else "class", obj_type.__module__, obj_type.__name__

    def derive_iterables(self, obj):
        # TODO: maybe move out the normalizer related bits.
        normalizer = None
        if self.is_normalizable_to_nested_structure(obj):
            normalizer_type = self.get_normalizer_for_item(obj)
            normalizer = normalizer_type()
            derived_item, _ = normalizer.normalize(obj)
        else:
            derived_item = obj

        if self.meta_structure_v2 or derived_item is not None:
            derived_item_type = type(derived_item)
        else:
            derived_item_type = None

        if self.is_sequence_like(derived_item):
            return derived_item_type, list(enumerate(derived_item)), normalizer
        elif self.is_dict_like(derived_item):
            return derived_item_type, list(derived_item.items()), normalizer
        else:  # leaf node
            return (
                derived_item_type,
                None,
                normalizer,
            )

    @staticmethod
    def try_serialize_as_primitive(obj):
        try:
            return msgpack.packb(obj, use_bin_type=True, strict_types=True)  # TODO: use msgpacknormalizer
        except TypeError:
            return None

    @staticmethod
    def deserialize_primitives(obj):
        return _msgpack_compat.unpackb(obj, raw=False)

    def will_obj_be_partially_pickled(self, obj):
        to_write = dict()
        self._create_meta_structure(obj, "dummy", to_write)
        msgpack_normalizer = MsgPackNormalizer()
        msgpack_normalizer.strict_mode = True  # To prevent msgpack from falling back to pickle silently
        base_normalizer = CompositeNormalizer(msgpack_normalizer, use_norm_failure_handler_known_types=False)
        for sym, obj_to_write in to_write.items():
            try:
                opt_custom = self.custom_normalizer.normalize(obj_to_write)
                if opt_custom is not None:
                    item, _ = opt_custom
                    base_normalizer.normalize(item, pickle_on_failure=False)
                else:
                    base_normalizer.normalize(obj_to_write, pickle_on_failure=False)
                # Note that we are fine with msgpack serialization, but not fall back to pickle for msgpack.
            except Exception:
                log.info("{} with key {} will be pickled".format(obj_to_write, sym))
                return True

        return False

    def _create_meta_structure(self, obj, sym, to_write, depth=0, original_symbol=None):
        if original_symbol is None:
            original_symbol = sym  # just used for error messages

        # Factor of 2 is because msgpack recurses with two stackframes for each level of nesting
        if depth > DEFAULT_RECURSE_LIMIT // 2:
            raise DataTooNestedException(
                f"Symbol {original_symbol} cannot be recursively normalized as it contains more than "
                f"{DEFAULT_RECURSE_LIMIT // 2} levels of nested dictionaries. This is a limitation of the msgpack serializer."
            )

        # Commit 450170d94 shows a non-recursive implementation of this function, but since `msgpack.packb` of the
        # result is itself recursive, there is little point to rewriting this function.
        item_type, iterables, normalizer = self.derive_iterables(obj)
        shortened_symbol = sym

        if self.meta_structure_v2:
            type_format, type_module, type_name = self._encode_metastruct_item_type_v2(item_type)
            norm_format, norm_module, norm_name = self._encode_metastruct_item_type_v2(type(normalizer))

            meta_struct = {
                "type_format": type_format,
                "type_module": type_module,
                "type_name": type_name,
                "leaf": False,
                "symbol": shortened_symbol,
                "__VER__": (
                    2
                ),  # Purposefully to be different from V1 so exception KeyError is raised if old releases try to read V2 metastructures
                "sub_keys": None,
                "data": None,
                "normalizer_format": norm_format,
                "normalizer_module": norm_module,
                "normalizer_name": norm_name,
            }
        else:
            meta_struct = {
                "type": item_type,
                "leaf": False,
                "symbol": shortened_symbol,
                "__version__": 1,
                "sub_keys": None,
                "data": None,
                "normalization_info": (normalizer, None),
            }

        serialized_as_primitive = self.try_serialize_as_primitive(obj)
        if serialized_as_primitive:
            meta_struct["leaf"] = True
            meta_struct["data"] = serialized_as_primitive
        elif not iterables:
            # Use the shortened name for the actual writes to avoid having obscenely large key sizes.
            key_name = self.compact_v1(sym)
            to_write[key_name] = obj
            meta_struct["leaf"] = True

            # We currently rely on scrambling (with compact_v1) the symbol name at write time to generate stream IDs for the
            # data being written under leaf nodes. We also use compact_v1 at read time to look up these stream IDs based on the "symbol"
            # in the metastruct. This makes it impossible to change compact_v1 in a backwards and forwards compatible way.
            # To give us a way to improve this in future, for example when recursive normalizers are added to the Library API,
            # we more recently started recording the key_name explicitly in the metastruct. If using the key_name, you must
            # bear in mind that old metastructs do not have it.
            meta_struct["key_name"] = key_name
        else:
            meta_struct["sub_keys"] = []
            for k, v in iterables:
                # Note: It's fine to not worry about the separator given we just use it to form some sort of vaguely
                # readable name in the end when the leaf node is retrieved.
                str_k = str(k)
                if issubclass(item_type, collections.abc.MutableMapping) and self.SEPARATOR in str_k:
                    raise UnsupportedKeyInDictionary(
                        f"Dictionary keys used with recursive normalizers cannot contain [{self.SEPARATOR}]. "
                        f"Encountered key {k} while writing symbol {original_symbol}"
                    )
                key_till_now = "{}{}{}".format(sym, self.SEPARATOR, str_k)
                meta_struct["sub_keys"].append(
                    self._create_meta_structure(
                        v, key_till_now, to_write, depth=depth + 1, original_symbol=original_symbol
                    )
                )

        return meta_struct

    def create_meta_structure(self, obj, sym):
        to_write = dict()
        meta_struct = self._create_meta_structure(obj, sym, to_write)

        return meta_struct, to_write

    def is_named_tuple_class(self, class_obj):
        return tuple in getattr(class_obj, "__bases__", [])

    def _get_class_type_from_flattened(self, type_format, type_module, type_name):
        if type_format is None:
            return None
        check(type_format == "class", f"Unexpected serialized key type format {type_format} in meta structure")
        try:
            module = sys.modules[type_module]
            return getattr(module, type_name)
        except Exception as e:
            raise RuntimeError(f"{type_module}::{type_name} defined in meta structure cannot be loaded: {e}")

    def _deserialize_leaf_node(self, meta_struct, key_map):
        if meta_struct["data"]:
            return self.deserialize_primitives(meta_struct["data"])
        else:
            return key_map[self.compact_v1(meta_struct["symbol"])]

    def _reconstruct_collection(self, type_of_key, child_keys, key_map):
        is_tuple = False
        if self.is_named_tuple_class(type_of_key):
            base_struct = type_of_key(*(type_of_key._fields))
            is_tuple = True
        else:
            base_struct = type_of_key()

        if isinstance(base_struct, collections.abc.MutableMapping):
            for key in child_keys:
                split_on = key["symbol"]
                actual_key = split_on.split(self.SEPARATOR)[-1]
                base_struct[actual_key] = self.create_original_obj_from_metastruct(key, key_map)
        elif isinstance(base_struct, collections.abc.Sequence):
            args = [self.create_original_obj_from_metastruct(key, key_map) for key in child_keys]
            if is_tuple:
                base_struct = type_of_key(*args)
            else:
                base_struct = type_of_key(args)

        return base_struct

    def _create_original_obj_from_metastruct_v1(self, meta_struct, key_map):
        if meta_struct["leaf"]:
            return self._deserialize_leaf_node(meta_struct, key_map)

        # In V1, type is simply the pickled object
        type_of_key = meta_struct["type"]
        child_keys = meta_struct["sub_keys"]
        # meta should always be None; Still loaded here for backward compatibility
        normalizer, meta = meta_struct["normalization_info"]

        base_struct = self._reconstruct_collection(type_of_key, child_keys, key_map)

        # Once the entire substructure has been constructed back, check if had a normalization step and denormalize it.
        if normalizer:
            return normalizer.denormalize(base_struct, meta)

        return base_struct

    def _create_original_obj_from_metastruct_v2(self, meta_struct, key_map):
        if meta_struct["leaf"]:
            return self._deserialize_leaf_node(meta_struct, key_map)

        type_of_key = self._get_class_type_from_flattened(
            meta_struct["type_format"], meta_struct["type_module"], meta_struct["type_name"]
        )

        child_keys = meta_struct["sub_keys"]
        normalizer_format = meta_struct["normalizer_format"]
        normalizer_module = meta_struct["normalizer_module"]
        normalizer_name = meta_struct["normalizer_name"]

        base_struct = self._reconstruct_collection(type_of_key, child_keys, key_map)

        # Once the entire substructure has been constructed back, check if had a normalization step and denormalize it.
        normalizer_class = self._get_class_type_from_flattened(normalizer_format, normalizer_module, normalizer_name)
        if normalizer_class is not None:
            normalizer = normalizer_class()
            return normalizer.denormalize(base_struct, None)

        return base_struct

    def create_original_obj_from_metastruct(self, meta_struct, key_map):
        # Different meta structure before V2 (__version__ -> __VER__) as old releases only logs error for incompatible versions.
        # As old releases checks version by running meta_struct["__version__"], we can leverage the KeyError
        meta_struct_ver = meta_struct.get("__version__", meta_struct.get("__VER__", None))
        if meta_struct_ver == 1:
            return self._create_original_obj_from_metastruct_v1(meta_struct, key_map)
        elif meta_struct_ver == 2:
            return self._create_original_obj_from_metastruct_v2(meta_struct, key_map)
        else:
            raise RuntimeError(f"Could not identify version of nested normalizer used, Got: {meta_struct}")
