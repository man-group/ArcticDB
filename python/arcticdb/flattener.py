"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import collections
import hashlib
import msgpack

from arcticdb import _msgpack_compat
from arcticdb.log import version as log
from arcticdb.version_store._custom_normalizers import get_custom_normalizer
from arcticdb.version_store._normalization import MsgPackNormalizer, CompositeNormalizer


class Flattener:
    # Probably a bad idea given the dict key could have this, fine for now as write does not allow this symbol anyways.
    SEPARATOR = "__"
    MAX_KEY_LENGTH = 100

    def __init__(self):
        self.custom_normalizer = get_custom_normalizer(False)

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

    def derive_iterables(self, obj):
        # TODO: maybe move out the normalizer related bits.
        normalizer_used = None
        normalization_metadata = None
        if self.is_normalizable_to_nested_structure(obj):
            normalizer = self.get_normalizer_for_item(obj)()
            derived_item, meta = normalizer.normalize(obj)
            normalizer_used = normalizer
        else:
            derived_item = obj

        if self.is_sequence_like(derived_item):
            return type(derived_item), list(enumerate(derived_item)), (normalizer_used, normalization_metadata)
        elif self.is_dict_like(derived_item):
            return type(derived_item), list(derived_item.items()), (normalizer_used, normalization_metadata)
        else:  # leaf node
            return (
                type(derived_item) if derived_item is not None else None,
                None,
                (normalizer_used, normalization_metadata),
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
                    item, custom_norm_meta = opt_custom
                    base_normalizer.normalize(item, pickle_on_failure=False)
                else:
                    base_normalizer.normalize(obj_to_write, pickle_on_failure=False)
                # Note that we are fine with msgpack serialization, but not fall back to pickle for msgpack.
            except Exception:
                log.info("{} with key {} will be pickled".format(obj_to_write, sym))
                return True

        return False

    def _create_meta_structure(self, obj, sym, to_write):
        # TODO: convert to non recursive and remove to_write from func arguments to not rely on external state.
        item_type, iterables, normalization_info = self.derive_iterables(obj)
        shortened_symbol = sym
        meta_struct = {
            "type": item_type,
            "leaf": False,
            "symbol": shortened_symbol,
            "__version__": 1,
            "sub_keys": None,
            "data": None,
            "normalization_info": normalization_info,
        }

        serialized_as_primitive = self.try_serialize_as_primitive(obj)
        if serialized_as_primitive:
            meta_struct["leaf"] = True
            meta_struct["data"] = serialized_as_primitive

            return meta_struct

        if not iterables:
            # Use the shortened name for the actual writes to avoid having obscenely large key sizes.
            to_write[self.compact_v1(sym)] = obj
            meta_struct["leaf"] = True
            return meta_struct

        meta_struct["sub_keys"] = []
        for k, v in iterables:
            # Note: It's fine to not worry about the separator given we just use it to form some sort of vaguely
            # readable name in the end when the leaf node is retrieved.
            key_till_now = "{}{}{}".format(sym, self.SEPARATOR, str(k))
            meta_struct["sub_keys"].append(self._create_meta_structure(v, key_till_now, to_write))

        return meta_struct

    def create_meta_structure(self, obj, sym):
        to_write = dict()
        meta_struct = self._create_meta_structure(obj, sym, to_write)

        return meta_struct, to_write

    def is_named_tuple_class(self, class_obj):
        return tuple in getattr(class_obj, "__bases__", [])

    def _create_original_obj_from_metastruct_new_v1(self, meta_struct, key_map):
        # TODO: convert to non recursive.
        if meta_struct["leaf"]:
            if meta_struct["data"]:
                return self.deserialize_primitives(meta_struct["data"])
            else:
                return key_map[self.compact_v1(meta_struct["symbol"])]

        type_of_key = meta_struct["type"]
        child_keys = meta_struct["sub_keys"]
        normalizer_used, meta = meta_struct["normalization_info"]
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
                base_struct[actual_key] = self.create_original_obj_from_metastruct_new(key, key_map)
        elif isinstance(base_struct, collections.abc.Sequence):
            args = [self.create_original_obj_from_metastruct_new(key, key_map) for key in child_keys]
            if is_tuple:
                base_struct = type_of_key(*args)
            else:
                base_struct = type_of_key(args)
        # Once the entire substructure has been constructed back, check if had a normalization step and denormalize it.
        if normalizer_used:
            return normalizer_used.denormalize(base_struct, meta)

        return base_struct

    def create_original_obj_from_metastruct_new(self, meta_struct, key_map):
        if meta_struct["__version__"] and meta_struct["__version__"] == 1:
            return self._create_original_obj_from_metastruct_new_v1(meta_struct, key_map)
        else:
            log.error("Could not identify version of nested normalizer used, Got:", meta_struct)
