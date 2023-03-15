"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import importlib
import operator
from abc import abstractmethod, ABCMeta

from arcticdb.log import version as log
from typing import Any, Optional, Tuple, Type


class CustomNormalizer(object):
    __metaclass__ = ABCMeta

    # N.B. This should be stateless
    @abstractmethod
    def normalize(self, item, **kwargs):
        # type: (Any)->Optional[Tuple[Union[TimeFrame, DataFrame, Series], CustomNormalizerMeta]]
        pass

    @abstractmethod
    def denormalize(self, item, norm_meta):
        # type: (Any, CustomNormalizerMeta)->Any
        pass


def _fq_class_name(t):
    # type: (Type)->AnyStr
    return "{}.{}".format(t.__module__, t.__name__)


def _fq_class_name_to_type(fqn):
    dpos = fqn.rfind(".")
    if dpos == -1:
        return globals()[fqn]
    else:
        mod_name = fqn[:dpos]
        class_name = fqn[dpos + 1 :]
        mod = importlib.import_module(mod_name)
        return getattr(mod, class_name)


class CustomNormalizerRegistry(object):
    def __init__(self):
        self._normalizer_types = {}

    def register(self, norm, priority=10.0):
        # type: (Any, float)->bool
        norm_class = norm.__class__
        log.debug("Registering type {}".format(_fq_class_name(norm_class)))
        if norm_class in self._normalizer_types:
            return False

        self._normalizer_types[norm_class] = priority

    def normalizers(self):
        # type: ()->Iterable[Type]
        types_and_priorities = sorted(self._normalizer_types.items(), key=operator.itemgetter(1))
        return [tp[0] for tp in types_and_priorities]

    def clear(self):
        self._normalizer_types.clear()


_registry = CustomNormalizerRegistry()
register_normalizer = _registry.register
registered_normalizers = _registry.normalizers
clear_registered_normalizers = _registry.clear


class CompositeCustomNormalizer(CustomNormalizer):
    def __init__(self, types, fail_on_missing_type):
        self._normalizers = [t() for t in types]
        self._normalizer_by_typename = {_fq_class_name(n.__class__): n for n in self._normalizers}
        self._fail_on_missing_type = fail_on_missing_type

    def get_normalizer_for_item(self, item):
        for n in self._normalizers:
            if n.normalize(item):
                return n.__class__

        return None

    def normalize(self, item, **kwargs):
        for n in self._normalizers:
            opt = n.normalize(item, **kwargs)
            if opt is not None:
                item, custom_meta = opt
                custom_meta.class_name = _fq_class_name(n.__class__)
                return item, custom_meta

    def denormalize(self, item, norm_meta):
        n = self._normalizer_by_typename.get(norm_meta.class_name)
        if n is None:
            if not self._fail_on_missing_type:
                return item

            raise Exception(
                "Could not find normalizer for type {} and fail on missing type was set".format(norm_meta.class_name)
            )

        return n.denormalize(item, norm_meta)

    def __setstate__(self, state):
        self._fail_on_missing_type = state["fail_on_missing"]
        for cls in state["class_names"]:
            try:
                normalizer = _fq_class_name_to_type(cls)()
                self._normalizer_by_typename[cls] = normalizer
                self._normalizers.append(normalizer)
            except ImportError:
                if not self._fail_on_missing_type:
                    continue

    def __getstate__(self):
        return {"class_names": list(self._normalizer_by_typename.keys()), "fail_on_missing": self._fail_on_missing_type}


def get_custom_normalizer(fail_on_missing):
    return CompositeCustomNormalizer(registered_normalizers(), fail_on_missing)
