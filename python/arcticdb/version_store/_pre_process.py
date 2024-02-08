import pandas as pd
import re
from contextlib import contextmanager
from typing import Any, Sequence, Dict
import difflib
import itertools

from arcticdb_ext.version_store import IndexRange as _IndexRange
from arcticdb.exceptions import ArcticDbNotYetImplemented, ArcticNativeException
from arcticdb.log import version as log
from arcticdb.version_store._normalization import normalize_dt_range_to_ts
from arcticdb_link.python.arcticdb.supported_types import DateRangeInput
from arcticdb.exceptions import StreamDescriptorMismatch


_STREAM_DESCRIPTOR_SPLIT = re.compile(r"(?<=>), ")
_BATCH_BAD_ARGS: Dict[Any, Sequence[str]] = {}


def normalize_dt_range(dtr: DateRangeInput) -> _IndexRange:
    start, end = normalize_dt_range_to_ts(dtr)
    return _IndexRange(start.value, end.value)


def handle_categorical_columns(symbol, data, throw=True):
    if isinstance(data, (pd.DataFrame, pd.Series)):
        categorical_columns = []
        if isinstance(data, pd.DataFrame):
            for column_name, dtype in data.dtypes.items():
                if dtype.name == "category":
                    categorical_columns.append(column_name)
        else:
            # Series
            if data.dtype.name == "category":
                categorical_columns.append(data.name)
        if len(categorical_columns) > 0:
            message = (
                "Symbol: {}\nDataFrame/Series contains categorical data, cannot append or update\nCategorical"
                " columns: {}".format(symbol, categorical_columns)
            )
            if throw:
                raise ArcticDbNotYetImplemented(message)
            else:
                log.warn(message)


def check_batch_kwargs(batch_fun, non_batch_fun, kwargs: Dict):
    cached = _BATCH_BAD_ARGS.get(batch_fun, None)
    if not cached:
        batch_args = set(batch_fun.__code__.co_varnames[: batch_fun.__code__.co_argcount])
        none_args = set(non_batch_fun.__code__.co_varnames[: non_batch_fun.__code__.co_argcount])
        cached = _BATCH_BAD_ARGS[batch_fun] = none_args - batch_args
    union = cached & kwargs.keys()
    if union:
        log.warning("Using non-batch arguments {} with {}", union, batch_fun.__name__)


@contextmanager
def diff_long_stream_descriptor_mismatch(nvs):  # Diffing strings is easier done in Python than C++
    try:
        yield
    except StreamDescriptorMismatch as sdm:
        nvs.last_mismatch_msg = sdm.args[0]
        preamble, existing, new_val = sdm.args[0].split("\n")
        existing = _STREAM_DESCRIPTOR_SPLIT.split(existing[existing.find("=") + 1 :])
        new_val = _STREAM_DESCRIPTOR_SPLIT.split(new_val[new_val.find("=") + 1 :])
        diff = difflib.unified_diff(existing, new_val, n=0)
        new_msg_lines = (
            preamble,
            "(Showing only the mismatch. Full col list saved in the `last_mismatch_msg` attribute of the lib instance.",
            "'-' marks columns missing from the argument, '+' for unexpected.)",
            *(x for x in itertools.islice(diff, 3, None) if not x.startswith("@@")),
        )
        sdm.args = ("\n".join(new_msg_lines),)
        raise

