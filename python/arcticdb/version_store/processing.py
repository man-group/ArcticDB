"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from collections import namedtuple
import copy
from dataclasses import dataclass
import datetime
from math import inf

import numpy as np
import pandas as pd
from pandas.tseries.frequencies import to_offset

from typing import Dict, NamedTuple, Optional, Tuple, Union

from arcticdb.exceptions import ArcticDbNotYetImplemented, ArcticNativeException, UserInputException
from arcticdb.version_store._normalization import normalize_dt_range_to_ts
from arcticdb.preconditions import check
from arcticdb.supported_types import DateRangeInput, time_types as supported_time_types

from arcticdb_ext.version_store import PipelineOptimisation as _Optimisation
from arcticdb_ext.version_store import ExpressionContext as _ExpressionContext
from arcticdb_ext.version_store import FilterClause as _FilterClause
from arcticdb_ext.version_store import ProjectClause as _ProjectClause
from arcticdb_ext.version_store import GroupByClause as _GroupByClause
from arcticdb_ext.version_store import AggregationClause as _AggregationClause
from arcticdb_ext.version_store import ResampleClauseLeftClosed as _ResampleClauseLeftClosed
from arcticdb_ext.version_store import ResampleClauseRightClosed as _ResampleClauseRightClosed
from arcticdb_ext.version_store import ResampleBoundary as _ResampleBoundary
from arcticdb_ext.version_store import RowRangeClause as _RowRangeClause
from arcticdb_ext.version_store import DateRangeClause as _DateRangeClause
from arcticdb_ext.version_store import RowRangeType as _RowRangeType
from arcticdb_ext.version_store import ExpressionName as _ExpressionName
from arcticdb_ext.version_store import ColumnName as _ColumnName
from arcticdb_ext.version_store import ValueName as _ValueName
from arcticdb_ext.version_store import ValueSetName as _ValueSetName
from arcticdb_ext.version_store import Value as _Value
from arcticdb_ext.version_store import ValueSet as _ValueSet
from arcticdb_ext.version_store import (
    ValueBool,
    ValueUint8,
    ValueUint16,
    ValueUint32,
    ValueUint64,
    ValueInt8,
    ValueInt16,
    ValueInt32,
    ValueInt64,
    ValueFloat32,
    ValueFloat64,
)
from arcticdb_ext.version_store import ExpressionNode as _ExpressionNode
from arcticdb_ext.version_store import OperationType as _OperationType

COLUMN = "COLUMN"


class ExpressionNode:
    # Required so that comparisons like:
    # np.int(0) < <ExpressionNode object>
    # work as we want
    __array_priority__ = 100

    def __init__(self):
        self.left = self.right = self.operator = None
        self.name = None

    @classmethod
    def compose(cls, left, operator, right):
        output = cls()
        output.left = left
        output.operator = operator
        output.right = right
        return output

    @classmethod
    def column_ref(cls, left):
        return cls.compose(left, COLUMN, None)

    def _apply(self, right, operator):
        left = ExpressionNode.compose(self.left, self.operator, self.right)
        self = ExpressionNode()
        self.left = left
        self.operator = operator
        self.right = right
        return self

    def _rapply(self, left, operator):
        right = ExpressionNode.compose(self.left, self.operator, self.right)
        self = ExpressionNode()
        self.right = right
        self.operator = operator
        self.left = left
        return self

    def __abs__(self):
        return ExpressionNode.compose(self, _OperationType.ABS, None)

    def __neg__(self):
        return ExpressionNode.compose(self, _OperationType.NEG, None)

    def __invert__(self):
        return ExpressionNode.compose(self, _OperationType.NOT, None)

    def __add__(self, right):
        return self._apply(right, _OperationType.ADD)

    def __sub__(self, right):
        return self._apply(right, _OperationType.SUB)

    def __mul__(self, right):
        return self._apply(right, _OperationType.MUL)

    def __truediv__(self, right):
        return self._apply(right, _OperationType.DIV)

    def __eq__(self, right):
        if is_supported_sequence(right):
            return self.isin(right)
        else:
            return self._apply(right, _OperationType.EQ)

    def __ne__(self, right):
        if is_supported_sequence(right):
            return self.isnotin(right)
        else:
            return self._apply(right, _OperationType.NE)

    def __lt__(self, right):
        return self._apply(right, _OperationType.LT)

    def __le__(self, right):
        return self._apply(right, _OperationType.LE)

    def __gt__(self, right):
        return self._apply(right, _OperationType.GT)

    def __ge__(self, right):
        return self._apply(right, _OperationType.GE)

    def __and__(self, right):
        if right is True:
            return self
        elif right is False:
            return False
        else:
            return self._apply(right, _OperationType.AND)

    def __or__(self, right):
        if right is True:
            return True
        elif right is False:
            return self
        else:
            return self._apply(right, _OperationType.OR)

    def __xor__(self, right):
        if right is True:
            return ~self
        elif right is False:
            return self
        else:
            return self._apply(right, _OperationType.XOR)

    def __radd__(self, left):
        return self._rapply(left, _OperationType.ADD)

    def __rsub__(self, left):
        return self._rapply(left, _OperationType.SUB)

    def __rmul__(self, left):
        return self._rapply(left, _OperationType.MUL)

    def __rtruediv__(self, left):
        return self._rapply(left, _OperationType.DIV)

    def __rand__(self, left):
        if left is True:
            return self
        elif left is False:
            return False
        else:
            return self._rapply(left, _OperationType.AND)

    def __ror__(self, left):
        if left is True:
            return True
        elif left is False:
            return self
        else:
            return self._rapply(left, _OperationType.OR)

    def __rxor__(self, left):
        if left is True:
            return ~self
        elif left is False:
            return self
        else:
            return self._rapply(left, _OperationType.XOR)

    def __bool__(self):
        raise UserInputException(
            "'and', 'or', and 'not' operators not supported in ArcticDB querying operations,"
            " please use the bitwise equivalents '&', '|', and '~' respectively"
        )

    def isin(self, *args):
        value_list = value_list_from_args(*args)
        return self._apply(value_list, _OperationType.ISIN)

    def isnotin(self, *args):
        value_list = value_list_from_args(*args)
        return self._apply(value_list, _OperationType.ISNOTIN)

    def isna(self):
        return self.isnull()

    def isnull(self):
        return ExpressionNode.compose(self, _OperationType.ISNULL, None)

    def notna(self):
        return self.notnull()

    def notnull(self):
        return ExpressionNode.compose(self, _OperationType.NOTNULL, None)

    def __str__(self):
        return self.get_name()

    def get_name(self):
        if not self.name:
            if self.operator == COLUMN:
                self.name = 'Column["{}"]'.format(self.left)
            elif self.operator in [_OperationType.ABS, _OperationType.NEG, _OperationType.NOT]:
                self.name = "{}({})".format(self.operator.name, self.left)
            else:
                if isinstance(self.left, ExpressionNode):
                    left = str(self.left)
                else:
                    left = to_string(self.left)
                if isinstance(self.right, ExpressionNode):
                    right = str(self.right)
                else:
                    right = to_string(self.right)
                self.name = "({} {} {})".format(left, self.operator.name, right)
        return self.name


def is_supported_sequence(obj):
    return isinstance(obj, (list, set, frozenset, tuple, np.ndarray))


def nanoseconds_from_utc(time):
    # We convert every time type to a pandas Timestamp because it:
    # - is consistent with how we normalize time types when normalizing
    # - has nanosecond precision
    # - if tzinfo is set will give nanoseconds since UTC
    return int(pd.Timestamp(time).value)


def nanoseconds_timedelta(timedelta):
    return int(pd.Timedelta(timedelta).value)


def value_list_from_args(*args):
    if len(args) == 1 and is_supported_sequence(args[0]):
        collection = args[0]
    else:
        collection = args
    array_list = []
    value_set = set()
    contains_integer = False
    if len(collection) > 0:
        for value in collection:
            if value not in value_set:
                value_set.add(value)
                if isinstance(value, supported_time_types):
                    value = nanoseconds_from_utc(value)
                elem = np.array([value]) if isinstance(value, (int, np.integer)) else np.full(1, value, dtype=None)
                array_list.append(elem)
                contains_integer = contains_integer or isinstance(value, (int, np.integer))
        value_list = np.concatenate(array_list)
        if contains_integer and value_list.dtype == np.float64:
            raise UserInputException("Invalid datatype conversion to double")
        if value_list.dtype == np.float16:
            value_list = value_list.astype(np.float32)
        elif value_list.dtype.kind == "U":
            value_list = value_list.tolist()
    else:
        # Return an empty list. This will call the string ctor for ValueSet, but also set a bool flag so that numeric
        # types also behave as expected
        value_list = []
    return value_list


# These are just used for shallow/deep copying, pickling, and equality checks
PythonFilterClause = namedtuple("PythonFilterClause", ["expr"])
PythonProjectionClause = namedtuple("PythonProjectionClause", ["name", "expr"])
PythonGroupByClause = namedtuple("PythonGroupByClause", ["name"])
PythonAggregationClause = namedtuple("PythonAggregationClause", ["aggregations"])
PythonDateRangeClause = namedtuple("PythonDateRangeClause", ["start", "end"])


class PythonRowRangeClause(NamedTuple):
    row_range_type: _RowRangeType = None
    n: int = None
    start: int = None
    end: int = None

# Would be cleaner if all Python*Clause classes were dataclasses, but this is used for pickling, so hard to change now
@dataclass
class PythonResampleClause:
    rule: str
    closed: _ResampleBoundary
    label: _ResampleBoundary
    aggregations: Dict[str, Union[str, Tuple[str, str]]] = None
    # In nanosecods
    offset: int = 0
    origin: Union[str, pd.Timestamp] = "epoch"


class QueryBuilder:
    """
    Build a query to process read results with. Syntax is designed to be similar to Pandas:

        q = adb.QueryBuilder()
        q = q[q["a"] < 5] (equivalent to q = q[q.a < 5] provided the column name is also a valid Python variable name)
        dataframe = lib.read(symbol, query_builder=q).data

    For Group By and Aggregation functionality please see the documentation for the `groupby`. For projection
    functionality, see the documentation for the `apply` method.

    Supported arithmetic operations when projection or filtering:

    * Binary arithmetic: +, -, *, /
    * Unary arithmetic: -, abs

    Supported filtering operations:

    * isna, isnull, notna, and notnull - return all rows where a specified column is/is not NaN or None. isna is
    equivalent to isnull, and notna is equivalent to notnull, i.e. no distinction is made between NaN and None values
    in column types that support both (e.g. strings). For example:
        ```
        q = q[q["col"].isna()]
        ```
    
    * Binary comparisons: <, <=, >, >=, ==, !=
    * Unary NOT: ~
    * Binary combinators: &, |, ^
    * List membership: isin, isnotin (also accessible with == and !=)

    isin/isnotin accept lists, sets, frozensets, 1D ndarrays, or *args unpacking. For example:

        l = [1, 2, 3]
        q.isin(l)

    is equivalent to...

        q.isin(1, 2, 3)

    Boolean columns can be filtered on directly:

        q = adb.QueryBuilder()
        q = q[q["boolean_column"]]

    and combined with other operations intuitively:

        q = adb.QueryBuilder()
        q = q[(q["boolean_column_1"] & ~q["boolean_column_2"]) & (q["numeric_column"] > 0)]

    Arbitrary combinations of these expressions is possible, for example:

        q = q[(((q["a"] * q["b"]) / 5) < (0.7 * q["c"])) & (q["b"] != 12)]

    See tests/unit/arcticdb/version_store/test_filtering.py for more example uses.

    #Timestamp filtering
    pandas.Timestamp, datetime.datetime, pandas.Timedelta, and datetime.timedelta objects are supported.
    Note that internally all of these types are converted to nanoseconds (since epoch in the Timestamp/datetime
    cases). This means that nonsensical operations such as multiplying two times together are permitted (but not
    encouraged).
    #Restrictions
    String equality/inequality (and isin/isnotin) is supported for printable ASCII characters only.
    Although not prohibited, it is not recommended to use ==, !=, isin, or isnotin with floating point values.
    #Exceptions
    inf or -inf values are provided for comparison
    Column involved in query is a Categorical
    Symbol is pickled
    Column involved in query is not present in symbol
    Query involves comparing strings using <, <=, >, or >= operators
    Query involves comparing a string to one or more numeric values, or vice versa
    Query involves arithmetic with a column containing strings
    """

    def __init__(self):
        # Note that these lists will be modified using + rather than append so that operations like this work:
        # q = QueryBuilder()
        # q_copy = q
        # q = q[q["col"] == 0]
        # without modifying q_copy
        self.clauses = []
        # This is hacky, but the alternative is implementing pickle for the C++ classes of all the clauses, and the tree
        # of classes these depend on, which is A LOT
        self._python_clauses = []
        self._optimisation = _Optimisation.SPEED

    def apply(self, name, expr):
        """
        Apply enables new columns to be created using supported QueryBuilder numeric operations. See the documentation for the
        QueryBuilder class for more information on supported expressions - any expression valid in a filter is valid when using
        `apply`.

        Parameters
        ----------
        name: `str`
            Name of the column to be created
        expr:
            Expression

        Examples
        --------

        >>> df = pd.DataFrame(
            {
                "VWAP": np.arange(0, 10, dtype=np.float64),
                "ASK": np.arange(10, 20, dtype=np.uint16),
                "VOL_ACC": np.arange(20, 30, dtype=np.int32),
            },
            index=np.arange(10),
        )
        >>> lib.write("expression", df)
        >>> q = adb.QueryBuilder()
        >>> q = q.apply("ADJUSTED", q["ASK"] * q["VOL_ACC"] + 7)
        >>> lib.read("expression", query_builder=q).data
        VOL_ACC  ASK  VWAP  ADJUSTED
        0     20   10   0.0       207
        1     21   11   1.0       238
        2     22   12   2.0       271
        3     23   13   3.0       306
        4     24   14   4.0       343
        5     25   15   5.0       382
        6     26   16   6.0       423
        7     27   17   7.0       466
        8     28   18   8.0       511
        9     29   19   9.0       558

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        input_columns, expression_context = visit_expression(expr)
        self.clauses = self.clauses + [_ProjectClause(input_columns, name, expression_context)]
        self._python_clauses = self._python_clauses + [PythonProjectionClause(name, expr)]
        return self

    def groupby(self, name: str):
        """
        Group symbol by column name. GroupBy operations must be followed by an aggregation operator. Currently the following five aggregation
        operators are supported:

        * "mean" - compute the mean of the group
        * "sum" - compute the sum of the group
        * "min" - compute the min of the group
        * "max" - compute the max of the group
        * "count" - compute the count of group

        For usage examples, see below.

        Parameters
        ----------
        name: `str`
            Name of the column to group on. Note that currently GroupBy only supports single-column groupings.

        Examples
        --------
        Average (mean) over two groups:

        >>> df = pd.DataFrame(
            {
                "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"],
                "to_mean": [1.1, 1.4, 2.5, np.nan, 2.2],
            },
            index=np.arange(5),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.groupby("grouping_column").agg({"to_mean": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data
        
                       to_mean
             group_1  1.666667
             group_2       2.2

        Max over one group:

        >>> df = pd.DataFrame(
            {
                "grouping_column": ["group_1", "group_1", "group_1"],
                "to_max": [1, 5, 4],
            },
            index=np.arange(3),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.groupby("grouping_column").agg({"to_max": "max"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data
        
                     to_max
            group_1  5

        Max and Mean:

        >>> df = pd.DataFrame(
            {
                "grouping_column": ["group_1", "group_1", "group_1"],
                "to_mean": [1.1, 1.4, 2.5],
                "to_max": [1.1, 1.4, 2.5]
            },
            index=np.arange(3),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.groupby("grouping_column").agg({"to_max": "max", "to_mean": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data
        
                     to_max   to_mean
            group_1     2.5  1.666667

        Min and max over one column, mean over another:

        >>> df = pd.DataFrame(
            {
                "grouping_column": ["group_1", "group_1", "group_1", "group_2", "group_2"],
                "agg_1": [1, 2, 3, 4, 5],
                "agg_2": [1.1, 1.4, 2.5, np.nan, 2.2],
            },
            index=np.arange(5),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.groupby("grouping_column")
        >>> q = q.agg({"agg_1_min": ("agg_1", "min"), "agg_1_max": ("agg_1", "max"), "agg_2": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data

                     agg_1_min  agg_1_max     agg_2
            group_1          1          3  1.666667
            group_2          4          5       2.2

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.clauses = self.clauses + [_GroupByClause(name)]
        self._python_clauses = self._python_clauses + [PythonGroupByClause(name)]
        return self

    def agg(self, aggregations: Dict[str, Union[str, Tuple[str, str]]]):
        # Only makes sense if previous stage is a group-by or resample
        check(
            len(self.clauses) and isinstance(self.clauses[-1], (_GroupByClause, _ResampleClauseLeftClosed, _ResampleClauseRightClosed)),
            f"Aggregation only makes sense after groupby or resample",
        )
        for k, v in aggregations.items():
            check(isinstance(v, (str, tuple)), f"Values in agg dict expected to be strings or tuples, received {v} of type {type(v)}")
            if isinstance(v, str):
                aggregations[k] = v.lower()
            elif isinstance(v, tuple):
                check(
                    len(v) == 2 and (isinstance(v[0], str) and isinstance(v[1], str)),
                    f"Tuple values in agg dict expected to have 2 string elements, received {v}"
                )
                aggregations[k] = (v[0], v[1].lower())

        if isinstance(self.clauses[-1], _GroupByClause):
            self.clauses = self.clauses + [_AggregationClause(self.clauses[-1].grouping_column, aggregations)]
            self._python_clauses = self._python_clauses + [PythonAggregationClause(aggregations)]
        else:
            self.clauses[-1].set_aggregations(aggregations)
            self._python_clauses[-1].aggregations = aggregations
        return self


    def resample(
            self,
            rule: Union[str, pd.DateOffset],
            closed: Optional[str] = None,
            label: Optional[str] = None,
            offset: Optional[Union[str, pd.Timedelta]] = None,
            origin: Union[str, pd.Timestamp] = 'epoch'
    ):
        """
        Resample a symbol on the index. The symbol must be datetime indexed. Resample operations must be followed by
        an aggregation operator. Currently, the following 7 aggregation operators are supported:

        * "mean" - compute the mean of the group
        * "sum" - compute the sum of the group
        * "min" - compute the min of the group
        * "max" - compute the max of the group
        * "count" - compute the count of group
        * "first" - compute the first value in the group
        * "last" - compute the last value in the group

        Note that not all aggregators are supported with all column types:

        * Numeric columns - support all aggregators
        * Bool columns - support all aggregators
        * String columns - support count, first, and last aggregators
        * Datetime columns - support all aggregators EXCEPT sum

        Note that time-buckets which contain no index values in the symbol will NOT be included in the returned
        DataFrame. This is not the same as Pandas default behaviour.
        Resampling is currently not supported with:

        * Dynamic schema where an aggregation column is missing from one or more of the row-slices.
        * Sparse data.

        The resample results match pandas resample with `origin="epoch"`. We plan to add an 'origin' argument in
        a future release and will then change the default value to '"start_day"' to match the Pandas default. This
        will change the results in cases where the rule is not a multiple of 24 hours.

        Parameters
        ----------
        rule: Union[`str`, 'pd.DataOffset']
            The frequency at which to resample the data. Supported rule strings are ns, us, ms, s, min, h, and D, and
            multiples/combinations of these, such as 1h30min. pd.DataOffset objects representing frequencies from this
            set are also accepted.
        closed: Optional['str'], default=None
            Which boundary of each time-bucket is closed. Must be one of 'left' or 'right'. If not provided, the default
            is left for all currently supported frequencies.
        label: Optional['str'], default=None
            Which boundary of each time-bucket is used as the index value in the returned DataFrame. Must be one of
            'left' or 'right'. If not provided, the default is left for all currently supported frequencies.
        offset: Optional[Union[str, pd.Timedelta]] default=None
            Offset the start of each bucket. Supported strings are the same as in `pd.Timedelta`. If offset is larger than
            rule then `offset` modulo `rule` is used as an offset.
        origin: Optional[Union[str, pd.Timestamp]] default='start_day'
            The timestamp on which to adjust the grouping. Supported string are:

            * epoch: origin is 1970-01-01
            * start: origin is the first value of the timeseries
            * start_day: origin is the first day at midnight of the timeseries
            * end: origin is the last value of the timeseries
            * end_day: origin is the ceiling midnight of the last day
        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.

        Raises
        -------
        ArcticDbNotYetImplemented
            A frequency string or Pandas DateOffset object are provided to the rule argument outside the supported
            frequencies listed above.
        ArcticNativeException
            The closed or label arguments are not one of "left" or "right"
        SchemaException
            Raised on call to read if:

            * If the aggregation specified is not compatible with the type of the column being aggregated as
              specified above.
            * The library has dynamic schema enabled, and at least one of the columns being aggregated is missing
              from at least one row-slice.
            * At least one of the columns being aggregated contains sparse data.

        Examples
        --------
        Resample two hours worth of minutely data down to hourly data, summing the column 'to_sum':

        >>> df = pd.DataFrame(
            {
                "to_sum": np.arange(120),
            },
            index=pd.date_range("2024-01-01", freq="min", periods=120),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.resample("h").agg({"to_sum": "sum"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data

                                 to_sum
            2024-01-01 00:00:00    1770
            2024-01-01 01:00:00    5370

        As above, but specifying that the closed boundary of each time-bucket is the right hand side, and also to label
        the output by the right boundary:

        >>> q = adb.QueryBuilder()
        >>> q = q.resample("h", closed="right", label="right").agg({"to_sum": "sum"})
        >>> lib.read("symbol", query_builder=q).data
        
                                 to_sum
            2024-01-01 00:00:00       0
            2024-01-01 01:00:00    1830
            2024-01-01 02:00:00    5310

        Nones, NaNs, and NaTs are omitted from aggregations:

        >>> df = pd.DataFrame(
            {
                "to_mean": [1.0, np.nan, 2.0],
            },
            index=pd.date_range("2024-01-01", freq="min", periods=3),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.resample("h").agg({"to_mean": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data

                                 to_mean
            2024-01-01 00:00:00      1.5

        Output column names can be controlled through the format of the dict passed to agg:

        >>> df = pd.DataFrame(
            {
                "agg_1": [1, 2, 3, 4, 5],
                "agg_2": [1.0, 2.0, 3.0, np.nan, 5.0],
            },
            index=pd.date_range("2024-01-01", freq="min", periods=5),
        )
        >>> q = adb.QueryBuilder()
        >>> q = q.resample("h")
        >>> q = q.agg({"agg_1_min": ("agg_1", "min"), "agg_1_max": ("agg_1", "max"), "agg_2": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data

                                 agg_1_min  agg_1_max     agg_2
            2024-01-01 00:00:00          1          5      2.75
        """
        rule = rule.freqstr if isinstance(rule, pd.DateOffset) else rule
        # We use floor and ceiling later to round user-provided date ranges and or start/end index values of the symbol
        # before calling pandas.date_range to generate the bucket boundaries, but floor and ceiling only work with
        # well-defined intervals that are multiples of whole ns/us/ms/s/min/h/D
        try:
            pd.Timestamp(0).floor(rule)
        except ValueError:
            raise ArcticDbNotYetImplemented(f"Frequency string '{rule}' not yet supported. Valid frequency strings "
                                            f"are ns, us, ms, s, min, h, D, and multiples/combinations thereof such "
                                            f"as 1h30min")
        if offset:
            try:
                offset_ns = to_offset(offset).nanos
            except ValueError:
                raise UserInputException(f'Argument offset must be pd.Timedelta or pd.Timedelta covertible string. Got "{offset}" instead.')
        else:
            offset_ns = 0

        if not (type(origin) is pd.Timestamp or origin in ["start", "end", "start_day", "end_day", "epoch"]):
            raise UserInputException(f'Argument origin must be either of type pd.Timestamp or one of ["start", "end", "start_day", "end_day", "epoch"]. Got {offset} instead')
        if type(origin) is pd.Timestamp:
            origin = origin.value
        # This set is documented here:
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.resample.html#pandas.Series.resample
        # and lifted directly from pandas.core.resample.TimeGrouper.__init__, and so is inherently fragile to upstream
        # changes
        end_types = {"M", "A", "Q", "BM", "BA", "BQ", "W"}
        boundary_map = {
            "left": _ResampleBoundary.LEFT,
            "right": _ResampleBoundary.RIGHT,
            None: _ResampleBoundary.RIGHT if rule in end_types or origin in ["end", "end_day"] else _ResampleBoundary.LEFT
        }
        check(closed in boundary_map.keys(), f"closed kwarg to resample must be `left`, 'right', or None, but received '{closed}'")
        check(label in boundary_map.keys(), f"label kwarg to resample must be `left`, 'right', or None, but received '{closed}'")
        if boundary_map[closed] == _ResampleBoundary.LEFT:
            self.clauses = self.clauses + [_ResampleClauseLeftClosed(rule, boundary_map[label], offset_ns, origin)]
        else:
            self.clauses = self.clauses + [_ResampleClauseRightClosed(rule, boundary_map[label], offset_ns, origin)]
        self._python_clauses = self._python_clauses + [PythonResampleClause(rule=rule, closed=boundary_map[closed], label=boundary_map[label], offset=offset_ns, origin=origin)]
        return self


    # TODO: specify type of other must be QueryBuilder with from __future__ import annotations once only Python 3.7+
    # supported
    def then(self, other):
        """
        Applies processing specified in other after any processing already defined for this QueryBuilder.

        Parameters
        ----------
        other: `QueryBuilder`
            QueryBuilder to apply after this one in the processing pipeline.

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.clauses = self.clauses + other.clauses
        self._python_clauses = self._python_clauses + other._python_clauses
        return self

    # TODO: specify type of other must be QueryBuilder with from __future__ import annotations once only Python 3.7+
    # supported
    def prepend(self, other):
        """
        Applies processing specified in other before any processing already defined for this QueryBuilder.

        Parameters
        ----------
        other: `QueryBuilder`
            QueryBuilder to apply before this one in the processing pipeline.

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.clauses = other.clauses + self.clauses
        self._python_clauses = other._python_clauses + self._python_clauses
        return self

    def head(self, n: int = 5):
        """
        Filter out all but the first n rows of data. If n is negative, return all rows except the last n rows.

        Parameters
        ----------
        n : int, default=5
            Number of rows to select if non-negative, otherwise number of rows to exclude.

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.clauses = self.clauses + [_RowRangeClause(_RowRangeType.HEAD, n)]
        self._python_clauses = self._python_clauses + [PythonRowRangeClause(row_range_type=_RowRangeType.HEAD, n=n)]
        return self

    def tail(self, n: int = 5):
        """
        Filter out all but the last n rows of data. If n is negative, return all rows except the first n rows.

        Parameters
        ----------
        n : int, default=5
            Number of rows to select if non-negative, otherwise number of rows to exclude.

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.clauses = self.clauses + [_RowRangeClause(_RowRangeType.TAIL, n)]
        self._python_clauses = self._python_clauses + [PythonRowRangeClause(row_range_type=_RowRangeType.TAIL, n=n)]
        return self

    def row_range(self, row_range: Tuple[int, int]):
        """
        Row range to read data for. Inclusive of the lower bound, exclusive of the upper bound.
        Should behave the same as df.iloc[start:end], including in the handling of negative start/end values.

        Parameters
        ----------
        row_range : Tuple[int, int]
            Row range to read data for. Inclusive of the lower bound, exclusive of the upper bound.

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        start = row_range[0]
        end = row_range[1]

        self.clauses = self.clauses + [_RowRangeClause(start, end)]
        self._python_clauses = self._python_clauses + [PythonRowRangeClause(start=start, end=end)]
        return self

    def date_range(self, date_range: DateRangeInput):
        """
        DateRange to read data for.  Applicable only for Pandas data with a DateTime index. Returns only the part
        of the data that falls within the given range. If this is the only processing clause being applied, then the
        returned data object will use less memory than passing date_range directly as an argument to the read method, at
         the cost of possibly being slightly slower.

        Parameters
        ----------
        date_range: `DateRangeInput`
            A date range in the same format as accepted by the read method.

        Examples
        --------
        >>> q = adb.QueryBuilder()
        >>> q = q.date_range((pd.Timestamp("2000-01-01"), pd.Timestamp("2001-01-01")))

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        start, end = normalize_dt_range_to_ts(date_range)
        self.clauses = self.clauses + [_DateRangeClause(start.value, end.value)]
        self._python_clauses = self._python_clauses + [PythonDateRangeClause(start.value, end.value)]
        return self

    def __eq__(self, right):
        if not isinstance(right, QueryBuilder):
            return False
        return self._optimisation == right._optimisation and str(self) == str(right)

    def __str__(self):
        return " | ".join(str(clause) for clause in self.clauses)

    def __getitem__(self, item):
        if isinstance(item, str):
            return ExpressionNode.column_ref(item)
        else:
            # This handles the case where the filtering is on a single boolean column
            # e.g. q = q[q["col"]]
            if isinstance(item, ExpressionNode) and item.operator == COLUMN:
                item = ExpressionNode.compose(item, _OperationType.IDENTITY, None)
            input_columns, expression_context = visit_expression(item)
            self_copy = copy.deepcopy(self)
            self_copy.clauses = self.clauses + [_FilterClause(input_columns, expression_context, self_copy._optimisation)]
            self_copy._python_clauses = self_copy._python_clauses + [PythonFilterClause(item)]
            return self_copy

    def __setitem__(self, key, item):
        return self.apply(key, item)

    def __getattr__(self, key):
        return self[key]

    def __getstate__(self):
        rv = vars(self).copy()
        del rv["clauses"]
        return rv

    def __setstate__(self, state):
        vars(self).update(state)
        self.clauses = []
        for python_clause in self._python_clauses:
            if isinstance(python_clause, PythonFilterClause):
                input_columns, expression_context = visit_expression(python_clause.expr)
                self.clauses = self.clauses + [_FilterClause(input_columns, expression_context, self._optimisation)]
            elif isinstance(python_clause, PythonProjectionClause):
                input_columns, expression_context = visit_expression(python_clause.expr)
                self.clauses = self.clauses + [_ProjectClause(input_columns, python_clause.name, expression_context)]
            elif isinstance(python_clause, PythonGroupByClause):
                self.clauses = self.clauses + [_GroupByClause(python_clause.name)]
            elif isinstance(python_clause, PythonAggregationClause):
                self.clauses = self.clauses + [_AggregationClause(self.clauses[-1].grouping_column, python_clause.aggregations)]
            elif isinstance(python_clause, PythonResampleClause):
                if python_clause.closed == _ResampleBoundary.LEFT:
                    self.clauses = self.clauses + [_ResampleClauseLeftClosed(python_clause.rule, python_clause.label, python_clause.offset, python_clause.origin)]
                else:
                    self.clauses = self.clauses + [_ResampleClauseRightClosed(python_clause.rule, python_clause.label, python_clause.offset, python_clause.origin)]
                if python_clause.aggregations is not None:
                    self.clauses[-1].set_aggregations(python_clause.aggregations)
            elif isinstance(python_clause, PythonRowRangeClause):
                if python_clause.start is not None and python_clause.end is not None:
                    self.clauses = self.clauses + [_RowRangeClause(python_clause.start, python_clause.end)]
                else:
                    self.clauses = self.clauses + [_RowRangeClause(python_clause.row_range_type, python_clause.n)]
            elif isinstance(python_clause, PythonDateRangeClause):
                self.clauses = self.clauses + [_DateRangeClause(python_clause.start, python_clause.end)]
            else:
                raise ArcticNativeException(
                    f"Unrecognised clause type {type(python_clause)} when unpickling QueryBuilder"
                )

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__setstate__(self.__getstate__())
        return result

    # Might want to apply different optimisations to different clauses once projections/group-bys are implemented
    def optimise_for_speed(self):
        """Process query as fast as possible (the default behaviour)"""
        self._optimisation = _Optimisation.SPEED
        for clause in self.clauses:
            if hasattr(clause, "set_pipeline_optimisation"):
                clause.set_pipeline_optimisation(_Optimisation.SPEED)

    def optimise_for_memory(self):
        """Reduce peak memory usage during the query, at the expense of some performance.

        Optimisations applied:

        * Memory used by strings that are present in segments read from storage, but are not required in the final dataframe that will be presented back to the user, is reclaimed earlier in the processing pipeline.
        """
        self._optimisation = _Optimisation.MEMORY
        for clause in self.clauses:
            if hasattr(clause, "set_pipeline_optimisation"):
                clause.set_pipeline_optimisation(_Optimisation.MEMORY)


CONSTRUCTOR_MAP = {
    "u": {1: ValueUint8, 2: ValueUint16, 4: ValueUint32, 8: ValueUint64},
    "i": {1: ValueInt8, 2: ValueInt16, 4: ValueInt32, 8: ValueInt64},
    "f": {1: ValueFloat32, 2: ValueFloat32, 4: ValueFloat32, 8: ValueFloat64},
}


def create_value(value):
    if value in [inf, -inf]:
        raise ArcticNativeException("Infinite values not supported in queries")

    if isinstance(value, np.floating):
        f = CONSTRUCTOR_MAP.get(value.dtype.kind).get(value.dtype.itemsize)
    elif isinstance(value, np.integer):
        min_scalar_type = np.min_scalar_type(value)
        f = CONSTRUCTOR_MAP.get(min_scalar_type.kind).get(min_scalar_type.itemsize)
    elif isinstance(value, supported_time_types):
        value = nanoseconds_from_utc(value)
        f = ValueInt64
    elif isinstance(value, (datetime.timedelta, pd.Timedelta)):
        value = nanoseconds_timedelta(value)
        f = ValueInt64
    elif isinstance(value, bool):
        f = ValueBool
    else:
        f = _Value
    return f(value)


def to_string(leaf):
    if isinstance(leaf, (np.ndarray, list)):
        # Truncate value set keys to first 100 characters
        key = str(leaf)[:100]
    else:
        if isinstance(leaf, str):
            key = "Str({})".format(leaf)
        elif isinstance(leaf, bool):
            key = "Bool({})".format(leaf)
        else:
            key = "Num({})".format(leaf)
    return key


def visit_expression(expr):
    def _visit(node):
        def _visit_child(node):
            def _handle_leaf(node):
                key = to_string(node)
                if isinstance(node, (np.ndarray, list)):
                    # There is a possibility that two distinct value sets have the same repr, eiter if the first 100
                    # chars match, or if the repr is truncated like '[   0    1    2 ... 9997 9998 9999]'
                    # Append -vX to handle this case, while keeping ValueSet keys short and readable in most cases
                    if key not in valueset_keys:
                        valueset_keys[key] = 0
                    else:
                        valueset_keys[key] += 1
                    key = key + "-v" + str(valueset_keys[key])
                    expression_context.add_value_set(key, _ValueSet(node))
                    return _ValueSetName(key)
                else:
                    expression_context.add_value(key, create_value(node))
                    return _ValueName(key)

            if isinstance(node, ExpressionNode):
                if node.operator == COLUMN:
                    input_columns.add(node.left)
                    return _ColumnName(node.left)
                else:
                    _visit(node)
                    return _ExpressionName(node.get_name())
            else:
                return _handle_leaf(node)

        if isinstance(node, bool):
            raise ArcticNativeException("Query is trivially {}".format(node))

        left = _visit_child(node.left)
        if node.right is not None:
            right = _visit_child(node.right)
            expression_node = _ExpressionNode(left, right, node.operator)
        else:
            expression_node = _ExpressionNode(left, node.operator)
        expression_context.add_expression_node(node.get_name(), expression_node)

    expression_context = _ExpressionContext()
    input_columns = set()
    valueset_keys = dict()
    _visit(expr)
    expression_context.root_node_name = _ExpressionName(expr.get_name())
    return input_columns, expression_context
