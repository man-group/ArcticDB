"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
import copy
import datetime
from math import inf

import numpy as np
import pandas as pd

from abc import ABC, abstractmethod

from arcticdb.exceptions import ArcticNativeException
from arcticdb.supported_types import time_types as supported_time_types

from arcticdb_ext.version_store import ExecutionContextOptimisation as _Optimisation
from arcticdb_ext.version_store import ExecutionContext as _ExecutionContext
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

from arcticdb_ext.version_store import ClauseBuilder as _ClauseBuilder

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

    def isin(self, *args):
        value_list = value_list_from_args(*args)
        return self._apply(value_list, _OperationType.ISIN)

    def isnotin(self, *args):
        value_list = value_list_from_args(*args)
        return self._apply(value_list, _OperationType.ISNOTIN)

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


def value_list_from_args(*args):
    if len(args) == 1 and is_supported_sequence(args[0]):
        collection = args[0]
    else:
        collection = args
    array_list = []
    value_set = set()
    if len(collection) > 0:
        for value in collection:
            if value not in value_set:
                value_set.add(value)
                if isinstance(value, supported_time_types):
                    value = int(value.timestamp() * 1_000_000_000)
                dtype = np.min_scalar_type(value) if isinstance(value, (int, np.integer)) else None
                array_list.append(np.full(1, value, dtype=dtype))
        value_list = np.concatenate(array_list)
        if value_list.dtype == np.float16:
            value_list = value_list.astype(np.float32)
        elif value_list.dtype.kind == "U":
            value_list = value_list.tolist()
    else:
        # Return an empty list. This will call the string ctor for ValueSet, but also set a bool flag so that numeric
        # types also behave as expected
        value_list = []
    return value_list


class PyClauseBase(ABC):
    @abstractmethod
    def to_cpp(self, clause_builder) -> None:
        pass


class WhereClause(PyClauseBase):
    def __init__(self, expr):
        self.expr = expr

    def __str__(self):
        return "WhereClause: {}".format(str(self.expr))

    def to_cpp(self, clause_builder):
        clause_builder.add_FilterClause(visit_expression(self.expr))


class ProjectClause(PyClauseBase):
    def __init__(self, name, expr):
        self.name = name
        self.expr = expr

    def __str__(self):
        return "ProjectClause:{} -> {}".format(str(self.expr), self.name)

    def to_cpp(self, clause_builder):
        clause_builder.add_ProjectClause(self.name, visit_expression(self.expr))


class Aggregation:
    def __init__(self, source, operator):
        self.source = source
        self.operator = operator

    def __str__(self):
        return "{}({})".format(self.operator, self.source)

    def to_cpp(self, clause_builder):
        # TODO: Move to dictionary
        if self.operator.lower() == "sum":
            clause_builder.add_SumAggregationOperator(self.source, self.source)
        elif self.operator.lower() == "mean":
            clause_builder.add_MeanAggregationOperator(self.source, self.source)
        elif self.operator.lower() == "max":
            clause_builder.add_MaxAggregationOperator(self.source, self.source)
        elif self.operator.lower() == "min":
            clause_builder.add_MinAggregationOperator(self.source, self.source)
        else:
            raise ValueError("Aggregation operators are limited to 'sum', 'mean', 'max' and 'min'.")


class GroupByClause(PyClauseBase):
    def __init__(self, key, query_builder):
        self.key = key
        self.query_builder = query_builder
        self.aggregations = {}

    def __str__(self):
        return "GroupByClause: key={}, [{}]".format(
            str(self.key), ", ".join(["{} <- {}".format(k, v) for k, v in self.aggregations.items()])
        )

    def agg(self, aggregations):
        for key, value in aggregations.items():
            self.aggregations[key] = Aggregation(key, value)

        return self.query_builder

    def to_cpp(self, clause_builder):
        def _expression_root_only(col_name: str):
            _ec = _ExecutionContext()
            _ec.root_node_name = _ExpressionName(col_name)

            return _ec

        clause_builder.prepare_AggregationClause(_expression_root_only(self.key))
        for agg in self.aggregations.values():
            agg.to_cpp(clause_builder)
        clause_builder.finalize_AggregationClause()


class QueryBuilder:
    """
    Build a query to process read results with. Syntax is designed to be similar to Pandas:

        >>> q = QueryBuilder()
        >>> q = q[q["a"] < 5] (equivalent to q = q[q.a < 5] provided the column name is also a valid Python variable name)
        >>> dataframe = lib.read(symbol, query_builder=q).data

    QueryBuilder objects are stateful, and so should not be reused without reinitialising:

    >>> q = QueryBuilder()

    For Group By and Aggregation functionality please see the documentation for the `groupby`. For projection
    functionality, see the documentation for the `apply` method.

    Supported numeric operations when filtering:

    * Binary comparisons: <, <=, >, >=, ==, !=

    * Unary NOT: ~

    * Binary arithmetic: +, -, *, /

    * Unary arithmetic: -, abs

    * Binary combinators: &, |, ^

    * List membership: isin, isnotin (also accessible with == and !=)

    isin/isnotin accept lists, sets, frozensets, 1D ndarrays, or *args unpacking. For example:

    >>> l = [1, 2, 3]
    >>> q.isin(l)

    is equivalent to...

    >>> q.isin(1, 2, 3)

    Boolean columns can be filtered on directly:

    >>> q = QueryBuilder()
    >>> q = q[q["boolean_column"]]

    and combined with other operations intuitively:

    >>> q = QueryBuilder()
    >>> q = q[(q["boolean_column_1"] & ~q["boolean_column_2"]) & (q["numeric_column"] > 0)]

    Arbitrary combinations of these expressions is possible, for example:

    >>> q = q[(((q["a"] * q["b"]) / 5) < (0.7 * q["c"])) & (q["b"] != 12)]

    See tests/unit/arcticdb/version_store/test_filtering.py for more example uses.

    Timestamp filtering:
        pandas.Timestamp, datetime.datetime, pandas.Timedelta, and datetime.timedelta objects are supported.
        Note that internally all of these types are converted to nanoseconds (since epoch in the Timestamp/datetime
        cases). This means that nonsensical operations such as multiplying two times together are permitted (but not
        encouraged).
    Restrictions:
        String equality/inequality (and isin/isnotin) is supported for printable ASCII characters only.
        Although not prohibited, it is not recommended to use ==, !=, isin, or isnotin with floating point values.
    Exceptions:
        inf or -inf values are provided for comparison
        Column involved in query is a Categorical
        Symbol is pickled
        Column involved in query is not present in symbol
        Query involves comparing strings using <, <=, >, or >= operators
        Query involves comparing a string to one or more numeric values, or vice versa
        Query involves arithmetic with a column containing strings
    """

    def __init__(self):
        self.stages = []
        self._optimisation = _Optimisation.SPEED

        self._clause_builder = _ClauseBuilder()

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
        >>> q = QueryBuilder()
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
        self.stages.append(ProjectClause(name, expr))
        return self

    def groupby(self, expr: str):
        """
        Group symbol by column name. GroupBy operations must be followed by an aggregation operator. Currently the following four aggregation 
        operators are supported:
            * "mean" - compute the mean of the group
            * "sum" - compute the sum of the group
            * "min" - compute the min of the group
            * "max" - compute the max of the group
        
        For usage examples, see below.

        Parameters
        ----------
        expr: `str`
            Name of the symbol to group on. Note that currently GroupBy only supports single-column groupings.

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
        >>> q = QueryBuilder()
        >>> q = q.groupby("grouping_column").agg({"to_mean": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data
                     to_mean
            group_1  1.666667
            group_2       NaN

        Max over one group:

        >>> df = pd.DataFrame(
            {
                "grouping_column": ["group_1", "group_1", "group_1"],
                "to_max": [1, 5, 4],
            },
            index=np.arange(3),
        )
        >>> q = QueryBuilder()
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
        >>> q = QueryBuilder()
        >>> q = q.groupby("grouping_column").agg({"to_max": "max", "to_mean": "mean"})
        >>> lib.write("symbol", df)
        >>> lib.read("symbol", query_builder=q).data
                        to_max   to_mean
            group_1     2.5  1.666667

        Returns
        -------
        QueryBuilder
            Modified QueryBuilder object.
        """
        self.stages.append(GroupByClause(expr, self))
        return self.stages[-1]

    def __eq__(self, right):
        return str(self) == str(right)

    def __str__(self):
        return " | ".join(str(e) for e in self.stages)

    def __getitem__(self, item):
        if isinstance(item, str):
            return ExpressionNode.column_ref(item)
        else:
            # This handles the case where the filtering is on a single boolean column
            # e.g. q = q[q["col"]]
            if isinstance(item, ExpressionNode) and item.operator == COLUMN:
                item = ExpressionNode.compose(item, _OperationType.IDENTITY, None)
            self.stages.append(WhereClause(item))
            return self

    def __getattr__(self, key):
        return self[key]

    def __getstate__(self):
        rv = vars(self).copy()
        del rv["_clause_builder"]
        return rv

    def __setstate__(self, state):
        vars(self).update(state)
        self._clause_builder = _ClauseBuilder()

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k != "_clause_builder":
                setattr(result, k, copy.deepcopy(v, memo))
        result._clause_builder = _ClauseBuilder()
        return result

    # Might want to apply different optimisations to different clauses once projections/group-bys are implemented
    def optimise_for_speed(self):
        """Process query as fast as possible (the default behaviour)"""
        self._optimisation = _Optimisation.SPEED

    def optimise_for_memory(self):
        """Reduce peak memory usage during the query, at the expense of some performance.

        Optimisations applied:

        * Memory used by strings that are present in segments read from storage, but are not required in the final dataframe that will be presented back to the user, is reclaimed earlier in the processing pipeline.
        """
        self._optimisation = _Optimisation.MEMORY

    def execution_contexts(self):
        res = [visit_expression(stage.expr) for stage in self.stages]
        for execution_context in res:
            execution_context.optimisation = self._optimisation
        return res

    def finalize_clause_builder(self):
        for py_clause in self.stages:
            py_clause.to_cpp(self._clause_builder)

        return self._clause_builder


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
        value = int(value.timestamp() * 1_000_000_000)
        f = ValueInt64
    elif isinstance(value, pd.Timedelta):
        value = value.value
        f = ValueInt64
    elif isinstance(value, datetime.timedelta):
        value = int(value.total_seconds() * 1_000_000_000)
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
                    execution_context.add_value_set(key, _ValueSet(node))
                    return _ValueSetName(key)
                else:
                    execution_context.add_value(key, create_value(node))
                    return _ValueName(key)

            if isinstance(node, ExpressionNode):
                if node.operator == COLUMN:
                    execution_context.add_column(node.left)
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
        execution_context.add_expression_node(node.get_name(), expression_node)

    execution_context = _ExecutionContext()
    valueset_keys = dict()
    _visit(expr)
    execution_context.root_node_name = _ExpressionName(expr.get_name())
    return execution_context
