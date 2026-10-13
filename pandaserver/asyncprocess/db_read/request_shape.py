"""
Shape and cost checks of the queries in an async db_read request.

A request is {"queries": [query, ...]}. A query reads one or more tables of the allowlist
(tables.py) with the same conditions, one statement per table, and has these fields:

    tables: names of the tables
    where: conditions, each [column, op] or [column, op, value], all of which must hold.
        op is one of =, !=, <, <=, >, >=, in, is null, is not null and prefix. The value of
        "in" is a list, or {"from_query": i, "column": c} for the values of column c in the
        answer to the earlier query i of the same request. "prefix" matches the start of a
        text, taken literally. A time is written in ISO 8601, and read as UTC when it has no
        offset; {"__datetime__": ...}, the way the API writes times, is read too
    columns: columns to return. By default every column but the large objects
    group_by: columns, or day(column) and hour(column) for a time column
    aggregates: [function, column], the function being count, min or max; ["count", "*"]
        counts rows
    order_by: names the query returns, each a name or [name, "asc" | "desc"]
    limit: most rows to return from each table

Values must have the type of their column: a number for a number column, a text for a text
column, a time for a time column. The database would convert the others, but converting a
column rather than the value keeps it from using its index.

The checks are two:
    parse_query: the shape, without the database
    check_query: against the data dictionary, that the columns exist and that the cost is
        within the budget. The cost is the index lookups: a lookup of a LOCAL index probes
        each partition its window touches, so it costs keys x partitions touched; a GLOBAL
        index, or a table that is not partitioned, costs keys. The partitions of a time key
        made ahead, which start after the present on the database's clock, are not counted:
        the time keys of these tables say when a row was written, so no row is in them yet. A
        table with no index to look up is read only when it is small and not partitioned, at
        a cost of 1

A query that fails either is rejected with the name of the rule it broke, and the other
queries of the request still run.
"""

import datetime
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from pandaserver.asyncprocess.db_read.schema_info import (
    KIND_LOB,
    KIND_NUMBER,
    KIND_OTHER,
    KIND_TEXT,
    KIND_TIME,
    Bound,
    PartitionInfo,
    TableInfo,
)
from pandaserver.asyncprocess.db_read.tables import AllowedTable, get_allowed_table

# most queries in one request
MAX_QUERIES = 10
# most rows a query returns from each table, and the default
MAX_ROWS = 1000
# most values in one "in", which is also the most Oracle takes in one IN list
MAX_KEYS = 1000
# most conditions in one query
MAX_CONDITIONS = 20
# most rows of a table read without an index, by the optimizer statistics
SMALL_TABLE_ROWS = 10000

OPS = ("=", "!=", "<", "<=", ">", ">=", "in", "is null", "is not null", "prefix")
NULL_OPS = ("is null", "is not null")
AGGREGATE_FUNCTIONS = ("count", "min", "max")
TRUNCATIONS = ("day", "hour")

_QUERY_FIELDS = ("tables", "where", "columns", "group_by", "aggregates", "order_by", "limit")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TRUNCATED_NAME = re.compile(r"^(day|hour)\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)$", re.IGNORECASE)


class QueryRejected(Exception):
    """A query broke a rule. The rule is a fixed name a caller can test, the message says how."""

    def __init__(self, rule: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.rule = rule
        self.message = message
        self.detail = detail


@dataclass(frozen=True)
class FromQuery:
    # position of the earlier query in the request
    query: int
    # lower-case column of its answer
    column: str


@dataclass(frozen=True)
class Condition:
    # lower-case column
    column: str
    # one of OPS
    op: str
    # None for the null ops, a list or FromQuery for "in", else one value
    value: Any


@dataclass(frozen=True)
class GroupKey:
    column: str
    # None, or one of TRUNCATIONS
    truncation: str | None

    @property
    def name(self) -> str:
        return f"{self.truncation}({self.column})" if self.truncation else self.column


@dataclass(frozen=True)
class Aggregate:
    # one of AGGREGATE_FUNCTIONS
    function: str
    # None for count(*)
    column: str | None

    @property
    def name(self) -> str:
        return f"{self.function}({self.column or '*'})"


@dataclass(frozen=True)
class OrderKey:
    # a name the query returns
    name: str
    descending: bool


@dataclass(frozen=True)
class Query:
    tables: tuple[AllowedTable, ...]
    where: tuple[Condition, ...]
    # None for the default
    columns: tuple[str, ...] | None
    group_by: tuple[GroupKey, ...]
    aggregates: tuple[Aggregate, ...]
    order_by: tuple[OrderKey, ...]
    limit: int

    @property
    def grouped(self) -> bool:
        return bool(self.group_by or self.aggregates)

    def from_queries(self) -> list[FromQuery]:
        return [condition.value for condition in self.where if isinstance(condition.value, FromQuery)]


@dataclass(frozen=True)
class TablePlan:
    table: AllowedTable
    info: TableInfo
    # the conditions of the query, with the values read for the columns of this table
    where: tuple[Condition, ...]
    # index lookups the statement costs
    probes: int


def _name(value: Any, what: str) -> str:
    if not isinstance(value, str) or not _NAME.match(value):
        raise QueryRejected("bad_shape", f"{what} must be a name of letters, digits and _, got {value!r}")
    return value.lower()


def _list(value: Any, what: str) -> list[Any]:
    if not isinstance(value, list):
        raise QueryRejected("bad_shape", f"{what} must be a list")
    return value


def _scalar(value: Any, what: str) -> Any:
    # a time as the API writes it
    if isinstance(value, dict) and set(value) == {"__datetime__"}:
        value = value["__datetime__"]
    # bool is an int to Python, and no column takes one
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise QueryRejected("bad_value", f"{what} must be a number or a text, got {value!r}")
    return value


def _parse_condition(raw: Any, position: int) -> Condition:
    if not isinstance(raw, list) or len(raw) not in (2, 3):
        raise QueryRejected("bad_shape", f"a condition must be [column, op] or [column, op, value], got {raw!r}")
    column = _name(raw[0], "the column of a condition")
    op = " ".join(str(raw[1]).lower().split()) if isinstance(raw[1], str) else None
    if op not in OPS:
        raise QueryRejected("bad_shape", f"op must be one of {', '.join(OPS)}, got {raw[1]!r}")
    if op in NULL_OPS:
        if len(raw) != 2:
            raise QueryRejected("bad_shape", f"{op} takes no value")
        return Condition(column, op, None)
    if len(raw) != 3:
        raise QueryRejected("bad_shape", f"{op} takes a value")
    value = raw[2]
    if op != "in":
        if isinstance(value, dict) and "from_query" in value:
            raise QueryRejected("bad_shape", f"values from a query go with in, not with {op}")
        value = _scalar(value, f"the value of {column} {op}")
        if op == "prefix" and (not isinstance(value, str) or not value):
            raise QueryRejected("bad_value", f"the value of {column} prefix must be a text")
        return Condition(column, op, value)
    if isinstance(value, dict) and "from_query" in value:
        if set(value) != {"from_query", "column"}:
            raise QueryRejected("bad_shape", 'a value from a query must be {"from_query": i, "column": c}')
        index = value["from_query"]
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < position:
            raise QueryRejected("bad_shape", f"from_query must be the position of an earlier query, got {index!r}")
        return Condition(column, op, FromQuery(index, _name(value["column"], "the column of from_query")))
    values = _list(value, f"the value of {column} in")
    if not values:
        raise QueryRejected("bad_value", f"the value of {column} in has no values")
    if len(values) > MAX_KEYS:
        raise QueryRejected("too_many_keys", f"{column} in has {len(values)} values", keys=len(values), max_keys=MAX_KEYS)
    return Condition(column, op, [_scalar(item, f"a value of {column} in") for item in values])


def _parse_group_key(raw: Any) -> GroupKey:
    if isinstance(raw, str):
        match = _TRUNCATED_NAME.match(raw)
        if match:
            return GroupKey(match.group(2).lower(), match.group(1).lower())
    return GroupKey(_name(raw, "a group_by entry"), None)


def _parse_aggregate(raw: Any) -> Aggregate:
    if not isinstance(raw, list) or len(raw) != 2:
        raise QueryRejected("bad_shape", f"an aggregate must be [function, column], got {raw!r}")
    function = raw[0].lower() if isinstance(raw[0], str) else None
    if function not in AGGREGATE_FUNCTIONS:
        raise QueryRejected("bad_shape", f"the function of an aggregate must be one of {', '.join(AGGREGATE_FUNCTIONS)}, got {raw[0]!r}")
    if raw[1] == "*":
        if function != "count":
            raise QueryRejected("bad_shape", f"{function} needs a column")
        return Aggregate(function, None)
    return Aggregate(function, _name(raw[1], "the column of an aggregate"))


def _parse_order_key(raw: Any) -> OrderKey:
    direction = "asc"
    if isinstance(raw, list):
        if len(raw) != 2 or not isinstance(raw[1], str) or raw[1].lower() not in ("asc", "desc"):
            raise QueryRejected("bad_shape", f'an order_by entry must be a name or [name, "asc" | "desc"], got {raw!r}')
        raw, direction = raw[0], raw[1].lower()
    if not isinstance(raw, str):
        raise QueryRejected("bad_shape", f"an order_by entry must name what the query returns, got {raw!r}")
    name = raw.lower().replace(" ", "")
    return OrderKey(name, direction == "desc")


def _unique(names: Sequence[str], what: str) -> None:
    if len(set(names)) != len(names):
        raise QueryRejected("bad_shape", f"{what} names something twice")


def parse_query(raw: Any, position: int) -> Query:
    """
    Check the shape of a query, without the database.

    Args:
        raw(Any): the query as the request gives it
        position(int): its position in the request, which an "in" from an earlier query refers below

    Returns:
        Query: the query, names in lower case

    Raises:
        QueryRejected: when the query is not well formed
    """
    if not isinstance(raw, dict):
        raise QueryRejected("bad_shape", "a query must be an object")
    unknown = sorted(set(raw) - set(_QUERY_FIELDS))
    if unknown:
        raise QueryRejected("bad_shape", f"a query has no field {', '.join(map(str, unknown))}")
    # tables
    table_names = _list(raw.get("tables"), "tables")
    if not table_names:
        raise QueryRejected("bad_shape", "tables names no table")
    tables: list[AllowedTable] = []
    for name in table_names:
        table = get_allowed_table(name) if isinstance(name, str) else None
        if table is None:
            raise QueryRejected("unknown_table", f"{name!r} is not a table a request may read", table=name)
        tables.append(table)
    _unique([table.name for table in tables], "tables")
    # conditions
    raw_where = _list(raw.get("where", []), "where")
    if len(raw_where) > MAX_CONDITIONS:
        raise QueryRejected("bad_shape", f"a query has at most {MAX_CONDITIONS} conditions")
    where = tuple(_parse_condition(item, position) for item in raw_where)
    # what to return
    columns = None
    if "columns" in raw:
        columns = tuple(_name(item, "a column") for item in _list(raw["columns"], "columns"))
        if not columns:
            raise QueryRejected("bad_shape", "columns names no column")
        _unique(columns, "columns")
    group_by = tuple(_parse_group_key(item) for item in _list(raw.get("group_by", []), "group_by"))
    aggregates = tuple(_parse_aggregate(item) for item in _list(raw.get("aggregates", []), "aggregates"))
    _unique([key.name for key in group_by] + [aggregate.name for aggregate in aggregates], "group_by and aggregates")
    if columns is not None and (group_by or aggregates):
        raise QueryRejected("bad_shape", "a query with group_by or aggregates returns those, so it takes no columns")
    order_by = tuple(_parse_order_key(item) for item in _list(raw.get("order_by", []), "order_by"))
    _unique([key.name for key in order_by], "order_by")
    returned: Sequence[str] | None = columns
    if group_by or aggregates:
        returned = [key.name for key in group_by] + [aggregate.name for aggregate in aggregates]
    if returned is not None:
        for key in order_by:
            if key.name not in returned:
                raise QueryRejected("bad_shape", f"order_by names {key.name}, which the query does not return")
    else:
        for key in order_by:
            _name(key.name, "an order_by entry of a query returning columns")
    # rows
    limit = raw.get("limit", MAX_ROWS)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_ROWS:
        raise QueryRejected("bad_shape", f"limit must be from 1 to {MAX_ROWS}, got {limit!r}")
    return Query(tuple(tables), where, columns, group_by, aggregates, order_by, limit)


def parse_time(value: str) -> datetime.datetime:
    """
    Read a time in ISO 8601 as the naive UTC time the database holds.

    Args:
        value(str): the time, read as UTC when it has no offset

    Returns:
        datetime: the time in UTC, without a time zone

    Raises:
        ValueError: when it is not a time
    """
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def _read_value(value: Any, kind: str, table: str, column: str) -> Any:
    if kind == KIND_NUMBER and isinstance(value, (int, float)):
        return value
    if kind == KIND_TEXT and isinstance(value, str):
        return value
    if kind == KIND_TIME and isinstance(value, str):
        try:
            return parse_time(value)
        except ValueError:
            pass
    raise QueryRejected("bad_value", f"{value!r} is not a value of {table}.{column}, a {kind} column", table=table, column=column)


def _compare(left: Any, right: Any) -> int | None:
    # -1, 0 or 1, or None when the two cannot be compared, such as a time with a number
    try:
        order: int = (left > right) - (left < right)
    except TypeError:
        return None
    return order


def _may_be_above(value: Any, bound: Any, inclusive: bool) -> bool:
    # whether value may satisfy "> bound", or ">= bound" when inclusive. Values that cannot be
    # compared may, which counts a partition too many rather than one too few
    order = _compare(value, bound)
    return order is None or order == 1 or (inclusive and order == 0)


def count_touched_partitions(partitioning: PartitionInfo, where: Sequence[Condition], now: datetime.datetime | None = None) -> int:
    """
    Count the partitions the conditions on the partition key leave to read.

    Args:
        partitioning(PartitionInfo): the partitions of the table
        where(list): conditions with values read for the table. A value that cannot be compared
            with the bounds, and an "in" from a query, leave every partition to read
        now(datetime): the present on the database's clock, in UTC, or None. A partition of a time
            key starting after it is made ahead and holds no row yet, so it is not counted

    Returns:
        int: partitions touched, at least 1
    """
    # the values the key may take, None for any, and the window it must be in
    points: set[Any] | None = None
    lowers: list[tuple[Any, bool]] = []
    uppers: list[tuple[Any, bool]] = []
    for condition in where:
        if condition.column != partitioning.key_column:
            continue
        if condition.op == "=":
            values = {condition.value}
        elif condition.op == "in" and isinstance(condition.value, list):
            values = set(condition.value)
        else:
            if condition.op in (">", ">="):
                lowers.append((condition.value, condition.op == ">="))
            elif condition.op in ("<", "<="):
                uppers.append((condition.value, condition.op == "<="))
            continue
        points = values if points is None else points & values

    def in_window(value: Any) -> bool:
        # value >= or > each lower, and value <= or < each upper
        return all(_may_be_above(value, bound, inclusive) for bound, inclusive in lowers) and all(
            _may_be_above(bound, value, inclusive) for bound, inclusive in uppers
        )

    def touched(bound_lower: Bound | None, bound_upper: Bound | None) -> bool:
        # the partition holds the values v with bound_lower <= v < bound_upper
        def in_partition(value: Any) -> bool:
            return (bound_lower is None or _may_be_above(value, bound_lower, True)) and (bound_upper is None or _may_be_above(bound_upper, value, False))

        if points is not None:
            return any(in_window(point) and in_partition(point) for point in points)
        # some v < bound_upper must pass each lower, and some v >= bound_lower each upper
        if bound_upper is not None and not all(_may_be_above(bound_upper, bound, False) for bound, _ in lowers):
            return False
        if bound_lower is not None and not all(_may_be_above(bound, bound_lower, inclusive) for bound, inclusive in uppers):
            return False
        return True

    def made_ahead(bound_lower: Bound | None) -> bool:
        # a bound that is not a time cannot be compared with now, which keeps the partition
        return now is not None and bound_lower is not None and _compare(bound_lower, now) == 1

    n_touched = sum(1 for bound_lower, bound_upper in partitioning.bounds if touched(bound_lower, bound_upper) and not made_ahead(bound_lower))
    return max(n_touched, 1)


def _kind_of(table: AllowedTable, info: TableInfo, column: str) -> str:
    kind = info.columns.get(column)
    if kind is None:
        raise QueryRejected("unknown_column", f"{table.name} has no column {column}", table=table.name, column=column)
    return kind


def check_query(
    query: Query, infos: dict[str, TableInfo], from_sizes: dict[FromQuery, int], budget: int, now: datetime.datetime | None = None
) -> list[TablePlan]:
    """
    Check a query against the data dictionary and price it.

    Args:
        query(Query): the query, from parse_query
        infos(dict): the dictionary entry of each table of the query, by the table's name
        from_sizes(dict): how many values each "in" from a query has, which the earlier answers tell
        budget(int): most index lookups the query may cost, over all its tables
        now(datetime): the present on the database's clock, in UTC, before which the partitions
            counted start, or None to count them all

    Returns:
        list: a plan for each table of the query, in its order

    Raises:
        QueryRejected: when the query names what a table does not have, gives a value of the wrong
            type, has no index to look up, or costs more than the budget
    """
    plans: list[TablePlan] = []
    for table in query.tables:
        info = infos[table.name]

        # what the query returns
        for column in query.columns or ():
            _kind_of(table, info, column)
        for key in query.group_by:
            if _kind_of(table, info, key.column) in (KIND_LOB, KIND_OTHER):
                raise QueryRejected("bad_value", f"{table.name}.{key.column} cannot be grouped", table=table.name, column=key.column)
            if key.truncation and _kind_of(table, info, key.column) != KIND_TIME:
                raise QueryRejected("bad_value", f"{key.name} needs a time column", table=table.name, column=key.column)
        for aggregate in query.aggregates:
            if aggregate.column is not None and _kind_of(table, info, aggregate.column) in (KIND_LOB, KIND_OTHER):
                raise QueryRejected("bad_value", f"{aggregate.name} cannot be taken of {table.name}", table=table.name, column=aggregate.column)
        if not query.grouped and query.columns is None:
            for order_key in query.order_by:
                _kind_of(table, info, order_key.name)
        # conditions, with their values read for the columns of this table
        where: list[Condition] = []
        for condition in query.where:
            kind = _kind_of(table, info, condition.column)
            if kind in (KIND_LOB, KIND_OTHER):
                raise QueryRejected("bad_value", f"{table.name}.{condition.column} cannot be compared", table=table.name, column=condition.column)
            if condition.op == "prefix" and kind != KIND_TEXT:
                raise QueryRejected(
                    "bad_value",
                    f"prefix needs a text column, and {table.name}.{condition.column} is a {kind} column",
                    table=table.name,
                    column=condition.column,
                )
            value = condition.value
            if isinstance(value, list):
                value = [_read_value(item, kind, table.name, condition.column) for item in value]
            elif condition.op not in NULL_OPS and not isinstance(value, FromQuery):
                value = _read_value(value, kind, table.name, condition.column)
            where.append(Condition(condition.column, condition.op, value))
        # keys to look up, by column
        keys: dict[str, int] = {}
        for condition in where:
            if condition.op == "=":
                n_keys = 1
            elif condition.op == "in":
                n_keys = from_sizes[condition.value] if isinstance(condition.value, FromQuery) else len(condition.value)
                if n_keys > MAX_KEYS:
                    # only an "in" from a query gets here, parse_query having refused a longer list
                    raise QueryRejected("too_many_keys", f"{condition.column} in has {n_keys} values from a query", keys=n_keys, max_keys=MAX_KEYS)
            else:
                continue
            keys[condition.column] = min(n_keys, keys.get(condition.column, n_keys))
        # the cheapest index to look up
        probes: int | None = None
        partitioning = info.partitioning
        n_touched = count_touched_partitions(partitioning, where, now) if partitioning is not None else 1
        for index in info.indexes:
            if index.leading_column not in keys:
                continue
            n_keys = keys[index.leading_column]
            if partitioning is not None and index.local and index.leading_column != partitioning.key_column:
                cost = n_keys * n_touched
            else:
                # a global index is one structure; and each key of the partition key is in one partition
                cost = n_keys
            probes = cost if probes is None else min(probes, cost)
        if probes is None:
            if partitioning is None and info.num_rows is not None and info.num_rows <= SMALL_TABLE_ROWS:
                probes = 1
            else:
                raise QueryRejected(
                    "no_index_key",
                    f"{table.name} is read only with = or in on the first column of an index, which no condition gives",
                    table=table.name,
                )
        plans.append(TablePlan(table, info, tuple(where), probes))
    total = sum(plan.probes for plan in plans)
    if total > budget:
        raise QueryRejected(
            "probe_budget",
            f"the query costs {total} index lookups, over the budget of {budget}",
            probes={plan.table.name: plan.probes for plan in plans},
            budget=budget,
        )
    return plans
