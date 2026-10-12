"""
What the async db_read requests need to know of a table, as the data dictionary tells it.
"""

import datetime
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pandaserver.taskbuffer.db_proxy_mods.base_module import parse_partition_bounds

# a partition bound: a time for a table partitioned on a time column, an int for one
# partitioned on a number, such as the JEDI tables on jediTaskID
Bound = datetime.datetime | int

# kinds of column. The kind decides how a value in a request is read, and what a request
# may do with the column
KIND_NUMBER = "number"
KIND_TEXT = "text"
KIND_TIME = "time"
# CLOB and the like: returned only when named, and never compared
KIND_LOB = "lob"
# anything else, such as RAW: returned, and never compared
KIND_OTHER = "other"


@dataclass(frozen=True)
class IndexInfo:
    # lower-case name of the first column. An index led by an expression is not kept, since no
    # condition of a request can use it
    leading_column: str
    # True for an index partitioned like its table, which a lookup probes once per partition
    # touched; False for a global index or one on a table that is not partitioned
    local: bool


@dataclass(frozen=True)
class PartitionInfo:
    # lower-case name of the partition key column
    key_column: str
    # [lower, upper) of each partition, None where it is open
    bounds: tuple[tuple[Bound | None, Bound | None], ...]


@dataclass(frozen=True)
class TableInfo:
    owner: str
    table_name: str
    # lower-case column names in the order of the table, each with its kind
    columns: dict[str, str]
    indexes: tuple[IndexInfo, ...]
    # None when the table is not partitioned
    partitioning: PartitionInfo | None
    # rows according to the optimizer statistics, or None when there are none
    num_rows: int | None


class TableNotVisible(Exception):
    """The data dictionary shows no columns of the table, which is what a missing grant looks like."""


# runs one statement of the data dictionary and returns its rows
RunStatement = Callable[[str, dict[str, Any]], list[tuple[Any, ...]]]

# how long a table read from the dictionary is kept, which is how long a new index or partition
# may go unseen
CACHE_SECONDS = 600

_COMMENT = " /* db_read schema_info */"

_ORACLE_KINDS = {
    "NUMBER": KIND_NUMBER,
    "FLOAT": KIND_NUMBER,
    "INTEGER": KIND_NUMBER,
    "BINARY_FLOAT": KIND_NUMBER,
    "BINARY_DOUBLE": KIND_NUMBER,
    "VARCHAR2": KIND_TEXT,
    "NVARCHAR2": KIND_TEXT,
    "CHAR": KIND_TEXT,
    "NCHAR": KIND_TEXT,
    "DATE": KIND_TIME,
    "CLOB": KIND_LOB,
    "NCLOB": KIND_LOB,
    "BLOB": KIND_LOB,
    "LONG": KIND_LOB,
}

_POSTGRES_KINDS = {
    "smallint": KIND_NUMBER,
    "integer": KIND_NUMBER,
    "bigint": KIND_NUMBER,
    "numeric": KIND_NUMBER,
    "real": KIND_NUMBER,
    "double precision": KIND_NUMBER,
    "character varying": KIND_TEXT,
    "character": KIND_TEXT,
    "text": KIND_TEXT,
    "date": KIND_TIME,
    "timestamp without time zone": KIND_TIME,
    "timestamp with time zone": KIND_TIME,
    "json": KIND_LOB,
    "jsonb": KIND_LOB,
}

_INTEGER = re.compile(r"^\s*(-?\d+)\s*$")
# FOR VALUES FROM (lower) TO (upper), as pg_get_expr spells a range partition
_POSTGRES_RANGE = re.compile(r"^FOR VALUES FROM \((.*)\) TO \((.*)\)$")
# RANGE (column), as pg_get_partkeydef spells a range partitioning on one column
_POSTGRES_RANGE_KEY = re.compile(r"^RANGE \(([A-Za-z_][A-Za-z0-9_]*)\)$")

_cache: dict[tuple[str, str, str], tuple[float, TableInfo]] = {}
_cache_lock = threading.Lock()


def column_kind(backend: str, data_type: str) -> str:
    """
    Get the kind of a column from its data type as the dictionary spells it.

    Args:
        backend(str): oracle or postgres
        data_type(str): the data type, such as TIMESTAMP(6) for Oracle or bigint for Postgres

    Returns:
        str: one of the KIND_ constants
    """
    if backend == "postgres":
        return _POSTGRES_KINDS.get(data_type, KIND_OTHER)
    if data_type.startswith("TIMESTAMP"):
        return KIND_TIME
    return _ORACLE_KINDS.get(data_type, KIND_OTHER)


def parse_bound(text: str | None, kind: str) -> Bound | None:
    """
    Read one partition bound.

    Args:
        text(str): the bound, an Oracle HIGH_VALUE or one side of a Postgres FOR VALUES
        kind(str): kind of the partition key column

    Returns:
        datetime or int or None: the bound, or None for MAXVALUE, MINVALUE and what cannot be read,
            which leaves that side of the partition open
    """
    if kind == KIND_TIME:
        stamps = parse_partition_bounds(text)
        return stamps[0] if len(stamps) == 1 else None
    if kind == KIND_NUMBER:
        match = _INTEGER.match((text or "").strip("'"))
        return int(match.group(1)) if match else None
    return None


def read_table_info(run: RunStatement, backend: str, owner: str, table_name: str) -> TableInfo:
    """
    Read a table from the data dictionary.

    Args:
        run(callable): runs a statement and returns its rows; the caller puts each round trip under its limits
        backend(str): oracle or postgres
        owner(str): schema owning the table
        table_name(str): name of the table

    Returns:
        TableInfo: the table

    Raises:
        TableNotVisible: when the dictionary shows no columns of the table
    """
    if backend == "postgres":
        return _read_postgres(run, owner.lower(), table_name.lower())
    return _read_oracle(run, owner.upper(), table_name.upper())


def _read_oracle(run: RunStatement, owner: str, table_name: str) -> TableInfo:
    names = {":owner": owner, ":table_name": table_name}
    rows = run("SELECT column_name, data_type FROM ALL_TAB_COLUMNS WHERE owner=:owner AND table_name=:table_name ORDER BY column_id" + _COMMENT, names)
    if not rows:
        raise TableNotVisible(f"{owner}.{table_name}")
    columns = {name.lower(): column_kind("oracle", data_type) for name, data_type in rows}
    # an index led by an expression is led by a hidden column such as SYS_NC00034$, which is not
    # among the columns above, and is left out with them
    rows = run(
        "SELECT c.column_name, p.locality FROM ALL_INDEXES i "
        "JOIN ALL_IND_COLUMNS c ON c.index_owner=i.owner AND c.index_name=i.index_name AND c.column_position=1 "
        "LEFT JOIN ALL_PART_INDEXES p ON p.owner=i.owner AND p.index_name=i.index_name "
        "WHERE i.table_owner=:owner AND i.table_name=:table_name " + _COMMENT,
        names,
    )
    indexes = tuple(IndexInfo(column.lower(), locality == "LOCAL") for column, locality in rows if column.lower() in columns)
    rows = run("SELECT num_rows FROM ALL_TABLES WHERE owner=:owner AND table_name=:table_name " + _COMMENT, names)
    num_rows = int(rows[0][0]) if rows and rows[0][0] is not None else None
    partitioning = None
    rows = run("SELECT partitioning_type FROM ALL_PART_TABLES WHERE owner=:owner AND table_name=:table_name " + _COMMENT, names)
    if rows:
        partitioning_type = rows[0][0]
        key_rows = run(
            "SELECT column_name FROM ALL_PART_KEY_COLUMNS WHERE owner=:owner AND name=:table_name AND object_type='TABLE' ORDER BY column_position " + _COMMENT,
            names,
        )
        bound_rows = run(
            "SELECT high_value FROM ALL_TAB_PARTITIONS WHERE table_owner=:owner AND table_name=:table_name ORDER BY partition_position " + _COMMENT, names
        )
        key_column = key_rows[0][0].lower() if key_rows else ""
        # bounds are read for a range on one column, interval partitioning being a kind of range;
        # for anything else every partition is taken to hold any value
        readable = partitioning_type == "RANGE" and len(key_rows) == 1 and key_column in columns
        bounds: list[tuple[Bound | None, Bound | None]] = []
        lower: Bound | None = None
        for (high_value,) in bound_rows:
            if not readable:
                bounds.append((None, None))
                continue
            # a partition starts where the one before it ends
            upper = parse_bound(high_value, columns[key_column])
            bounds.append((lower, upper))
            lower = upper
        partitioning = PartitionInfo(key_column, tuple(bounds))
    return TableInfo(owner, table_name, columns, indexes, partitioning, num_rows)


def _read_postgres(run: RunStatement, owner: str, table_name: str) -> TableInfo:
    names = {":owner": owner, ":table_name": table_name}
    rows = run(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=:owner AND table_name=:table_name ORDER BY ordinal_position "
        + _COMMENT,
        names,
    )
    if not rows:
        raise TableNotVisible(f"{owner}.{table_name}")
    columns = {name.lower(): column_kind("postgres", data_type) for name, data_type in rows}
    rows = run(
        "SELECT t.relkind, t.reltuples, pg_get_partkeydef(t.oid) FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace "
        "WHERE n.nspname=:owner AND t.relname=:table_name " + _COMMENT,
        names,
    )
    relkind, reltuples, partition_key = rows[0] if rows else ("r", None, None)
    # the statistics say 0, or -1 from Postgres 14 on, for a table never analyzed
    num_rows = int(reltuples) if reltuples is not None and reltuples > 0 else None
    # an index on a partitioned table is partitioned with it: Postgres has no other kind. An
    # index led by an expression has 0 as its first column, which no column matches
    rows = run(
        "SELECT a.attname FROM pg_index x JOIN pg_class t ON t.oid=x.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=x.indkey[0] "
        "WHERE n.nspname=:owner AND t.relname=:table_name " + _COMMENT,
        names,
    )
    indexes = tuple(IndexInfo(column.lower(), relkind == "p") for (column,) in rows if column.lower() in columns)
    partitioning = None
    if relkind == "p":
        match = _POSTGRES_RANGE_KEY.match(partition_key or "")
        key_column = match.group(1).lower() if match else ""
        readable = key_column in columns
        rows = run(
            "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i "
            "JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent "
            "JOIN pg_namespace n ON n.oid=p.relnamespace "
            "WHERE n.nspname=:owner AND p.relname=:table_name " + _COMMENT,
            names,
        )
        bounds: list[tuple[Bound | None, Bound | None]] = []
        for (bound,) in rows:
            # the default partition, and the bound of anything but a range, take any value
            sides = _POSTGRES_RANGE.match(bound or "") if readable else None
            if sides is None:
                bounds.append((None, None))
            else:
                bounds.append((parse_bound(sides.group(1), columns[key_column]), parse_bound(sides.group(2), columns[key_column])))
        partitioning = PartitionInfo(key_column, tuple(bounds))
    return TableInfo(owner, table_name, columns, indexes, partitioning, num_rows)


def get_table_info(run: RunStatement, backend: str, owner: str, table_name: str) -> TableInfo:
    """
    Get a table from the data dictionary, read again after CACHE_SECONDS.

    Args:
        run(callable): runs a statement and returns its rows, used when the table is read
        backend(str): oracle or postgres
        owner(str): schema owning the table
        table_name(str): name of the table

    Returns:
        TableInfo: the table

    Raises:
        TableNotVisible: when the dictionary shows no columns of the table
    """
    key = (backend, owner.lower(), table_name.lower())
    with _cache_lock:
        cached = _cache.get(key)
    if cached is not None and time.monotonic() - cached[0] < CACHE_SECONDS:
        return cached[1]
    info = read_table_info(run, backend, owner, table_name)
    with _cache_lock:
        _cache[key] = (time.monotonic(), info)
    return info
