"""
What the async db_read requests need to know of a table, as the data dictionary tells it.
"""

import datetime
from dataclasses import dataclass

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
