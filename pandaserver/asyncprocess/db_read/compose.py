"""
SQL of the statements of an async db_read request.

Each table of a query gets a statement of its own. The names in it come from the allowlist
and the data dictionary, never from the text of the request, and every value is a bind
variable. A statement asks for one row more than the limit of its query, so that a result
cut at the limit says so: the limit is written in the statement because with Postgres the
driver fetches the whole result when it executes, which a fetch of fewer rows does not stop.

The text keeps clear of what WrappedCursor rewrites for Postgres: there is no ":" or "%"
outside the bind variables, and each bind variable is followed by a space, a comma or a
parenthesis.
"""

import re
from dataclasses import dataclass
from typing import Any

from pandaserver.asyncprocess.db_read.request_shape import (
    NULL_OPS,
    Aggregate,
    FromQuery,
    GroupKey,
    Query,
    TablePlan,
)
from pandaserver.asyncprocess.db_read.schema_info import KIND_LOB, STATEMENT_COMMENT

# the escape character of a prefix match. Not a backslash, which the two databases and the
# Python string between them each read their own way
_LIKE_ESCAPE = "!"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")

_COMPARISONS = {"=": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">="}


@dataclass(frozen=True)
class Statement:
    sql: str
    binds: dict[str, Any]
    # names of what each row holds, in order
    names: tuple[str, ...]
    # most rows to keep. The statement asks for one more, which tells the result was cut
    limit: int


def _identifier(name: str) -> str:
    # names come from the allowlist and the dictionary, so this is a second guard, not the first
    if not _IDENTIFIER.match(name):
        raise ValueError(f"{name!r} is not a plain identifier")
    return name


def escape_prefix(prefix: str) -> str:
    """
    Get the LIKE pattern that matches a text starting with the prefix, taken literally.

    Args:
        prefix(str): the start of the text

    Returns:
        str: the pattern, to be used with ESCAPE '!'
    """
    escaped = prefix.replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2).replace("%", f"{_LIKE_ESCAPE}%").replace("_", f"{_LIKE_ESCAPE}_")
    return escaped + "%"


def _group_expression(key: GroupKey, backend: str) -> str:
    column = _identifier(key.column)
    if key.truncation is None:
        return column
    if backend == "postgres":
        return f"date_trunc('{key.truncation}',{column})"
    return f"TRUNC({column},'{'DD' if key.truncation == 'day' else 'HH24'}')"


def _aggregate_expression(aggregate: Aggregate) -> str:
    argument = "*" if aggregate.column is None else _identifier(aggregate.column)
    return f"{aggregate.function.upper()}({argument})"


def compose_statement(plan: TablePlan, query: Query, from_values: dict[FromQuery, list[Any]], backend: str) -> Statement | None:
    """
    Compose the statement that reads one table of a query.

    Args:
        plan(TablePlan): the table, from check_query
        query(Query): the query
        from_values(dict): the values of each "in" from a query
        backend(str): oracle or postgres

    Returns:
        Statement or None: the statement, or None when an "in" from a query has no values, which no
            row can satisfy
    """
    binds: dict[str, Any] = {}

    def bind(value: Any) -> str:
        name = f":r{len(binds)}"
        binds[name] = value
        return name

    # what to return
    expressions: list[str] = []
    names: list[str] = []
    if query.grouped:
        for key in query.group_by:
            expressions.append(_group_expression(key, backend))
            names.append(key.name)
        for aggregate in query.aggregates:
            expressions.append(_aggregate_expression(aggregate))
            names.append(aggregate.name)
    else:
        columns = query.columns or tuple(column for column, kind in plan.info.columns.items() if kind != KIND_LOB)
        expressions = [_identifier(column) for column in columns]
        names = list(columns)
    expression_of = dict(zip(names, expressions))
    # conditions
    conditions: list[str] = []
    for condition in plan.where:
        column = _identifier(condition.column)
        if condition.op in NULL_OPS:
            conditions.append(f"{column} {condition.op.upper()}")
        elif condition.op == "in":
            values = from_values[condition.value] if isinstance(condition.value, FromQuery) else condition.value
            if not values:
                return None
            conditions.append(f"{column} IN ({','.join(bind(value) for value in values)})")
        elif condition.op == "prefix":
            conditions.append(f"{column} LIKE {bind(escape_prefix(condition.value))} ESCAPE '{_LIKE_ESCAPE}'")
        else:
            conditions.append(f"{column} {_COMPARISONS[condition.op]} {bind(condition.value)}")
    sql = f"SELECT {','.join(expressions)} FROM {_identifier(plan.info.owner)}.{_identifier(plan.info.table_name)}"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    if query.group_by:
        sql += " GROUP BY " + ",".join(_group_expression(key, backend) for key in query.group_by)
    if query.order_by:
        # a name the query does not return is a column of the table, which check_query made sure of
        order = [f"{expression_of.get(key.name) or _identifier(key.name)}{' DESC' if key.descending else ''}" for key in query.order_by]
        sql += " ORDER BY " + ",".join(order)
    # each clause starts with a space, which is what keeps a bind variable ending the one before apart from it
    if backend == "postgres":
        sql += f" LIMIT {query.limit + 1}"
    else:
        sql += f" FETCH FIRST {query.limit + 1} ROWS ONLY"
    return Statement(sql + STATEMENT_COMMENT, binds, tuple(names), query.limit)
