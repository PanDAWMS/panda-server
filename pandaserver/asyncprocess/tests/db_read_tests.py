# Description: Unit tests for the shape and cost checks and the SQL of the async db_read requests.
# The tables are described the way PanDAWMS/panda-database schema/oracle has them; no database is used
import datetime
import unittest
from typing import Any
from unittest import mock

from pandaserver.asyncprocess.db_read import schema_info
from pandaserver.asyncprocess.db_read.compose import compose_statement, escape_prefix
from pandaserver.asyncprocess.db_read.request_shape import (
    MAX_KEYS,
    FromQuery,
    Query,
    QueryRejected,
    TablePlan,
    check_query,
    count_touched_partitions,
    parse_query,
)
from pandaserver.asyncprocess.db_read.schema_info import (
    KIND_LOB,
    KIND_NUMBER,
    KIND_OTHER,
    KIND_TEXT,
    KIND_TIME,
    STATEMENT_COMMENT,
    IndexInfo,
    PartitionInfo,
    TableInfo,
    TableNotVisible,
    get_table_info,
    read_table_info,
)
from pandaserver.taskbuffer.WrappedCursor import convert_query_in_printf_format

DAY = datetime.timedelta(days=1)
D0 = datetime.datetime(2026, 10, 1)

JOB_COLUMNS = {
    "pandaid": KIND_NUMBER,
    "jeditaskid": KIND_NUMBER,
    "jobstatus": KIND_TEXT,
    "computingsite": KIND_TEXT,
    "modificationtime": KIND_TIME,
}

# not partitioned, so every index is one structure
JOBS_ACTIVE4 = TableInfo(
    "ATLAS_PANDA",
    "jobsActive4",
    dict(JOB_COLUMNS),
    (IndexInfo("pandaid", False), IndexInfo("jeditaskid", False), IndexInfo("jobstatus", False), IndexInfo("computingsite", False)),
    None,
)
JOBS_DEFINED4 = TableInfo("ATLAS_PANDA", "jobsDefined4", dict(JOB_COLUMNS), (IndexInfo("pandaid", False),), None)
# daily partitions on modificationTime: the range partition it is anchored on, then four
# interval partitions, each starting where the one before ends as Oracle tells it
JOBS_ARCHIVED4 = TableInfo(
    "ATLAS_PANDA",
    "jobsArchived4",
    dict(JOB_COLUMNS),
    (IndexInfo("pandaid", True), IndexInfo("jeditaskid", True)),
    PartitionInfo("modificationtime", ((None, D0), (D0, D0 + DAY), (D0 + DAY, D0 + 2 * DAY), (D0 + 2 * DAY, D0 + 3 * DAY), (D0 + 3 * DAY, D0 + 4 * DAY))),
)
# partitions of 500000 tasks on jediTaskID, with its primary key LOCAL
JEDI_TASKS = TableInfo(
    "ATLAS_PANDA",
    "JEDI_Tasks",
    {"jeditaskid": KIND_NUMBER, "status": KIND_TEXT, "modificationtime": KIND_TIME, "splitrule": KIND_TEXT},
    (IndexInfo("jeditaskid", True), IndexInfo("status", True)),
    PartitionInfo("jeditaskid", ((None, 500000), (500000, 1000000), (1000000, 1500000))),
)
# a few hundred rows, with a primary key that starts with app
CONFIG = TableInfo(
    "ATLAS_PANDA",
    "CONFIG",
    {"app": KIND_TEXT, "component": KIND_TEXT, "key": KIND_TEXT, "value": KIND_TEXT, "vo": KIND_TEXT},
    (IndexInfo("app", False),),
    None,
)
JOB_PARAMS = TableInfo(
    "ATLAS_PANDA",
    "jobParamsTable",
    {"pandaid": KIND_NUMBER, "modificationtime": KIND_TIME, "jobparameters": KIND_LOB},
    (IndexInfo("pandaid", True),),
    PartitionInfo("modificationtime", ((None, D0), (D0, D0 + DAY))),
)

INFOS = {info.table_name: info for info in (JOBS_ACTIVE4, JOBS_DEFINED4, JOBS_ARCHIVED4, JEDI_TASKS, CONFIG, JOB_PARAMS)}


def parse(raw: dict[str, Any], position: int = 0) -> Query:
    return parse_query(raw, position)


def check(raw: dict[str, Any], budget: int = 1000, from_sizes: dict[FromQuery, int] | None = None, position: int = 0) -> list[TablePlan]:
    return check_query(parse(raw, position), INFOS, from_sizes or {}, budget)


class TestParseQuery(unittest.TestCase):
    def assertRejected(self, rule: str, raw: Any, position: int = 0) -> QueryRejected:
        with self.assertRaises(QueryRejected) as context:
            parse_query(raw, position)
        self.assertEqual(context.exception.rule, rule, context.exception.message)
        return context.exception

    def test_full_query(self) -> None:
        query = parse(
            {
                "tables": ["JOBSACTIVE4", "jobsArchived4"],
                "where": [["PandaID", "IN", [1, 2]], ["modificationTime", ">=", {"__datetime__": "2026-10-01T00:00:00"}], ["jobStatus", "is  not null"]],
                "group_by": ["computingSite", "day(modificationTime)"],
                "aggregates": [["count", "*"], ["MAX", "modificationTime"]],
                "order_by": [["count(*)", "desc"], "computingsite"],
                "limit": 10,
            }
        )
        self.assertEqual([table.name for table in query.tables], ["jobsActive4", "jobsArchived4"])
        self.assertEqual(
            [(c.column, c.op, c.value) for c in query.where],
            [("pandaid", "in", [1, 2]), ("modificationtime", ">=", "2026-10-01T00:00:00"), ("jobstatus", "is not null", None)],
        )
        self.assertEqual([key.name for key in query.group_by], ["computingsite", "day(modificationtime)"])
        self.assertEqual([aggregate.name for aggregate in query.aggregates], ["count(*)", "max(modificationtime)"])
        self.assertEqual([(key.name, key.descending) for key in query.order_by], [("count(*)", True), ("computingsite", False)])
        self.assertEqual(query.limit, 10)

    def test_defaults(self) -> None:
        query = parse({"tables": ["CONFIG"]})
        self.assertEqual((query.where, query.columns, query.limit), ((), None, 1000))

    def test_tables(self) -> None:
        self.assertRejected("unknown_table", {"tables": ["users"]})
        self.assertRejected("unknown_table", {"tables": ["jobs_StatusLog"]})
        self.assertRejected("bad_shape", {"tables": []})
        self.assertRejected("bad_shape", {"tables": ["jobsActive4", "JOBSACTIVE4"]})
        self.assertRejected("bad_shape", {"tables": ["jobsActive4"], "colums": ["pandaid"]})

    def test_conditions(self) -> None:
        tables = ["jobsActive4"]
        self.assertRejected("bad_shape", {"tables": tables, "where": [["pandaid", "like", "1%"]]})
        self.assertRejected("bad_shape", {"tables": tables, "where": [["pandaid", "is null", 1]]})
        self.assertRejected("bad_shape", {"tables": tables, "where": [["pandaid", "="]]})
        self.assertRejected("bad_shape", {"tables": tables, "where": [["pandaid; drop", "=", 1]]})
        self.assertRejected("bad_value", {"tables": tables, "where": [["pandaid", "in", []]]})
        self.assertRejected("bad_value", {"tables": tables, "where": [["pandaid", "=", True]]})
        self.assertRejected("bad_value", {"tables": tables, "where": [["pandaid", "=", None]]})
        self.assertRejected("bad_value", {"tables": tables, "where": [["jobstatus", "prefix", ""]]})
        rejected = self.assertRejected("too_many_keys", {"tables": tables, "where": [["pandaid", "in", list(range(MAX_KEYS + 1))]]})
        self.assertEqual(rejected.detail, {"keys": MAX_KEYS + 1, "max_keys": MAX_KEYS})
        self.assertRejected("bad_shape", {"tables": tables, "where": [["pandaid", "=", 1]] * 21})

    def test_from_query(self) -> None:
        raw = {"tables": ["jobsActive4"], "where": [["jeditaskid", "in", {"from_query": 0, "column": "jediTaskID"}]]}
        self.assertEqual(parse(raw, position=1).where[0].value, FromQuery(0, "jeditaskid"))
        # only an earlier query
        self.assertRejected("bad_shape", raw, position=0)
        self.assertRejected("bad_shape", {"tables": ["jobsActive4"], "where": [["jeditaskid", "=", {"from_query": 0, "column": "c"}]]}, position=1)
        self.assertRejected("bad_shape", {"tables": ["jobsActive4"], "where": [["jeditaskid", "in", {"from_query": 0}]]}, position=1)

    def test_returned(self) -> None:
        tables = ["jobsActive4"]
        self.assertRejected("bad_shape", {"tables": tables, "columns": ["pandaid"], "group_by": ["jobstatus"]})
        self.assertRejected("bad_shape", {"tables": tables, "columns": []})
        self.assertRejected("bad_shape", {"tables": tables, "columns": ["pandaid", "PandaID"]})
        self.assertRejected("bad_shape", {"tables": tables, "aggregates": [["min", "*"]]})
        self.assertRejected("bad_shape", {"tables": tables, "aggregates": [["sum", "pandaid"]]})
        self.assertRejected("bad_shape", {"tables": tables, "columns": ["pandaid"], "order_by": ["jobstatus"]})
        self.assertRejected("bad_shape", {"tables": tables, "group_by": ["jobstatus"], "order_by": ["pandaid"]})
        self.assertRejected("bad_shape", {"tables": tables, "order_by": [["pandaid", "down"]]})
        self.assertRejected("bad_shape", {"tables": tables, "limit": 1001})
        self.assertRejected("bad_shape", {"tables": tables, "limit": 0})
        self.assertRejected("bad_shape", {"tables": tables, "limit": True})


class TestCountTouchedPartitions(unittest.TestCase):
    def count(self, where: list[list[Any]], now: datetime.datetime | None = None) -> int:
        plans = check({"tables": ["jobsArchived4"], "where": [["pandaid", "=", 1]] + where})
        assert JOBS_ARCHIVED4.partitioning is not None
        return count_touched_partitions(JOBS_ARCHIVED4.partitioning, plans[0].where, now)

    def test_window(self) -> None:
        self.assertEqual(self.count([]), 5)
        # the partition [D0+1, D0+2) and the ones after it
        self.assertEqual(self.count([["modificationtime", ">=", "2026-10-02T00:00:00"]]), 3)
        self.assertEqual(self.count([["modificationtime", ">", "2026-10-02T00:00:00"]]), 3)
        # an upper bound at the start of a partition leaves it out only when exclusive
        self.assertEqual(self.count([["modificationtime", "<", "2026-10-02T00:00:00"]]), 2)
        self.assertEqual(self.count([["modificationtime", "<=", "2026-10-02T00:00:00"]]), 3)
        self.assertEqual(self.count([["modificationtime", ">=", "2026-10-02T06:00:00"], ["modificationtime", "<", "2026-10-02T18:00:00"]]), 1)
        # the narrower of two lower bounds wins
        self.assertEqual(self.count([["modificationtime", ">=", "2026-10-01T00:00:00"], ["modificationtime", ">=", "2026-10-04T00:00:00"]]), 1)
        # before the oldest bound, only the partition open below
        self.assertEqual(self.count([["modificationtime", "<", "2026-09-01T00:00:00"]]), 1)

    def test_points(self) -> None:
        self.assertEqual(self.count([["modificationtime", "=", "2026-10-02T12:00:00"]]), 1)
        self.assertEqual(self.count([["modificationtime", "in", ["2026-10-02T12:00:00", "2026-10-03T12:00:00", "2026-10-03T13:00:00"]]]), 2)
        # a point at a bound is in the partition that starts there
        self.assertEqual(self.count([["modificationtime", "in", ["2026-10-02T00:00:00"]]]), 1)
        # a point outside the window leaves nothing to read, and the count does not go below 1
        self.assertEqual(self.count([["modificationtime", "in", ["2026-10-02T12:00:00"]], ["modificationtime", ">=", "2026-10-03T00:00:00"]]), 1)

    def test_made_ahead(self) -> None:
        # on 2026-10-02, the partitions of 2026-10-03 and 2026-10-04 are made but hold no row
        now = D0 + DAY + datetime.timedelta(hours=12)
        self.assertEqual(self.count([], now), 3)
        self.assertEqual(self.count([["modificationtime", ">=", "2026-10-02T00:00:00"]], now), 1)
        # the partition holding now counts from its first second
        self.assertEqual(self.count([], D0 + DAY), 3)
        # a point ahead is in no partition counted, and the count does not go below 1
        self.assertEqual(self.count([["modificationtime", "=", "2026-10-04T12:00:00"]], now), 1)
        # a key that is not a time keeps every partition
        assert JEDI_TASKS.partitioning is not None
        plans = check({"tables": ["JEDI_Tasks"], "where": [["jeditaskid", ">=", 600000], ["status", "=", "running"]]})
        self.assertEqual(count_touched_partitions(JEDI_TASKS.partitioning, plans[0].where, now), 2)
        # and the cost of a query follows
        query = parse({"tables": ["jobsArchived4"], "where": [["pandaid", "in", [1, 2]]]}, 0)
        self.assertEqual(check_query(query, INFOS, {}, 1000)[0].probes, 10)
        self.assertEqual(check_query(query, INFOS, {}, 1000, now=now)[0].probes, 6)

    def test_numbers(self) -> None:
        assert JEDI_TASKS.partitioning is not None
        plans = check({"tables": ["JEDI_Tasks"], "where": [["jeditaskid", "in", [10, 499999, 500000]]]})
        self.assertEqual(count_touched_partitions(JEDI_TASKS.partitioning, plans[0].where), 2)
        plans = check({"tables": ["JEDI_Tasks"], "where": [["jeditaskid", ">=", 600000], ["status", "=", "running"]]})
        self.assertEqual(count_touched_partitions(JEDI_TASKS.partitioning, plans[0].where), 2)


class TestCheckQuery(unittest.TestCase):
    def assertRejected(self, rule: str, raw: dict[str, Any], **kwargs: Any) -> QueryRejected:
        with self.assertRaises(QueryRejected) as context:
            check(raw, **kwargs)
        self.assertEqual(context.exception.rule, rule, context.exception.message)
        return context.exception

    def test_job_tables(self) -> None:
        plans = check({"tables": ["jobsDefined4", "jobsActive4", "jobsArchived4"], "where": [["pandaid", "in", [1, 2, 3]]]})
        # a LOCAL index is probed in each of the five partitions
        self.assertEqual([plan.probes for plan in plans], [3, 3, 15])
        plans = check({"tables": ["jobsArchived4"], "where": [["pandaid", "in", [1, 2, 3]], ["modificationtime", ">=", "2026-10-03T00:00:00"]]})
        self.assertEqual(plans[0].probes, 6)

    def test_cheapest_index(self) -> None:
        plans = check({"tables": ["jobsActive4"], "where": [["jobstatus", "=", "running"], ["pandaid", "in", [1, 2]]]})
        self.assertEqual(plans[0].probes, 1)

    def test_partition_key_index(self) -> None:
        # each task is in one partition
        plans = check({"tables": ["JEDI_Tasks"], "where": [["jeditaskid", "in", [10, 600000, 1200000]]]})
        self.assertEqual(plans[0].probes, 3)
        # status is not the partition key, so its LOCAL index is probed in each partition
        plans = check({"tables": ["JEDI_Tasks"], "where": [["status", "=", "running"]]})
        self.assertEqual(plans[0].probes, 3)

    def test_budget(self) -> None:
        rejected = self.assertRejected("probe_budget", {"tables": ["jobsActive4", "jobsArchived4"], "where": [["pandaid", "in", [1, 2, 3]]]}, budget=17)
        self.assertEqual(rejected.detail, {"probes": {"jobsActive4": 3, "jobsArchived4": 15}, "budget": 17})
        check({"tables": ["jobsActive4", "jobsArchived4"], "where": [["pandaid", "in", [1, 2, 3]]]}, budget=18)

    def test_no_index_key(self) -> None:
        # a prefix is no key, though computingsite leads an index
        self.assertRejected("no_index_key", {"tables": ["jobsActive4"], "where": [["computingsite", "prefix", "CERN"]]})
        self.assertRejected("no_index_key", {"tables": ["jobsActive4"], "where": [["pandaid", ">", 1]]})
        # a window on the partition key alone is not enough
        self.assertRejected("no_index_key", {"tables": ["jobsArchived4"], "where": [["modificationtime", ">=", "2026-10-03T00:00:00"]]})
        # however small the table
        self.assertRejected("no_index_key", {"tables": ["CONFIG"], "where": [["component", "=", "jedi"], ["key", "=", "x"]]})
        self.assertRejected("no_index_key", {"tables": ["CONFIG"]})
        plans = check({"tables": ["CONFIG"], "where": [["app", "in", ["jedi", "pandaserver"]], ["component", "=", "jedi"], ["key", "=", "x"]]})
        self.assertEqual(plans[0].probes, 2)

    def test_values(self) -> None:
        # a text for a number column would make the database convert the column
        rejected = self.assertRejected("bad_value", {"tables": ["jobsActive4"], "where": [["pandaid", "=", "1"]]})
        self.assertEqual(rejected.detail, {"table": "jobsActive4", "column": "pandaid"})
        self.assertRejected("bad_value", {"tables": ["jobsActive4"], "where": [["jobstatus", "=", 1], ["pandaid", "=", 1]]})
        self.assertRejected("bad_value", {"tables": ["jobsActive4"], "where": [["pandaid", "=", 1], ["modificationtime", ">", "yesterday"]]})
        self.assertRejected("bad_value", {"tables": ["jobsActive4"], "where": [["pandaid", "prefix", "1"]]})
        self.assertRejected("bad_value", {"tables": ["jobParamsTable"], "where": [["pandaid", "=", 1], ["jobparameters", "prefix", "x"]]})
        plans = check({"tables": ["jobsActive4"], "where": [["pandaid", "=", 1], ["modificationtime", ">", "2026-10-01T02:00:00+02:00"]]})
        self.assertEqual(plans[0].where[1].value, datetime.datetime(2026, 10, 1, 0, 0))

    def test_columns(self) -> None:
        rejected = self.assertRejected("unknown_column", {"tables": ["jobsActive4"], "columns": ["nosuch"], "where": [["pandaid", "=", 1]]})
        self.assertEqual(rejected.detail, {"table": "jobsActive4", "column": "nosuch"})
        self.assertRejected("unknown_column", {"tables": ["jobsActive4"], "where": [["pandaid", "=", 1]], "order_by": ["nosuch"]})
        self.assertRejected("bad_value", {"tables": ["jobsActive4"], "where": [["pandaid", "=", 1]], "group_by": ["day(jobstatus)"]})
        self.assertRejected("bad_value", {"tables": ["jobParamsTable"], "where": [["pandaid", "=", 1]], "aggregates": [["max", "jobparameters"]]})

    def test_from_query(self) -> None:
        reference = FromQuery(0, "pandaid")
        raw = {"tables": ["jobsArchived4"], "where": [["pandaid", "in", {"from_query": 0, "column": "pandaid"}]]}
        plans = check(raw, from_sizes={reference: 4}, position=1)
        self.assertEqual(plans[0].probes, 20)
        rejected = self.assertRejected("too_many_keys", raw, from_sizes={reference: MAX_KEYS + 1}, position=1)
        self.assertEqual(rejected.detail, {"keys": MAX_KEYS + 1, "max_keys": MAX_KEYS})


class TestCompose(unittest.TestCase):
    def compose(self, raw: dict[str, Any], backend: str, from_values: dict[FromQuery, list[Any]] | None = None, position: int = 0) -> Any:
        query = parse(raw, position)
        from_values = from_values or {}
        plans = check_query(query, INFOS, {reference: len(values) for reference, values in from_values.items()}, 1000)
        return [compose_statement(plan, query, from_values, backend) for plan in plans]

    def test_oracle(self) -> None:
        (statement,) = self.compose(
            {
                "tables": ["jobsArchived4"],
                "where": [
                    ["pandaid", "in", [1, 2]],
                    ["modificationtime", ">=", "2026-10-01T00:00:00"],
                    ["jobstatus", "!=", "failed"],
                    ["computingsite", "is null"],
                ],
                "columns": ["pandaid", "jobstatus"],
                "order_by": [["pandaid", "desc"]],
                "limit": 100,
            },
            "oracle",
        )
        self.assertEqual(
            statement.sql,
            "SELECT pandaid,jobstatus FROM ATLAS_PANDA.jobsArchived4 "
            "WHERE pandaid IN (:r0,:r1) AND modificationtime >= :r2 AND jobstatus <> :r3 AND computingsite IS NULL "
            "ORDER BY pandaid DESC FETCH FIRST 101 ROWS ONLY" + STATEMENT_COMMENT,
        )
        self.assertEqual(statement.binds, {":r0": 1, ":r1": 2, ":r2": D0, ":r3": "failed"})
        self.assertEqual((statement.names, statement.limit), (("pandaid", "jobstatus"), 100))

    def test_grouped(self) -> None:
        raw = {
            "tables": ["jobsActive4"],
            "where": [["jeditaskid", "=", 7]],
            "group_by": ["jobstatus", "hour(modificationtime)"],
            "aggregates": [["count", "*"], ["min", "pandaid"]],
            "order_by": ["hour(modificationtime)"],
        }
        (oracle,) = self.compose(raw, "oracle")
        self.assertEqual(
            oracle.sql,
            "SELECT jobstatus,TRUNC(modificationtime,'HH24'),COUNT(*),MIN(pandaid) FROM ATLAS_PANDA.jobsActive4 WHERE jeditaskid = :r0 "
            "GROUP BY jobstatus,TRUNC(modificationtime,'HH24') ORDER BY TRUNC(modificationtime,'HH24') FETCH FIRST 1001 ROWS ONLY" + STATEMENT_COMMENT,
        )
        self.assertEqual(oracle.names, ("jobstatus", "hour(modificationtime)", "count(*)", "min(pandaid)"))
        (postgres,) = self.compose(raw, "postgres")
        self.assertIn("date_trunc('hour',modificationtime)", postgres.sql)
        self.assertTrue(postgres.sql.endswith(" LIMIT 1001" + STATEMENT_COMMENT))

    def test_postgres_conversion(self) -> None:
        # what WrappedCursor makes of the statement: one %s per bind variable, in order, and nothing else touched
        (statement,) = self.compose(
            {
                "tables": ["jobsActive4"],
                "where": [["pandaid", "in", [1, 2]], ["computingsite", "prefix", "CERN_1%"], ["modificationtime", "<", "2026-10-01T00:00:00"]],
                "group_by": ["day(modificationtime)"],
                "aggregates": [["count", "*"]],
            },
            "postgres",
        )
        sql, params = convert_query_in_printf_format(statement.sql, [statement.binds], {})
        # the comment comes through the conversion as it is, which a ":" or a "%" in it would not
        self.assertEqual(convert_query_in_printf_format("SELECT 1" + STATEMENT_COMMENT, [{}], {})[0], "SELECT 1" + STATEMENT_COMMENT)
        self.assertEqual(
            sql,
            "SELECT date_trunc('day',modificationtime),COUNT(*) FROM ATLAS_PANDA.jobsActive4 "
            "WHERE pandaid IN (%s,%s) AND computingsite LIKE %s ESCAPE '!' AND modificationtime < %s "
            "GROUP BY date_trunc('day',modificationtime) LIMIT 1001" + STATEMENT_COMMENT,
        )
        self.assertEqual(params, [[1, 2, "CERN!_1!%%", D0]])

    def test_default_columns_leave_out_large_objects(self) -> None:
        (statement,) = self.compose({"tables": ["jobParamsTable"], "where": [["pandaid", "=", 1]]}, "oracle")
        self.assertEqual(statement.names, ("pandaid", "modificationtime"))
        (statement,) = self.compose({"tables": ["jobParamsTable"], "where": [["pandaid", "=", 1]], "columns": ["jobparameters"]}, "oracle")
        self.assertEqual(statement.names, ("jobparameters",))

    def test_from_query(self) -> None:
        raw = {"tables": ["jobsActive4"], "where": [["pandaid", "in", {"from_query": 0, "column": "pandaid"}]]}
        (statement,) = self.compose(raw, "oracle", from_values={FromQuery(0, "pandaid"): [5, 6]}, position=1)
        self.assertEqual(statement.binds, {":r0": 5, ":r1": 6})
        # no values, so no row can match and there is nothing to run
        self.assertEqual(self.compose(raw, "oracle", from_values={FromQuery(0, "pandaid"): []}, position=1), [None])

    def test_escape_prefix(self) -> None:
        self.assertEqual(escape_prefix("a_b%c!"), "a!_b!%c!!%")


def oracle_date(day: str) -> str:
    return f"TO_DATE(' {day} 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"


class FakeDictionary:
    # answers the statements of schema_info by the view each reads from, the one after FROM
    def __init__(self, answers: dict[str, list[tuple[Any, ...]]]) -> None:
        self.answers = answers
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
        self.statements.append((sql, binds))
        for view, rows in self.answers.items():
            if f"FROM {view} " in sql:
                return rows
        return []


class TestSchemaInfo(unittest.TestCase):
    def setUp(self) -> None:
        schema_info._cache.clear()

    def test_oracle_time_partitions(self) -> None:
        run = FakeDictionary(
            {
                "ALL_TAB_COLUMNS": [
                    ("PANDAID", "NUMBER"),
                    ("MODIFICATIONTIME", "DATE"),
                    ("JOBSTATUS", "VARCHAR2"),
                    ("STARTTIME", "TIMESTAMP(6)"),
                    ("JOBPARAMETERS", "CLOB"),
                    ("ROWID_X", "RAW"),
                ],
                # the hidden column of an index on an expression
                "ALL_INDEXES": [("PANDAID", "LOCAL"), ("SYS_NC00034$", "LOCAL"), ("JOBSTATUS", None)],
                "ALL_PART_TABLES": [("RANGE",)],
                "ALL_PART_KEY_COLUMNS": [("MODIFICATIONTIME",)],
                "ALL_TAB_PARTITIONS": [(oracle_date("2012-08-01"),), (oracle_date("2026-10-02"),), ("TIMESTAMP' 2026-10-03 00:00:00'",)],
            }
        )
        info = read_table_info(run, "oracle", "atlas_panda", "jobsArchived4")
        self.assertEqual((info.owner, info.table_name), ("ATLAS_PANDA", "JOBSARCHIVED4"))
        self.assertEqual(
            info.columns,
            {
                "pandaid": KIND_NUMBER,
                "modificationtime": KIND_TIME,
                "jobstatus": KIND_TEXT,
                "starttime": KIND_TIME,
                "jobparameters": KIND_LOB,
                "rowid_x": KIND_OTHER,
            },
        )
        self.assertEqual(info.indexes, (IndexInfo("pandaid", True), IndexInfo("jobstatus", False)))
        self.assertEqual(
            info.partitioning,
            PartitionInfo(
                "modificationtime",
                (
                    (None, datetime.datetime(2012, 8, 1)),
                    (datetime.datetime(2012, 8, 1), datetime.datetime(2026, 10, 2)),
                    (datetime.datetime(2026, 10, 2), datetime.datetime(2026, 10, 3)),
                ),
            ),
        )
        self.assertEqual(run.statements[0][1], {":owner": "ATLAS_PANDA", ":table_name": "JOBSARCHIVED4"})

    def test_oracle_number_partitions(self) -> None:
        run = FakeDictionary(
            {
                "ALL_TAB_COLUMNS": [("JEDITASKID", "NUMBER"), ("STATUS", "VARCHAR2")],
                "ALL_INDEXES": [("JEDITASKID", "LOCAL")],
                "ALL_PART_TABLES": [("RANGE",)],
                "ALL_PART_KEY_COLUMNS": [("JEDITASKID",)],
                "ALL_TAB_PARTITIONS": [("500000",), ("1000000",), ("MAXVALUE",)],
            }
        )
        info = read_table_info(run, "oracle", "ATLAS_PANDA", "JEDI_Tasks")
        assert info.partitioning is not None
        self.assertEqual(info.partitioning.bounds, ((None, 500000), (500000, 1000000), (1000000, None)))

    def test_oracle_other_partitioning(self) -> None:
        # list partitions, whose bounds say nothing of a range
        run = FakeDictionary(
            {
                "ALL_TAB_COLUMNS": [("ID", "NUMBER"), ("VO", "VARCHAR2")],
                "ALL_PART_TABLES": [("LIST",)],
                "ALL_PART_KEY_COLUMNS": [("VO",)],
                "ALL_TAB_PARTITIONS": [("'atlas'",), ("'other'",)],
            }
        )
        info = read_table_info(run, "oracle", "ATLAS_PANDA", "T")
        self.assertEqual(info.partitioning, PartitionInfo("vo", ((None, None), (None, None))))

    def test_postgres(self) -> None:
        run = FakeDictionary(
            {
                "information_schema.columns": [
                    ("pandaid", "bigint"),
                    ("modificationtime", "timestamp without time zone"),
                    ("jobstatus", "character varying"),
                    ("metadata", "jsonb"),
                ],
                "pg_class": [("p", "RANGE (modificationtime)")],
                "pg_index": [("pandaid",)],
                "pg_inherits": [("FOR VALUES FROM ('2026-10-01 00:00:00') TO ('2026-10-02 00:00:00')",), ("DEFAULT",)],
            }
        )
        info = read_table_info(run, "postgres", "DOMA_PANDA", "jobsArchived4")
        self.assertEqual((info.owner, info.table_name), ("doma_panda", "jobsarchived4"))
        self.assertEqual(info.columns["metadata"], KIND_LOB)
        self.assertEqual(info.indexes, (IndexInfo("pandaid", True),))
        self.assertEqual(
            info.partitioning,
            PartitionInfo("modificationtime", ((datetime.datetime(2026, 10, 1), datetime.datetime(2026, 10, 2)), (None, None))),
        )
        # what WrappedCursor makes of the statements: the two bind variables and nothing else
        self.assertEqual(len(run.statements), 4)
        for sql, binds in run.statements:
            converted, params = convert_query_in_printf_format(sql, [binds], {})
            self.assertNotIn(":", converted)
            self.assertEqual(params, [["doma_panda", "jobsarchived4"]])

    def test_postgres_not_partitioned(self) -> None:
        run = FakeDictionary({"information_schema.columns": [("key", "character varying")], "pg_class": [("r", None)], "pg_index": [("key",)]})
        info = read_table_info(run, "postgres", "doma_panda", "config")
        self.assertEqual((info.indexes, info.partitioning), ((IndexInfo("key", False),), None))

    def test_not_visible(self) -> None:
        with self.assertRaises(TableNotVisible):
            read_table_info(FakeDictionary({}), "oracle", "ATLAS_PANDAARCH", "jobsArchived")

    def test_cache(self) -> None:
        run = FakeDictionary({"ALL_TAB_COLUMNS": [("ID", "NUMBER")]})
        with mock.patch.object(schema_info.time, "monotonic", return_value=1000.0):
            first = get_table_info(run, "oracle", "ATLAS_PANDA", "CONFIG")
            self.assertIs(get_table_info(run, "oracle", "atlas_panda", "config"), first)
        n_statements = len(run.statements)
        with mock.patch.object(schema_info.time, "monotonic", return_value=1000.0 + schema_info.CACHE_SECONDS):
            self.assertIsNot(get_table_info(run, "oracle", "ATLAS_PANDA", "CONFIG"), first)
        self.assertGreater(len(run.statements), n_statements)


if __name__ == "__main__":
    unittest.main()
