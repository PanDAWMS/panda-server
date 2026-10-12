# Description: Unit tests for the handler of the async db_read requests: the batch, its deadline
# and the result it stores. The read-only connection is a fake, and no database is used
import datetime
import decimal
import json
import time
import unittest
from collections.abc import Callable, Generator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest import mock

from pandaserver.asyncprocess import db_read_handlers
from pandaserver.asyncprocess.db_read_handlers import _Batch, batch_deadline, encode_value
from pandaserver.asyncprocess.tests.db_read_tests import INFOS
from pandaserver.config import daemon_config, panda_config

AS_OF = datetime.datetime(2026, 10, 12, 12, 0, 0)
D0 = datetime.datetime(2026, 10, 1)

Answer = Callable[[str, dict[str, Any]], list[tuple[Any, ...]]]


class FakeCursor:
    def __init__(self, answer: Answer) -> None:
        self.answer = answer
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.arraysize = 0
        self.rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, binds: dict[str, Any] | None = None) -> None:
        self.executed.append((sql, dict(binds or {})))
        self.rows = self.answer(sql, binds or {})

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class FakeProxy:
    # what _Batch uses of a DBProxy of the read-only account
    backend = "oracle"

    def __init__(self, answer: Answer) -> None:
        self.call_timeout_ms = 30000
        self.cur = FakeCursor(answer)
        self.conn = mock.Mock()
        self.timeouts: list[int | None] = []
        self.reconnects = 0

    def set_call_timeout(self, timeout_ms: int | None) -> None:
        self.timeouts.append(timeout_ms)

    def get_db_utc_now(self) -> datetime.datetime:
        return AS_OF

    def connect(self, reconnect: bool = False) -> bool:
        self.reconnects += 1
        return True


TABLE_ROWS: dict[str, list[tuple[Any, ...]]] = {
    "jobsActive4": [(1, 10, D0), (2, 10, D0 + datetime.timedelta(days=1)), (3, 11, D0)],
    "JEDI_Tasks": [(10, "running"), (11, "done")],
}


def table_answer(sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
    # the rows of the table a statement reads, whatever its conditions
    for table, rows in TABLE_ROWS.items():
        if f".{table} " in sql:
            return rows
    return []


Q_JOBS = {"tables": ["jobsActive4"], "where": [["pandaid", "in", [1, 2, 3]]], "columns": ["pandaid", "jeditaskid", "modificationtime"]}
Q_TASKS = {"tables": ["JEDI_Tasks"], "where": [["jeditaskid", "in", {"from_query": 0, "column": "jeditaskid"}]], "columns": ["jeditaskid", "status"]}


class BatchTestBase(unittest.TestCase):
    def setUp(self) -> None:
        patch = mock.patch.object(db_read_handlers, "get_table_info", side_effect=lambda run, backend, owner, table: INFOS[table])
        patch.start()
        self.addCleanup(patch.stop)

    def run_batch(
        self, queries: list[Any], answer: Answer = table_answer, seconds: float = 300, **batch_attrs: Any
    ) -> tuple[dict[str, Any], FakeProxy, _Batch]:
        proxy = FakeProxy(answer)
        batch = _Batch(proxy, time.monotonic() + seconds, 1000, mock.Mock())  # type: ignore[arg-type]
        for name, value in batch_attrs.items():
            setattr(batch, name, value)
        return batch.run(queries), proxy, batch


class TestBatch(BatchTestBase):
    def test_from_query(self) -> None:
        data, proxy, _ = self.run_batch([Q_JOBS, Q_TASKS])
        self.assertEqual(data["as_of"], {"__datetime__": "2026-10-12T12:00:00"})
        jobs, tasks = data["answers"]
        self.assertEqual((jobs["status"], jobs["truncated"]), ("done", False))
        self.assertEqual(jobs["tables"][0]["rows"][1], {"pandaid": 2, "jeditaskid": 10, "modificationtime": {"__datetime__": "2026-10-02T00:00:00"}})
        self.assertEqual((jobs["tables"][0]["probes"], jobs["tables"][0]["table"]), (3, "jobsActive4"))
        self.assertEqual(tasks["tables"][0]["rows"], [{"jeditaskid": 10, "status": "running"}, {"jeditaskid": 11, "status": "done"}])
        # the read-only transaction comes first, and the values of the in are taken each once
        self.assertTrue(proxy.cur.executed[0][0].startswith("SET TRANSACTION READ ONLY"))
        self.assertEqual(proxy.cur.executed[-1][1], {":r0": 10, ":r1": 11})
        # each round trip after the first statement, as_of and the two queries, is limited to the time
        # left, which DBProxy.set_call_timeout cuts down to the connection's own limit; the end puts that back
        self.assertEqual(len(proxy.timeouts), 4)
        self.assertTrue(all(timeout is not None and 0 < timeout <= 300000 for timeout in proxy.timeouts[:-1]))
        self.assertIsNone(proxy.timeouts[-1])
        self.assertGreaterEqual(proxy.conn.rollback.call_count, 2)

    def test_truncation_reaches_what_takes_values_from_it(self) -> None:
        data, _, _ = self.run_batch([dict(Q_JOBS, limit=2), Q_TASKS])
        jobs, tasks = data["answers"]
        self.assertEqual((jobs["truncated"], jobs["tables"][0]["truncated"], len(jobs["tables"][0]["rows"])), (True, True, 2))
        # its own table was not cut, but its keys were
        self.assertEqual((tasks["truncated"], tasks["tables"][0]["truncated"]), (True, False))

    def test_rejected_query_does_not_stop_the_batch(self) -> None:
        data, _, _ = self.run_batch([{"tables": ["users"]}, Q_JOBS, Q_TASKS])
        rejected, jobs, tasks = data["answers"]
        self.assertEqual((rejected["status"], rejected["rule"]), ("rejected", "unknown_table"))
        self.assertEqual(jobs["status"], "done")
        # query 0 has no rows to take values from
        self.assertEqual((tasks["status"], tasks["rule"]), ("rejected", "from_query"))

    def test_from_query_column_must_be_returned(self) -> None:
        q_tasks = dict(Q_TASKS, where=[["jeditaskid", "in", {"from_query": 0, "column": "jobstatus"}]])
        data, _, _ = self.run_batch([Q_JOBS, q_tasks])
        self.assertEqual(data["answers"][1]["rule"], "from_query")

    def test_database_error_stops_the_batch(self) -> None:
        def answer(sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
            if ".jobsActive4 " in sql:
                raise RuntimeError("ORA-00942: table or view does not exist")
            return table_answer(sql, binds)

        data, proxy, _ = self.run_batch([Q_JOBS, Q_TASKS], answer=answer)
        failed, after = data["answers"]
        self.assertEqual((failed["status"], failed["code"]), ("error", "ORA-00942"))
        self.assertEqual((after["status"], after["code"]), ("error", "not_run"))
        self.assertEqual(proxy.reconnects, 0)

    def test_deadline(self) -> None:
        batch_box: list[_Batch] = []

        def answer(sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
            # the time runs out while the first query runs
            if ".jobsActive4 " in sql:
                batch_box[0].deadline = time.monotonic() - 1
            return table_answer(sql, binds)

        proxy = FakeProxy(answer)
        batch = _Batch(proxy, time.monotonic() + 300, 1000, mock.Mock())  # type: ignore[arg-type]
        batch_box.append(batch)
        data = batch.run([Q_JOBS, Q_TASKS, Q_JOBS])
        self.assertEqual([answer["status"] for answer in data["answers"]], ["done", "deadline", "deadline"])

    def test_call_timeout(self) -> None:
        def answer(sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
            if ".jobsActive4 " in sql:
                raise RuntimeError("DPI-1067: call timeout of 30000 ms exceeded with ORA-3156")
            return table_answer(sql, binds)

        # the connection's own limit ran out: the query is too heavy
        data, proxy, _ = self.run_batch([Q_JOBS, Q_TASKS], answer=answer)
        self.assertEqual([(a["status"], a.get("code")) for a in data["answers"]], [("error", "DPI-1067"), ("error", "not_run")])
        self.assertEqual(proxy.reconnects, 1)
        # the time left ran out first: the batch had no time for it
        data, proxy, _ = self.run_batch([Q_JOBS, Q_TASKS], answer=answer, seconds=5)
        self.assertEqual([a["status"] for a in data["answers"]], ["deadline", "deadline"])
        self.assertEqual(proxy.reconnects, 1)

    def test_result_size(self) -> None:
        # room for the statement and one row
        sql_bytes = 200
        data, _, _ = self.run_batch([Q_JOBS, Q_JOBS], bytes_left=sql_bytes + 100)
        jobs, after = data["answers"]
        self.assertEqual((jobs["status"], jobs["truncated"], len(jobs["tables"][0]["rows"])), ("done", True, 1))
        self.assertEqual((after["status"], after["rule"]), ("rejected", "result_size"))

    def test_encode_value(self) -> None:
        self.assertEqual(encode_value(D0), {"__datetime__": "2026-10-01T00:00:00"})
        self.assertEqual(encode_value(datetime.date(2026, 10, 1)), {"__datetime__": "2026-10-01"})
        self.assertEqual((encode_value(decimal.Decimal("12")), encode_value(decimal.Decimal("1.5"))), (12, 1.5))
        self.assertEqual(encode_value(b"\x01\xff"), "01ff")
        self.assertEqual(encode_value(SimpleNamespace(read=lambda: "large text")), "large text")
        self.assertEqual(json.dumps(encode_value(None)), "null")


class TestDeadline(unittest.TestCase):
    def test_batch_deadline(self) -> None:
        # the cycle started 100 s before the request, and its timeout comes first
        self.assertEqual(batch_deadline(1000.0, 900.0, 300, 480), 900.0 + 300 - 30)
        self.assertEqual(batch_deadline(1000.0, None, 300, 480), 1000.0 + 300 - 30)
        # a long timeout leaves the age at which a running request is handed out again
        self.assertEqual(batch_deadline(1000.0, 900.0, 3600, 480), 1000.0 + 480 - 30)

    def test_daemon_timeout(self) -> None:
        self.assertEqual(daemon_config.get_timeout({"period": 10}), 30)
        self.assertEqual(daemon_config.get_timeout({"period": 2400}), 6000)
        self.assertEqual(daemon_config.get_timeout({"period": 10, "timeout": 300}), 300)

    def test_find_daemon(self) -> None:
        config = {
            "old": {"module": "async_request_daemon", "enable": False, "period": 5},
            "async_request_daemon": {"enable": True, "period": 10, "timeout": 300},
        }
        for value in (config, json.dumps(config)):
            with mock.patch.object(daemon_config, "config", value, create=True):
                self.assertEqual(daemon_config.find_daemon("async_request_daemon"), config["async_request_daemon"])
                self.assertIsNone(daemon_config.find_daemon("nosuch"))


class TestRun(BatchTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.proxy = FakeProxy(table_answer)

        @contextmanager
        def get() -> Generator[FakeProxy, None, None]:
            yield self.proxy

        patches: list[Any] = [
            mock.patch.object(db_read_handlers, "_get_reader", return_value=SimpleNamespace(proxyPool=SimpleNamespace(get=get))),
            mock.patch.object(daemon_config, "find_daemon", return_value={"period": 10, "timeout": 300}),
            mock.patch.object(panda_config, "backend", "oracle"),
            mock.patch.object(panda_config, "db_read_probe_budget", 1000, create=True),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def test_answers(self) -> None:
        logger = mock.Mock()
        success, message, data = db_read_handlers._run({"queries": [Q_JOBS], "requester": "alice"}, time.monotonic(), logger)
        self.assertTrue(success, message)
        assert data is not None
        self.assertEqual(data["answers"][0]["status"], "done")
        # one line saying who read what, at what cost and how fast
        line = logger.info.call_args[0][0]
        self.assertIn("db_read by alice", line)
        self.assertIn('"probes": 3', line)

    def test_refusals(self) -> None:
        logger = mock.Mock()
        self.assertFalse(db_read_handlers._run({"queries": []}, time.monotonic(), logger)[0])
        self.assertFalse(db_read_handlers._run({"queries": [Q_JOBS] * 11}, time.monotonic(), logger)[0])
        # the default timeout of a daemon running every 10 s leaves nothing after the margin
        with mock.patch.object(daemon_config, "find_daemon", return_value={"period": 10}):
            success, message, _ = db_read_handlers._run({"queries": [Q_JOBS]}, time.monotonic(), logger)
        self.assertFalse(success)
        self.assertIn("no time left", message)
        with mock.patch.object(daemon_config, "find_daemon", return_value=None):
            self.assertFalse(db_read_handlers._run({"queries": [Q_JOBS]}, time.monotonic(), logger)[0])


class TestReader(unittest.TestCase):
    def test_not_configured(self) -> None:
        with mock.patch.object(panda_config, "dbuser_reader", None, create=True):
            with self.assertRaises(db_read_handlers.ReaderUnavailable):
                db_read_handlers._get_reader()

    def test_connected_once_with_the_reader_options(self) -> None:
        task_buffer_class = mock.Mock()
        with (
            mock.patch.object(db_read_handlers, "_reader", None),
            mock.patch.object(panda_config, "dbuser_reader", "ATLAS_PANDA_READER", create=True),
            mock.patch.object(panda_config, "dbpasswd_reader", "secret", create=True),
            mock.patch.object(panda_config, "dbhost_reader", None, create=True),
            mock.patch.object(panda_config, "dbhost", "ADCR", create=True),
            mock.patch.object(panda_config, "db_read_call_timeout", 30, create=True),
            mock.patch("pandaserver.taskbuffer.TaskBuffer.TaskBuffer", task_buffer_class),
        ):
            first = db_read_handlers._get_reader()
            self.assertIs(db_read_handlers._get_reader(), first)
        task_buffer_class.return_value.init.assert_called_once_with(
            "ADCR",
            "secret",
            nDBConnection=1,
            useTimeout=False,
            requester="db_read",
            dbuser="ATLAS_PANDA_READER",
            call_timeout_ms=30000,
            max_connect_attempts=1,
        )


class TestHandler(unittest.TestCase):
    ROW = {"request_id": "some-uuid", "parameters": json.dumps({"queries": [Q_JOBS], "requester": "alice"})}

    def test_result_is_stored(self) -> None:
        task_buffer = mock.Mock()
        with mock.patch.object(db_read_handlers, "_run", return_value=(False, "no time left", None)):
            db_read_handlers._handle_db_read(self.ROW, task_buffer, mock.Mock(), "any")
        task_buffer.finish_async_result.assert_called_once_with(
            "some-uuid", "any", "done", result=json.dumps({"success": False, "message": "no time left", "data": None})
        )

    def test_crash_is_stored_as_failed(self) -> None:
        task_buffer = mock.Mock()
        with mock.patch.object(db_read_handlers, "_run", side_effect=KeyError("boom")):
            db_read_handlers._handle_db_read(self.ROW, task_buffer, mock.Mock(), "any")
        args, kwargs = task_buffer.finish_async_result.call_args
        self.assertEqual(args, ("some-uuid", "any", "failed"))
        self.assertFalse(kwargs["retriable"])
        self.assertIn("KeyError", kwargs["error_msg"])


if __name__ == "__main__":
    unittest.main()
