# Description: Unit tests for the read of the worker rows of a job. No database is used
import datetime
import unittest
from typing import Any
from unittest import mock

from pandaserver.taskbuffer.db_proxy_mods.worker_module import (
    JOB_WORKER_ROW_COLUMNS,
    WorkerModule,
)
from pandaserver.taskbuffer.WorkerSpec import WorkerSpec
from pandaserver.taskbuffer.WrappedCursor import convert_query_in_printf_format

AS_OF = datetime.datetime(2026, 11, 22, 9, 0, 0)
PILOT_COLUMNS = ("pilotStartTime", "pilotEndTime", "pilotStatus", "pilotStatusSyncTime")


class TestGetJobWorkerRows(unittest.TestCase):
    def setUp(self) -> None:
        self.proxy = mock.MagicMock()
        self.proxy.backend = "oracle"
        self.proxy.get_db_utc_now.return_value = AS_OF
        self.proxy._commit.return_value = True

    def worker_rows(self, *worker_ids: int) -> list[tuple[Any, ...]]:
        index = JOB_WORKER_ROW_COLUMNS.index("workerID")
        return [tuple(worker_id if i == index else None for i in range(len(JOB_WORKER_ROW_COLUMNS))) for worker_id in worker_ids]

    def test_columns(self) -> None:
        # every column of Harvester_Workers: those of WorkerSpec, then the ones the pilot sets
        self.assertEqual(JOB_WORKER_ROW_COLUMNS, WorkerSpec._attributes + PILOT_COLUMNS)
        self.assertEqual(len(JOB_WORKER_ROW_COLUMNS), 31)
        self.assertEqual(len(set(JOB_WORKER_ROW_COLUMNS)), 31)

    def test_rows(self) -> None:
        row = tuple(f"value of {column}" for column in JOB_WORKER_ROW_COLUMNS)
        self.proxy.cur.fetchall.return_value = [row]
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 1000)
        self.assertTrue(success, message)
        self.assertEqual(data, {"rows": [dict(zip(JOB_WORKER_ROW_COLUMNS, row))], "truncated": False, "max_rows": 1000, "as_of": AS_OF})
        for column in ("harvesterID", "workerID", "status", "submitTime", "startTime", "endTime", "diagMessage") + PILOT_COLUMNS:
            self.assertEqual(data["rows"][0][column], f"value of {column}")
        sql, var_map = self.proxy.cur.execute.call_args.args
        self.assertIn("SELECT w.harvesterID,w.workerID,w.batchID,", sql)
        self.assertIn(",w.minRamCount,w.pilotStartTime,w.pilotEndTime,w.pilotStatus,w.pilotStatusSyncTime FROM ", sql)
        self.assertIn(
            "FROM ATLAS_PANDA.Harvester_Workers w, ATLAS_PANDA.Harvester_Rel_Jobs_Workers r "
            "WHERE w.harvesterID=r.harvesterID AND w.workerID=r.workerID AND r.PandaID=:PandaID "
            "ORDER BY w.harvesterID, w.workerID FETCH FIRST 1001 ROWS ONLY",
            sql,
        )
        self.assertIn("/* DBProxy.get_job_worker_rows */", sql)
        self.assertEqual(var_map, {":PandaID": 7000000001})
        # the database time is taken in the transaction, before the rows are read
        self.proxy.conn.begin.assert_called_once()
        self.proxy._commit.assert_called_once()

    def test_no_worker(self) -> None:
        self.proxy.cur.fetchall.return_value = []
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 1000)
        self.assertTrue(success, message)
        self.assertEqual(data, {"rows": [], "truncated": False, "max_rows": 1000, "as_of": AS_OF})

    def test_truncated(self) -> None:
        self.proxy.cur.fetchall.return_value = self.worker_rows(11, 12, 13)
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 2)
        self.assertTrue(success, message)
        self.assertTrue(data["truncated"])
        self.assertEqual([row["workerID"] for row in data["rows"]], [11, 12])
        sql, _ = self.proxy.cur.execute.call_args.args
        self.assertIn("FETCH FIRST 3 ROWS ONLY", sql)

    def test_not_truncated_at_max_rows(self) -> None:
        self.proxy.cur.fetchall.return_value = self.worker_rows(11, 12)
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 2)
        self.assertTrue(success, message)
        self.assertFalse(data["truncated"])
        self.assertEqual([row["workerID"] for row in data["rows"]], [11, 12])

    def test_postgres(self) -> None:
        # what WrappedCursor makes of the statement on Postgres: the bind in place, and FETCH FIRST left as it is
        self.proxy.backend = "postgres"
        self.proxy.cur.fetchall.return_value = []
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 1000)
        self.assertTrue(success, message)
        statement, var_map = self.proxy.cur.execute.call_args.args
        sql, params = convert_query_in_printf_format(statement, [var_map], {})
        self.assertIn("AND r.PandaID=%s ORDER BY w.harvesterID, w.workerID FETCH FIRST 1001 ROWS ONLY", sql)
        self.assertEqual(params, [[7000000001]])

    def test_database_error(self) -> None:
        self.proxy.cur.execute.side_effect = Exception("ORA-00942: table or view does not exist")
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 1000)
        self.assertEqual((success, data), (False, {"error": "ORA-00942"}))
        self.assertIn("ORA-00942", message)
        self.proxy._rollback.assert_called_once()

    def test_commit_error(self) -> None:
        self.proxy.cur.fetchall.return_value = []
        self.proxy._commit.return_value = False
        success, message, data = WorkerModule.get_job_worker_rows(self.proxy, 7000000001, 1000)
        self.assertFalse(success)
        self.assertIn("error", data)
        self.proxy._rollback.assert_called_once()


if __name__ == "__main__":
    unittest.main()
