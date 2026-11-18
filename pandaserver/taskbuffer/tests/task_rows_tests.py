# Description: Unit tests for the reads of the job and dataset rows of a task. No database is used
import datetime
import unittest
from typing import Any
from unittest import mock

from pandaserver.taskbuffer.db_proxy_mods.task_standalone_module import (
    TASK_JOB_ROW_COLUMNS,
    TaskStandaloneModule,
    ceil_to_day,
    floor_to_day,
)
from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec

AS_OF = datetime.datetime(2026, 11, 18, 9, 0, 0)
ARCHIVE_RETAINED_FROM = datetime.datetime(2024, 1, 1)


class TestDayBounds(unittest.TestCase):
    def test_floor(self) -> None:
        self.assertEqual(floor_to_day(datetime.datetime(2026, 11, 9, 23, 55, 1, 5)), datetime.datetime(2026, 11, 9))
        self.assertEqual(floor_to_day(datetime.datetime(2026, 11, 9)), datetime.datetime(2026, 11, 9))

    def test_ceil(self) -> None:
        self.assertEqual(ceil_to_day(datetime.datetime(2026, 11, 18, 0, 0, 0, 1)), datetime.datetime(2026, 11, 19))
        self.assertEqual(ceil_to_day(datetime.datetime(2026, 11, 18)), datetime.datetime(2026, 11, 18))


class TestGetTaskJobRows(unittest.TestCase):
    def setUp(self) -> None:
        self.proxy = mock.MagicMock()
        self.proxy.backend = "oracle"
        self.proxy.get_db_utc_now.return_value = AS_OF
        self.proxy._commit.return_value = True
        self.partitions = {"jobsArchived4": 3, "jobsArchived": 2}

        def partition_window(
            owner: str, table_name: str, since: datetime.datetime | None, until: datetime.datetime | None
        ) -> tuple[int, datetime.datetime | None]:
            return self.partitions[table_name], ARCHIVE_RETAINED_FROM if table_name == "jobsArchived" else None

        self.proxy.get_partition_window.side_effect = partition_window
        # the task was made 5 minutes after midnight, so the slack moves the window into the day before
        self.proxy.cur.fetchone.return_value = (datetime.datetime(2026, 11, 10, 0, 5, 0),)
        self.job_row = tuple(f"value of {column}" for column in TASK_JOB_ROW_COLUMNS)
        self.proxy.cur.fetchall.side_effect = [[self.job_row], [("finished", 7), ("running", 2)]]

    def statements(self) -> list[tuple[str, dict[str, Any]]]:
        return [call.args for call in self.proxy.cur.execute.call_args_list]

    def test_window_from_task_creation(self) -> None:
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertTrue(success, message)
        since, until = datetime.datetime(2026, 11, 9), datetime.datetime(2026, 11, 19)
        self.assertEqual(
            data,
            {
                "rows": [dict(zip(TASK_JOB_ROW_COLUMNS, self.job_row))],
                "truncated": False,
                "max_rows": 1000,
                "counts": {"finished": 7, "running": 2},
                "as_of": AS_OF,
                "since": since,
                "until": until,
                "retained_from": ARCHIVE_RETAINED_FROM,
                "partitions": 5,
                "probes": 2 * (2 + 5),
                "budget": 1000,
            },
        )
        self.assertEqual(
            [call.args for call in self.proxy.get_partition_window.call_args_list],
            [("ATLAS_PANDA", "jobsArchived4", since, until), ("ATLAS_PANDAARCH", "jobsArchived", since, until)],
        )
        (task_sql, task_vars), (rows_sql, rows_vars), (counts_sql, counts_vars) = self.statements()
        self.assertIn("SELECT creationDate FROM ATLAS_PANDA.JEDI_Tasks WHERE jediTaskID=:jediTaskID", task_sql)
        self.assertEqual(rows_vars, {":jediTaskID": 123, ":since": since, ":until": until})
        self.assertIs(counts_vars, rows_vars)
        for sql in (rows_sql, counts_sql):
            for table in ("ATLAS_PANDA.jobsDefined4", "ATLAS_PANDA.jobsActive4", "ATLAS_PANDA.jobsArchived4", "ATLAS_PANDAARCH.jobsArchived"):
                self.assertIn(f"FROM {table} WHERE jediTaskID=:jediTaskID", sql)
            self.assertEqual(sql.count("UNION "), 3)
            self.assertNotIn("UNION ALL", sql)
            # the window is put on the two partitioned tables only
            self.assertEqual(sql.count("modificationTime>=:since"), 2)
            self.assertEqual(sql.count("modificationTime<:until"), 2)
        self.assertIn("ORDER BY PandaID FETCH FIRST 1001 ROWS ONLY", rows_sql)
        self.assertIn("SELECT jobStatus,COUNT(*) FROM (SELECT PandaID,jobStatus FROM", counts_sql)
        self.assertIn("GROUP BY jobStatus", counts_sql)

    def test_since_given(self) -> None:
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, datetime.datetime(2026, 11, 12, 8, 0, 0), 1000, 1000)
        self.assertTrue(success, message)
        self.assertEqual(data["since"], datetime.datetime(2026, 11, 12))
        # the task is not read when since is given
        self.assertEqual(len(self.statements()), 2)
        self.assertNotIn("JEDI_Tasks", self.statements()[0][0])

    def test_task_not_found(self) -> None:
        self.proxy.cur.fetchone.return_value = None
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertTrue(success, message)
        self.assertIsNone(data["since"])
        self.assertIsNone(self.proxy.get_partition_window.call_args_list[0].args[2])
        rows_sql, rows_vars = self.statements()[1]
        self.assertNotIn(":since", rows_vars)
        self.assertNotIn("modificationTime>=", rows_sql)
        self.assertEqual(rows_sql.count("modificationTime<:until"), 2)

    def test_until_near_midnight(self) -> None:
        self.proxy.get_db_utc_now.return_value = datetime.datetime(2026, 11, 18, 23, 55, 0)
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertTrue(success, message)
        self.assertEqual(data["until"], datetime.datetime(2026, 11, 20))

    def test_no_partition_counted_as_one(self) -> None:
        self.partitions = {"jobsArchived4": 0, "jobsArchived": 0}
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertTrue(success, message)
        self.assertEqual((data["partitions"], data["probes"]), (2, 8))

    def test_over_budget(self) -> None:
        self.partitions = {"jobsArchived4": 300, "jobsArchived": 300}
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertFalse(success)
        self.assertEqual(data, {"rejected": "probe_budget", "probes": 2 * (2 + 600), "partitions": 600, "budget": 1000})
        # only the task was read
        self.assertEqual(len(self.statements()), 1)
        self.proxy._commit.assert_called_once()

    def test_truncated(self) -> None:
        rows = [tuple([panda_id] + [None] * (len(TASK_JOB_ROW_COLUMNS) - 1)) for panda_id in (1, 2, 3)]
        self.proxy.cur.fetchall.side_effect = [rows, [("finished", 3)]]
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 2)
        self.assertTrue(success, message)
        self.assertTrue(data["truncated"])
        self.assertEqual([row["PandaID"] for row in data["rows"]], [1, 2])
        self.assertEqual(data["counts"], {"finished": 3})
        self.assertIn("FETCH FIRST 3 ROWS ONLY", self.statements()[1][0])

    def test_postgres_limit(self) -> None:
        self.proxy.backend = "postgres"
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertTrue(success, message)
        rows_sql = self.statements()[1][0]
        self.assertIn("ORDER BY PandaID LIMIT 1001", rows_sql)
        self.assertNotIn("FETCH FIRST", rows_sql)

    def test_database_error(self) -> None:
        self.proxy.cur.fetchall.side_effect = Exception("ORA-01013: user requested cancel of current operation")
        success, message, data = TaskStandaloneModule.get_task_job_rows(self.proxy, 123, None, 1000, 1000)
        self.assertEqual((success, data), (False, {"error": "ORA-01013"}))
        self.proxy._rollback.assert_called_once()


class TestGetTaskDatasetRows(unittest.TestCase):
    def setUp(self) -> None:
        self.proxy = mock.MagicMock()
        self.proxy.backend = "oracle"
        self.proxy.get_db_utc_now.return_value = AS_OF
        self.proxy._commit.return_value = True
        self.columns = JediDatasetSpec.columnNames().split(",")

    def test_rows(self) -> None:
        row = tuple(f"value of {column}" for column in self.columns)
        self.proxy.cur.fetchall.return_value = [row]
        success, message, data = TaskStandaloneModule.get_task_dataset_rows(self.proxy, 123, 1000)
        self.assertTrue(success, message)
        self.assertEqual(data, {"rows": [dict(zip(self.columns, row))], "truncated": False, "max_rows": 1000, "as_of": AS_OF})
        for column in ("jediTaskID", "datasetID", "masterID", "type", "status", "nFiles"):
            self.assertIn(column, data["rows"][0])
        sql, var_map = self.proxy.cur.execute.call_args.args
        self.assertIn("FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:jediTaskID ORDER BY datasetID FETCH FIRST 1001 ROWS ONLY", sql)
        self.assertEqual(var_map, {":jediTaskID": 123})

    def test_truncated(self) -> None:
        self.proxy.cur.fetchall.return_value = [tuple([dataset_id] + [None] * (len(self.columns) - 1)) for dataset_id in (11, 12, 13)]
        success, message, data = TaskStandaloneModule.get_task_dataset_rows(self.proxy, 123, 2)
        self.assertTrue(success, message)
        self.assertTrue(data["truncated"])
        self.assertEqual([row["jediTaskID"] for row in data["rows"]], [11, 12])

    def test_database_error(self) -> None:
        self.proxy.cur.execute.side_effect = Exception("ORA-00942: table or view does not exist")
        success, message, data = TaskStandaloneModule.get_task_dataset_rows(self.proxy, 123, 1000)
        self.assertEqual((success, data), (False, {"error": "ORA-00942"}))


if __name__ == "__main__":
    unittest.main()
