# Description: Unit tests for the reads of jobs, tasks and datasets by key. No database is used
import datetime
import unittest
from typing import Any
from unittest import mock

from pandaserver.taskbuffer import task_split_rules
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.db_proxy_mods import job_standalone_module
from pandaserver.taskbuffer.db_proxy_mods.job_standalone_module import (
    JobStandaloneModule,
)
from pandaserver.taskbuffer.db_proxy_mods.misc_standalone_module import (
    MiscStandaloneModule,
)
from pandaserver.taskbuffer.db_proxy_mods.task_utils_module import TaskUtilsModule
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

LOST = "ORA-03113: end-of-file on communication channel"


class TestPeekRaiseError(unittest.TestCase):
    def setUp(self) -> None:
        self.proxy = mock.MagicMock()
        self.proxy.cur.execute.side_effect = Exception(LOST)
        patcher = mock.patch.object(job_standalone_module.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def test_peek_job_raises_at_once(self) -> None:
        with self.assertRaisesRegex(Exception, "ORA-03113"):
            JobStandaloneModule.peekJob(self.proxy, 7000000000, True, True, True, True, forAnal=True, raise_error=True)
        self.assertEqual(self.proxy.cur.execute.call_count, 1)
        self.sleep.assert_not_called()

    def test_peek_job_as_before_without(self) -> None:
        self.assertIsNone(JobStandaloneModule.peekJob(self.proxy, 7000000000, True, True, True, True, forAnal=True))
        self.assertEqual(self.proxy.cur.execute.call_count, 3)
        job = JobStandaloneModule.peekJob(self.proxy, 7000000000, True, True, True, True)
        self.assertIsInstance(job, JobSpec)
        self.assertEqual(job.jobStatus, "unknown")  # type: ignore[union-attr]

    def test_peek_job_log(self) -> None:
        with self.assertRaisesRegex(Exception, "ORA-03113"):
            JobStandaloneModule.peekJobLog(self.proxy, 7000000000, 90, raise_error=True)
        self.assertIsNone(JobStandaloneModule.peekJobLog(self.proxy, 7000000000, 90))


class TestGetFullJobStatus(unittest.TestCase):
    """Job 1 is live, 2 is nowhere, 3 a live table cannot be read for, and 4 the archive cannot be read for."""

    def setUp(self) -> None:
        self.job = JobSpec()
        self.proxy = mock.MagicMock()

        def peek_job(job_id: int, *args: Any, raise_error: bool = False) -> JobSpec | None:
            if job_id == 3 and raise_error:
                raise Exception(LOST)
            return self.job if job_id == 1 else None

        def peek_job_log(job_id: int, days: int | None, raise_error: bool = False) -> JobSpec | None:
            if job_id == 4 and raise_error:
                raise Exception("ORA-01013: user requested cancel of current operation")
            return None

        self.proxy.peekJob.side_effect = peek_job
        self.proxy.peekJobLog.side_effect = peek_job_log
        self.task_buffer = TaskBuffer.__new__(TaskBuffer)
        self.task_buffer.proxyPool = mock.MagicMock()
        self.task_buffer.proxyPool.get.return_value.__enter__.return_value = self.proxy

    def test_errors_reported(self) -> None:
        jobs = self.task_buffer.getFullJobStatus([1, 2, 3, 4], report_errors=True)
        self.assertEqual(jobs, [self.job, None, {"error": "ORA-03113"}, {"error": "ORA-01013"}])
        # the job the live tables could not be read for is not looked for in the archive
        self.assertEqual([call.args[0] for call in self.proxy.peekJobLog.call_args_list], [2, 4])

    def test_errors_as_not_found_by_default(self) -> None:
        self.assertEqual(self.task_buffer.getFullJobStatus([1, 2, 3, 4]), [self.job, None, None, None])
        self.assertEqual({call.kwargs["raise_error"] for call in self.proxy.peekJob.call_args_list}, {False})

    def test_unexpected_error_raised_by_default(self) -> None:
        self.proxy.peekJob.side_effect = ValueError("broken")
        with self.assertRaises(ValueError):
            self.task_buffer.getFullJobStatus([1])


class TestGetDatasetsByName(unittest.TestCase):
    AS_OF = datetime.datetime(2026, 10, 14, 9, 0, 0)
    RETAINED_FROM = datetime.datetime(2026, 7, 1)

    def setUp(self) -> None:
        self.proxy = mock.MagicMock()
        self.proxy.get_db_utc_now.return_value = self.AS_OF
        self.proxy.get_partition_window.return_value = (5, self.RETAINED_FROM)
        self.proxy._commit.return_value = True

    def test_rows(self) -> None:
        columns = DatasetSpec.columnNames().split(",")
        row = tuple(f"value of {column}" for column in columns)
        self.proxy.cur.fetchall.return_value = [row]
        success, message, data = MiscStandaloneModule.get_datasets_by_name(self.proxy, ["a_sub1", "b_sub2"], 1000)
        self.assertTrue(success, message)
        self.assertEqual(data, {"rows": [dict(zip(columns, row))], "as_of": self.AS_OF, "retained_from": self.RETAINED_FROM, "partitions": 5, "budget": 1000})
        self.assertIn("creationdate", data["rows"][0])
        sql, var_map = self.proxy.cur.execute.call_args[0]
        self.assertIn("FROM ATLAS_PANDA.Datasets WHERE name IN (", sql)
        self.assertEqual(sorted(var_map.values()), ["a_sub1", "b_sub2"])

    def test_over_budget(self) -> None:
        success, message, data = MiscStandaloneModule.get_datasets_by_name(self.proxy, [f"a_sub{i}" for i in range(201)], 1000)
        self.assertFalse(success)
        self.assertEqual(data, {"rejected": "probe_budget", "keys": 201, "partitions": 5, "budget": 1000})
        self.proxy.cur.execute.assert_not_called()

    def test_database_error(self) -> None:
        self.proxy.cur.execute.side_effect = Exception("ORA-00942: table or view does not exist")
        success, message, data = MiscStandaloneModule.get_datasets_by_name(self.proxy, ["a_sub1"], 1000)
        self.assertEqual((success, data), (False, {"error": "ORA-00942"}))


class TestTaskDetailsSplitRule(unittest.TestCase):
    def _details(self, split_rule: str | None) -> dict[str, Any]:
        row: list[str | None] = [None] * len(JediTaskSpec.attributes)
        row[JediTaskSpec.attributes.index("splitRule")] = split_rule
        proxy = mock.MagicMock()
        proxy.cur.fetchone.side_effect = [tuple(row), (0, 0, 0, 0)]
        proxy.getClobObj.side_effect = [(True, [["template"]]), (True, [['{"cliParams": "prun"}']])]
        details: dict[str, Any] = TaskUtilsModule.get_task_details_json(proxy, 1)
        return details

    def test_raw_kept(self) -> None:
        raw = "CI=1,MF=1"
        details = self._details(raw)
        self.assertEqual(details["split_rule_raw"], raw)
        self.assertEqual(details["splitRule"], task_split_rules.decode_split_rule(raw))
        self.assertNotEqual(details["splitRule"], raw)

    def test_null(self) -> None:
        details = self._details(None)
        self.assertIsNone(details["split_rule_raw"])
        self.assertIsNone(details["splitRule"])


if __name__ == "__main__":
    unittest.main()
