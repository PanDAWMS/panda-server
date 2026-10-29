# Description: Unit tests for the Job API methods
import datetime
import os
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from pandaserver.api.v1 import job_api
from pandaserver.api.v1.http_client import HttpClient, api_url_ssl
from pandaserver.srvcore.panda_request import PandaRequest

# To run tests with real IDs, set environment variables:
#   PANDA_JOB_ID_TEST=<job_id>  JEDI_TASK_ID_TEST=<task_id>
PANDA_JOB_ID = int(os.environ.get("PANDA_JOB_ID_TEST", -1))
JEDI_TASK_ID = int(os.environ.get("JEDI_TASK_ID_TEST", -1))


class TestJobAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.http_client = HttpClient()

    def test_get_status(self) -> None:
        # def get_status(req: PandaRequest, job_ids: List[int], timeout: int = 60) -> Dict:
        url = f"{api_url_ssl}/job/get_status"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless of whether job exists (returns empty or None entries)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_description(self) -> None:
        # def get_description(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/get_description"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless (returns [None] for unknown job IDs)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_description_incl_archive(self) -> None:
        # def get_description_incl_archive(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/get_description_incl_archive"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless (returns [None] for unknown job IDs)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_status_history(self) -> None:
        # def get_status_history(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/get_status_history"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertTrue(output["success"])
        self.assertEqual(set(output["data"]), {"rows", "as_of", "retained_from", "since", "until", "partitions", "budget", "record_statuschange"})
        # an unknown job has no rows rather than failing the call
        if PANDA_JOB_ID == -1:
            self.assertEqual(output["data"]["rows"], [])
        for row in output["data"]["rows"]:
            self.assertEqual(row["PandaID"], PANDA_JOB_ID)

    def test_get_status_history_window(self) -> None:
        # a window of a few days touches a few daily partitions, and comes back widened to whole UTC days
        url = f"{api_url_ssl}/job/get_status_history"
        print(f"Testing URL: {url}")
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        since = now - datetime.timedelta(days=3)
        data = {"job_ids": [PANDA_JOB_ID], "since": since.strftime("%Y-%m-%d %H:%M:%S"), "until": now.strftime("%Y-%m-%d %H:%M:%S")}
        status, output = self.http_client.get(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertTrue(output["success"])
        # both ends move out by 10 minutes of clock slack, then to whole UTC days
        slack = datetime.timedelta(minutes=10)
        self.assertEqual(output["data"]["since"], {"__datetime__": (since - slack).strftime("%Y-%m-%dT00:00:00")})
        self.assertEqual(output["data"]["until"], {"__datetime__": (now + slack + datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")})
        self.assertLessEqual(output["data"]["partitions"], 6)

    def test_get_status_history_invalid_window(self) -> None:
        url = f"{api_url_ssl}/job/get_status_history"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID], "since": "2026-10-12 00:00:00", "until": "2026-10-11 00:00:00"}
        status, output = self.http_client.get(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertFalse(output["success"])
        self.assertEqual(output["data"], {"rejected": "invalid_window"})

    def test_get_status_history_over_budget(self) -> None:
        # 200 jobs x ~93 daily partitions exceed the default db_read_probe_budget of 1000, so the
        # call is refused as a whole instead of being answered for the first few jobs
        url = f"{api_url_ssl}/job/get_status_history"
        print(f"Testing URL: {url}")
        data = {"job_ids": list(range(1, 201))}
        status, output = self.http_client.get(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertFalse(output["success"])
        self.assertEqual(output["data"]["rejected"], "probe_budget")
        self.assertEqual(output["data"]["keys"], 200)
        self.assertGreater(output["data"]["keys"] * output["data"]["partitions"], output["data"]["budget"])

    def test_get_status_history_post_long_list(self) -> None:
        # 500 ten-digit job IDs make a URL of about 9500 bytes, over Apache's default request line
        # limit of 8190, so they go in a POST body. A window of an hour touches at most 2 daily
        # partitions, and 500 x 2 is within the default db_read_probe_budget of 1000
        url = f"{api_url_ssl}/job/get_status_history"
        print(f"Testing URL: {url}")
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        since = now - datetime.timedelta(hours=1)
        data = {
            "job_ids": list(range(7_000_000_000, 7_000_000_500)),
            "since": since.strftime("%Y-%m-%d %H:%M:%S"),
            "until": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        status, output = self.http_client.post(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertTrue(output["success"], output["message"])
        self.assertLessEqual(output["data"]["partitions"], 2)

    def test_get_description_incl_archive_report_errors(self) -> None:
        # a job not found is still null; an error would be {"error": code}
        url = f"{api_url_ssl}/job/get_description_incl_archive"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID, 1], "report_errors": True}
        status, output = self.http_client.post(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertTrue(output["success"], output["message"])
        self.assertEqual(len(output["data"]), 2)
        self.assertIsNone(output["data"][1])

    def test_get_datasets_by_name(self) -> None:
        url = f"{api_url_ssl}/job/get_datasets_by_name"
        print(f"Testing URL: {url}")
        data = {"names": ["no.such.dataset_sub0000001", "no.such.dataset_sub0000002"]}
        status, output = self.http_client.post(url, data)
        print(output)
        self.assertEqual(status, 0)
        self.assertTrue(output["success"], output["message"])
        self.assertEqual(output["data"]["rows"], [])
        self.assertLessEqual(2 * output["data"]["keys_per_name"] * output["data"]["partitions"], output["data"]["budget"])

    def test_get_datasets_by_name_over_budget(self) -> None:
        # the most names a call may give, plus one
        url = f"{api_url_ssl}/job/get_datasets_by_name"
        status, output = self.http_client.post(url, {"names": ["no.such.dataset"]})
        self.assertTrue(output["success"], output["message"])
        n_names = output["data"]["budget"] // (output["data"]["keys_per_name"] * output["data"]["partitions"]) + 1
        status, output = self.http_client.post(url, {"names": [f"no.such.dataset_sub{i}" for i in range(n_names)]})
        print(output)
        self.assertEqual(status, 0)
        self.assertFalse(output["success"])
        self.assertIn(output["data"].get("rejected"), ("probe_budget", "too_many_keys"))

    def test_get_metadata_for_analysis_jobs(self) -> None:
        # def get_metadata_for_analysis_jobs(req: PandaRequest, task_id: int) -> Dict:
        url = f"{api_url_ssl}/job/get_metadata_for_analysis_jobs"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]

        if JEDI_TASK_ID == -1:
            # Fake task returns success=True with "No metadata found" message
            expected_response = {"status": 0, "success": True, "message": "No metadata found"}
        else:
            # Real task
            expected_response = {"status": 0, "success": True, "message": ""}
            output["message"] = ""
        self.assertEqual(output, expected_response)

    def test_kill(self) -> None:
        # def kill(req, job_ids: List[int], code: int = None, ...) -> Dict:
        url = f"{api_url_ssl}/job/kill"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # killJobs always returns success=True
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reassign(self) -> None:
        # def reassign(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/reassign"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["message"]

        # reassignJobs always returns success=True
        expected_response = {"status": 0, "success": True, "data": None}
        self.assertEqual(output, expected_response)


class TestStatusHistoryRequestMethod(unittest.TestCase):
    """Unit tests for the HTTP methods get_status_history takes (no live server needed)."""

    ANSWER: dict[str, Any] = {"rows": [], "as_of": None, "retained_from": None, "since": None, "until": None, "partitions": 1, "budget": 1000}

    @staticmethod
    def _request(method: str) -> PandaRequest:
        """A request the endpoint decorator lets through with the given method: secure, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": method, "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _call(self, method: str, **kwargs: Any) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.get_job_status_history.return_value = (True, "", dict(self.ANSWER))
        with mock.patch.object(job_api, "global_task_buffer", task_buffer):
            out = job_api.get_status_history(self._request(method), **kwargs)
        return out, task_buffer

    def test_post_takes_more_job_ids_than_a_url_holds(self) -> None:
        # a JSON body keeps the types, so the IDs reach the lookup as they were sent
        job_ids = list(range(7_000_000_000, 7_000_000_500))
        out, task_buffer = self._call("POST", job_ids=job_ids, since="2026-10-12 14:30:45")
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(task_buffer.get_job_status_history.call_args[0][0], job_ids)

    def test_get_converts_the_strings_of_the_url(self) -> None:
        out, task_buffer = self._call("GET", job_ids=["7000000000", "7000000001"], since="2026-10-12 14:30:45")
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(task_buffer.get_job_status_history.call_args[0][0], [7_000_000_000, 7_000_000_001])

    def test_post_of_strings_is_refused(self) -> None:
        # as a form-encoded body gives them: only the values of a GET are converted
        out, task_buffer = self._call("POST", job_ids=["7000000000", "7000000001"])
        self.assertFalse(out["success"])
        self.assertIn("Type error", out["message"])
        task_buffer.get_job_status_history.assert_not_called()


class TestDescriptionReportErrors(unittest.TestCase):
    """Unit tests for report_errors of get_description_incl_archive (no live server needed)."""

    @staticmethod
    def _request() -> PandaRequest:
        """A request the endpoint decorator lets through: secure, POST, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": "POST", "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _call(self, jobs: list[Any], **kwargs: Any) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.getFullJobStatus.return_value = jobs
        with mock.patch.object(job_api, "global_task_buffer", task_buffer):
            out = job_api.get_description_incl_archive(self._request(), job_ids=[1, 2, 3], **kwargs)
        return out, task_buffer

    def test_errors_reported(self) -> None:
        job = mock.MagicMock()
        job.to_dict_advanced.return_value = {"PandaID": 1}
        out, task_buffer = self._call([job, None, {"error": "ORA-03113"}], report_errors=True)
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(out["data"], [{"PandaID": 1}, None, {"error": "ORA-03113"}])
        self.assertIs(task_buffer.getFullJobStatus.call_args.kwargs["report_errors"], True)

    def test_errors_not_reported_by_default(self) -> None:
        out, task_buffer = self._call([None, None, None])
        self.assertTrue(out["success"], out["message"])
        self.assertIs(task_buffer.getFullJobStatus.call_args.kwargs["report_errors"], False)


class TestGetDatasetsByName(unittest.TestCase):
    """Unit tests for the checks of get_datasets_by_name (no live server needed)."""

    ANSWER: dict[str, Any] = {"rows": [], "as_of": None, "retained_from": None, "partitions": 5, "budget": 1000}

    @staticmethod
    def _request(method: str) -> PandaRequest:
        """A request the endpoint decorator lets through with the given method: secure, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": method, "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _call(self, names: Any, method: str = "POST", answer: tuple[bool, str, dict[str, Any]] | None = None) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.get_datasets_by_name.return_value = answer or (True, "", dict(self.ANSWER))
        with mock.patch.object(job_api, "global_task_buffer", task_buffer), mock.patch.object(job_api.panda_config, "db_read_probe_budget", 1000, create=True):
            out = job_api.get_datasets_by_name(self._request(method), names=names)
        return out, task_buffer

    # the same dataset in both forms of its name
    SCOPED = "mc23_13p6TeV:mc23_13p6TeV.601229.PhPy8EG.evgen.EVNT.e8514_tid41234567_00"
    BARE = "mc23_13p6TeV.601229.PhPy8EG.evgen.EVNT.e8514_tid41234567_00"

    def _answer(self, *row_names: str) -> tuple[bool, str, dict[str, Any]]:
        """A successful lookup with one row for each of the given names."""
        return True, "", dict(self.ANSWER, rows=[{"vuid": f"vuid-{i}", "name": name} for i, name in enumerate(row_names)])

    def test_names_looked_up_once(self) -> None:
        out, task_buffer = self._call(["a.x_sub1", "b.y_sub2", "a.x_sub1"])
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0], (["a.x_sub1", "a:a.x_sub1", "b.y_sub2", "b:b.y_sub2"], 1000))

    def test_scoped_name_looked_up_without_scope(self) -> None:
        out, task_buffer = self._call([self.SCOPED])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], [self.SCOPED, self.BARE])

    def test_unscoped_name_looked_up_with_scope(self) -> None:
        out, task_buffer = self._call([self.BARE])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], [self.BARE, self.SCOPED])
        # the scope of a user dataset is its first two parts
        out, task_buffer = self._call(["user.alice.test.v1_sub123"])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], ["user.alice.test.v1_sub123", "user.alice:user.alice.test.v1_sub123"])

    def test_no_empty_form(self) -> None:
        out, task_buffer = self._call(["s:"])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], ["s:"])

    def test_both_forms_given(self) -> None:
        out, task_buffer = self._call([self.SCOPED, self.BARE], answer=self._answer(self.BARE, self.SCOPED))
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], [self.SCOPED, self.BARE])
        self.assertEqual([row["asked_as"] for row in out["data"]["rows"]], [[self.SCOPED, self.BARE], [self.SCOPED, self.BARE]])

    def test_rows_say_which_names_they_answer(self) -> None:
        out, task_buffer = self._call([self.SCOPED, "other.x_sub9"], answer=self._answer(self.BARE, "other:other.x_sub9"))
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(
            [(row["name"], row["asked_as"]) for row in out["data"]["rows"]], [(self.BARE, [self.SCOPED]), ("other:other.x_sub9", ["other.x_sub9"])]
        )
        self.assertEqual(out["data"]["keys_per_name"], 2)
        self.assertEqual(out["data"]["partitions"], 5)

    def test_get(self) -> None:
        out, task_buffer = self._call(["a.x_sub1"], method="GET")
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(task_buffer.get_datasets_by_name.call_args[0][0], ["a.x_sub1", "a:a.x_sub1"])

    def test_refused(self) -> None:
        out, task_buffer = self._call([])
        self.assertEqual((out["success"], out["data"]), (False, {"rejected": "no_keys"}))
        # the cap counts keys, two a name
        out, task_buffer = self._call([f"a.x_sub{i}" for i in range(501)])
        self.assertEqual((out["success"], out["data"]), (False, {"rejected": "too_many_keys", "keys": 1002, "max_keys": 1000, "keys_per_name": 2}))
        task_buffer.get_datasets_by_name.assert_not_called()
        out, task_buffer = self._call([f"a.x_sub{i}" for i in range(500)])
        self.assertEqual(len(task_buffer.get_datasets_by_name.call_args[0][0]), 1000)

    def test_refusal_of_the_lookup_passed_on(self) -> None:
        refusal = {"rejected": "probe_budget", "keys": 402, "partitions": 5, "budget": 1000}
        out, task_buffer = self._call([f"a.x_sub{i}" for i in range(201)], answer=(False, "over", dict(refusal)))
        self.assertEqual((out["success"], out["message"], out["data"]), (False, "over", dict(refusal, keys_per_name=2)))

    def test_error_passed_on(self) -> None:
        out, task_buffer = self._call(["a.x_sub1"], answer=(False, "database error ORA-00942", {"error": "ORA-00942"}))
        self.assertEqual((out["success"], out["message"], out["data"]), (False, "database error ORA-00942", {"error": "ORA-00942"}))


if __name__ == "__main__":
    unittest.main()
