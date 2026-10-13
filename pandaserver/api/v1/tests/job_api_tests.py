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


if __name__ == "__main__":
    unittest.main()
