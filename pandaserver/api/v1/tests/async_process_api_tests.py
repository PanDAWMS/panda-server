# TODO: the secure-endpoint results depend on the cert used for the call.
# These results assume the cert's DN is in the `allowAsyncRequest` list.

import datetime
import json
import socket
import unittest
import uuid
from unittest import mock

from pandaserver.api.v1 import async_process_api, common
from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE

NO_SSL_RESPONSE = {"success": False, "message": "SSL secure connection is required", "data": None}


class TestAsyncProcessAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()
        self.urls = [api_url, api_url_ssl]

    def test_submit_grep_request_no_ssl(self):
        full_url = f"{api_url}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {"pattern": "ERROR", "log_filename": "panda-server.log", "service_name": "server"}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_submit_grep_request_missing_target(self):
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {"pattern": "ERROR", "log_filename": "panda-server.log"}
        status, output = self.http_client.post(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": "exactly one of service_name or machine_name must be provided",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_submit_grep_request_both_targets(self):
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "service_name": "server",
            "machine_name": socket.getfqdn(),
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": "exactly one of service_name or machine_name must be provided",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_submit_grep_request_invalid_filename(self):
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {"pattern": "ERROR", "log_filename": "../etc/passwd", "service_name": "server"}
        status, output = self.http_client.post(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": "invalid log_filename: must not contain path separators",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_submit_grep_request_success(self):
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "machine_name": socket.getfqdn(),
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertTrue(output["success"])
        self.assertIsInstance(output["data"], dict)
        request_id = output["data"]["request_id"]
        self.assertIsInstance(request_id, str)
        self.assertEqual(len(request_id), 36)

    def test_get_result_not_found(self):
        missing_id = str(uuid.uuid4())
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/async_process/get_result"
                print(f"Testing URL: {full_url}")
                data = {"request_id": missing_id}
                status, output = self.http_client.get(full_url, data)
                print(output)
                expected_response = {
                    "success": False,
                    "message": f"request_id '{missing_id}' not found",
                    "data": None,
                }
                self.assertEqual(output, expected_response)

    def test_get_result_pending(self):
        submit_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {submit_url}")
        submit_data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "machine_name": socket.getfqdn(),
        }
        status, submit_output = self.http_client.post(submit_url, submit_data)
        print(submit_output)
        if not submit_output.get("success"):
            raise unittest.SkipTest(f"submit_grep_request did not succeed: {submit_output.get('message')}")
        request_id = submit_output["data"]["request_id"]

        get_url = f"{api_url_ssl}/async_process/get_result"
        print(f"Testing URL: {get_url}")
        status, output = self.http_client.get(get_url, {"request_id": request_id})
        print(output)
        self.assertTrue(output["success"])
        self.assertIsInstance(output["data"], dict)
        self.assertEqual(output["data"]["overall_status"], "pending")
        self.assertIsInstance(output["data"]["expected_machines"], list)
        self.assertIsInstance(output["data"]["results"], list)

    def test_submit_sleep_echo_no_ssl(self):
        full_url = f"{api_url}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 1}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_submit_sleep_echo_invalid_seconds(self):
        full_url = f"{api_url_ssl}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 100000}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertFalse(output["success"])

    def test_submit_sleep_echo_success(self):
        full_url = f"{api_url_ssl}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 1}
        status, output = self.http_client.post(full_url, data)
        print(output)
        if not output.get("success"):
            raise unittest.SkipTest(f"submit_sleep_echo_request did not succeed: {output.get('message')}")
        self.assertIsInstance(output["data"], dict)
        request_id = output["data"]["request_id"]
        self.assertIsInstance(request_id, str)
        self.assertEqual(len(request_id), 36)


class TestAsyncAccessControl(unittest.TestCase):
    """Unit tests for the access-control helpers (no live server needed)."""

    def _row(self, requester, access=None):
        params = {"requester": requester}
        if access is not None:
            params["access"] = access
        return {"parameters": json.dumps(params)}

    def test_set_owner_info_default_owner(self):
        with mock.patch.object(common, "get_dn", return_value="dn"), mock.patch.object(common, "clean_user_id", return_value="alice"):
            params = common.set_owner_info({"pattern": "x"}, req=object())
        self.assertEqual(params["requester"], "alice")
        self.assertEqual(params["access"], "owner")
        self.assertEqual(params["pattern"], "x")

    def test_set_owner_info_explicit_access(self):
        with mock.patch.object(common, "get_dn", return_value="dn"), mock.patch.object(common, "clean_user_id", return_value="alice"):
            params = common.set_owner_info({}, req=object(), access="anyone")
        self.assertEqual(params["access"], "anyone")

    def _authorize(self, caller, req_row, production_role=False):
        with (
            mock.patch.object(common, "get_dn", return_value="dn"),
            mock.patch.object(common, "clean_user_id", return_value=caller),
            mock.patch.object(common, "has_production_role", return_value=production_role),
        ):
            return common.is_authorized_to_read(object(), req_row)

    def test_owner_matching_caller_ok(self):
        ok, _ = self._authorize("alice", self._row("alice", "owner"))
        self.assertTrue(ok)

    def test_owner_other_caller_denied(self):
        ok, _ = self._authorize("bob", self._row("alice", "owner"))
        self.assertFalse(ok)

    def test_production_role_caller_ok(self):
        ok, _ = self._authorize("bob", self._row("alice", "production"), production_role=True)
        self.assertTrue(ok)

    def test_production_non_role_non_owner_denied(self):
        ok, _ = self._authorize("bob", self._row("alice", "production"), production_role=False)
        self.assertFalse(ok)

    def test_anyone_any_caller_ok(self):
        ok, _ = self._authorize("bob", self._row("alice", "anyone"))
        self.assertTrue(ok)

    def test_missing_access_defaults_to_owner(self):
        ok, _ = self._authorize("alice", self._row("alice"))
        self.assertTrue(ok)
        ok, _ = self._authorize("bob", self._row("alice"))
        self.assertFalse(ok)

    def test_unknown_access_denied(self):
        ok, _ = self._authorize("alice", self._row("alice", "bogus"))
        self.assertFalse(ok)


class TestStructuredResult(unittest.TestCase):
    """Unit tests for the structured-payload shape of get_result (no live server needed)."""

    REQUEST_ROW = {"request_type": "dc_force_to_staging", "parameters": json.dumps({"requester": "alice", "access": "production", "structured_result": True})}

    def _result_row(self, **over):
        row = {
            "machine_name": ANY_MACHINE,
            "status": "done",
            "result": json.dumps({"success": True, "message": "status has become staging", "data": {"request_id": 123}}),
            "truncated": 0,
            "error_msg": None,
            "attempts": 1,
            "started_at": datetime.datetime(2026, 8, 6, 7, 49, 51),
            "finished_at": datetime.datetime(2026, 8, 6, 7, 49, 55),
            "stderr": None,
            "return_code": None,
        }
        row.update(over)
        return row

    def test_done_payload_is_hoisted(self):
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row()])
        self.assertTrue(out["success"])
        self.assertEqual(out["message"], "status has become staging")
        self.assertEqual(out["data"], {"request_id": 123})
        self.assertEqual(out["async_meta"]["status"], "done")
        self.assertEqual(out["async_meta"]["started_at"], "2026-08-06 07:49:51")
        # the raw-output fields of the per-machine shape must not leak in
        for key in ("stderr", "return_code", "truncated", "results", "overall_status"):
            self.assertNotIn(key, out)

    def test_failed_operation_is_reported_as_success_false(self):
        payload = json.dumps({"success": False, "message": "failed to get corresponding request", "data": None})
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result=payload)])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "failed to get corresponding request")
        self.assertIsNone(out["data"])
        self.assertEqual(out["async_meta"]["status"], "done")

    def test_pending_and_running(self):
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request is pending")
        self.assertEqual(out["async_meta"], {"status": "pending", "attempts": 0, "started_at": None, "finished_at": None, "error_msg": None})

        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(status="running", result=None, finished_at=None)])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request is running")
        self.assertIsNone(out["async_meta"]["finished_at"])

    def test_crashed_handler(self):
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(status="failed", result=None, error_msg="Traceback ... boom")])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request failed before producing a result")
        self.assertIn("boom", out["async_meta"]["error_msg"])

    def test_payload_without_success_never_looks_unfinished(self):
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result=json.dumps({"message": "odd"}))])
        self.assertFalse(out["success"])
        self.assertEqual(out["async_meta"]["status"], "done")

    def test_corrupt_payload_is_a_poll_failure(self):
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result="{not json")])
        self.assertFalse(out["success"])
        self.assertIn("failed to decode stored result", out["message"])
        # a response without async_meta means the poll itself failed
        self.assertNotIn("async_meta", out)

    def test_get_result_dispatches_on_the_flag(self):
        task_buffer = mock.MagicMock()
        task_buffer.get_async_request.return_value = self.REQUEST_ROW
        task_buffer.get_async_results.return_value = [self._result_row()]
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "is_authorized_to_read", return_value=(True, "ok")),
        ):
            out = async_process_api.get_result(object(), request_id="some-uuid")
        self.assertIn("async_meta", out)

        # without the flag the request keeps the per-machine shape
        grep_row = {"request_type": "grep", "parameters": json.dumps({"requester": "alice", "access": "owner"}), "expected_machines": json.dumps(["a.cern.ch"])}
        task_buffer.get_async_request.return_value = grep_row
        task_buffer.get_async_results.return_value = [self._result_row(machine_name="a.cern.ch", result="matched line")]
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "is_authorized_to_read", return_value=(True, "ok")),
        ):
            out = async_process_api.get_result(object(), request_id="some-uuid")
        self.assertNotIn("async_meta", out)
        self.assertEqual(out["data"]["overall_status"], "complete")
        self.assertEqual(out["data"]["results"][0]["result"], "matched line")


# Run tests
if __name__ == "__main__":
    unittest.main()
