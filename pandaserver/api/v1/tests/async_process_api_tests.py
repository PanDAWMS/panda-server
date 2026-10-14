# TODO: the secure-endpoint results depend on the cert used for the call.
# These results assume the cert's DN is in the `allowAsyncRequest` list.

import datetime
import json
import socket
import time
import unittest
import uuid
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from pandaserver.api.v1 import async_process_api, common
from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE

NO_SSL_RESPONSE = {"success": False, "message": "SSL secure connection is required", "data": None}

# a db_read query costing one index lookup, whatever the job
DB_READ_QUERY = {"tables": ["jobsActive4"], "where": [["PandaID", "=", 1]], "columns": ["PandaID", "jobStatus"]}
# seconds to wait for the server to answer a db_read request, a few cycles of async_request_daemon
DB_READ_WAIT_SECONDS = 120


class TestAsyncProcessAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.http_client = HttpClient()

    def test_submit_grep_request_no_ssl(self) -> None:
        full_url = f"{api_url}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {"pattern": "ERROR", "log_filename": "panda-server.log", "service_name": "server"}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_submit_grep_request_missing_target(self) -> None:
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

    def test_submit_grep_request_both_targets(self) -> None:
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

    def test_submit_grep_request_invalid_filename(self) -> None:
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

    def test_submit_grep_request_max_matches_out_of_range(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "service_name": "server",
            "max_matches": async_process_api.MAX_MATCH_LIMIT + 1,
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": f"invalid max_matches: must be between 1 and {async_process_api.MAX_MATCH_LIMIT}",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_submit_grep_request_tail_bytes_out_of_range(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "service_name": "server",
            "tail_bytes": 0,
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": f"invalid tail_bytes: must be between 1 and {async_process_api.MAX_TAIL_BYTES}",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_submit_grep_request_bounded_success(self) -> None:
        """A bounded query is how a multi-gigabyte log should be asked about."""
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "pattern": "ERROR",
            "log_filename": "panda-server.log",
            "machine_name": socket.getfqdn(),
            "max_matches": 10,
            "tail_bytes": 1_000_000,
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertTrue(output["success"])
        self.assertIsInstance(output["data"]["async_id"], str)

    def test_submit_grep_request_success(self) -> None:
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
        async_id = output["data"]["async_id"]
        self.assertIsInstance(async_id, str)
        self.assertEqual(len(async_id), 36)
        # the pre-rename key is still served alongside it
        self.assertEqual(output["data"]["request_id"], async_id)

    def test_submit_grep_request_fixed_strings_whole_records(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_grep_request"
        print(f"Testing URL: {full_url}")
        data = {
            "fixed_strings": ["ERROR", "Traceback (most recent call last):"],
            "whole_records": True,
            "log_filename": "panda-server.log",
            "machine_name": socket.getfqdn(),
        }
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertTrue(output["success"], output["message"])
        self.assertEqual(len(output["data"]["async_id"]), 36)

    def test_submit_log_list_request_success(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_log_list_request"
        print(f"Testing URL: {full_url}")
        status, output = self.http_client.post(full_url, {"machine_name": socket.getfqdn()})
        print(output)
        self.assertTrue(output["success"], output["message"])
        self.assertEqual(len(output["data"]["async_id"]), 36)

    def test_get_result_no_ssl(self) -> None:
        # a result is read by its owner only, whose DN comes with a secure connection
        full_url = f"{api_url}/async_process/get_result"
        print(f"Testing URL: {full_url}")
        status, output = self.http_client.get(full_url, {"async_id": str(uuid.uuid4())})
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_get_result_not_found(self) -> None:
        missing_id = str(uuid.uuid4())
        full_url = f"{api_url_ssl}/async_process/get_result"
        print(f"Testing URL: {full_url}")
        data = {"async_id": missing_id}
        status, output = self.http_client.get(full_url, data)
        print(output)
        expected_response = {
            "success": False,
            "message": f"async_id '{missing_id}' not found",
            "data": None,
        }
        self.assertEqual(output, expected_response)

    def test_get_result_pending(self) -> None:
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
        async_id = submit_output["data"]["async_id"]

        get_url = f"{api_url_ssl}/async_process/get_result"
        print(f"Testing URL: {get_url}")
        status, output = self.http_client.get(get_url, {"async_id": async_id})
        print(output)
        self.assertTrue(output["success"])
        self.assertIsInstance(output["data"], dict)
        self.assertEqual(output["data"]["overall_status"], "pending")
        self.assertIsInstance(output["data"]["expected_machines"], list)
        self.assertIsInstance(output["data"]["results"], list)

    def test_submit_sleep_echo_no_ssl(self) -> None:
        full_url = f"{api_url}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 1}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_submit_sleep_echo_invalid_seconds(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 100000}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertFalse(output["success"])

    def test_submit_sleep_echo_success(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_sleep_echo_request"
        print(f"Testing URL: {full_url}")
        data = {"service_name": "server", "message": "hi", "seconds": 1}
        status, output = self.http_client.post(full_url, data)
        print(output)
        if not output.get("success"):
            raise unittest.SkipTest(f"submit_sleep_echo_request did not succeed: {output.get('message')}")
        self.assertIsInstance(output["data"], dict)
        async_id = output["data"]["async_id"]
        self.assertIsInstance(async_id, str)
        self.assertEqual(len(async_id), 36)
        # the pre-rename key is still served alongside it
        self.assertEqual(output["data"]["request_id"], async_id)

    def test_submit_db_read_request_no_ssl(self) -> None:
        full_url = f"{api_url}/async_process/submit_db_read_request"
        print(f"Testing URL: {full_url}")
        data = {"queries": [DB_READ_QUERY]}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertEqual(output, NO_SSL_RESPONSE)

    def test_submit_db_read_request_too_many_queries(self) -> None:
        full_url = f"{api_url_ssl}/async_process/submit_db_read_request"
        print(f"Testing URL: {full_url}")
        data = {"queries": [DB_READ_QUERY] * 11}
        status, output = self.http_client.post(full_url, data)
        print(output)
        self.assertFalse(output["success"])

    def test_submit_db_read_request_answered(self) -> None:
        # the queries run on the read-only account of the server, so the server needs dbuser_reader in its
        # cfg and a timeout of async_request_daemon long enough for them, and the answer takes a cycle or two
        submit_url = f"{api_url_ssl}/async_process/submit_db_read_request"
        print(f"Testing URL: {submit_url}")
        # the tables are checked against the allowlist when the queries run, refusing that query alone
        data = {"queries": [DB_READ_QUERY, {"tables": ["users"], "where": [["name", "=", "x"]]}]}
        status, submit_output = self.http_client.post(submit_url, data)
        print(submit_output)
        if not submit_output.get("success"):
            raise unittest.SkipTest(f"submit_db_read_request did not succeed: {submit_output.get('message')}")
        async_id = submit_output["data"]["async_id"]

        get_url = f"{api_url_ssl}/async_process/get_result"
        print(f"Testing URL: {get_url}")
        deadline = time.monotonic() + DB_READ_WAIT_SECONDS
        while True:
            status, output = self.http_client.get(get_url, {"async_id": async_id})
            print(output)
            # async_meta is missing only when the poll itself failed
            meta = output.get("async_meta")
            if meta is None or meta["status"] in ("done", "failed") or time.monotonic() > deadline:
                break
            time.sleep(5)
        self.assertEqual(output.get("async_meta", {}).get("status"), "done", output)
        self.assertTrue(output["success"], output["message"])
        answers = output["data"]["answers"]
        self.assertEqual(len(answers), 2)
        self.assertEqual(answers[0]["status"], "done", answers[0])
        self.assertEqual([table["table"] for table in answers[0]["tables"]], ["jobsActive4"])
        self.assertEqual((answers[1]["status"], answers[1]["rule"]), ("rejected", "unknown_table"), answers[1])


class TestAsyncAccessControl(unittest.TestCase):
    """Unit tests for the access-control helpers (no live server needed)."""

    def _row(self, requester: str | None, access: str | None = None) -> dict[str, str]:
        params = {"requester": requester}
        if access is not None:
            params["access"] = access
        return {"parameters": json.dumps(params)}

    def test_set_owner_info_default_owner(self) -> None:
        with mock.patch.object(common, "get_dn", return_value="dn"), mock.patch.object(common, "clean_user_id", return_value="alice"):
            # get_dn is patched out, so nothing reads the request; the cast says so
            params = common.set_owner_info({"pattern": "x"}, req=cast(PandaRequest, object()))
        self.assertEqual(params["requester"], "alice")
        self.assertEqual(params["access"], "owner")
        self.assertEqual(params["pattern"], "x")

    def test_set_owner_info_explicit_access(self) -> None:
        with mock.patch.object(common, "get_dn", return_value="dn"), mock.patch.object(common, "clean_user_id", return_value="alice"):
            params = common.set_owner_info({}, req=cast(PandaRequest, object()), access="anyone")
        self.assertEqual(params["access"], "anyone")

    def _authorize(self, caller: str, req_row: dict[str, Any], production_role: bool = False) -> tuple[bool, str]:
        with (
            mock.patch.object(common, "get_dn", return_value="dn"),
            mock.patch.object(common, "clean_user_id", return_value=caller),
            mock.patch.object(common, "has_production_role", return_value=production_role),
        ):
            return common.is_authorized_to_read(cast(PandaRequest, object()), req_row)

    def test_owner_matching_caller_ok(self) -> None:
        ok, _ = self._authorize("alice", self._row("alice", "owner"))
        self.assertTrue(ok)

    def test_owner_other_caller_denied(self) -> None:
        ok, _ = self._authorize("bob", self._row("alice", "owner"))
        self.assertFalse(ok)

    def test_production_role_caller_ok(self) -> None:
        ok, _ = self._authorize("bob", self._row("alice", "production"), production_role=True)
        self.assertTrue(ok)

    def test_production_non_role_non_owner_denied(self) -> None:
        ok, _ = self._authorize("bob", self._row("alice", "production"), production_role=False)
        self.assertFalse(ok)

    def test_anyone_any_caller_ok(self) -> None:
        ok, _ = self._authorize("bob", self._row("alice", "anyone"))
        self.assertTrue(ok)

    def test_missing_access_defaults_to_owner(self) -> None:
        ok, _ = self._authorize("alice", self._row("alice"))
        self.assertTrue(ok)
        ok, _ = self._authorize("bob", self._row("alice"))
        self.assertFalse(ok)

    def test_unknown_access_denied(self) -> None:
        ok, _ = self._authorize("alice", self._row("alice", "bogus"))
        self.assertFalse(ok)


class TestStructuredResult(unittest.TestCase):
    """Unit tests for the structured-payload shape of get_result (no live server needed)."""

    REQUEST_ROW = {"request_type": "dc_force_to_staging", "parameters": json.dumps({"requester": "alice", "access": "production", "structured_result": True})}

    @staticmethod
    def _request() -> PandaRequest:
        """A request the endpoint decorator lets through: secure, GET, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": "GET", "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _result_row(self, **over: Any) -> dict[str, Any]:
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

    def test_done_payload_is_hoisted(self) -> None:
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row()])
        self.assertTrue(out["success"])
        self.assertEqual(out["message"], "status has become staging")
        self.assertEqual(out["data"], {"request_id": 123})
        self.assertEqual(out["async_meta"]["status"], "done")
        self.assertEqual(out["async_meta"]["started_at"], "2026-08-06 07:49:51")
        # the raw-output fields of the per-machine shape must not leak in
        for key in ("stderr", "return_code", "truncated", "results", "overall_status"):
            self.assertNotIn(key, out)

    def test_failed_operation_is_reported_as_success_false(self) -> None:
        payload = json.dumps({"success": False, "message": "failed to get corresponding request", "data": None})
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result=payload)])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "failed to get corresponding request")
        self.assertIsNone(out["data"])
        self.assertEqual(out["async_meta"]["status"], "done")

    def test_pending_and_running(self) -> None:
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request is pending")
        self.assertEqual(out["async_meta"], {"status": "pending", "attempts": 0, "started_at": None, "finished_at": None, "error_msg": None})

        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(status="running", result=None, finished_at=None)])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request is running")
        self.assertIsNone(out["async_meta"]["finished_at"])

    def test_crashed_handler(self) -> None:
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(status="failed", result=None, error_msg="Traceback ... boom")])
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "request failed before producing a result")
        self.assertIn("boom", out["async_meta"]["error_msg"])

    def test_payload_without_success_never_looks_unfinished(self) -> None:
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result=json.dumps({"message": "odd"}))])
        self.assertFalse(out["success"])
        self.assertEqual(out["async_meta"]["status"], "done")

    def test_corrupt_payload_is_a_poll_failure(self) -> None:
        out = async_process_api._structured_result_response(self.REQUEST_ROW, [self._result_row(result="{not json")])
        self.assertFalse(out["success"])
        self.assertIn("failed to decode stored result", out["message"])
        # a response without async_meta means the poll itself failed
        self.assertNotIn("async_meta", out)

    def test_get_result_dispatches_on_the_flag(self) -> None:
        task_buffer = mock.MagicMock()
        task_buffer.get_async_request.return_value = self.REQUEST_ROW
        task_buffer.get_async_results.return_value = [self._result_row()]
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "is_authorized_to_read", return_value=(True, "ok")),
        ):
            out = async_process_api.get_result(self._request(), async_id="some-uuid")
        self.assertIn("async_meta", out)

        # without the flag the request keeps the per-machine shape
        grep_row = {"request_type": "grep", "parameters": json.dumps({"requester": "alice", "access": "owner"}), "expected_machines": json.dumps(["a.cern.ch"])}
        task_buffer.get_async_request.return_value = grep_row
        task_buffer.get_async_results.return_value = [self._result_row(machine_name="a.cern.ch", result="matched line")]
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "is_authorized_to_read", return_value=(True, "ok")),
        ):
            out = async_process_api.get_result(self._request(), async_id="some-uuid")
        self.assertNotIn("async_meta", out)
        self.assertEqual(out["data"]["overall_status"], "complete")
        self.assertEqual(out["data"]["results"][0]["result"], "matched line")

    def _poll(self, **kwargs: Any) -> tuple[dict[str, Any], mock.MagicMock]:
        """Call get_result with whatever id keywords are given, against a request that is done."""
        task_buffer = mock.MagicMock()
        task_buffer.get_async_request.return_value = self.REQUEST_ROW
        task_buffer.get_async_results.return_value = [self._result_row()]
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "is_authorized_to_read", return_value=(True, "ok")),
        ):
            return async_process_api.get_result(self._request(), **kwargs), task_buffer

    def test_pre_rename_request_id_still_polls(self) -> None:
        # a caller that has not moved off request_id must get the same answer as one sending async_id
        out, task_buffer = self._poll(request_id="some-uuid")
        self.assertEqual(out["async_meta"]["status"], "done")
        task_buffer.get_async_request.assert_called_once_with("some-uuid")

    def test_async_id_wins_over_the_pre_rename_name(self) -> None:
        out, task_buffer = self._poll(async_id="new-uuid", request_id="old-uuid")
        self.assertEqual(out["async_meta"]["status"], "done")
        task_buffer.get_async_request.assert_called_once_with("new-uuid")

    def test_neither_id_is_a_poll_failure(self) -> None:
        out, task_buffer = self._poll()
        self.assertFalse(out["success"])
        self.assertEqual(out["message"], "async_id must be provided")
        # nothing was looked up, so the caller cannot read the miss as "request not found"
        task_buffer.get_async_request.assert_not_called()
        self.assertNotIn("async_meta", out)


class TestSubmitDbReadRequest(unittest.TestCase):
    """Unit tests for submit_db_read_request (no live server needed)."""

    QUERY = {"tables": ["jobsActive4"], "where": [["PandaID", "=", 1]]}

    @staticmethod
    def _request() -> PandaRequest:
        """A request the endpoint decorator lets through: secure, POST, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": "POST", "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _submit(self, queries: Any, authorized: bool = True, alive: list[str] | None = None) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.get_alive_machines.return_value = ["a.cern.ch"] if alive is None else alive
        task_buffer.insert_async_request.return_value = True
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "_is_authorized_with_allowlist", return_value=(authorized, "checked")),
            mock.patch.object(common, "get_dn", return_value="dn"),
            mock.patch.object(common, "clean_user_id", return_value="alice"),
        ):
            out = async_process_api.submit_db_read_request(self._request(), queries=queries)
        return out, task_buffer

    def test_success(self) -> None:
        out, task_buffer = self._submit([self.QUERY])
        self.assertTrue(out["success"], out["message"])
        async_id, request_type, parameters_json, service_name, machine_name, expected = task_buffer.insert_async_request.call_args[0]
        self.assertEqual(out["data"], {"async_id": async_id})
        self.assertEqual((request_type, service_name, machine_name, expected), ("db_read", "server", ANY_MACHINE, None))
        # owner-only, with the payload reported at the top level of get_result
        self.assertEqual(json.loads(parameters_json), {"queries": [self.QUERY], "requester": "alice", "access": "owner", "structured_result": True})

    def test_refused(self) -> None:
        for queries, kwargs in (
            ([self.QUERY], {"authorized": False}),
            ([], {}),
            ([self.QUERY] * 11, {}),
            ([{"tables": ["jobsActive4"], "where": [["computingSite", "=", "x" * 200_000]]}], {}),
            ([self.QUERY], {"alive": []}),
            (["not a query"], {}),
        ):
            out, task_buffer = self._submit(queries, **kwargs)
            self.assertFalse(out["success"], (queries, kwargs))
            task_buffer.insert_async_request.assert_not_called()


class TestSubmitGrepRequest(unittest.TestCase):
    """Unit tests for the checks of submit_grep_request (no live server needed)."""

    @staticmethod
    def _request() -> PandaRequest:
        """A request the endpoint decorator lets through: secure, POST, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": "POST", "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _submit(self, **kwargs: Any) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.get_alive_machines.return_value = ["a.cern.ch", "b.cern.ch"]
        task_buffer.insert_async_request.return_value = True
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "_is_authorized_with_allowlist", return_value=(True, "checked")),
            mock.patch.object(common, "get_dn", return_value="dn"),
            mock.patch.object(common, "clean_user_id", return_value="alice"),
        ):
            out = async_process_api.submit_grep_request(self._request(), log_filename="panda-adder.log", service_name="server", **kwargs)
        return out, task_buffer

    @staticmethod
    def _parameters(task_buffer: mock.MagicMock) -> dict[str, Any]:
        parameters: dict[str, Any] = json.loads(task_buffer.insert_async_request.call_args[0][2])
        return parameters

    def test_pattern(self) -> None:
        out, task_buffer = self._submit(pattern="ERROR")
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(self._parameters(task_buffer)["pattern"], "ERROR")
        self.assertNotIn("fixed_strings", self._parameters(task_buffer))
        self.assertNotIn("whole_records", self._parameters(task_buffer))

    def test_whole_records(self) -> None:
        out, task_buffer = self._submit(pattern="ERROR", whole_records=True)
        self.assertTrue(out["success"], out["message"])
        self.assertIs(self._parameters(task_buffer)["whole_records"], True)

    def test_fixed_strings(self) -> None:
        fixed_strings = ["getJobs < SITE 2026-10-08T00:00:01 >", "a.b+c/d"]
        out, task_buffer = self._submit(fixed_strings=fixed_strings)
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(self._parameters(task_buffer)["fixed_strings"], fixed_strings)
        self.assertNotIn("pattern", self._parameters(task_buffer))

    def test_widest_tail_bytes(self) -> None:
        out, task_buffer = self._submit(pattern="ERROR", tail_bytes=3 << 30)
        self.assertTrue(out["success"], out["message"])
        self.assertEqual(self._parameters(task_buffer)["tail_bytes"], 3 << 30)

    def test_refused(self) -> None:
        refused: tuple[dict[str, Any], ...] = (
            {},
            {"pattern": "ERROR", "tail_bytes": 0},
            {"pattern": "ERROR", "tail_bytes": (3 << 30) + 1},
            {"pattern": "ERROR", "fixed_strings": ["ERROR"]},
            {"fixed_strings": []},
            {"fixed_strings": ["ERROR", ""]},
            {"fixed_strings": ["two\nlines"]},
            {"fixed_strings": ["x" * 1001]},
            {"fixed_strings": ["x"] * 1001},
        )
        for kwargs in refused:
            out, task_buffer = self._submit(**kwargs)
            self.assertFalse(out["success"], kwargs)
            task_buffer.insert_async_request.assert_not_called()


class TestSubmitLogListRequest(unittest.TestCase):
    """Unit tests for submit_log_list_request (no live server needed)."""

    @staticmethod
    def _request() -> PandaRequest:
        """A request the endpoint decorator lets through: secure, POST, and not a limited proxy."""
        return cast(PandaRequest, SimpleNamespace(subprocess_env={"REQUEST_METHOD": "POST", "SSL_CLIENT_S_DN": "/CN=alice"}))

    def _submit(self, authorized: bool = True, **kwargs: Any) -> tuple[dict[str, Any], mock.MagicMock]:
        task_buffer = mock.MagicMock()
        task_buffer.get_alive_machines.return_value = ["a.cern.ch", "b.cern.ch"]
        task_buffer.insert_async_request.return_value = True
        with (
            mock.patch.object(async_process_api, "global_task_buffer", task_buffer),
            mock.patch.object(async_process_api, "_is_authorized_with_allowlist", return_value=(authorized, "checked")),
            mock.patch.object(common, "get_dn", return_value="dn"),
            mock.patch.object(common, "clean_user_id", return_value="alice"),
        ):
            out = async_process_api.submit_log_list_request(self._request(), **kwargs)
        return out, task_buffer

    def test_success(self) -> None:
        out, task_buffer = self._submit(service_name="server")
        self.assertTrue(out["success"], out["message"])
        async_id, request_type, parameters_json, service_name, machine_name, expected = task_buffer.insert_async_request.call_args[0]
        self.assertEqual(out["data"], {"async_id": async_id, "request_id": async_id})
        self.assertEqual((request_type, service_name, machine_name), ("log_list", "server", None))
        self.assertEqual(json.loads(expected), ["a.cern.ch", "b.cern.ch"])
        self.assertEqual(json.loads(parameters_json), {"requester": "alice", "access": "owner"})

    def test_refused(self) -> None:
        refused: tuple[dict[str, Any], ...] = (
            {},
            {"service_name": "server", "machine_name": "a.cern.ch"},
            {"service_name": "server", "authorized": False},
        )
        for kwargs in refused:
            out, task_buffer = self._submit(**kwargs)
            self.assertFalse(out["success"], kwargs)
            task_buffer.insert_async_request.assert_not_called()


# Run tests
if __name__ == "__main__":
    unittest.main()
