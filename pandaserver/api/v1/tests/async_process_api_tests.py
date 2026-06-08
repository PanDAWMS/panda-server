# TODO: the secure-endpoint results depend on the cert used for the call.
# These results assume the cert's DN is in the `allowAsyncRequest` list.

import json
import socket
import unittest
import uuid
from unittest import mock

from pandaserver.api.v1 import async_process_api
from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl

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
        with mock.patch.object(async_process_api, "get_dn", return_value="dn"), mock.patch.object(async_process_api, "clean_user_id", return_value="alice"):
            params = async_process_api._set_owner_info({"pattern": "x"}, req=object())
        self.assertEqual(params["requester"], "alice")
        self.assertEqual(params["access"], "owner")
        self.assertEqual(params["pattern"], "x")

    def test_set_owner_info_explicit_access(self):
        with mock.patch.object(async_process_api, "get_dn", return_value="dn"), mock.patch.object(async_process_api, "clean_user_id", return_value="alice"):
            params = async_process_api._set_owner_info({}, req=object(), access="anyone")
        self.assertEqual(params["access"], "anyone")

    def _authorize(self, caller, req_row, production_role=False):
        with (
            mock.patch.object(async_process_api, "get_dn", return_value="dn"),
            mock.patch.object(async_process_api, "clean_user_id", return_value=caller),
            mock.patch.object(async_process_api, "has_production_role", return_value=production_role),
        ):
            return async_process_api._is_authorized_to_read(object(), req_row)

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


# Run tests
if __name__ == "__main__":
    unittest.main()
