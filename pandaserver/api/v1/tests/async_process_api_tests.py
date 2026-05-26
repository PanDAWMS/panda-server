# TODO: the secure-endpoint results depend on the cert used for the call.
# These results assume the cert's DN is in the `allowAsyncRequest` list.

import socket
import unittest
import uuid

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


# Run tests
if __name__ == "__main__":
    unittest.main()
