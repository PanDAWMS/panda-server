# Description: Unit tests for the Data Carousel API methods
import json
import os
import time
import unittest
import uuid
from datetime import datetime, timedelta
from unittest import mock

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl
from pandaserver.asyncprocess import data_carousel_handlers
from pandaserver.taskbuffer import data_carousel_ops

# to run the tests with a real Data Carousel request ID or dataset name by setting the environment variable
REQUEST_ID = int(os.environ.get("REQUEST_ID", None))
DATASET = str(os.environ.get("DATASET", None))

# how long to wait for the async request daemon to process a submitted request
POLL_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 5


class TestDataCarouselAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_change_staging_destination_by_request_id(self):
        url = f"{api_url_ssl}/data_carousel/change_staging_destination"
        print(f"Testing URL: {url}")
        data = {"request_id": REQUEST_ID}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("request_id"), REQUEST_ID)
            self.assertIsInstance(output["data"].get("new_request_id"), int)

    def test_change_staging_destination_by_dataset(self):
        url = f"{api_url_ssl}/data_carousel/change_staging_destination"
        print(f"Testing URL: {url}")
        data = {"dataset": DATASET}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("dataset"), DATASET)
            self.assertIsInstance(output["data"].get("new_request_id"), int)

    def test_change_staging_source_by_request_id(self):
        url = f"{api_url_ssl}/data_carousel/change_staging_source"
        print(f"Testing URL: {url}")
        data = {"request_id": REQUEST_ID}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("request_id"), REQUEST_ID)
            self.assertIsInstance(output["data"].get("ddm_rule_id"), str)

    def test_change_staging_source_by_dataset(self):
        url = f"{api_url_ssl}/data_carousel/change_staging_source"
        print(f"Testing URL: {url}")
        data = {"dataset": DATASET}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("dataset"), DATASET)
            self.assertIsInstance(output["data"].get("ddm_rule_id"), str)

    def test_force_to_staging_by_request_id(self):
        url = f"{api_url_ssl}/data_carousel/force_to_staging"
        print(f"Testing URL: {url}")
        data = {"request_id": REQUEST_ID}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("request_id"), REQUEST_ID)
            self.assertEqual(output["data"].get("status"), "staging")
            self.assertIsInstance(output["data"].get("ddm_rule_id"), str)

    def test_force_to_staging_by_dataset(self):
        url = f"{api_url_ssl}/data_carousel/force_to_staging"
        print(f"Testing URL: {url}")
        data = {"dataset": DATASET}
        status, output = self.http_client.post(url, data)
        print(status, output)
        if status == 0 and output.get("data"):
            self.assertEqual(output["data"].get("dataset"), DATASET)
            self.assertEqual(output["data"].get("status"), "staging")
            self.assertIsInstance(output["data"].get("ddm_rule_id"), str)


class TestDataCarouselAsyncAPI(unittest.TestCase):
    """Tests for the asynchronous flavour of the Data Carousel API; they need a live async request daemon."""

    def setUp(self):
        self.http_client = HttpClient()

    def _submit(self, method, data):
        url = f"{api_url_ssl}/data_carousel/{method}"
        print(f"Testing URL: {url}")
        status, output = self.http_client.post(url, data)
        print(status, output)
        return status, output

    def _assert_submitted(self, output):
        """Check the submit response and return the async_id."""
        self.assertTrue(output["success"])
        self.assertIsInstance(output["data"], dict)
        async_id = output["data"]["async_id"]
        self.assertIsInstance(async_id, str)
        self.assertEqual(len(async_id), 36)
        return async_id

    def _poll(self, async_id):
        """Poll get_result until the request reaches a terminal state, or skip the test on timeout."""
        url = f"{api_url_ssl}/data_carousel/get_result"
        print(f"Testing URL: {url}")
        deadline = time.time() + POLL_TIMEOUT_SECONDS
        while time.time() < deadline:
            status, output = self.http_client.get(url, {"async_id": async_id})
            print(status, output)
            self.assertTrue(output["success"])
            if output["data"]["status"] in ("done", "failed"):
                return output["data"]
            time.sleep(POLL_INTERVAL_SECONDS)
        raise unittest.SkipTest(f"async_id={async_id} not processed within {POLL_TIMEOUT_SECONDS} sec; is the async request daemon running?")

    def test_submit_without_target(self):
        status, output = self._submit("submit_force_to_staging", {})
        expected_response = {"success": False, "message": "either request_id or dataset must be provided", "data": None}
        self.assertEqual(output, expected_response)

    def test_get_result_not_found(self):
        missing_id = str(uuid.uuid4())
        url = f"{api_url_ssl}/data_carousel/get_result"
        print(f"Testing URL: {url}")
        status, output = self.http_client.get(url, {"async_id": missing_id})
        print(status, output)
        expected_response = {"success": False, "message": f"async_id '{missing_id}' not found", "data": None}
        self.assertEqual(output, expected_response)

    def test_submit_change_staging_destination_by_request_id(self):
        status, output = self._submit("submit_change_staging_destination", {"request_id": REQUEST_ID})
        async_id = self._assert_submitted(output)
        data = self._poll(async_id)
        self.assertEqual(data["status"], "done")
        if data["result"]["success"]:
            self.assertEqual(data["result"]["data"].get("request_id"), REQUEST_ID)
            self.assertIsInstance(data["result"]["data"].get("new_request_id"), int)

    def test_submit_change_staging_source_by_request_id(self):
        status, output = self._submit("submit_change_staging_source", {"request_id": REQUEST_ID})
        async_id = self._assert_submitted(output)
        data = self._poll(async_id)
        self.assertEqual(data["status"], "done")
        if data["result"]["success"]:
            self.assertEqual(data["result"]["data"].get("request_id"), REQUEST_ID)
            self.assertIsInstance(data["result"]["data"].get("ddm_rule_id"), str)

    def test_submit_force_to_staging_by_dataset(self):
        status, output = self._submit("submit_force_to_staging", {"dataset": DATASET})
        async_id = self._assert_submitted(output)
        data = self._poll(async_id)
        self.assertEqual(data["status"], "done")
        if data["result"]["success"]:
            self.assertEqual(data["result"]["data"].get("dataset"), DATASET)
            self.assertEqual(data["result"]["data"].get("status"), "staging")

    def test_submit_retire_unused_by_request_id(self):
        status, output = self._submit("submit_retire_unused", {"request_id": REQUEST_ID})
        async_id = self._assert_submitted(output)
        data = self._poll(async_id)
        self.assertEqual(data["status"], "done")
        if data["result"]["success"]:
            self.assertEqual(data["result"]["data"].get("request_id"), REQUEST_ID)


class TestDataCarouselTargetValidation(unittest.TestCase):
    """Unit tests for the argument validation shared by both flavours (no live server needed)."""

    def test_no_target(self):
        is_valid, err_msg = data_carousel_ops.validate_target(None, None)
        self.assertFalse(is_valid)
        self.assertEqual(err_msg, "either request_id or dataset must be provided")

    def test_non_numeric_request_id(self):
        is_valid, err_msg = data_carousel_ops.validate_target("not_a_number", None)
        self.assertFalse(is_valid)
        self.assertEqual(err_msg, "invalid request_id: not_a_number")

    def test_request_id_as_string_accepted(self):
        # parameters round-trip through JSON on the asynchronous path
        self.assertEqual(data_carousel_ops.validate_target("123", None), (True, ""))

    def test_dataset_only(self):
        self.assertEqual(data_carousel_ops.validate_target(None, "scope:dataset"), (True, ""))


class TestDataCarouselAsyncHandlers(unittest.TestCase):
    """Unit tests for the async request handlers (no live server or DB needed)."""

    def setUp(self):
        self.task_buffer = mock.MagicMock()
        self.tmp_logger = mock.MagicMock()
        self.row = {
            "request_id": "some-uuid",
            "parameters": json.dumps({"request_id": 123, "dataset": None, "requester": "alice", "access": "production"}),
        }
        # the handlers build the interface lazily; keep it out of the way
        self.dcif_patch = mock.patch.object(data_carousel_handlers, "_get_dcif", return_value=mock.MagicMock())
        self.dcif_patch.start()
        self.addCleanup(self.dcif_patch.stop)

    def _run_with_operation(self, operation):
        """Run the force_to_staging handler with the operation replaced by the given callable."""
        with mock.patch.dict(data_carousel_ops.OPERATIONS, {"force_to_staging": operation}):
            data_carousel_handlers.HANDLERS["dc_force_to_staging"](self.row, self.task_buffer, self.tmp_logger, "any")
        self.task_buffer.finish_async_result.assert_called_once()
        return self.task_buffer.finish_async_result.call_args

    def test_owner_and_access_not_passed_to_operation(self):
        operation = mock.MagicMock(return_value=(True, "ok", {"request_id": 123}))
        self._run_with_operation(operation)
        _, kwargs = operation.call_args
        self.assertEqual(kwargs, {"request_id": 123, "dataset": None})

    def test_success_stored_as_done(self):
        operation = mock.MagicMock(return_value=(True, "ok", {"request_id": 123}))
        args, kwargs = self._run_with_operation(operation)
        self.assertEqual(args[2], "done")
        self.assertEqual(json.loads(kwargs["result"]), {"success": True, "message": "ok", "data": {"request_id": 123}})

    def test_operation_failure_stored_as_done_and_never_retried(self):
        # a failed operation must be terminal: retrying would apply the DDM changes twice
        operation = mock.MagicMock(return_value=(False, "failed to get corresponding request", None))
        args, kwargs = self._run_with_operation(operation)
        self.assertEqual(args[2], "done")
        self.assertEqual(json.loads(kwargs["result"]), {"success": False, "message": "failed to get corresponding request", "data": None})
        self.assertNotIn("retriable", kwargs)

    def test_exception_stored_as_non_retriable_failure(self):
        operation = mock.MagicMock(side_effect=RuntimeError("boom"))
        args, kwargs = self._run_with_operation(operation)
        self.assertEqual(args[2], "failed")
        self.assertFalse(kwargs["retriable"])
        self.assertIn("boom", kwargs["error_msg"])

    def test_unknown_operation_stored_as_non_retriable_failure(self):
        with mock.patch.dict(data_carousel_ops.OPERATIONS, {}, clear=True):
            data_carousel_handlers.HANDLERS["dc_force_to_staging"](self.row, self.task_buffer, self.tmp_logger, "any")
        args, kwargs = self.task_buffer.finish_async_result.call_args
        self.assertEqual(args[2], "failed")
        self.assertFalse(kwargs["retriable"])


# Run tests
if __name__ == "__main__":
    unittest.main()
