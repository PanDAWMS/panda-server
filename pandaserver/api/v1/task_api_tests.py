# Description: Unit tests for the Harvester API methods in the task module
import os
import unittest

from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl

# to run the tests with a real JEDI Task ID by setting the environment variable
JEDI_TASK_ID = os.environ.get("TEST_TASK_API_JEDI_TASK_ID", -1)


class TestTaskAPI(unittest.TestCase):
    def setUp(self):
        # Set up a mock TaskBuffer and initialize it
        self.http_client = HttpClient()

    # Add the unit tests for each API method here

    def test_retry(self):
        url = f"{api_url_ssl}/task/retry"
        print(f"Testing URL: {url}")
        data = {"jedi_task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        output["status"] = status
        output["message"] = ""

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        else:
            # Real task
            expected_response = {"status": 0, "success": True, "data": 0, "message": ""}
        self.assertEqual(output, expected_response)

    def test_enable_job_cloning(self):
        url = f"{api_url_ssl}/task/enable_job_cloning"
        print(f"Testing URL: {url}")
        data = {"jedi_task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_disable_job_cloning(self):
        url = f"{api_url_ssl}/task/disable_job_cloning"
        print(f"Testing URL: {url}")
        data = {"jedi_task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
