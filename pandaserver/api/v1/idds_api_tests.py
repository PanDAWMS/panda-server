# Description: Unit tests for the IDDS API methods
# TODO: These tests are placeholders and need to be completed
import unittest

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl


class TestIDDSAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_relay_idds_command(self):
        url = f"{api_url_ssl}/idds/relay_idds_command"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_execute_idds_workflow_command(self):
        url = f"{api_url_ssl}/statistics/execute_idds_workflow_command"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
