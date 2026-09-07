# Description: Unit tests for the Statistics API methods
import unittest
from typing import Any

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl


class TestTaskAPI(unittest.TestCase):
    def setUp(self) -> None:
        self.http_client = HttpClient()

    def test_job_stats_by_cloud(self) -> None:
        url = f"{api_url_ssl}/statistics/job_stats_by_cloud"
        print(f"Testing URL: {url}")
        data = {"type": "production"}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_production_job_stats_by_cloud_and_processing_type(self) -> None:
        url = f"{api_url_ssl}/statistics/production_job_stats_by_cloud_and_processing_type"
        print(f"Testing URL: {url}")
        data: dict[str, Any] = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_active_job_stats_by_site(self) -> None:
        url = f"{api_url_ssl}/statistics/active_job_stats_by_site"
        print(f"Testing URL: {url}")
        data: dict[str, Any] = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_active_job_detailed_stats_by_site(self) -> None:
        url = f"{api_url_ssl}/statistics/active_job_detailed_stats_by_site"
        print(f"Testing URL: {url}")
        data: dict[str, Any] = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_job_stats_by_site_and_resource_type(self) -> None:
        url = f"{api_url_ssl}/statistics/job_stats_by_site_and_resource_type"
        print(f"Testing URL: {url}")
        data = {"time_window": 12 * 60}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        # Remove the data field from the response for comparison
        del output["data"]

        expected_response = {"success": True, "message": "", "status": 0}

        self.assertEqual(output, expected_response)

    def test_job_stats_by_site_share_and_resource_type(self) -> None:
        url = f"{api_url_ssl}/statistics/job_stats_by_site_share_and_resource_type"
        print(f"Testing URL: {url}")
        data = {"time_window": 12 * 60}
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
