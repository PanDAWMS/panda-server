# Description: Unit tests for the Data Carousel API methods
import os
import unittest
from datetime import datetime, timedelta

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl

# to run the tests with a real Data Carousel request ID or dataset name by setting the environment variable
REQUEST_ID = int(os.environ.get("REQUEST_ID", None))
DATASET = str(os.environ.get("DATASET", None))


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


# Run tests
if __name__ == "__main__":
    unittest.main()
