# Description: Unit tests for the System API methods
import unittest

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl


class TestSystemAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_get_attributes(self):
        url = f"{api_url_ssl}/system/get_attributes"
        print(f"Testing URL: {url}")
        data = {"test_key_string": "test_value", "test_key_int": 12345}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_user_attributes(self):
        url = f"{api_url_ssl}/system/get_user_attributes"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_voms_attributes(self):
        url = f"{api_url_ssl}/system/get_voms_attributes"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_is_alive(self):
        url = f"{api_url_ssl}/system/is_alive"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
