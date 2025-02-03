import unittest
from datetime import datetime, timezone

from pandaserver.api.v1.http_client import HttpClient, base_url, base_url_ssl

# Get current UTC time with microseconds. The format needs to be compatible with the one used in the database
now_utc = datetime.now(timezone.utc)
formatted_time = now_utc.strftime("%d.%m.%y %H:%M:%S") + f".{now_utc.microsecond // 1000:02d}"

KEY = "test_key"
VALUE = "test_value"


class TestSecretManagementAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()
        self.urls = [base_url, base_url_ssl]  # Run tests with both base URLs

    def test_set_user_secrets(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/set_user_secrets"
                print(f"Testing URL: {full_url}")
                data = {"key": KEY, "value": VALUE}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, "OK"]
                self.assertEqual(output, expected_response)

    def test_get_user_secrets(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/get_user_secrets"
                print(f"Testing URL: {full_url}")
                data = {"keys": KEY}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, {KEY: VALUE}]
                self.assertEqual(output, expected_response)

    def test_get_key_pair(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/get_key_pair"
                print(f"Testing URL: {full_url}")
                data = {"public_key_name": "a", "private_key_name": "b"}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, {KEY: VALUE}]
                self.assertEqual(output, expected_response)

    def test_get_proxy(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/get_proxy"
                print(f"Testing URL: {full_url}")
                data = {"role": "atlas", "dn": "atlpilo2"}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, {KEY: VALUE}]
                self.assertEqual(output, expected_response)

    def test_get_access_token(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/get_access_token"
                print(f"Testing URL: {full_url}")
                data = {"client_name": "pilot_server"}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, {KEY: VALUE}]
                self.assertEqual(output, expected_response)

    def test_get_token_key(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/get_token_key"
                print(f"Testing URL: {full_url}")
                data = {"client_name": "pilot_server"}
                status, output = self.http_client.post(full_url, data)
                print(output)
                expected_response = [True, {KEY: VALUE}]
                self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
