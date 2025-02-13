# TODO: the result of the tests depend on the cert/token used for the call. These particular results are for running with user pandasv1

import unittest
from datetime import datetime, timezone

from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl

# Get current UTC time with microseconds. The format needs to be compatible with the one used in the database
now_utc = datetime.now(timezone.utc)
formatted_time = now_utc.strftime("%d.%m.%y %H:%M:%S") + f".{now_utc.microsecond // 1000:02d}"

KEY = "test_key"
VALUE = "test_value"

NO_SSL_RESPONSE = {"success": False, "message": "SSL secure connection is required", "data": None}


class TestSecretManagementAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()
        self.urls = [api_url, api_url_ssl]  # Run tests with both base URLs

    def test_set_user_secrets(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/set_user_secrets"
                print(f"Testing URL: {full_url}")
                data = {"key": KEY, "value": VALUE}
                status, output = self.http_client.post(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    expected_response = {"success": True, "message": "OK", "data": None}
                self.assertEqual(output, expected_response)

    def test_get_user_secrets(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/get_user_secrets"
                print(f"Testing URL: {full_url}")
                data = {"keys": KEY}
                status, output = self.http_client.get(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    expected_response = {"success": True, "message": "", "data": '{"test_key": "test_value"}'}
                self.assertEqual(output, expected_response)

    def test_get_key_pair(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/get_key_pair"
                print(f"Testing URL: {full_url}")
                data = {"public_key_name": "a", "private_key_name": "b"}
                status, output = self.http_client.get(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    expected_response = {
                        "success": False,
                        "message": "Failed since 'pandasv1' not authorized with 'k' in ATLAS_PANDAMETA.USERS.GRIDPREF",
                        "data": None,
                    }
                self.assertEqual(output, expected_response)

    def test_get_proxy(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/get_proxy"
                print(f"Testing URL: {full_url}")
                data = {"role": "atlas", "dn": "atlpilo2"}
                status, output = self.http_client.get(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    expected_response = {"success": False, "message": "'proxy' not found for atlpilo2", "data": None}
                self.assertEqual(output, expected_response)

    def test_get_access_token(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/get_access_token"
                print(f"Testing URL: {full_url}")
                data = {"client_name": "pilot_server"}
                status, output = self.http_client.get(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    expected_response = {"success": False, "message": "failed since token key is invalid for pilot_server", "data": None}
                self.assertEqual(output, expected_response)

    def test_get_token_key(self):
        for url in self.urls:
            with self.subTest(base_url=url):
                full_url = f"{url}/creds/get_token_key"
                print(f"Testing URL: {full_url}")
                data = {"client_name": "pilot_server"}
                status, output = self.http_client.get(full_url, data)
                print(output)
                if url.startswith(api_url):
                    expected_response = NO_SSL_RESPONSE
                else:
                    del output["data"]
                    del output["message"]
                    expected_response = {"success": True}
                self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
