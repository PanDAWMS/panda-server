import json
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
        # Set up a mock TaskBuffer and initialize it
        self.http_client = HttpClient()

    def test_set_user_secrets(self):
        url = f"{base_url_ssl}/set_user_secrets"

        key = KEY
        value = VALUE

        data = {"key": key, "value": value}
        status, output = self.http_client.post(url, data)

        expected_response = [True, "OK"]
        self.assertEqual(output, expected_response)

    def test_get_user_secrets(self):
        url = f"{base_url_ssl}/get_user_secrets"

        data = {"keys": KEY}
        status, output = self.http_client.post(url, data)

        expected_response = [True, {KEY: VALUE}]
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
