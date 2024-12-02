import json
import unittest
from datetime import datetime, timezone

from pandaserver.api.v1.http_client import HttpClient, api_url, api_url_ssl


class TestHarvesterAPI(unittest.TestCase):
    def setUp(self):
        # Set up a mock TaskBuffer and initialize it
        self.http_client = HttpClient()

    # Add the unit tests for each API method here


# Run tests
if __name__ == "__main__":
    unittest.main()
