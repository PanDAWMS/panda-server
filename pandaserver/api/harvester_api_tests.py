import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from pandaserver.api.http_client import HttpClient, base_url, base_url_ssl


class TestHarvesterAPI(unittest.TestCase):
    def setUp(self):
        # Set up a mock TaskBuffer and initialize it
        self.mock_task_buffer = MagicMock()
        self.http_client = HttpClient()

    def test_update_workers(self):
        url = f"{base_url_ssl}/update_workers"
        harvester_id = "harvester_mock"
        workers = json.dumps([{"worker_id": "1", "status": "running"}])
        data = {"harvester_id": harvester_id, "workers": workers}

        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = True, "Workers updated"
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
