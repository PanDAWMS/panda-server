import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from pandaserver.api.http_client import HttpClient, base_url, base_url_ssl

# Get current UTC time with microseconds. The format needs to be compatible with the one used in the database
now_utc = datetime.now(timezone.utc)
formatted_time = now_utc.strftime("%d.%m.%y %H:%M:%S") + f".{now_utc.microsecond // 1000:02d}"


class TestHarvesterAPI(unittest.TestCase):
    def setUp(self):
        # Set up a mock TaskBuffer and initialize it
        self.mock_task_buffer = MagicMock()
        self.http_client = HttpClient()

    def test_update_workers(self):
        url = f"{base_url_ssl}/update_workers"
        worker = {
            "workerID": 1,
            "batchID": 1,
            "queueName": "queue1",
            "status": "running",
            "computingSite": "site1",
            "nCore": 1,
            "nodeID": None,
            "submitTime": formatted_time,
            "startTime": formatted_time,
            "endTime": None,
            "jobType": "managed",
            "resourceType": "SCORE",
            "nativeExitCode": None,
            "nativeStatus": None,
            "diagMessage": None,
            "nJobs": 1,
            "computingElement": "ce1",
            "syncLevel": 0,
            "submissionHost": "submissionhost1",
            "harvesterHost": "harvesterhost1",
            "errorCode": None,
            "minRamCount": 2000,
        }
        workers = [worker]

        harvester_id = "harvester_mock"
        data = {"harvester_id": harvester_id, "workers": workers}

        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, "Workers updated"]
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
