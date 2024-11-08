import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from pandaserver.api.http_client import HttpClient, base_url, base_url_ssl

# Get current UTC time with microseconds. The format needs to be compatible with the one used in the database
now_utc = datetime.now(timezone.utc)
formatted_time = now_utc.strftime("%d.%m.%y %H:%M:%S") + f".{now_utc.microsecond // 1000:02d}"

HARVESTER_ID = "test_fbarreir"
HARVESTER_HOST = "test_fbarreir.cern.ch"
PANDA_QUEUE = "test_queue"


class TestHarvesterAPI(unittest.TestCase):
    def setUp(self):
        # Set up a mock TaskBuffer and initialize it
        self.http_client = HttpClient()

    def test_update_harvester_service_metrics(self):
        url = f"{base_url_ssl}/update_harvester_service_metrics"
        harvester_id = HARVESTER_ID
        harvester_host = HARVESTER_HOST
        creation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        metric = {
            "rss_mib": 2737.36,
            "memory_pc": 39.19,
            "cpu_pc": 15.23,
            "volume_data_pc": 20.0,
            "cert_lifetime": {
                "/data/atlpan/proxy/x509up_u25606_prod": 81,
                "/data/atlpan/proxy/x509up_u25606_pilot": 81,
                "/cephfs/atlpan/harvester/proxy/x509up_u25606_prod": 96,
                "/cephfs/atlpan/harvester/proxy/x509up_u25606_pilot": 96,
            },
        }

        # DBProxy expects the metrics in json format and stores them directly in the database
        metrics = [[creation_time, harvester_host, json.dumps(metric)]]

        data = {"harvester_id": harvester_id, "metrics": metrics}
        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, [True]]
        self.assertEqual(output, expected_response)

    def test_add_harvester_dialogs(self):
        url = f"{base_url_ssl}/add_harvester_dialogs"
        harvester_id = HARVESTER_ID

        dialog = {
            "diagID": 1,
            "moduleName": "test_module",
            "identifier": "test identifier",
            "creationTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "messageLevel": "INFO",
            "diagMessage": "test message",
        }

        dialogs = [dialog]

        data = {"harvester_id": harvester_id, "dialogs": dialogs}
        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, True]
        self.assertEqual(output, expected_response)

    def test_harvester_heartbeat(self):
        url = f"{base_url_ssl}/harvester_heartbeat"
        harvester_id = HARVESTER_ID
        data = {"harvester_id": harvester_id, "data": []}
        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, "succeeded"]
        self.assertEqual(output, expected_response)

    def test_get_worker_statistics(self):
        url = f"{base_url_ssl}/get_worker_statistics"
        data = {}
        status, output = self.http_client.post(url, data)
        expected_response = [True, {}]

        # the statistics can't be predicted, so we just check the type of the response
        self.assertEqual(expected_response[0], output[0])
        self.assertEqual(type(expected_response[1]), type(output[1]))

    def test_report_worker_statistics(self):
        url = f"{base_url_ssl}/report_worker_statistics"
        harvester_id = HARVESTER_ID
        panda_queue = PANDA_QUEUE
        # the json loads is done in DBProxy
        statistics = json.dumps({"user": {"SCORE": {"running": 1, "submitted": 1}}, "managed": {"MCORE": {"running": 1, "submitted": 1}}})
        data = {"harvester_id": harvester_id, "panda_queue": panda_queue, "statistics": statistics}
        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, "OK"]
        self.assertEqual(output, expected_response)

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
            "submitTime": "02-NOV-24 00:02:18",
            "startTime": "02-NOV-24 00:02:18",
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

        harvester_id = HARVESTER_ID
        data = {"harvester_id": harvester_id, "workers": workers}

        status, output = self.http_client.post(url, data)

        # Assert
        expected_response = [True, [True]]
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
