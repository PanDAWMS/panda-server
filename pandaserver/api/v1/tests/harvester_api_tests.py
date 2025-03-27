import json
import unittest
from datetime import datetime, timezone

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl

# Get current UTC time with microseconds. The format needs to be compatible with the one used in the database
now_utc = datetime.now(timezone.utc)
formatted_time = now_utc.strftime("%d.%m.%y %H:%M:%S") + f".{now_utc.microsecond // 1000:02d}"

HARVESTER_ID = "test_fbarreir"
HARVESTER_HOST = "test_fbarreir.cern.ch"
PANDA_QUEUE = "test_queue"


class TestHarvesterAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_update_service_metrics(self):
        url = f"{api_url_ssl}/harvester/update_service_metrics"
        print(f"Testing URL: {url}")
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
        print(output)
        expected_response = {"success": True, "message": "", "data": [True]}
        self.assertEqual(output, expected_response)

    def test_add_dialogs(self):
        url = f"{api_url_ssl}/harvester/add_dialogs"
        print(f"Testing URL: {url}")
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
        print(output)
        expected_response = {"success": True, "message": "", "data": None}
        self.assertEqual(output, expected_response)

    def test_heartbeat(self):
        url = f"{api_url_ssl}/harvester/heartbeat"
        print(f"Testing URL: {url}")
        harvester_id = HARVESTER_ID
        data = {"harvester_id": harvester_id, "data": []}
        status, output = self.http_client.post(url, data)

        expected_response = {"success": True, "message": "", "data": None}
        self.assertEqual(output, expected_response)

    def test_get_worker_statistics(self):
        url = f"{api_url_ssl}/harvester/get_worker_statistics"
        print(f"Testing URL: {url}")
        data = {}
        status, output = self.http_client.get(url, data)
        print(output)
        # the statistics can't be predicted, so we just check the type of the response
        self.assertEqual(True, output["success"])
        self.assertEqual(dict, type(output["data"]))

    def test_get_current_worker_id(self):
        url = f"{api_url_ssl}/harvester/get_current_worker_id"
        print(f"Testing URL: {url}")
        data = {"harvester_id": HARVESTER_ID}
        status, output = self.http_client.get(url, data)
        print(output)
        # the current/max worker id can't be predicted, so we just check the type of the response
        self.assertEqual(True, output["success"])
        self.assertEqual(int, type(output["data"]))

    def test_report_worker_statistics(self):
        url = f"{api_url_ssl}/harvester/report_worker_statistics"
        print(f"Testing URL: {url}")
        harvester_id = HARVESTER_ID
        panda_queue = PANDA_QUEUE
        # the json loads is done in DBProxy
        statistics = json.dumps({"user": {"SCORE": {"running": 1, "submitted": 1}}, "managed": {"MCORE": {"running": 1, "submitted": 1}}})
        data = {"harvester_id": harvester_id, "panda_queue": panda_queue, "statistics": statistics}
        status, output = self.http_client.post(url, data)
        print(output)
        expected_response = {"success": True, "message": "OK", "data": None}
        self.assertEqual(output, expected_response)

    def test_update_workers(self):
        url = f"{api_url_ssl}/harvester/update_workers"
        print(f"Testing URL: {url}")
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
        print(output)
        expected_response = {"success": True, "message": "", "data": [True]}

        self.assertEqual(output, expected_response)

    def test_acquire_commands(self):
        url = f"{api_url_ssl}/harvester/acquire_commands"
        print(f"Testing URL: {url}")

        harvester_id = HARVESTER_ID
        n_commands = 1
        data = {"harvester_id": harvester_id, "n_commands": n_commands}
        status, output = self.http_client.post(url, data)
        print(output)
        # the commands can't be predicted, so we just check the type of the response
        self.assertEqual(True, output["success"])
        self.assertEqual(list, type(output["data"]))

    def test_acknowledge_commands(self):
        url = f"{api_url_ssl}/harvester/acknowledge_commands"
        print(f"Testing URL: {url}")
        command_ids = [1]
        data = {"command_ids": command_ids}
        status, output = self.http_client.post(url, data)
        print(output)
        expected_response = {"success": True, "message": "", "data": None}
        self.assertEqual(output, expected_response)

    def test_add_target_slots(self):
        url = f"{api_url_ssl}/harvester/add_target_slots"
        print(f"Testing URL: {url}")
        panda_queue = PANDA_QUEUE
        slots = 0
        data = {"panda_queue": panda_queue, "slots": slots}
        status, output = self.http_client.post(url, data)
        print(output)
        self.assertEqual(True, output["success"])
        self.assertEqual(None, output["data"])


# Run tests
if __name__ == "__main__":
    unittest.main()
