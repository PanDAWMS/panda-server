# Description: Unit tests for the Pilot API methods
import os
import unittest
from datetime import datetime, timedelta

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl


class TestPilotAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_acquire_jobs(self):
        url = f"{api_url_ssl}/pilot/acquire_jobs"
        print(f"Testing URL: {url}")
        data = {
            "site_name": "CERN",
            "timeout": 60,
            "memory": 999999999,
            "disk_space": 999999999,
            "prod_source_label": "managed",
            "node": "aipanda120.cern.ch",
            "computing_element": "CERN",
            "prod_user_id": None,
            "get_proxy_key": None,
            "task_id": None,
            "n_jobs": 1,
            "background": False,
            "resource_type": "SCORE",
            "harvester_id": "imaginary_harvester",
            "worker_id": 12345,
            "scheduler_id": "imaginary_scheduler",
            "job_type": "user",
            "via_topic": False,
        }

        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        self.assertEqual(output, expected_response)

    def test_acquire_jobs(self):
        url = f"{api_url_ssl}/pilot/acquire_jobs"
        print(f"Testing URL: {url}")
        data = {
            "site_name": "CERN",
            "timeout": 60,
            "memory": 999999999,
            "disk_space": 999999999,
            "prod_source_label": "managed",
            "node": "aipanda120.cern.ch",
            "computing_element": "CERN",
            "prod_user_id": None,
            "get_proxy_key": None,
            "task_id": None,
            "n_jobs": 1,
            "background": False,
            "resource_type": "SCORE",
            "harvester_id": "imaginary_harvester",
            "worker_id": 12345,
            "scheduler_id": "imaginary_scheduler",
            "job_type": "user",
            "via_topic": False,
        }

        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        self.assertEqual(output, expected_response)

    def test_get_job_status(self):
        url = f"{api_url_ssl}/pilot/get_job_status"
        print(f"Testing URL: {url}")
        data = {
            "job_ids": "4674379299",
            "timeout": 60,
        }

        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        self.assertEqual(output, expected_response)

    def test_update_job(self):
        url = f"{api_url_ssl}/pilot/update_job"
        print(f"Testing URL: {url}")
        data = {"job_id": 4674379299, "job_status": "starting"}

        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        self.assertEqual(output, expected_response)

    def test_update_worker_node(self):
        url = f"{api_url_ssl}/pilot/update_worker_node"
        print(f"Testing URL: {url}")
        data = {
            "site": "CERN",
            "panda_queue": "CERN",
            "host_name": "slot1@wn1.cern.ch",
            "cpu_model": "AMD EPYC 7B12",
            "n_logical_cpus": 64,
            "n_sockets": 2,
            "cores_per_socket": 8,
            "threads_per_core": 2,
            "cpu_architecture": "x86_64",
            "cpu_architecture_level": "x86_64-v3",
            "clock_speed": 2.7,
            "total_memory": 3350,
            "total_local_disk": 75,
        }

        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True, "data": None, "message": "Inserted new worker node."}
        self.assertEqual(output, expected_response)

    def test_update_worker_node_gpu(self):
        url = f"{api_url_ssl}/pilot/update_worker_node_gpu"
        print(f"Testing URL: {url}")
        data = {
            "site": "CERN",
            "host_name": "slot1@lxplus940.cern.ch",
            "vendor": "NVIDIA",
            "model": "Tesla T4",
            "architecture": "Turing",
            "vram": 15360,
            "framework": "CUDA",
            "framework_version": "12.9",
            "driver_version": "575.51.03",
            "count": 1,
        }

        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status

        expected_response = {"status": 0, "success": True, "data": None, "message": "Inserted new worker node GPU."}
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
