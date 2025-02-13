# Description: Unit tests for the Pilot API methods
import os
import unittest
from datetime import datetime, timedelta

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl


class TestTaskAPI(unittest.TestCase):
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


"""
def get_job_status(req: PandaRequest, job_ids: str, timeout: int = 60) -> dict:
    pass

def update_job(
        req: PandaRequest,
        job_id: int,
        job_status: str,
        trans_exit_code: str = None,
        pilot_error_code: str = None,
        pilot_error_diag: str = None,
        timeout: int = 60,
        job_output_report: str = "",
        node: str = None,
        cpu_consumption_time: int = None,
        cpu_consumption_unit: str = None,
        scheduler_id: str = None,
        pilot_id: str = None,
        site_name: str = None,
        pilot_log: str = "",
        meta_data: str = "",
        cpu_conversion_factor: float = None,
        exe_error_code: int = None,
        exe_error_diag: str = None,
        pilot_timing: str = None,
        start_time: str = None,
        end_time: str = None,
        n_events: int = None,
        n_input_files: int = None,
        batch_id: str = None,
        attempt_nr: int = None,
        job_metrics: str = None,
        stdout: str = "",
        job_sub_status: str = None,
        core_count: int = None,
        max_rss: int = None,
        max_vmem: int = None,
        max_swap: int = None,
        max_pss: int = None,
        avg_rss: int = None,
        avg_vmem: int = None,
        avg_swap: int = None,
        avg_pss: int = None,
        tot_rchar: int = None,
        tot_wchar: int = None,
        tot_rbytes: int = None,
        tot_wbytes: int = None,
        rate_rchar: int = None,
        rate_wchar: int = None,
        rate_rbytes: int = None,
        rate_wbytes: int = None,
        corrupted_files: str = None,
        mean_core_count: int = None,
        cpu_architecture_level: int = None,
):
    pass

def update_jobs_bulk(req, job_list: List, harvester_id: str = None):
    pass
"""

# Run tests
if __name__ == "__main__":
    unittest.main()
