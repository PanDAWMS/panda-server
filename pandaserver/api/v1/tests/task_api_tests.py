# Description: Unit tests for the Task API methods
import os
import unittest
from datetime import datetime, timedelta

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl

# to run the tests with a real JEDI Task ID by setting the environment variable
JEDI_TASK_ID = os.environ.get("JEDI_TASK_ID_TEST", -1)


class TestTaskAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_retry(self):
        url = f"{api_url_ssl}/task/retry"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        output["message"] = ""

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False, "data": 2, "message": ""}
        else:
            # Real task
            expected_response = {"status": 0, "success": True, "data": 0, "message": ""}
        self.assertEqual(output, expected_response)

    def test_enable_jumbo_jobs(self):
        url = f"{api_url_ssl}/task/enable_jumbo_jobs"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "jumbo_jobs_total": 1}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_enable_job_cloning(self):
        url = f"{api_url_ssl}/task/enable_job_cloning"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_disable_job_cloning(self):
        url = f"{api_url_ssl}/task/disable_job_cloning"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_increase_attempts(self):
        url = f"{api_url_ssl}/task/increase_attempts"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_resume(self):
        url = f"{api_url_ssl}/task/resume"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_release(self):
        url = f"{api_url_ssl}/task/release"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reassign(self):
        # reassign(req: PandaRequest, task_id: int, site: str = None, cloud: str = None, nucleus: str = None, soft: bool = None, mode: str = None):
        url = f"{api_url_ssl}/task/reassign"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "site": "CERN"}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_pause(self):
        url = f"{api_url_ssl}/task/pause"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_kill(self):
        url = f"{api_url_ssl}/task/kill"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_finish(self):
        # def finish(req: PandaRequest, task_id: int, soft: bool = False, broadcast: bool = False) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/finish"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "soft": False, "broadcast": False}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reactivate(self):
        # def reactivate(req: PandaRequest, task_id: int, keep_attempt_nr: bool = False, trigger_job_generation: bool = False) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/reactivate"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # This method does not distinguish between real and fake tasks
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_avalanche(self):
        url = f"{api_url_ssl}/task/avalanche"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reload_input(self):
        url = f"{api_url_ssl}/task/reload_input"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reassign_global_share(self):
        url = f"{api_url_ssl}/task/reassign_global_share"
        print(f"Testing URL: {url}")
        data = {"task_id_list": [JEDI_TASK_ID], "share": "Test", "reassign_running_jobs": False}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_status(self):
        url = f"{api_url_ssl}/task/get_status"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_details(self):
        url = f"{api_url_ssl}/task/get_details"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "include_parameters": False, "include_status": False}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_change_attribute(self):
        url = f"{api_url_ssl}/task/change_attribute"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "attribute_name": "coreCount", "value": 8}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_change_modification_time(self):
        url = f"{api_url_ssl}/task/change_modification_time"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "positive_hour_offset": 1}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_change_priority(self):
        url = f"{api_url_ssl}/task/change_priority"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "priority": 1}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_change_split_rule(self):
        url = f"{api_url_ssl}/task/change_split_rule"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "attribute_name": "AI", "value": 1}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_tasks_modified_since(self):
        # def get_tasks_modified_since(req, since: str, dn: str = "", full: bool = False, min_task_id: int = None, prod_source_label: str = "user") -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/get_tasks_modified_since"
        print(f"Testing URL: {url}")

        # generate the date one week ago
        one_week_ago = datetime.now() - timedelta(weeks=1)
        one_week_ago_str = one_week_ago.strftime("%Y-%m-%d %H:%M:%S")

        data = {"since": one_week_ago_str, "dn": "test", "full": False, "min_task_id": 1, "prod_source_label": "user"}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_datasets_and_files(self):
        # def get_datasets_and_files(req, task_id, dataset_types: List = ("input", "pseudo_input")) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/get_datasets_and_files"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID, "dataset_types": ["input", "inexisting_type"]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_job_ids(self):
        # def get_job_ids(req: PandaRequest, task_id: int) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/get_job_ids"
        print(f"Testing URL: {url}")
        data = {"task_id": [JEDI_TASK_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # This method does not distinguish between real and fake tasks
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_insert_task_parameters(self):
        # def insert_task_parameters(req: PandaRequest, task_parameters: Dict, parent_tid: int = None) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/insert_task_parameters"
        print(f"Testing URL: {url}")
        data = {"task_parameters": {"test": 1, "taskName": "test_task"}, "parent_tid": 1}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_task_parameters(self):
        # def get_task_parameters(req: PandaRequest, task_id: int) -> Dict[str, Any]:
        url = f"{api_url_ssl}/task/get_task_parameters"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        if JEDI_TASK_ID == -1:
            # Fake task should not be found
            expected_response = {"status": 0, "success": False}
        else:
            # Real task
            expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)


# Run tests
if __name__ == "__main__":
    unittest.main()
