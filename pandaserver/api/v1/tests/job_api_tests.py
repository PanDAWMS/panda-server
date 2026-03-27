# Description: Unit tests for the Job API methods
import os
import unittest

from pandaserver.api.v1.http_client import HttpClient, api_url_ssl

# To run tests with real IDs, set environment variables:
#   PANDA_JOB_ID_TEST=<job_id>  JEDI_TASK_ID_TEST=<task_id>
PANDA_JOB_ID = int(os.environ.get("PANDA_JOB_ID_TEST", -1))
JEDI_TASK_ID = int(os.environ.get("JEDI_TASK_ID_TEST", -1))


class TestJobAPI(unittest.TestCase):
    def setUp(self):
        self.http_client = HttpClient()

    def test_get_status(self):
        # def get_status(req: PandaRequest, job_ids: List[int], timeout: int = 60) -> Dict:
        url = f"{api_url_ssl}/job/get_status"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless of whether job exists (returns empty or None entries)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_description(self):
        # def get_description(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/get_description"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless (returns [None] for unknown job IDs)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_description_incl_archive(self):
        # def get_description_incl_archive(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/get_description_incl_archive"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # Returns success=True regardless (returns [None] for unknown job IDs)
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_get_metadata_for_analysis_jobs(self):
        # def get_metadata_for_analysis_jobs(req: PandaRequest, task_id: int) -> Dict:
        url = f"{api_url_ssl}/job/get_metadata_for_analysis_jobs"
        print(f"Testing URL: {url}")
        data = {"task_id": JEDI_TASK_ID}
        status, output = self.http_client.get(url, data)
        print(output)
        output["status"] = status
        del output["data"]

        if JEDI_TASK_ID == -1:
            # Fake task returns success=True with "No metadata found" message
            expected_response = {"status": 0, "success": True, "message": "No metadata found"}
        else:
            # Real task
            expected_response = {"status": 0, "success": True, "message": ""}
            output["message"] = ""
        self.assertEqual(output, expected_response)

    def test_kill(self):
        # def kill(req, job_ids: List[int], code: int = None, ...) -> Dict:
        url = f"{api_url_ssl}/job/kill"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["data"]
        del output["message"]

        # killJobs always returns success=True
        expected_response = {"status": 0, "success": True}
        self.assertEqual(output, expected_response)

    def test_reassign(self):
        # def reassign(req: PandaRequest, job_ids: List[int]) -> Dict:
        url = f"{api_url_ssl}/job/reassign"
        print(f"Testing URL: {url}")
        data = {"job_ids": [PANDA_JOB_ID]}
        status, output = self.http_client.post(url, data)
        print(output)
        output["status"] = status
        del output["message"]

        # reassignJobs always returns success=True
        expected_response = {"status": 0, "success": True, "data": None}
        self.assertEqual(output, expected_response)


if __name__ == "__main__":
    unittest.main()
