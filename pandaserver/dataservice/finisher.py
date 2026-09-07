"""
finish transferring jobs

"""

import datetime
import json
import re
import sys
import threading
from typing import TYPE_CHECKING, Any, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

if TYPE_CHECKING:
    # TaskBuffer imports this package, so naming it for real here would close the cycle.
    # Annotations are evaluated at runtime in this tree, so the uses below are quoted.
    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

# logger
_logger = PandaLogger().getLogger("finisher")


class Finisher(threading.Thread):
    """
    A class used to finish transferring jobs

    Attributes
    ----------
    dataset : DatasetSpec
        The dataset to be transferred
    taskBuffer : TaskBuffer
        The task buffer that contains the jobs
    job : JobSpec, optional
        The job to be transferred (default is None)
    site : str, optional
        The site where the job is to be transferred (default is None)

    Methods
    -------
    run():
        Starts the thread to finish transferring jobs
    """

    # constructor
    def __init__(self, taskBuffer: "TaskBuffer", dataset: DatasetSpec | None, job: JobSpec | None = None, site: str | None = None) -> None:
        """
        Constructs all the necessary attributes for the Finisher object.

        Parameters
        ----------
            taskBuffer : TaskBuffer
                The task buffer that contains the jobs
            dataset : DatasetSpec
                The dataset to be transferred
            job : JobSpec, optional
                The job to be transferred (default is None)
            site : str, optional
                The site where the job is to be transferred (default is None)
        """
        threading.Thread.__init__(self)
        self.dataset: DatasetSpec | None = dataset
        self.task_buffer = taskBuffer
        self.job = job
        self.site = site

    def create_json_doc(self, job: JobSpec, failed_files: List[str], no_out_files: List[str]) -> str:
        """
        This function creates a JSON document for the jobs.

        Parameters:
        job (JobSpec): The job specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.

        Returns:
        str: The created JSON document as a string.
        """
        json_dict: dict[str, dict[str, Any]] = {}
        for file in job.Files:
            if file.type in ["output", "log"]:
                # skip failed or no-output files
                if file.lfn in failed_files + no_out_files:
                    continue
                file_dict = {
                    "guid": file.GUID,
                    "fsize": file.fsize,
                    "full_lfn": file.lfn,
                }
                if file.checksum.startswith("ad:"):
                    file_dict["adler32"] = re.sub("^ad:", "", file.checksum)
                else:
                    file_dict["md5sum"] = re.sub("^md5:", "", file.checksum)
                json_dict[file.lfn] = file_dict
        return json.dumps(json_dict)

    def update_job_output_report(self, job: JobSpec, failed_files: List[str], no_out_files: List[str]) -> None:
        """
        This function updates the job output report.

        Parameters:
        job (JobSpec): The job specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.
        """
        json_data = self.create_json_doc(job, failed_files, no_out_files)
        record_status = "finished" if not failed_files else "failed"
        tmp_ret = self.task_buffer.updateJobOutputReport(
            panda_id=job.PandaID,
            attempt_nr=job.attemptNr,
            data=json_data,
        )
        if not tmp_ret:
            self.task_buffer.insertJobOutputReport(
                panda_id=job.PandaID,
                prod_source_label=job.prodSourceLabel,
                job_status=record_status,
                attempt_nr=job.attemptNr,
                data=json_data,
            )

    def check_file_status(self, job: JobSpec) -> tuple[bool, list[str], list[str]]:
        """
        This function checks the status of the files for the job.

        Parameters:
        job (JobSpec): The job specification.

        Returns:
        tuple: A tuple containing three elements:
            - bool: True if all files are ready, False otherwise.
            - list: A list of failed files.
            - list: A list of files with no output.
        """
        tmp_log = LogWrapper(_logger, f"check_file_status-{naive_utcnow().isoformat('/')}")
        failed_files: list[str] = []
        no_out_files: list[str] = []
        for file in job.Files:
            if file.type in ("output", "log"):
                if file.status == "failed":
                    failed_files.append(file.lfn)
                elif file.status == "nooutput":
                    no_out_files.append(file.lfn)
                elif file.status != "ready":
                    tmp_log.debug(f"Job: {job.PandaID} file:{file.lfn} {file.status} != ready")
                    return False, failed_files, no_out_files
        return True, failed_files, no_out_files

    # main
    def run(self) -> None:
        """
        Starts the thread to finish transferring jobs
        """
        tmp_log = LogWrapper(_logger, f"run-{naive_utcnow().isoformat('/')}")
        # start
        try:
            by_call_back = False
            dataset = self.dataset
            # a Finisher is made either for a job or for a dataset, and what it is made for
            # names it in the start and end messages
            if self.job is not None:
                label = str(self.job.PandaID)
                tmp_log.debug(f"start: {label}")
                panda_ids = [self.job.PandaID]
                jobs = [self.job]
            elif dataset is not None:
                by_call_back = True
                label = dataset.name
                tmp_log.debug(f"start: {label}")
                panda_ids = self.task_buffer.updateOutFilesReturnPandaIDs(dataset.name)
                # set flag for T2 cleanup
                dataset.status = "cleanup"
                self.task_buffer.updateDatasets([dataset])
                jobs = self.task_buffer.peekJobs(panda_ids, fromDefined=False, fromArchived=False, fromWaiting=False)
            else:
                tmp_log.error("neither a job nor a dataset was given")
                return

            tmp_log.debug(f"IDs: {panda_ids}")
            if len(panda_ids) != 0:
                # loop over all jobs
                for job in jobs:
                    if job is None or job.jobStatus != "transferring":
                        continue
                    job_ready, failed_files, no_out_files = self.check_file_status(job)
                    # finish job
                    if job_ready:
                        if by_call_back:
                            tmp_log.debug(f"Job: {job.PandaID} all files ready")
                        else:
                            tmp_log.debug(f"Job: {job.PandaID} all files checked with catalog")
                        # create JSON
                        try:
                            self.update_job_output_report(job, failed_files, no_out_files)
                        except Exception:
                            exc_type, value, _ = sys.exc_info()
                            tmp_log.error(f"Job: {job.PandaID} {exc_type} {value}")
                    tmp_log.debug(f"Job: {job.PandaID} status: {job.jobStatus}")
            tmp_log.debug(f"end: {label}")
        except Exception:
            exc_type, value, _ = sys.exc_info()
            tmp_log.error(f"run() : {exc_type} {value}")
