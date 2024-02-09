"""
finish transferring jobs

"""

import sys
import threading
import datetime
import json


from typing import List
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.brokerage.SiteMapper import SiteMapper

# logger
_logger = PandaLogger().getLogger("Finisher")


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
    def __init__(self, taskBuffer, dataset, job: str = None, site: str = None):
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
        self.dataset = dataset
        self.task_buffer = taskBuffer
        self.job = job
        self.site = site

    def create_json_doc(self, job, failed_files: List[str], no_out_files: List[str]):
        """
        This function creates a JSON document for the jobs.

        Parameters:
        job (JobSpec): The job specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.

        Returns:
        str: The created JSON document as a string.
        """
        json_dict = {"files": {"output": []}}
        for file in job.Files:
            if file.lfn in failed_files + no_out_files:
                continue
            file_dict = {
                "name": file.lfn,
                "file_guid": file.GUID,
                "fsize": file.fsize,
                "surl": file.surl,
                "full_lfn": file.lfn,
                "endpoint": [],
            }
            if file.checksum.startswith("ad:"):
                file_dict["adler32"] = file.checksum
            else:
                file_dict["md5sum"] = "md5:" + file.checksum
            job_dict = {"subFiles": [file_dict]}
            json_dict["files"]["output"].append(job_dict)
        return json.dumps(json_dict)

    def update_job_output_report(self, job, failed_files: List[str], no_out_files: List[str]):
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

    def check_file_status(self, job):
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
        tmp_log = LogWrapper(_logger,
                             f"check_file_status-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        failed_files = []
        no_out_files = []
        for file in job.Files:
            if file.type in ('output', 'log'):
                if file.status == "failed":
                    failed_files.append(file.lfn)
                elif file.status == "nooutput":
                    no_out_files.append(file.lfn)
                elif file.status != "ready":
                    tmp_log.debug(f"Job: {job.PandaID} file:{file.lfn} {file.status} != ready")
                    return False, failed_files, no_out_files
        return True, failed_files, no_out_files

    # main
    def run(self):
        """
        Starts the thread to finish transferring jobs
        """
        tmp_log = LogWrapper(_logger,
                             f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        # start
        try:
            by_call_back = False
            if self.job is None:
                by_call_back = True
                tmp_log.debug(f"start: {self.dataset.name}")
                tmp_log.debug(f"callback from {self.site}")
                # instantiate site mapper
                site_mapper = SiteMapper(self.task_buffer)
                # get computing_site/destination_se
                computing_site, destination_se = self.task_buffer.getDestSE(self.dataset.name)
                if destination_se is None:
                    # try to get computing_site/destination_se from ARCH to delete sub
                    # even if no active jobs left
                    computing_site, destination_se = self.task_buffer.getDestSE(self.dataset.name, True)
                    if destination_se is None:
                        tmp_log.error(f"cannot get source/destination for {self.dataset.name}")
                        tmp_log.debug(f"end: {self.dataset.name}")
                        return
                tmp_log.debug(f"src: {computing_site}")
                tmp_log.debug(f"dst: {destination_se}")
                # get corresponding token
                tmp_source_site_spec = site_mapper.getSite(computing_site)
                tmp_dst_site_spec = site_mapper.getSite(destination_se)
                tmp_log.debug(tmp_dst_site_spec.setokens_output)
                dest_token = None
                for scope in tmp_dst_site_spec.se_tokens_output:
                    for se_token in tmp_dst_site_spec.setokens_output[scope]:
                        for tmp_ddm_id in tmp_dst_site_spec.setokens_output[scope][se_token]:
                            if self.site == tmp_ddm_id:
                                dest_token = se_token
                                break
                tmp_log.debug(f"use Token={dest_token}")
                # get required tokens
                required_tokens = self.task_buffer.getDestTokens(self.dataset.name)
                if required_tokens is None:
                    tmp_log.error(f"cannot get required token for {self.dataset.name}")
                    tmp_log.debug(f"end: {self.dataset.name}")
                    return
                tmp_log.debug(f"req Token={required_tokens}")
                # make bit_map for the token
                bit_map = 1
                if len(required_tokens.split(",")) > 1:
                    for tmp_req_token in required_tokens.split(","):
                        if tmp_req_token == dest_token:
                            break
                        # shift one bit
                        bit_map <<= 1
                # completed bit_map
                comp_bit_map = (1 << len(required_tokens.split(","))) - 1
                # ignore the lowest bit for T1, file on DISK is already there
                if tmp_source_site_spec.ddm_output == tmp_dst_site_spec.ddm_output:
                    comp_bit_map = comp_bit_map & 0xFFFE
                # update bit_map in DB
                updated_bit_map = self.task_buffer.updateTransferStatus(self.dataset.name, bit_map)
                tmp_log.debug(f"transfer status:{hex(updated_bit_map)} - comp:{hex(comp_bit_map)} - bit:{hex(bit_map)}")
                # update output files
                if (updated_bit_map & comp_bit_map) == comp_bit_map:
                    panda_ids = self.task_buffer.updateOutFilesReturnPandaIDs(self.dataset.name)
                    # set flag for T2 cleanup
                    self.dataset.status = "cleanup"
                    self.task_buffer.updateDatasets([self.dataset])
                else:
                    tmp_log.debug(f"end: {self.dataset.name}")
                    return
            else:
                tmp_log.debug(f"start: {self.job.PandaID}")
                # update input files
                panda_ids = [self.job.PandaID]
            tmp_log.debug(f"IDs: {panda_ids}")
            if len(panda_ids) != 0:
                # get job
                if self.job is None:
                    jobs = self.task_buffer.peekJobs(panda_ids, fromDefined=False,
                                                     fromArchived=False, fromWaiting=False)
                else:
                    jobs = [self.job]
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

            if self.job is None:
                tmp_log.debug(f"end: {self.dataset.name}")
            else:
                tmp_log.debug(f"end: {self.job.PandaID}")
        except Exception:
            exc_type, value, _ = sys.exc_info()
            tmp_log.error(f"run() : {exc_type} {value}")
