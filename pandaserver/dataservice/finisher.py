"""
finish transferring jobs

"""

import sys
import threading
import datetime
import json
import re

from typing import List
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

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
        json_dict = {}
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
            tmp_log.debug(f"start: {self.job.PandaID}")
            # update input files
            panda_ids = [self.job.PandaID]
            tmp_log.debug(f"IDs: {panda_ids}")
            if len(panda_ids) != 0:
                # get job
                jobs = [self.job]
                # loop over all jobs
                for job in jobs:
                    if job == "transferring":
                        job_ready, failed_files, no_out_files = self.check_file_status(job)
                        # finish job
                        if job_ready:
                            tmp_log.debug(f"Job: {job.PandaID} all files checked with catalog")
                            # # create JSON
                            try:
                                self.update_job_output_report(job, failed_files, no_out_files)
                            except Exception:
                                exc_type, value, _ = sys.exc_info()
                                tmp_log.error(f"Job: {job.PandaID} {exc_type} {value}")
                            # create XML
                            # try:
                            #     import xml.dom.minidom
                            #
                            #     dom = xml.dom.minidom.getDOMImplementation()
                            #     doc = dom.createDocument(None, "xml", None)
                            #     top_node = doc.createElement("POOLFILECATALOG")
                            #     for file in job.Files:
                            #         if file.type in ["output", "log"]:
                            #             # skip failed or no-output files
                            #             if file.lfn in failed_files + no_out_files:
                            #                 continue
                            #             # File
                            #             file_node = doc.createElement("File")
                            #             file_node.setAttribute("ID", file.GUID)
                            #             # LFN
                            #             log_node = doc.createElement("logical")
                            #             lfn_node = doc.createElement("lfn")
                            #             lfn_node.setAttribute("name", file.lfn)
                            #             # metadata
                            #             fsize_node = doc.createElement("metadata")
                            #             fsize_node.setAttribute("att_name", "fsize")
                            #             fsize_node.setAttribute("att_value", str(file.fsize))
                            #             # checksum
                            #             if file.checksum.startswith("ad:"):
                            #                 # adler32
                            #                 checksum_node = doc.createElement("metadata")
                            #                 checksum_node.setAttribute("att_name", "adler32")
                            #                 checksum_node.setAttribute(
                            #                     "att_value",
                            #                     re.sub("^ad:", "", file.checksum),
                            #                 )
                            #             else:
                            #                 # md5sum
                            #                 checksum_node = doc.createElement("metadata")
                            #                 checksum_node.setAttribute("att_name", "md5sum")
                            #                 checksum_node.setAttribute(
                            #                     "att_value",
                            #                     re.sub("^md5:", "", file.checksum),
                            #                 )
                            #             # append nodes
                            #             log_node.appendChild(lfn_node)
                            #             file_node.appendChild(log_node)
                            #             file_node.appendChild(fsize_node)
                            #             file_node.appendChild(checksum_node)
                            #             top_node.appendChild(file_node)
                            #     # status of the job record
                            #     if not failed_files:
                            #         record_status = "finished"
                            #     else:
                            #         record_status = "failed"
                            #     # write to file
                            #     # xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,job.PandaID,record_status,
                            #     #                            str(uuid.uuid4()))
                            #     # oXML = open(xmlFile,"w")
                            #     # oXML.write(top_node.toxml())
                            #     # oXML.close()
                            #     # write to job output report table, try update first
                            #     tmp_ret = self.task_buffer.updateJobOutputReport(
                            #         panda_id=job.PandaID,
                            #         attempt_nr=job.attemptNr,
                            #         data=top_node.toxml(),
                            #     )
                            #     if not tmp_ret:
                            #         # then try insert
                            #         self.task_buffer.insertJobOutputReport(
                            #             panda_id=job.PandaID,
                            #             prod_source_label=job.prodSourceLabel,
                            #             job_status=record_status,
                            #             attempt_nr=job.attemptNr,
                            #             data=top_node.toxml(),
                            #         )
                            # except Exception:
                            #     exc_type, value, _ = sys.exc_info()
                            #     tmp_log.error(f"Job: {job.PandaID} {exc_type} {value}")

                    tmp_log.debug(f"Job: {job.PandaID} status: {job.jobStatus}")
            tmp_log.debug(f"end: {self.job.PandaID}")
        except Exception:
            exc_type, value, _ = sys.exc_info()
            tmp_log.error(f"run() : {exc_type} {value}")
