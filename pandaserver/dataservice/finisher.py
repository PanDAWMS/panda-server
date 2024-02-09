"""
finish transferring jobs

"""

import re
import sys
import threading
import datetime
import xml


from typing import List
from xml.dom.minidom import Document
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

    def create_metadata_node(self, doc: Document, att_name: str, att_value: str):
        """
        Create a metadata node for the XML document.

        Parameters:
        doc (xml.dom.minidom.Document): The XML document.
        att_name (str): The attribute name.
        att_value (str): The attribute value.

        Returns:
        xml.dom.minidom.Element: The created metadata node.
        """
        metadata_node = doc.createElement("metadata")
        metadata_node.setAttribute("att_name", att_name)
        metadata_node.setAttribute("att_value", str(att_value))
        return metadata_node

    def create_file_node(self, doc: Document, file, failed_files: List[str],
                         no_out_files: List[str]):
        """
        This function creates a file node for the XML document.

        Parameters:
        doc (xml.dom.minidom.Document): The XML document.
        file (FileSpec): The file specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.

        Returns:
        xml.dom.minidom.Element: The created file node or None if the file is in the list of failed files or no output files.
        """
        if file.lfn in failed_files + no_out_files:
            return None
        file_node = doc.createElement("File")
        file_node.setAttribute("ID", file.GUID)
        log_node = doc.createElement("logical")
        lfn_node = doc.createElement("lfn")
        lfn_node.setAttribute("name", file.lfn)
        log_node.appendChild(lfn_node)
        file_node.appendChild(log_node)
        file_node.appendChild(self.create_metadata_node(doc, "fsize", file.fsize))
        if file.checksum.startswith("ad:"):
            file_node.appendChild(self.create_metadata_node(doc, "adler32",
                                                            re.sub("^ad:", "", file.checksum)))
        else:
            file_node.appendChild(self.create_metadata_node(doc, "md5sum",
                                                            re.sub("^md5:", "", file.checksum)))
        return file_node

    def create_xml_doc(self, job, failed_files: List[str], no_out_files: List[str]):
        """
        This function creates an XML document for the jobs.

        Parameters:
        job (JobSpec): The job specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.

        Returns:
        str: The created XML document as a string.
        """
        dom = xml.dom.minidom.getDOMImplementation()
        doc = dom.createDocument(None, "xml", None)
        top_node = doc.createElement("POOLFILECATALOG")
        for file in job.Files:
            if file.type in ["output", "log"]:
                file_node = self.create_file_node(doc, file, failed_files, no_out_files)
                if file_node is not None:
                    top_node.appendChild(file_node)
        doc.appendChild(top_node)
        return doc.toxml()

    def update_job_output_report(self, job, failed_files: List[str], no_out_files: List[str]):
        """
        This function updates the job output report.

        Parameters:
        job (JobSpec): The job specification.
        failed_files (list): List of failed files.
        no_out_files (list): List of files with no output.
        """
        xml_data = self.create_xml_doc(job, failed_files, no_out_files)
        record_status = "finished" if not failed_files else "failed"
        tmp_ret = self.task_buffer.updateJobOutputReport(
            panda_id=job.PandaID,
            attempt_nr=job.attemptNr,
            data=xml_data,
        )
        if not tmp_ret:
            self.task_buffer.insertJobOutputReport(
                panda_id=job.PandaID,
                prod_source_label=job.prodSourceLabel,
                job_status=record_status,
                attempt_nr=job.attemptNr,
                data=xml_data,
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
                # TODO: #prodanaly use the scope, but don't know job information
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
                        # create XML
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
