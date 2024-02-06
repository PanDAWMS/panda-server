"""
finish transferring jobs

"""

import re
import sys
import threading

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
    def __init__(self, taskBuffer, dataset, job=None, site=None):
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
                # FIXME when callback from BNLPANDA disappeared
                if self.site == "BNLPANDA":
                    self.site = "BNL-OSG2_ATLASMCDISK"
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
                    ids = self.task_buffer.updateOutFilesReturnPandaIDs(self.dataset.name)
                    # set flag for T2 cleanup
                    self.dataset.status = "cleanup"
                    self.task_buffer.updateDatasets([self.dataset])
                else:
                    tmp_log.debug(f"end: {self.dataset.name}")
                    return
            else:
                tmp_log.debug(f"start: {self.job.PandaID}")
                # update input files
                ids = [self.job.PandaID]
            tmp_log.debug(f"IDs: {ids}")
            if len(ids) != 0:
                # get job
                if self.job is None:
                    jobs = self.task_buffer.peekJobs(ids, fromDefined=False, fromArchived=False, fromWaiting=False)
                else:
                    jobs = [self.job]
                # loop over all jobs
                for job in jobs:
                    if job is None:
                        continue
                    tmp_log.debug(f"Job: {job.PandaID}")
                    if job.jobStatus == "transferring":
                        job_ready = True
                        failed_files = []
                        no_out_files = []
                        # check file status
                        for file in job.Files:
                            if file.type == "output" or file.type == "log":
                                if file.status == "failed":
                                    failed_files.append(file.lfn)
                                elif file.status == "nooutput":
                                    no_out_files.append(file.lfn)
                                elif file.status != "ready":
                                    tmp_log.debug(f"Job: {job.PandaID} file:{file.lfn} {file.status} != ready")
                                    job_ready = False
                                    break
                        # finish job
                        if job_ready:
                            if by_call_back:
                                tmp_log.debug(f"Job: {job.PandaID} all files ready")
                            else:
                                tmp_log.debug(f"Job: {job.PandaID} all files checked with catalog")
                            # create XML
                            try:
                                import xml.dom.minidom

                                dom = xml.dom.minidom.getDOMImplementation()
                                doc = dom.createDocument(None, "xml", None)
                                top_node = doc.createElement("POOLFILECATALOG")
                                for file in job.Files:
                                    if file.type in ["output", "log"]:
                                        # skip failed or no-output files
                                        if file.lfn in failed_files + no_out_files:
                                            continue
                                        # File
                                        file_node = doc.createElement("File")
                                        file_node.setAttribute("ID", file.GUID)
                                        # LFN
                                        log_node = doc.createElement("logical")
                                        lfn_node = doc.createElement("lfn")
                                        lfn_node.setAttribute("name", file.lfn)
                                        # metadata
                                        filesize_node = doc.createElement("metadata")
                                        filesize_node.setAttribute("att_name", "fsize")
                                        filesize_node.setAttribute("att_value", str(file.fsize))
                                        # checksum
                                        if file.checksum.startswith("ad:"):
                                            # adler32
                                            checksum_node = doc.createElement("metadata")
                                            checksum_node.setAttribute("att_name", "adler32")
                                            checksum_node.setAttribute(
                                                "att_value",
                                                re.sub("^ad:", "", file.checksum),
                                            )
                                        else:
                                            # md5sum
                                            checksum_node = doc.createElement("metadata")
                                            checksum_node.setAttribute("att_name", "md5sum")
                                            checksum_node.setAttribute(
                                                "att_value",
                                                re.sub("^md5:", "", file.checksum),
                                            )
                                        # append nodes
                                        log_node.appendChild(lfn_node)
                                        file_node.appendChild(log_node)
                                        file_node.appendChild(filesize_node)
                                        file_node.appendChild(checksum_node)
                                        top_node.appendChild(file_node)
                                # status of the job record
                                if not failed_files:
                                    record_status = "finished"
                                else:
                                    record_status = "failed"
                                # write to file
                                # xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,job.PandaID,record_status,
                                #                            str(uuid.uuid4()))
                                # oXML = open(xmlFile,"w")
                                # oXML.write(top_node.toxml())
                                # oXML.close()
                                # write to job output report table, try update first
                                tmp_ret = self.task_buffer.updateJobOutputReport(
                                    panda_id=job.PandaID,
                                    attempt_nr=job.attemptNr,
                                    data=top_node.toxml(),
                                )
                                if not tmp_ret:
                                    # then try insert
                                    self.task_buffer.insertJobOutputReport(
                                        panda_id=job.PandaID,
                                        prod_source_label=job.prodSourceLabel,
                                        job_status=record_status,
                                        attempt_nr=job.attemptNr,
                                        data=top_node.toxml(),
                                    )
                            except Exception:
                                type, value, trace_back = sys.exc_info()
                                tmp_log.error(f"Job: {job.PandaID} {type} {value}")
                    tmp_log.debug(f"Job: {job.PandaID} status: {job.jobStatus}")
            # end
            if self.job is None:
                tmp_log.debug(f"end: {self.dataset.name}")
            else:
                tmp_log.debug(f"end: {self.job.PandaID}")
        except Exception:
            type, value, trace_back = sys.exc_info()
            tmp_log.error(f"run() : {type} {value}")
