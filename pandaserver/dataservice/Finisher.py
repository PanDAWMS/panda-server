"""
finish transferring jobs

"""

import datetime
import re
import sys
import threading
import uuid

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config

# logger
_logger = PandaLogger().getLogger("Finisher")


class Finisher(threading.Thread):
    # constructor
    def __init__(self, taskBuffer, dataset, job=None, site=None):
        threading.Thread.__init__(self)
        self.dataset = dataset
        self.taskBuffer = taskBuffer
        self.job = job
        self.site = site

    # main
    def run(self):
        # start
        try:
            byCallback = False
            if self.job is None:
                byCallback = True
                _logger.debug(f"start: {self.dataset.name}")
                _logger.debug(f"callback from {self.site}")
                # FIXME when callback from BNLPANDA disappeared
                if self.site == "BNLPANDA":
                    self.site = "BNL-OSG2_ATLASMCDISK"
                # instantiate site mapper
                siteMapper = SiteMapper(self.taskBuffer)
                # get computingSite/destinationSE
                computingSite, destinationSE = self.taskBuffer.getDestSE(self.dataset.name)
                if destinationSE is None:
                    # try to get computingSite/destinationSE from ARCH to delete sub
                    # even if no active jobs left
                    computingSite, destinationSE = self.taskBuffer.getDestSE(self.dataset.name, True)
                    if destinationSE is None:
                        _logger.error(f"cannot get source/destination for {self.dataset.name}")
                        _logger.debug(f"end: {self.dataset.name}")
                        return
                _logger.debug(f"src: {computingSite}")
                _logger.debug(f"dst: {destinationSE}")
                # get corresponding token
                tmpSrcSiteSpec = siteMapper.getSite(computingSite)
                tmpDstSiteSpec = siteMapper.getSite(destinationSE)
                _logger.debug(tmpDstSiteSpec.setokens_output)
                destToken = None
                for scope in tmpDstSiteSpec.setokens_output:
                    for setoken in tmpDstSiteSpec.setokens_output[scope]:
                        for tmpDdmId in tmpDstSiteSpec.setokens_output[scope][setoken]:
                            if self.site == tmpDdmId:
                                destToken = setoken
                                break
                _logger.debug(f"use Token={destToken}")
                # get required tokens
                reqTokens = self.taskBuffer.getDestTokens(self.dataset.name)
                if reqTokens is None:
                    _logger.error(f"cannot get required token for {self.dataset.name}")
                    _logger.debug(f"end: {self.dataset.name}")
                    return
                _logger.debug(f"req Token={reqTokens}")
                # make bitmap for the token
                bitMap = 1
                if len(reqTokens.split(",")) > 1:
                    for tmpReqToken in reqTokens.split(","):
                        if tmpReqToken == destToken:
                            break
                        # shift one bit
                        bitMap <<= 1
                # completed bitmap
                compBitMap = (1 << len(reqTokens.split(","))) - 1
                # ignore the lowest bit for T1, file on DISK is already there
                # TODO: #prodanaly use the scope, but don't know job information
                if tmpSrcSiteSpec.ddm_output == tmpDstSiteSpec.ddm_output:
                    compBitMap = compBitMap & 0xFFFE
                # update bitmap in DB
                updatedBitMap = self.taskBuffer.updateTransferStatus(self.dataset.name, bitMap)
                _logger.debug(f"transfer status:{hex(updatedBitMap)} - comp:{hex(compBitMap)} - bit:{hex(bitMap)}")
                # update output files
                if (updatedBitMap & compBitMap) == compBitMap:
                    ids = self.taskBuffer.updateOutFilesReturnPandaIDs(self.dataset.name)
                    # set flag for T2 cleanup
                    self.dataset.status = "cleanup"
                    self.taskBuffer.updateDatasets([self.dataset])
                else:
                    _logger.debug(f"end: {self.dataset.name}")
                    return
            else:
                _logger.debug(f"start: {self.job.PandaID}")
                # update input files
                ids = [self.job.PandaID]
            _logger.debug(f"IDs: {ids}")
            if len(ids) != 0:
                # get job
                if self.job is None:
                    jobs = self.taskBuffer.peekJobs(ids, fromDefined=False, fromArchived=False, fromWaiting=False)
                else:
                    jobs = [self.job]
                # loop over all jobs
                for job in jobs:
                    if job is None:
                        continue
                    _logger.debug(f"Job: {job.PandaID}")
                    if job.jobStatus == "transferring":
                        jobReady = True
                        failedFiles = []
                        noOutFiles = []
                        # check file status
                        for file in job.Files:
                            if file.type == "output" or file.type == "log":
                                if file.status == "failed":
                                    failedFiles.append(file.lfn)
                                elif file.status == "nooutput":
                                    noOutFiles.append(file.lfn)
                                elif file.status != "ready":
                                    _logger.debug(f"Job: {job.PandaID} file:{file.lfn} {file.status} != ready")
                                    jobReady = False
                                    break
                        # finish job
                        if jobReady:
                            if byCallback:
                                _logger.debug(f"Job: {job.PandaID} all files ready")
                            else:
                                _logger.debug(f"Job: {job.PandaID} all files checked with catalog")
                            # create XML
                            try:
                                import xml.dom.minidom

                                dom = xml.dom.minidom.getDOMImplementation()
                                doc = dom.createDocument(None, "xml", None)
                                topNode = doc.createElement("POOLFILECATALOG")
                                for file in job.Files:
                                    if file.type in ["output", "log"]:
                                        # skip failed or no-output files
                                        if file.lfn in failedFiles + noOutFiles:
                                            continue
                                        # File
                                        fileNode = doc.createElement("File")
                                        fileNode.setAttribute("ID", file.GUID)
                                        # LFN
                                        logNode = doc.createElement("logical")
                                        lfnNode = doc.createElement("lfn")
                                        lfnNode.setAttribute("name", file.lfn)
                                        # metadata
                                        fsizeNode = doc.createElement("metadata")
                                        fsizeNode.setAttribute("att_name", "fsize")
                                        fsizeNode.setAttribute("att_value", str(file.fsize))
                                        # checksum
                                        if file.checksum.startswith("ad:"):
                                            # adler32
                                            chksumNode = doc.createElement("metadata")
                                            chksumNode.setAttribute("att_name", "adler32")
                                            chksumNode.setAttribute(
                                                "att_value",
                                                re.sub("^ad:", "", file.checksum),
                                            )
                                        else:
                                            # md5sum
                                            chksumNode = doc.createElement("metadata")
                                            chksumNode.setAttribute("att_name", "md5sum")
                                            chksumNode.setAttribute(
                                                "att_value",
                                                re.sub("^md5:", "", file.checksum),
                                            )
                                        # append nodes
                                        logNode.appendChild(lfnNode)
                                        fileNode.appendChild(logNode)
                                        fileNode.appendChild(fsizeNode)
                                        fileNode.appendChild(chksumNode)
                                        topNode.appendChild(fileNode)
                                # status of the job record
                                if failedFiles == []:
                                    record_status = "finished"
                                else:
                                    record_status = "failed"
                                # write to file
                                # xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,job.PandaID,record_status,
                                #                            str(uuid.uuid4()))
                                # oXML = open(xmlFile,"w")
                                # oXML.write(topNode.toxml())
                                # oXML.close()
                                # write to job output report table, try update first
                                tmp_ret = self.taskBuffer.updateJobOutputReport(
                                    panda_id=job.PandaID,
                                    attempt_nr=job.attemptNr,
                                    data=topNode.toxml(),
                                )
                                if not tmp_ret:
                                    # then try insert
                                    self.taskBuffer.insertJobOutputReport(
                                        panda_id=job.PandaID,
                                        prod_source_label=job.prodSourceLabel,
                                        job_status=record_status,
                                        attempt_nr=job.attemptNr,
                                        data=topNode.toxml(),
                                    )
                            except Exception:
                                type, value, traceBack = sys.exc_info()
                                _logger.error(f"Job: {job.PandaID} {type} {value}")
                    _logger.debug(f"Job: {job.PandaID} status: {job.jobStatus}")
            # end
            if self.job is None:
                _logger.debug(f"end: {self.dataset.name}")
            else:
                _logger.debug(f"end: {self.job.PandaID}")
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"run() : {type} {value}")
