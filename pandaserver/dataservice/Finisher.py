'''
finish transferring jobs

'''

import re
import sys
import uuid
import threading
import datetime

from pandaserver.config import panda_config
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Finisher')


class Finisher (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,dataset,job=None,site=None):
        threading.Thread.__init__(self)
        self.dataset    = dataset
        self.taskBuffer = taskBuffer
        self.job        = job
        self.site       = site


    # main
    def run(self):
        # start
        try:
            byCallback = False
            if self.job is None:
                byCallback = True
                _logger.debug("start: %s" % self.dataset.name)
                _logger.debug("callback from %s" % self.site)
                # FIXME when callback from BNLPANDA disappeared
                if self.site == 'BNLPANDA':
                    self.site = 'BNL-OSG2_ATLASMCDISK'
                # instantiate site mapper
                siteMapper = SiteMapper(self.taskBuffer)
                # get computingSite/destinationSE
                computingSite,destinationSE = self.taskBuffer.getDestSE(self.dataset.name)
                if destinationSE is None:
                    # try to get computingSite/destinationSE from ARCH to delete sub
                    # even if no active jobs left
                    computingSite, destinationSE = self.taskBuffer.getDestSE(self.dataset.name,True)
                    if destinationSE is None:
                        _logger.error("cannot get source/destination for %s" % self.dataset.name)
                        _logger.debug("end: %s" % self.dataset.name)
                        return
                _logger.debug("src: %s" % computingSite)
                _logger.debug("dst: %s" % destinationSE)
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
                _logger.debug("use Token=%s" % destToken)
                # get required tokens
                reqTokens = self.taskBuffer.getDestTokens(self.dataset.name)
                if reqTokens is None:
                    _logger.error("cannot get required token for %s" % self.dataset.name)
                    _logger.debug("end: %s" % self.dataset.name)
                    return
                _logger.debug("req Token=%s" % reqTokens)
                # make bitmap for the token
                bitMap = 1
                if len(reqTokens.split(','))>1:
                    for tmpReqToken in reqTokens.split(','):
                        if tmpReqToken == destToken:
                            break
                        # shift one bit
                        bitMap <<= 1
                # completed bitmap
                compBitMap = (1 << len(reqTokens.split(',')))-1
                # ignore the lowest bit for T1, file on DISK is already there
                # TODO: #prodanaly use the scope, but don't know job information
                if tmpSrcSiteSpec.ddm_output == tmpDstSiteSpec.ddm_output:
                    compBitMap = compBitMap & 0xFFFE
                # update bitmap in DB
                updatedBitMap = self.taskBuffer.updateTransferStatus(self.dataset.name,bitMap)
                _logger.debug("transfer status:%s - comp:%s - bit:%s" % (hex(updatedBitMap),hex(compBitMap),hex(bitMap)))
                # update output files
                if (updatedBitMap & compBitMap) == compBitMap:
                    ids = self.taskBuffer.updateOutFilesReturnPandaIDs(self.dataset.name)
                    # set flag for T2 cleanup
                    self.dataset.status = 'cleanup'
                    self.taskBuffer.updateDatasets([self.dataset])
                else:
                    _logger.debug("end: %s" % self.dataset.name)
                    return
            else:
                _logger.debug("start: %s" % self.job.PandaID)
                # update input files
                ids = [self.job.PandaID]
            _logger.debug("IDs: %s" % ids)
            if len(ids) != 0:
                # get job
                if self.job is None:
                    jobs = self.taskBuffer.peekJobs(ids,fromDefined=False,fromArchived=False,fromWaiting=False)
                else:
                    jobs = [self.job]
                # loop over all jobs
                for job in jobs:
                    if job is None:
                        continue
                    _logger.debug("Job: %s" % job.PandaID)
                    if job.jobStatus == 'transferring':
                        jobReady = True
                        failedFiles = []
                        noOutFiles = []
                        # check file status
                        for file in job.Files:
                            if file.type == 'output' or file.type == 'log':
                                if file.status == 'failed':
                                    failedFiles.append(file.lfn)
                                elif file.status == 'nooutput':
                                    noOutFiles.append(file.lfn)
                                elif file.status != 'ready':
                                    _logger.debug("Job: %s file:%s %s != ready" % (job.PandaID,file.lfn,file.status))
                                    jobReady = False
                                    break
                        # finish job
                        if jobReady:
                            if byCallback:
                                _logger.debug("Job: %s all files ready" % job.PandaID)
                            else:
                                _logger.debug("Job: %s all files checked with catalog" % job.PandaID)
                            # create XML
                            try:
                                import xml.dom.minidom
                                dom = xml.dom.minidom.getDOMImplementation()
                                doc = dom.createDocument(None,'xml',None)
                                topNode = doc.createElement("POOLFILECATALOG")
                                for file in job.Files:
                                    if file.type in ['output','log']:
                                        # skip failed or no-output files
                                        if file.lfn in failedFiles+noOutFiles:
                                            continue
                                        # File
                                        fileNode = doc.createElement("File")
                                        fileNode.setAttribute("ID",file.GUID)
                                        # LFN
                                        logNode = doc.createElement("logical")
                                        lfnNode = doc.createElement("lfn")
                                        lfnNode.setAttribute('name',file.lfn)
                                        # metadata
                                        fsizeNode    = doc.createElement("metadata")
                                        fsizeNode.setAttribute("att_name","fsize")
                                        fsizeNode.setAttribute("att_value",str(file.fsize))
                                        # checksum
                                        if file.checksum.startswith('ad:'):
                                            # adler32
                                            chksumNode    = doc.createElement("metadata")
                                            chksumNode.setAttribute("att_name","adler32")
                                            chksumNode.setAttribute("att_value",re.sub('^ad:','',file.checksum))
                                        else:
                                            # md5sum
                                            chksumNode    = doc.createElement("metadata")
                                            chksumNode.setAttribute("att_name","md5sum")
                                            chksumNode.setAttribute("att_value",re.sub('^md5:','',file.checksum))
                                        # append nodes
                                        logNode.appendChild(lfnNode)
                                        fileNode.appendChild(logNode)
                                        fileNode.appendChild(fsizeNode)
                                        fileNode.appendChild(chksumNode)
                                        topNode.appendChild(fileNode)
                                # status of the job record
                                if failedFiles == []:
                                    record_status = 'finished'
                                else:
                                    record_status = 'failed'
                                # write to file
                                # xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,job.PandaID,record_status,
                                #                            str(uuid.uuid4()))
                                # oXML = open(xmlFile,"w")
                                # oXML.write(topNode.toxml())
                                # oXML.close()
                                # write to job output report table, try update first
                                tmp_ret = self.taskBuffer.updateJobOutputReport(
                                    panda_id=job.PandaID, attempt_nr=job.attemptNr, data=topNode.toxml())
                                if not tmp_ret:
                                    # then try insert
                                    self.taskBuffer.insertJobOutputReport(
                                        panda_id=job.PandaID, prod_source_label=job.prodSourceLabel,
                                        job_status=record_status, attempt_nr=job.attemptNr, data=topNode.toxml())
                            except Exception:
                                type, value, traceBack = sys.exc_info()
                                _logger.error("Job: %s %s %s" % (job.PandaID,type,value))
                    _logger.debug("Job: %s status: %s" % (job.PandaID,job.jobStatus))
            # end
            if self.job is None:
                _logger.debug("end: %s" % self.dataset.name)
            else:
                _logger.debug("end: %s" % self.job.PandaID)
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("run() : %s %s" % (type,value))
