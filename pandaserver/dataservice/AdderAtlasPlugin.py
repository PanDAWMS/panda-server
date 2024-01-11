"""
add data to dataset

"""

import copy
import datetime
import gc
import re
import sys
import time
import traceback

from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils, ErrorCode
from pandaserver.dataservice.AdderPluginBase import AdderPluginBase
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.srvcore.MailUtils import MailUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from rucio.common.exception import (
    DataIdentifierNotFound,
    FileConsistencyMismatch,
    InsufficientAccountLimit,
    InvalidObject,
    InvalidPath,
    InvalidRSEExpression,
    RSENotFound,
    RSEProtocolNotSupported,
    UnsupportedOperation,
)


class AdderAtlasPlugin(AdderPluginBase):
    # constructor
    def __init__(self, job, **params):
        AdderPluginBase.__init__(self, job, params)
        self.jobID = self.job.PandaID
        self.jobStatus = self.job.jobStatus
        self.datasetMap = {}
        self.addToTopOnly = False
        self.goToTransferring = False
        self.logTransferring = False
        self.subscriptionMap = {}
        self.pandaDDM = False
        self.goToMerging = False

    # main
    def execute(self):
        try:
            self.logger.debug(f"start plugin : {self.jobStatus}")
            # backend
            self.ddmBackEnd = self.job.getDdmBackEnd()
            if self.ddmBackEnd is None:
                self.ddmBackEnd = "rucio"
            self.logger.debug(f"ddm backend = {self.ddmBackEnd}")
            # add files only to top-level datasets for transferring jobs
            if self.job.jobStatus == "transferring":
                self.addToTopOnly = True
                self.logger.debug("adder for transferring")
            # use PandaDDM for ddm jobs
            if self.job.prodSourceLabel == "ddm":
                self.pandaDDM = True
            # check if the job goes to merging
            if self.job.produceUnMerge():
                self.goToMerging = True
            # check if the job should go to transferring
            srcSiteSpec = self.siteMapper.getSite(self.job.computingSite)
            scopeSrcSiteSpec_input, scopeSrcSiteSpec_output = select_scope(srcSiteSpec, self.job.prodSourceLabel, self.job.job_label)
            tmpSrcDDM = srcSiteSpec.ddm_output[scopeSrcSiteSpec_output]
            if self.job.prodSourceLabel == "user" and self.job.destinationSE not in self.siteMapper.siteSpecList:
                # DQ2 ID was set by using --destSE for analysis job to transfer output
                tmpDstDDM = self.job.destinationSE
            else:
                dstSiteSpec = self.siteMapper.getSite(self.job.destinationSE)
                scopeDstSiteSpec_input, scopeDstSiteSpec_output = select_scope(dstSiteSpec, self.job.prodSourceLabel, self.job.job_label)
                tmpDstDDM = dstSiteSpec.ddm_output[scopeDstSiteSpec_output]
                # protection against disappearance of dest from schedconfig
                if not self.siteMapper.checkSite(self.job.destinationSE) and self.job.destinationSE != "local":
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"destinationSE {self.job.destinationSE} is unknown in schedconfig"
                    self.logger.error(f"{self.job.ddmErrorDiag}")
                    # set fatal error code and return
                    self.result.setFatal()
                    return
            # protection against disappearance of src from schedconfig
            if not self.siteMapper.checkSite(self.job.computingSite):
                self.job.ddmErrorCode = ErrorCode.EC_Adder
                self.job.ddmErrorDiag = f"computingSite {self.job.computingSite} is unknown in schedconfig"
                self.logger.error(f"{self.job.ddmErrorDiag}")
                # set fatal error code and return
                self.result.setFatal()
                return
            # check if the job has something to transfer
            self.logger.debug(f"alt stage-out:{str(self.job.altStgOutFileList())}")
            somethingToTransfer = False
            for file in self.job.Files:
                if file.type == "output" or file.type == "log":
                    if file.status == "nooutput":
                        continue
                    if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is None and file.lfn not in self.job.altStgOutFileList():
                        somethingToTransfer = True
                        break
            self.logger.debug(f"DDM src:{tmpSrcDDM} dst:{tmpDstDDM}")
            job_type = JobUtils.translate_prodsourcelabel_to_jobtype(srcSiteSpec.type, self.job.prodSourceLabel)
            if re.search("^ANALY_", self.job.computingSite) is not None:
                # analysis site. Should be obsoleted by the next check
                pass
            elif job_type == JobUtils.ANALY_PS:
                # analysis job
                pass
            elif self.job.computingSite == self.job.destinationSE:
                # same site ID for computingSite and destinationSE
                pass
            elif tmpSrcDDM == tmpDstDDM:
                # same DQ2ID for src/dest
                pass
            elif self.addToTopOnly:
                # already in transferring
                pass
            elif self.goToMerging:
                # no transferring for merging
                pass
            elif self.job.jobStatus == "failed":
                # failed jobs
                if self.job.prodSourceLabel in ["managed", "test"]:
                    self.logTransferring = True
            elif (
                self.job.jobStatus == "finished"
                and (EventServiceUtils.isEventServiceJob(self.job) or EventServiceUtils.isJumboJob(self.job))
                and not EventServiceUtils.isJobCloningJob(self.job)
            ):
                # transfer only log file for normal ES jobs
                self.logTransferring = True
            elif not somethingToTransfer:
                # nothing to transfer
                pass
            else:
                self.goToTransferring = True
            self.logger.debug(f"somethingToTransfer={somethingToTransfer}")
            self.logger.debug(f"goToTransferring={self.goToTransferring}")
            self.logger.debug(f"logTransferring={self.logTransferring}")
            self.logger.debug(f"goToMerging={self.goToMerging}")
            self.logger.debug(f"addToTopOnly={self.addToTopOnly}")
            retOut = self._updateOutputs()
            self.logger.debug(f"added outputs with {retOut}")
            if retOut != 0:
                self.logger.debug("terminated when adding")
                return
            # succeeded
            self.result.setSucceeded()
            self.logger.debug("end plugin")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            errStr = f"execute() : {errtype} {errvalue}"
            errStr += traceback.format_exc()
            self.logger.debug(errStr)
            # set fatal error code
            self.result.setFatal()
        # return
        return

    # update output files
    def _updateOutputs(self):
        # return if non-DQ2
        if self.pandaDDM or self.job.destinationSE == "local":
            return 0
        # get the computingsite spec and scope
        srcSiteSpec = self.siteMapper.getSite(self.job.computingSite)
        scopeSrcSiteSpec_input, scopeSrcSiteSpec_output = select_scope(srcSiteSpec, self.job.prodSourceLabel, self.job.job_label)
        # zip file map
        zipFileMap = self.job.getZipFileMap()
        # get campaign
        campaign = None
        nEventsInput = dict()
        if self.job.jediTaskID not in [0, None, "NULL"]:
            tmpRet = self.taskBuffer.getTaskAttributesPanda(self.job.jediTaskID, ["campaign"])
            if "campaign" in tmpRet:
                campaign = tmpRet["campaign"]
            for fileSpec in self.job.Files:
                if fileSpec.type == "input":
                    tmpDict = self.taskBuffer.getJediFileAttributes(
                        fileSpec.PandaID,
                        fileSpec.jediTaskID,
                        fileSpec.datasetID,
                        fileSpec.fileID,
                        ["nEvents"],
                    )
                    if "nEvents" in tmpDict:
                        nEventsInput[fileSpec.lfn] = tmpDict["nEvents"]
        # add nEvents info for zip files
        for tmpZipFileName in zipFileMap:
            tmpZipContents = zipFileMap[tmpZipFileName]
            if tmpZipFileName not in self.extraInfo["nevents"]:
                for tmpZipContent in tmpZipContents:
                    for tmpLFN in list(nEventsInput):
                        if re.search("^" + tmpZipContent + "$", tmpLFN) is not None and tmpLFN in nEventsInput and nEventsInput[tmpLFN] is not None:
                            self.extraInfo["nevents"].setdefault(tmpZipFileName, 0)
                            self.extraInfo["nevents"][tmpZipFileName] += nEventsInput[tmpLFN]
        # check files
        idMap = {}
        # fileList = []
        subMap = {}
        dsDestMap = {}
        distDSs = set()
        osDsFileMap = {}
        zipFiles = {}
        contZipMap = {}
        subToDsMap = {}
        dsIdToDsMap = self.taskBuffer.getOutputDatasetsJEDI(self.job.PandaID)
        self.logger.debug(f"dsInJEDI={str(dsIdToDsMap)}")
        for file in self.job.Files:
            # gc
            gc.collect()
            if file.type == "output" or file.type == "log":
                # prepare the site spec and scope for the destinationSE site
                dstSESiteSpec = self.siteMapper.getSite(file.destinationSE)
                scopeDstSESiteSpec_input, scopeDstSESiteSpec_output = select_scope(dstSESiteSpec, self.job.prodSourceLabel, self.job.job_label)

                # added to map
                if file.datasetID in dsIdToDsMap:
                    subToDsMap[file.destinationDBlock] = dsIdToDsMap[file.datasetID]
                else:
                    subToDsMap[file.destinationDBlock] = file.dataset
                # append to fileList
                # commented since unused elsewhere
                # fileList.append(file.lfn)
                # add only log file for failed jobs
                if self.jobStatus == "failed" and file.type != "log":
                    continue
                # add only log file for successful ES jobs
                if (
                    self.job.jobStatus == "finished"
                    and EventServiceUtils.isEventServiceJob(self.job)
                    and not EventServiceUtils.isJobCloningJob(self.job)
                    and file.type != "log"
                ):
                    continue
                # add only log for jumbo jobs
                if EventServiceUtils.isJumboJob(self.job) and file.type != "log":
                    continue
                # skip no output or failed
                if file.status in ["nooutput", "failed"]:
                    continue
                # check if zip file
                if file.lfn in zipFileMap:
                    isZipFile = True
                    if file.lfn not in zipFiles and not self.addToTopOnly:
                        zipFiles[file.lfn] = dict()
                else:
                    isZipFile = False
                # check if zip content
                zipFileName = None
                if not isZipFile and not self.addToTopOnly:
                    for tmpZipFileName in zipFileMap:
                        tmpZipContents = zipFileMap[tmpZipFileName]
                        for tmpZipContent in tmpZipContents:
                            if re.search("^" + tmpZipContent + "$", file.lfn) is not None:
                                zipFileName = tmpZipFileName
                                break
                        if zipFileName is not None:
                            break
                    if zipFileName is not None:
                        if zipFileName not in zipFiles:
                            zipFiles[zipFileName] = dict()
                        contZipMap[file.lfn] = zipFileName
                try:
                    # check nevents
                    if file.type == "output" and not isZipFile and not self.addToTopOnly and self.job.prodSourceLabel in ["managed"]:
                        if file.lfn not in self.extraInfo["nevents"]:
                            toSkip = False
                            # exclude some formats
                            for patt in ["TXT", "NTUP", "HIST"]:
                                if file.lfn.startswith(patt):
                                    toSkip = True
                            if not toSkip:
                                errMsg = f"nevents is missing in jobReport for {file.lfn}"
                                self.logger.warning(errMsg)
                                self.job.ddmErrorCode = ErrorCode.EC_MissingNumEvents
                                raise ValueError(errMsg)
                        if file.lfn not in self.extraInfo["guid"] or file.GUID != self.extraInfo["guid"][file.lfn]:
                            self.logger.debug(f"extraInfo = {str(self.extraInfo)}")
                            errMsg = f"GUID is inconsistent between jobReport and pilot report for {file.lfn}"
                            self.logger.warning(errMsg)
                            self.job.ddmErrorCode = ErrorCode.EC_InconsistentGUID
                            raise ValueError(errMsg)
                    # fsize
                    fsize = None
                    if file.fsize not in ["NULL", "", 0]:
                        try:
                            fsize = int(file.fsize)
                        except Exception:
                            type, value, traceBack = sys.exc_info()
                            self.logger.error(f"{self.jobID} : {type} {value}")
                    # use top-level dataset name for alternative stage-out
                    if file.lfn not in self.job.altStgOutFileList():
                        fileDestinationDBlock = file.destinationDBlock
                    else:
                        fileDestinationDBlock = file.dataset
                    # append to map
                    if fileDestinationDBlock not in idMap:
                        idMap[fileDestinationDBlock] = []
                    fileAttrs = {
                        "guid": file.GUID,
                        "lfn": file.lfn,
                        "size": fsize,
                        "checksum": file.checksum,
                        "ds": fileDestinationDBlock,
                    }
                    # add SURLs if LFC registration is required
                    if self.useCentralLFC():
                        fileAttrs["surl"] = self.extraInfo["surl"][file.lfn]
                        if fileAttrs["surl"] is None:
                            del fileAttrs["surl"]
                        # get destination
                        if fileDestinationDBlock not in dsDestMap:
                            toConvert = True
                            if file.lfn in self.job.altStgOutFileList():
                                toConvert = False
                                # alternative stage-out
                                if DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None:
                                    # RSE is specified
                                    tmpDestList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                else:
                                    if file.destinationDBlockToken in dstSESiteSpec.setokens_output[scopeDstSESiteSpec_output]:
                                        # get endpoint for token
                                        tmpDestList = [dstSESiteSpec.setokens_output[scopeDstSESiteSpec_output][file.destinationDBlockToken]]
                                    else:
                                        # use defalt endpoint
                                        tmpDestList = [dstSESiteSpec.ddm_output[scopeDstSESiteSpec_output]]
                            elif file.destinationDBlockToken in ["", None, "NULL"]:
                                # use default endpoint
                                tmpDestList = [srcSiteSpec.ddm_output[scopeSrcSiteSpec_output]]
                            elif (
                                DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None
                                and srcSiteSpec.ddm_output[scopeSrcSiteSpec_output] == dstSESiteSpec.ddm_output[scopeDstSESiteSpec_output]
                            ):
                                tmpDestList = [DataServiceUtils.getDestinationSE(file.destinationDBlockToken)]
                                # RSE is specified
                                toConvert = False
                            elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                                tmpDestList = [DataServiceUtils.getDistributedDestination(file.destinationDBlockToken)]
                                distDSs.add(fileDestinationDBlock)
                                # RSE is specified for distributed datasets
                                toConvert = False
                            elif (
                                srcSiteSpec.cloud != self.job.cloud
                                and (not srcSiteSpec.ddm_output[scopeSrcSiteSpec_output].endswith("PRODDISK"))
                                and (self.job.prodSourceLabel not in ["user", "panda"])
                            ):
                                # T1 used as T2
                                tmpDestList = [srcSiteSpec.ddm_output[scopeSrcSiteSpec_output]]
                            else:
                                tmpDestList = []
                                tmpSeTokens = srcSiteSpec.setokens_output[scopeSrcSiteSpec_output]
                                for tmpDestToken in file.destinationDBlockToken.split(","):
                                    if tmpDestToken in tmpSeTokens:
                                        tmpDest = tmpSeTokens[tmpDestToken]
                                    else:
                                        tmpDest = srcSiteSpec.ddm_output[scopeSrcSiteSpec_output]
                                    if tmpDest not in tmpDestList:
                                        tmpDestList.append(tmpDest)
                            # add
                            dsDestMap[fileDestinationDBlock] = tmpDestList
                    # extra meta data
                    if self.ddmBackEnd == "rucio":
                        if file.lfn in self.extraInfo["lbnr"]:
                            fileAttrs["lumiblocknr"] = self.extraInfo["lbnr"][file.lfn]
                        if file.lfn in self.extraInfo["nevents"]:
                            fileAttrs["events"] = self.extraInfo["nevents"][file.lfn]
                        elif self.extraInfo["nevents"] != {}:
                            fileAttrs["events"] = None
                        # if file.jediTaskID not in [0,None,'NULL']:
                        #    fileAttrs['task_id'] = file.jediTaskID
                        fileAttrs["panda_id"] = file.PandaID
                        if campaign not in ["", None]:
                            fileAttrs["campaign"] = campaign
                    # extract OS files
                    hasNormalURL = True
                    if file.lfn in self.extraInfo["endpoint"] and self.extraInfo["endpoint"][file.lfn] != []:
                        # hasNormalURL = False # FIXME once the pilot chanages to send srm endpoints in addition to OS
                        for pilotEndPoint in self.extraInfo["endpoint"][file.lfn]:
                            # pilot uploaded to endpoint consistently with original job definition
                            if pilotEndPoint in dsDestMap[fileDestinationDBlock]:
                                hasNormalURL = True
                            else:
                                # uploaded to S3
                                if pilotEndPoint not in osDsFileMap:
                                    osDsFileMap[pilotEndPoint] = {}
                                osFileDestinationDBlock = file.dataset
                                if osFileDestinationDBlock not in osDsFileMap[pilotEndPoint]:
                                    osDsFileMap[pilotEndPoint][osFileDestinationDBlock] = []
                                copiedFileAttrs = copy.copy(fileAttrs)
                                del copiedFileAttrs["surl"]
                                osDsFileMap[pilotEndPoint][osFileDestinationDBlock].append(copiedFileAttrs)
                    if hasNormalURL:
                        # add file to be added to dataset
                        idMap[fileDestinationDBlock].append(fileAttrs)
                        # add file to be added to zip
                        if not isZipFile and zipFileName is not None:
                            if "files" not in zipFiles[zipFileName]:
                                zipFiles[zipFileName]["files"] = []
                            zipFiles[zipFileName]["files"].append(fileAttrs)
                        if isZipFile and not self.addToTopOnly:
                            # copy file attribute for zip file registration
                            for tmpFileAttrName in fileAttrs:
                                tmpFileAttrVal = fileAttrs[tmpFileAttrName]
                                zipFiles[file.lfn][tmpFileAttrName] = tmpFileAttrVal
                            zipFiles[file.lfn]["scope"] = file.scope
                            zipFiles[file.lfn]["rse"] = dsDestMap[fileDestinationDBlock]
                    # for subscription
                    if (
                        self.job.prodSourceLabel in ["managed", "test", "software", "user", "rucio_test"] + JobUtils.list_ptest_prod_sources
                        and re.search("_sub\d+$", fileDestinationDBlock) is not None
                        and (not self.addToTopOnly)
                        and self.job.destinationSE != "local"
                    ):
                        if self.siteMapper is None:
                            self.logger.error("SiteMapper is None")
                        else:
                            # get dataset spec
                            if fileDestinationDBlock not in self.datasetMap:
                                tmpDS = self.taskBuffer.queryDatasetWithMap({"name": fileDestinationDBlock})
                                self.datasetMap[fileDestinationDBlock] = tmpDS
                            # check if valid dataset
                            if self.datasetMap[fileDestinationDBlock] is None:
                                self.logger.error(f": cannot find {fileDestinationDBlock} in DB")
                            else:
                                if self.datasetMap[fileDestinationDBlock].status not in ["defined"]:
                                    # not a fresh dataset
                                    self.logger.debug(
                                        f": subscription was already made for {self.datasetMap[fileDestinationDBlock].status}:{fileDestinationDBlock}"
                                    )
                                else:
                                    # get DQ2 IDs
                                    tmpSrcDDM = srcSiteSpec.ddm_output[scopeSrcSiteSpec_output]
                                    if self.job.prodSourceLabel == "user" and file.destinationSE not in self.siteMapper.siteSpecList:
                                        # DQ2 ID was set by using --destSE for analysis job to transfer output
                                        tmpDstDDM = file.destinationSE
                                    else:
                                        if DataServiceUtils.getDestinationSE(file.destinationDBlockToken) is not None:
                                            tmpDstDDM = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                                        else:
                                            tmpDstDDM = dstSESiteSpec.ddm_output[scopeDstSESiteSpec_output]
                                    # if src != dest or multi-token
                                    if (tmpSrcDDM != tmpDstDDM) or (tmpSrcDDM == tmpDstDDM and file.destinationDBlockToken.count(",") != 0):
                                        optSub = {
                                            "DATASET_COMPLETE_EVENT": [
                                                f"http://{panda_config.pserverhosthttp}:{panda_config.pserverporthttp}/server/panda/datasetCompleted"
                                            ]
                                        }
                                        # append
                                        if fileDestinationDBlock not in subMap:
                                            subMap[fileDestinationDBlock] = []
                                            # sources
                                            optSource = {}
                                            # set sources
                                            if file.destinationDBlockToken in [
                                                "NULL",
                                                "",
                                            ]:
                                                # use default DQ2 ID as source
                                                optSource[tmpSrcDDM] = {"policy": 0}
                                            else:
                                                # convert token to DQ2 ID
                                                dq2ID = tmpSrcDDM
                                                # use the first token's location as source for T1D1
                                                tmpSrcToken = file.destinationDBlockToken.split(",")[0]
                                                if tmpSrcToken in srcSiteSpec.setokens_output[scopeSrcSiteSpec_output]:
                                                    dq2ID = srcSiteSpec.setokens_output[scopeSrcSiteSpec_output][tmpSrcToken]
                                                optSource[dq2ID] = {"policy": 0}
                                            # T1 used as T2
                                            if (
                                                srcSiteSpec.cloud != self.job.cloud
                                                and (not tmpSrcDDM.endswith("PRODDISK"))
                                                and (self.job.prodSourceLabel not in ["user", "panda"])
                                            ):
                                                # register both DATADISK and PRODDISK as source locations
                                                if "ATLASPRODDISK" in srcSiteSpec.setokens_output[scopeSrcSiteSpec_output]:
                                                    dq2ID = srcSiteSpec.setokens_output[scopeSrcSiteSpec_output]["ATLASPRODDISK"]
                                                    optSource[dq2ID] = {"policy": 0}
                                                if tmpSrcDDM not in optSource:
                                                    optSource[tmpSrcDDM] = {"policy": 0}
                                            # use another location when token is set
                                            if file.destinationDBlockToken not in [
                                                "NULL",
                                                "",
                                            ]:
                                                tmpDQ2IDList = []
                                                tmpDstTokens = file.destinationDBlockToken.split(",")
                                                # remove the first one because it is already used as a location
                                                if tmpSrcDDM == tmpDstDDM:
                                                    tmpDstTokens = tmpDstTokens[1:]
                                                # loop over all tokens
                                                for idxToken, tmpDstToken in enumerate(tmpDstTokens):
                                                    dq2ID = tmpDstDDM
                                                    if tmpDstToken in dstSESiteSpec.setokens_output[scopeDstSESiteSpec_output]:
                                                        dq2ID = dstSESiteSpec.setokens_output[scopeDstSESiteSpec_output][tmpDstToken]
                                                    # keep the fist destination for multi-hop
                                                    if idxToken == 0:
                                                        firstDestDDM = dq2ID
                                                    else:
                                                        # use the fist destination as source for T1D1
                                                        optSource = {}
                                                        optSource[firstDestDDM] = {"policy": 0}
                                                    # remove looping subscription
                                                    if dq2ID == tmpSrcDDM:
                                                        continue
                                                    # avoid duplication
                                                    if dq2ID not in tmpDQ2IDList:
                                                        subMap[fileDestinationDBlock].append((dq2ID, optSub, optSource))
                                            else:
                                                # use default DDM
                                                for dq2ID in tmpDstDDM.split(","):
                                                    subMap[fileDestinationDBlock].append((dq2ID, optSub, optSource))
                except Exception as e:
                    errStr = str(e)
                    self.logger.error(errStr + " " + traceback.format_exc())
                    self.result.setFatal()
                    self.job.ddmErrorDiag = "failed before adding files : " + errStr
                    return 1
        # release some memory
        del dsIdToDsMap
        gc.collect()
        # zipping input files
        if len(zipFileMap) > 0 and not self.addToTopOnly:
            for fileSpec in self.job.Files:
                if fileSpec.type != "input":
                    continue
                zipFileName = None
                for tmpZipFileName in zipFileMap:
                    tmpZipContents = zipFileMap[tmpZipFileName]
                    for tmpZipContent in tmpZipContents:
                        if re.search("^" + tmpZipContent + "$", fileSpec.lfn) is not None:
                            zipFileName = tmpZipFileName
                            break
                    if zipFileName is not None:
                        break
                if zipFileName is not None:
                    if zipFileName not in zipFiles:
                        continue
                    if "files" not in zipFiles[zipFileName]:
                        zipFiles[zipFileName]["files"] = []
                    fileAttrs = {
                        "guid": fileSpec.GUID,
                        "lfn": fileSpec.lfn,
                        "size": fileSpec.fsize,
                        "checksum": fileSpec.checksum,
                        "ds": fileSpec.dataset,
                    }
                    zipFiles[zipFileName]["files"].append(fileAttrs)
        # cleanup submap
        for tmpKey in list(subMap):
            if subMap[tmpKey] == []:
                del subMap[tmpKey]
        # add data to original dataset
        for destinationDBlock in list(idMap):
            origDBlock = None
            match = re.search("^(.+)_sub\d+$", destinationDBlock)
            if match is not None:
                # add files to top-level datasets
                origDBlock = subToDsMap[destinationDBlock]
                if (not self.goToTransferring) or (not self.addToTopOnly and destinationDBlock in distDSs):
                    idMap[origDBlock] = idMap[destinationDBlock]
            # add files to top-level datasets only
            if self.addToTopOnly or self.goToMerging or (destinationDBlock in distDSs and origDBlock is not None):
                del idMap[destinationDBlock]
            # skip sub unless getting transferred
            if origDBlock is not None:
                if not self.goToTransferring and not self.logTransferring and destinationDBlock in idMap:
                    del idMap[destinationDBlock]
        # print idMap
        self.logger.debug(f"idMap = {idMap}")
        self.logger.debug(f"subMap = {subMap}")
        self.logger.debug(f"dsDestMap = {dsDestMap}")
        self.logger.debug(f"extraInfo = {str(self.extraInfo)}")
        # check consistency of destinationDBlock
        hasSub = False
        for destinationDBlock in idMap:
            match = re.search("^(.+)_sub\d+$", destinationDBlock)
            if match is not None:
                hasSub = True
                break
        if idMap != {} and self.goToTransferring and not hasSub:
            errStr = "no sub datasets for transferring. destinationDBlock may be wrong"
            self.logger.error(errStr)
            self.result.setFatal()
            self.job.ddmErrorDiag = "failed before adding files : " + errStr
            return 1
        # add data
        self.logger.debug("addFiles start")
        # count the number of files
        regNumFiles = 0
        regFileList = []
        for tmpRegDS in idMap:
            tmpRegList = idMap[tmpRegDS]
            for tmpRegItem in tmpRegList:
                if tmpRegItem["lfn"] not in regFileList:
                    regNumFiles += 1
                    regFileList.append(tmpRegItem["lfn"])
        # decompose idMap
        if not self.useCentralLFC():
            destIdMap = {None: idMap}
        else:
            destIdMap = self.decomposeIdMap(idMap, dsDestMap, osDsFileMap, subToDsMap)
        # add files
        nTry = 3
        for iTry in range(nTry):
            isFatal = False
            isFailed = False
            regStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            try:
                if not self.useCentralLFC():
                    regMsgStr = f"File registraion for {regNumFiles} files "
                else:
                    regMsgStr = f"File registration with backend={self.ddmBackEnd} for {regNumFiles} files "
                if len(zipFiles) > 0:
                    self.logger.debug(f"registerZipFiles {str(zipFiles)}")
                    rucioAPI.registerZipFiles(zipFiles)
                self.logger.debug(f"registerFilesInDatasets {str(destIdMap)} zip={str(contZipMap)}")
                out = rucioAPI.registerFilesInDataset(destIdMap, contZipMap)
            except (
                DataIdentifierNotFound,
                FileConsistencyMismatch,
                UnsupportedOperation,
                InvalidPath,
                InvalidObject,
                RSENotFound,
                RSEProtocolNotSupported,
                InvalidRSEExpression,
                KeyError,
            ):
                # fatal errors
                errType, errValue = sys.exc_info()[:2]
                out = f"{errType} : {errValue}"
                out += traceback.format_exc()
                isFatal = True
                isFailed = True
            except Exception:
                # unknown errors
                errType, errValue = sys.exc_info()[:2]
                out = f"{errType} : {errValue}"
                out += traceback.format_exc()
                if (
                    "value too large for column" in out
                    or "unique constraint (ATLAS_RUCIO.DIDS_GUID_IDX) violate" in out
                    or "unique constraint (ATLAS_RUCIO.DIDS_PK) violated" in out
                    or "unique constraint (ATLAS_RUCIO.ARCH_CONTENTS_PK) violated" in out
                ):
                    isFatal = True
                else:
                    isFatal = False
                isFailed = True
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
            self.logger.debug(regMsgStr + "took %s.%03d sec" % (regTime.seconds, regTime.microseconds / 1000))
            # failed
            if isFailed or isFatal:
                self.logger.error(f"{out}")
                if (iTry + 1) == nTry or isFatal:
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    # extract important error string
                    extractedErrStr = DataServiceUtils.extractImportantError(out)
                    errMsg = "Could not add files to DDM: "
                    if extractedErrStr == "":
                        self.job.ddmErrorDiag = errMsg + out.split("\n")[-1]
                    else:
                        self.job.ddmErrorDiag = errMsg + extractedErrStr
                    if isFatal:
                        self.result.setFatal()
                    else:
                        self.result.setTemporary()
                    return 1
                self.logger.error(f"Try:{iTry}")
                # sleep
                time.sleep(10)
            else:
                self.logger.debug(f"{str(out)}")
                break
        # release some memory
        del destIdMap
        del dsDestMap
        del osDsFileMap
        del zipFiles
        del contZipMap
        gc.collect()
        # register dataset subscription
        if self.job.processingType == "urgent" or self.job.currentPriority > 1000:
            subActivity = "Express"
        else:
            subActivity = "Production Output"
        if self.job.prodSourceLabel not in ["user"]:
            for tmpName in subMap:
                tmpVal = subMap[tmpName]
                for dq2ID, optSub, optSource in tmpVal:
                    if not self.goToMerging:
                        # make subscription for prod jobs
                        repLifeTime = 14
                        self.logger.debug(
                            "%s %s %s"
                            % (
                                "registerDatasetSubscription",
                                (tmpName, dq2ID),
                                {
                                    "activity": subActivity,
                                    "replica_lifetime": repLifeTime,
                                },
                            )
                        )
                        for iDDMTry in range(3):
                            isFailed = False
                            try:
                                status = rucioAPI.registerDatasetSubscription(
                                    tmpName,
                                    [dq2ID],
                                    owner="panda",
                                    activity=subActivity,
                                    lifetime=repLifeTime,
                                )
                                out = "OK"
                                break
                            except InvalidRSEExpression:
                                status = False
                                errType, errValue = sys.exc_info()[:2]
                                out = f"{errType} {errValue}"
                                isFailed = True
                                self.job.ddmErrorCode = ErrorCode.EC_Subscription
                                break
                            except Exception:
                                status = False
                                errType, errValue = sys.exc_info()[:2]
                                out = f"{errType} {errValue}"
                                isFailed = True
                                # retry for temporary errors
                                time.sleep(10)
                        if isFailed:
                            self.logger.error(f"{out}")
                            if self.job.ddmErrorCode == ErrorCode.EC_Subscription:
                                # fatal error
                                self.job.ddmErrorDiag = f"subscription failure with {out}"
                                self.result.setFatal()
                            else:
                                # temoprary errors
                                self.job.ddmErrorCode = ErrorCode.EC_Adder
                                self.job.ddmErrorDiag = f"could not register subscription : {tmpName}"
                                self.result.setTemporary()
                            return 1
                        self.logger.debug(f"{str(out)}")
                    else:
                        # register location
                        tmpDsNameLoc = subToDsMap[tmpName]
                        repLifeTime = 14
                        for tmpLocName in optSource:
                            self.logger.debug(
                                "%s %s %s %s"
                                % (
                                    "registerDatasetLocation",
                                    tmpDsNameLoc,
                                    tmpLocName,
                                    {"lifetime": "14 days"},
                                )
                            )
                            for iDDMTry in range(3):
                                out = "OK"
                                isFailed = False
                                try:
                                    rucioAPI.registerDatasetLocation(
                                        tmpDsNameLoc,
                                        [tmpLocName],
                                        owner="panda",
                                        activity=subActivity,
                                        lifetime=repLifeTime,
                                    )
                                    out = "OK"
                                    break
                                except Exception:
                                    status = False
                                    errType, errValue = sys.exc_info()[:2]
                                    out = f"{errType} {errValue}"
                                    isFailed = True
                                    # retry for temporary errors
                                    time.sleep(10)
                            if isFailed:
                                self.logger.error(f"{out}")
                                if self.job.ddmErrorCode == ErrorCode.EC_Location:
                                    # fatal error
                                    self.job.ddmErrorDiag = f"location registration failure with {out}"
                                    self.result.setFatal()
                                else:
                                    # temoprary errors
                                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                                    self.job.ddmErrorDiag = f"could not register location : {tmpDsNameLoc}"
                                    self.result.setTemporary()
                                return 1
                            self.logger.debug(f"{str(out)}")
                    # set dataset status
                    self.datasetMap[tmpName].status = "running"
            # keep subscriptions
            self.subscriptionMap = subMap
            # collect list of transfring jobs
            for tmpFile in self.job.Files:
                if tmpFile.type in ["log", "output"]:
                    if self.goToTransferring or (self.logTransferring and tmpFile.type == "log"):
                        # don't go to tranferring for successful ES jobs
                        if (
                            self.job.jobStatus == "finished"
                            and (EventServiceUtils.isEventServiceJob(self.job) and EventServiceUtils.isJumboJob(self.job))
                            and not EventServiceUtils.isJobCloningJob(self.job)
                        ):
                            continue
                        # skip distributed datasets
                        if tmpFile.destinationDBlock in distDSs:
                            continue
                        # skip no output
                        if tmpFile.status == "nooutput":
                            continue
                        # skip alternative stage-out
                        if tmpFile.lfn in self.job.altStgOutFileList():
                            continue
                        self.result.transferringFiles.append(tmpFile.lfn)
        elif "--mergeOutput" not in self.job.jobParameters:
            # send request to DaTRI unless files will be merged
            tmpTopDatasets = {}
            # collect top-level datasets
            for tmpName in subMap:
                tmpVal = subMap[tmpName]
                for dq2ID, optSub, optSource in tmpVal:
                    tmpTopName = subToDsMap[tmpName]
                    # append
                    if tmpTopName not in tmpTopDatasets:
                        tmpTopDatasets[tmpTopName] = []
                    if dq2ID not in tmpTopDatasets[tmpTopName]:
                        tmpTopDatasets[tmpTopName].append(dq2ID)
            # remove redundant CN from DN
            tmpDN = self.job.prodUserID
            # send request
            if tmpTopDatasets != {} and self.jobStatus == "finished":
                try:
                    status, userInfo = rucioAPI.finger(tmpDN)
                    if not status:
                        raise RuntimeError(f"user info not found for {tmpDN} with {userInfo}")
                    userEPs = []
                    # loop over all output datasets
                    for tmpDsName in tmpTopDatasets:
                        dq2IDlist = tmpTopDatasets[tmpDsName]
                        for tmpDQ2ID in dq2IDlist:
                            if tmpDQ2ID == "NULL":
                                continue
                            if tmpDQ2ID not in userEPs:
                                userEPs.append(tmpDQ2ID)
                            # use group account for group.*
                            if tmpDsName.startswith("group") and self.job.workingGroup not in ["", "NULL", None]:
                                tmpDN = self.job.workingGroup
                            else:
                                tmpDN = userInfo["nickname"]
                            tmpMsg = f"registerDatasetLocation for Rucio ds={tmpDsName} site={tmpDQ2ID} id={tmpDN}"
                            self.logger.debug(tmpMsg)
                            rucioAPI.registerDatasetLocation(
                                tmpDsName,
                                [tmpDQ2ID],
                                owner=tmpDN,
                                activity="Analysis Output",
                            )
                    # set dataset status
                    for tmpName in subMap:
                        self.datasetMap[tmpName].status = "running"
                except (InsufficientAccountLimit, InvalidRSEExpression) as errType:
                    tmpMsg = f"Rucio rejected to transfer files to {','.join(userEPs)} since {errType}"
                    self.logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"Rucio failed with {errType}"
                    # set dataset status
                    for tmpName in subMap:
                        self.datasetMap[tmpName].status = "running"
                    # send warning
                    tmpST = self.taskBuffer.update_problematic_resource_info(self.job.prodUserName, self.job.jediTaskID, userEPs[0], "dest")
                    if not tmpST:
                        self.logger.debug("skip to send warning since already done")
                    else:
                        toAdder = self.taskBuffer.getEmailAddr(self.job.prodUserName)
                        if toAdder is None or toAdder.startswith("notsend"):
                            self.logger.debug("skip to send warning since suppressed")
                        else:
                            tmpSM = self.sendEmail(toAdder, tmpMsg, self.job.jediTaskID)
                            self.logger.debug(f"sent warning with {tmpSM}")
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    tmpMsg = f"registerDatasetLocation failed with {errType} {errValue}"
                    self.logger.error(tmpMsg)
                    self.job.ddmErrorCode = ErrorCode.EC_Adder
                    self.job.ddmErrorDiag = f"Rucio failed with {errType} {errValue}"
        # collect list of merging files
        if self.goToMerging and self.jobStatus not in ["failed", "cancelled", "closed"]:
            for tmpFileList in idMap.values():
                for tmpFile in tmpFileList:
                    if tmpFile["lfn"] not in self.result.mergingFiles:
                        self.result.mergingFiles.append(tmpFile["lfn"])
        # register ES files
        if (EventServiceUtils.isEventServiceJob(self.job) or EventServiceUtils.isJumboJob(self.job)) and not EventServiceUtils.isJobCloningJob(self.job):
            if self.job.registerEsFiles():
                try:
                    self.registerEventServiceFiles()
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    self.logger.error(f"failed to register ES files with {errType}:{errValue}")
                    self.result.setTemporary()
                    return 1
        # properly finished
        self.logger.debug("addFiles end")
        return 0

    # use cerntral LFC
    def useCentralLFC(self):
        if not self.addToTopOnly:
            return True
        return False

    # decompose idMap
    def decomposeIdMap(self, idMap, dsDestMap, osDsFileMap, subToDsMap):
        # add item for top datasets
        for tmpDS in list(dsDestMap):
            tmpTopDS = subToDsMap[tmpDS]
            if tmpTopDS != tmpDS:
                dsDestMap[tmpTopDS] = dsDestMap[tmpDS]
        destIdMap = {}
        for tmpDS in idMap:
            tmpFiles = idMap[tmpDS]
            for tmpDest in dsDestMap[tmpDS]:
                if tmpDest not in destIdMap:
                    destIdMap[tmpDest] = {}
                destIdMap[tmpDest][tmpDS] = tmpFiles
        # add OS stuff
        for tmpDest in osDsFileMap:
            tmpIdMap = osDsFileMap[tmpDest]
            for tmpDS in tmpIdMap:
                tmpFiles = tmpIdMap[tmpDS]
                if tmpDest not in destIdMap:
                    destIdMap[tmpDest] = {}
                destIdMap[tmpDest][tmpDS] = tmpFiles
        return destIdMap

    # send email notification
    def sendEmail(self, toAdder, message, jediTaskID):
        # subject
        mailSubject = f"PANDA WARNING for TaskID:{jediTaskID} with --destSE"
        # message
        mailBody = f"Hello,\n\nTaskID:{jediTaskID} cannot process the --destSE request\n\n"
        mailBody += f"Reason : {message}\n"
        # send
        retVal = MailUtils().send(toAdder, mailSubject, mailBody)
        # return
        return retVal

    # register ES files
    def registerEventServiceFiles(self):
        self.logger.debug("registering ES files")
        try:
            # get ES dataset name
            esDataset = EventServiceUtils.getEsDatasetName(self.job.jediTaskID)
            # collect files
            idMap = dict()
            fileSet = set()
            for fileSpec in self.job.Files:
                if fileSpec.type != "zipoutput":
                    continue
                if fileSpec.lfn in fileSet:
                    continue
                fileSet.add(fileSpec.lfn)
                # make file data
                fileData = {
                    "scope": EventServiceUtils.esScopeDDM,
                    "name": fileSpec.lfn,
                    "bytes": fileSpec.fsize,
                    "panda_id": fileSpec.PandaID,
                    "task_id": fileSpec.jediTaskID,
                }
                if fileSpec.GUID not in [None, "NULL", ""]:
                    fileData["guid"] = fileSpec.GUID
                if fileSpec.dispatchDBlockToken not in [None, "NULL", ""]:
                    try:
                        fileData["events"] = int(fileSpec.dispatchDBlockToken)
                    except Exception:
                        pass
                if fileSpec.checksum not in [None, "NULL", ""]:
                    fileData["checksum"] = fileSpec.checksum
                # get endpoint ID
                epID = int(fileSpec.destinationSE.split("/")[0])
                # convert to DDM endpoint
                rse = self.taskBuffer.convertObjIDtoEndPoint(panda_config.endpoint_mapfile, epID)
                if rse is not None and rse["is_deterministic"]:
                    epName = rse["name"]
                    if epName not in idMap:
                        idMap[epName] = dict()
                    if esDataset not in idMap[epName]:
                        idMap[epName][esDataset] = []
                    idMap[epName][esDataset].append(fileData)
            # add files to dataset
            if idMap != {}:
                self.logger.debug(f"adding ES files {str(idMap)}")
                try:
                    rucioAPI.registerFilesInDataset(idMap)
                except DataIdentifierNotFound:
                    self.logger.debug("ignored DataIdentifierNotFound")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            errStr = f" : {errtype} {errvalue}"
            errStr += traceback.format_exc()
            self.logger.error(errStr)
            raise
        self.logger.debug("done")
