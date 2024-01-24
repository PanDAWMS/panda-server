"""
find candidate site to distribute input datasets

"""

import datetime
import fnmatch
import math
import random
import re
import sys
import time
import uuid

import pandaserver.brokerage.broker
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.JobSpec import JobSpec

# logger
_logger = PandaLogger().getLogger("DynDataDistributer")


def initLogger(pLogger):
    # redirect logging to parent
    global _logger
    _logger = pLogger


# NG datasets
ngDataTypes = ["RAW", "HITS", "RDO", "ESD", "EVNT"]

# excluded provenance
ngProvenance = []

# protection for max number of replicas
protectionMaxNumReplicas = 10

# max number of waiting jobs
maxWaitingJobs = 200

# max number of waiting jobsets
maxWaitingJobsets = 2

# clouds with small T1 to make replica at T2
cloudsWithSmallT1 = ["IT"]

# files in datasets
g_filesInDsMap = {}


class DynDataDistributer:
    # constructor
    def __init__(self, jobs, taskBuffer, siteMapper, simul=False, token=None, logger=None):
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        if token is None:
            self.token = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" ")
        else:
            self.token = token
        # use a fixed list since some clouds don't have active T2s
        self.pd2pClouds = ["CA", "DE", "ES", "FR", "IT", "ND", "NL", "TW", "UK", "US"]
        self.simul = simul
        self.lastMessage = ""
        self.cachedSizeMap = {}
        self.shareMoUForT2 = None
        self.mapTAGandParentGUIDs = {}
        self.tagParentInfo = {}
        self.parentLfnToTagMap = {}
        self.logger = logger

    # get candidate sites for subscription
    def getCandidates(
        self,
        inputDS,
        prodsourcelabel,
        job_label,
        checkUsedFile=True,
        useHidden=False,
        useCloseSites=False,
    ):
        # return for failure
        failedRet = False, {"": {"": ([], [], [], 0, False, False, 0, 0, [])}}
        # get replica locations
        if inputDS.endswith("/"):
            # container
            status, tmpRepMaps = self.getListDatasetReplicasInContainer(inputDS)
            # get used datasets
            if status and checkUsedFile:
                status, tmpUsedDsList = self.getUsedDatasets(tmpRepMaps)
                # remove unused datasets
                newRepMaps = {}
                for tmpKey in tmpRepMaps:
                    tmpVal = tmpRepMaps[tmpKey]
                    if tmpKey in tmpUsedDsList:
                        newRepMaps[tmpKey] = tmpVal
                tmpRepMaps = newRepMaps
        else:
            # normal dataset
            status, tmpRepMap = self.getListDatasetReplicas(inputDS)
            tmpRepMaps = {inputDS: tmpRepMap}
        if not status:
            # failed
            self.putLog(f"failed to get replica locations for {inputDS}", "error")
            return failedRet
        # get close sites
        closeSitesMap = {}
        # get all sites
        allSiteMap = []
        for tmpSiteName in self.siteMapper.siteSpecList:
            tmpSiteSpec = self.siteMapper.siteSpecList[tmpSiteName]
            # check cloud
            if tmpSiteSpec.cloud not in self.pd2pClouds:
                continue
            # ignore test sites
            if "test" in tmpSiteName.lower():
                continue
            # analysis only
            if not tmpSiteSpec.runs_analysis():
                continue
            # skip GPU
            if tmpSiteSpec.isGPU():
                continue
            # skip VP
            if tmpSiteSpec.use_vp(JobUtils.ANALY_PS):
                continue
            # online
            if tmpSiteSpec.status not in ["online"]:
                continue
            allSiteMap.append(tmpSiteSpec)
        # NG DQ2 IDs
        ngDQ2SuffixList = ["LOCALGROUPDISK", "STAGING"]
        # loop over all clouds
        returnMap = {}
        cloud = "WORLD"
        # loop over all datasets
        for tmpDS in tmpRepMaps:
            tmpRepMap = tmpRepMaps[tmpDS]
            candSites = []
            sitesComDS = []
            sitesCompPD2P = []
            t1HasReplica = False
            t1HasPrimary = False
            nSecReplicas = 0
            candForMoU = []
            # check sites
            nUserSub = 0
            for tmpSiteSpec in allSiteMap:
                tmp_scope_input, tmp_scope_output = select_scope(tmpSiteSpec, prodsourcelabel, job_label)
                if tmp_scope_input not in tmpSiteSpec.ddm_endpoints_input:
                    continue
                rses = tmpSiteSpec.ddm_endpoints_input[tmp_scope_input].getLocalEndPoints()
                hasReplica = False
                for tmpDQ2ID in tmpRepMap:
                    tmpStatMap = tmpRepMap[tmpDQ2ID]
                    if tmpDQ2ID in rses and tmpStatMap[0]["total"] == tmpStatMap[0]["found"] and tmpDQ2ID.endswith("DATADISK"):
                        # complete
                        sitesComDS.append(tmpSiteSpec.sitename)
                        hasReplica = True
                        break
                # site doesn't have a replica
                if hasReplica or not useCloseSites:
                    candSites.append(tmpSiteSpec.sitename)
            # append
            returnMap.setdefault(tmpDS, {})
            if sitesComDS:
                candSites = sitesComDS
            returnMap[tmpDS][cloud] = (
                candSites,
                sitesComDS,
                sitesCompPD2P,
                nUserSub,
                t1HasReplica,
                t1HasPrimary,
                nSecReplicas,
                0,
                candForMoU,
            )
        # return
        return True, returnMap

    # get list of replicas for a dataset
    def getListDatasetReplicas(self, dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog(f"{iDDMTry}/{nTry} listDatasetReplicas {dataset}")
            status, out = rucioAPI.listDatasetReplicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break
        # result
        if status != 0:
            self.putLog(out, "error")
            self.putLog(f"bad response for {dataset}", "error")
            return False, {}
        self.putLog(f"getListDatasetReplicas->{str(out)}")
        return True, out

    # get replicas for a container
    def getListDatasetReplicasInContainer(self, container):
        # response for failure
        resForFailure = False, {}f
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog(f"{iDDMTry}/{nTry} listDatasetsInContainer {container}")
            datasets, out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(60)
            else:
                break
        if datasets is None:
            self.putLog(out, "error")
            self.putLog(f"bad DDM response for {container}", "error")
            return resForFailure
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            # get replicas
            status, tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return resForFailure
            # append
            allRepMap[dataset] = tmpRepSites
        # return
        self.putLog("getListDatasetReplicasInContainer done")
        return True, allRepMap

    # get datasets used by jobs
    def getUsedDatasets(self, datasetMap):
        resForFailure = (False, [])
        # loop over all datasets
        usedDsList = []
        for datasetName in datasetMap:
            # get file list
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog(f"{iDDMTry}/{nTry} listFilesInDataset {datasetName}")
                    fileItems, out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType, errValue = sys.exc_info()[:2]
                    out = f"{errType} {errValue}"
                    time.sleep(60)
            if not status:
                self.putLog(out, "error")
                self.putLog(f"bad DDM response to get size of {datasetName}", "error")
                return resForFailure
            # get
            # check if jobs use the dataset
            usedFlag = False
            for tmpJob in self.jobs:
                for tmpFile in tmpJob.Files:
                    if tmpFile.type == "input" and tmpFile.lfn in fileItems:
                        usedFlag = True
                        break
                # escape
                if usedFlag:
                    break
            # used
            if usedFlag:
                usedDsList.append(datasetName)
        # return
        self.putLog(f"used datasets = {str(usedDsList)}")
        return True, usedDsList

    # get file from dataset
    def getFileFromDataset(self, datasetName, guid, randomMode=False, nSamples=1):
        resForFailure = (False, None)
        # get files in datasets
        global g_filesInDsMap
        if datasetName not in g_filesInDsMap:
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog(f"{iDDMTry}/{nTry} listFilesInDataset {datasetName}")
                    fileItems, out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType, errValue = sys.exc_info()[:2]
                    out = f"{errType} {errValue}"
                    time.sleep(60)
            if not status:
                self.putLog(out, "error")
                self.putLog(f"bad DDM response to get size of {datasetName}", "error")
                return resForFailure
            # append
            g_filesInDsMap[datasetName] = fileItems
        # random mode
        if randomMode:
            tmpList = list(g_filesInDsMap[datasetName])
            random.shuffle(tmpList)
            retList = []
            for iSamples in range(nSamples):
                if iSamples < len(tmpList):
                    tmpLFN = tmpList[iSamples]
                    retMap = g_filesInDsMap[datasetName][tmpLFN]
                    retMap["lfn"] = tmpLFN
                    retMap["dataset"] = datasetName
                    retList.append(retMap)
            return True, retList
        # return
        for tmpLFN in g_filesInDsMap[datasetName]:
            tmpVal = g_filesInDsMap[datasetName][tmpLFN]
            if uuid.UUID(tmpVal["guid"]) == uuid.UUID(guid):
                retMap = tmpVal
                retMap["lfn"] = tmpLFN
                retMap["dataset"] = datasetName
                return True, retMap
        return resForFailure

    # register new dataset container with datasets
    def registerDatasetContainerWithDatasets(self, containerName, files, replicaMap, nSites=1, owner=None):
        # parse DN
        if owner is not None:
            status, userInfo = rucioAPI.finger(owner)
            if not status:
                self.putLog(f"failed to finger: {userInfo}")
            else:
                owner = userInfo["nickname"]
            self.putLog(f"parsed DN={owner}")
        # sort by locations
        filesMap = {}
        for tmpFile in files:
            tmpLocations = sorted(replicaMap[tmpFile["dataset"]])
            newLocations = []
            # skip STAGING
            for tmpLocation in tmpLocations:
                if not tmpLocation.endswith("STAGING"):
                    newLocations.append(tmpLocation)
            if newLocations == []:
                continue
            tmpLocations = newLocations
            tmpKey = tuple(tmpLocations)
            filesMap.setdefault(tmpKey, [])
            # append file
            filesMap[tmpKey].append(tmpFile)
        # get nfiles per dataset
        nFilesPerDataset, tmpR = divmod(len(files), nSites)
        if nFilesPerDataset == 0:
            nFilesPerDataset = 1
        maxFilesPerDataset = 1000
        if nFilesPerDataset >= maxFilesPerDataset:
            nFilesPerDataset = maxFilesPerDataset
        # register new datasets
        datasetNames = []
        tmpIndex = 1
        for tmpLocations in filesMap:
            tmpFiles = filesMap[tmpLocations]
            tmpSubIndex = 0
            while tmpSubIndex < len(tmpFiles):
                tmpDsName = containerName[:-1] + "_%04d" % tmpIndex
                tmpRet = self.registerDatasetWithLocation(
                    tmpDsName,
                    tmpFiles[tmpSubIndex : tmpSubIndex + nFilesPerDataset],
                    # tmpLocations,owner=owner)
                    tmpLocations,
                    owner=None,
                )
                # failed
                if not tmpRet:
                    self.putLog(f"failed to register {tmpDsName}", "error")
                    return False
                # append dataset
                datasetNames.append(tmpDsName)
                tmpIndex += 1
                tmpSubIndex += nFilesPerDataset
        # register container
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                self.putLog(f"{iDDMTry}/{nTry} registerContainer {containerName}")
                status = rucioAPI.registerContainer(containerName, datasetNames)
                out = "OK"
                break
            except Exception:
                status = False
                errType, errValue = sys.exc_info()[:2]
                out = f"{errType} {errValue}"
                time.sleep(10)
        if not status:
            self.putLog(out, "error")
            self.putLog(f"bad DDM response to register {containerName}", "error")
            return False
        # return
        self.putLog(out)
        return True

    # register new dataset with locations
    def registerDatasetWithLocation(self, datasetName, files, locations, owner=None):
        resForFailure = False
        # get file info
        guids = []
        lfns = []
        fsizes = []
        chksums = []
        for tmpFile in files:
            guids.append(tmpFile["guid"])
            lfns.append(tmpFile["scope"] + ":" + tmpFile["lfn"])
            fsizes.append(int(tmpFile["filesize"]))
            chksums.append(tmpFile["checksum"])
        # register new dataset
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                self.putLog(f"{iDDMTry}/{nTry} registerNewDataset {datasetName} len={len(files)}")
                out = rucioAPI.registerDataset(datasetName, lfns, guids, fsizes, chksums, lifetime=14)
                self.putLog(out)
                break
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                self.putLog(f"{errType} {errValue}", "error")
                if iDDMTry + 1 == nTry:
                    self.putLog(f"failed to register {datasetName} in rucio")
                    return resForFailure
                time.sleep(10)
        # freeze dataset
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog(f"{iDDMTry}/{nTry} freezeDataset {datasetName}")
            try:
                rucioAPI.closeDataset(datasetName)
                status = True
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                out = f"failed to freeze : {errtype} {errvalue}"
                status = False
            if not status:
                time.sleep(10)
            else:
                break
        if not status:
            self.putLog(out, "error")
            self.putLog(f"bad DDM response to freeze {datasetName}", "error")
            return resForFailure
        # register locations
        for tmpLocation in locations:
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog(f"{iDDMTry}/{nTry} registerDatasetLocation {datasetName} {tmpLocation}")
                    out = rucioAPI.registerDatasetLocation(datasetName, [tmpLocation], 14, owner)
                    self.putLog(out)
                    status = True
                    break
                except Exception:
                    status = False
                    errType, errValue = sys.exc_info()[:2]
                    self.putLog(f"{errType} {errValue}", "error")
                    if iDDMTry + 1 == nTry:
                        self.putLog(f"failed to register {datasetName} in rucio")
                        return resForFailure
                    time.sleep(10)
            if not status:
                self.putLog(out, "error")
                self.putLog(f"bad DDM response to set owner {datasetName}", "error")
                return resForFailure
        return True

    # list datasets by file GUIDs
    def listDatasetsByGUIDs(self, guids, dsFilters):
        resForFailure = (False, {})
        resForFatal = (False, {"isFatal": True})
        # get size of datasets
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog(f"{iDDMTry}/{nTry} listDatasetsByGUIDs GUIDs={str(guids)}")
            try:
                out = rucioAPI.listDatasetsByGUIDs(guids)
                status = True
                break
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                out = f"failed to get datasets with GUIDs : {errtype} {errvalue}"
                status = False
                time.sleep(10)
        if not status:
            self.putLog(out, "error")
            self.putLog("bad response to list datasets by GUIDs", "error")
            if "DataIdentifierNotFound" in out:
                return resForFatal
            return resForFailure
        self.putLog(out)
        # get map
        retMap = {}
        try:
            outMap = out
            for guid in guids:
                tmpDsNames = []
                # GUID not found
                if guid not in outMap:
                    self.putLog(f"GUID={guid} not found", "error")
                    return resForFatal
                # ignore junk datasets
                for tmpDsName in outMap[guid]:
                    if (
                        tmpDsName.startswith("panda")
                        or tmpDsName.startswith("user")
                        or tmpDsName.startswith("group")
                        or tmpDsName.startswith("archive")
                        or re.search("_sub\d+$", tmpDsName) is not None
                        or re.search("_dis\d+$", tmpDsName) is not None
                        or re.search("_shadow$", tmpDsName) is not None
                    ):
                        continue
                    # check with filters
                    if dsFilters != []:
                        flagMatch = False
                        for tmpFilter in dsFilters:
                            if fnmatch.fnmatchcase(tmpDsName, tmpFilter):
                                flagMatch = True
                                break
                        # not match
                        if not flagMatch:
                            continue
                    # append
                    tmpDsNames.append(tmpDsName)
                # empty
                if tmpDsNames == []:
                    self.putLog(f"no datasets found for GUID={guid}")
                    continue
                # duplicated
                if len(tmpDsNames) != 1:
                    self.putLog(f"use the first dataset in {str(tmpDsNames)} for GUID:{guid}")
                # append
                retMap[guid] = tmpDsNames[0]
        except Exception:
            self.putLog("failed to list datasets by GUIDs", "error")
            return resForFailure
        return True, retMap

    # convert event/run list to datasets
    def convertEvtRunToDatasets(
        self,
        runEvtList,
        dsType,
        streamName,
        dsFilters,
        amiTag,
        user,
        runEvtGuidMap,
        ei_api,
    ):
        self.putLog(f"convertEvtRunToDatasets type={dsType} stream={streamName} dsPatt={str(dsFilters)} amitag={amiTag}")
        # check data type
        failedRet = False, {}, []
        fatalRet = False, {"isFatal": True}, []
        streamRef = "Stream" + dsType
        # import event lookup client
        if runEvtGuidMap == {}:
            if len(runEvtList) == 0:
                self.putLog("Empty list for run and events was provided", type="error")
                return failedRet
            # Hadoop EI
            from .eventLookupClientEI import eventLookupClientEI

            elssiIF = eventLookupClientEI()
            # loop over all events
            nEventsPerLoop = 500
            iEventsTotal = 0
            while iEventsTotal < len(runEvtList):
                tmpRunEvtList = runEvtList[iEventsTotal : iEventsTotal + nEventsPerLoop]
                self.putLog(f"EI lookup for {iEventsTotal}/{len(runEvtList)}")
                iEventsTotal += nEventsPerLoop
                regStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                guidListELSSI, tmpCom, tmpOut, tmpErr = elssiIF.doLookup(
                    tmpRunEvtList,
                    stream=streamName,
                    tokens=streamRef,
                    amitag=amiTag,
                    user=user,
                    ei_api=ei_api,
                )
                regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
                self.putLog(f"EI command: {tmpCom}")
                self.putLog(f"took {regTime.seconds}.{regTime.microseconds / 1000:03f} sec for {len(tmpRunEvtList)} events")
                # failed
                if tmpErr not in [None, ""] or len(guidListELSSI) == 0:
                    self.putLog(tmpCom)
                    self.putLog(tmpOut)
                    self.putLog(tmpErr)
                    self.putLog("invalid return from EventIndex", type="error")
                    return failedRet
                # check events
                for runNr, evtNr in tmpRunEvtList:
                    paramStr = f"Run:{runNr} Evt:{evtNr} Stream:{streamName}"
                    self.putLog(paramStr)
                    tmpRunEvtKey = (int(runNr), int(evtNr))
                    # not found
                    if tmpRunEvtKey not in guidListELSSI or len(guidListELSSI[tmpRunEvtKey]) == 0:
                        self.putLog(tmpCom)
                        self.putLog(tmpOut)
                        self.putLog(tmpErr)
                        errStr = f"no GUIDs were found in EventIndex for {paramStr}"
                        self.putLog(errStr, type="error")
                        return fatalRet
                    # append
                    runEvtGuidMap[tmpRunEvtKey] = guidListELSSI[tmpRunEvtKey]
        # convert to datasets
        allDatasets = []
        allFiles = []
        allLocations = {}
        for tmpIdx in runEvtGuidMap:
            tmpguids = runEvtGuidMap[tmpIdx]
            runNr, evtNr = tmpIdx
            tmpDsRet, tmpDsMap = self.listDatasetsByGUIDs(tmpguids, dsFilters)
            # failed
            if not tmpDsRet:
                self.putLog("failed to convert GUIDs to datasets", type="error")
                if "isFatal" in tmpDsMap and tmpDsMap["isFatal"] is True:
                    return fatalRet
                return failedRet
            # empty
            if tmpDsMap == {}:
                self.putLog(
                    f"there is no dataset for Run:{runNr} Evt:{evtNr} GUIDs:{str(tmpguids)}",
                    type="error",
                )
                return fatalRet
            if len(tmpDsMap) != 1:
                self.putLog(
                    f"there are multiple datasets {str(tmpDsMap)} for Run:{runNr} Evt:{evtNr} GUIDs:{str(tmpguids)}",
                    type="error",
                )
                return fatalRet
            # append
            for tmpGUID in tmpDsMap:
                tmpDsName = tmpDsMap[tmpGUID]
                # collect dataset names
                if tmpDsName not in allDatasets:
                    allDatasets.append(tmpDsName)
                    # get location
                    statRep, replicaMap = self.getListDatasetReplicas(tmpDsName)
                    # failed
                    if not statRep:
                        self.putLog(
                            f"failed to get locations for DS:{tmpDsName}",
                            type="error",
                        )
                        return failedRet
                    # collect locations
                    tmpLocationList = []
                    for tmpLocation in replicaMap:
                        # use only complete replicas
                        dsStatDict = replicaMap[tmpLocation][0]
                        if dsStatDict["total"] is not None and dsStatDict["total"] == dsStatDict["found"]:
                            if tmpLocation not in tmpLocationList:
                                tmpLocationList.append(tmpLocation)
                    allLocations[tmpDsName] = tmpLocationList
                # get file info
                tmpFileRet, tmpFileInfo = self.getFileFromDataset(tmpDsName, tmpGUID)
                # failed
                if not tmpFileRet:
                    self.putLog(
                        f"failed to get fileinfo for GUID:{tmpGUID} DS:{tmpDsName}",
                        type="error",
                    )
                    return failedRet
                # collect files
                allFiles.append(tmpFileInfo)
        # return
        self.putLog(f"converted to {str(allDatasets)}, {str(allLocations)}, {str(allFiles)}")
        return True, allLocations, allFiles

    # put log
    def putLog(self, msg, type="debug", sendLog=False, actionTag="", tagsMap={}):
        if self.logger is None:
            tmpMsg = self.token + " " + str(msg)
        else:
            tmpMsg = str(msg)
        if type == "error":
            if self.logger is None:
                _logger.error(tmpMsg)
            else:
                self.logger.error(tmpMsg)
            # keep last error message
            self.lastMessage = tmpMsg
        else:
            if self.logger is None:
                _logger.debug(tmpMsg)
            else:
                self.logger.debug(tmpMsg)
        # send to logger
        if sendLog:
            tmpMsg = self.token + " - "
            if actionTag != "":
                tmpMsg += f"action={actionTag} "
                for tmpTag in tagsMap:
                    tmpTagVal = tagsMap[tmpTag]
                    tmpMsg += f"{tmpTag}={tmpTagVal} "
            tmpMsg += "- " + msg
            tmpPandaLogger = PandaLogger()
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({"Type": "pd2p"})
            tmpLog = tmpPandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if type == "error":
                tmpLog.error(tmpMsg)
            else:
                tmpLog.info(tmpMsg)
            # release HTTP handler
            tmpPandaLogger.release()
            time.sleep(1)