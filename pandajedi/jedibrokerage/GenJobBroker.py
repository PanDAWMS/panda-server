import random
import re

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedirefine import RefinerUtils
from pandaserver.srvcore import CoreUtils

from . import AtlasBrokerUtils
from .JobBrokerBase import JobBrokerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# brokerage for general purpose
class GenJobBroker(JobBrokerBase):
    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        JobBrokerBase.__init__(self, ddmIF, taskBufferIF)

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger, f"<jediTaskID={taskSpec.jediTaskID}>")
        tmpLog.debug("start")
        # return for failure
        retFatal = self.SC_FATAL, inputChunk
        retTmpError = self.SC_FAILED, inputChunk
        # set cloud
        try:
            if not taskParamMap:
                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
                taskParamMap = RefinerUtils.decodeJSON(taskParam)
            if not taskSpec.cloud and "cloud" in taskParamMap:
                taskSpec.cloud = taskParamMap["cloud"]
        except Exception:
            pass
        # get sites in the cloud
        site_preassigned = True
        if taskSpec.site not in ["", None]:
            tmpLog.debug(f"site={taskSpec.site} is pre-assigned")
            if self.siteMapper.checkSite(taskSpec.site):
                scanSiteList = [taskSpec.site]
            else:
                scanSiteList = []
                for tmpSite in self.siteMapper.getCloud(taskSpec.cloud)["sites"]:
                    if re.search(taskSpec.site, tmpSite):
                        scanSiteList.append(tmpSite)
                if not scanSiteList:
                    tmpLog.error(f"unknown site={taskSpec.site}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
        elif inputChunk.getPreassignedSite() is not None:
            scanSiteList = [inputChunk.getPreassignedSite()]
            tmpLog.debug(f"site={inputChunk.getPreassignedSite()} is pre-assigned in masterDS")
        else:
            site_preassigned = False
            scanSiteList = self.siteMapper.getCloud(taskSpec.cloud)["sites"]
            # remove NA
            if "NA" in scanSiteList:
                scanSiteList.remove("NA")
            tmpLog.debug(f"cloud={taskSpec.cloud} has {len(scanSiteList)} candidates")
        tmpLog.debug(f"initial {len(scanSiteList)} candidates")
        ######################################
        # selection for status and PandaSite
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check site status
            if tmpSiteSpec.status != "online" and not site_preassigned:
                tmpLog.debug(f"  skip {tmpSiteName} due to status={tmpSiteSpec.status}")
                continue
            # check PandaSite
            if "PandaSite" in taskParamMap and taskParamMap["PandaSite"]:
                if tmpSiteSpec.pandasite != taskParamMap["PandaSite"]:
                    tmpLog.debug(f"  skip {tmpSiteName} due to wrong PandaSite={tmpSiteSpec.pandasite} <> {taskParamMap['PandaSite']}")
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug(f"{len(scanSiteList)} candidates passed site status check")
        if scanSiteList == []:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for scratch disk
        minDiskCountS = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCountS = minDiskCountS // 1024 // 1024
        # size for direct IO sites
        if taskSpec.useLocalIO():
            minDiskCountR = minDiskCountS
        else:
            minDiskCountR = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()
            minDiskCountR = minDiskCountR // 1024 // 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir:
                if CoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
                    minDiskCount = minDiskCountR
                else:
                    minDiskCount = minDiskCountS
                if minDiskCount > tmpSiteSpec.maxwdir:
                    tmpLog.debug(f"  skip {tmpSiteName} due to small scratch disk={tmpSiteSpec.maxwdir} < {minDiskCount}")
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug(f"{len(scanSiteList)} candidates passed scratch disk check")
        if scanSiteList == []:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for available space in SE
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            # check at the site
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # free space must be >= 200GB
            diskThreshold = 200
            tmpSpaceSize = tmpSiteSpec.space
            if tmpSiteSpec.space and tmpSpaceSize < diskThreshold:
                tmpLog.debug(f"  skip {tmpSiteName} due to disk shortage in SE = {tmpSiteSpec.space} < {diskThreshold}GB")
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug(f"{len(scanSiteList)} candidates passed SE space check")
        if not scanSiteList:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for walltime
        minWalltime = taskSpec.walltime
        if minWalltime not in [0, None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxtime != 0 and minWalltime > tmpSiteSpec.maxtime:
                    tmpLog.debug(f"  skip {tmpSiteName} due to short site walltime={tmpSiteSpec.maxtime}(site upper limit) < {minWalltime}")
                    continue
                if tmpSiteSpec.mintime != 0 and minWalltime < tmpSiteSpec.mintime:
                    tmpLog.debug(f"  skip {tmpSiteName} due to short job walltime={tmpSiteSpec.mintime}(site lower limit) > {minWalltime}")
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.debug(f"{len(scanSiteList)} candidates passed walltime check ={minWalltime}{taskSpec.walltimeUnit}")
            if not scanSiteList:
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for MP
        if taskSpec.coreCount is not None and taskSpec.coreCount >= 0:
            if not site_preassigned:
                newScanSiteList = []
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # check at the site
                    is_ok = False
                    if taskSpec.coreCount == 0:
                        # any
                        is_ok = True
                    elif taskSpec.coreCount == 1:
                        # score
                        if tmpSiteSpec.coreCount in [0, 1, -1, None]:
                            is_ok = True
                    else:
                        # mcore
                        if tmpSiteSpec.coreCount in [0, -1]:
                            is_ok = True
                        elif tmpSiteSpec.coreCount and tmpSiteSpec.coreCount >= taskSpec.coreCount:
                            is_ok = True
                    if is_ok:
                        newScanSiteList.append(tmpSiteName)
                    else:
                        tmpLog.info(
                            f"  skip site={tmpSiteName} due to core mismatch site:{tmpSiteSpec.coreCount} <> task:{taskSpec.coreCount} criteria=-cpucore"
                        )
                scanSiteList = newScanSiteList
                tmpLog.info(f"{len(scanSiteList)} candidates passed for core count check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
        ######################################
        # selection for memory
        origMinRamCount = inputChunk.getMaxRamCount()
        if not site_preassigned and origMinRamCount:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement
                if taskSpec.ramPerCore():
                    if tmpSiteSpec.coreCount and tmpSiteSpec.coreCount > 0:
                        minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    else:
                        minRamCount = origMinRamCount * (taskSpec.coreCount if taskSpec.coreCount else 1)
                    minRamCount += taskSpec.baseRamCount if taskSpec.baseRamCount else 0
                else:
                    minRamCount = origMinRamCount
                # site max memory requirement
                site_maxmemory = tmpSiteSpec.maxrss if tmpSiteSpec.maxrss else 0
                # check at the site
                if site_maxmemory and minRamCount and minRamCount > site_maxmemory:
                    tmpMsg = f"  skip site={tmpSiteName} due to site RAM shortage. {site_maxmemory} (site upper limit) less than {minRamCount} "
                    tmpLog.debug(tmpMsg)
                    continue
                # site min memory requirement
                site_minmemory = tmpSiteSpec.minrss if tmpSiteSpec.minrss else 0
                if site_minmemory and minRamCount and minRamCount < site_minmemory:
                    tmpMsg = f"  skip site={tmpSiteName} due to job RAM shortage. {site_minmemory} (site lower limit) greater than {minRamCount} "
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.debug(f"{len(scanSiteList)} candidates passed memory check")
            if not scanSiteList:
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection with processing types
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            allowed_list = tmpSiteSpec.get_allowed_processing_types()
            exclude_list = tmpSiteSpec.get_excluded_processing_types()
            if allowed_list is not None:
                if taskSpec.processingType not in allowed_list:
                    tmpLog.debug(f"  skip {tmpSiteName} due to processing type not in allowed list")
                    continue
            if exclude_list is not None:
                if taskSpec.processingType in exclude_list:
                    tmpLog.debug(f"  skip {tmpSiteName} due to processing type in excluded list")
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug(f"{len(scanSiteList)} candidates passed allowed/excluded processing type check")
        if not scanSiteList:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for nPilot
        nWNmap = self.taskBufferIF.getCurrentSiteData()
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmp_site_spec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            nPilot = 0
            if tmpSiteName in nWNmap:
                nPilot = nWNmap[tmpSiteName]["getJob"] + nWNmap[tmpSiteName]["updateJob"]
            if nPilot == 0 and taskSpec.prodSourceLabel not in ["test"] and not tmp_site_spec.hasValueInCatchall("allow_no_pilot"):
                tmpLog.debug(f"  skip {tmpSiteName} due to no pilot")
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug(f"{len(scanSiteList)} candidates passed pilot activity check")
        if scanSiteList == []:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # sites already used by task
        tmpSt, sitesUsedByTask = self.taskBufferIF.getSitesUsedByTask_JEDI(taskSpec.jediTaskID)
        if not tmpSt:
            tmpLog.error("failed to get sites which already used by task")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # get list of available files
        availableFileMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # get list of site to be scanned
                tmpLog.debug(f"getting the list of available files for {datasetSpec.datasetName}")
                fileScanSiteList = []
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    if tmpSiteName in fileScanSiteList:
                        continue
                    fileScanSiteList.append(tmpSiteName)
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(fileScanSiteList, self.siteMapper, taskSpec.prodSourceLabel, None)
                # disable file lookup for merge jobs
                if inputChunk.isMerging:
                    checkCompleteness = False
                else:
                    checkCompleteness = True
                if not datasetSpec.isMaster():
                    useCompleteOnly = True
                else:
                    useCompleteOnly = False
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(
                    datasetSpec,
                    siteStorageEP,
                    self.siteMapper,
                    check_completeness=checkCompleteness,
                    file_scan_in_container=False,
                    complete_only=useCompleteOnly,
                    use_deep=True,
                )
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError("ddmIF.getAvailableFiles failed")
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception as e:
                tmpLog.error(f"failed to get available files with {e}")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # calculate weight
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        if not tmpSt:
            tmpLog.error("failed to get job statistics with priority")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # final procedure
        tmpLog.debug(f"final {len(scanSiteList)} candidates")
        weightMap = {}
        candidateSpecList = []
        preSiteCandidateSpec = None
        for tmpSiteName in scanSiteList:
            # get number of jobs in each job status. Using workQueueID=None to include non-JEDI jobs
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running", None, None)
            nAssigned = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "defined", None, None)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "activated", None, None)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + 1) / float(nAssigned + 1)
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # set weight
            siteCandidateSpec.weight = weight
            # files
            for tmpDatasetName, availableFiles in availableFileMap.items():
                if tmpSiteName in availableFiles:
                    siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]["localdisk"])
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if weight not in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)
        # limit the number of sites
        maxNumSites = 5
        weightList = sorted(weightMap.keys())
        weightList.reverse()
        for weightVal in weightList:
            if len(candidateSpecList) >= maxNumSites:
                break
            sitesWithWeight = weightMap[weightVal]
            random.shuffle(sitesWithWeight)
            candidateSpecList += sitesWithWeight[: (maxNumSites - len(candidateSpecList))]
        # collect site names
        scanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            scanSiteList.append(siteCandidateSpec.siteName)
        # append candidates
        newScanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            # append
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpLog.debug(f"  use {siteCandidateSpec.siteName} with weight={siteCandidateSpec.weight} nFiles={len(siteCandidateSpec.localDiskFiles)}")
        scanSiteList = newScanSiteList
        if scanSiteList == []:
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # return
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, inputChunk
