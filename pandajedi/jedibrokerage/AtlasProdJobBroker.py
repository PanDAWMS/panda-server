import copy
import datetime
import re

from dataservice.DataServiceUtils import select_scope
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandaserver.dataservice import DataServiceUtils
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils

from . import AtlasBrokerUtils
from .JobBrokerBase import JobBrokerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# definitions for network
AGIS_CLOSENESS = "AGIS_closeness"
BLOCKED_LINK = -1
MIN_CLOSENESS = 0  # closeness goes from 0(best) to 11(worst)
MAX_CLOSENESS = 11
# NWS tags need to be prepended with activity
TRANSFERRED_1H = "_done_1h"
TRANSFERRED_6H = "_done_6h"
QUEUED = "_queued"
ZERO_TRANSFERS = 0.00001
URG_ACTIVITY = "Express"
PRD_ACTIVITY = "Production Output"
# NWS tags for FTS throughput
FTS_1H = "dashb_mbps_1h"
FTS_1D = "dashb_mbps_1d"
FTS_1W = "dashb_mbps_1w"
APP = "jedi"
COMPONENT = "jobbroker"
VO = "atlas"

WORLD_NUCLEUS_WEIGHT = 4


# brokerage for ATLAS production
class AtlasProdJobBroker(JobBrokerBase):
    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        JobBrokerBase.__init__(self, ddmIF, taskBufferIF)
        self.dataSiteMap = {}
        self.suppressLogSending = False

        self.nwActive = taskBufferIF.getConfigValue(COMPONENT, "NW_ACTIVE", APP, VO)
        if self.nwActive is None:
            self.nwActive = False

        self.nwQueueImportance = taskBufferIF.getConfigValue(COMPONENT, "NW_QUEUE_IMPORTANCE", APP, VO)
        if self.nwQueueImportance is None:
            self.nwQueueImportance = 0.5
        self.nwThroughputImportance = 1 - self.nwQueueImportance

        self.nw_threshold = taskBufferIF.getConfigValue(COMPONENT, "NW_THRESHOLD", APP, VO)
        if self.nw_threshold is None:
            self.nw_threshold = 1.7

        self.queue_threshold = taskBufferIF.getConfigValue(COMPONENT, "NQUEUED_SAT_CAP", APP, VO)
        if self.queue_threshold is None:
            self.queue_threshold = 150

        self.total_queue_threshold = taskBufferIF.getConfigValue(COMPONENT, "NQUEUED_NUC_CAP_FOR_JOBS", APP, VO)
        if self.total_queue_threshold is None:
            self.total_queue_threshold = 10000

        self.nw_weight_multiplier = taskBufferIF.getConfigValue(COMPONENT, "NW_WEIGHT_MULTIPLIER", APP, VO)
        if self.nw_weight_multiplier is None:
            self.nw_weight_multiplier = 1

        self.io_intensity_cutoff = taskBufferIF.getConfigValue(COMPONENT, "IO_INTENSITY_CUTOFF", APP, VO)
        if self.io_intensity_cutoff is None:
            self.io_intensity_cutoff = 200

        self.max_prio_for_bootstrap = taskBufferIF.getConfigValue(COMPONENT, "MAX_PRIO_TO_BOOTSTRAP", APP, VO)
        if self.max_prio_for_bootstrap is None:
            self.max_prio_for_bootstrap = 150

        # load the SW availability map
        try:
            self.sw_map = taskBufferIF.load_sw_map()
        except BaseException:
            logger.error("Failed to load the SW tags map!!!")
            self.sw_map = {}

    def convertMBpsToWeight(self, mbps):
        """
        Takes MBps value and converts to a weight between 1 and 2
        """
        mbps_thresholds = [200, 100, 75, 50, 20, 10, 2, 1]
        weights = [2, 1.9, 1.8, 1.6, 1.5, 1.3, 1.2, 1.1]

        for threshold, weight in zip(mbps_thresholds, weights):
            if mbps > threshold:
                return weight
        return 1

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap, hintForTB=False, siteListForTB=None, glLog=None):
        # suppress sending log
        if hintForTB:
            self.suppressLogSending = True
        # make logger
        if glLog is None:
            tmpLog = MsgWrapper(
                logger,
                f"<jediTaskID={taskSpec.jediTaskID}>",
                monToken=f"<jediTaskID={taskSpec.jediTaskID} {naive_utcnow().isoformat('/')}>",
            )
        else:
            tmpLog = glLog

        if not hintForTB:
            tmpLog.debug(f"start currentPriority={taskSpec.currentPriority}")

        if self.nwActive:
            tmpLog.debug("Network weights are ACTIVE!")
        else:
            tmpLog.debug("Network weights are PASSIVE!")

        timeNow = naive_utcnow()

        # return for failure
        retFatal = self.SC_FATAL, inputChunk
        retTmpError = self.SC_FAILED, inputChunk

        # new maxwdir
        newMaxwdir = {}

        # short of work
        work_shortage = self.taskBufferIF.getConfigValue("core", "WORK_SHORTAGE", APP, VO)
        if work_shortage is True:
            tmp_status, core_statistics = self.taskBufferIF.get_core_statistics(taskSpec.vo, taskSpec.prodSourceLabel)
            if not tmp_status:
                tmpLog.error("failed to get core statistics")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            tmpLog.debug(f"Work shortage is {work_shortage}")
        else:
            core_statistics = {}

        # thresholds for incomplete datasets
        max_missing_input_files = self.taskBufferIF.getConfigValue("jobbroker", "MAX_MISSING_INPUT_FILES", "jedi", taskSpec.vo)
        if max_missing_input_files is None:
            max_missing_input_files = 10
        min_input_completeness = self.taskBufferIF.getConfigValue("jobbroker", "MIN_INPUT_COMPLETENESS", "jedi", taskSpec.vo)
        if min_input_completeness is None:
            min_input_completeness = 90

        # minimum brokerage weight
        min_weight_param = f"MIN_WEIGHT_{taskSpec.prodSourceLabel}_{taskSpec.gshare}"
        min_weight = self.taskBufferIF.getConfigValue("jobbroker", min_weight_param, "jedi", taskSpec.vo)
        if min_weight is None:
            min_weight_param = f"MIN_WEIGHT_{taskSpec.prodSourceLabel}"
            min_weight = self.taskBufferIF.getConfigValue("jobbroker", min_weight_param, "jedi", taskSpec.vo)
        if min_weight is None:
            min_weight = 0

        # get sites in the cloud
        siteSkippedTmp = dict()
        sitePreAssigned = False
        siteListPreAssigned = False
        if siteListForTB is not None:
            scanSiteList = siteListForTB

        elif taskSpec.site not in ["", None] and inputChunk.getPreassignedSite() is None:
            # convert to pseudo queues if needed
            scanSiteList = []
            for tmpSiteName in taskSpec.site.split(","):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec is None or not tmpSiteSpec.is_unified:
                    scanSiteList.append(tmpSiteName)
                else:
                    scanSiteList += self.get_pseudo_sites([tmpSiteName], self.siteMapper.getCloud(cloudName)["sites"])
            for tmpSiteName in scanSiteList:
                tmpLog.info(f"site={tmpSiteName} is pre-assigned criteria=+preassign")
            if len(scanSiteList) > 1:
                siteListPreAssigned = True
            else:
                sitePreAssigned = True

        elif inputChunk.getPreassignedSite() is not None:
            if (
                inputChunk.masterDataset.creationTime is not None
                and inputChunk.masterDataset.modificationTime is not None
                and inputChunk.masterDataset.modificationTime != inputChunk.masterDataset.creationTime
                and timeNow - inputChunk.masterDataset.modificationTime > datetime.timedelta(hours=24)
                and taskSpec.frozenTime is not None
                and timeNow - taskSpec.frozenTime > datetime.timedelta(hours=6)
            ):
                # ignore pre-assigned site since pmerge is timed out
                tmpLog.info("ignore pre-assigned for pmerge due to timeout")
                scanSiteList = self.siteMapper.getCloud(cloudName)["sites"]
                tmpLog.info(f"cloud={cloudName} has {len(scanSiteList)} candidates")
            else:
                # pmerge
                siteListPreAssigned = True
                scanSiteList = DataServiceUtils.getSitesShareDDM(self.siteMapper, inputChunk.getPreassignedSite(), JobUtils.PROD_PS, JobUtils.PROD_PS)
                scanSiteList.append(inputChunk.getPreassignedSite())
                tmp_msg = "use site={0} since they share DDM endpoints with original_site={1} which is pre-assigned in masterDS ".format(
                    str(scanSiteList), inputChunk.getPreassignedSite()
                )
                tmp_msg += "criteria=+premerge"
                tmpLog.info(tmp_msg)

        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)["sites"]
            tmpLog.info(f"cloud={cloudName} has {len(scanSiteList)} candidates")

        # high prio ES
        if taskSpec.useEventService() and not taskSpec.useJobCloning() and (taskSpec.currentPriority >= 900 or inputChunk.useScout()):
            esHigh = True
        else:
            esHigh = False

        # get job statistics
        tmpSt, jobStatMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        if not tmpSt:
            tmpLog.error("failed to get job statistics")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        tmp_st, transferring_job_map = self.taskBufferIF.get_num_jobs_with_status_by_nucleus(taskSpec.vo, "transferring")
        if not tmp_st:
            tmpLog.error("failed to get transferring job statistics")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        # get destination for WORLD cloud
        nucleusSpec = None
        nucleus_with_storages_unwritable_over_wan = {}
        if not hintForTB:
            # get nucleus
            nucleusSpec = self.siteMapper.getNucleus(taskSpec.nucleus)
            if nucleusSpec is None:
                tmpLog.error(f"unknown nucleus {taskSpec.nucleus}")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            sites_in_nucleus = nucleusSpec.allPandaSites
            # get black list
            for tmp_name, tmp_nucleus_dict in self.siteMapper.nuclei.items():
                receive_output_over_wan = tmp_nucleus_dict.get_default_endpoint_out()["detailed_status"].get("write_wan")
                if receive_output_over_wan in ["OFF", "TEST"]:
                    nucleus_with_storages_unwritable_over_wan[tmp_name] = receive_output_over_wan

        else:
            # use all sites in nuclei for WORLD task brokerage
            sites_in_nucleus = []
            for tmpNucleus in self.siteMapper.nuclei.values():
                sites_in_nucleus += tmpNucleus.allPandaSites

        # sites sharing SE with T1
        if len(sites_in_nucleus) > 0:
            sites_sharing_output_storages_in_nucleus = DataServiceUtils.getSitesShareDDM(
                self.siteMapper, sites_in_nucleus[0], JobUtils.PROD_PS, JobUtils.PROD_PS, True
            )
        else:
            sites_sharing_output_storages_in_nucleus = []

        # core count
        if inputChunk.isMerging and taskSpec.mergeCoreCount is not None:
            taskCoreCount = taskSpec.mergeCoreCount
        else:
            taskCoreCount = taskSpec.coreCount

        # MP
        if taskCoreCount is not None and taskCoreCount > 1:
            # use MCORE only
            useMP = "only"
        elif taskCoreCount == 0 and (taskSpec.currentPriority >= 900 or inputChunk.useScout()):
            # use MCORE only for ES scouts and close to completion
            useMP = "only"
        elif taskCoreCount == 0:
            # use MCORE and normal
            useMP = "any"
        else:
            # not use MCORE
            useMP = "unuse"

        # get workQueue
        workQueue = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
        if workQueue.is_global_share:
            wq_tag = workQueue.queue_name
            wq_tag_global_share = wq_tag
        else:
            wq_tag = workQueue.queue_id
            workQueueGS = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(None, taskSpec.gshare)
            wq_tag_global_share = workQueueGS.queue_name

        # init summary list
        self.init_summary_list("Job brokerage summary", None, scanSiteList)

        ######################################
        # check dataset completeness
        remote_source_available = True
        if inputChunk.getDatasets() and not taskSpec.inputPreStaging():
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                # skip distributed datasets
                is_distributed = self.ddmIF.isDistributedDataset(datasetName)
                if is_distributed:
                    tmpLog.debug(f"completeness check disabled for {datasetName} since it is distributed")
                    continue
                # check if complete replicas are available at online endpoints
                (
                    tmpSt,
                    tmpRet,
                    tmp_complete_disk_ok,
                    tmp_complete_tape_ok,
                    tmp_truly_complete_disk,
                    tmp_can_be_local_source,
                    tmp_can_be_remote_source,
                    tmp_list_of_complete_replica_locations,
                ) = AtlasBrokerUtils.get_sites_with_data(
                    [],
                    self.siteMapper,
                    self.ddmIF,
                    datasetName,
                    [],
                    max_missing_input_files,
                    min_input_completeness,
                )
                if tmpSt != Interaction.SC_SUCCEEDED:
                    tmpLog.error(f"failed to get available storage endpoints with {datasetName}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                # pending if the dataset is incomplete or missing at online endpoints
                if not tmp_complete_disk_ok and not tmp_complete_tape_ok:
                    err_msg = f"{datasetName} is "
                    if tmp_list_of_complete_replica_locations:
                        tmp_rse_list = ",".join(tmp_list_of_complete_replica_locations)
                        tmp_is_single = len(tmp_list_of_complete_replica_locations) == 1
                        err_msg += f"only complete at {tmp_rse_list} which "
                        err_msg += "is " if tmp_is_single else "are "
                        err_msg += "currently in downtime or offline"
                    else:
                        err_msg += "incomplete at any online storage"
                    tmpLog.error(err_msg)
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                tmp_msg = f"complete replicas of {datasetName} are available at online storages"
                if not tmp_can_be_remote_source:
                    if tmp_can_be_local_source:
                        tmp_msg += ", but files cannot be sent out to satellites since read_wan is not ON"
                        remote_source_available = False
                    else:
                        tmp_msg = f"complete replicas of {datasetName} are available at endpoints where read_wan/lan are not ON"
                        tmpLog.error(tmp_msg)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retTmpError
                tmpLog.debug(tmp_msg)

        ######################################
        # selection for status
        if not sitePreAssigned and not siteListPreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)

                # skip unified queues
                if tmpSiteSpec.is_unified:
                    continue
                # check site status
                msgFlag = False
                skipFlag = False

                if tmpSiteSpec.status in ["online", "standby"]:
                    newScanSiteList.append(tmpSiteName)
                else:
                    msgFlag = True
                    skipFlag = True

                if msgFlag:
                    tmpStr = f"  skip site={tmpSiteName} due to status={tmpSiteSpec.status} criteria=-status"
                    if skipFlag:
                        tmpLog.info(tmpStr)
                    else:
                        siteSkippedTmp[tmpSiteName] = tmpStr

            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed site status check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "status check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        #################################################
        # get the nucleus and the network map
        nucleus = taskSpec.nucleus
        storageMapping = self.taskBufferIF.getPandaSiteToOutputStorageSiteMapping()

        if nucleus:
            # get connectivity stats to the nucleus
            if inputChunk.isExpress():
                transferred_tag = f"{URG_ACTIVITY}{TRANSFERRED_6H}"
                queued_tag = f"{URG_ACTIVITY}{QUEUED}"
            else:
                transferred_tag = f"{PRD_ACTIVITY}{TRANSFERRED_6H}"
                queued_tag = f"{PRD_ACTIVITY}{QUEUED}"

            networkMap = self.taskBufferIF.getNetworkMetrics(nucleus, [AGIS_CLOSENESS, transferred_tag, queued_tag, FTS_1H, FTS_1D, FTS_1W])

        #####################################################
        # filtering out blacklisted or links with long queues
        if nucleus:
            # get the number of files being transferred to the nucleus
            if queued_tag in networkMap["total"]:
                totalQueued = networkMap["total"][queued_tag]
            else:
                totalQueued = 0

            tmpLog.debug(f"Total number of files being transferred to the nucleus : {totalQueued}")
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()
            for tmpPandaSiteName in self.get_unified_sites(scanSiteList):
                try:
                    tmpAtlasSiteName = storageMapping[tmpPandaSiteName]["default"]
                    skipFlag = False
                    tempFlag = False
                    criteria = "-link_unusable"
                    from_str = f"from satellite={tmpAtlasSiteName}"
                    reason = ""
                    if nucleus == tmpAtlasSiteName:
                        # nucleus
                        pass
                    elif nucleus in nucleus_with_storages_unwritable_over_wan:
                        # destination blacklisted
                        reason = (
                            nucleusSpec.get_default_endpoint_out()["ddm_endpoint_name"]
                            + f" at nucleus={nucleus} unwritable over WAN write_wan={nucleus_with_storages_unwritable_over_wan[nucleus]}"
                        )
                        criteria = "-dest_blacklisted"
                        from_str = ""
                        tempFlag = True
                    elif totalQueued >= self.total_queue_threshold:
                        # total exceed
                        reason = f"too many output files being transferred to the nucleus {totalQueued}(>{self.total_queue_threshold} total limit)"
                        criteria = "-links_full"
                        from_str = ""
                        tempFlag = True
                    elif tmpAtlasSiteName not in networkMap:
                        # Don't skip missing links for the moment. In later stages missing links
                        # default to the worst connectivity and will be penalized.
                        pass
                    elif AGIS_CLOSENESS in networkMap[tmpAtlasSiteName] and networkMap[tmpAtlasSiteName][AGIS_CLOSENESS] == BLOCKED_LINK:
                        # blocked link
                        reason = f"agis_closeness={networkMap[tmpAtlasSiteName][AGIS_CLOSENESS]}"
                        skipFlag = True
                    elif queued_tag in networkMap[tmpAtlasSiteName] and networkMap[tmpAtlasSiteName][queued_tag] >= self.queue_threshold:
                        # too many queued
                        reason = f"too many output files queued in the channel {networkMap[tmpAtlasSiteName][queued_tag]}(>{self.queue_threshold} link limit)"
                        tempFlag = True

                    # add
                    tmpStr = f"  skip site={tmpPandaSiteName} due to {reason}"
                    if from_str:
                        tmpStr += f", {from_str} to nucleus={nucleus}"
                    tmpStr += f": criteria={criteria}"
                    if skipFlag:
                        tmpLog.info(tmpStr)
                    else:
                        newScanSiteList.append(tmpPandaSiteName)
                        if tempFlag:
                            newSkippedTmp[tmpPandaSiteName] = tmpStr
                except KeyError:
                    # Don't skip missing links for the moment. In later stages missing links
                    # default to the worst connectivity and will be penalized.
                    newScanSiteList.append(tmpPandaSiteName)
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed link check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "link check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for high priorities
        t1WeightForHighPrio = 1
        if (taskSpec.currentPriority >= 800 or inputChunk.useScout()) and not sitePreAssigned and not siteListPreAssigned:
            if not taskSpec.useEventService():
                if taskSpec.currentPriority >= 900 or inputChunk.useScout():
                    t1WeightForHighPrio = 100
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)

            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if not tmpSiteSpec.is_opportunistic():
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmp_msg = f"  skip site={tmpSiteName} to avoid opportunistic for high priority jobs "
                    tmp_msg += "criteria=-opportunistic"
                    tmpLog.info(tmp_msg)

            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed for opportunistic check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "opportunistic check")

            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection to avoid slow or inactive sites
        if (
            (taskSpec.currentPriority >= 800 or inputChunk.useScout() or inputChunk.isMerging or taskSpec.mergeOutput())
            and not sitePreAssigned
            and not siteListPreAssigned
        ):
            # get inactive sites
            inactiveTimeLimit = 2
            inactiveSites = self.taskBufferIF.getInactiveSites_JEDI("production", inactiveTimeLimit)
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()

            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                nToGetAll = AtlasBrokerUtils.getNumJobs(jobStatMap, tmpSiteSpec.get_unified_name(), "activated") + AtlasBrokerUtils.getNumJobs(
                    jobStatMap, tmpSiteSpec.get_unified_name(), "starting"
                )

                if tmpSiteName in inactiveSites and nToGetAll > 0:
                    tmp_msg = "  skip site={0} since high prio/scouts/merge needs to avoid inactive sites (laststart is older than {1}h) ".format(
                        tmpSiteName, inactiveTimeLimit
                    )
                    tmp_msg += "criteria=-inactive"
                    # temporary problem
                    newSkippedTmp[tmpSiteName] = tmp_msg
                newScanSiteList.append(tmpSiteName)
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed for slowness/inactive check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "slowness/inactive check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for fairshare
        if sitePreAssigned and taskSpec.prodSourceLabel not in [JobUtils.PROD_PS] or workQueue.queue_name not in ["test", "validation"]:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if AtlasBrokerUtils.hasZeroShare(tmpSiteSpec, taskSpec, inputChunk.isMerging, tmpLog):
                    tmpLog.info(f"  skip site={tmpSiteName} due to zero share criteria=-zeroshare")
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed zero share check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "zero share check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for jumbo jobs
        if not sitePreAssigned and taskSpec.getNumJumboJobs() is None:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.useJumboJobs():
                    tmpLog.info(f"  skip site={tmpSiteName} since it is only for jumbo jobs criteria=-jumbo")
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed jumbo job check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "jumbo job check")
            if scanSiteList == []:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        elif not sitePreAssigned and taskSpec.getNumJumboJobs() is not None:
            nReadyEvents = self.taskBufferIF.getNumReadyEvents(taskSpec.jediTaskID)
            if nReadyEvents is not None:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if tmpSiteSpec.useJumboJobs():
                        minEvents = tmpSiteSpec.getMinEventsForJumbo()
                        if minEvents is not None and nReadyEvents < minEvents:
                            tmpLog.info(f"  skip site={tmpSiteName} since not enough events ({minEvents}<{nReadyEvents}) for jumbo criteria=-fewevents")
                            continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info(f"{len(scanSiteList)} candidates passed jumbo events check nReadyEvents={nReadyEvents}")
                self.add_summary_message(oldScanSiteList, scanSiteList, "jumbo events check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # selection for iointensity limits

        # get default disk IO limit from GDP config
        max_diskio_per_core_default = self.taskBufferIF.getConfigValue(COMPONENT, "MAX_DISKIO_DEFAULT", APP, VO)
        if not max_diskio_per_core_default:
            max_diskio_per_core_default = 10**10

        # get the current disk IO usage per site
        diskio_percore_usage = self.taskBufferIF.getAvgDiskIO_JEDI()
        unified_site_list = self.get_unified_sites(scanSiteList)
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in unified_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)

            # measured diskIO at queue
            diskio_usage_tmp = diskio_percore_usage.get(tmpSiteName, 0)

            # figure out queue or default limit
            if tmpSiteSpec.maxDiskio and tmpSiteSpec.maxDiskio > 0:
                # there is a limit specified in AGIS
                diskio_limit_tmp = tmpSiteSpec.maxDiskio
            else:
                # we need to use the default value from GDP Config
                diskio_limit_tmp = max_diskio_per_core_default

            # normalize task diskIO by site corecount
            diskio_task_tmp = taskSpec.diskIO
            if taskSpec.diskIO is not None and taskSpec.coreCount not in [None, 0, 1] and tmpSiteSpec.coreCount not in [None, 0]:
                diskio_task_tmp = taskSpec.diskIO // tmpSiteSpec.coreCount

            try:  # generate a log message parseable by logstash for monitoring
                log_msg = f"diskIO measurements: site={tmpSiteName} jediTaskID={taskSpec.jediTaskID} "
                if diskio_task_tmp is not None:
                    log_msg += f"diskIO_task={diskio_task_tmp:.2f} "
                if diskio_usage_tmp is not None:
                    log_msg += f"diskIO_site_usage={diskio_usage_tmp:.2f} "
                if diskio_limit_tmp is not None:
                    log_msg += f"diskIO_site_limit={diskio_limit_tmp:.2f} "
                tmpLog.info(log_msg)
            except Exception:
                tmpLog.debug("diskIO measurements: Error generating diskIO message")

            # if the task has a diskIO defined, the queue is over the IO limit and the task IO is over the limit
            if diskio_task_tmp and diskio_usage_tmp and diskio_limit_tmp and diskio_usage_tmp > diskio_limit_tmp and diskio_task_tmp > diskio_limit_tmp:
                tmp_msg = f"  skip site={tmpSiteName} due to diskIO overload criteria=-diskIO"
                newSkippedTmp[tmpSiteName] = tmp_msg

            newScanSiteList.append(tmpSiteName)

        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)

        tmpLog.info(f"{len(scanSiteList)} candidates passed diskIO check")
        self.add_summary_message(oldScanSiteList, scanSiteList, "diskIO check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for MP
        if not sitePreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            max_core_count = taskSpec.get_max_core_count()
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if useMP == "any" or (useMP == "only" and tmpSiteSpec.coreCount > 1) or (useMP == "unuse" and tmpSiteSpec.coreCount in [0, 1, None]):
                    if max_core_count and tmpSiteSpec.coreCount and tmpSiteSpec.coreCount > max_core_count:
                        tmpLog.info(
                            f"  skip site={tmpSiteName} due to larger core count site:{tmpSiteSpec.coreCount} than task_max={max_core_count} criteria=-max_cpucore"
                        )
                    else:
                        newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info(f"  skip site={tmpSiteName} due to core mismatch site:{tmpSiteSpec.coreCount} <> task:{taskCoreCount} criteria=-cpucore")
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed for core count check with policy={useMP}")
            self.add_summary_message(oldScanSiteList, scanSiteList, "core count check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for release
        cmt_config = taskSpec.get_sw_platform()
        is_regexp_cmt_config = False
        if cmt_config:
            if re.match(cmt_config, cmt_config) is None:
                is_regexp_cmt_config = True
        base_platform = taskSpec.get_base_platform()
        resolved_platforms = {}
        if taskSpec.transHome is not None:
            jsonCheck = AtlasBrokerUtils.JsonSoftwareCheck(self.siteMapper, self.sw_map)
            unified_site_list = self.get_unified_sites(scanSiteList)

            host_cpu_spec = taskSpec.get_host_cpu_spec()
            host_gpu_spec = taskSpec.get_host_gpu_spec()

            if taskSpec.get_base_platform() is None:
                use_container = False
            else:
                use_container = True

            need_cvmfs = False

            # 3 digits base release or normal tasks
            if (re.search("-\d+\.\d+\.\d+$", taskSpec.transHome) is not None) or (
                re.search("rel_\d+(\n|$)", taskSpec.transHome) is None and re.search("\d{4}-\d{2}-\d{2}T\d{4}$", taskSpec.transHome) is None
            ):
                cvmfs_repo = "atlas"
                sw_project = taskSpec.transHome.split("-")[0]
                sw_version = taskSpec.transHome.split("-")[1]
                cmt_config_only = False

            # nightlies
            else:
                cvmfs_repo = "nightlies"
                sw_project = None
                sw_version = None
                cmt_config_only = False

            siteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                unified_site_list,
                cvmfs_repo,
                sw_project,
                sw_version,
                cmt_config,
                need_cvmfs,
                cmt_config_only,
                need_container=use_container,
                container_name=taskSpec.container_name,
                only_tags_fc=taskSpec.use_only_tags_fc(),
                host_cpu_specs=host_cpu_spec,
                host_gpu_spec=host_gpu_spec,
                log_stream=tmpLog,
            )
            sitesAuto = copy.copy(siteListWithSW)
            sitesAny = []

            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)

            for tmpSiteName in unified_site_list:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if cmt_config:
                    platforms = AtlasBrokerUtils.resolve_cmt_config(tmpSiteName, cmt_config, base_platform, self.sw_map)
                    if platforms:
                        resolved_platforms[tmpSiteName] = platforms
                if tmpSiteName in siteListWithSW:
                    # passed
                    if not is_regexp_cmt_config or tmpSiteName in resolved_platforms:
                        newScanSiteList.append(tmpSiteName)
                    else:
                        # cmtconfig is not resolved
                        tmpLog.info(f"  skip site={tmpSiteName} due to unresolved regexp in cmtconfig={cmt_config} criteria=-regexpcmtconfig")
                elif (
                    not (taskSpec.container_name and taskSpec.use_only_tags_fc())
                    and host_cpu_spec is None
                    and host_gpu_spec is None
                    and tmpSiteSpec.releases == ["ANY"]
                ):
                    # release check is disabled or release is available
                    newScanSiteList.append(tmpSiteName)
                    sitesAny.append(tmpSiteName)
                else:
                    if tmpSiteSpec.releases == ["AUTO"]:
                        autoStr = "with AUTO"
                    else:
                        autoStr = "without AUTO"
                    # release is unavailable
                    tmpLog.info(
                        f"  skip site={tmpSiteName} {autoStr} due to missing SW cache={taskSpec.transHome}:{taskSpec.get_sw_platform()} sw_platform='{taskSpec.container_name}' "
                        f"or irrelevant HW cpu={str(host_cpu_spec)} gpu={str(host_gpu_spec)} criteria=-cache"
                    )

            sitesAuto = self.get_pseudo_sites(sitesAuto, scanSiteList)
            sitesAny = self.get_pseudo_sites(sitesAny, scanSiteList)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates ({len(sitesAuto)} with AUTO, {len(sitesAny)} with ANY) passed SW/HW check ")
            self.add_summary_message(oldScanSiteList, scanSiteList, "SW/HW check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for memory
        origMinRamCount = inputChunk.getMaxRamCount()
        if origMinRamCount not in [0, None]:
            if inputChunk.isMerging:
                strMinRamCount = f"{origMinRamCount}(MB)"
            else:
                str_ram_unit = taskSpec.ramUnit
                if str_ram_unit:
                    str_ram_unit = str_ram_unit.replace("PerCore", " ").strip()
                strMinRamCount = f"{origMinRamCount}({str_ram_unit})"
            if not inputChunk.isMerging and taskSpec.baseRamCount not in [0, None]:
                strMinRamCount += f"+{taskSpec.baseRamCount}"
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement
                minRamCount = origMinRamCount
                if taskSpec.ramPerCore() and not inputChunk.isMerging:
                    if tmpSiteSpec.coreCount not in [None, 0]:
                        minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    minRamCount += taskSpec.baseRamCount
                # compensate
                minRamCount = JobUtils.compensate_ram_count(minRamCount)
                # site max memory requirement
                site_maxmemory = 0
                if tmpSiteSpec.maxrss not in [0, None]:
                    site_maxmemory = tmpSiteSpec.maxrss
                # check at the site
                if site_maxmemory not in [0, None] and minRamCount != 0 and minRamCount > site_maxmemory:
                    tmp_msg = f"  skip site={tmpSiteName} due to site RAM shortage {site_maxmemory}(site upper limit) less than {minRamCount} "
                    tmp_msg += "criteria=-lowmemory"
                    tmpLog.info(tmp_msg)
                    continue
                # site min memory requirement
                site_minmemory = 0
                if tmpSiteSpec.minrss not in [0, None]:
                    site_minmemory = tmpSiteSpec.minrss
                if site_minmemory not in [0, None] and minRamCount != 0 and minRamCount < site_minmemory:
                    tmp_msg = f"  skip site={tmpSiteName} due to job RAM shortage {site_minmemory}(site lower limit) greater than {minRamCount} "
                    tmp_msg += "criteria=-highmemory"
                    tmpLog.info(tmp_msg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed memory check {strMinRamCount}")
            self.add_summary_message(oldScanSiteList, scanSiteList, "memory check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for scratch disk
        if taskSpec.outputScaleWithEvents():
            minDiskCount = max(taskSpec.getOutDiskSize() * inputChunk.getMaxAtomSize(getNumEvents=True), inputChunk.defaultOutputSize)
        else:
            minDiskCount = max(taskSpec.getOutDiskSize() * inputChunk.getMaxAtomSize(effectiveSize=True), inputChunk.defaultOutputSize)
        minDiskCount += taskSpec.getWorkDiskSize()
        minDiskCountL = minDiskCount
        minDiskCountD = minDiskCount
        minDiskCountL += inputChunk.getMaxAtomSize()
        minDiskCountL = minDiskCountL // 1024 // 1024
        minDiskCountD = minDiskCountD // 1024 // 1024
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check direct access
            if taskSpec.allowInputLAN() == "only" and not tmpSiteSpec.isDirectIO() and not tmpSiteSpec.always_use_direct_io() and not inputChunk.isMerging:
                tmp_msg = f"  skip site={tmpSiteName} since direct IO is disabled "
                tmp_msg += "criteria=-remoteio"
                tmpLog.info(tmp_msg)
                continue
            # check scratch size
            if tmpSiteSpec.maxwdir != 0:
                if CoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
                    # size for remote access
                    minDiskCount = minDiskCountD
                else:
                    # size for copy-to-scratch
                    minDiskCount = minDiskCountL

                # get site and task corecount to scale maxwdir
                if tmpSiteSpec.coreCount in [None, 0, 1]:
                    site_cc = 1
                else:
                    site_cc = tmpSiteSpec.coreCount

                if taskSpec.coreCount in [None, 0, 1] and not taskSpec.useEventService():
                    task_cc = 1
                else:
                    task_cc = site_cc

                maxwdir_scaled = tmpSiteSpec.maxwdir * task_cc / site_cc

                if minDiskCount > maxwdir_scaled:
                    tmp_msg = f"  skip site={tmpSiteName} due to small scratch disk {maxwdir_scaled} MB less than {minDiskCount} MB"
                    tmp_msg += " criteria=-disk"
                    tmpLog.info(tmp_msg)
                    continue
                newMaxwdir[tmpSiteName] = maxwdir_scaled
            newScanSiteList.append(tmpSiteName)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info(f"{len(scanSiteList)} candidates passed scratch disk check minDiskCount>{minDiskCount} MB")
        self.add_summary_message(oldScanSiteList, scanSiteList, "disk check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for available space in SE
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.PROD_PS, JobUtils.PROD_PS)
            # check endpoint
            tmp_default_output_endpoint = tmpSiteSpec.ddm_endpoints_output[scope_output].getEndPoint(tmpSiteSpec.ddm_output[scope_output])
            tmp_msg = None
            # check free size on output endpoint
            if tmp_default_output_endpoint is not None:
                tmpSpaceSize = 0
                if tmp_default_output_endpoint["space_free"] is not None:
                    tmpSpaceSize += tmp_default_output_endpoint["space_free"]
                if tmp_default_output_endpoint["space_expired"] is not None:
                    tmpSpaceSize += tmp_default_output_endpoint["space_expired"]
                diskThreshold = 200

                if (
                    tmpSpaceSize < diskThreshold and "skip_RSE_check" not in tmpSiteSpec.catchall
                ):  # skip_RSE_check: exceptional bypass of RSEs without storage reporting
                    tmp_msg = "  skip site={0} due to disk shortage at {1} {2}GB < {3}GB criteria=-disk".format(
                        tmpSiteName, tmpSiteSpec.ddm_output[scope_output], tmpSpaceSize, diskThreshold
                    )
            # check if blacklisted
            if not tmp_msg:
                tmp_msg = AtlasBrokerUtils.check_endpoints_with_blacklist(tmpSiteSpec, scope_input, scope_output, sites_in_nucleus, remote_source_available)
            if tmp_msg is not None:
                newSkippedTmp[tmpSiteName] = tmp_msg
            newScanSiteList.append(tmpSiteName)
        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info(f"{len(scanSiteList)} candidates passed Storage check")
        self.add_summary_message(oldScanSiteList, scanSiteList, "Storage check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for walltime
        if taskSpec.useEventService() and not taskSpec.useJobCloning():
            nEsConsumers = taskSpec.getNumEventServiceConsumer()
            if nEsConsumers is None:
                nEsConsumers = 1
            maxAttemptEsJob = taskSpec.getMaxAttemptEsJob()
            if maxAttemptEsJob is None:
                maxAttemptEsJob = EventServiceUtils.defMaxAttemptEsJob
            else:
                maxAttemptEsJob += 1
        else:
            nEsConsumers = 1
            maxAttemptEsJob = 1
        maxWalltime = None
        maxWalltime_dyn = None
        minWalltime_dyn = None
        minWalltime = None
        strMaxWalltime = None
        strMinWalltime = None
        strMaxWalltime_dyn = None
        strMinWalltime_dyn = None
        if not taskSpec.useHS06():
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
            if taskSpec.walltime is not None:
                minWalltime = taskSpec.walltime * tmpMaxAtomSize
            else:
                minWalltime = None
            # take # of consumers into account
            if not taskSpec.useEventService() or taskSpec.useJobCloning():
                strMinWalltime = f"walltime*inputSize={taskSpec.walltime}*{tmpMaxAtomSize}"
            else:
                strMinWalltime = f"walltime*inputSize/nEsConsumers/maxAttemptEsJob={taskSpec.walltime}*{tmpMaxAtomSize}/{nEsConsumers}/{maxAttemptEsJob}"
        else:
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(getNumEvents=True)
            if taskSpec.getCpuTime() is not None:
                minWalltime = taskSpec.getCpuTime() * tmpMaxAtomSize
                if taskSpec.dynamicNumEvents():
                    # use minGranularity as the smallest chunk
                    minGranularity = taskSpec.get_min_granularity()
                    minWalltime_dyn = taskSpec.getCpuTime() * minGranularity
                    strMinWalltime_dyn = f"cpuTime*minGranularity={taskSpec.getCpuTime()}*{minGranularity}"
                    # use most consecutive events as the largest chunk
                    eventJump, totalEvents = inputChunk.check_event_jump_and_sum()
                    # use maxEventsPerJob if smaller
                    maxEventsPerJob = taskSpec.get_max_events_per_job()
                    if maxEventsPerJob:
                        totalEvents = min(totalEvents, maxEventsPerJob)
                    maxWalltime_dyn = taskSpec.getCpuTime() * totalEvents
                    strMaxWalltime_dyn = f"cpuTime*maxEventsPerJob={taskSpec.getCpuTime()}*{totalEvents}"
            # take # of consumers into account
            if not taskSpec.useEventService() or taskSpec.useJobCloning():
                strMinWalltime = f"cpuTime*nEventsPerJob={taskSpec.getCpuTime()}*{tmpMaxAtomSize}"
            else:
                strMinWalltime = f"cpuTime*nEventsPerJob/nEsConsumers/maxAttemptEsJob={taskSpec.getCpuTime()}*{tmpMaxAtomSize}/{nEsConsumers}/{maxAttemptEsJob}"
        if minWalltime:
            minWalltime /= nEsConsumers * maxAttemptEsJob
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            siteMaxTime = tmpSiteSpec.maxtime
            origSiteMaxTime = siteMaxTime
            # check max walltime at the site
            tmpSiteStr = f"{siteMaxTime}"
            if taskSpec.useHS06():
                oldSiteMaxTime = siteMaxTime
                siteMaxTime -= taskSpec.baseWalltime
                tmpSiteStr = f"({oldSiteMaxTime}-{taskSpec.baseWalltime})"
            if siteMaxTime not in [None, 0] and tmpSiteSpec.coreCount not in [None, 0]:
                siteMaxTime *= tmpSiteSpec.coreCount
                tmpSiteStr += f"*{tmpSiteSpec.coreCount}"
            if taskSpec.useHS06():
                if siteMaxTime not in [None, 0]:
                    siteMaxTime *= tmpSiteSpec.corepower
                    tmpSiteStr += f"*{tmpSiteSpec.corepower}"
                siteMaxTime *= float(taskSpec.cpuEfficiency) / 100.0
                siteMaxTime = int(siteMaxTime)
                tmpSiteStr += f"*{taskSpec.cpuEfficiency}%"
            if origSiteMaxTime != 0:
                toSkip = False
                if minWalltime_dyn and tmpSiteSpec.mintime and tmpSiteSpec.mintime > 0:
                    if minWalltime_dyn > siteMaxTime:
                        tmp_msg = f"  skip site={tmpSiteName} due to short site walltime {tmpSiteStr} " f"(site upper limit) less than {strMinWalltime_dyn} "
                        toSkip = True
                else:
                    if minWalltime and minWalltime > siteMaxTime:
                        tmp_msg = f"  skip site={tmpSiteName} due to short site walltime {tmpSiteStr} " f"(site upper limit) less than {strMinWalltime} "
                        toSkip = True
                if toSkip:
                    tmp_msg += "criteria=-shortwalltime"
                    tmpLog.info(tmp_msg)
                    continue
            # sending scouts or merge or walltime-undefined jobs to only sites where walltime is more than 1 day
            if (
                (not sitePreAssigned and inputChunk.useScout())
                or inputChunk.isMerging
                or (not taskSpec.walltime and not taskSpec.walltimeUnit and not taskSpec.cpuTimeUnit)
                or (not taskSpec.getCpuTime() and taskSpec.cpuTimeUnit)
            ):
                minTimeForZeroWalltime = 24
                str_minTimeForZeroWalltime = f"{minTimeForZeroWalltime}hr*10HS06s"
                minTimeForZeroWalltime *= 60 * 60 * 10

                if tmpSiteSpec.coreCount not in [None, 0]:
                    minTimeForZeroWalltime *= tmpSiteSpec.coreCount
                    str_minTimeForZeroWalltime += f"*{tmpSiteSpec.coreCount}cores"

                if siteMaxTime != 0 and siteMaxTime < minTimeForZeroWalltime:
                    tmp_msg = f"  skip site={tmpSiteName} due to site walltime {tmpSiteStr} (site upper limit) insufficient "
                    if inputChunk.useScout():
                        tmp_msg += f"for scouts ({str_minTimeForZeroWalltime} at least) "
                        tmp_msg += "criteria=-scoutwalltime"
                    else:
                        tmp_msg += f"for zero walltime ({str_minTimeForZeroWalltime} at least) "
                        tmp_msg += "criteria=-zerowalltime"
                    tmpLog.info(tmp_msg)
                    continue
            # check min walltime at the site
            siteMinTime = tmpSiteSpec.mintime
            origSiteMinTime = siteMinTime
            tmpSiteStr = f"{siteMinTime}"
            if taskSpec.useHS06():
                oldSiteMinTime = siteMinTime
                siteMinTime -= taskSpec.baseWalltime
                tmpSiteStr = f"({oldSiteMinTime}-{taskSpec.baseWalltime})"
            if siteMinTime not in [None, 0] and tmpSiteSpec.coreCount not in [None, 0]:
                siteMinTime *= tmpSiteSpec.coreCount
                tmpSiteStr += f"*{tmpSiteSpec.coreCount}"
            if taskSpec.useHS06():
                if siteMinTime not in [None, 0]:
                    siteMinTime *= tmpSiteSpec.corepower
                    tmpSiteStr += f"*{tmpSiteSpec.corepower}"
                siteMinTime *= float(taskSpec.cpuEfficiency) / 100.0
                siteMinTime = int(siteMinTime)
                tmpSiteStr += f"*{taskSpec.cpuEfficiency}%"
            if origSiteMinTime != 0:
                toSkip = False
                if minWalltime_dyn and tmpSiteSpec.mintime and tmpSiteSpec.mintime > 0:
                    if minWalltime_dyn < siteMinTime and (maxWalltime_dyn is None or maxWalltime_dyn < siteMinTime):
                        tmp_msg = f"  skip site {tmpSiteName} due to short job walltime {tmpSiteStr} " f"(site lower limit) greater than {strMinWalltime_dyn} "
                        if maxWalltime_dyn:
                            tmp_msg += f"and {strMinWalltime_dyn} "
                        toSkip = True
                else:
                    if (minWalltime is None or minWalltime < siteMinTime) and (maxWalltime is None or maxWalltime < siteMinTime):
                        tmp_msg = f"  skip site {tmpSiteName} due to short job walltime {tmpSiteStr} " f"(site lower limit) greater than {strMinWalltime} "
                        if maxWalltime:
                            tmp_msg += f"and {strMaxWalltime} "
                        toSkip = True
                if toSkip:
                    tmp_msg += "criteria=-longwalltime"
                    tmpLog.info(tmp_msg)
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        if not taskSpec.useHS06():
            tmpLog.info(f"{len(scanSiteList)} candidates passed walltime check {minWalltime}({taskSpec.walltimeUnit})")
        else:
            tmpStr = f"{len(scanSiteList)} candidates passed walltime check {strMinWalltime}({taskSpec.cpuTimeUnit})"
            if maxWalltime:
                tmpStr += f" and {strMaxWalltime}"
            if minWalltime_dyn:
                tmpStr += f" for normal PQs, {strMinWalltime_dyn}"
                if maxWalltime_dyn:
                    tmpStr += f" and {strMaxWalltime_dyn}"
                tmpStr += " for PQs with non-zero mintime"
            tmpLog.info(tmpStr)
        self.add_summary_message(oldScanSiteList, scanSiteList, "walltime check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for network connectivity
        if not sitePreAssigned:
            ipConnectivity = taskSpec.getIpConnectivity()
            ipStack = taskSpec.getIpStack()
            if ipConnectivity or ipStack:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # check connectivity
                    if ipConnectivity:
                        wn_connectivity = tmpSiteSpec.get_wn_connectivity()
                        if wn_connectivity == "full":
                            pass
                        elif wn_connectivity == "http" and ipConnectivity == "http":
                            pass
                        else:
                            tmp_msg = f"  skip site={tmpSiteName} due to insufficient connectivity site={wn_connectivity} vs task={ipConnectivity} "
                            tmp_msg += "criteria=-network"
                            tmpLog.info(tmp_msg)
                            continue
                    # check IP stack
                    if ipStack:
                        wn_ipstack = tmpSiteSpec.get_ipstack()
                        if ipStack != wn_ipstack:
                            tmp_msg = f"  skip site={tmpSiteName} due to IP stack mismatch site={wn_ipstack} vs task={ipStack} "
                            tmp_msg += "criteria=-network"
                            tmpLog.info(tmp_msg)
                            continue

                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info(f"{len(scanSiteList)} candidates passed network check ({ipConnectivity})")
                self.add_summary_message(oldScanSiteList, scanSiteList, "network check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # selection for event service
        if not sitePreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # event service
                if taskSpec.useEventService() and not taskSpec.useJobCloning():
                    if tmpSiteSpec.getJobSeed() == "std":
                        tmp_msg = f"  skip site={tmpSiteName} since EventService is not allowed "
                        tmp_msg += "criteria=-es"
                        tmpLog.info(tmp_msg)
                        continue
                    if tmpSiteSpec.getJobSeed() == "eshigh" and not esHigh:
                        tmp_msg = f"  skip site={tmpSiteName} since low prio EventService is not allowed "
                        tmp_msg += "criteria=-eshigh"
                        tmpLog.info(tmp_msg)
                        continue
                else:
                    if tmpSiteSpec.getJobSeed() == "es":
                        tmp_msg = f"  skip site={tmpSiteName} since only EventService is allowed "
                        tmp_msg += "criteria=-nones"
                        tmpLog.info(tmp_msg)
                        continue
                # skip UCORE/SCORE
                if (
                    taskSpec.useEventService()
                    and not taskSpec.useJobCloning()
                    and tmpSiteSpec.sitename != tmpSiteSpec.get_unified_name()
                    and tmpSiteSpec.coreCount == 1
                ):
                    tmp_msg = f"  skip site={tmpSiteName} since EventService on UCORE/SCORE "
                    tmp_msg += "criteria=-es_ucore"
                    tmpLog.info(tmp_msg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed EventService check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "EventService check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for transferring
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            try:
                tmpAtlasSiteName = storageMapping[tmpSiteName]["default"]
                if tmpSiteName not in sites_in_nucleus + sites_sharing_output_storages_in_nucleus and nucleus != tmpAtlasSiteName:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # limit
                    def_maxTransferring = 2000
                    if tmpSiteSpec.transferringlimit == 0:
                        # use default value
                        maxTransferring = def_maxTransferring
                    else:
                        maxTransferring = tmpSiteSpec.transferringlimit
                    # transferring jobs with nuclei in downtime
                    n_jobs_bad_transfer = 0
                    if tmpSiteName in transferring_job_map:
                        for tmp_nucleus in transferring_job_map[tmpSiteName]:
                            if tmp_nucleus in nucleus_with_storages_unwritable_over_wan:
                                n_jobs_bad_transfer += transferring_job_map[tmpSiteName][tmp_nucleus]
                    # check at the site
                    nTraJobs = AtlasBrokerUtils.getNumJobs(jobStatMap, tmpSiteName, "transferring") - n_jobs_bad_transfer
                    nRunJobs = AtlasBrokerUtils.getNumJobs(jobStatMap, tmpSiteName, "running")
                    if max(maxTransferring, 2 * nRunJobs) < nTraJobs:
                        tmpStr = "  skip site=%s due to too many transferring=%s greater than max(%s,2x%s) criteria=-transferring" % (
                            tmpSiteName,
                            nTraJobs,
                            maxTransferring,
                            nRunJobs,
                        )
                        newSkippedTmp[tmpSiteName] = tmpStr
            except KeyError:
                pass
            newScanSiteList.append(tmpSiteName)
        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info(f"{len(scanSiteList)} candidates passed transferring check")
        self.add_summary_message(oldScanSiteList, scanSiteList, "transferring check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for pledge when work is short
        if not sitePreAssigned and work_shortage:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.is_opportunistic():
                    # skip opportunistic sites when plenty of work is unavailable
                    tmp_msg = f"  skip site={tmpSiteName} to avoid opportunistic in case of work shortage "
                    tmp_msg += "criteria=-non_pledged"
                    tmpLog.info(tmp_msg)
                    continue
                elif tmpSiteSpec.pledgedCPU is not None and tmpSiteSpec.pledgedCPU > 0:
                    # check number of cores
                    tmp_stat_dict = core_statistics.get(tmpSiteName, {})
                    n_running_cores = tmp_stat_dict.get("running", 0)
                    n_starting_cores = tmp_stat_dict.get("starting", 0)
                    tmpLog.debug(f"  {tmpSiteName} running={n_running_cores} starting={n_starting_cores}")
                    if n_running_cores + n_starting_cores > tmpSiteSpec.pledgedCPU:
                        tmp_msg = f"  skip site={tmpSiteName} since nCores(running+starting)={n_running_cores+n_starting_cores} more than pledgedCPU={tmpSiteSpec.pledgedCPU} "
                        tmp_msg += "in case of work shortage "
                        tmp_msg += "criteria=-over_pledged"
                        tmpLog.info(tmp_msg)
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed pledge check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "pledge check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for T1 weight
        if taskSpec.getT1Weight() < 0 and not inputChunk.isMerging:
            useT1Weight = True
        else:
            useT1Weight = False
        t1Weight = taskSpec.getT1Weight()
        if t1Weight == 0:
            tmpLog.info(f"IO intensity {taskSpec.ioIntensity}")
            # use T1 weight if IO intensive
            t1Weight = 1
            if taskSpec.ioIntensity is not None and taskSpec.ioIntensity > 500:
                t1Weight = WORLD_NUCLEUS_WEIGHT

        oldScanSiteList = copy.copy(scanSiteList)
        if t1Weight < 0 and not inputChunk.isMerging:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if tmpSiteName not in sites_in_nucleus + sites_sharing_output_storages_in_nucleus:
                    tmpLog.info(f"  skip site={tmpSiteName} due to negative T1 weight criteria=-t1weight")
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            t1Weight = 1
        t1Weight = max(t1Weight, t1WeightForHighPrio)
        tmpLog.info(f"T1 weight {t1Weight}")
        tmpLog.info(f"{len(scanSiteList)} candidates passed T1 weight check")
        self.add_summary_message(oldScanSiteList, scanSiteList, "T1 weight check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for full chain
        if nucleusSpec:
            full_chain = taskSpec.check_full_chain_with_nucleus(nucleusSpec)
            oldScanSiteList = copy.copy(scanSiteList)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if full_chain:
                    # skip PQs not in the nucleus
                    if tmpSiteName not in sites_in_nucleus:
                        tmpLog.info(f"  skip site={tmpSiteName} not in nucleus for full chain criteria=-full_chain")
                        continue
                else:
                    # skip PQs only for full-chain
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if tmpSiteSpec.bare_nucleus_mode() == "only":
                        tmpLog.info(f"  skip site={tmpSiteName} only for full chain criteria=-not_full_chain")
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed full chain ({full_chain}) check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "full chain check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for nPilot
        nPilotMap = {}
        if not sitePreAssigned and not siteListPreAssigned:
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                nPilot = 0
                if tmpSiteName in nWNmap:
                    nPilot = nWNmap[tmpSiteName]["getJob"] + nWNmap[tmpSiteName]["updateJob"]
                # skip no pilot sites unless the task and the site use jumbo jobs or the site is standby
                if (
                    nPilot == 0
                    and ("test" not in taskSpec.prodSourceLabel or inputChunk.isExpress())
                    and (taskSpec.getNumJumboJobs() is None or not tmpSiteSpec.useJumboJobs())
                    and tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type) is None
                ):
                    tmpStr = f"  skip site={tmpSiteName} due to no pilot criteria=-nopilot"
                    newSkippedTmp[tmpSiteName] = tmpStr
                newScanSiteList.append(tmpSiteName)
                nPilotMap[tmpSiteName] = nPilot
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed pilot activity check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "pilot check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        # return if to give a hint for task brokerage
        if hintForTB:
            ######################################
            # temporary problems
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                if tmpSiteName in siteSkippedTmp:
                    tmpLog.info(siteSkippedTmp[tmpSiteName])
                else:
                    newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed temporary problem check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "temp problem check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            return self.SC_SUCCEEDED, scanSiteList

        ######################################
        # get available files
        siteSizeMap = {}
        siteSizeMapWT = {}
        availableFileMap = {}
        siteFilesMap = {}
        siteFilesMapWT = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(
                    self.get_unified_sites(scanSiteList), self.siteMapper, JobUtils.PROD_PS, JobUtils.PROD_PS
                )
                # disable file lookup for merge jobs or secondary datasets
                checkCompleteness = True
                useCompleteOnly = False
                if inputChunk.isMerging:
                    checkCompleteness = False
                if not datasetSpec.isMaster() or (taskSpec.ioIntensity and taskSpec.ioIntensity > self.io_intensity_cutoff and not taskSpec.inputPreStaging()):
                    useCompleteOnly = True
                # get available files per site/endpoint
                tmpLog.debug(f"getting available files for {datasetSpec.datasetName}")
                tmpAvFileMap = self.ddmIF.getAvailableFiles(
                    datasetSpec,
                    siteStorageEP,
                    self.siteMapper,
                    check_completeness=checkCompleteness,
                    storage_token=datasetSpec.storageToken,
                    complete_only=useCompleteOnly,
                )
                tmpLog.debug("got")
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError("ddmIF.getAvailableFiles failed")
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception as e:
                tmp_str = str(e).replace("\\n", " ")
                tmpLog.error(f"failed to get available files with {tmp_str}")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            # loop over all sites to get the size of available files
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                siteSizeMap.setdefault(tmpSiteName, 0)
                siteSizeMapWT.setdefault(tmpSiteName, 0)
                siteFilesMap.setdefault(tmpSiteName, set())
                siteFilesMapWT.setdefault(tmpSiteName, set())
                # get the total size of available files
                if tmpSiteName in availableFileMap[datasetSpec.datasetName]:
                    availableFiles = availableFileMap[datasetSpec.datasetName][tmpSiteName]
                    for tmpFileSpec in availableFiles["localdisk"] + availableFiles["cache"]:
                        if tmpFileSpec.lfn not in siteFilesMap[tmpSiteName]:
                            siteSizeMap[tmpSiteName] += tmpFileSpec.fsize
                            siteSizeMapWT[tmpSiteName] += tmpFileSpec.fsize
                        siteFilesMap[tmpSiteName].add(tmpFileSpec.lfn)
                        siteFilesMapWT[tmpSiteName].add(tmpFileSpec.lfn)
                    for tmpFileSpec in availableFiles["localtape"]:
                        if tmpFileSpec.lfn not in siteFilesMapWT[tmpSiteName]:
                            siteSizeMapWT[tmpSiteName] += tmpFileSpec.fsize
                            siteFilesMapWT[tmpSiteName].add(tmpFileSpec.lfn)
        # get max total size
        allLFNs = set()
        totalSize = 0
        for datasetSpec in inputChunk.getDatasets():
            for fileSpec in datasetSpec.Files:
                if fileSpec.lfn not in allLFNs:
                    allLFNs.add(fileSpec.lfn)
                    try:
                        totalSize += fileSpec.fsize
                    except Exception:
                        pass
        # get max num of available files
        maxNumFiles = len(allLFNs)

        ######################################
        # selection for fileSizeToMove
        moveSizeCutoffGB = self.taskBufferIF.getConfigValue(COMPONENT, "SIZE_CUTOFF_TO_MOVE_INPUT", APP, VO)
        if moveSizeCutoffGB is None:
            moveSizeCutoffGB = 10
        moveNumFilesCutoff = self.taskBufferIF.getConfigValue(COMPONENT, "NUM_CUTOFF_TO_MOVE_INPUT", APP, VO)
        if moveNumFilesCutoff is None:
            moveNumFilesCutoff = 100
        if (
            not sitePreAssigned
            and totalSize > 0
            and not inputChunk.isMerging
            and taskSpec.ioIntensity is not None
            and taskSpec.ioIntensity > self.io_intensity_cutoff
            and not (taskSpec.useEventService() and not taskSpec.useJobCloning())
        ):
            newScanSiteList = []
            newScanSiteListWT = []
            msgList = []
            msgListDT = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # file size to move in MB
                mbToMove = int((totalSize - siteSizeMap[tmpSiteName]) / (1024 * 1024))
                nFilesToMove = maxNumFiles - len(siteFilesMap[tmpSiteName])
                mbToMoveWT = int((totalSize - siteSizeMapWT[tmpSiteName]) / (1024 * 1024))
                nFilesToMoveWT = maxNumFiles - len(siteFilesMapWT[tmpSiteName])
                if min(mbToMove, mbToMoveWT) > moveSizeCutoffGB * 1024 or min(nFilesToMove, nFilesToMoveWT) > moveNumFilesCutoff:
                    tmp_msg = f"  skip site={tmpSiteName} "
                    if mbToMove > moveSizeCutoffGB * 1024:
                        tmp_msg += f"since size of missing input is too large ({int(mbToMove / 1024)} GB > {moveSizeCutoffGB} GB) "
                    else:
                        tmp_msg += f"since the number of missing input files is too large ({nFilesToMove} > {moveNumFilesCutoff}) "
                    tmp_msg += f"for IO intensive task ({taskSpec.ioIntensity} > {self.io_intensity_cutoff} kBPerS) "
                    tmp_msg += "criteria=-io"
                    msgListDT.append(tmp_msg)
                    continue
                if mbToMove > moveSizeCutoffGB * 1024 or nFilesToMove > moveNumFilesCutoff:
                    tmp_msg = f"  skip site={tmpSiteName} "
                    if mbToMove > moveSizeCutoffGB * 1024:
                        tmp_msg += f"since size of missing disk input is too large ({int(mbToMove / 1024)} GB > {moveSizeCutoffGB} GB) "
                    else:
                        tmp_msg += f"since the number of missing disk input files is too large ({nFilesToMove} > {moveNumFilesCutoff}) "
                    tmp_msg += f"for IO intensive task ({taskSpec.ioIntensity} > {self.io_intensity_cutoff} kBPerS) "
                    tmp_msg += "criteria=-io"
                    msgList.append(tmp_msg)
                    newScanSiteListWT.append(tmpSiteName)
                else:
                    newScanSiteList.append(tmpSiteName)
            if len(newScanSiteList + newScanSiteListWT) == 0:
                # disable if no candidate
                tmpLog.info("disabled IO check since no candidate passed")
            else:
                for tmp_msg in msgListDT:
                    tmpLog.info(tmp_msg)
                if len(newScanSiteList) == 0:
                    # use candidates with TAPE replicas
                    newScanSiteList = newScanSiteListWT
                else:
                    for tmp_msg in msgList:
                        tmpLog.info(tmp_msg)
                oldScanSiteList = copy.copy(scanSiteList)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates passed IO check")
                self.add_summary_message(oldScanSiteList, scanSiteList, "IO check")
                if scanSiteList == []:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # temporary problems
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in scanSiteList:
            if tmpSiteName in siteSkippedTmp:
                tmpLog.info(siteSkippedTmp[tmpSiteName])
            else:
                newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.info(f"{len(scanSiteList)} candidates passed temporary problem check")
        self.add_summary_message(oldScanSiteList, scanSiteList, "temporary problem check")
        if scanSiteList == []:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # calculate weight
        jobStatPrioMapGS = dict()
        jobStatPrioMapGSOnly = dict()
        if workQueue.is_global_share:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        else:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo, taskSpec.prodSourceLabel)
            if tmpSt:
                tmpSt, jobStatPrioMapGS = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
                tmpSt, jobStatPrioMapGSOnly = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo, True)
        if tmpSt:
            # count jobs per resource type
            tmpSt, tmpStatMapRT = self.taskBufferIF.getJobStatisticsByResourceTypeSite(workQueue)
        if not tmpSt:
            tmpLog.error("failed to get job statistics with priority")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        workerStat = self.taskBufferIF.ups_load_worker_stats()
        upsQueues = set(self.taskBufferIF.ups_get_queues())
        tmpLog.info(f"calculate weight and check cap for {len(scanSiteList)} candidates")
        cutoffName = f"NQUEUELIMITSITE_{taskSpec.gshare}"
        cutOffValue = self.taskBufferIF.getConfigValue(COMPONENT, cutoffName, APP, VO)
        if not cutOffValue:
            cutOffValue = 20
        else:
            tmpLog.info(f"using {cutoffName}={cutOffValue} as lower limit for nQueued")
        weightMapPrimary = {}
        weightMapSecondary = {}
        weightMapJumbo = {}
        largestNumRun = None
        newScanSiteList = []
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            if not workQueue.is_global_share and esHigh and tmpSiteSpec.getJobSeed() == "eshigh":
                tmp_wq_tag = wq_tag_global_share
                tmp_jobStatPrioMap = jobStatPrioMapGS
            else:
                tmp_wq_tag = wq_tag
                tmp_jobStatPrioMap = jobStatPrioMap
            nRunning = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "running", None, tmp_wq_tag)
            nRunningAll = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "running", None, None)
            corrNumPilotStr = ""
            if not workQueue.is_global_share:
                # correction factor for nPilot
                nRunningGS = AtlasBrokerUtils.getNumJobs(jobStatPrioMapGS, tmpSiteName, "running", None, wq_tag_global_share)
                nRunningGSOnly = AtlasBrokerUtils.getNumJobs(jobStatPrioMapGSOnly, tmpSiteName, "running", None, wq_tag_global_share)
                corrNumPilot = float(nRunningGS - nRunningGSOnly + 1) / float(nRunningGS + 1)
                corrNumPilotStr = f"*(nRunResourceQueue({nRunningGS - nRunningGSOnly})+1)/(nRunGlobalShare({nRunningGS})+1)"
            else:
                corrNumPilot = 1
            nDefined = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "defined", None, tmp_wq_tag) + self.getLiveCount(tmpSiteName)
            nAssigned = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "assigned", None, tmp_wq_tag)
            nActivated = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "activated", None, tmp_wq_tag)
            nStarting = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, "starting", None, tmp_wq_tag)
            if tmpSiteName in nPilotMap:
                nPilot = nPilotMap[tmpSiteName]
            else:
                nPilot = 0
            # get num workers
            nWorkers = 0
            nWorkersCutoff = 20
            if tmpSiteName in workerStat:
                for tmpHarvesterID, tmpLabelStat in workerStat[tmpSiteName].items():
                    for tmpHarvesterID, tmpResStat in tmpLabelStat.items():
                        for tmpResType, tmpCounts in tmpResStat.items():
                            for tmpStatus, tmpNum in tmpCounts.items():
                                if tmpStatus in ["running", "submitted"]:
                                    nWorkers += tmpNum
                # cap
                nWorkers = min(nWorkersCutoff, nWorkers)
            # use nWorkers to bootstrap
            if (
                nPilot > 0
                and nRunning < nWorkersCutoff
                and nWorkers > nRunning
                and tmpSiteName in upsQueues
                and taskSpec.currentPriority <= self.max_prio_for_bootstrap
            ):
                tmpLog.debug(f"using nWorkers={nWorkers} as nRunning at {tmpPseudoSiteName} since original nRunning={nRunning} is low")
                nRunning = nWorkers
            # take into account the number of standby jobs
            numStandby = tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type)
            if numStandby is None:
                pass
            elif taskSpec.currentPriority > self.max_prio_for_bootstrap:
                # don't use numSlots for high prio tasks
                tmpLog.debug(f"ignored numSlots at {tmpPseudoSiteName} due to prio={taskSpec.currentPriority} > {self.max_prio_for_bootstrap}")
            elif numStandby == 0:
                # use the number of starting jobs as the number of standby jobs
                nRunning = nStarting + nRunning
                tmpLog.debug(f"using nStarting+nRunning at {tmpPseudoSiteName} to set nRunning={nRunning} due to numSlot={numStandby}")
            else:
                # the number of standby jobs is defined
                nRunning = max(int(numStandby / tmpSiteSpec.coreCount), nRunning)
                tmpLog.debug(f"using numSlots={numStandby}/coreCount at {tmpPseudoSiteName} to set nRunning={nRunning}")
            manyAssigned = float(nAssigned + 1) / float(nActivated + 1)
            manyAssigned = min(2.0, manyAssigned)
            manyAssigned = max(1.0, manyAssigned)
            # take into account nAssigned when jobs need input data transfer
            if tmpSiteName not in siteSizeMap or siteSizeMap[tmpSiteName] >= totalSize:
                useAssigned = False
            else:
                useAssigned = True
            # stat with resource type
            RT_Cap = 2
            if tmpSiteName in tmpStatMapRT and taskSpec.resource_type in tmpStatMapRT[tmpSiteName]:
                useCapRT = True
                tmpRTrunning = tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get("running", 0)
                tmpRTrunning = max(tmpRTrunning, nRunning)
                tmpRTqueue = tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get("defined", 0)
                if useAssigned:
                    tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get("assigned", 0)
                tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get("activated", 0)
                tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get("starting", 0)
            else:
                useCapRT = False
                tmpRTqueue = 0
                tmpRTrunning = 0
            if totalSize == 0 or totalSize - siteSizeMap[tmpSiteName] <= 0:
                weight = float(nRunning + 1) / float(nActivated + nStarting + nDefined + 10)
                weightStr = "nRun={0} nAct={1} nStart={3} nDef={4} nPilot={7}{9} totalSizeMB={5} totalNumFiles={8} " "nRun_rt={10} nQueued_rt={11} "
            else:
                weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + nDefined + 10) / manyAssigned
                weightStr = (
                    "nRun={0} nAct={1} nAss={2} nStart={3} nDef={4} manyAss={6} nPilot={7}{9} totalSizeMB={5} "
                    "totalNumFiles={8} nRun_rt={10} nQueued_rt={11} "
                )
            weightStr = weightStr.format(
                nRunning,
                nActivated,
                nAssigned,
                nStarting,
                nDefined,
                int(totalSize / 1024 / 1024),
                manyAssigned,
                nPilot,
                maxNumFiles,
                corrNumPilotStr,
                tmpRTrunning,
                tmpRTqueue,
            )
            # reduce weights by taking data availability into account
            skipRemoteData = False
            if totalSize > 0:
                # file size to move in MB
                mbToMove = int((totalSize - siteSizeMap[tmpSiteName]) / (1024 * 1024))
                # number of files to move
                nFilesToMove = maxNumFiles - len(siteFilesMap[tmpSiteName])
                # consider size and # of files
                if tmpSiteSpec.use_only_local_data() and (mbToMove > 0 or nFilesToMove > 0):
                    skipRemoteData = True
                else:
                    weight = weight * (totalSize + siteSizeMap[tmpSiteName]) / totalSize / (nFilesToMove / 100 + 1)
                    weightStr += f"fileSizeToMoveMB={mbToMove} nFilesToMove={nFilesToMove} "
            # T1 weight
            if tmpSiteName in sites_in_nucleus + sites_sharing_output_storages_in_nucleus:
                weight *= t1Weight
                weightStr += f"t1W={t1Weight} "
            if useT1Weight:
                weightStr += f"nRunningAll={nRunningAll} "
            # apply network metrics to weight
            if nucleus:
                tmpAtlasSiteName = None
                try:
                    tmpAtlasSiteName = storageMapping[tmpSiteName]["default"]
                except KeyError:
                    tmpLog.debug(f"Panda site {tmpSiteName} was not in site mapping. Default network values will be given")

                try:
                    closeness = networkMap[tmpAtlasSiteName][AGIS_CLOSENESS]
                except KeyError:
                    tmpLog.debug(f"No {AGIS_CLOSENESS} information found in network matrix from {tmpAtlasSiteName}({tmpSiteName}) to {nucleus}")
                    closeness = MAX_CLOSENESS * 0.7

                try:
                    nFilesInQueue = networkMap[tmpAtlasSiteName][queued_tag]
                except KeyError:
                    tmpLog.debug(f"No {queued_tag} information found in network matrix from {tmpAtlasSiteName} ({tmpSiteName}) to {nucleus}")
                    nFilesInQueue = 0

                mbps = None
                try:
                    mbps = networkMap[tmpAtlasSiteName][FTS_1W]
                    mbps = networkMap[tmpAtlasSiteName][FTS_1D]
                    mbps = networkMap[tmpAtlasSiteName][FTS_1H]
                except KeyError:
                    if mbps is None:
                        tmpLog.debug(f"No dynamic FTS mbps information found in network matrix from {tmpAtlasSiteName}({tmpSiteName}) to {nucleus}")

                # network weight: value between 1 and 2, except when nucleus == satellite
                if nucleus == tmpAtlasSiteName:  # 25 per cent weight boost for processing in nucleus itself
                    weightNwQueue = 2.5
                    weightNwThroughput = 2.5
                else:
                    # queue weight: the more in queue, the lower the weight
                    weightNwQueue = 2 - (nFilesInQueue * 1.0 / self.queue_threshold)

                    # throughput weight: the higher the throughput, the higher the weight
                    if mbps is not None:
                        weightNwThroughput = self.convertMBpsToWeight(mbps)
                    else:
                        weightNwThroughput = 1 + ((MAX_CLOSENESS - closeness) * 1.0 / (MAX_CLOSENESS - MIN_CLOSENESS))

                # combine queue and throughput weights
                weightNw = self.nwQueueImportance * weightNwQueue + self.nwThroughputImportance * weightNwThroughput

                weightStr += f"weightNw={weightNw} ( closeness={closeness} nFilesQueued={nFilesInQueue} throughputMBps={mbps} Network weight:{self.nwActive} )"

                # If network measurements in active mode, apply the weight
                if self.nwActive:
                    weight *= weightNw

                tmpLog.info(
                    "subject=network_data src={1} dst={2} weight={3} weightNw={4} "
                    "weightNwThroughput={5} weightNwQueued={6} mbps={7} closeness={8} nqueued={9}".format(
                        taskSpec.jediTaskID, tmpAtlasSiteName, nucleus, weight, weightNw, weightNwThroughput, weightNwQueue, mbps, closeness, nFilesInQueue
                    )
                )

            # make candidate
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName, tmpSiteName)
            # override attributes
            siteCandidateSpec.override_attribute("maxwdir", newMaxwdir.get(tmpSiteName))
            platforms = resolved_platforms.get(tmpSiteName)
            if platforms:
                siteCandidateSpec.override_attribute("platforms", platforms)
            # set weight and params
            siteCandidateSpec.weight = weight
            siteCandidateSpec.nRunningJobs = nRunning
            siteCandidateSpec.nAssignedJobs = nAssigned
            # set available files
            for tmpDatasetName, availableFiles in availableFileMap.items():
                if tmpSiteName in availableFiles:
                    siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]["localdisk"])
                    siteCandidateSpec.add_local_tape_files(availableFiles[tmpSiteName]["localtape"])
                    siteCandidateSpec.add_cache_files(availableFiles[tmpSiteName]["cache"])
                    siteCandidateSpec.add_remote_files(availableFiles[tmpSiteName]["remote"])
            # add files as remote since WAN access is allowed
            if taskSpec.allowInputWAN() and tmpSiteSpec.allowWanInputAccess():
                siteCandidateSpec.remoteProtocol = "direct"
                for datasetSpec in inputChunk.getDatasets():
                    siteCandidateSpec.add_remote_files(datasetSpec.Files)

            # check if site is locked
            lockedByBrokerage = self.checkSiteLock(taskSpec.vo, taskSpec.prodSourceLabel, tmpPseudoSiteName, taskSpec.workQueue_ID, taskSpec.resource_type)

            # check cap with nRunning
            nPilot *= corrNumPilot
            cutOffFactor = 2
            if tmpSiteSpec.capability == "ucore":
                if not inputChunk.isExpress():
                    siteCandidateSpec.nRunningJobsCap = max(cutOffValue, cutOffFactor * tmpRTrunning)
                siteCandidateSpec.nQueuedJobs = tmpRTqueue
            else:
                if not inputChunk.isExpress():
                    siteCandidateSpec.nRunningJobsCap = max(cutOffValue, cutOffFactor * nRunning)
                if useAssigned:
                    siteCandidateSpec.nQueuedJobs = nActivated + nAssigned + nStarting
                else:
                    siteCandidateSpec.nQueuedJobs = nActivated + nStarting
            if taskSpec.getNumJumboJobs() is None or not tmpSiteSpec.useJumboJobs():
                forJumbo = False
            else:
                forJumbo = True
            # OK message. Use jumbo as primary by default
            if not forJumbo:
                okMsg = f"  use site={tmpPseudoSiteName} with weight={weight} {weightStr} criteria=+use"
                okAsPrimay = False
            else:
                okMsg = f"  use site={tmpPseudoSiteName} for jumbo jobs with weight={weight} {weightStr} criteria=+usejumbo"
                okAsPrimay = True
            # checks
            if lockedByBrokerage:
                ngMsg = f"  skip site={tmpPseudoSiteName} due to locked by another brokerage "
                ngMsg += "criteria=-lock"
            elif skipRemoteData:
                ngMsg = f"  skip site={tmpPseudoSiteName} due to non-local data "
                ngMsg += "criteria=-non_local"
            elif not inputChunk.isExpress() and tmpSiteSpec.capability != "ucore" and siteCandidateSpec.nQueuedJobs > siteCandidateSpec.nRunningJobsCap:
                if not useAssigned:
                    ngMsg = f"  skip site={tmpPseudoSiteName} weight={weight} due to nDefined+nActivated+nStarting={siteCandidateSpec.nQueuedJobs} "
                    ngMsg += "(nAssigned ignored due to data locally available) "
                else:
                    ngMsg = f"  skip site={tmpPseudoSiteName} weight={weight} due to nDefined+nActivated+nAssigned+nStarting={siteCandidateSpec.nQueuedJobs} "
                ngMsg += f"greater than max({cutOffValue},{cutOffFactor}*nRun) "
                ngMsg += f"{weightStr} "
                ngMsg += "criteria=-cap"
            elif self.nwActive and inputChunk.isExpress() and weightNw < self.nw_threshold * self.nw_weight_multiplier:
                ngMsg = f"  skip site={tmpPseudoSiteName} due to low network weight for express task weightNw={weightNw} threshold={self.nw_threshold} "
                ngMsg += f"{weightStr} "
                ngMsg += "criteria=-lowNetworkWeight"
            elif useCapRT and tmpRTqueue > max(cutOffValue, tmpRTrunning * RT_Cap) and not inputChunk.isExpress():
                ngMsg = f"  skip site={tmpSiteName} since "
                if useAssigned:
                    ngMsg += f"nDefined_rt+nActivated_rt+nAssigned_rt+nStarting_rt={tmpRTqueue} "
                else:
                    ngMsg += f"nDefined_rt+nActivated_rt+nStarting_rt={tmpRTqueue} "
                    ngMsg += "(nAssigned_rt ignored due to data locally available) "
                ngMsg += "with gshare+resource_type is greater than "
                ngMsg += "max({0},{1}*nRun_rt={1}*{2}) ".format(cutOffValue, RT_Cap, tmpRTrunning)
                ngMsg += "criteria=-cap_rt"
            elif (
                nRunning + nActivated + nAssigned + nStarting + nDefined == 0
                and taskSpec.currentPriority <= self.max_prio_for_bootstrap
                and not inputChunk.isMerging
                and not inputChunk.isExpress()
            ):
                okMsg = f"  use site={tmpPseudoSiteName} to bootstrap (no running or queued jobs) criteria=+use"
                ngMsg = f"  skip site={tmpPseudoSiteName} as others being bootstrapped (no running or queued jobs), "
                ngMsg += f"weight={weight} {weightStr} "
                ngMsg += "criteria=-others_bootstrap"
                okAsPrimay = True
                # set weight to 0 for subsequent processing
                weight = 0
                siteCandidateSpec.weight = weight
            else:
                if min_weight > 0 and weight < min_weight:
                    ngMsg = f"  skip site={tmpPseudoSiteName} due to weight below the minimum {min_weight_param}={min_weight}, "
                    ngMsg += f"weight={weight} {weightStr} "
                    ngMsg += "criteria=-below_min_weight"
                elif useT1Weight:
                    ngMsg = f"  skip site={tmpPseudoSiteName} due to low total "
                    ngMsg += f"nRunningAll={nRunningAll} for negative T1 weight "
                    ngMsg += "criteria=-t1_weight"
                    if not largestNumRun or largestNumRun[-1] < nRunningAll:
                        largestNumRun = (tmpPseudoSiteName, nRunningAll)
                        okAsPrimay = True
                        # copy primaries to secondary map
                        for tmpWeight, tmpCandidates in weightMapPrimary.items():
                            weightMapSecondary.setdefault(tmpWeight, [])
                            weightMapSecondary[tmpWeight] += tmpCandidates
                        weightMapPrimary = {}
                else:
                    ngMsg = f"  skip site={tmpPseudoSiteName} due to low weight, "
                    ngMsg += f"weight={weight} {weightStr} "
                    ngMsg += "criteria=-low_weight"
                    okAsPrimay = True
            # add to jumbo or primary or secondary
            if forJumbo:
                # only OK sites for jumbo
                if not okAsPrimay:
                    continue
                weightMap = weightMapJumbo
            elif okAsPrimay:
                weightMap = weightMapPrimary
            else:
                weightMap = weightMapSecondary
            # add weight
            if weight not in weightMap:
                weightMap[weight] = []
            weightMap[weight].append((siteCandidateSpec, okMsg, ngMsg))
        # use only primary candidates
        weightMap = weightMapPrimary
        # use all weights
        weightRank = None
        # dump NG message
        for tmpWeight in weightMapSecondary.keys():
            for siteCandidateSpec, tmpOkMsg, tmpNgMsg in weightMapSecondary[tmpWeight]:
                tmpLog.info(tmpNgMsg)
        if weightMapPrimary == {}:
            tmpLog.info("available sites all capped")
        # add jumbo sites
        for weight, tmpList in weightMapJumbo.items():
            if weight not in weightMap:
                weightMap[weight] = []
            for tmpItem in tmpList:
                weightMap[weight].append(tmpItem)
        # max candidates for WORLD
        maxSiteCandidates = 10
        newScanSiteList = []
        weightList = sorted(weightMap.keys())
        weightList.reverse()
        # put 0 at the head of the list to give priorities bootstrap PQs
        if 0 in weightList:
            weightList.remove(0)
            weightList.insert(0, 0)
        for weightIdx, tmpWeight in enumerate(weightList):
            for siteCandidateSpec, tmpOkMsg, tmpNgMsg in weightMap[tmpWeight]:
                # candidates for jumbo jobs
                if taskSpec.getNumJumboJobs() is not None:
                    tmpSiteSpec = self.siteMapper.getSite(siteCandidateSpec.siteName)
                    if tmpSiteSpec.useJumboJobs():
                        # use site for jumbo jobs
                        tmpLog.info(tmpOkMsg)
                        inputChunk.addSiteCandidateForJumbo(siteCandidateSpec)
                        if inputChunk.useJumbo not in ["fake", "only"]:
                            continue
                # candidates for normal jobs
                if (weightRank is None or weightIdx < weightRank) and (maxSiteCandidates is None or len(newScanSiteList) < maxSiteCandidates):
                    # use site
                    tmpLog.info(tmpOkMsg)
                    newScanSiteList.append(siteCandidateSpec.siteName)
                    inputChunk.addSiteCandidate(siteCandidateSpec)
                else:
                    # dump NG message
                    tmpLog.info(tmpNgMsg)
        oldScanSiteList = copy.copy(scanSiteList)
        scanSiteList = newScanSiteList
        self.add_summary_message(oldScanSiteList, scanSiteList, "final check")
        # final check
        if scanSiteList == []:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        self.dump_summary(tmpLog, scanSiteList)
        # return
        tmpLog.info("done")
        return self.SC_SUCCEEDED, inputChunk
