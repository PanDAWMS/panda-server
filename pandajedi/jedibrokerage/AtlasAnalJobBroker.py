import copy
import datetime
import math
import random
import re
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import JobUtils

from . import AtlasBrokerUtils
from .JobBrokerBase import JobBrokerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])

APP = "jedi"
COMPONENT = "jobbroker"
VO = "atlas"


# brokerage for ATLAS analysis
class AtlasAnalJobBroker(JobBrokerBase):
    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        JobBrokerBase.__init__(self, ddmIF, taskBufferIF)
        self.dataSiteMap = {}

        # load the SW availability map
        try:
            self.sw_map = taskBufferIF.load_sw_map()
        except Exception:
            logger.error("Failed to load the SW tags map!!!")
            self.sw_map = {}

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap):
        # make logger
        if inputChunk.masterDataset:
            msg_tag = f"<jediTaskID={taskSpec.jediTaskID} datasetID={inputChunk.masterDataset.datasetID}>"
        else:
            msg_tag = f"<jediTaskID={taskSpec.jediTaskID}>"
        tmpLog = MsgWrapper(logger, msg_tag, monToken=f"<jediTaskID={taskSpec.jediTaskID} {naive_utcnow().isoformat('/')}>")
        tmpLog.debug("start")
        # return for failure
        retFatal = self.SC_FATAL, inputChunk
        retTmpError = self.SC_FAILED, inputChunk
        # new maxwdir
        newMaxwdir = {}
        # get primary site candidates
        sitePreAssigned = False
        siteListPreAssigned = False
        excludeList = []
        includeList = None
        scanSiteList = []
        # problematic sites
        problematic_sites_dict = {}
        # not to use VP replicas for merging, scouts, and forceStaged
        if inputChunk.isMerging or taskSpec.avoid_vp() or taskSpec.useScout() or taskSpec.useLocalIO():
            useVP = False
        else:
            useVP = True
        # avoid VP queues for merging
        avoidVP = False
        if inputChunk.isMerging:
            avoidVP = True
        # get workQueue
        workQueue = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)

        # site limitation
        if taskSpec.useLimitedSites():
            if "excludedSite" in taskParamMap:
                excludeList = taskParamMap["excludedSite"]
                # str to list for task retry
                try:
                    if not isinstance(excludeList, list):
                        excludeList = excludeList.split(",")
                except Exception:
                    pass
            if "includedSite" in taskParamMap:
                includeList = taskParamMap["includedSite"]
                # str to list for task retry
                if includeList == "":
                    includeList = None
                try:
                    if not isinstance(includeList, list):
                        includeList = includeList.split(",")
                    siteListPreAssigned = True
                except Exception:
                    pass
        # loop over all sites
        for siteName, tmpSiteSpec in self.siteMapper.siteSpecList.items():
            if tmpSiteSpec.type == "analysis" or tmpSiteSpec.is_grandly_unified():
                scanSiteList.append(siteName)
        # preassigned
        preassignedSite = taskSpec.site
        if preassignedSite not in ["", None]:
            # site is pre-assigned
            if not self.siteMapper.checkSite(preassignedSite):
                # check ddm for unknown site
                includeList = []
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
                    if scope_input in tmpSiteSpec.ddm_endpoints_input and preassignedSite in tmpSiteSpec.ddm_endpoints_input[scope_input].all:
                        includeList.append(tmpSiteName)
                if not includeList:
                    includeList = None
                    tmpLog.info(f"site={preassignedSite} is ignored since unknown")
                else:
                    tmpLog.info(f"site={preassignedSite} is converted to {','.join(includeList)}")
                preassignedSite = None
            else:
                tmpLog.info(f"site={preassignedSite} is pre-assigned")
                sitePreAssigned = True
                if preassignedSite not in scanSiteList:
                    scanSiteList.append(preassignedSite)
        tmpLog.info(f"initial {len(scanSiteList)} candidates")
        # allowed remote access protocol
        allowedRemoteProtocol = "fax"
        # MP
        if taskSpec.coreCount is not None and taskSpec.coreCount > 1:
            # use MCORE only
            useMP = "only"
        elif taskSpec.coreCount == 0:
            # use MCORE and normal
            useMP = "any"
        else:
            # not use MCORE
            useMP = "unuse"
        # get statistics of failures
        timeWindowForFC = self.taskBufferIF.getConfigValue("anal_jobbroker", "TW_DONE_JOB_STAT", "jedi", taskSpec.vo)
        if timeWindowForFC is None:
            timeWindowForFC = 6

        # get minimum bad jobs to skip PQ
        minBadJobsToSkipPQ = self.taskBufferIF.getConfigValue("anal_jobbroker", "MIN_BAD_JOBS_TO_SKIP_PQ", "jedi", taskSpec.vo)
        if minBadJobsToSkipPQ is None:
            minBadJobsToSkipPQ = 5

        # get total job stat
        totalJobStat = self.get_task_common("totalJobStat")
        if totalJobStat is None:
            if taskSpec.workingGroup:
                totalJobStat = self.taskBufferIF.countJobsPerTarget_JEDI(taskSpec.workingGroup, False)
            else:
                totalJobStat = self.taskBufferIF.countJobsPerTarget_JEDI(taskSpec.origUserName, True)
            self.set_task_common("totalJobStat", totalJobStat)
        # check total to cap
        if totalJobStat:
            if taskSpec.workingGroup:
                gdp_token_jobs = "CAP_RUNNING_GROUP_JOBS"
                gdp_token_cores = "CAP_RUNNING_GROUP_CORES"
            else:
                gdp_token_jobs = "CAP_RUNNING_USER_JOBS"
                gdp_token_cores = "CAP_RUNNING_USER_CORES"
            maxNumRunJobs = self.taskBufferIF.getConfigValue("prio_mgr", gdp_token_jobs)
            maxNumRunCores = self.taskBufferIF.getConfigValue("prio_mgr", gdp_token_cores)
            maxFactor = 2

            if maxNumRunJobs:
                if totalJobStat["nRunJobs"] > maxNumRunJobs:
                    tmpLog.error(f"throttle to generate jobs due to too many running jobs {totalJobStat['nRunJobs']} > {gdp_token_jobs}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                elif totalJobStat["nQueuedJobs"] > maxFactor * maxNumRunJobs:
                    tmpLog.error(f"throttle to generate jobs due to too many queued jobs {totalJobStat['nQueuedJobs']} > {maxFactor}x{gdp_token_jobs}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
            if maxNumRunCores:
                if totalJobStat["nRunCores"] > maxNumRunCores:
                    tmpLog.error(f"throttle to generate jobs due to too many running cores {totalJobStat['nRunCores']} > {gdp_token_cores}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                elif totalJobStat["nQueuedCores"] > maxFactor * maxNumRunCores:
                    tmpLog.error(f"throttle to generate jobs due to too many queued cores {totalJobStat['nQueuedCores']} > {maxFactor}x{gdp_token_cores}")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        # get gshare usage
        ret_val, gshare_usage_dict = AtlasBrokerUtils.getGShareUsage(tbIF=self.taskBufferIF, gshare=taskSpec.gshare)
        if not ret_val:
            tmpLog.warning(f"failed to get gshare usage of {taskSpec.gshare}")
        elif not gshare_usage_dict:
            tmpLog.warning(f"got empty gshare usage of {taskSpec.gshare}")

        # get L1 share usage
        l1_share_usage_dict = None
        if taskSpec.gshare == "User Analysis":
            l1_share_name = "L1 User Analysis"
            ret_val, l1_share_usage_dict = AtlasBrokerUtils.getGShareUsage(tbIF=self.taskBufferIF, gshare=l1_share_name)
            if not ret_val:
                tmpLog.warning(f"failed to get gshare usage of {l1_share_name}")
            elif not l1_share_usage_dict:
                tmpLog.warning(f"got empty gshare usage of {l1_share_name}")

        # get analy sites classification
        ret_val, analy_sites_class_dict = AtlasBrokerUtils.getAnalySitesClass(tbIF=self.taskBufferIF)
        if not ret_val:
            tmpLog.warning("failed to get analy sites classification")
        elif not analy_sites_class_dict:
            analy_sites_class_dict = {}
            tmpLog.warning("got empty analy sites classification")

        # get user usage
        ret_val, user_eval_dict = AtlasBrokerUtils.getUserEval(tbIF=self.taskBufferIF, user=taskSpec.origUserName)
        if not ret_val:
            tmpLog.warning(f"failed to get user evaluation of {taskSpec.origUserName}")
        elif not user_eval_dict:
            tmpLog.warning(f"got empty user evaluation of {taskSpec.origUserName}")

        # task evaluation
        ret_val, task_eval_dict = AtlasBrokerUtils.getUserTaskEval(tbIF=self.taskBufferIF, taskID=taskSpec.jediTaskID)
        if not ret_val:
            tmpLog.warning("failed to get user task evaluation")
        elif not task_eval_dict:
            tmpLog.warning("got empty user task evaluation")

        # parameters about User Analysis threshold
        threshold_A = self.taskBufferIF.getConfigValue("analy_eval", "USER_USAGE_THRESHOLD_A", "pandaserver", taskSpec.vo)
        if threshold_A is None:
            threshold_A = 1000
        threshold_B = self.taskBufferIF.getConfigValue("analy_eval", "USER_USAGE_THRESHOLD_B", "pandaserver", taskSpec.vo)
        if threshold_B is None:
            threshold_B = 10000
        user_analyis_to_throttle_threshold_perc_A = 100
        user_analyis_to_throttle_threshold_perc_B = min(95, user_analyis_to_throttle_threshold_perc_A)
        user_analyis_to_throttle_threshold_perc_C = min(90, user_analyis_to_throttle_threshold_perc_B)
        user_analyis_throttle_intensity_A = 1.0

        # parameters about Analysis Stabilizer
        base_queue_length_per_pq = self.taskBufferIF.getConfigValue("anal_jobbroker", "BASE_QUEUE_LENGTH_PER_PQ", "jedi", taskSpec.vo)
        if base_queue_length_per_pq is None:
            base_queue_length_per_pq = 100
        base_expected_wait_hour_on_pq = self.taskBufferIF.getConfigValue("anal_jobbroker", "BASE_EXPECTED_WAIT_HOUR_ON_PQ", "jedi", taskSpec.vo)
        if base_expected_wait_hour_on_pq is None:
            base_expected_wait_hour_on_pq = 8
        base_default_queue_length_per_pq_user = self.taskBufferIF.getConfigValue("anal_jobbroker", "BASE_DEFAULT_QUEUE_LENGTH_PER_PQ_USER", "jedi", taskSpec.vo)
        if base_default_queue_length_per_pq_user is None:
            base_default_queue_length_per_pq_user = 5
        base_queue_ratio_on_pq = self.taskBufferIF.getConfigValue("anal_jobbroker", "BASE_QUEUE_RATIO_ON_PQ", "jedi", taskSpec.vo)
        if base_queue_ratio_on_pq is None:
            base_queue_ratio_on_pq = 0.05
        static_max_queue_running_ratio = self.taskBufferIF.getConfigValue("anal_jobbroker", "STATIC_MAX_QUEUE_RUNNING_RATIO", "jedi", taskSpec.vo)
        if static_max_queue_running_ratio is None:
            static_max_queue_running_ratio = 2.0
        max_expected_wait_hour = self.taskBufferIF.getConfigValue("anal_jobbroker", "MAX_EXPECTED_WAIT_HOUR", "jedi", taskSpec.vo)
        if max_expected_wait_hour is None:
            max_expected_wait_hour = 12.0

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

        # throttle User Analysis tasks when close to gshare target
        if taskSpec.gshare in ["User Analysis"] and gshare_usage_dict and task_eval_dict:
            try:
                usage_percent = gshare_usage_dict["usage_perc"] * 100
                if l1_share_usage_dict and l1_share_usage_dict.get("eqiv_usage_perc") is not None:
                    usage_percent = min(usage_percent, l1_share_usage_dict["eqiv_usage_perc"] * 100)
                task_class_value = task_eval_dict["class"]
                usage_slot_ratio_A = 0.5
                if user_eval_dict:
                    usage_slot_ratio_A = 1.0 - user_eval_dict["rem_slots_A"] / threshold_A

                if task_class_value == -1 and usage_percent > user_analyis_to_throttle_threshold_perc_C:
                    # C-tasks to throttle
                    tmpLog.error(
                        "throttle to generate jobs due to gshare {gshare} > {threshold}% of target and task in class C".format(
                            gshare=taskSpec.gshare, threshold=user_analyis_to_throttle_threshold_perc_C
                        )
                    )
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                elif task_class_value == 0 and usage_percent > user_analyis_to_throttle_threshold_perc_B:
                    # B-tasks to throttle
                    tmpLog.error(
                        "throttle to generate jobs due to gshare {gshare} > {threshold}% of target and task in class B".format(
                            gshare=taskSpec.gshare, threshold=user_analyis_to_throttle_threshold_perc_B
                        )
                    )
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                elif (
                    task_class_value == 1 and usage_percent * usage_slot_ratio_A * user_analyis_throttle_intensity_A > user_analyis_to_throttle_threshold_perc_A
                ):
                    # A-tasks to throttle
                    tmpLog.error(
                        "throttle to generate jobs due to gshare {gshare} > {threshold:.3%} of target and task in class A".format(
                            gshare=taskSpec.gshare, threshold=user_analyis_throttle_intensity_A / (usage_slot_ratio_A + 2**-20)
                        )
                    )
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
            except Exception as e:
                tmpLog.error(f"got error when checking low-ranked tasks to throttle; skipped : {e}")

        # check global disk quota
        if taskSpec.workingGroup:
            global_quota_ok, near_global_limit, quota_msg = self.ddmIF.check_global_quota(taskSpec.workingGroup)
            local_quota_ok, endpoints_over_local_quota = self.ddmIF.get_endpoints_over_local_quota(taskSpec.workingGroup)
        else:
            global_quota_ok, near_global_limit, quota_msg = self.ddmIF.check_global_quota(taskSpec.userName)
            local_quota_ok, endpoints_over_local_quota = self.ddmIF.get_endpoints_over_local_quota(taskSpec.userName)

        # over global quota
        if not global_quota_ok:
            tmpLog.error(f"throttle to generate jobs due to {quota_msg}")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        # close to global quota
        if near_global_limit and not inputChunk.isMerging:
            tmpLog.error(f"throttle to generate only merge jobs due to {quota_msg}")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        # cannot get local quota
        if not local_quota_ok:
            tmpLog.error(f"failed to check local quota")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        # get failure count
        failureCounts = self.get_task_common("failureCounts")
        if failureCounts is None:
            failureCounts = self.taskBufferIF.getFailureCountsForTask_JEDI(taskSpec.jediTaskID, timeWindowForFC)
            self.set_task_common("failureCounts", failureCounts)

        # IO intensity cutoff in kB/sec to allow input transfers
        io_intensity_key = "IO_INTENSITY_CUTOFF_USER"
        io_intensity_cutoff = self.taskBufferIF.getConfigValue("anal_jobbroker", io_intensity_key, "jedi", taskSpec.vo)

        # timelimit for data locality check
        loc_check_timeout_key = "DATA_CHECK_TIMEOUT_USER"
        loc_check_timeout_val = self.taskBufferIF.getConfigValue("anal_jobbroker", loc_check_timeout_key, "jedi", taskSpec.vo)

        # check input datasets
        element_map = dict()
        ddsList = set()
        complete_disk_ok = {}
        complete_tape_ok = {}
        true_complete_disk_ok = {}
        can_be_local_source = {}
        can_be_remote_source = {}
        list_of_complete_replica_locations = {}
        if inputChunk.getDatasets():
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                isDistributed = None
                if datasetName not in self.dataSiteMap:
                    # get the list of sites where data is available
                    tmpLog.debug(f"getting the list of sites where {datasetName} is available")
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
                        self.get_unified_sites(scanSiteList),
                        self.siteMapper,
                        self.ddmIF,
                        datasetName,
                        element_map.get(datasetSpec.datasetName),
                        max_missing_input_files,
                        min_input_completeness,
                    )
                    if tmpSt in [Interaction.JEDITemporaryError, Interaction.JEDITimeoutError]:
                        tmpLog.error(f"temporary failed to get the list of sites where data is available, since {tmpRet}")
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retTmpError
                    if tmpSt == Interaction.JEDIFatalError:
                        tmpLog.error(f"fatal error when getting the list of sites where data is available, since {tmpRet}")
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retFatal
                    # append
                    self.dataSiteMap[datasetName] = tmpRet
                    complete_disk_ok[datasetName] = tmp_complete_disk_ok
                    complete_tape_ok[datasetName] = tmp_complete_tape_ok
                    true_complete_disk_ok[datasetName] = tmp_truly_complete_disk
                    can_be_local_source[datasetName] = tmp_can_be_local_source
                    can_be_remote_source[datasetName] = tmp_can_be_remote_source
                    list_of_complete_replica_locations[datasetName] = tmp_list_of_complete_replica_locations
                    if datasetName.startswith("ddo"):
                        tmpLog.debug(f" {len(tmpRet)} sites")
                    else:
                        tmpLog.debug(f" {len(tmpRet)} sites : {str(tmpRet)}")
                        # check if distributed
                        if tmpRet != {}:
                            isDistributed = True
                            for tmpMap in tmpRet.values():
                                for tmpVal in tmpMap.values():
                                    if tmpVal["state"] == "complete":
                                        isDistributed = False
                                        break
                                if not isDistributed:
                                    break
                            if isDistributed or datasetName.endswith("/"):
                                # check if really distributed
                                isDistributed = self.ddmIF.isDistributedDataset(datasetName)
                                if isDistributed or datasetName.endswith("/"):
                                    isDistributed = True
                                    tmpLog.debug(f" {datasetName} is distributed")
                                    ddsList.add(datasetName)
                                    # disable VP since distributed datasets triggers transfers
                                    useVP = False
                                    avoidVP = True
                tmp_rse_list = ",".join(list_of_complete_replica_locations[datasetName])
                tmpLog.debug(
                    f"replica_availability disk:{complete_disk_ok[datasetName]} tape:{complete_tape_ok[datasetName]}, is_distributed:{isDistributed}, remote_readable:{can_be_remote_source[datasetName]}, rses={tmp_rse_list}"
                )
                # check if the data is available at somewhere
                if not complete_disk_ok[datasetName] and not complete_tape_ok[datasetName] and isDistributed is not True:
                    err_msg = f"{datasetName} is "
                    if list_of_complete_replica_locations[datasetName]:
                        tmp_is_single = len(list_of_complete_replica_locations[datasetName]) == 1
                        err_msg += f"only complete at {tmp_rse_list} which "
                        err_msg += "is " if tmp_is_single else "are "
                        err_msg += "currently in downtime or offline. "
                    else:
                        err_msg += "incomplete at online storage. "
                    if not taskSpec.allow_incomplete_input():
                        tmpLog.error(err_msg)
                        taskSpec.setErrDiag(err_msg)
                        retVal = retTmpError
                        return retVal
                    else:
                        err_msg += "However, the task allows incomplete input."
                        tmpLog.info(err_msg)

        # check if any input dataset is remotely unavailable
        remote_source_available = True
        remote_source_msg = ""
        for tmp_dataset_name, tmp_ok in can_be_remote_source.items():
            if not tmp_ok:
                remote_source_msg = f"data locality cannot be ignored since {tmp_dataset_name} is unreadable over WAN"
                remote_source_available = False
                break

        # two loops with/without data locality check
        scan_site_list_loops = [(copy.copy(scanSiteList), True)]
        to_ignore_data_loc = False
        if len(inputChunk.getDatasets()) > 0:
            nRealDS = 0
            for datasetSpec in inputChunk.getDatasets():
                if not datasetSpec.isPseudo():
                    nRealDS += 1
            task_prio_cutoff_for_input_data_motion = 2000
            tmp_msg = "ignoring input data locality in the second loop due to "
            if taskSpec.taskPriority >= task_prio_cutoff_for_input_data_motion:
                to_ignore_data_loc = True
                tmp_msg += f"high taskPriority {taskSpec.taskPriority} is larger than or equal to {task_prio_cutoff_for_input_data_motion}"
            elif io_intensity_cutoff and taskSpec.ioIntensity and io_intensity_cutoff >= taskSpec.ioIntensity:
                to_ignore_data_loc = True
                tmp_msg += f"low ioIntensity {taskSpec.ioIntensity} is less than or equal to {io_intensity_key} ({io_intensity_cutoff})"
            elif loc_check_timeout_val and taskSpec.frozenTime and naive_utcnow() - taskSpec.frozenTime > datetime.timedelta(hours=loc_check_timeout_val):
                to_ignore_data_loc = True
                tmp_msg += "check timeout (last successful cycle at {} was more than {} ({}hrs) ago)".format(
                    taskSpec.frozenTime, loc_check_timeout_key, loc_check_timeout_val
                )
            if not remote_source_available:
                tmpLog.info(remote_source_msg)
            elif to_ignore_data_loc:
                tmpLog.info(tmp_msg)
                scan_site_list_loops.append((copy.copy(scanSiteList), False))
            elif taskSpec.taskPriority > 1000 or nRealDS > 1 or taskSpec.getNumSitesPerJob() > 0:
                # add a loop without data locality check for high priority tasks, tasks with multiple input datasets, or tasks with job cloning
                scan_site_list_loops.append((copy.copy(scanSiteList), False))
            # element map
            for datasetSpec in inputChunk.getDatasets():
                if datasetSpec.datasetName.endswith("/"):
                    file_list = [f.lfn for f in datasetSpec.Files]
                    element_map[datasetSpec.datasetName] = self.taskBufferIF.get_origin_datasets(taskSpec.jediTaskID, datasetSpec.datasetName, file_list)

        retVal = None
        checkDataLocality = False
        scanSiteWoVP = []
        summaryList = []
        site_list_with_data = None
        overall_site_list = set()
        for i_loop, (scanSiteList, checkDataLocality) in enumerate(scan_site_list_loops):
            useUnionLocality = False
            self.init_summary_list("Job brokerage summary", f"data locality check: {checkDataLocality}", scanSiteList)
            if checkDataLocality:
                tmpLog.debug("!!! look for candidates WITH data locality check")
            else:
                tmpLog.debug("!!! look for candidates WITHOUT data locality check")
            ######################################
            # selection for data availability
            hasDDS = False
            dataWeight = {}
            remoteSourceList = {}
            sites_in_nucleus = []
            for datasetSpec in inputChunk.getDatasets():
                datasetSpec.reset_distributed()

            if inputChunk.getDatasets() != [] and checkDataLocality:
                oldScanSiteList = copy.copy(scanSiteList)
                oldScanUnifiedSiteList = self.get_unified_sites(oldScanSiteList)

                if ddsList:
                    hasDDS = True

                for datasetSpec in inputChunk.getDatasets():
                    datasetName = datasetSpec.datasetName
                    if datasetName in ddsList:
                        datasetSpec.setDistributed()

                # get the list of sites where data is available
                scanSiteList = None
                scanSiteListOnDisk = None
                scanSiteListUnion = None
                scanSiteListOnDiskUnion = None
                scanSiteWoVpUnion = None

                for datasetName, tmpDataSite in self.dataSiteMap.items():
                    # check if incomplete replica is allowed
                    if datasetName in ddsList:
                        useIncomplete = True
                    elif true_complete_disk_ok.get(datasetName) is False:
                        useIncomplete = True
                    else:
                        useIncomplete = False
                    # get sites where replica is available
                    tmpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=True, use_incomplete=useIncomplete)
                    tmpDiskSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=False, use_vp=useVP, use_incomplete=useIncomplete)
                    tmpNonVpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=True, use_vp=False, use_incomplete=useIncomplete)
                    # make weight map for local
                    for tmpSiteName in tmpSiteList:
                        if tmpSiteName not in dataWeight:
                            dataWeight[tmpSiteName] = 0
                        # give more weight to disk
                        if tmpSiteName in tmpDiskSiteList:
                            dataWeight[tmpSiteName] += 1
                        else:
                            dataWeight[tmpSiteName] += 0.001

                    # first list
                    if scanSiteList is None:
                        scanSiteList = []
                        for tmpSiteName in tmpSiteList:
                            if tmpSiteName not in oldScanUnifiedSiteList:
                                continue
                            if tmpSiteName not in scanSiteList:
                                scanSiteList.append(tmpSiteName)
                        scanSiteListOnDisk = set()
                        for tmpSiteName in tmpDiskSiteList:
                            if tmpSiteName not in oldScanUnifiedSiteList:
                                continue
                            scanSiteListOnDisk.add(tmpSiteName)
                        scanSiteWoVP = tmpNonVpSiteList
                        scanSiteListUnion = set(scanSiteList)
                        scanSiteListOnDiskUnion = set(scanSiteListOnDisk)
                        scanSiteWoVpUnion = set(scanSiteWoVP)
                    else:
                        # pickup sites which have all data
                        newScanList = []
                        for tmpSiteName in tmpSiteList:
                            if tmpSiteName in scanSiteList and tmpSiteName not in newScanList:
                                newScanList.append(tmpSiteName)
                            scanSiteListUnion.add(tmpSiteName)
                        scanSiteList = newScanList
                        tmpLog.debug(f"{datasetName} is available at {len(scanSiteList)} sites")
                        # pickup sites which have all data on DISK
                        newScanListOnDisk = set()
                        for tmpSiteName in tmpDiskSiteList:
                            if tmpSiteName in scanSiteListOnDisk:
                                newScanListOnDisk.add(tmpSiteName)
                            scanSiteListOnDiskUnion.add(tmpSiteName)
                        scanSiteListOnDisk = newScanListOnDisk
                        # get common elements
                        scanSiteWoVP = list(set(scanSiteWoVP).intersection(tmpNonVpSiteList))
                        scanSiteWoVpUnion = scanSiteWoVpUnion.union(tmpNonVpSiteList)
                    tmpLog.debug(
                        f"{datasetName} is available at {len(scanSiteList)} sites. complete disk replica: {true_complete_disk_ok[datasetName]}, complete tape replica: {complete_tape_ok[datasetName]}"
                    )
                    tmpLog.debug(f"{datasetName} is available at {len(scanSiteListOnDisk)} sites on DISK")
                # check for preassigned
                if sitePreAssigned:
                    if preassignedSite not in scanSiteList and preassignedSite not in scanSiteListUnion:
                        scanSiteList = []
                        tmpLog.info(f"data is unavailable locally or remotely at preassigned site {preassignedSite}")
                    elif preassignedSite not in scanSiteList:
                        scanSiteList = list(scanSiteListUnion)
                elif len(scanSiteListOnDisk) > 0:
                    # use only disk sites
                    scanSiteList = list(scanSiteListOnDisk)
                elif not scanSiteList and scanSiteListUnion:
                    tmpLog.info("use union list for data locality check since no site has all data")
                    if scanSiteListOnDiskUnion:
                        scanSiteList = list(scanSiteListOnDiskUnion)
                    elif scanSiteListUnion:
                        scanSiteList = list(scanSiteListUnion)
                    scanSiteWoVP = list(scanSiteWoVpUnion)
                    useUnionLocality = True
                    # disable VP since union locality triggers transfers to VP
                    avoidVP = True
                scanSiteList = self.get_pseudo_sites(scanSiteList, oldScanSiteList)
                # dump
                for tmpSiteName in oldScanSiteList:
                    if tmpSiteName not in scanSiteList:
                        pass
                sites_in_nucleus = copy.copy(scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates have input data")
                self.add_summary_message(oldScanSiteList, scanSiteList, "input data check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue

            ######################################
            # selection for status
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # skip unified queues
                if tmpSiteSpec.is_unified:
                    continue
                # check site status
                skipFlag = False
                if tmpSiteSpec.status in ["offline"]:
                    skipFlag = True
                elif tmpSiteSpec.status in ["brokeroff", "test"]:
                    if siteListPreAssigned:
                        pass
                    elif not sitePreAssigned:
                        skipFlag = True
                    elif preassignedSite not in [tmpSiteName, tmpSiteSpec.get_unified_name()]:
                        skipFlag = True
                if not skipFlag:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info(f"  skip site={tmpSiteName} due to status={tmpSiteSpec.status} criteria=-status")
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed site status check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "status check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue

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
            for tmpSiteName in unified_site_list:
                tmp_site_spec = self.siteMapper.getSite(tmpSiteName)

                # measured diskIO at queue
                diskio_usage_tmp = diskio_percore_usage.get(tmpSiteName, 0)

                # figure out queue or default limit
                if tmp_site_spec.maxDiskio and tmp_site_spec.maxDiskio > 0:
                    # there is a limit specified in AGIS
                    diskio_limit_tmp = tmp_site_spec.maxDiskio
                else:
                    # we need to use the default value from GDP Config
                    diskio_limit_tmp = max_diskio_per_core_default

                # normalize task diskIO by site corecount
                diskio_task_tmp = taskSpec.diskIO
                if taskSpec.diskIO is not None and taskSpec.coreCount not in [None, 0, 1] and tmp_site_spec.coreCount not in [None, 0]:
                    diskio_task_tmp = taskSpec.diskIO / tmp_site_spec.coreCount

                try:  # generate a log message parseable by logstash for monitoring
                    log_msg = f"diskIO measurements: site={tmpSiteName} jediTaskID={taskSpec.jediTaskID} "
                    if diskio_task_tmp is not None:
                        log_msg += f"diskIO_task={diskio_task_tmp:.2f} "
                    if diskio_usage_tmp is not None:
                        log_msg += f"diskIO_site_usage={diskio_usage_tmp:.2f} "
                    if diskio_limit_tmp is not None:
                        log_msg += f"diskIO_site_limit={diskio_limit_tmp:.2f} "
                    # tmpLog.info(log_msg)
                except Exception:
                    tmpLog.debug("diskIO measurements: Error generating diskIO message")

                # if the task has a diskIO defined, the queue is over the IO limit and the task IO is over the limit
                if diskio_task_tmp and diskio_usage_tmp and diskio_limit_tmp and diskio_usage_tmp > diskio_limit_tmp and diskio_task_tmp > diskio_limit_tmp:
                    tmpLog.info(f"  skip site={tmpSiteName} due to diskIO overload criteria=-diskIO")
                    continue

                newScanSiteList.append(tmpSiteName)

            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)

            tmpLog.info(f"{len(scanSiteList)} candidates passed diskIO check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "diskIO check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            ######################################
            # selection for VP
            if taskSpec.avoid_vp() or avoidVP or not checkDataLocality:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if not tmpSiteSpec.use_vp(JobUtils.ANALY_PS):
                        newScanSiteList.append(tmpSiteName)
                    else:
                        tmpLog.info(f"  skip site={tmpSiteName} to avoid VP")
                scanSiteList = newScanSiteList
                tmpLog.info(f"{len(scanSiteList)} candidates passed for avoidVP")
                self.add_summary_message(oldScanSiteList, scanSiteList, "avoid VP queue check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # selection for MP
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
                    tmpLog.info(
                        "  skip site=%s due to core mismatch cores_site=%s <> cores_task=%s criteria=-cpucore"
                        % (tmpSiteName, tmpSiteSpec.coreCount, taskSpec.coreCount)
                    )
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed for useMP={useMP}")
            self.add_summary_message(oldScanSiteList, scanSiteList, "CPU core check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # selection for GPU + architecture
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            jsonCheck = AtlasBrokerUtils.JsonSoftwareCheck(self.siteMapper, self.sw_map)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.isGPU() and not taskSpec.is_hpo_workflow():
                    if taskSpec.get_sw_platform() in ["", None]:
                        tmpLog.info(f"  skip site={tmpSiteName} since architecture is required for GPU queues")
                        continue
                    siteListWithCMTCONFIG = [tmpSiteSpec.get_unified_name()]
                    siteListWithCMTCONFIG, sitesNoJsonCheck = jsonCheck.check(siteListWithCMTCONFIG, None, None, None, taskSpec.get_sw_platform(), False, True)

                    if len(siteListWithCMTCONFIG) == 0:
                        tmpLog.info(f"  skip site={tmpSiteName} since architecture={taskSpec.get_sw_platform()} is unavailable")
                        continue

                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info(f"{len(scanSiteList)} candidates passed for architecture check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "architecture check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # selection for closed
            if not sitePreAssigned and not inputChunk.isMerging:
                oldScanSiteList = copy.copy(scanSiteList)
                newScanSiteList = []
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    if tmpSiteName in failureCounts and "closed" in failureCounts[tmpSiteName]:
                        nClosed = failureCounts[tmpSiteName]["closed"]
                        if nClosed > 0:
                            tmpLog.info(f"  skip site={tmpSiteName} due to n_closed={nClosed} criteria=-closed")
                            continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates passed for closed")
                self.add_summary_message(oldScanSiteList, scanSiteList, "too many closed check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # selection for release
            cmt_config = taskSpec.get_sw_platform()
            is_regexp_cmt_config = False
            if cmt_config:
                if re.match(cmt_config, cmt_config) is None:
                    is_regexp_cmt_config = True
            base_platform = taskSpec.get_base_platform()
            resolved_platforms = {}
            host_cpu_spec = taskSpec.get_host_cpu_spec()
            host_gpu_spec = taskSpec.get_host_gpu_spec()
            if not sitePreAssigned:
                unified_site_list = self.get_unified_sites(scanSiteList)
                if taskSpec.transHome is not None:
                    transHome = taskSpec.transHome
                else:
                    transHome = ""
                # remove AnalysisTransforms-
                transHome = re.sub("^[^-]+-*", "", transHome)
                transHome = re.sub("_", "-", transHome)
                if (
                    re.search("rel_\d+(\n|$)", transHome) is None
                    and taskSpec.transHome not in ["AnalysisTransforms", None]
                    and re.search("\d{4}-\d{2}-\d{2}T\d{4}$", transHome) is None
                    and re.search("-\d+\.\d+\.\d+$", transHome) is None
                ):
                    # cache is checked
                    siteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                        unified_site_list,
                        "atlas",
                        transHome.split("-")[0],
                        transHome.split("-")[1],
                        taskSpec.get_sw_platform(),
                        False,
                        False,
                        container_name=taskSpec.container_name,
                        only_tags_fc=taskSpec.use_only_tags_fc(),
                        host_cpu_specs=host_cpu_spec,
                        host_gpu_spec=host_gpu_spec,
                        log_stream=tmpLog,
                    )
                    sitesAuto = copy.copy(siteListWithSW)

                elif (transHome == "" and taskSpec.transUses is not None) or (
                    re.search("-\d+\.\d+\.\d+$", transHome) is not None and (taskSpec.transUses is None or re.search("-\d+\.\d+$", taskSpec.transUses) is None)
                ):
                    siteListWithSW = []
                    sitesNoJsonCheck = unified_site_list
                    # remove Atlas-
                    if taskSpec.transUses is not None:
                        transUses = taskSpec.transUses.split("-")[-1]
                    else:
                        transUses = None
                    if transUses is not None:
                        # release is checked
                        tmpSiteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                            unified_site_list,
                            "atlas",
                            "AtlasOffline",
                            transUses,
                            taskSpec.get_sw_platform(),
                            False,
                            False,
                            container_name=taskSpec.container_name,
                            only_tags_fc=taskSpec.use_only_tags_fc(),
                            host_cpu_specs=host_cpu_spec,
                            host_gpu_spec=host_gpu_spec,
                            log_stream=tmpLog,
                        )
                        siteListWithSW += tmpSiteListWithSW
                    if len(transHome.split("-")) == 2:
                        tmpSiteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                            sitesNoJsonCheck,
                            "atlas",
                            transHome.split("-")[0],
                            transHome.split("-")[1],
                            taskSpec.get_sw_platform(),
                            False,
                            False,
                            container_name=taskSpec.container_name,
                            only_tags_fc=taskSpec.use_only_tags_fc(),
                            host_cpu_specs=host_cpu_spec,
                            host_gpu_spec=host_gpu_spec,
                            log_stream=tmpLog,
                        )
                        siteListWithSW += tmpSiteListWithSW
                    sitesAuto = copy.copy(siteListWithSW)

                else:
                    # nightlies or standalone uses only AUTO
                    if taskSpec.transHome is not None:
                        # CVMFS check for nightlies
                        siteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                            unified_site_list,
                            "nightlies",
                            None,
                            None,
                            taskSpec.get_sw_platform(),
                            True,
                            False,
                            container_name=taskSpec.container_name,
                            only_tags_fc=taskSpec.use_only_tags_fc(),
                            host_cpu_specs=host_cpu_spec,
                            host_gpu_spec=host_gpu_spec,
                            log_stream=tmpLog,
                        )
                        sitesAuto = copy.copy(siteListWithSW)

                    else:
                        # no CVMFS check for standalone SW
                        siteListWithSW, sitesNoJsonCheck = jsonCheck.check(
                            unified_site_list,
                            None,
                            None,
                            None,
                            taskSpec.get_sw_platform(),
                            False,
                            True,
                            container_name=taskSpec.container_name,
                            only_tags_fc=taskSpec.use_only_tags_fc(),
                            host_cpu_specs=host_cpu_spec,
                            host_gpu_spec=host_gpu_spec,
                            log_stream=tmpLog,
                        )
                        sitesAuto = copy.copy(siteListWithSW)

                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                sitesAny = []
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
                    elif host_cpu_spec is None and host_gpu_spec is None and tmpSiteSpec.releases == ["ANY"]:
                        # release check is disabled or release is available
                        newScanSiteList.append(tmpSiteName)
                        sitesAny.append(tmpSiteName)
                    else:
                        # release is unavailable
                        tmpLog.info(
                            f"  skip site={tmpSiteName} due to missing SW cache={taskSpec.transHome}:{taskSpec.get_sw_platform()} container_name='{taskSpec.container_name}' "
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
                    retVal = retTmpError
                    continue
            ######################################
            # selection for memory
            origMinRamCount = inputChunk.getMaxRamCount()
            if origMinRamCount not in [0, None]:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # scale RAM by nCores
                    minRamCount = origMinRamCount
                    if taskSpec.ramPerCore() and not inputChunk.isMerging:
                        if tmpSiteSpec.coreCount not in [None, 0]:
                            minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    minRamCount = JobUtils.compensate_ram_count(minRamCount)
                    # site max memory requirement
                    site_maxmemory = 0
                    if tmpSiteSpec.maxrss not in [0, None]:
                        site_maxmemory = tmpSiteSpec.maxrss
                    if site_maxmemory not in [0, None] and minRamCount != 0 and minRamCount > site_maxmemory:
                        tmpLog.info(
                            "  skip site={0} due to site RAM shortage. site_maxmemory={1} < job_minramcount={2} criteria=-lowmemory".format(
                                tmpSiteName, site_maxmemory, minRamCount
                            )
                        )
                        continue
                    # site min memory requirement
                    site_minmemory = 0
                    if tmpSiteSpec.minrss not in [0, None]:
                        site_minmemory = tmpSiteSpec.minrss
                    if site_minmemory not in [0, None] and minRamCount != 0 and minRamCount < site_minmemory:
                        tmpLog.info(
                            "  skip site={0} due to job RAM shortage. site_minmemory={1} > job_minramcount={2} criteria=-highmemory".format(
                                tmpSiteName, site_minmemory, minRamCount
                            )
                        )
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                ramUnit = taskSpec.ramUnit
                if ramUnit is None:
                    ramUnit = "MB"
                tmpLog.info(f"{len(scanSiteList)} candidates passed memory check = {minRamCount} {ramUnit}")
                self.add_summary_message(oldScanSiteList, scanSiteList, "memory check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # selection for scratch disk
            tmpMaxAtomSize = inputChunk.getMaxAtomSize()
            if not inputChunk.isMerging:
                tmpEffAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
                tmpOutDiskSize = taskSpec.getOutDiskSize()
                tmpWorkDiskSize = taskSpec.getWorkDiskSize()
                minDiskCountS = tmpOutDiskSize * tmpEffAtomSize + tmpWorkDiskSize + tmpMaxAtomSize
                minDiskCountS = minDiskCountS // 1024 // 1024
                maxSizePerJob = taskSpec.getMaxSizePerJob()
                if maxSizePerJob is None:
                    maxSizePerJob = None
                else:
                    maxSizePerJob //= 1024 * 1024
                # size for direct IO sites
                minDiskCountR = tmpOutDiskSize * tmpEffAtomSize + tmpWorkDiskSize
                minDiskCountR = minDiskCountR // 1024 // 1024
                tmpLog.info(f"maxAtomSize={tmpMaxAtomSize} effectiveAtomSize={tmpEffAtomSize} outDiskCount={tmpOutDiskSize} workDiskSize={tmpWorkDiskSize}")
            else:
                maxSizePerJob = None
                minDiskCountS = 2 * tmpMaxAtomSize // 1024 // 1024
                minDiskCountR = "NA"
            tmpLog.info(f"minDiskCountScratch={minDiskCountS} minDiskCountRemote={minDiskCountR} nGBPerJobInMB={maxSizePerJob}")
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxwdir:
                    if CoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
                        minDiskCount = minDiskCountR
                        if maxSizePerJob is not None and not taskSpec.useLocalIO():
                            tmpMinDiskCountR = tmpOutDiskSize * maxSizePerJob + tmpWorkDiskSize
                            tmpMinDiskCountR /= 1024 * 1024
                            if tmpMinDiskCountR > minDiskCount:
                                minDiskCount = tmpMinDiskCountR
                    else:
                        minDiskCount = minDiskCountS
                        if maxSizePerJob is not None and maxSizePerJob > minDiskCount:
                            minDiskCount = maxSizePerJob

                    # get site and task corecount to scale maxwdir
                    if tmpSiteSpec.coreCount in [None, 0, 1]:
                        site_cc = 1
                    else:
                        site_cc = tmpSiteSpec.coreCount

                    if taskSpec.coreCount in [None, 0, 1]:
                        task_cc = 1
                    else:
                        task_cc = site_cc

                    maxwdir_scaled = tmpSiteSpec.maxwdir * task_cc / site_cc

                    if minDiskCount > maxwdir_scaled:
                        tmpLog.info(f"  skip site={tmpSiteName} due to small scratch disk={maxwdir_scaled} < {minDiskCount} criteria=-disk")
                        continue
                    newMaxwdir[tmpSiteName] = maxwdir_scaled
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed scratch disk check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "scratch disk check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # selection for available space in SE
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                # check endpoint
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
                if scope_output not in tmpSiteSpec.ddm_endpoints_output:
                    tmpLog.info(f"  skip site={tmpSiteName} since {scope_output} output endpoint undefined criteria=-disk")
                    continue
                tmp_output_endpoint = tmpSiteSpec.ddm_endpoints_output[scope_output].getEndPoint(tmpSiteSpec.ddm_output[scope_output])
                if tmp_output_endpoint is not None:
                    # free space must be >= 200GB
                    diskThreshold = 200
                    tmpSpaceSize = 0
                    if tmp_output_endpoint["space_expired"] is not None:
                        tmpSpaceSize += tmp_output_endpoint["space_expired"]
                    if tmp_output_endpoint["space_free"] is not None:
                        tmpSpaceSize += tmp_output_endpoint["space_free"]
                    if (
                        tmpSpaceSize < diskThreshold and "skip_RSE_check" not in tmpSiteSpec.catchall
                    ):  # skip_RSE_check: exceptional bypass of RSEs without storage reporting
                        tmpLog.info(f"  skip site={tmpSiteName} due to disk shortage in SE {tmpSpaceSize} < {diskThreshold}GB criteria=-disk")
                        continue
                # check if blacklisted
                tmp_msg = AtlasBrokerUtils.check_endpoints_with_blacklist(tmpSiteSpec, scope_input, scope_output, sites_in_nucleus, remote_source_available)
                if tmp_msg is not None:
                    tmpLog.info(f"  skip site={tmpSiteName} since {tmpSiteSpec.ddm_output[scope_output]} is blacklisted in DDM criteria=-blacklist")
                    continue
                # local quota
                if tmpSiteSpec.ddm_output[scope_output] in endpoints_over_local_quota:
                    tmpLog.info(f"  skip site={tmpSiteName} since {tmpSiteSpec.ddm_output[scope_output]} is over local quota criteria=-local_quota")
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed SE space check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "storage space check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # selection for walltime
            if not taskSpec.useHS06():
                tmpMaxAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
                if taskSpec.walltime is not None:
                    minWalltime = taskSpec.walltime * tmpMaxAtomSize
                else:
                    minWalltime = None
                strMinWalltime = f"walltime*inputSize={taskSpec.walltime}*{tmpMaxAtomSize}"
            else:
                tmpMaxAtomSize = inputChunk.getMaxAtomSize(getNumEvents=True)
                if taskSpec.getCpuTime() is not None:
                    minWalltime = taskSpec.getCpuTime() * tmpMaxAtomSize
                else:
                    minWalltime = None
                strMinWalltime = f"cpuTime*nEventsPerJob={taskSpec.getCpuTime()}*{tmpMaxAtomSize}"
            if minWalltime and minWalltime > 0 and not inputChunk.isMerging:
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
                    if origSiteMaxTime != 0 and minWalltime and minWalltime > siteMaxTime:
                        tmpMsg = f"  skip site={tmpSiteName} due to short site walltime {tmpSiteStr} (site upper limit) less than {strMinWalltime} "
                        tmpMsg += "criteria=-shortwalltime"
                        tmpLog.info(tmpMsg)
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
                    if origSiteMinTime != 0 and (minWalltime is None or minWalltime < siteMinTime):
                        tmpMsg = f"  skip site {tmpSiteName} due to short job walltime {tmpSiteStr} (site lower limit) greater than {strMinWalltime} "
                        tmpMsg += "criteria=-longwalltime"
                        tmpLog.info(tmpMsg)
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                if not taskSpec.useHS06():
                    tmpLog.info(
                        "{0} candidates passed walltime check {1}({2})".format(
                            len(scanSiteList), strMinWalltime, taskSpec.walltimeUnit + "PerMB" if taskSpec.walltimeUnit else "kSI2ksecondsPerMB"
                        )
                    )
                else:
                    tmpLog.info(f"{len(scanSiteList)} candidates passed walltime check {strMinWalltime}({taskSpec.cpuTimeUnit}*nEventsPerJob)")
                self.add_summary_message(oldScanSiteList, scanSiteList, "walltime check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # selection for nPilot
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            nPilotMap = {}
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                # check at the site
                nPilot = 0
                if tmpSiteName in nWNmap:
                    nPilot = nWNmap[tmpSiteName]["getJob"] + nWNmap[tmpSiteName]["updateJob"]
                if nPilot == 0 and taskSpec.prodSourceLabel not in ["test"]:
                    tmpLog.info(f"  skip site={tmpSiteName} due to no pilot criteria=-nopilot")
                    if not self.testMode:
                        continue
                newScanSiteList.append(tmpSiteName)
                nPilotMap[tmpSiteName] = nPilot
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed pilot activity check")
            self.add_summary_message(oldScanSiteList, scanSiteList, "pilot activity check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # check inclusion and exclusion
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            sitesForANY = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                autoSite = False
                # check exclusion
                if AtlasBrokerUtils.isMatched(tmpSiteName, excludeList):
                    tmpLog.info(f"  skip site={tmpSiteName} excluded criteria=-excluded")
                    continue

                # check inclusion
                if includeList is not None and not AtlasBrokerUtils.isMatched(tmpSiteName, includeList):
                    if "AUTO" in includeList:
                        autoSite = True
                    else:
                        tmpLog.info(f"  skip site={tmpSiteName} not included criteria=-notincluded")
                        continue
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)

                # check cloud
                if taskSpec.cloud not in [None, "", "any", tmpSiteSpec.cloud]:
                    tmpLog.info(f"  skip site={tmpSiteName} cloud mismatch criteria=-cloudmismatch")
                    continue
                if autoSite:
                    sitesForANY.append(tmpSiteName)
                else:
                    newScanSiteList.append(tmpSiteName)
            # use AUTO sites if no sites are included
            if newScanSiteList == []:
                newScanSiteList = sitesForANY
            else:
                for tmpSiteName in sitesForANY:
                    tmpLog.info(f"  skip site={tmpSiteName} not included criteria=-notincluded")
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info(f"{len(scanSiteList)} candidates passed inclusion/exclusion")
            self.add_summary_message(oldScanSiteList, scanSiteList, "include/exclude check")
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error("no candidates")
                retVal = retTmpError
                continue
            ######################################
            # sites already used by task
            tmpSt, sitesUsedByTask = self.taskBufferIF.getSitesUsedByTask_JEDI(taskSpec.jediTaskID)
            if not tmpSt:
                tmpLog.error("failed to get sites which already used by task")
                retVal = retTmpError
                continue
            sitesUsedByTask = self.get_unified_sites(sitesUsedByTask)
            ######################################
            # calculate weight
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
            if not tmpSt:
                tmpLog.error("failed to get job statistics with priority")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            tmpSt, siteToRunRateMap = AtlasBrokerUtils.getSiteToRunRateStats(tbIF=self.taskBufferIF, vo=taskSpec.vo)
            if not tmpSt:
                tmpLog.error("failed to get site to-running rate")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            # check for preassigned
            if sitePreAssigned:
                oldScanSiteList = copy.copy(scanSiteList)
                if preassignedSite not in scanSiteList and preassignedSite not in self.get_unified_sites(scanSiteList):
                    tmpLog.info(f"preassigned site {preassignedSite} did not pass all tests")
                    self.add_summary_message(oldScanSiteList, [], "preassign check")
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retFatal
                    continue
                else:
                    newScanSiteList = []
                    for tmpPseudoSiteName in scanSiteList:
                        tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                        tmpSiteName = tmpSiteSpec.get_unified_name()
                        if tmpSiteName != preassignedSite:
                            tmpLog.info(f"  skip site={tmpPseudoSiteName} non pre-assigned site criteria=-nonpreassigned")
                            continue
                        newScanSiteList.append(tmpSiteName)
                    scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates passed preassigned check")
                self.add_summary_message(oldScanSiteList, scanSiteList, "preassign check")
            ######################################
            # selection for hospital
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            hasNormalSite = False
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                if not tmpSiteName.endswith("_HOSPITAL"):
                    hasNormalSite = True
                    break
            if hasNormalSite:
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    # remove hospital
                    if tmpSiteName.endswith("_HOSPITAL"):
                        tmpLog.info(f"  skip site={tmpSiteName} due to hospital queue criteria=-hospital")
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates passed hospital check")
                self.add_summary_message(oldScanSiteList, scanSiteList, "hospital check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # cap with resource type
            if not sitePreAssigned:
                # count jobs per resource type
                tmpRet, tmpStatMap = self.taskBufferIF.getJobStatisticsByResourceTypeSite(workQueue)
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                RT_Cap = 2
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    tmpUnifiedName = tmpSiteSpec.get_unified_name()
                    if tmpUnifiedName in tmpStatMap and taskSpec.resource_type in tmpStatMap[tmpUnifiedName]:
                        tmpSiteStatMap = tmpStatMap[tmpUnifiedName][taskSpec.resource_type]
                        tmpRTrunning = tmpSiteStatMap.get("running", 0)
                        tmpRTqueue = tmpSiteStatMap.get("defined", 0)
                        tmpRTqueue += tmpSiteStatMap.get("assigned", 0)
                        tmpRTqueue += tmpSiteStatMap.get("activated", 0)
                        tmpRTqueue += tmpSiteStatMap.get("starting", 0)
                        if tmpRTqueue > max(20, tmpRTrunning * RT_Cap):
                            tmpMsg = f"  skip site={tmpSiteName} "
                            tmpMsg += "since nQueue/max(20,nRun) with gshare+resource_type is "
                            tmpMsg += f"{tmpRTqueue}/max(20,{tmpRTrunning}) > {RT_Cap} "
                            tmpMsg += "criteria=-cap_rt"
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info(f"{len(scanSiteList)} candidates passed for cap with gshare+resource_type check")
                self.add_summary_message(oldScanSiteList, scanSiteList, "cap with gshare+resource_type check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
            ######################################
            # selection for un-overloaded sites
            if not inputChunk.isMerging:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                overloadedNonVP = []
                msgList = []
                msgListVP = []
                minQueue = self.taskBufferIF.getConfigValue("anal_jobbroker", "OVERLOAD_MIN_QUEUE", "jedi", taskSpec.vo)
                if minQueue is None:
                    minQueue = 20
                ratioOffset = self.taskBufferIF.getConfigValue("anal_jobbroker", "OVERLOAD_RATIO_OFFSET", "jedi", taskSpec.vo)
                if ratioOffset is None:
                    ratioOffset = 1.2
                grandRatio = AtlasBrokerUtils.get_total_nq_nr_ratio(jobStatPrioMap, taskSpec.gshare)
                tmpLog.info(f"grand nQueue/nRunning ratio : {grandRatio}")
                tmpLog.info(f"sites with non-VP data : {','.join(scanSiteWoVP)}")
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    # get nQueue and nRunning
                    nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running", workQueue_tag=taskSpec.gshare)
                    nQueue = 0
                    for jobStatus in ["defined", "assigned", "activated", "starting"]:
                        nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, workQueue_tag=taskSpec.gshare)
                    # skip if overloaded
                    if nQueue > minQueue and (nRunning == 0 or float(nQueue) / float(nRunning) > grandRatio * ratioOffset):
                        tmpMsg = f"  skip site={tmpPseudoSiteName} "
                        tmpMsg += f"nQueue>minQueue({minQueue}) and "
                        if nRunning == 0:
                            tmpMsg += "nRunning=0 "
                            problematic_sites_dict.setdefault(tmpSiteName, set())
                            problematic_sites_dict[tmpSiteName].add(f"nQueue({nQueue})>minQueue({minQueue}) and nRunning=0")
                        else:
                            tmpMsg += f"nQueue({nQueue})/nRunning({nRunning}) > grandRatio({grandRatio:.2f})*offset({ratioOffset}) "
                        if tmpSiteName in scanSiteWoVP or checkDataLocality is False or inputChunk.getDatasets() == []:
                            tmpMsg += "criteria=-overloaded"
                            overloadedNonVP.append(tmpPseudoSiteName)
                            msgListVP.append(tmpMsg)
                        else:
                            tmpMsg += "and VP criteria=-overloaded_vp"
                            msgList.append(tmpMsg)
                    else:
                        newScanSiteList.append(tmpPseudoSiteName)
                if len(newScanSiteList) > 0:
                    scanSiteList = newScanSiteList
                    for tmpMsg in msgList + msgListVP:
                        tmpLog.info(tmpMsg)
                else:
                    scanSiteList = overloadedNonVP
                    for tmpMsg in msgList:
                        tmpLog.info(tmpMsg)
                tmpLog.info(f"{len(scanSiteList)} candidates passed overload check")
                self.add_summary_message(oldScanSiteList, scanSiteList, "overload check")
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue
            ######################################
            # consider grid usage of the user
            user_name = CoreUtils.clean_user_id(taskSpec.userName)
            tmpSt, jobsStatsPerUser = AtlasBrokerUtils.getUsersJobsStats(
                tbIF=self.taskBufferIF, vo=taskSpec.vo, prod_source_label=taskSpec.prodSourceLabel, cache_lifetime=60
            )
            if not tmpSt:
                tmpLog.error("failed to get users jobs statistics")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                return retTmpError
            elif not inputChunk.isMerging:
                # loop over sites
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    # get info about site
                    nRunning_pq_total = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running")
                    nRunning_pq_in_gshare = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running", workQueue_tag=taskSpec.gshare)
                    nQueue_pq_in_gshare = 0
                    for jobStatus in ["defined", "assigned", "activated", "starting"]:
                        nQueue_pq_in_gshare += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, workQueue_tag=taskSpec.gshare)
                    # get to-running-rate
                    try:
                        site_to_running_rate = siteToRunRateMap[tmpSiteName]
                        if isinstance(site_to_running_rate, dict):
                            site_to_running_rate = sum(site_to_running_rate.values())
                    except KeyError:
                        site_to_running_rate = 0
                    finally:
                        to_running_rate = nRunning_pq_in_gshare * site_to_running_rate / nRunning_pq_total if nRunning_pq_total > 0 else 0
                    # get conditions of the site whether to throttle
                    if nQueue_pq_in_gshare < base_queue_length_per_pq:
                        # not throttle since overall queue length of the site is not large enough
                        tmpLog.debug(f"not throttle on {tmpSiteName} since nQ({nQueue_pq_in_gshare}) < base queue length ({base_queue_length_per_pq})")
                        continue
                    allowed_queue_length_from_wait_time = base_expected_wait_hour_on_pq * to_running_rate
                    if nQueue_pq_in_gshare < allowed_queue_length_from_wait_time:
                        # not statisfy since overall waiting time of the site is not long enough
                        tmpLog.debug(
                            "not throttle on {0} since nQ({1}) < {2:.3f} = toRunningRate({3:.3f} /hr) * base wait time ({4} hr)".format(
                                tmpSiteName, nQueue_pq_in_gshare, allowed_queue_length_from_wait_time, to_running_rate, base_expected_wait_hour_on_pq
                            )
                        )
                        continue
                    # get user jobs stats under the gshare
                    try:
                        user_jobs_stats_map = jobsStatsPerUser[tmpSiteName][taskSpec.gshare][user_name]
                    except KeyError:
                        continue
                    else:
                        nQ_pq_user = user_jobs_stats_map["nQueue"]
                        nR_pq_user = user_jobs_stats_map["nRunning"]
                        nUsers_pq = len(jobsStatsPerUser[tmpSiteName][taskSpec.gshare])
                        try:
                            nR_pq = jobsStatsPerUser[tmpSiteName][taskSpec.gshare]["_total"]["nRunning"]
                        except KeyError:
                            nR_pq = nRunning_pq_in_gshare
                    # evaluate max nQueue per PQ
                    nQ_pq_limit_map = {
                        "base_limit": base_queue_length_per_pq,
                        "static_limit": static_max_queue_running_ratio * nR_pq,
                        "dynamic_limit": max_expected_wait_hour * to_running_rate,
                    }
                    max_nQ_pq = max(nQ_pq_limit_map.values())
                    # description for max nQueue per PQ
                    description_of_max_nQ_pq = f"max_nQ_pq({max_nQ_pq:.3f}) "
                    for k, v in nQ_pq_limit_map.items():
                        if v == max_nQ_pq:
                            if k in ["base_limit"]:
                                description_of_max_nQ_pq += f"= {k} = BASE_QUEUE_LENGTH_PER_PQ({base_queue_length_per_pq})"
                            elif k in ["static_limit"]:
                                description_of_max_nQ_pq += f"= {k} = STATIC_MAX_QUEUE_RUNNING_RATIO({static_max_queue_running_ratio:.3f}) * nR_pq({nR_pq})"
                            elif k in ["dynamic_limit"]:
                                description_of_max_nQ_pq += "= {key} = MAX_EXPECTED_WAIT_HOUR({value:.3f} hr) * toRunningRate_pq({trr:.3f} /hr)".format(
                                    key=k, value=max_expected_wait_hour, trr=to_running_rate
                                )
                            break
                    # evaluate fraction per user
                    user_fraction_map = {
                        "equal_distr": 1 / nUsers_pq,
                        "prop_to_nR": nR_pq_user / nR_pq if nR_pq > 0 else 0,
                    }
                    max_user_fraction = max(user_fraction_map.values())
                    # description for max fraction per user
                    description_of_max_user_fraction = f"max_user_fraction({max_user_fraction:.3f}) "
                    for k, v in user_fraction_map.items():
                        if v == max_user_fraction:
                            if k in ["equal_distr"]:
                                description_of_max_user_fraction += f"= {k} = 1 / nUsers_pq({nUsers_pq})"
                            elif k in ["prop_to_nR"]:
                                description_of_max_user_fraction += f"= {k} = nR_pq_user({nR_pq_user}) / nR_pq({nR_pq})"
                            break
                    # evaluate max nQueue per PQ per user
                    nQ_pq_user_limit_map = {
                        "constant_base_user_limit": base_default_queue_length_per_pq_user,
                        "ratio_base_user_limit": base_queue_ratio_on_pq * nR_pq,
                        "dynamic_user_limit": max_nQ_pq * max_user_fraction,
                    }
                    max_nQ_pq_user = max(nQ_pq_user_limit_map.values())
                    # description for max fraction per user
                    description_of_max_nQ_pq_user = f"max_nQ_pq_user({max_nQ_pq_user:.3f}) "
                    for k, v in nQ_pq_user_limit_map.items():
                        if v == max_nQ_pq_user:
                            if k in ["constant_base_user_limit"]:
                                description_of_max_nQ_pq_user += f"= {k} = BASE_DEFAULT_QUEUE_LENGTH_PER_PQ_USER({base_default_queue_length_per_pq_user})"
                            elif k in ["ratio_base_user_limit"]:
                                description_of_max_nQ_pq_user += f"= {k} = BASE_QUEUE_RATIO_ON_PQ({base_queue_ratio_on_pq:.3f}) * nR_pq({nR_pq})"
                            elif k in ["dynamic_user_limit"]:
                                description_of_max_nQ_pq_user += f"= {k} = max_nQ_pq({max_nQ_pq:.3f}) * max_user_fraction({max_user_fraction:.3f})"
                                description_of_max_nQ_pq_user += f" , where {description_of_max_nQ_pq} , and {description_of_max_user_fraction}"
                            break
                    # # Analysis Stabilizer: skip sites where the user queues too much
                    if nQ_pq_user > max_nQ_pq_user:
                        tmpMsg = f" consider {tmpSiteName} unsuitable for the user due to long queue of the user: "
                        tmpMsg += f"nQ_pq_user({nQ_pq_user}) > {description_of_max_nQ_pq_user} "
                        # view as problematic site in order to throttle
                        problematic_sites_dict.setdefault(tmpSiteName, set())
                        problematic_sites_dict[tmpSiteName].add(tmpMsg)
                    # problematic sites with too many failed and closed jobs
                    if tmpSiteName in failureCounts:
                        nFailed = failureCounts[tmpSiteName].get("failed", 0)
                        nClosed = failureCounts[tmpSiteName].get("closed", 0)
                        nFinished = failureCounts[tmpSiteName].get("finished", 0)
                        if not inputChunk.isMerging and (nFailed + nClosed) > max(2 * nFinished, minBadJobsToSkipPQ):
                            problematic_sites_dict.setdefault(tmpSiteName, set())
                            problematic_sites_dict[tmpSiteName].add("too many failed or closed jobs for last 6h")
            # check if good sites are still available after removing problematic sites when
            # * the brokerage has two loops
            # * it is doing the first loop with data locality check
            # * data locality check is disabled in the second loop due to low IO, special task priority, or timeout
            if len(scan_site_list_loops) > 1 and i_loop == 0 and to_ignore_data_loc:
                candidates_with_problems = []
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    if tmpSiteName in problematic_sites_dict:
                        candidates_with_problems.append([tmpPseudoSiteName, list(problematic_sites_dict[tmpSiteName])[0]])
                if len(scanSiteList) == len(candidates_with_problems):
                    for tmpPseudoSiteName, error_diag in candidates_with_problems:
                        tmpLog.info(f"  skip site={tmpPseudoSiteName} due to a temporary user-specific problem: {error_diag} criteria=-tmp_user_problem")
                    tmpLog.info("0 candidates passed temporally user-specific problem check")
                    self.add_summary_message(scanSiteList, [], "temp user problem check")
                    self.dump_summary(tmpLog)
                    tmpLog.error("no candidates")
                    retVal = retTmpError
                    continue

            ############
            # loop end
            overall_site_list.update(scanSiteList)
            if site_list_with_data is None:
                # preserve site list with data
                site_list_with_data = set(scanSiteList)
            if len(overall_site_list) >= taskSpec.getNumSitesPerJob():
                retVal = None
                break
        # failed
        if retVal is not None:
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retVal
        # get list of available files
        scanSiteList = list(overall_site_list)
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
                    if tmpSiteName in remoteSourceList and datasetSpec.datasetName in remoteSourceList[tmpSiteName]:
                        for tmpRemoteSite in remoteSourceList[tmpSiteName][datasetSpec.datasetName]:
                            if tmpRemoteSite not in fileScanSiteList:
                                fileScanSiteList.append(tmpRemoteSite)
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(fileScanSiteList, self.siteMapper, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
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
                    use_vp=useVP,
                    file_scan_in_container=False,
                    complete_only=useCompleteOnly,
                    element_list=element_map.get(datasetSpec.datasetName),
                )
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError("ddmIF.getAvailableFiles failed")
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed to get available files with {errtype.__name__} {errvalue}")
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        # make data weight
        totalSize = 0
        totalNumFiles = 0
        totalDiskSizeMap = dict()
        totalTapeSizeMap = dict()
        for datasetSpec in inputChunk.getDatasets():
            totalNumFiles += len(datasetSpec.Files)
            for fileSpec in datasetSpec.Files:
                totalSize += fileSpec.fsize
            if datasetSpec.datasetName in availableFileMap:
                for tmpSiteName, tmpAvFileMap in availableFileMap[datasetSpec.datasetName].items():
                    totalDiskSizeMap.setdefault(tmpSiteName, 0)
                    totalTapeSizeMap.setdefault(tmpSiteName, 0)
                    for fileSpec in tmpAvFileMap["localdisk"]:
                        totalDiskSizeMap[tmpSiteName] += fileSpec.fsize
                    for fileSpec in tmpAvFileMap["localtape"]:
                        totalTapeSizeMap[tmpSiteName] += fileSpec.fsize
        totalSize //= 1024 * 1024 * 1024
        tmpLog.info(f"totalInputSize={totalSize} GB")
        for tmpSiteName in totalDiskSizeMap.keys():
            totalDiskSizeMap[tmpSiteName] //= 1024 * 1024 * 1024
        for tmpSiteName in totalTapeSizeMap.keys():
            totalTapeSizeMap[tmpSiteName] //= 1024 * 1024 * 1024
        ######################################
        # final procedure
        tmpLog.info(f"{len(scanSiteList)} candidates for final check")
        weightMap = {}
        weightStr = {}
        candidateSpecList = []
        preSiteCandidateSpec = None
        basic_weight_compar_map = {}
        workerStat = self.taskBufferIF.ups_load_worker_stats()
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running", workQueue_tag=taskSpec.gshare)
            nDefined = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "defined", workQueue_tag=taskSpec.gshare)
            nAssigned = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "assigned", workQueue_tag=taskSpec.gshare)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "activated", workQueue_tag=taskSpec.gshare)
            nStarting = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "starting", workQueue_tag=taskSpec.gshare)
            # get num workers
            nWorkers = 0
            nWorkersCutoff = 20
            if tmpSiteName in workerStat:
                for _, tmpLabelStat in workerStat[tmpSiteName].items():
                    for _, tmpResStat in tmpLabelStat.items():
                        for tmpResType, tmpCounts in tmpResStat.items():
                            for tmpStatus, tmpNum in tmpCounts.items():
                                if tmpStatus in ["running", "submitted"]:
                                    nWorkers += tmpNum
                # cap
                nWorkers = min(nWorkersCutoff, nWorkers)
            # use nWorkers to bootstrap
            if tmpSiteName in nPilotMap and nPilotMap[tmpSiteName] > 0 and nRunning < nWorkersCutoff and nWorkers > nRunning:
                tmpLog.debug(f"using nWorkers={nWorkers} as nRunning at {tmpPseudoSiteName} since original nRunning={nRunning} is low")
                nRunning = nWorkers
            # take into account the number of standby jobs
            numStandby = tmpSiteSpec.getNumStandby(taskSpec.gshare, taskSpec.resource_type)
            if numStandby is None:
                pass
            elif numStandby == 0:
                # use the number of starting jobs as the number of standby jobs
                nRunning = nStarting + nRunning
                tmpLog.debug(f"using dynamic workload provisioning at {tmpPseudoSiteName} to set nRunning={nRunning}")
            else:
                # the number of standby jobs is defined
                nRunning = max(int(numStandby / tmpSiteSpec.coreCount), nRunning)
                tmpLog.debug(f"using static workload provisioning at {tmpPseudoSiteName} with nStandby={numStandby} to set nRunning={nRunning}")
            nFailed = 0
            nClosed = 0
            nFinished = 0
            if tmpSiteName in failureCounts:
                if "failed" in failureCounts[tmpSiteName]:
                    nFailed = failureCounts[tmpSiteName]["failed"]
                if "closed" in failureCounts[tmpSiteName]:
                    nClosed = failureCounts[tmpSiteName]["closed"]
                if "finished" in failureCounts[tmpSiteName]:
                    nFinished = failureCounts[tmpSiteName]["finished"]
            # to-running rate
            try:
                site_to_running_rate = siteToRunRateMap[tmpSiteName]
                if isinstance(site_to_running_rate, dict):
                    site_to_running_rate = sum(site_to_running_rate.values())
            except KeyError:
                to_running_rate_str = "0(unknown)"
                to_running_rate = 0
            else:
                site_n_running = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "running")
                to_running_rate = nRunning * site_to_running_rate / site_n_running if site_n_running > 0 else 0
                to_running_rate_str = f"{to_running_rate:.3f}"
            # site class value; by default mid-class (= 0) if unclassified
            site_class_value = analy_sites_class_dict.get(tmpSiteName, 0)
            site_class_value = 0 if site_class_value is None else site_class_value
            # calculate original basic weight
            orig_basic_weight = float(nRunning + 1) / float(nActivated + nAssigned + nDefined + nStarting + 1)
            # available site, take in account of new basic weight
            basic_weight_compar_map[tmpSiteName] = {}
            basic_weight_compar_map[tmpSiteName]["orig"] = orig_basic_weight
            basic_weight_compar_map[tmpSiteName]["trr"] = to_running_rate
            basic_weight_compar_map[tmpSiteName]["nq"] = nActivated + nAssigned + nDefined + nStarting
            basic_weight_compar_map[tmpSiteName]["nr"] = nRunning
            basic_weight_compar_map[tmpSiteName]["class"] = site_class_value
            basic_weight_compar_map[tmpSiteName]["nDefined"] = nDefined
            basic_weight_compar_map[tmpSiteName]["nActivated"] = nActivated
            basic_weight_compar_map[tmpSiteName]["nStarting"] = nStarting
            basic_weight_compar_map[tmpSiteName]["nAssigned"] = nAssigned
            basic_weight_compar_map[tmpSiteName]["nFailed"] = nFailed
            basic_weight_compar_map[tmpSiteName]["nClosed"] = nClosed
            basic_weight_compar_map[tmpSiteName]["nFinished"] = nFinished
        # compute new basic weight
        try:
            n_avail_sites = len(basic_weight_compar_map)
            if n_avail_sites == 0:
                tmpLog.debug("WEIGHT-COMPAR: zero available sites, skip")
            else:
                # task class value
                task_class_value = task_eval_dict.get("class", 1) if task_eval_dict is not None else 1
                # get nFilesPerJob
                if not inputChunk.isMerging:
                    nFilesPerJob = taskSpec.getNumFilesPerJob()
                else:
                    nFilesPerJob = taskSpec.getNumFilesPerMergeJob()
                if nFilesPerJob is None or nFilesPerJob < 1:
                    nFilesPerJob = 1
                #
                _maxSizePerJob = taskSpec.getMaxSizePerJob()
                if _maxSizePerJob is not None:
                    _maxSizePerJob += inputChunk.defaultOutputSize
                    _maxSizePerJob += taskSpec.getWorkDiskSize()
                else:
                    if taskSpec.useScout():
                        _maxSizePerJob = inputChunk.maxInputSizeScouts * 1024 * 1024
                    else:
                        _maxSizePerJob = inputChunk.maxInputSizeAvalanche * 1024 * 1024
                # count subchunks
                n_subchunks = 0
                while True:
                    subchunk, _ = inputChunk.getSubChunk(
                        None,
                        maxNumFiles=taskSpec.getMaxNumFilesPerJob(),
                        nFilesPerJob=taskSpec.getNumFilesPerJob(),
                        walltimeGradient=(taskSpec.getCpuTime() if taskSpec.useHS06() else None),
                        maxWalltime=(taskSpec.getMaxWalltime() if taskSpec.getMaxWalltime() is not None else 345600),
                        sizeGradients=taskSpec.getOutDiskSize(),
                        sizeIntercepts=taskSpec.getWorkDiskSize(),
                        maxSize=_maxSizePerJob,
                        nEventsPerJob=taskSpec.getNumEventsPerJob(),
                        coreCount=taskSpec.coreCount,
                        corePower=10,
                        respectLB=taskSpec.respectLumiblock(),
                    )
                    if subchunk is None:
                        break
                    else:
                        n_subchunks += 1
                inputChunk.resetUsedCounters()
                n_jobs_to_submit = n_subchunks
                # parameters for small additional weight
                weight_epsilon_init = 0.001
                weight_epsilon_hi_mid = 0.05
                weight_epsilon_hi_lo = 0.002
                weight_epsilon_mid_lo = 0.1
                # initialize
                tmpSt, siteToRunRateMap = AtlasBrokerUtils.getSiteToRunRateStats(tbIF=self.taskBufferIF, vo=taskSpec.vo)
                weight_comparison_avail_sites = set(basic_weight_compar_map.keys())
                site_class_n_site_dict = {1: 0, 0: 0, -1: 0}
                site_class_rem_q_len_dict = {1: 0, 0: 0, -1: 0}
                total_rem_q_len = 0
                # loop over sites for metrics
                for site, bw_map in basic_weight_compar_map.items():
                    # site class count
                    site_class_n_site_dict[bw_map["class"]] += 1
                    # get info about site
                    nRunning_pq_total = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, site, "running")
                    nRunning_pq_in_gshare = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, site, "running", workQueue_tag=taskSpec.gshare)
                    nQueue_pq_in_gshare = 0
                    for jobStatus in ["defined", "assigned", "activated", "starting"]:
                        nQueue_pq_in_gshare += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, site, jobStatus, workQueue_tag=taskSpec.gshare)
                    # get to-running-rate
                    try:
                        site_to_running_rate = siteToRunRateMap[site]
                        if isinstance(site_to_running_rate, dict):
                            site_to_running_rate = sum(site_to_running_rate.values())
                    except Exception:
                        site_to_running_rate = 0
                    finally:
                        to_running_rate = nRunning_pq_in_gshare * site_to_running_rate / nRunning_pq_total if nRunning_pq_total > 0 else 0
                    # get user jobs stats under the gshare
                    try:
                        user_jobs_stats_map = jobsStatsPerUser[site][taskSpec.gshare][user_name]
                    except KeyError:
                        nQ_pq_user = 0
                        nR_pq_user = 0
                    else:
                        nQ_pq_user = user_jobs_stats_map["nQueue"]
                        nR_pq_user = user_jobs_stats_map["nRunning"]
                    try:
                        nUsers_pq = len(jobsStatsPerUser[site][taskSpec.gshare])
                    except KeyError:
                        nUsers_pq = 1
                    try:
                        nR_pq = jobsStatsPerUser[site][taskSpec.gshare]["_total"]["nRunning"]
                    except KeyError:
                        nR_pq = nRunning_pq_in_gshare
                    # evaluate max nQueue per PQ
                    nQ_pq_limit_map = {
                        "base_limit": base_queue_length_per_pq,
                        "static_limit": static_max_queue_running_ratio * nR_pq,
                        "dynamic_limit": max_expected_wait_hour * to_running_rate,
                    }
                    max_nQ_pq = max(nQ_pq_limit_map.values())
                    # evaluate fraction per user
                    user_fraction_map = {
                        "equal_distr": 1 / (nUsers_pq),
                        "prop_to_nR": nR_pq_user / nR_pq if nR_pq > 0 else 0,
                    }
                    max_user_fraction = max(user_fraction_map.values())
                    # evaluate max nQueue per PQ per user
                    nQ_pq_user_limit_map = {
                        "constant_base_user_limit": base_default_queue_length_per_pq_user,
                        "ratio_base_user_limit": base_queue_ratio_on_pq * nR_pq,
                        "dynamic_user_limit": max_nQ_pq * max_user_fraction,
                    }
                    max_nQ_pq_user = max(nQ_pq_user_limit_map.values())
                    # fill in metrics for the site
                    bw_map["user_r"] = nR_pq_user
                    bw_map["user_q_len"] = nQ_pq_user
                    bw_map["max_q_len"] = max_nQ_pq_user
                    bw_map["rem_q_len"] = max(bw_map["max_q_len"] - bw_map["user_q_len"], 0)
                    site_class_rem_q_len_dict[bw_map["class"]] += bw_map["rem_q_len"]
                    total_rem_q_len += bw_map["rem_q_len"]
                # main weight, for User Analysis, determined by number of jobs to submit to each site class
                main_weight_site_class_dict = {1: weight_epsilon_init, 0: weight_epsilon_init, -1: weight_epsilon_init}
                if taskSpec.gshare in ["User Analysis", "Express Analysis"]:
                    n_jobs_to_submit_rem = min(n_jobs_to_submit, total_rem_q_len)
                    if task_class_value == -1:
                        # C-task
                        main_weight_site_class_dict[-1] = n_jobs_to_submit_rem
                    elif task_class_value == 0:
                        # B-task
                        main_weight_site_class_dict[0] = min(n_jobs_to_submit_rem, site_class_rem_q_len_dict[0])
                        n_jobs_to_submit_rem -= main_weight_site_class_dict[0]
                        main_weight_site_class_dict[-1] = n_jobs_to_submit_rem
                        if main_weight_site_class_dict[0] > 0:
                            main_weight_site_class_dict[0] -= weight_epsilon_mid_lo
                            main_weight_site_class_dict[-1] += weight_epsilon_mid_lo
                    else:
                        # S-task or A-task or unclassified task
                        main_weight_site_class_dict[1] = min(n_jobs_to_submit_rem, site_class_rem_q_len_dict[1])
                        n_jobs_to_submit_rem -= main_weight_site_class_dict[1]
                        main_weight_site_class_dict[0] = min(n_jobs_to_submit_rem, site_class_rem_q_len_dict[0])
                        n_jobs_to_submit_rem -= main_weight_site_class_dict[0]
                        main_weight_site_class_dict[-1] = n_jobs_to_submit_rem
                        if main_weight_site_class_dict[1] > 0:
                            main_weight_site_class_dict[1] -= weight_epsilon_hi_mid + weight_epsilon_hi_lo
                            main_weight_site_class_dict[0] += weight_epsilon_hi_mid
                            main_weight_site_class_dict[-1] += weight_epsilon_hi_lo
                # find the weights
                for site in weight_comparison_avail_sites:
                    bw_map = basic_weight_compar_map[site]
                    # main weight by site & task class for User Analysis, and constant for group shares
                    nbw_main = n_jobs_to_submit
                    if taskSpec.gshare in ["User Analysis", "Express Analysis"]:
                        nbw_main = main_weight_site_class_dict[bw_map["class"]]
                    # secondary weight proportional to remaing queue length
                    nbw_sec = 1
                    if taskSpec.gshare in ["User Analysis", "Express Analysis"]:
                        _nbw_numer = max(bw_map["rem_q_len"] - nbw_main / site_class_n_site_dict[bw_map["class"]], nbw_main * 0.001)
                        reduced_site_class_rem_q_len = site_class_rem_q_len_dict[bw_map["class"]] - nbw_main
                        if reduced_site_class_rem_q_len > 0 and not inputChunk.isMerging:
                            nbw_sec = _nbw_numer / reduced_site_class_rem_q_len
                        elif site_class_rem_q_len_dict[bw_map["class"]] > 0:
                            nbw_sec = bw_map["rem_q_len"] / site_class_rem_q_len_dict[bw_map["class"]]
                        elif site_class_n_site_dict[bw_map["class"]] > 0:
                            nbw_sec = 1 / site_class_n_site_dict[bw_map["class"]]
                    else:
                        reduced_total_rem_q_len = total_rem_q_len - nbw_main
                        _nbw_numer = max(bw_map["rem_q_len"] - nbw_main / n_avail_sites, nbw_main * 0.001)
                        if reduced_total_rem_q_len > 0 and not inputChunk.isMerging:
                            nbw_sec = _nbw_numer / reduced_total_rem_q_len
                        elif total_rem_q_len > 0:
                            nbw_sec = bw_map["rem_q_len"] / total_rem_q_len
                        elif site_class_n_site_dict[bw_map["class"]] > 0:
                            nbw_sec = 1 / n_avail_sites
                    # new basic weight
                    new_basic_weight = nbw_main * nbw_sec + 2**-20
                    bw_map["new"] = new_basic_weight
                # log message to compare weights
                orig_sum = 0
                new_sum = 0
                for bw_map in basic_weight_compar_map.values():
                    orig_sum += bw_map["orig"]
                    new_sum += bw_map["new"]
                for site in basic_weight_compar_map:
                    bw_map = basic_weight_compar_map[site]
                    if bw_map["nr"] == 0:
                        trr_over_r = None
                    else:
                        trr_over_r = bw_map["trr"] / bw_map["nr"]
                    bw_map["trr_over_r"] = f"{trr_over_r:6.3f}" if trr_over_r is not None else "None"
                    if orig_sum == 0:
                        normalized_orig = 0
                    else:
                        normalized_orig = bw_map["orig"] / orig_sum
                    bw_map["normalized_orig"] = normalized_orig
                    if new_sum == 0:
                        normalized_new = 0
                    else:
                        normalized_new = bw_map["new"] / new_sum
                    bw_map["normalized_new"] = normalized_new
                prt_str_list = []
                prt_str_temp = (
                    ""
                    " {site:>30} |"
                    " {class:>2} |"
                    " {nq:>6} |"
                    " {nr:>6} |"
                    " {trr:7.2f} |"
                    " {user_q_len:>5} |"
                    " {max_q_len:9.2f} |"
                    " {rem_q_len:9.2f} |"
                    " {orig:7.2f} |"
                    " {new:7.3f} |"
                    " {normalized_orig:6.1%} |"
                    " {normalized_new:6.1%} |"
                )
                prt_str_title = (
                    ""
                    " {site:>30} |"
                    " {cl:>2} |"
                    " {nq:>6} |"
                    " {nr:>6} |"
                    " {trr:>7} |"
                    " {user_q_len:>5} |"
                    " {max_q_len:>9} |"
                    " {rem_q_len:>9} |"
                    " {orig:>7} |"
                    " {new:>7} |"
                    " {normalized_orig:>6} |"
                    " {normalized_new:>6} |"
                ).format(
                    site="Site",
                    cl="Cl",
                    nq="Q",
                    nr="R",
                    trr="TRR",
                    user_q_len="UserQ",
                    max_q_len="UserQ_max",
                    rem_q_len="UserQ_rem",
                    orig="Wb_orig",
                    new="Wb_new",
                    normalized_orig="orig_%",
                    normalized_new="new_%",
                )
                prt_str_list.append(prt_str_title)
                for site in sorted(basic_weight_compar_map):
                    bw_map = basic_weight_compar_map[site]
                    prt_str = prt_str_temp.format(site=site, **{k: (x if x is not None else math.nan) for k, x in bw_map.items()})
                    prt_str_list.append(prt_str)
                tmpLog.info(f"gshare: {taskSpec.gshare} ,{' merging,' if inputChunk.isMerging else ''} task_class: {task_class_value}")
                tmpLog.debug(
                    "WEIGHT-COMPAR: for gshare={},{} cl={}, nJobsEst={} got \n{}".format(
                        taskSpec.gshare, (" merging," if inputChunk.isMerging else ""), task_class_value, n_jobs_to_submit, "\n".join(prt_str_list)
                    )
                )
        except Exception as e:
            tmpLog.error(f"{traceback.format_exc()}")
        # choose basic weight
        _basic_weight_version = "new"
        # finish computing weight
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            bw_map = basic_weight_compar_map[tmpSiteName]
            # fill basic weight
            weight = bw_map[_basic_weight_version]
            # penalty according to throttled jobs
            nThrottled = 0
            if tmpSiteName in remoteSourceList:
                nThrottled = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, "throttled", workQueue_tag=taskSpec.gshare)
                weight /= float(nThrottled + 1)
            # normalize weights by taking data availability into account
            diskNorm = 10
            tapeNorm = 1000
            localSize = totalSize
            if checkDataLocality and not useUnionLocality:
                tmpDataWeight = 1
                if tmpSiteName in dataWeight:
                    weight *= dataWeight[tmpSiteName]
                    tmpDataWeight = dataWeight[tmpSiteName]
            else:
                tmpDataWeight = 1
                if totalSize > 0:
                    if tmpSiteName in totalDiskSizeMap:
                        tmpDataWeight += totalDiskSizeMap[tmpSiteName] / diskNorm
                        localSize = totalDiskSizeMap[tmpSiteName]
                    elif tmpSiteName in totalTapeSizeMap:
                        tmpDataWeight += totalTapeSizeMap[tmpSiteName] / tapeNorm
                        localSize = totalTapeSizeMap[tmpSiteName]
                weight *= tmpDataWeight
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName, tmpSiteName)
            # preassigned
            if sitePreAssigned and tmpSiteName == preassignedSite:
                preSiteCandidateSpec = siteCandidateSpec
            # override attributes
            siteCandidateSpec.override_attribute("maxwdir", newMaxwdir.get(tmpSiteName))
            if cmt_config:
                platforms = resolved_platforms.get(tmpSiteName)
                if platforms:
                    siteCandidateSpec.override_attribute("platforms", platforms)
            # set weight
            siteCandidateSpec.weight = weight
            tmpStr = (
                f"""weight={weight:.7f} """
                f"""gshare={taskSpec.gshare} class={bw_map["class"]} trr={bw_map["trr"]:.3f} """
                f"""userR={bw_map["user_r"]} userQ={bw_map["user_q_len"]} userQRem={bw_map["rem_q_len"]:.3f} """
                f"""nRunning={bw_map["nr"]} nDefined={bw_map["nDefined"]} nActivated={bw_map["nActivated"]} """
                f"""nStarting={bw_map["nStarting"]} nAssigned={bw_map["nAssigned"]} """
                f"""nFailed={bw_map["nFailed"]} nClosed={bw_map["nClosed"]} nFinished={bw_map["nFinished"]} """
                f"""dataW={tmpDataWeight} totalInGB={totalSize} localInGB={localSize} nFiles={totalNumFiles} """
            )
            weightStr[tmpPseudoSiteName] = tmpStr
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if weight not in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)
        # sort candidates by weights and data availability
        candidateSpecListWithData = []
        weightList = sorted(weightMap.keys())
        weightList.reverse()
        for weightVal in weightList:
            sitesWithWeight = weightMap[weightVal]
            for tmp_site in list(sitesWithWeight):
                if tmp_site in site_list_with_data:
                    candidateSpecListWithData.append(tmp_site)
                    sitesWithWeight.remove(tmp_site)
            random.shuffle(sitesWithWeight)
            candidateSpecList += sitesWithWeight
        candidateSpecList = candidateSpecListWithData + candidateSpecList
        # limit the number of sites
        if not hasDDS:
            if taskSpec.getNumSitesPerJob() > 1:
                # use the number of sites in the task spec for job cloning
                maxNumSites = max(len(site_list_with_data), taskSpec.getNumSitesPerJob())
            else:
                maxNumSites = 10
        else:
            # use all sites for distributed datasets
            maxNumSites = None
        # remove problematic sites
        oldScanSiteList = copy.copy(scanSiteList)
        candidateSpecList = AtlasBrokerUtils.skipProblematicSites(
            candidateSpecList, set(problematic_sites_dict), sitesUsedByTask, preSiteCandidateSpec, maxNumSites, tmpLog
        )
        # append preassigned
        if sitePreAssigned and preSiteCandidateSpec is not None and preSiteCandidateSpec not in candidateSpecList:
            candidateSpecList.append(preSiteCandidateSpec)
        # collect site names
        scanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            scanSiteList.append(siteCandidateSpec.siteName)
        # append candidates
        newScanSiteList = []
        msgList = []
        below_min_weight = []
        for siteCandidateSpec in candidateSpecList:
            tmpPseudoSiteName = siteCandidateSpec.siteName
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # preassigned
            if sitePreAssigned and tmpSiteName != preassignedSite:
                tmpLog.info(f"  skip site={tmpPseudoSiteName} non pre-assigned site criteria=-nonpreassigned")
                try:
                    del weightStr[tmpPseudoSiteName]
                except Exception:
                    pass
                continue
            # below minimum brokerage weight
            if min_weight > 0 and siteCandidateSpec.weight < min_weight:
                below_min_weight.append(tmpSiteName)
                continue
            # set available files
            if inputChunk.getDatasets() == [] or (not checkDataLocality and not tmpSiteSpec.use_only_local_data()):
                isAvailable = True
            else:
                isAvailable = False
            isAvailableBase = isAvailable
            for tmpDatasetName, availableFiles in availableFileMap.items():
                tmpDatasetSpec = inputChunk.getDatasetWithName(tmpDatasetName)
                isAvailable = isAvailableBase
                # check remote files
                if tmpSiteName in remoteSourceList and tmpDatasetName in remoteSourceList[tmpSiteName] and not tmpSiteSpec.use_only_local_data():
                    for tmpRemoteSite in remoteSourceList[tmpSiteName][tmpDatasetName]:
                        if tmpRemoteSite in availableFiles and len(tmpDatasetSpec.Files) <= len(availableFiles[tmpRemoteSite]["localdisk"]):
                            # use only remote disk files
                            siteCandidateSpec.add_remote_files(availableFiles[tmpRemoteSite]["localdisk"])
                            # set remote site and access protocol
                            siteCandidateSpec.remoteProtocol = allowedRemoteProtocol
                            siteCandidateSpec.remoteSource = tmpRemoteSite
                            isAvailable = True
                            break
                # local files
                if tmpSiteName in availableFiles:
                    if (
                        len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]["localdisk"])
                        or len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]["cache"])
                        or len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]["localtape"])
                        or tmpDatasetSpec.isDistributed()
                        or true_complete_disk_ok[tmpDatasetName] is False
                        or ((checkDataLocality is False or useUnionLocality) and not tmpSiteSpec.use_only_local_data())
                    ):
                        siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]["localdisk"])
                        # add cached files to local list since cached files go to pending when reassigned
                        siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]["cache"])
                        siteCandidateSpec.add_local_tape_files(availableFiles[tmpSiteName]["localtape"])
                        siteCandidateSpec.add_cache_files(availableFiles[tmpSiteName]["cache"])
                        siteCandidateSpec.add_remote_files(availableFiles[tmpSiteName]["remote"])
                        siteCandidateSpec.addAvailableFiles(availableFiles[tmpSiteName]["all"])
                        isAvailable = True
                    else:
                        tmpMsg = "{0} is incomplete at {1} : nFiles={2} nLocal={3} nCached={4} nTape={5}"
                        tmpLog.debug(
                            tmpMsg.format(
                                tmpDatasetName,
                                tmpPseudoSiteName,
                                len(tmpDatasetSpec.Files),
                                len(availableFiles[tmpSiteName]["localdisk"]),
                                len(availableFiles[tmpSiteName]["cache"]),
                                len(availableFiles[tmpSiteName]["localtape"]),
                            )
                        )
                if not isAvailable:
                    break
            # append
            if not isAvailable:
                tmpLog.info(f"  skip site={siteCandidateSpec.siteName} file unavailable criteria=-fileunavailable")
                try:
                    del weightStr[siteCandidateSpec.siteName]
                except Exception:
                    pass
                continue
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpMsg = "  use site={0} with {1} nLocalDisk={2} nLocalTape={3} nCache={4} nRemote={5} criteria=+use".format(
                siteCandidateSpec.siteName,
                weightStr[siteCandidateSpec.siteName],
                len(siteCandidateSpec.localDiskFiles),
                len(siteCandidateSpec.localTapeFiles),
                len(siteCandidateSpec.cacheFiles),
                len(siteCandidateSpec.remoteFiles),
            )
            msgList.append(tmpMsg)
            del weightStr[siteCandidateSpec.siteName]
        # dump
        for tmpPseudoSiteName in oldScanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            tmpWeightStr = None
            if tmpSiteName in weightStr:
                tmpWeightStr = weightStr[tmpSiteName]
            elif tmpPseudoSiteName in weightStr:
                tmpWeightStr = weightStr[tmpPseudoSiteName]
            if tmpWeightStr is not None:
                if tmpSiteName in problematic_sites_dict:
                    bad_reasons = " ; ".join(list(problematic_sites_dict[tmpSiteName]))
                    tmpMsg = f"  skip site={tmpPseudoSiteName} {bad_reasons} ; with {tmpWeightStr} criteria=-badsite"
                elif tmpSiteName in below_min_weight:
                    tmpMsg = f"  skip site={tmpPseudoSiteName} due to weight below the minimum {min_weight_param}={min_weight} with {tmpWeightStr} criteria=-below_min_weight"
                else:
                    tmpMsg = f"  skip site={tmpPseudoSiteName} due to low weight and not-used by old jobs with {tmpWeightStr} criteria=-low_weight"
                tmpLog.info(tmpMsg)
        for tmpMsg in msgList:
            tmpLog.info(tmpMsg)
        scanSiteList = newScanSiteList
        self.add_summary_message(oldScanSiteList, scanSiteList, "final check")
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error("no candidates")
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        self.dump_summary(tmpLog, scanSiteList)
        # return
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, inputChunk
