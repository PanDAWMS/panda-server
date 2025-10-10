import copy
import datetime
import math
import random
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import (
    ListWithLock,
    MapWithLock,
    ThreadPool,
    WorkerThread,
)
from pandajedi.jedirefine import RefinerUtils
from pandaserver.dataservice import DataServiceUtils

from . import AtlasBrokerUtils
from .AtlasProdJobBroker import AtlasProdJobBroker
from .TaskBrokerBase import TaskBrokerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# brokerage for ATLAS production
class AtlasProdTaskBroker(TaskBrokerBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskBrokerBase.__init__(self, taskBufferIF, ddmIF)

    # main to check
    def doCheck(self, taskSpecList):
        return self.SC_SUCCEEDED, {}

    # main to assign
    def doBrokerage(self, inputList, vo, prodSourceLabel, workQueue, resource_name):
        # list with a lock
        inputListWorld = ListWithLock([])

        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug("start doBrokerage")

        # return for failure
        retTmpError = self.SC_FAILED
        tmpLog.debug(f"vo={vo} label={prodSourceLabel} queue={workQueue.queue_name} resource_name={resource_name} nTasks={len(inputList)}")

        # build the map with Remaining Work by priority
        allRwMap = {}

        # loop over all tasks and build the list of WORLD tasks. Nowadays all tasks are WORLD
        for tmpJediTaskID, tmpInputList in inputList:
            for taskSpec, cloudName, inputChunk in tmpInputList:
                if taskSpec.useWorldCloud():
                    inputListWorld.append((taskSpec, inputChunk))

        # broker WORLD tasks
        if inputListWorld:
            # thread pool
            threadPool = ThreadPool()
            # get full RW for WORLD
            fullRWs = self.taskBufferIF.calculateWorldRWwithPrio_JEDI(vo, prodSourceLabel, None, None)
            if fullRWs is None:
                tmpLog.error("failed to calculate full WORLD RW")
                return retTmpError
            # get RW per priority
            for taskSpec, inputChunk in inputListWorld:
                if taskSpec.currentPriority not in allRwMap:
                    tmpRW = self.taskBufferIF.calculateWorldRWwithPrio_JEDI(vo, prodSourceLabel, workQueue, taskSpec.currentPriority)
                    if tmpRW is None:
                        tmpLog.error(f"failed to calculate RW with prio={taskSpec.currentPriority}")
                        return retTmpError
                    allRwMap[taskSpec.currentPriority] = tmpRW

            # live counter for RWs
            liveCounter = MapWithLock(allRwMap)
            # make workers
            ddmIF = self.ddmIF.getInterface(vo)
            for iWorker in range(4):
                thr = AtlasProdTaskBrokerThread(inputListWorld, threadPool, self.taskBufferIF, ddmIF, fullRWs, liveCounter, workQueue)
                thr.start()
            threadPool.join(60 * 10)
        # return
        tmpLog.debug("doBrokerage done")
        return self.SC_SUCCEEDED


# thread for real worker
class AtlasProdTaskBrokerThread(WorkerThread):
    # constructor
    def __init__(self, inputList, threadPool, taskbufferIF, ddmIF, fullRW, prioRW, workQueue):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributres
        self.inputList = inputList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = "taskbrokerage"
        self.fullRW = fullRW
        self.prioRW = prioRW
        self.numTasks = 0
        self.workQueue = workQueue
        self.summaryList = None

    # init summary list
    def init_summary_list(self, header, comment, initial_list):
        self.summaryList = []
        self.summaryList.append(f"===== {header} =====")
        if comment:
            self.summaryList.append(comment)
        self.summaryList.append(f"the number of initial candidates: {len(initial_list)}")

    # dump summary
    def dump_summary(self, tmp_log, final_candidates=None):
        if not self.summaryList:
            return
        tmp_log.info("")
        for m in self.summaryList:
            tmp_log.info(m)
        if not final_candidates:
            final_candidates = []
        tmp_log.info(f"the number of final candidates: {len(final_candidates)}")
        tmp_log.info("")

    # make summary
    def add_summary_message(self, old_list, new_list, message):
        if old_list and len(old_list) != len(new_list):
            red = int(math.ceil(((len(old_list) - len(new_list)) * 100) / len(old_list)))
            self.summaryList.append(f"{len(old_list):>5} -> {len(new_list):>3} candidates, {red:>3}% cut : {message}")

    # post-process for errors
    def post_process_for_error(self, task_spec, tmp_log, msg, dump_summary=True):
        if dump_summary:
            self.dump_summary(tmp_log)
        tmp_log.error(msg)
        task_spec.resetChangedList()
        task_spec.setErrDiag(tmp_log.uploadLog(task_spec.jediTaskID))
        self.taskBufferIF.updateTask_JEDI(task_spec, {"jediTaskID": task_spec.jediTaskID}, oldStatus=["assigning"], updateDEFT=False, setFrozenTime=False)

    # main function
    def runImpl(self):
        # cutoff for disk in TB
        diskThreshold = self.taskBufferIF.getConfigValue(self.msgType, f"DISK_THRESHOLD_{self.workQueue.queue_name}", "jedi", "atlas")
        if diskThreshold is None:
            diskThreshold = self.taskBufferIF.getConfigValue(self.msgType, "DISK_THRESHOLD", "jedi", "atlas")
            if diskThreshold is None:
                diskThreshold = 100
        diskThreshold *= 1024
        # cutoff for free disk in TB
        free_disk_cutoff = self.taskBufferIF.getConfigValue(self.msgType, f"FREE_DISK_CUTOFF", "jedi", "atlas")
        if free_disk_cutoff is None:
            free_disk_cutoff = 1000
        # dataset type to ignore file availability check
        datasetTypeToSkipCheck = ["log"]
        # thresholds for data availability check
        thrInputSize = self.taskBufferIF.getConfigValue(self.msgType, "INPUT_SIZE_THRESHOLD", "jedi", "atlas")
        if thrInputSize is None:
            thrInputSize = 1
        thrInputSize *= 1024 * 1024 * 1024
        thrInputNum = self.taskBufferIF.getConfigValue(self.msgType, "INPUT_NUM_THRESHOLD", "jedi", "atlas")
        if thrInputNum is None:
            thrInputNum = 100
        thrInputSizeFrac = self.taskBufferIF.getConfigValue(self.msgType, "INPUT_SIZE_FRACTION", "jedi", "atlas")
        if thrInputSizeFrac is None:
            thrInputSizeFrac = 10
        thrInputSizeFrac = float(thrInputSizeFrac) / 100
        thrInputNumFrac = self.taskBufferIF.getConfigValue(self.msgType, "INPUT_NUM_FRACTION", "jedi", "atlas")
        if thrInputNumFrac is None:
            thrInputNumFrac = 10
        thrInputNumFrac = float(thrInputNumFrac) / 100
        cutOffRW = 50
        negWeightTape = 0.001
        minIoIntensityWithLD = self.taskBufferIF.getConfigValue(self.msgType, "MIN_IO_INTENSITY_WITH_LOCAL_DATA", "jedi", "atlas")
        if minIoIntensityWithLD is None:
            minIoIntensityWithLD = 200
        minInputSizeWithLD = self.taskBufferIF.getConfigValue(self.msgType, "MIN_INPUT_SIZE_WITH_LOCAL_DATA", "jedi", "atlas")
        if minInputSizeWithLD is None:
            minInputSizeWithLD = 10000
        maxTaskPrioWithLD = self.taskBufferIF.getConfigValue(self.msgType, "MAX_TASK_PRIO_WITH_LOCAL_DATA", "jedi", "atlas")
        if maxTaskPrioWithLD is None:
            maxTaskPrioWithLD = 800
        # ignore data locality once the period passes (in days)
        data_location_check_period = self.taskBufferIF.getConfigValue(self.msgType, "DATA_LOCATION_CHECK_PERIOD", "jedi", "atlas")
        if not data_location_check_period:
            data_location_check_period = 7
        # main
        lastJediTaskID = None
        siteMapper = self.taskBufferIF.get_site_mapper()
        while True:
            try:
                taskInputList = self.inputList.get(1)
                # no more datasets
                if len(taskInputList) == 0:
                    self.logger.debug(f"{self.__class__.__name__} terminating after processing {self.numTasks} tasks since no more inputs ")
                    return
                # loop over all tasks
                for taskSpec, inputChunk in taskInputList:
                    lastJediTaskID = taskSpec.jediTaskID
                    # make logger
                    tmpLog = MsgWrapper(self.logger, f"<jediTaskID={taskSpec.jediTaskID}>", monToken=f"jediTaskID={taskSpec.jediTaskID}")
                    tmpLog.debug("start")
                    tmpLog.info(f"thrInputSize:{thrInputSize} thrInputNum:{thrInputNum} thrInputSizeFrac:{thrInputSizeFrac} thrInputNumFrac:{thrInputNumFrac}")
                    tmpLog.info(f"full-chain:{taskSpec.get_full_chain()} ioIntensity={taskSpec.ioIntensity}")
                    # read task parameters
                    try:
                        taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
                        taskParamMap = RefinerUtils.decodeJSON(taskParam)
                    except Exception:
                        tmpLog.error("failed to read task params")
                        taskSpec.resetChangedList()
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        self.taskBufferIF.updateTask_JEDI(
                            taskSpec, {"jediTaskID": taskSpec.jediTaskID}, oldStatus=["assigning"], updateDEFT=False, setFrozenTime=False
                        )
                        continue
                    # RW
                    taskRW = self.taskBufferIF.calculateTaskWorldRW_JEDI(taskSpec.jediTaskID)
                    # get nuclei
                    nucleusList = copy.copy(siteMapper.nuclei)
                    if taskSpec.get_full_chain():
                        # use satellites with bareNucleus as nuclei for full chain
                        for tmpNucleus, tmpNucleusSpec in siteMapper.satellites.items():
                            if tmpNucleusSpec.get_bare_nucleus_mode():
                                nucleusList[tmpNucleus] = tmpNucleusSpec
                    # init summary list
                    self.init_summary_list("Task brokerage summary", None, nucleusList)
                    if taskSpec.nucleus in siteMapper.nuclei:
                        candidateNucleus = taskSpec.nucleus
                    elif taskSpec.nucleus in siteMapper.satellites:
                        nucleusList = siteMapper.satellites
                        candidateNucleus = taskSpec.nucleus
                    else:
                        tmpLog.info(f"got {len(nucleusList)} candidates")
                        ######################################
                        # check data
                        dataset_availability_info = {}
                        to_skip = False
                        for datasetSpec in inputChunk.getDatasets():
                            # only for real datasets
                            if datasetSpec.isPseudo():
                                continue
                            # ignore DBR
                            if DataServiceUtils.isDBR(datasetSpec.datasetName):
                                continue
                            # skip locality check
                            if DataServiceUtils.getDatasetType(datasetSpec.datasetName) in datasetTypeToSkipCheck:
                                continue
                            # primary only
                            if taskParamMap.get("taskBrokerOnMaster") is True and not datasetSpec.isMaster():
                                continue
                            # use deep scan for primary dataset unless data carousel
                            if datasetSpec.isMaster() and not taskSpec.inputPreStaging():
                                deepScan = True
                            else:
                                deepScan = False
                            # get nuclei where data is available
                            tmp_status, tmp_data_map, remote_source_available = AtlasBrokerUtils.getNucleiWithData(
                                siteMapper, self.ddmIF, datasetSpec.datasetName, list(nucleusList.keys()), deepScan
                            )
                            if tmp_status != Interaction.SC_SUCCEEDED:
                                self.post_process_for_error(taskSpec, tmpLog, f"failed to get nuclei where data is available, since {tmp_data_map}", False)
                                to_skip = True
                                break
                            # sum
                            for tmpNucleus, tmpVals in tmp_data_map.items():
                                if tmpNucleus not in dataset_availability_info:
                                    dataset_availability_info[tmpNucleus] = tmpVals
                                else:
                                    dataset_availability_info[tmpNucleus] = dict(
                                        (k, v + tmpVals[k]) for (k, v) in dataset_availability_info[tmpNucleus].items() if not isinstance(v, bool)
                                    )
                                # set remote_source_available to True if any is readable over WAN
                                if tmpVals.get("can_be_remote_source"):
                                    dataset_availability_info[tmpNucleus]["can_be_remote_source"] = True
                            if not remote_source_available:
                                self.post_process_for_error(
                                    taskSpec, tmpLog, f"dataset={datasetSpec.datasetName} is incomplete/missing at online endpoints with read_wan=ON", False
                                )
                                to_skip = True
                                break
                        if to_skip:
                            continue
                        ######################################
                        # check status
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            if tmpNucleusSpec.state not in ["ACTIVE"]:
                                tmpLog.info(f"  skip nucleus={tmpNucleus} due to status={tmpNucleusSpec.state} criteria=-status")
                            else:
                                newNucleusList[tmpNucleus] = tmpNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info(f"{len(nucleusList)} candidates passed status check")
                        self.add_summary_message(oldNucleusList, nucleusList, "status check")
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                            continue
                        ######################################
                        # check status of transfer backlog
                        t1Weight = taskSpec.getT1Weight()
                        if t1Weight < 0:
                            tmpLog.info("skip transfer backlog check due to negative T1Weight")
                        else:
                            newNucleusList = {}
                            oldNucleusList = copy.copy(nucleusList)
                            backlogged_nuclei = self.taskBufferIF.getBackloggedNuclei()
                            for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                                if tmpNucleus in backlogged_nuclei:
                                    tmpLog.info(f"  skip nucleus={tmpNucleus} due to long transfer backlog criteria=-transfer_backlog")
                                else:
                                    newNucleusList[tmpNucleus] = tmpNucleusSpec
                            nucleusList = newNucleusList
                            tmpLog.info(f"{len(nucleusList)} candidates passed transfer backlog check")
                            self.add_summary_message(oldNucleusList, nucleusList, "transfer backlog check")
                            if not nucleusList:
                                self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                                continue
                        ######################################
                        # check endpoint
                        fractionFreeSpace = {}
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        tmpStat, tmpDatasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID, ["output", "log"])
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            to_skip = False
                            origNucleusSpec = tmpNucleusSpec
                            for tmpDatasetSpec in tmpDatasetSpecList:
                                tmpNucleusSpec = origNucleusSpec
                                # use secondary nucleus for full-chain if defined
                                if taskSpec.get_full_chain() and tmpNucleusSpec.get_secondary_nucleus():
                                    tmpNucleusSpec = siteMapper.getNucleus(tmpNucleusSpec.get_secondary_nucleus())
                                # ignore distributed datasets
                                if DataServiceUtils.getDistributedDestination(tmpDatasetSpec.storageToken) is not None:
                                    continue
                                # get endpoint with the pattern
                                tmpEP = tmpNucleusSpec.getAssociatedEndpoint(tmpDatasetSpec.storageToken)
                                tmp_ddm_endpoint_name = tmpEP["ddm_endpoint_name"]
                                if tmpEP is None:
                                    tmpLog.info(f"  skip nucleus={tmpNucleus} since no endpoint with {tmpDatasetSpec.storageToken} criteria=-match")
                                    to_skip = True
                                    break
                                # check blacklist
                                read_wan_status = tmpEP["detailed_status"].get("read_wan")
                                if read_wan_status in ["OFF", "TEST"]:
                                    tmpLog.info(
                                        f"  skip nucleus={tmpNucleus} since {tmp_ddm_endpoint_name} has read_wan={read_wan_status} criteria=-source_blacklist"
                                    )
                                    to_skip = True
                                    break
                                write_wan_status = tmpEP["detailed_status"].get("write_wan")
                                if write_wan_status in ["OFF", "TEST"]:
                                    tmpLog.info(
                                        f"  skip nucleus={tmpNucleus} since {tmp_ddm_endpoint_name} has write_wan={read_wan_status} criteria=-destination_blacklist"
                                    )
                                    to_skip = True
                                    break
                                # check space
                                tmpSpaceSize = 0
                                if tmpEP["space_free"]:
                                    tmpSpaceSize += tmpEP["space_free"]
                                if tmpEP["space_expired"]:
                                    tmpSpaceSize += tmpEP["space_expired"]
                                tmpSpaceToUse = 0
                                if tmpNucleus in self.fullRW:
                                    # 0.25GB per cpuTime/corePower/day
                                    tmpSpaceToUse = int(self.fullRW[tmpNucleus] / 10 / 24 / 3600 * 0.25)
                                if tmpSpaceSize - tmpSpaceToUse < diskThreshold:
                                    tmpLog.info(
                                        "  skip nucleus={0} since disk shortage (free {1} TB - reserved {2} TB < thr {3} TB) at endpoint {4} criteria=-space".format(
                                            tmpNucleus, tmpSpaceSize // 1024, tmpSpaceToUse // 1024, diskThreshold // 1024, tmpEP["ddm_endpoint_name"]
                                        )
                                    )
                                    to_skip = True
                                    break
                                # keep fraction of free space
                                if tmpNucleus not in fractionFreeSpace:
                                    fractionFreeSpace[tmpNucleus] = {"total": 0, "free": 0}
                                try:
                                    tmpOld = float(fractionFreeSpace[tmpNucleus]["free"]) / float(fractionFreeSpace[tmpNucleus]["total"])
                                except Exception:
                                    tmpOld = None
                                try:
                                    tmpNew = float(tmpSpaceSize - tmpSpaceToUse) / float(tmpEP["space_total"])
                                except Exception:
                                    tmpNew = None
                                if tmpNew is not None and (tmpOld is None or tmpNew < tmpOld):
                                    fractionFreeSpace[tmpNucleus] = {"total": tmpEP["space_total"], "free": tmpSpaceSize - tmpSpaceToUse}
                            if not to_skip:
                                newNucleusList[tmpNucleus] = origNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info(f"{len(nucleusList)} candidates passed endpoint check with DISK_THRESHOLD={diskThreshold // 1024} TB")
                        self.add_summary_message(oldNucleusList, nucleusList, "storage endpoint check")
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                            continue
                        ######################################
                        # ability to execute jobs
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        # get all panda sites
                        tmpSiteList = []
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            tmpSiteList += tmpNucleusSpec.allPandaSites
                        tmpSiteList = list(set(tmpSiteList))
                        tmpLog.debug("===== start for job check")
                        jobBroker = AtlasProdJobBroker(self.ddmIF, self.taskBufferIF)
                        tmp_status, tmp_data_map = jobBroker.doBrokerage(taskSpec, taskSpec.cloud, inputChunk, None, True, tmpSiteList, tmpLog)
                        tmpLog.debug("===== done for job check")
                        if tmp_status != Interaction.SC_SUCCEEDED:
                            tmpLog.error("no sites can run jobs")
                            taskSpec.resetChangedList()
                            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                            self.taskBufferIF.updateTask_JEDI(
                                taskSpec, {"jediTaskID": taskSpec.jediTaskID}, oldStatus=["assigning"], updateDEFT=False, setFrozenTime=False
                            )
                            continue
                        okNuclei = set()
                        for tmpSite in tmp_data_map:
                            siteSpec = siteMapper.getSite(tmpSite)
                            okNuclei.add(siteSpec.pandasite)
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            if tmpNucleus in okNuclei:
                                newNucleusList[tmpNucleus] = tmpNucleusSpec
                            else:
                                tmpLog.info(f"  skip nucleus={tmpNucleus} due to missing ability to run jobs criteria=-job")
                        nucleusList = newNucleusList
                        tmpLog.info(f"{len(nucleusList)} candidates passed job check")
                        self.add_summary_message(oldNucleusList, nucleusList, "job check")
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                            continue
                        ######################################
                        # data locality
                        time_now = naive_utcnow()
                        if taskSpec.frozenTime and time_now - taskSpec.frozenTime > datetime.timedelta(days=data_location_check_period):
                            tmpLog.info(f"disabled data check since the task was in assigning for " f"{data_location_check_period} days")
                        else:
                            dataset_availability_info = {k: v for k, v in dataset_availability_info.items() if k in nucleusList}
                            if dataset_availability_info != {}:
                                newNucleusList = {}
                                oldNucleusList = copy.copy(nucleusList)
                                # skip if no data
                                skipMsgList = []
                                for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                                    if taskSpec.inputPreStaging() and dataset_availability_info[tmpNucleus]["ava_num_any"] > 0:
                                        # use incomplete replicas for data carousel since the completeness is guaranteed
                                        newNucleusList[tmpNucleus] = tmpNucleusSpec
                                    elif (
                                        dataset_availability_info[tmpNucleus]["tot_size"] > thrInputSize
                                        and dataset_availability_info[tmpNucleus]["ava_size_any"]
                                        < dataset_availability_info[tmpNucleus]["tot_size"] * thrInputSizeFrac
                                    ):
                                        tmpMsg = "  skip nucleus={0} due to insufficient input size {1}B < {2}*{3} criteria=-insize".format(
                                            tmpNucleus,
                                            dataset_availability_info[tmpNucleus]["ava_size_any"],
                                            dataset_availability_info[tmpNucleus]["tot_size"],
                                            thrInputSizeFrac,
                                        )
                                        skipMsgList.append(tmpMsg)
                                    elif (
                                        dataset_availability_info[tmpNucleus]["tot_num"] > thrInputNum
                                        and dataset_availability_info[tmpNucleus]["ava_num_any"]
                                        < dataset_availability_info[tmpNucleus]["tot_num"] * thrInputNumFrac
                                    ):
                                        tmpMsg = "  skip nucleus={0} due to short number of input files {1} < {2}*{3} criteria=-innum".format(
                                            tmpNucleus,
                                            dataset_availability_info[tmpNucleus]["ava_num_any"],
                                            dataset_availability_info[tmpNucleus]["tot_num"],
                                            thrInputNumFrac,
                                        )
                                        skipMsgList.append(tmpMsg)
                                    else:
                                        newNucleusList[tmpNucleus] = tmpNucleusSpec
                                totInputSize = list(dataset_availability_info.values())[0]["tot_size"] / 1024 / 1024 / 1024
                                data_locality_check_str = (
                                    f"(ioIntensity ({taskSpec.ioIntensity}) is None or less than {minIoIntensityWithLD} kBPerS "
                                    f"and input size ({int(totInputSize)} GB) is less than "
                                    f"{minInputSizeWithLD}) "
                                    f"or (task.currentPriority ("
                                    f"{taskSpec.currentPriority}) is higher than or equal to {maxTaskPrioWithLD}) "
                                    f"or the task is in assigning for {data_location_check_period} days"
                                )
                                if len(newNucleusList) > 0:
                                    nucleusList = newNucleusList
                                    for tmpMsg in skipMsgList:
                                        tmpLog.info(tmpMsg)
                                elif (
                                    (taskSpec.ioIntensity is None or taskSpec.ioIntensity <= minIoIntensityWithLD) and totInputSize <= minInputSizeWithLD
                                ) or taskSpec.currentPriority >= maxTaskPrioWithLD:
                                    dataset_availability_info = {}
                                    tmpLog.info(f"  disable data locality check since no nucleus has input data, {data_locality_check_str}")
                                else:
                                    # no candidate + unavoidable data locality check
                                    nucleusList = newNucleusList
                                    for tmpMsg in skipMsgList:
                                        tmpLog.info(tmpMsg)
                                    tmpLog.info(f"  the following conditions required to disable data locality check: {data_locality_check_str}")
                                tmpLog.info(f"{len(nucleusList)} candidates passed data check")
                                self.add_summary_message(oldNucleusList, nucleusList, "data check")
                                if not nucleusList:
                                    self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                                    continue
                        ######################################
                        # check for full chain
                        newNucleusList = {}
                        oldNucleusList = copy.copy(nucleusList)
                        parent_full_chain = False
                        if taskSpec.get_full_chain() and taskSpec.jediTaskID != taskSpec.parent_tid:
                            tmpStat, parentTaskSpec = self.taskBufferIF.getTaskWithID_JEDI(taskSpec.parent_tid, False)

                            if not tmpStat or parentTaskSpec is None:
                                self.post_process_for_error(taskSpec, tmpLog, f"failed to get parent taskSpec with jediTaskID={taskSpec.parent_tid}", False)
                                continue
                            parent_full_chain = parentTaskSpec.check_full_chain_with_nucleus(siteMapper.getNucleus(parentTaskSpec.nucleus))
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            # nucleus to run only full-chain tasks
                            if tmpNucleusSpec.get_bare_nucleus_mode() == "only" and taskSpec.get_full_chain() is None:
                                tmpLog.info(f"  skip nucleus={tmpNucleus} since only full-chain tasks are allowed criteria=-full_chain")
                                continue
                            # task requirement to run on nucleus with full-chain support
                            if tmpNucleusSpec.get_bare_nucleus_mode() is None and (
                                taskSpec.check_full_chain_with_mode("only") or taskSpec.check_full_chain_with_mode("require")
                            ):
                                tmpLog.info(f"  skip nucleus={tmpNucleus} due to lack of full-chain capability criteria=-full_chain")
                                continue
                            # check parent task
                            if taskSpec.get_full_chain() and parent_full_chain:
                                if tmpNucleus != parentTaskSpec.nucleus:
                                    tmpLog.info(f"  skip nucleus={tmpNucleus} since the parent of the full-chain ran elsewhere criteria=-full_chain")
                                    continue
                            newNucleusList[tmpNucleus] = tmpNucleusSpec
                        nucleusList = newNucleusList
                        tmpLog.info(f"{len(nucleusList)} candidates passed full-chain check")
                        self.add_summary_message(oldNucleusList, nucleusList, "full-chain check")
                        if not nucleusList:
                            self.post_process_for_error(taskSpec, tmpLog, "no candidates")
                            continue
                        self.dump_summary(tmpLog, nucleusList)
                        ######################################
                        # weight
                        self.prioRW.acquire()
                        nucleusRW = self.prioRW[taskSpec.currentPriority]
                        self.prioRW.release()
                        totalWeight = 0
                        nucleusweights = []
                        for tmpNucleus, tmpNucleusSpec in nucleusList.items():
                            if tmpNucleus not in nucleusRW:
                                nucleusRW[tmpNucleus] = 0
                            wStr = "1"
                            # with RW
                            if tmpNucleus in nucleusRW and nucleusRW[tmpNucleus] and nucleusRW[tmpNucleus] >= cutOffRW:
                                weight = 1 / float(nucleusRW[tmpNucleus])
                                wStr += f"/( RW={nucleusRW[tmpNucleus]} )"
                            else:
                                weight = 1
                                wStr += f"/(1 : RW={nucleusRW[tmpNucleus]}<{cutOffRW})"
                            # with data
                            if dataset_availability_info != {}:
                                if dataset_availability_info[tmpNucleus]["tot_size"] > 0:
                                    # use input size only when high IO intensity
                                    if taskSpec.ioIntensity is None or taskSpec.ioIntensity > minIoIntensityWithLD:
                                        weight *= float(dataset_availability_info[tmpNucleus]["ava_size_any"])
                                        weight /= float(dataset_availability_info[tmpNucleus]["tot_size"])
                                        wStr += f"* ( available_input_size_DISKTAPE={dataset_availability_info[tmpNucleus]['ava_size_any']} )"
                                        wStr += f"/ ( total_input_size={dataset_availability_info[tmpNucleus]['tot_size']} )"
                                    # negative weight for tape
                                    if dataset_availability_info[tmpNucleus]["ava_size_any"] > dataset_availability_info[tmpNucleus]["ava_size_disk"]:
                                        weight *= negWeightTape
                                        wStr += f"*( weight_TAPE={negWeightTape} )"
                            # fraction of free space
                            if tmpNucleus in fractionFreeSpace:
                                try:
                                    tmpFrac = float(fractionFreeSpace[tmpNucleus]["free"]) / float(fractionFreeSpace[tmpNucleus]["total"])
                                    weight *= tmpFrac
                                    wStr += f"*( free_space={fractionFreeSpace[tmpNucleus]['free'] // 1024} TB )/( total_space={fractionFreeSpace[tmpNucleus]['total'] // 1024} TB )"
                                    free_disk_in_tb = fractionFreeSpace[tmpNucleus]["free"] // 1024
                                    free_disk_term = min(free_disk_cutoff, free_disk_in_tb)
                                    weight *= free_disk_term
                                    wStr += f"*min( free_space={free_disk_in_tb} TB, FREE_DISK_CUTOFF={free_disk_cutoff} TB )"
                                except Exception:
                                    pass
                            tmpLog.info(f"  use nucleus={tmpNucleus} weight={weight} {wStr} criteria=+use")
                            totalWeight += weight
                            nucleusweights.append((tmpNucleus, weight))
                        ######################################
                        # final selection
                        tgtWeight = random.uniform(0, totalWeight)
                        candidateNucleus = None
                        for tmpNucleus, weight in nucleusweights:
                            tgtWeight -= weight
                            if tgtWeight <= 0:
                                candidateNucleus = tmpNucleus
                                break
                        if candidateNucleus is None:
                            candidateNucleus = nucleusweights[-1][0]
                    ######################################
                    # update
                    nucleusSpec = nucleusList[candidateNucleus]
                    # get output/log datasets
                    tmpStat, tmpDatasetSpecs = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID, ["output", "log"])
                    # get destinations
                    retMap = {taskSpec.jediTaskID: AtlasBrokerUtils.getDictToSetNucleus(nucleusSpec, tmpDatasetSpecs)}
                    tmp_ret = self.taskBufferIF.setCloudToTasks_JEDI(retMap)
                    tmpLog.info(f"  set nucleus={candidateNucleus} with {tmp_ret} criteria=+set")
                    if tmp_ret:
                        tmpMsg = "set task_status=ready"
                        tmpLog.sendMsg(tmpMsg, self.msgType)
                    # update RW table
                    self.prioRW.acquire()
                    for prio, rwMap in self.prioRW.items():
                        if prio > taskSpec.currentPriority:
                            continue
                        if candidateNucleus in rwMap:
                            if not rwMap[candidateNucleus]:
                                rwMap[candidateNucleus] = 0
                            rwMap[candidateNucleus] += taskRW
                        else:
                            rwMap[candidateNucleus] = taskRW
                    self.prioRW.release()
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                errMsg = f"{self.__class__.__name__}.runImpl() failed with {errtype.__name__} {errvalue} "
                errMsg += f"lastJediTaskID={lastJediTaskID} "
                errMsg += traceback.format_exc()
                logger.error(errMsg)
