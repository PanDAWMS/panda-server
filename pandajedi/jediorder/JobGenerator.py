import copy
import datetime
import gc
import os
import random
import re
import socket
import sys
import time
import traceback
from urllib.parse import unquote

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
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
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils, ParseJobXML
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client as PandaClient

from .JediKnight import JediKnight
from .JobBroker import JobBroker
from .JobSplitter import JobSplitter
from .JobThrottler import JobThrottler
from .TaskSetupper import TaskSetupper

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# exceptions
class UnresolvedParam(Exception):
    pass


# time profile level
TIME_PROFILE_OFF = 0
TIME_PROFILE_ON = 1
TIME_PROFILE_DEEP = 2


# worker class to generate jobs
class JobGenerator(JediKnight):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, cloudList, withThrottle=True, execJobs=True, loopCycle_cust=None):
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-gen"
        self.cloudList = cloudList
        self.withThrottle = withThrottle
        self.execJobs = execJobs
        self.loopCycle_cust = loopCycle_cust
        self.paramsToGetTasks = None

    # main
    def start(self):
        # start base class
        # JediKnight.start(self)
        # global thread pool
        globalThreadPool = ThreadPool()

        # probability of running inactive gshare rtype combinations
        try:
            inactive_poll_probability = jedi_config.jobgen.inactive_poll_probability
        except AttributeError:
            inactive_poll_probability = 0.25

        # go into main loop
        while True:
            startTime = naive_utcnow()
            tmpLog = MsgWrapper(logger)
            try:
                tmpLog.debug("start")
                # get SiteMapper
                siteMapper = self.taskBufferIF.get_site_mapper()
                tmpLog.debug("got siteMapper")
                # get work queue mapper
                workQueueMapper = self.taskBufferIF.getWorkQueueMap()
                tmpLog.debug("got workQueueMapper")
                # get resource types
                resource_types = self.taskBufferIF.load_resource_types()
                if not resource_types:
                    raise RuntimeError("failed to get resource types")
                tmpLog.debug("got resource types")
                # get Throttle
                throttle = JobThrottler(self.vos, self.prodSourceLabels)
                throttle.initializeMods(self.taskBufferIF)
                tmpLog.debug("got Throttle")
                # get TaskSetupper
                taskSetupper = TaskSetupper(self.vos, self.prodSourceLabels)
                taskSetupper.initializeMods(self.taskBufferIF, self.ddmIF)
                # loop over all vos
                tmpLog.debug("go into loop")
                for vo in self.vos:
                    # get the active gshare rtypes combinations in order to reduce polling frequency on unused combinations
                    active_gshare_rtypes = self.taskBufferIF.get_active_gshare_rtypes(vo)

                    # check if job submission is enabled
                    isUP = self.taskBufferIF.getConfigValue("jobgen", "JOB_SUBMISSION", "jedi", vo)
                    if isUP is False:
                        tmpLog.debug(f"job submission is disabled for VO={vo}")
                        continue
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # loop over all clouds
                        random.shuffle(self.cloudList)
                        workQueueList = workQueueMapper.getAlignedQueueList(vo, prodSourceLabel)
                        for cloudName in self.cloudList:
                            tmpLog.debug(f"{len(workQueueList)} workqueues for vo:{vo} label:{prodSourceLabel}")
                            for workQueue in workQueueList:
                                for resource_type in resource_types:
                                    workqueue_name_nice = "_".join(workQueue.queue_name.split(" "))
                                    cycleStr = "pid={0} vo={1} cloud={2} queue={3} ( id={4} ) label={5} resource_type={6}".format(
                                        self.pid, vo, cloudName, workqueue_name_nice, workQueue.queue_id, prodSourceLabel, resource_type.resource_name
                                    )
                                    tmpLog_inner = MsgWrapper(logger, cycleStr)

                                    # reduce the polling frequency on unused combinations
                                    active = (
                                        workQueue.queue_name in active_gshare_rtypes
                                        and resource_type.resource_name in active_gshare_rtypes[workQueue.queue_name]
                                    )
                                    if active_gshare_rtypes and not active:
                                        if random.uniform(0, 1) > inactive_poll_probability:
                                            tmpLog_inner.debug(f"skipping {cycleStr} due to inactivity")
                                            continue

                                    tmpLog_inner.debug(f"start {cycleStr}")
                                    # check if to lock
                                    lockFlag = self.toLockProcess(vo, prodSourceLabel, workQueue.queue_name, cloudName)
                                    flagLocked = False
                                    if lockFlag:
                                        tmpLog_inner.debug("check if to lock")
                                        # lock
                                        flagLocked = self.taskBufferIF.lockProcess_JEDI(
                                            vo=vo,
                                            prodSourceLabel=prodSourceLabel,
                                            cloud=cloudName,
                                            workqueue_id=workQueue.queue_id,
                                            resource_name=resource_type.resource_name,
                                            component=None,
                                            pid=self.pid,
                                        )
                                        if not flagLocked:
                                            tmpLog_inner.debug("skip since locked by another process")
                                            continue

                                    # throttle
                                    tmpLog_inner.debug(f"check throttle with {throttle.getClassName(vo, prodSourceLabel)}")
                                    try:
                                        tmpSt, thrFlag = throttle.toBeThrottled(vo, prodSourceLabel, cloudName, workQueue, resource_type.resource_name)
                                    except Exception:
                                        errtype, errvalue = sys.exc_info()[:2]
                                        tmpLog_inner.error(f"throttler failed with {errtype} {errvalue}")
                                        tmpLog_inner.error(f"throttler failed with traceback {traceback.format_exc()}")
                                        raise RuntimeError("crashed when checking throttle")
                                    if tmpSt != self.SC_SUCCEEDED:
                                        raise RuntimeError("failed to check throttle")
                                    mergeUnThrottled = None
                                    if thrFlag is True:
                                        if flagLocked:
                                            tmpLog_inner.debug("throttled")
                                            self.taskBufferIF.unlockProcess_JEDI(
                                                vo=vo,
                                                prodSourceLabel=prodSourceLabel,
                                                cloud=cloudName,
                                                workqueue_id=workQueue.queue_id,
                                                resource_name=resource_type.resource_name,
                                                component=None,
                                                pid=self.pid,
                                            )
                                            continue
                                    elif thrFlag is False:
                                        pass
                                    else:
                                        # leveled flag
                                        mergeUnThrottled = not throttle.mergeThrottled(vo, workQueue.queue_type, thrFlag)
                                        if not mergeUnThrottled:
                                            tmpLog_inner.debug("throttled including merge")
                                            if flagLocked:
                                                self.taskBufferIF.unlockProcess_JEDI(
                                                    vo=vo,
                                                    prodSourceLabel=prodSourceLabel,
                                                    cloud=cloudName,
                                                    workqueue_id=workQueue.queue_id,
                                                    resource_name=resource_type.resource_name,
                                                    component=None,
                                                    pid=self.pid,
                                                )
                                                continue
                                        else:
                                            tmpLog_inner.debug("only merge is unthrottled")

                                    tmpLog_inner.debug(f"minPriority={throttle.minPriority} maxNumJobs={throttle.maxNumJobs}")
                                    # get typical number of files
                                    typicalNumFilesMap = self.taskBufferIF.getTypicalNumInput_JEDI(vo, prodSourceLabel, workQueue, useResultCache=600)
                                    if typicalNumFilesMap is None:
                                        raise RuntimeError("failed to get typical number of files")
                                    # get params
                                    tmpParamsToGetTasks = self.getParamsToGetTasks(vo, prodSourceLabel, workQueue.queue_name, cloudName)
                                    nTasksToGetTasks = tmpParamsToGetTasks["nTasks"]
                                    nFilesToGetTasks = tmpParamsToGetTasks["nFiles"]
                                    tmpLog_inner.debug(f"nTasks={nTasksToGetTasks} nFiles={nFilesToGetTasks} to get tasks")
                                    # get number of tasks to generate new jumbo jobs
                                    numTasksWithRunningJumbo = self.taskBufferIF.getNumTasksWithRunningJumbo_JEDI(vo, prodSourceLabel, cloudName, workQueue)
                                    if not self.withThrottle:
                                        numTasksWithRunningJumbo = 0
                                    maxNumTasksWithRunningJumbo = 50
                                    if numTasksWithRunningJumbo < maxNumTasksWithRunningJumbo:
                                        numNewTaskWithJumbo = maxNumTasksWithRunningJumbo - numTasksWithRunningJumbo
                                        if numNewTaskWithJumbo < 0:
                                            numNewTaskWithJumbo = 0
                                    else:
                                        numNewTaskWithJumbo = 0
                                    # release lock when lack of jobs
                                    lackOfJobs = False
                                    if thrFlag is False and flagLocked and throttle.lackOfJobs:
                                        tmpLog_inner.debug(f"unlock {cycleStr} for multiple processes to quickly fill the queue until nQueueLimit is reached")
                                        self.taskBufferIF.unlockProcess_JEDI(
                                            vo=vo,
                                            prodSourceLabel=prodSourceLabel,
                                            cloud=cloudName,
                                            workqueue_id=workQueue.queue_id,
                                            resource_name=resource_type.resource_name,
                                            component=None,
                                            pid=self.pid,
                                        )
                                        lackOfJobs = True
                                    # get the list of input
                                    tmpList = self.taskBufferIF.getTasksToBeProcessed_JEDI(
                                        self.pid,
                                        vo,
                                        workQueue,
                                        prodSourceLabel,
                                        cloudName,
                                        nTasks=nTasksToGetTasks,
                                        nFiles=nFilesToGetTasks,
                                        minPriority=throttle.minPriority,
                                        maxNumJobs=throttle.maxNumJobs,
                                        typicalNumFilesMap=typicalNumFilesMap,
                                        mergeUnThrottled=mergeUnThrottled,
                                        numNewTaskWithJumbo=numNewTaskWithJumbo,
                                        resource_name=resource_type.resource_name,
                                    )
                                    if tmpList is None:
                                        # failed
                                        tmpLog_inner.error("failed to get the list of input chunks to generate jobs")
                                    else:
                                        tmpLog_inner.debug(f"got {len(tmpList)} input tasks")
                                        if len(tmpList) != 0:
                                            # put to a locked list
                                            inputList = ListWithLock(tmpList)
                                            # make thread pool
                                            threadPool = ThreadPool()
                                            # make lock if necessary
                                            if lockFlag:
                                                liveCounter = MapWithLock()
                                            else:
                                                liveCounter = None
                                            # make list for brokerage lock
                                            brokerageLockIDs = ListWithLock([])
                                            # make workers
                                            nWorker = jedi_config.jobgen.nWorkers
                                            for iWorker in range(nWorker):
                                                thr = JobGeneratorThread(
                                                    inputList,
                                                    threadPool,
                                                    self.taskBufferIF,
                                                    self.ddmIF,
                                                    siteMapper,
                                                    self.execJobs,
                                                    taskSetupper,
                                                    self.pid,
                                                    workQueue,
                                                    resource_type.resource_name,
                                                    cloudName,
                                                    liveCounter,
                                                    brokerageLockIDs,
                                                    lackOfJobs,
                                                    resource_types,
                                                )
                                                globalThreadPool.add(thr)
                                                thr.start()
                                            # join
                                            tmpLog_inner.debug("try to join")
                                            threadPool.join(60 * 10)
                                            # unlock locks made by brokerage
                                            for brokeragelockID in brokerageLockIDs:
                                                self.taskBufferIF.unlockProcessWithPID_JEDI(
                                                    vo, prodSourceLabel, workQueue.queue_name, resource_type.resource_name, brokeragelockID, True
                                                )
                                            tmpLog_inner.debug(f"dump one-time pool : {threadPool.dump()} remTasks={inputList.dump()}")
                                    # unlock
                                    self.taskBufferIF.unlockProcess_JEDI(
                                        vo=vo,
                                        prodSourceLabel=prodSourceLabel,
                                        cloud=cloudName,
                                        workqueue_id=workQueue.queue_id,
                                        resource_name=resource_type.resource_name,
                                        component=None,
                                        pid=self.pid,
                                    )
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {errtype.__name__}:{errvalue} {traceback.format_exc()}")
            # unlock just in case
            try:
                for vo in self.vos:
                    for prodSourceLabel in self.prodSourceLabels:
                        for cloudName in self.cloudList:
                            for workQueue in workQueueList:
                                self.taskBufferIF.unlockProcess_JEDI(
                                    vo=vo,
                                    prodSourceLabel=prodSourceLabel,
                                    cloud=cloudName,
                                    workqueue_id=workQueue.queue_id,
                                    resource_name=resource_type.resource_name,
                                    component=None,
                                    pid=self.pid,
                                )
            except Exception:
                pass
            try:
                # clean up global thread pool
                globalThreadPool.clean()
                # dump
                tmpLog.debug(f"dump global pool : {globalThreadPool.dump()}")
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed to dump global pool with {errtype.__name__} {errvalue}")
            tmpLog.debug("end")
            # memory check
            try:
                memLimit = 1.5 * 1024
                memNow = CoreUtils.getMemoryUsage()
                tmpLog.debug(f"memUsage now {memNow} MB pid={os.getpid()}")
                if memNow > memLimit:
                    tmpLog.warning(f"memory limit exceeds {memNow} > {memLimit} MB pid={os.getpid()}")
                tmpLog.debug("trigger garbage collection")
                gc.collect()
            except Exception as e:
                tmpLog.error(f"failed in garbage collection with {str(e)}")
            # sleep if needed. It can be specified for the particular JobGenerator instance or use the default value
            if self.loopCycle_cust:
                loopCycle = self.loopCycle_cust
            else:
                loopCycle = jedi_config.jobgen.loopCycle
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            tmpLog.debug(f"loopCycle {loopCycle}s; sleeping {sleepPeriod}s")
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)

    # get parameters to get tasks
    def getParamsToGetTasks(self, vo, prodSourceLabel, queueName, cloudName):
        paramsList = ["nFiles", "nTasks"]
        # get group specified params
        if self.paramsToGetTasks is None:
            self.paramsToGetTasks = {}
            # loop over all params
            for paramName in paramsList:
                self.paramsToGetTasks[paramName] = {}
                configParamName = paramName + "PerGroup"
                # check if param is defined in config
                if hasattr(jedi_config.jobgen, configParamName):
                    tmpConfParams = getattr(jedi_config.jobgen, configParamName)
                    for item in tmpConfParams.split(","):
                        # decompose config params
                        try:
                            tmpVOs, tmpProdSourceLabels, tmpQueueNames, tmpCloudNames, nXYZ = item.split(":")
                            # loop over all VOs
                            for tmpVO in tmpVOs.split("|"):
                                if tmpVO == "":
                                    tmpVO = "any"
                                self.paramsToGetTasks[paramName][tmpVO] = {}
                                # loop over all labels
                                for tmpProdSourceLabel in tmpProdSourceLabels.split("|"):
                                    if tmpProdSourceLabel == "":
                                        tmpProdSourceLabel = "any"
                                    self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel] = {}
                                    # loop over all queues
                                    for tmpQueueName in tmpQueueNames.split("|"):
                                        if tmpQueueName == "":
                                            tmpQueueName = "any"
                                        self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName] = {}
                                        for tmpCloudName in tmpCloudNames.split("|"):
                                            if tmpCloudName == "":
                                                tmpCloudName = "any"
                                            # add
                                            self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName][tmpCloudName] = int(nXYZ)
                        except Exception:
                            pass
        # make return
        retMap = {}
        for paramName in paramsList:
            # set default
            retMap[paramName] = getattr(jedi_config.jobgen, paramName)
            if paramName in self.paramsToGetTasks:
                # check VO
                if vo in self.paramsToGetTasks[paramName]:
                    tmpVO = vo
                elif "any" in self.paramsToGetTasks[paramName]:
                    tmpVO = "any"
                else:
                    continue
                # check label
                if prodSourceLabel in self.paramsToGetTasks[paramName][tmpVO]:
                    tmpProdSourceLabel = prodSourceLabel
                elif "any" in self.paramsToGetTasks[paramName][tmpVO]:
                    tmpProdSourceLabel = "any"
                else:
                    continue
                # check queue
                if queueName in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel]:
                    tmpQueueName = queueName
                elif "any" in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel]:
                    tmpQueueName = "any"
                else:
                    continue
                # check cloud
                if cloudName in self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName]:
                    tmpCloudName = cloudName
                else:
                    tmpCloudName = "any"
                # set
                retMap[paramName] = self.paramsToGetTasks[paramName][tmpVO][tmpProdSourceLabel][tmpQueueName][tmpCloudName]
        # get from config table
        nFiles = self.taskBufferIF.getConfigValue("jobgen", f"NFILES_{queueName}", "jedi", vo)
        if nFiles is not None:
            retMap["nFiles"] = nFiles
        nTasks = self.taskBufferIF.getConfigValue("jobgen", f"NTASKS_{queueName}", "jedi", vo)
        if nTasks is not None:
            retMap["nTasks"] = nTasks
        # return
        return retMap

    # check if lock process
    def toLockProcess(self, vo, prodSourceLabel, queueName, cloudName):
        try:
            # check if param is defined in config
            if hasattr(jedi_config.jobgen, "lockProcess"):
                for item in jedi_config.jobgen.lockProcess.split(","):
                    tmpVo, tmpProdSourceLabel, tmpQueueName, tmpCloudName = item.split(":")
                    if tmpVo not in ["", "any", None] and vo not in tmpVo.split("|"):
                        continue
                    if tmpProdSourceLabel not in ["", "any", None] and prodSourceLabel not in tmpProdSourceLabel.split("|"):
                        continue
                    if tmpQueueName not in ["", "any", None] and queueName not in tmpQueueName.split("|"):
                        continue
                    if tmpCloudName not in ["", "any", None] and cloudName not in tmpCloudName.split("|"):
                        continue
                    return True
        except Exception:
            pass
        return False


# thread for real worker
class JobGeneratorThread(WorkerThread):
    # constructor
    def __init__(
        self,
        inputList,
        threadPool,
        taskbufferIF,
        ddmIF,
        siteMapper,
        execJobs,
        taskSetupper,
        pid,
        workQueue,
        resource_name,
        cloud,
        liveCounter,
        brokerageLockIDs,
        lackOfJobs,
        resource_types,
    ):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributes
        self.inputList = inputList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.siteMapper = siteMapper
        self.execJobs = execJobs
        self.numGenJobs = 0
        self.taskSetupper = taskSetupper
        self.msgType = "jobgenerator"
        self.pid = pid
        self.buildSpecMap = {}
        self.finished_lib_specs_map = {}
        self.active_lib_specs_map = {}
        self.workQueue = workQueue
        self.resource_name = resource_name
        self.cloud = cloud
        self.liveCounter = liveCounter
        self.brokerageLockIDs = brokerageLockIDs
        self.lackOfJobs = lackOfJobs
        self.resource_types = resource_types
        self.time_profile_level = TIME_PROFILE_OFF

    # main
    def runImpl(self):
        workqueue_name_nice = "_".join(self.workQueue.queue_name.split(" "))
        while True:
            try:
                lastJediTaskID = None
                # get a part of list
                nInput = 1
                taskInputList = self.inputList.get(nInput)
                # no more datasets
                if len(taskInputList) == 0:
                    self.logger.debug(f"{self.__class__.__name__} terminating after generating {self.numGenJobs} jobs since no more inputs ")
                    if self.numGenJobs > 0:
                        prefix = "<VO={0} queue_type={1} cloud={2} queue={3} resource_type={4}>".format(
                            self.workQueue.VO, self.workQueue.queue_type, self.cloud, workqueue_name_nice, self.resource_name
                        )
                        tmpMsg = f": submitted {self.numGenJobs} jobs"
                        tmpLog = MsgWrapper(self.logger, monToken=prefix)
                        tmpLog.info(prefix + tmpMsg)
                        tmpLog.sendMsg(tmpMsg, self.msgType)
                    return
                # loop over all tasks
                for tmpJediTaskID, inputList in taskInputList:
                    lastJediTaskID = tmpJediTaskID
                    # loop over all inputs
                    nBrokergeFailed = 0
                    nSubmitSucceeded = 0
                    task_common_dict = {}
                    for idxInputList, tmpInputItem in enumerate(inputList):
                        taskSpec, cloudName, inputChunk = tmpInputItem
                        # reset error dialog
                        taskSpec.errorDialog = None
                        # reset map of buildSpec
                        self.buildSpecMap = {}
                        main_stop_watch = CoreUtils.StopWatch("main")
                        loopStart = naive_utcnow()
                        # make logger
                        tmpLog = MsgWrapper(
                            self.logger,
                            f"<jediTaskID={taskSpec.jediTaskID} datasetID={inputChunk.masterIndexName}>",
                            monToken=f"<jediTaskID={taskSpec.jediTaskID}>",
                        )
                        tmpLog.info(f"start to generate with VO={taskSpec.vo} cloud={cloudName} queue={workqueue_name_nice} resource_type={self.resource_name}")
                        tmpLog.debug(main_stop_watch.get_elapsed_time("init"))
                        tmpLog.sendMsg("start to generate jobs", self.msgType)
                        readyToSubmitJob = False
                        jobsSubmitted = False
                        goForward = True
                        taskParamMap = None
                        oldStatus = taskSpec.status
                        # extend sandbox lifetime
                        if goForward:
                            if not inputChunk.isMerging and not (hasattr(jedi_config.jobgen, "touchSandbox") and not jedi_config.jobgen.touchSandbox):
                                tmpStat, tmpOut, taskParamMap = self.touchSandoboxFiles(taskSpec, taskParamMap, tmpLog)
                                if tmpStat != Interaction.SC_SUCCEEDED:
                                    tmpLog.error("failed to extend lifetime of sandbox file")
                                    taskSpec.setOnHold()
                                    taskSpec.setErrDiag(tmpOut)
                                    goForward = False
                        # initialize brokerage
                        if goForward:
                            jobBroker = JobBroker(taskSpec.vo, taskSpec.prodSourceLabel)
                            tmpStat = jobBroker.initializeMods(self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud), self.taskBufferIF)
                            if not tmpStat:
                                tmpErrStr = "failed to initialize JobBroker"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                                goForward = False
                            # set live counter
                            jobBrokerCore = jobBroker.getImpl(taskSpec.vo, taskSpec.prodSourceLabel)
                            jobBrokerCore.setLiveCounter(self.liveCounter)
                            # set lock ID
                            jobBrokerCore.setLockID(self.pid, self.ident)
                            # set common dict
                            jobBrokerCore.set_task_common_dict(task_common_dict)
                        # read task params if necessary
                        if taskSpec.useLimitedSites():
                            tmpStat, taskParamMap = self.readTaskParams(taskSpec, taskParamMap, tmpLog)
                            if not tmpStat:
                                tmpErrStr = "failed to read task params"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                                goForward = False
                        # run brokerage
                        lockCounter = False
                        pendingJumbo = False
                        if goForward:
                            if self.liveCounter is not None and not inputChunk.isMerging and not self.lackOfJobs:
                                tmpLog.debug(main_stop_watch.get_elapsed_time("lock counter"))
                                self.liveCounter.acquire()
                                lockCounter = True
                            tmpLog.debug(main_stop_watch.get_elapsed_time("brokerage"))
                            tmpLog.debug(f"run brokerage with {jobBroker.getClassName(taskSpec.vo, taskSpec.prodSourceLabel)}")
                            try:
                                tmpStat, inputChunk = jobBroker.doBrokerage(taskSpec, cloudName, inputChunk, taskParamMap)
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                tmpLog.error(f"brokerage crashed with {errtype.__name__}:{errvalue} {traceback.format_exc()}")
                                tmpStat = Interaction.SC_FAILED
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                nBrokergeFailed += 1
                                if inputChunk is not None and inputChunk.hasCandidatesForJumbo():
                                    pendingJumbo = True
                                else:
                                    tmpErrStr = f"brokerage failed for {nBrokergeFailed} input datasets when trying {len(inputList)} datasets."
                                    tmpLog.error(f"{tmpErrStr} {taskSpec.get_original_error_dialog()}")
                                    if nSubmitSucceeded == 0:
                                        taskSpec.setOnHold()
                                    taskSpec.setErrDiag(tmpErrStr, True)
                                goForward = False
                            else:
                                # collect brokerage lock ID
                                brokerageLockID = jobBroker.getBaseLockID(taskSpec.vo, taskSpec.prodSourceLabel)
                                if brokerageLockID is not None:
                                    self.brokerageLockIDs.append(brokerageLockID)
                        # run splitter
                        if goForward:
                            splitter = JobSplitter()
                            try:
                                # lock counter
                                if self.liveCounter is not None and not inputChunk.isMerging and not lockCounter:
                                    tmpLog.debug(main_stop_watch.get_elapsed_time("lock counter"))
                                    self.liveCounter.acquire()
                                    lockCounter = True
                                    # update nQueuedJobs since live counter may not have been
                                    # considered in brokerage
                                    tmpMsg = inputChunk.update_n_queue(self.liveCounter)
                                    tmpLog.debug(f"updated nQueue at {tmpMsg}")
                                tmpLog.debug(main_stop_watch.get_elapsed_time("run splitter"))
                                tmpStat, subChunks, isSkipped = splitter.doSplit(taskSpec, inputChunk, self.siteMapper, allow_chunk_size_limit=True)
                                if tmpStat == Interaction.SC_SUCCEEDED and isSkipped:
                                    # run again without chunk size limit to generate jobs for skipped snippet
                                    tmpStat, tmpChunks, isSkipped = splitter.doSplit(taskSpec, inputChunk, self.siteMapper, allow_chunk_size_limit=False)
                                    if tmpStat == Interaction.SC_SUCCEEDED:
                                        subChunks += tmpChunks
                                # * remove the last sub-chunk when inputChunk is read in a block
                                #   since alignment could be broken in the last sub-chunk
                                #    e.g., inputChunk=10 -> subChunks=4,4,2 and remove 2
                                # * don't remove it for the last inputChunk
                                # e.g., inputChunks = 10(remove),10(remove),3(not remove)
                                if (
                                    len(subChunks) > 0
                                    and len(subChunks[-1]["subChunks"]) > 1
                                    and inputChunk.masterDataset is not None
                                    and inputChunk.readBlock is True
                                ):
                                    subChunks[-1]["subChunks"] = subChunks[-1]["subChunks"][:-1]
                                # update counter
                                if lockCounter:
                                    for tmpSubChunk in subChunks:
                                        self.liveCounter.add(tmpSubChunk["siteName"], len(tmpSubChunk["subChunks"]))
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                tmpLog.error(f"splitter crashed with {errtype.__name__}:{errvalue} {traceback.format_exc()}")
                                tmpStat = Interaction.SC_FAILED
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpErrStr = "splitting failed"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                                goForward = False
                        # release counter
                        if lockCounter:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("release counter"))
                            self.liveCounter.release()
                        # lock task
                        if goForward:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("lock task"))
                            tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                            if tmpStat is False:
                                tmpLog.debug("skip due to lock failure")
                                continue
                        # generate jobs
                        if goForward:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("job generator"))
                            try:
                                tmpStat, pandaJobs, datasetToRegister, oldPandaIDs, parallelOutMap, outDsMap = self.doGenerate(
                                    taskSpec, cloudName, subChunks, inputChunk, tmpLog, taskParamMap=taskParamMap, splitter=splitter
                                )
                            except Exception as e:
                                tmpErrStr = f"job generator crashed with {str(e)}"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setErrDiag(tmpErrStr)
                                tmpStat = Interaction.SC_FAILED
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpErrStr = "job generation failed."
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr, append=True, prepend=True)
                                goForward = False
                            elif not pandaJobs:
                                tmpErrStr = "candidates became full after the brokerage decision " "and skipped during the submission cycle"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                                goForward = False
                        # lock task
                        if goForward:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("lock task"))
                            tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                            if tmpStat is False:
                                tmpLog.debug("skip due to lock failure")
                                continue
                        # setup task
                        if goForward:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("task setup"))
                            tmpLog.debug(f"run setupper with {self.taskSetupper.getClassName(taskSpec.vo, taskSpec.prodSourceLabel)}")
                            tmpStat = self.taskSetupper.doSetup(taskSpec, datasetToRegister, pandaJobs)
                            if (
                                tmpStat == Interaction.SC_FATAL
                                and taskSpec.frozenTime is not None
                                and naive_utcnow() - taskSpec.frozenTime > datetime.timedelta(days=7)
                            ):
                                tmpErrStr = "fatal error when setting up task"
                                tmpLog.error(tmpErrStr)
                                taskSpec.status = "exhausted"
                                taskSpec.setErrDiag(tmpErrStr, True)
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpErrStr = "failed to setup task"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr, True)
                            else:
                                readyToSubmitJob = True
                                if taskSpec.toRegisterDatasets() and (not taskSpec.mergeOutput() or inputChunk.isMerging):
                                    taskSpec.registeredDatasets()
                        # lock task
                        if goForward:
                            tmpLog.debug(main_stop_watch.get_elapsed_time("lock task"))
                            tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                            if tmpStat is False:
                                tmpLog.debug("skip due to lock failure")
                                continue
                        # submit
                        if readyToSubmitJob:
                            # check if first submission
                            if oldStatus == "ready" and inputChunk.useScout():
                                firstSubmission = True
                            else:
                                firstSubmission = False
                            # type of relation
                            if inputChunk.isMerging:
                                relationType = "merge"
                            else:
                                relationType = "retry"
                            # submit
                            fqans = taskSpec.makeFQANs()
                            tmpLog.info(f"submit njobs={len(pandaJobs)} jobs with FQAN={','.join(str(fqan) for fqan in fqans)}")
                            tmpLog.debug(main_stop_watch.get_elapsed_time(f"{len(pandaJobs)} job submission"))
                            iJobs = 0
                            nJobsInBunch = 500
                            resSubmit = []
                            esJobsetMap = {}
                            unprocessedMap = {}
                            while iJobs < len(pandaJobs):
                                tmpResSubmit, esJobsetMap, unprocessedMap = self.taskBufferIF.storeJobs(
                                    pandaJobs[iJobs : iJobs + nJobsInBunch],
                                    taskSpec.userName,
                                    fqans=fqans,
                                    toPending=True,
                                    oldPandaIDs=oldPandaIDs[iJobs : iJobs + nJobsInBunch],
                                    relationType=relationType,
                                    esJobsetMap=esJobsetMap,
                                    getEsJobsetMap=True,
                                    unprocessedMap=unprocessedMap,
                                    bulk_job_insert=True,
                                    trust_user=True,
                                )
                                if isinstance(tmpResSubmit, str):
                                    tmpLog.error(f"failed to store jobs with {tmpResSubmit}")
                                    break
                                resSubmit += tmpResSubmit
                                self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                                iJobs += nJobsInBunch
                            pandaIDs = []
                            nSkipJumbo = 0
                            for pandaJob, items in zip(pandaJobs, resSubmit):
                                if items[0] != "NULL":
                                    pandaIDs.append(items[0])
                                elif EventServiceUtils.isJumboJob(pandaJob):
                                    nSkipJumbo += 1
                                    pandaIDs.append(items[0])
                            if nSkipJumbo > 0:
                                tmpLog.debug(f"{nSkipJumbo} jumbo jobs were skipped")
                            # check if submission was successful
                            if len(pandaIDs) == len(pandaJobs) and pandaJobs:
                                tmpMsg = (
                                    f"successfully submitted jobs_submitted={len(pandaIDs)} / jobs_possible={len(pandaJobs)} "
                                    f"for VO={taskSpec.vo} cloud={cloudName} queue={workqueue_name_nice} "
                                    f"resource_type={self.resource_name} status={oldStatus} nucleus={taskSpec.nucleus} "
                                    f"pmerge={'Y' if inputChunk.isMerging else 'N'}"
                                )

                                tmpLog.info(tmpMsg)
                                tmpLog.sendMsg(tmpMsg, self.msgType)
                                if self.execJobs:
                                    # skip fake co-jumbo and unsubmitted jumbo
                                    pandaIDsForExec = []
                                    for pandaID, pandaJob in zip(pandaIDs, pandaJobs):
                                        if pandaJob.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs:
                                            continue
                                        if pandaID == "NULL":
                                            continue
                                        pandaIDsForExec.append(pandaID)
                                    tmpLog.debug(main_stop_watch.get_elapsed_time(f"{len(pandaIDsForExec)} job execution"))
                                    statExe, retExe = PandaClient.reassign_jobs(pandaIDsForExec)
                                    tmpLog.info(f"exec {len(pandaIDsForExec)} jobs with status={retExe}")
                                jobsSubmitted = True
                                nSubmitSucceeded += 1
                                if inputChunk.isMerging:
                                    # don't change task status by merging
                                    pass
                                elif taskSpec.usePrePro():
                                    taskSpec.status = "preprocessing"
                                elif inputChunk.useScout():
                                    taskSpec.status = "scouting"
                                else:
                                    taskSpec.status = "running"
                                    # scout was skipped
                                    if taskSpec.useScout():
                                        taskSpec.setUseScout(False)
                            else:
                                tmpErrStr = f"submitted only {len(pandaIDs)}/{len(pandaJobs)}"
                                tmpLog.error(tmpErrStr)
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                            # the number of generated jobs
                            self.numGenJobs += len(pandaIDs)
                        # lock task
                        tmpLog.debug(main_stop_watch.get_elapsed_time("lock task"))
                        tmpStat = self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                        if tmpStat is False:
                            tmpLog.debug("skip due to lock failure")
                            continue
                        # reset unused files
                        nFileReset = self.taskBufferIF.resetUnusedFiles_JEDI(taskSpec.jediTaskID, inputChunk)
                        # set jumbo flag
                        if pendingJumbo:
                            tmpFlagStat = self.taskBufferIF.setUseJumboFlag_JEDI(taskSpec.jediTaskID, "pending")
                            tmpErrStr = "going to generate jumbo or real co-jumbo jobs when needed"
                            tmpLog.debug(tmpErrStr)
                            if tmpFlagStat:
                                taskSpec.setErrDiag(None)
                            else:
                                taskSpec.setOnHold()
                                taskSpec.setErrDiag(tmpErrStr)
                        elif jobsSubmitted and taskSpec.getNumJumboJobs() is not None and inputChunk.useJumbo is not None:
                            self.taskBufferIF.setUseJumboFlag_JEDI(taskSpec.jediTaskID, "running")
                        # unset lockedBy when all inputs are done for a task
                        setOldModTime = False
                        if idxInputList + 1 == len(inputList):
                            taskSpec.lockedBy = None
                            taskSpec.lockedTime = None
                            if taskSpec.status in ["running", "scouting"]:
                                setOldModTime = True
                        else:
                            taskSpec.lockedBy = self.pid
                            taskSpec.lockedTime = naive_utcnow()
                        # update task
                        retDB = self.taskBufferIF.updateTask_JEDI(
                            taskSpec,
                            {"jediTaskID": taskSpec.jediTaskID},
                            oldStatus=JediTaskSpec.statusForJobGenerator() + ["pending"],
                            setOldModTime=setOldModTime,
                        )
                        tmpMsg = f"set task_status={taskSpec.status} oldTask={setOldModTime} with {str(retDB)}"
                        if taskSpec.errorDialog not in ["", None]:
                            tmpMsg += " " + taskSpec.errorDialog
                        tmpLog.sendMsg(tmpMsg, self.msgType)
                        tmpLog.info(tmpMsg)
                        regTime = naive_utcnow() - loopStart
                        tmpLog.debug(main_stop_watch.get_elapsed_time(""))
                        tmpLog.info(f"done. took cycle_t={regTime.seconds} sec")
            except Exception as e:
                logger.error("%s.runImpl() failed with {} lastJediTaskID={} {}".format(self.__class__.__name__, str(e), lastJediTaskID, traceback.format_exc()))

    # read task parameters
    def readTaskParams(self, taskSpec, taskParamMap, tmpLog):
        # already read
        if taskParamMap is not None:
            return True, taskParamMap
        try:
            # read task parameters
            taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
            taskParamMap = RefinerUtils.decodeJSON(taskParam)
            return True, taskParamMap
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"task param conversion from json failed with {errtype.__name__}:{errvalue}")
            return False, None

    # generate jobs
    def doGenerate(self, taskSpec, cloudName, inSubChunkList, inputChunk, tmpLog, simul=False, taskParamMap=None, splitter=None):
        # return for failure
        failedRet = Interaction.SC_FAILED, None, None, None, None, None
        # read task parameters
        tmpStat, taskParamMap = self.readTaskParams(taskSpec, taskParamMap, tmpLog)
        if not tmpStat:
            return failedRet
        # special priorities
        scoutPriority = 901
        mergePriority = 5000
        # register datasets
        registerDatasets = taskSpec.toRegisterDatasets()
        try:
            # load XML
            xmlConfig = None
            if taskSpec.useLoadXML():
                loadXML = taskParamMap["loadXML"]
                xmlConfig = ParseJobXML.dom_parser(xmlStr=loadXML)
            # using grouping with boundaryID
            useBoundary = taskSpec.useGroupWithBoundaryID()
            # loop over all sub chunks
            jobSpecList = []
            outDsMap = {}
            datasetToRegister = []
            oldPandaIDs = []
            siteDsMap = {}
            esIndex = 0
            parallelOutMap = {}
            dddMap = {}
            fileIDPool = []
            if inputChunk.isMerging:
                confKey = f"MERGE_JOB_MAX_WALLTIME_{taskSpec.prodSourceLabel}"
                merge_max_walltime = self.taskBufferIF.getConfigValue("jobgen", confKey, "jedi", taskSpec.vo)
            else:
                merge_max_walltime = None
            # count expected number of jobs
            totalNormalJobs = 0
            n_jobs_per_site = {}
            for tmpInChunk in inSubChunkList:
                totalNormalJobs += len(tmpInChunk["subChunks"])
                site_name = tmpInChunk["siteName"]
                n_jobs_per_site.setdefault(site_name, 0)
                n_jobs_per_site[site_name] += len(tmpInChunk["subChunks"])
            # get number of outputs per job and bulk-fetch file IDs
            if totalNormalJobs > 0:
                if taskSpec.instantiateTmpl():
                    output_dataset_types = ["tmpl_output", "tmpl_log"]
                else:
                    output_dataset_types = ["output", "log"]
                tmp_stat, tmp_dataset_specs = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID, output_dataset_types)
                if not tmp_stat:
                    tmpLog.error("cannot get output/log datasets")
                    return failedRet
                num_outputs_per_job = len(tmp_dataset_specs)
                if not simul and num_outputs_per_job > 0:
                    fileIDPool = self.taskBufferIF.bulkFetchFileIDs_JEDI(taskSpec.jediTaskID, num_outputs_per_job * totalNormalJobs)
                else:
                    fileIDPool = range(num_outputs_per_job * totalNormalJobs)
            else:
                num_outputs_per_job = None

            # get random seeds
            random_seed_list = None
            random_seed_dataset = None
            if taskSpec.useRandomSeed() and not inputChunk.isMerging:
                tmp_stat, (random_seed_list, random_seed_dataset) = self.taskBufferIF.getRandomSeed_JEDI(taskSpec.jediTaskID, simul, totalNormalJobs)
                if not tmp_stat:
                    tmpLog.error("failed to get random seeds")
                    return failedRet

            # check if the chunks produce outputs that need to be merged later
            if taskSpec.mergeOutput() and not inputChunk.isMerging:
                to_produce_outputs_merged_later = True
            else:
                to_produce_outputs_merged_later = False

            # check if the chunks instantiate template datasets
            if to_produce_outputs_merged_later or taskSpec.instantiateTmpl():
                instantiate_template_dataset = True
            else:
                instantiate_template_dataset = False

            # bulk fetch output files unless XML config is used to describe relationship between input and output files, or the task is configured
            # to produce multiple output files for each output stream per job, or output filenames inherit from input filenames, since in these cases
            # the output files cannot be fetched in one-go
            fetched_out_sub_chunks = {}
            fetched_serial_numbers = {}
            fetched_parallel_out_map = {}
            if (
                xmlConfig is None
                and (useBoundary is None or not useBoundary["outMap"])
                and not taskSpec.on_site_merging()
                and taskSpec.getFieldNumToLFN() is None
            ):
                for site_name in n_jobs_per_site:
                    # set site name if the chunks instantiate template datasets
                    if to_produce_outputs_merged_later or (taskSpec.instantiateTmpl() and taskSpec.instantiateTmplSite()):
                        site_to_instantiate = site_name
                    else:
                        site_to_instantiate = None
                    # bulk fetch output files
                    (
                        fetched_out_sub_chunks[site_name],
                        fetched_serial_numbers[site_name],
                        tmp_datasets_to_register,
                        siteDsMap,
                        fetched_parallel_out_map[site_name],
                    ) = self.taskBufferIF.getOutputFiles_JEDI(
                        taskSpec.jediTaskID,
                        None,
                        simul,
                        instantiate_template_dataset,
                        site_to_instantiate,
                        to_produce_outputs_merged_later,
                        False,
                        None,
                        siteDsMap,
                        "",
                        registerDatasets,
                        None,
                        fileIDPool,
                        n_jobs_per_site[site_name],
                        bulk_fetch_for_multiple_jobs=True,
                        master_dataset_id=inputChunk.masterIndexName,
                    )
                    for tmp_dataset_spec in tmp_datasets_to_register:
                        if tmp_dataset_spec not in datasetToRegister:
                            datasetToRegister.append(tmp_dataset_spec)
                    if not simul:
                        try:
                            fileIDPool = fileIDPool[num_outputs_per_job * n_jobs_per_site[site_name] :]
                        except Exception:
                            fileIDPool = []

            # task queued time
            task_queued_time = taskSpec.get_queued_time()

            # loop over all sub chunks
            for tmpInChunk in inSubChunkList:
                site_name_list = tmpInChunk["siteName"].split(",")
                siteName = tmpInChunk["siteName"]
                inSubChunks = tmpInChunk["subChunks"]
                siteCandidate = tmpInChunk["siteCandidate"]
                siteSpec = self.siteMapper.getSite(site_name_list[0])
                scope_input, scope_output = select_scope(siteSpec, taskSpec.prodSourceLabel, JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType))
                buildFileSpec = None
                build_file_spec_map = {}
                # make preprocessing job
                if taskSpec.usePrePro():
                    tmpStat, preproJobSpec, tmpToRegister = self.doGeneratePrePro(
                        taskSpec, cloudName, siteName, siteSpec, taskParamMap, inSubChunks, tmpLog, simul
                    )
                    if tmpStat != Interaction.SC_SUCCEEDED:
                        tmpLog.error("failed to generate prepro job")
                        return failedRet
                    # append
                    jobSpecList.append(preproJobSpec)
                    oldPandaIDs.append([])
                    # append datasets
                    for tmpToRegisterItem in tmpToRegister:
                        if tmpToRegisterItem not in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                    break
                # make build job
                elif taskSpec.useBuild():
                    for idx, tmp_site_name in enumerate(site_name_list):
                        tmp_site_spec = self.siteMapper.getSite(tmp_site_name)
                        tmpStat, tmp_buildJobSpec, tmp_buildFileSpec, tmpToRegister = self.doGenerateBuild(
                            taskSpec, cloudName, tmp_site_name, tmp_site_spec, taskParamMap, tmpLog, siteCandidate, simul
                        )
                        if tmpStat != Interaction.SC_SUCCEEDED:
                            tmpLog.error("failed to generate build job")
                            return failedRet
                        if idx == 0:
                            buildJobSpec, buildFileSpec = tmp_buildJobSpec, tmp_buildFileSpec
                        # append
                        if tmp_buildJobSpec is not None:
                            jobSpecList.append(tmp_buildJobSpec)
                            oldPandaIDs.append([])
                        build_file_spec_map[tmp_site_spec.get_unified_name()] = tmp_buildFileSpec
                        # append datasets
                        for tmpToRegisterItem in tmpToRegister:
                            if tmpToRegisterItem not in datasetToRegister:
                                datasetToRegister.append(tmpToRegisterItem)
                # make normal jobs
                tmpJobSpecList = []
                tmpMasterEventsList = []
                stop_watch = CoreUtils.StopWatch("gen_cycle")
                i_cycle = 0
                for inSubChunk in inSubChunks:
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        stop_watch.reset()
                        tmpLog.debug(stop_watch.get_elapsed_time(f"init {i_cycle}"))
                    i_cycle += 1
                    subOldPandaIDs = []
                    jobSpec = JobSpec()
                    jobSpec.jobDefinitionID = 0
                    jobSpec.jobExecutionID = 0
                    jobSpec.attemptNr = self.getLargestAttemptNr(inSubChunk)
                    if taskSpec.disableAutoRetry():
                        # disable server/pilot retry
                        jobSpec.maxAttempt = -1
                    elif taskSpec.useEventService(siteSpec) and not inputChunk.isMerging:
                        # set max attempt for event service
                        if taskSpec.getMaxAttemptEsJob() is None:
                            jobSpec.maxAttempt = jobSpec.attemptNr + EventServiceUtils.defMaxAttemptEsJob
                        else:
                            jobSpec.maxAttempt = jobSpec.attemptNr + taskSpec.getMaxAttemptEsJob()
                    else:
                        jobSpec.maxAttempt = jobSpec.attemptNr
                    jobSpec.jobName = taskSpec.taskName + ".$ORIGINPANDAID"
                    if inputChunk.isMerging:
                        # use merge trf
                        jobSpec.transformation = taskParamMap["mergeSpec"]["transPath"]
                    else:
                        jobSpec.transformation = taskSpec.transPath
                    platforms = siteCandidate.get_overridden_attribute("platforms")
                    if platforms:
                        jobSpec.cmtConfig = platforms
                    else:
                        jobSpec.cmtConfig = taskSpec.get_platforms()
                    if taskSpec.transHome is not None:
                        jobSpec.homepackage = re.sub("-(?P<dig>\d+\.)", "/\g<dig>", taskSpec.transHome)
                        jobSpec.homepackage = re.sub("\r", "", jobSpec.homepackage)
                    jobSpec.prodSourceLabel = taskSpec.prodSourceLabel
                    if inputChunk.isMerging:
                        # set merging type
                        jobSpec.processingType = "pmerge"
                    else:
                        jobSpec.processingType = taskSpec.processingType
                    jobSpec.jediTaskID = taskSpec.jediTaskID
                    jobSpec.taskID = taskSpec.jediTaskID
                    jobSpec.jobsetID = taskSpec.reqID
                    jobSpec.reqID = taskSpec.reqID
                    jobSpec.workingGroup = taskSpec.workingGroup
                    jobSpec.countryGroup = taskSpec.countryGroup
                    if inputChunk.useJumbo in ["fake", "only"]:
                        jobSpec.computingSite = EventServiceUtils.siteIdForWaitingCoJumboJobs
                    else:
                        if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging and taskSpec.getNumEventServiceConsumer() is not None:
                            # keep concatenated site names which are converted when increasing consumers
                            jobSpec.computingSite = siteName
                        else:
                            jobSpec.computingSite = siteSpec.get_unified_name()
                    if taskSpec.cloud_as_vo():
                        jobSpec.cloud = taskSpec.cloud
                    else:
                        jobSpec.cloud = cloudName
                    jobSpec.nucleus = taskSpec.nucleus
                    jobSpec.VO = taskSpec.vo
                    jobSpec.prodSeriesLabel = "pandatest"
                    jobSpec.AtlasRelease = taskSpec.transUses
                    jobSpec.AtlasRelease = re.sub("\r", "", jobSpec.AtlasRelease)
                    jobSpec.maxCpuCount = taskSpec.walltime
                    jobSpec.maxCpuUnit = taskSpec.walltimeUnit
                    if inputChunk.isMerging and splitter is not None:
                        jobSpec.maxDiskCount = splitter.sizeGradientsPerInSizeForMerge
                    else:
                        jobSpec.maxDiskCount = taskSpec.getOutDiskSize()
                    jobSpec.maxDiskUnit = "MB"
                    if inputChunk.isMerging and taskSpec.mergeCoreCount is not None:
                        jobSpec.coreCount = taskSpec.mergeCoreCount
                    elif inputChunk.isMerging and siteSpec.sitename != siteSpec.get_unified_name():
                        jobSpec.coreCount = 1
                    else:
                        if taskSpec.coreCount == 1 or siteSpec.coreCount in [None, 0]:
                            jobSpec.coreCount = 1
                        elif siteSpec.coreCount == -1:
                            jobSpec.coreCount = taskSpec.coreCount
                        else:
                            jobSpec.coreCount = siteSpec.coreCount
                    jobSpec.minRamCount, jobSpec.minRamUnit = JobUtils.getJobMinRamCount(taskSpec, inputChunk, siteSpec, jobSpec.coreCount)
                    # calculate the hs06 occupied by the job
                    if siteSpec.corepower:
                        jobSpec.hs06 = (jobSpec.coreCount or 1) * siteSpec.corepower  # default 0 and None corecount to 1
                    jobSpec.diskIO = taskSpec.diskIO
                    jobSpec.ipConnectivity = "yes"
                    jobSpec.metadata = ""
                    if inputChunk.isMerging:
                        # give higher priority to merge jobs
                        jobSpec.assignedPriority = mergePriority
                    elif inputChunk.useScout():
                        # give higher priority to scouts max(scoutPriority,taskPrio+1)
                        if taskSpec.currentPriority < scoutPriority:
                            jobSpec.assignedPriority = scoutPriority
                        else:
                            jobSpec.assignedPriority = taskSpec.currentPriority + 1
                    else:
                        jobSpec.assignedPriority = taskSpec.currentPriority
                    jobSpec.currentPriority = jobSpec.assignedPriority
                    jobSpec.lockedby = "jedi"
                    jobSpec.workQueue_ID = taskSpec.workQueue_ID
                    jobSpec.gshare = taskSpec.gshare
                    jobSpec.container_name = taskSpec.container_name
                    jobSpec.job_label = JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType)
                    # disable reassign
                    if taskSpec.disableReassign():
                        jobSpec.relocationFlag = 2
                    # using grouping with boundaryID
                    boundaryID = None
                    # flag for merging
                    isUnMerging = False
                    # special handling
                    specialHandling = ""
                    # DDM backend
                    tmpDdmBackEnd = taskSpec.getDdmBackEnd()
                    if tmpDdmBackEnd is not None:
                        if specialHandling == "":
                            specialHandling = f"ddm:{tmpDdmBackEnd},"
                        else:
                            specialHandling += f",ddm:{tmpDdmBackEnd},"
                    # set specialHandling for Event Service
                    if (taskSpec.useEventService(siteSpec) or taskSpec.is_fine_grained_process()) and not inputChunk.isMerging:
                        specialHandling += EventServiceUtils.getHeaderForES(esIndex)
                        if taskSpec.useJumbo is None:
                            # normal ES job
                            jobSpec.eventService = EventServiceUtils.esJobFlagNumber
                        else:
                            # co-jumbo job
                            jobSpec.eventService = EventServiceUtils.coJumboJobFlagNumber
                    # inputs
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("inputs"))
                    prodDBlock = None
                    setProdDBlock = False
                    totalMasterSize = 0
                    totalMasterEvents = 0
                    totalFileSize = 0
                    lumiBlockNr = None
                    setSpecialHandlingForJC = False
                    setInputPrestaging = False
                    segmentName = None
                    segmentID = None
                    for tmpDatasetSpec, tmpFileSpecList in inSubChunk:
                        # get boundaryID if grouping is done with boundaryID
                        if useBoundary is not None and boundaryID is None and tmpDatasetSpec.isMaster():
                            boundaryID = tmpFileSpecList[0].boundaryID
                        # get prodDBlock
                        if not tmpDatasetSpec.isPseudo():
                            if tmpDatasetSpec.isMaster():
                                jobSpec.prodDBlock = tmpDatasetSpec.datasetName
                                setProdDBlock = True
                            else:
                                prodDBlock = tmpDatasetSpec.datasetName
                        # get segment information
                        if taskSpec.is_work_segmented() and tmpDatasetSpec.isMaster() and tmpDatasetSpec.isPseudo():
                            segmentID = tmpDatasetSpec.datasetID
                            segmentName = tmpDatasetSpec.containerName.split("/")[0]
                        # making files
                        for tmpFileSpec in tmpFileSpecList:
                            if inputChunk.isMerging:
                                tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec, setType="input")
                            else:
                                tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec)
                            # set status
                            if tmpFileSpec.locality in ["localdisk", "remote"]:
                                tmpInFileSpec.status = "ready"
                            elif tmpFileSpec.locality == "cache":
                                tmpInFileSpec.status = "cached"
                            elif tmpFileSpec.locality == "localtape":
                                setInputPrestaging = True
                            # local IO
                            if taskSpec.useLocalIO() or (inputChunk.isMerging and "useLocalIO" in taskParamMap["mergeSpec"]):
                                tmpInFileSpec.prodDBlockToken = "local"
                            jobSpec.addFile(tmpInFileSpec)
                            # use remote access
                            if tmpFileSpec.locality == "remote":
                                jobSpec.transferType = siteCandidate.remoteProtocol
                                jobSpec.sourceSite = siteCandidate.remoteSource
                            elif not inputChunk.isMerging and not taskSpec.useLocalIO() and (taskSpec.allowInputLAN() is not None and siteSpec.isDirectIO()):
                                jobSpec.transferType = "direct"
                            # collect old PandaIDs
                            if tmpFileSpec.PandaID is not None and tmpFileSpec.PandaID not in subOldPandaIDs:
                                subOldPandaIDs.append(tmpFileSpec.PandaID)
                                """
                                subOldMergePandaIDs = self.taskBufferIF.getOldMergeJobPandaIDs_JEDI(taskSpec.jediTaskID,
                                                                                                    tmpFileSpec.PandaID)
                                subOldPandaIDs += subOldMergePandaIDs
                                """
                            # set specialHandling for normal Event Service
                            if (
                                (taskSpec.useEventService(siteSpec) or taskSpec.is_fine_grained_process())
                                and not inputChunk.isMerging
                                and tmpDatasetSpec.isMaster()
                            ):
                                if not taskSpec.useJobCloning() or not setSpecialHandlingForJC:
                                    if taskSpec.useJobCloning():
                                        # single event range for job cloning
                                        if tmpFileSpec.startEvent is not None:
                                            nEventsPerWorker = tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
                                        else:
                                            nEventsPerWorker = tmpFileSpec.nEvents
                                        setSpecialHandlingForJC = True
                                    elif taskSpec.is_fine_grained_process():
                                        nEventsPerWorker = 1
                                    else:
                                        nEventsPerWorker = taskSpec.getNumEventsPerWorker()
                                    # set start and end events
                                    tmpStartEvent = tmpFileSpec.startEvent
                                    if tmpStartEvent is None:
                                        tmpStartEvent = 0
                                    tmpEndEvent = tmpFileSpec.endEvent
                                    if tmpEndEvent is None:
                                        tmpEndEvent = tmpFileSpec.nEvents - 1
                                        if tmpEndEvent is None:
                                            tmpEndEvent = 0
                                    if tmpEndEvent < tmpStartEvent:
                                        tmpEndEvent = tmpStartEvent
                                    # first event number and offset without/with in-file positional event numbers
                                    if not taskSpec.inFilePosEvtNum():
                                        tmpFirstEventOffset = taskSpec.getFirstEventOffset()
                                        tmpFirstEvent = tmpFileSpec.firstEvent
                                        if tmpFirstEvent is None:
                                            tmpFirstEvent = tmpFirstEventOffset
                                    else:
                                        tmpFirstEventOffset = None
                                        tmpFirstEvent = None
                                    specialHandling += EventServiceUtils.encodeFileInfo(
                                        tmpFileSpec.lfn,
                                        tmpStartEvent,
                                        tmpEndEvent,
                                        nEventsPerWorker,
                                        taskSpec.getMaxAttemptES(),
                                        tmpFirstEventOffset,
                                        tmpFirstEvent,
                                    )
                            # calculate total master size
                            if tmpDatasetSpec.isMaster():
                                totalMasterSize += CoreUtils.getEffectiveFileSize(
                                    tmpFileSpec.fsize, tmpFileSpec.startEvent, tmpFileSpec.endEvent, tmpFileSpec.nEvents
                                )
                                totalMasterEvents += tmpFileSpec.getEffectiveNumEvents()
                                # set failure count
                                if tmpFileSpec.failedAttempt is not None:
                                    if jobSpec.failedAttempt in [None, "NULL"] or jobSpec.failedAttempt < tmpFileSpec.failedAttempt:
                                        jobSpec.failedAttempt = tmpFileSpec.failedAttempt
                            # total file size
                            if tmpInFileSpec.status != "cached":
                                totalFileSize += tmpFileSpec.fsize
                            # lumi block number
                            if tmpDatasetSpec.isMaster() and lumiBlockNr is None:
                                lumiBlockNr = tmpFileSpec.lumiBlockNr
                        # check if merging
                        if taskSpec.mergeOutput() and tmpDatasetSpec.isMaster() and not tmpDatasetSpec.toMerge():
                            isUnMerging = True
                        # tarball is downloaded by pilot
                        tarball_via_pilot = taskParamMap.get("tarBallViaDDM")
                        if tarball_via_pilot:
                            tmp_file_spec = FileSpec()
                            tmp_file_spec.lfn = tarball_via_pilot.split(":")[-1]
                            tmp_file_spec.type = "input"
                            tmp_file_spec.attemptNr = 0
                            tmp_file_spec.jediTaskID = taskSpec.jediTaskID
                            tmp_file_spec.dataset = tarball_via_pilot.split(":")[0]
                            tmp_file_spec.dispatchDBlock = tmp_file_spec.dataset
                            tmp_file_spec.prodDBlockToken = "local"
                            tmp_file_spec.status = "ready"
                            jobSpec.addFile(tmp_file_spec)
                    specialHandling = specialHandling[:-1]
                    # using job cloning
                    if setSpecialHandlingForJC:
                        specialHandling = EventServiceUtils.setHeaderForJobCloning(specialHandling, taskSpec.getJobCloningType())
                    # dynamic number of events
                    if not inputChunk.isMerging and taskSpec.dynamicNumEvents():
                        specialHandling = EventServiceUtils.setHeaderForDynNumEvents(specialHandling)
                    # merge ES on ObjectStore
                    if taskSpec.mergeEsOnOS():
                        specialHandling = EventServiceUtils.setHeaderForMergeAtOS(specialHandling)
                    # resurrect consumers
                    if taskSpec.resurrectConsumers():
                        specialHandling = EventServiceUtils.setHeaderToResurrectConsumers(specialHandling)
                    # set specialHandling
                    if specialHandling != "":
                        jobSpec.specialHandling = specialHandling
                    # set home cloud
                    if taskSpec.useWorldCloud():
                        jobSpec.setHomeCloud(siteSpec.cloud)
                    # allow partial finish
                    if taskSpec.allowPartialFinish():
                        jobSpec.setToAcceptPartialFinish()
                    # alternative stage-out
                    if taskSpec.mergeOutput() and not inputChunk.isMerging:
                        # disable alternative stage-out for pre-merge jobs
                        jobSpec.setAltStgOut("off")
                    elif taskSpec.getAltStageOut() is not None:
                        jobSpec.setAltStgOut(taskSpec.getAltStageOut())
                    # log to OS
                    if taskSpec.putLogToOS():
                        jobSpec.setToPutLogToOS()
                    # suppress execute string conversion
                    if taskSpec.noExecStrCnv():
                        jobSpec.setNoExecStrCnv()
                    # in-file positional event number
                    if taskSpec.inFilePosEvtNum():
                        jobSpec.setInFilePosEvtNum()
                    # register event service files
                    if taskSpec.registerEsFiles():
                        jobSpec.setRegisterEsFiles()
                    # use prefetcher
                    if taskSpec.usePrefetcher():
                        jobSpec.setUsePrefetcher()
                    # not discard events
                    if taskSpec.notDiscardEvents():
                        jobSpec.setNotDiscardEvents()
                    # decrement attemptNr of events only when failed
                    if taskSpec.decAttOnFailedES():
                        jobSpec.setDecAttOnFailedES()
                    # use zip to pin input files
                    if taskSpec.useZipToPin():
                        jobSpec.setUseZipToPin()
                    # write input to file
                    if taskSpec.writeInputToFile():
                        jobSpec.setToWriteInputToFile()
                    # set lumi block number
                    if lumiBlockNr is not None:
                        jobSpec.setLumiBlockNr(lumiBlockNr)
                    # looping check
                    if taskSpec.no_looping_check():
                        jobSpec.disable_looping_check()
                    # fake job
                    if jobSpec.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs:
                        jobSpec.setFakeJobToIgnore()
                    # use secondary dataset name as prodDBlock
                    if setProdDBlock is False and prodDBlock is not None:
                        jobSpec.prodDBlock = prodDBlock
                    # scout
                    if inputChunk.useScout():
                        jobSpec.setScoutJobFlag()
                    # prestaging
                    if setInputPrestaging:
                        jobSpec.setInputPrestaging()
                    # HPO
                    if taskSpec.is_hpo_workflow():
                        jobSpec.set_hpo_workflow()
                    # encode job parameters
                    if taskSpec.encode_job_params():
                        jobSpec.set_encode_job_params()
                    # use secrets
                    if taskSpec.use_secrets():
                        jobSpec.set_use_secrets()
                    # debug mode
                    if taskSpec.is_debug_mode():
                        jobSpec.set_debug_mode()
                    # publish status changes
                    if taskSpec.push_status_changes():
                        jobSpec.set_push_status_changes()
                    # push job
                    if taskSpec.push_job():
                        jobSpec.set_push_job()
                    # fine-grained process
                    if taskSpec.is_fine_grained_process():
                        EventServiceUtils.set_fine_grained(jobSpec)
                    # on-site merging
                    if taskSpec.on_site_merging():
                        jobSpec.set_on_site_merging()
                    # set task queued time
                    jobSpec.set_task_queued_time(task_queued_time)
                    # extract middle name
                    middleName = ""
                    if taskSpec.getFieldNumToLFN() is not None and jobSpec.prodDBlock not in [None, "NULL", ""]:
                        if inputChunk.isMerging:
                            # extract from LFN of unmerged files
                            for tmpDatasetSpec, tmpFileSpecList in inSubChunk:
                                if not tmpDatasetSpec.isMaster():
                                    try:
                                        middleName = "." + ".".join(tmpFileSpecList[0].lfn.split(".")[4 : 4 + len(taskSpec.getFieldNumToLFN())])
                                    except Exception:
                                        pass
                                    break
                        else:
                            # extract from file or dataset name
                            if taskSpec.useFileAsSourceLFN():
                                for tmpDatasetSpec, tmpFileSpecList in inSubChunk:
                                    if tmpDatasetSpec.isMaster():
                                        middleName = tmpFileSpecList[0].extractFieldsStr(taskSpec.getFieldNumToLFN())
                                        break
                            else:
                                tmpMidStr = jobSpec.prodDBlock.split(":")[-1]
                                tmpMidStrList = re.split("\.|_tid\d+", tmpMidStr)
                                if len(tmpMidStrList) >= max(taskSpec.getFieldNumToLFN()):
                                    middleName = ""
                                    for tmpFieldNum in taskSpec.getFieldNumToLFN():
                                        middleName += "." + tmpMidStrList[tmpFieldNum - 1]
                    # append segment name to middle name
                    if segmentName is not None:
                        if middleName:
                            middleName += "_"
                        middleName += segmentName
                    # set provenanceID
                    provenanceID = None
                    if useBoundary is not None and useBoundary["outMap"] is True:
                        provenanceID = boundaryID
                    # instantiate template datasets
                    instantiateTmpl = False
                    instantiatedSite = None
                    if isUnMerging:
                        instantiateTmpl = True
                        instantiatedSite = siteName
                    elif taskSpec.instantiateTmpl():
                        instantiateTmpl = True
                        if taskSpec.instantiateTmplSite():
                            instantiatedSite = siteName
                    else:
                        instantiateTmpl = False
                    # multiply maxCpuCount by total master size
                    try:
                        if jobSpec.maxCpuCount > 0:
                            jobSpec.maxCpuCount *= totalMasterSize
                            jobSpec.maxCpuCount = int(jobSpec.maxCpuCount)
                        else:
                            # negative cpu count to suppress looping job detection
                            jobSpec.maxCpuCount *= -1
                    except Exception:
                        pass
                    # maxWalltime
                    tmpMasterEventsList.append(totalMasterEvents)
                    CoreUtils.getJobMaxWalltime(taskSpec, inputChunk, totalMasterEvents, jobSpec, siteSpec)
                    # multiply maxDiskCount by total master size or # of events
                    try:
                        if inputChunk.isMerging:
                            jobSpec.maxDiskCount *= totalFileSize
                        elif not taskSpec.outputScaleWithEvents():
                            jobSpec.maxDiskCount *= totalMasterSize
                        else:
                            jobSpec.maxDiskCount *= totalMasterEvents
                    except Exception:
                        pass
                    # add offset to maxDiskCount
                    try:
                        if inputChunk.isMerging and splitter is not None:
                            jobSpec.maxDiskCount += max(taskSpec.getWorkDiskSize(), splitter.interceptsMerginForMerge)
                        else:
                            jobSpec.maxDiskCount += taskSpec.getWorkDiskSize()
                    except Exception:
                        pass
                    # add input size
                    if not CoreUtils.use_direct_io_for_job(taskSpec, siteSpec, inputChunk):
                        jobSpec.maxDiskCount += totalFileSize
                    # maxDiskCount in MB
                    jobSpec.maxDiskCount /= 1024 * 1024
                    jobSpec.maxDiskCount = int(jobSpec.maxDiskCount)
                    # cap not to go over site limit
                    if siteSpec.maxwdir and jobSpec.maxDiskCount and siteSpec.maxwdir < jobSpec.maxDiskCount:
                        jobSpec.maxDiskCount = siteSpec.maxwdir
                    # unset maxCpuCount and minRamCount for merge jobs
                    if inputChunk.isMerging:
                        if merge_max_walltime is None:
                            jobSpec.maxCpuCount = 0
                            jobSpec.maxWalltime = siteSpec.maxtime
                        else:
                            jobSpec.maxCpuCount = merge_max_walltime * 60 * 60
                            jobSpec.maxWalltime = jobSpec.maxCpuCount
                            if taskSpec.baseWalltime is not None:
                                jobSpec.maxWalltime += taskSpec.baseWalltime

                        if jobSpec.minRamCount in [None, "NULL"]:
                            # set 2GB RAM for merge jobs by default
                            jobSpec.minRamCount = 2000

                    # set retry RAM count
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("resource_type"))
                    retry_ram = taskSpec.get_ram_for_retry(jobSpec.minRamCount)
                    if retry_ram:
                        jobSpec.set_ram_for_retry(retry_ram)
                    try:
                        jobSpec.resource_type = JobUtils.get_resource_type_job(self.resource_types, jobSpec)
                    except Exception:
                        jobSpec.resource_type = "Undefined"
                        tmpLog.error(f"set resource_type excepted with {traceback.format_exc()}")
                    # XML config
                    xmlConfigJob = None
                    if xmlConfig is not None:
                        try:
                            xmlConfigJob = xmlConfig.jobs[boundaryID]
                        except Exception:
                            tmpLog.error(f"failed to get XML config for N={boundaryID}")
                            return failedRet
                    # num of output files per job
                    if taskSpec.on_site_merging():
                        tmpStat, taskParamMap = self.readTaskParams(taskSpec, taskParamMap, tmpLog)
                        if not tmpStat:
                            return failedRet
                        if "nEventsPerOutputFile" in taskParamMap and totalMasterEvents:
                            n_files_per_chunk = int(totalMasterEvents / taskParamMap["nEventsPerOutputFile"])
                        else:
                            n_files_per_chunk = 1
                    else:
                        n_files_per_chunk = 1
                    # outputs
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("outputs"))
                    if siteName in fetched_out_sub_chunks and fetched_out_sub_chunks[siteName]:
                        outSubChunk = fetched_out_sub_chunks[siteName].pop(0)
                        serialNr = fetched_serial_numbers[siteName].pop(0)
                        tmpToRegister = []
                        tmpParOutMap = fetched_parallel_out_map[siteName].pop(0)
                    else:
                        outSubChunk, serialNr, tmpToRegister, siteDsMap, tmpParOutMap = self.taskBufferIF.getOutputFiles_JEDI(
                            taskSpec.jediTaskID,
                            provenanceID,
                            simul,
                            instantiateTmpl,
                            instantiatedSite,
                            isUnMerging,
                            False,
                            xmlConfigJob,
                            siteDsMap,
                            middleName,
                            registerDatasets,
                            None,
                            fileIDPool,
                            n_files_per_chunk,
                            master_dataset_id=inputChunk.masterIndexName,
                        )
                    if outSubChunk is None:
                        # failed
                        tmpLog.error("failed to get OutputFiles")
                        return failedRet
                    # number of outputs per job
                    if not simul:
                        try:
                            fileIDPool = fileIDPool[num_outputs_per_job * n_files_per_chunk :]
                        except Exception:
                            fileIDPool = []
                    # update parallel output mapping
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("parallel output"))
                    for tmpParFileID, tmpParFileList in tmpParOutMap.items():
                        if tmpParFileID not in parallelOutMap:
                            parallelOutMap[tmpParFileID] = []
                        parallelOutMap[tmpParFileID] += tmpParFileList
                    for tmpToRegisterItem in tmpToRegister:
                        if tmpToRegisterItem not in datasetToRegister:
                            datasetToRegister.append(tmpToRegisterItem)
                    destinationDBlock = None
                    destination_data_block_type = None
                    for tmpFileSpec in outSubChunk.values():
                        # get dataset
                        if tmpFileSpec.datasetID not in outDsMap:
                            tmpStat, tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID, tmpFileSpec.datasetID)
                            # not found
                            if not tmpStat:
                                tmpLog.error(f"failed to get DS with datasetID={tmpFileSpec.datasetID}")
                                return failedRet
                            outDsMap[tmpFileSpec.datasetID] = tmpDataset
                        # convert to job's FileSpec
                        tmpDatasetSpec = outDsMap[tmpFileSpec.datasetID]
                        tmpOutFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec, useEventService=taskSpec.useEventService(siteSpec))
                        # stay output on site
                        if taskSpec.stayOutputOnSite():
                            tmpOutFileSpec.destinationSE = siteName
                            tmpOutFileSpec.destinationDBlockToken = f"dst:{siteSpec.ddm_output[scope_output]}"
                        # distributed dataset
                        tmpDistributedDestination = DataServiceUtils.getDistributedDestination(tmpOutFileSpec.destinationDBlockToken)
                        if tmpDistributedDestination is not None:
                            tmpDddKey = (siteName, tmpDistributedDestination)
                            if tmpDddKey not in dddMap:
                                dddMap[tmpDddKey] = siteSpec.ddm_endpoints_output[scope_output].getAssociatedEndpoint(tmpDistributedDestination)
                            if dddMap[tmpDddKey] is not None:
                                tmpOutFileSpec.destinationSE = siteName
                                tmpOutFileSpec.destinationDBlockToken = f"ddd:{dddMap[tmpDddKey]['ddm_endpoint_name']}"
                            else:
                                tmpOutFileSpec.destinationDBlockToken = "ddd:"
                        jobSpec.addFile(tmpOutFileSpec)
                        # use the first (preferably non-log) dataset as destinationDBlock
                        if destinationDBlock is None or destination_data_block_type == "log":
                            destinationDBlock = tmpDatasetSpec.datasetName
                            destination_data_block_type = tmpDatasetSpec.type
                    if destinationDBlock is not None:
                        jobSpec.destinationDBlock = destinationDBlock
                    # get datasetSpec for parallel jobs
                    for tmpFileSpecList in parallelOutMap.values():
                        for tmpFileSpec in tmpFileSpecList:
                            if tmpFileSpec.datasetID not in outDsMap:
                                tmpStat, tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID, tmpFileSpec.datasetID)
                                # not found
                                if not tmpStat:
                                    tmpLog.error(f"failed to get DS with datasetID={tmpFileSpec.datasetID}")
                                    return failedRet
                                outDsMap[tmpFileSpec.datasetID] = tmpDataset
                    # lib.tgz. cloning jobs will set it later in increaseEventServiceConsumers
                    paramList = []
                    if buildFileSpec is not None and not taskSpec.useEventService(siteSpec):
                        tmpBuildFileSpec = copy.copy(buildFileSpec)
                        jobSpec.addFile(tmpBuildFileSpec)
                        paramList.append(("LIB", buildFileSpec.lfn))
                    # placeholders for XML config
                    if xmlConfigJob is not None:
                        paramList.append(("XML_OUTMAP", xmlConfigJob.get_outmap_str(outSubChunk)))
                        paramList.append(("XML_EXESTR", xmlConfigJob.exec_string_enc()))
                    # middle name
                    paramList.append(("MIDDLENAME", middleName))
                    # segment ID
                    if segmentID is not None:
                        paramList.append(("SEGMENT_ID", segmentID))
                        paramList.append(("SEGMENT_NAME", segmentName))
                    # job parameter
                    if taskSpec.useEventService(siteSpec) and not taskSpec.useJobCloning():
                        useEStoMakeJP = True
                    else:
                        useEStoMakeJP = False
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("making job params"))
                    jobSpec.jobParameters, multiExecStep = self.makeJobParameters(
                        taskSpec,
                        inSubChunk,
                        outSubChunk,
                        serialNr,
                        paramList,
                        jobSpec,
                        simul,
                        taskParamMap,
                        inputChunk.isMerging,
                        jobSpec.Files,
                        useEStoMakeJP,
                        random_seed_list,
                        random_seed_dataset,
                        tmpLog,
                    )
                    if multiExecStep is not None:
                        jobSpec.addMultiStepExec(multiExecStep)
                    elif taskSpec.on_site_merging():
                        # add merge spec for on-site-merging
                        tmp_data = {"esmergeSpec": copy.deepcopy(taskParamMap["esmergeSpec"])}
                        tmp_data["esmergeSpec"]["nEventsPerOutputFile"] = taskParamMap["nEventsPerOutputFile"]
                        if "nEventsPerInputFile" in taskParamMap:
                            tmp_data["nEventsPerInputFile"] = taskParamMap["nEventsPerInputFile"]
                        jobSpec.addMultiStepExec(tmp_data)
                    # set destinationSE for fake co-jumbo
                    if inputChunk.useJumbo in ["fake", "only"]:
                        jobSpec.destinationSE = DataServiceUtils.checkJobDestinationSE(jobSpec)
                    # add
                    tmpJobSpecList.append(jobSpec)
                    oldPandaIDs.append(subOldPandaIDs)
                    # increment index of event service job
                    if (taskSpec.useEventService(siteSpec) or taskSpec.is_fine_grained_process()) and not inputChunk.isMerging:
                        esIndex += 1
                    # lock task
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time("lock task"))
                    if not simul and len(jobSpecList + tmpJobSpecList) % 50 == 0:
                        self.taskBufferIF.lockTask_JEDI(taskSpec.jediTaskID, self.pid)
                    if self.time_profile_level >= TIME_PROFILE_ON:
                        tmpLog.debug(stop_watch.get_elapsed_time(""))
                # increase event service consumers
                if taskSpec.useEventService(siteSpec) and not inputChunk.isMerging and inputChunk.useJumbo not in ["fake", "only"]:
                    nConsumers = taskSpec.getNumEventServiceConsumer()
                    if nConsumers is not None:
                        tmpJobSpecList, incOldPandaIDs = self.increaseEventServiceConsumers(
                            tmpJobSpecList,
                            nConsumers,
                            taskSpec.getNumSitesPerJob(),
                            parallelOutMap,
                            outDsMap,
                            oldPandaIDs[len(jobSpecList) :],
                            taskSpec,
                            inputChunk,
                            tmpMasterEventsList,
                            build_file_spec_map,
                        )
                        oldPandaIDs = oldPandaIDs[: len(jobSpecList)] + incOldPandaIDs
                # add to all list
                jobSpecList += tmpJobSpecList
            # sort
            if taskSpec.useEventService() and taskSpec.getNumSitesPerJob():
                jobSpecList, oldPandaIDs = self.sortParallelJobsBySite(jobSpecList, oldPandaIDs)
            # make jumbo jobs
            if taskSpec.getNumJumboJobs() is not None and inputChunk.useJumbo is not None and taskSpec.status == "running":
                if inputChunk.useJumbo == "only":
                    jobSpecList = self.makeJumboJobs(jobSpecList, taskSpec, inputChunk, simul, outDsMap, tmpLog)
                else:
                    jobSpecList += self.makeJumboJobs(jobSpecList, taskSpec, inputChunk, simul, outDsMap, tmpLog)
            # return
            return Interaction.SC_SUCCEEDED, jobSpecList, datasetToRegister, oldPandaIDs, parallelOutMap, outDsMap
        except UnresolvedParam:
            raise
        except Exception:
            tmpLog.error(f"{self.__class__.__name__}.doGenerate() failed with {traceback.format_exc()}")
            return failedRet

    # generate build jobs
    def doGenerateBuild(self, taskSpec, cloudName, siteName, siteSpec, taskParamMap, tmpLog, siteCandidate, simul=False):
        # return for failure
        failedRet = Interaction.SC_FAILED, None, None, None
        periodToUselibTgz = 7
        try:
            datasetToRegister = []
            # get sites which share DDM endpoint
            associated_sites = DataServiceUtils.getSitesShareDDM(
                self.siteMapper, siteName, taskSpec.prodSourceLabel, JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType), True
            )
            # convert to sorted unified names and remove duplicates
            associated_sites = [self.siteMapper.getSite(s).get_unified_name() for s in associated_sites]
            associated_sites = sorted(list(set(associated_sites)))
            if siteSpec.get_unified_name() in associated_sites:
                associated_sites.remove(siteSpec.get_unified_name())
            # key for map of buildSpec
            secondKey = [siteSpec.get_unified_name()] + associated_sites
            secondKey.sort()
            buildSpecMapKey = (taskSpec.jediTaskID, tuple(secondKey))
            tmpStat = True
            if buildSpecMapKey in self.buildSpecMap:
                # reuse active lib.tgz
                if self.buildSpecMap[buildSpecMapKey] in self.active_lib_specs_map:
                    fileSpec, datasetSpec = self.active_lib_specs_map[self.buildSpecMap[buildSpecMapKey]]
                else:
                    reuseDatasetID, reuseFileID = self.buildSpecMap[buildSpecMapKey]
                    tmpStat, fileSpec, datasetSpec = self.taskBufferIF.getOldBuildFileSpec_JEDI(taskSpec.jediTaskID, reuseDatasetID, reuseFileID)
                    if fileSpec is not None:
                        self.active_lib_specs_map[self.buildSpecMap[buildSpecMapKey]] = (fileSpec, datasetSpec)
            else:
                # get finished lib.tgz file
                if buildSpecMapKey in self.finished_lib_specs_map:
                    fileSpec, datasetSpec = self.finished_lib_specs_map[buildSpecMapKey]
                else:
                    tmpStat, fileSpec, datasetSpec = self.taskBufferIF.get_previous_build_file_spec(
                        taskSpec.jediTaskID, siteSpec.get_unified_name(), associated_sites
                    )
                    if fileSpec is not None:
                        self.finished_lib_specs_map[buildSpecMapKey] = (fileSpec, datasetSpec)
            # failed
            if not tmpStat:
                tmpLog.error(f"failed to get lib.tgz for jediTaskID={taskSpec.jediTaskID} siteName={siteName}")
                return failedRet
            # lib.tgz is already available
            if fileSpec is not None:
                if fileSpec.creationDate > naive_utcnow() - datetime.timedelta(days=periodToUselibTgz):
                    pandaFileSpec = fileSpec.convertToJobFileSpec(datasetSpec, setType="input")
                    pandaFileSpec.dispatchDBlock = pandaFileSpec.dataset
                    pandaFileSpec.prodDBlockToken = "local"
                    if fileSpec.status == "finished":
                        pandaFileSpec.status = "ready"
                    # make dummy jobSpec
                    jobSpec = JobSpec()
                    jobSpec.addFile(pandaFileSpec)
                    return Interaction.SC_SUCCEEDED, None, pandaFileSpec, datasetToRegister
                else:
                    # not to use very old lib.tgz
                    fileSpec = None
            # make job spec for build
            jobSpec = JobSpec()
            jobSpec.jobDefinitionID = 0
            jobSpec.jobExecutionID = 0
            jobSpec.attemptNr = 1
            jobSpec.maxAttempt = jobSpec.attemptNr
            jobSpec.jobName = taskSpec.taskName
            jobSpec.transformation = taskParamMap["buildSpec"]["transPath"]
            platforms = siteCandidate.get_overridden_attribute("platforms")
            if platforms:
                jobSpec.cmtConfig = platforms
            else:
                jobSpec.cmtConfig = taskSpec.get_platforms()
            if taskSpec.transHome is not None:
                jobSpec.homepackage = re.sub("-(?P<dig>\d+\.)", "/\g<dig>", taskSpec.transHome)
                jobSpec.homepackage = re.sub("\r", "", jobSpec.homepackage)
            jobSpec.prodSourceLabel = taskParamMap["buildSpec"]["prodSourceLabel"]
            jobSpec.processingType = taskSpec.processingType
            jobSpec.jediTaskID = taskSpec.jediTaskID
            jobSpec.jobsetID = taskSpec.reqID
            jobSpec.reqID = taskSpec.reqID
            jobSpec.workingGroup = taskSpec.workingGroup
            jobSpec.countryGroup = taskSpec.countryGroup
            jobSpec.computingSite = siteSpec.get_unified_name()
            jobSpec.nucleus = taskSpec.nucleus
            jobSpec.cloud = cloudName
            jobSpec.VO = taskSpec.vo
            jobSpec.prodSeriesLabel = "pandatest"
            jobSpec.AtlasRelease = taskSpec.transUses
            jobSpec.AtlasRelease = re.sub("\r", "", jobSpec.AtlasRelease)
            jobSpec.lockedby = "jedi"
            jobSpec.workQueue_ID = taskSpec.workQueue_ID
            jobSpec.gshare = taskSpec.gshare
            jobSpec.container_name = taskSpec.container_name
            jobSpec.metadata = ""

            if taskSpec.coreCount == 1 or siteSpec.coreCount in [None, 0] or siteSpec.sitename != siteSpec.get_unified_name():
                jobSpec.coreCount = 1
            else:
                jobSpec.coreCount = siteSpec.coreCount

            # for the memory, we are going to use 2GB as a default value, as this was the usual ATLAS slot memory and it has always fit the build jobs.
            # There is no other major reason and it could be over-dimensioned.
            if siteSpec.maxrss:
                jobSpec.minRamCount = min(2000 * jobSpec.coreCount, siteSpec.maxrss)
            else:
                jobSpec.minRamCount = 2000 * jobSpec.coreCount

            try:
                jobSpec.resource_type = JobUtils.get_resource_type_job(self.resource_types, jobSpec)
            except Exception:
                jobSpec.resource_type = "Undefined"
                tmpLog.error(f"set resource_type excepted with {traceback.format_exc()}")
            # calculate the hs06 occupied by the job
            if siteSpec.corepower:
                jobSpec.hs06 = (jobSpec.coreCount or 1) * siteSpec.corepower  # default 0 and None corecount to 1
            # set CPU count
            buildJobMaxWalltime = self.taskBufferIF.getConfigValue("jobgen", "BUILD_JOB_MAX_WALLTIME", "jedi", taskSpec.vo)
            if buildJobMaxWalltime is None:
                jobSpec.maxCpuCount = 0
                jobSpec.maxWalltime = siteSpec.maxtime
            else:
                jobSpec.maxCpuCount = buildJobMaxWalltime * 60 * 60
                jobSpec.maxWalltime = jobSpec.maxCpuCount
                if taskSpec.baseWalltime is not None:
                    jobSpec.maxWalltime += taskSpec.baseWalltime
            jobSpec.maxCpuUnit = taskSpec.walltimeUnit
            # make libDS name
            if (
                datasetSpec is None
                or fileSpec is None
                or datasetSpec.state == "closed"
                or datasetSpec.creationTime < naive_utcnow() - datetime.timedelta(days=periodToUselibTgz)
            ):
                # make new libDS
                reusedDatasetID = None
                libDsName = f"panda.{time.strftime('%m%d%H%M%S', time.gmtime())}.{naive_utcnow().microsecond}.lib._{jobSpec.jediTaskID:06d}"
            else:
                # reuse existing DS
                reusedDatasetID = datasetSpec.datasetID
                libDsName = datasetSpec.datasetName
            jobSpec.destinationDBlock = libDsName
            jobSpec.destinationSE = siteName
            # special handling
            specialHandling = ""
            # DDM backend
            tmpDdmBackEnd = taskSpec.getDdmBackEnd()
            if tmpDdmBackEnd is not None:
                if specialHandling == "":
                    specialHandling = f"ddm:{tmpDdmBackEnd},"
                else:
                    specialHandling += f"ddm:{tmpDdmBackEnd},"
            specialHandling = specialHandling[:-1]
            if specialHandling != "":
                jobSpec.specialHandling = specialHandling
            jobSpec.set_task_queued_time(taskSpec.get_queued_time())
            libDsFileNameBase = libDsName + ".$JEDIFILEID"
            # make lib.tgz
            libTgzName = libDsFileNameBase + ".lib.tgz"
            lib_file_spec = FileSpec()
            lib_file_spec.lfn = libTgzName
            lib_file_spec.type = "output"
            lib_file_spec.attemptNr = 0
            lib_file_spec.jediTaskID = taskSpec.jediTaskID
            lib_file_spec.dataset = jobSpec.destinationDBlock
            lib_file_spec.destinationDBlock = jobSpec.destinationDBlock
            lib_file_spec.destinationSE = jobSpec.destinationSE
            jobSpec.addFile(lib_file_spec)
            # make log
            logFileSpec = FileSpec()
            logFileSpec.lfn = libDsFileNameBase + ".log.tgz"
            logFileSpec.type = "log"
            logFileSpec.attemptNr = 0
            logFileSpec.jediTaskID = taskSpec.jediTaskID
            logFileSpec.dataset = jobSpec.destinationDBlock
            logFileSpec.destinationDBlock = jobSpec.destinationDBlock
            logFileSpec.destinationSE = jobSpec.destinationSE
            jobSpec.addFile(logFileSpec)
            # insert lib.tgz file
            tmpStat, fileIdMap = self.taskBufferIF.insertBuildFileSpec_JEDI(jobSpec, reusedDatasetID, simul)
            # failed
            if not tmpStat:
                tmpLog.error("failed to insert libDS for jediTaskID={0} siteName={0}".format(taskSpec.jediTaskID, siteName))
                return failedRet
            # set attributes
            for tmpFile in jobSpec.Files:
                tmpFile.fileID = fileIdMap[tmpFile.lfn]["fileID"]
                tmpFile.datasetID = fileIdMap[tmpFile.lfn]["datasetID"]
                tmpFile.scope = fileIdMap[tmpFile.lfn]["scope"]
                # set new LFN where place holder is replaced
                tmpFile.lfn = fileIdMap[tmpFile.lfn]["newLFN"]
                if tmpFile.datasetID not in datasetToRegister:
                    datasetToRegister.append(tmpFile.datasetID)
            # append to map of buildSpec
            self.buildSpecMap[buildSpecMapKey] = (fileIdMap[libTgzName]["datasetID"], fileIdMap[libTgzName]["fileID"])
            # parameter map
            paramMap = {}
            paramMap["OUT"] = lib_file_spec.lfn
            paramMap["IN"] = taskParamMap["buildSpec"]["archiveName"]
            paramMap["SURL"] = taskParamMap["sourceURL"]
            # job parameter
            jobSpec.jobParameters = self.makeBuildJobParameters(taskParamMap["buildSpec"]["jobParameters"], paramMap)
            # tarball is downloaded by pilot
            tarball_via_pilot = taskParamMap["buildSpec"].get("tarBallViaDDM")
            if tarball_via_pilot:
                tmp_file_spec = FileSpec()
                tmp_file_spec.lfn = tarball_via_pilot.split(":")[-1]
                tmp_file_spec.type = "input"
                tmp_file_spec.attemptNr = 0
                tmp_file_spec.jediTaskID = taskSpec.jediTaskID
                tmp_file_spec.dataset = tarball_via_pilot.split(":")[0]
                tmp_file_spec.dispatchDBlock = tmp_file_spec.dataset
                tmp_file_spec.prodDBlockToken = "local"
                tmp_file_spec.status = "ready"
                jobSpec.addFile(tmp_file_spec)
            # make file spec which will be used by runJobs
            runFileSpec = copy.copy(lib_file_spec)
            runFileSpec.dispatchDBlock = lib_file_spec.dataset
            runFileSpec.destinationDBlock = None
            runFileSpec.type = "input"
            runFileSpec.prodDBlockToken = "local"
            # return
            return Interaction.SC_SUCCEEDED, jobSpec, runFileSpec, datasetToRegister
        except Exception:
            tmpLog.error(f"{self.__class__.__name__}.doGenerateBuild() failed with {traceback.format_exc()}")
            return failedRet

    # generate preprocessing jobs
    def doGeneratePrePro(self, taskSpec, cloudName, siteName, siteSpec, taskParamMap, inSubChunks, tmpLog, simul=False):
        # return for failure
        failedRet = Interaction.SC_FAILED, None, None
        try:
            # make job spec for preprocessing
            jobSpec = JobSpec()
            jobSpec.jobDefinitionID = 0
            jobSpec.jobExecutionID = 0
            jobSpec.attemptNr = 1
            jobSpec.maxAttempt = jobSpec.attemptNr
            jobSpec.jobName = taskSpec.taskName
            jobSpec.transformation = taskParamMap["preproSpec"]["transPath"]
            jobSpec.prodSourceLabel = taskParamMap["preproSpec"]["prodSourceLabel"]
            jobSpec.processingType = taskSpec.processingType
            jobSpec.jediTaskID = taskSpec.jediTaskID
            jobSpec.jobsetID = taskSpec.reqID
            jobSpec.reqID = taskSpec.reqID
            jobSpec.workingGroup = taskSpec.workingGroup
            jobSpec.countryGroup = taskSpec.countryGroup
            jobSpec.computingSite = siteSpec.get_unified_name()
            jobSpec.nucleus = taskSpec.nucleus
            jobSpec.cloud = cloudName
            jobSpec.VO = taskSpec.vo
            jobSpec.prodSeriesLabel = "pandatest"
            """
            jobSpec.AtlasRelease     = taskSpec.transUses
            jobSpec.AtlasRelease     = re.sub('\r','',jobSpec.AtlasRelease)
            """
            jobSpec.lockedby = "jedi"
            jobSpec.workQueue_ID = taskSpec.workQueue_ID
            jobSpec.gshare = taskSpec.gshare
            jobSpec.destinationSE = siteName
            jobSpec.metadata = ""
            jobSpec.coreCount = 1
            if siteSpec.corepower:
                jobSpec.hs06 = (jobSpec.coreCount or 1) * siteSpec.corepower
            # get log file
            outSubChunk, serialNr, datasetToRegister, siteDsMap, parallelOutMap = self.taskBufferIF.getOutputFiles_JEDI(
                taskSpec.jediTaskID, None, simul, True, siteName, False, True
            )
            if outSubChunk is None:
                # failed
                tmpLog.error("doGeneratePrePro failed to get OutputFiles")
                return failedRet
            outDsMap = {}
            for tmpFileSpec in outSubChunk.values():
                # get dataset
                if tmpFileSpec.datasetID not in outDsMap:
                    tmpStat, tmpDataset = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID, tmpFileSpec.datasetID)
                    # not found
                    if not tmpStat:
                        tmpLog.error(f"doGeneratePrePro failed to get logDS with datasetID={tmpFileSpec.datasetID}")
                        return failedRet
                    outDsMap[tmpFileSpec.datasetID] = tmpDataset
                # convert to job's FileSpec
                tmpDatasetSpec = outDsMap[tmpFileSpec.datasetID]
                tmpOutFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec, setType="log")
                jobSpec.addFile(tmpOutFileSpec)
                jobSpec.destinationDBlock = tmpDatasetSpec.datasetName
            # make pseudo input
            tmpDatasetSpec, tmpFileSpecList = inSubChunks[0][0]
            tmpFileSpec = tmpFileSpecList[0]
            tmpInFileSpec = tmpFileSpec.convertToJobFileSpec(tmpDatasetSpec, setType="pseudo_input")
            jobSpec.addFile(tmpInFileSpec)
            # parameter map
            paramMap = {}
            paramMap["SURL"] = taskParamMap["sourceURL"]
            # job parameter
            jobSpec.jobParameters = self.makeBuildJobParameters(taskParamMap["preproSpec"]["jobParameters"], paramMap)
            # return
            return Interaction.SC_SUCCEEDED, jobSpec, datasetToRegister
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"{self.__class__.__name__}.doGeneratePrePro() failed with {traceback.format_exc()}")
            return failedRet

    # make job parameters
    def makeJobParameters(
        self,
        taskSpec,
        inSubChunk,
        outSubChunk,
        serialNr,
        paramList,
        jobSpec,
        simul,
        taskParamMap,
        isMerging,
        jobFileList,
        useEventService,
        random_seed_list,
        random_seed_dataset,
        tmp_log,
    ):
        stop_watch = CoreUtils.StopWatch("make_params")
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("init"))
        if not isMerging:
            parTemplate = taskSpec.jobParamsTemplate
        else:
            # use params template for merging
            parTemplate = taskParamMap["mergeSpec"]["jobParameters"]
        # make the list of stream/LFNs
        streamLFNsMap = {}
        # mapping between stream and dataset
        streamDsMap = {}
        # parameters for placeholders
        skipEvents = None
        maxEvents = 0
        firstEvent = None
        rndmSeed = None
        sourceURL = None
        # source URL
        if taskParamMap is not None and "sourceURL" in taskParamMap:
            sourceURL = taskParamMap["sourceURL"]
        # get random seed
        if taskSpec.useRandomSeed() and not isMerging:
            tmpRandomFileSpec = None
            if random_seed_list:
                tmpRandomFileSpec = random_seed_list.pop(0)
                tmpRandomDatasetSpec = random_seed_dataset
            else:
                tmpStat, randomSpecList = self.taskBufferIF.getRandomSeed_JEDI(taskSpec.jediTaskID, simul, 1)
                if tmpStat is True:
                    tmp_file_spec_list, tmpRandomDatasetSpec = randomSpecList
                    if tmp_file_spec_list:
                        tmpRandomFileSpec = tmp_file_spec_list[0]
            if tmpRandomFileSpec is not None:
                tmpJobFileSpec = tmpRandomFileSpec.convertToJobFileSpec(tmpRandomDatasetSpec, setType="pseudo_input")
                rndmSeed = tmpRandomFileSpec.firstEvent + taskSpec.getRndmSeedOffset()
                jobSpec.addFile(tmpJobFileSpec)
        # input
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("input"))
        for tmpDatasetSpec, tmpFileSpecList in inSubChunk:
            # stream name
            streamName = tmpDatasetSpec.streamName
            # LFNs
            tmpLFNs = []
            size_map = {}
            for tmpFileSpec in tmpFileSpecList:
                tmpLFNs.append(tmpFileSpec.lfn)
                size_map[tmpFileSpec.lfn] = tmpFileSpec.fsize
            # tweak file list if necessary
            if isMerging:
                # sort by descending size not to process empty files first
                tmpLFNs.sort(key=lambda kkk: size_map[kkk], reverse=True)
            elif taskSpec.dynamicNumEvents():
                # remove duplication for dynamic number of events while preserving the file order
                tmpLFNs = list(dict.fromkeys(tmpLFNs))
            elif not taskSpec.order_input_by():
                # sort by LFN unless the order is specified
                tmpLFNs.sort()
            # change stream name and LFNs for PFN list
            if taskSpec.useListPFN() and tmpDatasetSpec.isMaster() and tmpDatasetSpec.isPseudo():
                streamName = "IN"
                tmpPFNs = []
                for tmpLFN in tmpLFNs:
                    # remove serial number and num events
                    tmpPFN = taskParamMap["pfnList"][int(tmpLFN.split(":")[0])].split("^")[0]
                    tmpPFNs.append(tmpPFN)
                tmpLFNs = tmpPFNs
            # add to map
            streamLFNsMap[streamName] = tmpLFNs
            # collect dataset or container name to be used as tmp file name
            if tmpDatasetSpec.containerName not in [None, ""]:
                streamDsMap[streamName] = tmpDatasetSpec.containerName
            else:
                streamDsMap[streamName] = tmpDatasetSpec.datasetName
            streamDsMap[streamName] = re.sub("/$", "", streamDsMap[streamName])
            streamDsMap[streamName] = streamDsMap[streamName].split(":")[-1]
            # collect parameters for event-level split
            if tmpDatasetSpec.isMaster():
                # skipEvents and firstEvent
                for tmpFileSpec in tmpFileSpecList:
                    if firstEvent is None and tmpFileSpec.firstEvent is not None:
                        firstEvent = tmpFileSpec.firstEvent
                    if skipEvents is None and tmpFileSpec.startEvent is not None:
                        skipEvents = tmpFileSpec.startEvent
                    if tmpFileSpec.startEvent is not None and tmpFileSpec.endEvent is not None:
                        maxEvents += tmpFileSpec.endEvent - tmpFileSpec.startEvent + 1
        # set zero if undefined
        if skipEvents is None:
            skipEvents = 0
        # set -1 if maxEvents is zero since it breaks some apps
        if maxEvents == 0:
            maxEvents = -1
        # output
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("output"))
        for streamName, tmpFileSpec in outSubChunk.items():
            streamName = streamName.split("|")[0]
            streamLFNsMap.setdefault(streamName, [])
            streamLFNsMap[streamName].append(tmpFileSpec.lfn)
        # extract place holders with range expression, e.g., IN[0:2]
        for tmpMatch in re.finditer("\$\{([^\}]+)\}", parTemplate):
            tmpPatt = tmpMatch.group(1)
            # split to stream name and range expression
            tmpStRaMatch = re.search("([^\[]+)(.*)", tmpPatt)
            if tmpStRaMatch is not None:
                tmpStream = tmpStRaMatch.group(1)
                tmpRange = tmpStRaMatch.group(2)
                if tmpPatt != tmpStream and tmpStream in streamLFNsMap:
                    try:
                        exec(f"streamLFNsMap['{tmpPatt}']=streamLFNsMap['{tmpStream}']{tmpRange}", globals())
                    except Exception:
                        pass
        # loop over all streams to collect transient and final steams
        transientStreamCombo = {}
        streamToDelete = {}
        if isMerging:
            for streamName in streamLFNsMap.keys():
                # collect transient and final steams
                if streamName is not None and not streamName.startswith("TRN_"):
                    counterStreamName = "TRN_" + streamName
                    if streamName == "LOG_MERGE" and "TRN_LOG0" in streamLFNsMap:
                        transientStreamCombo[streamName] = {
                            "out": streamName,
                            "in": "TRN_LOG0",
                        }
                    elif counterStreamName not in streamLFNsMap:
                        # streams to be deleted
                        streamToDelete[streamName] = streamLFNsMap[streamName]
                        streamToDelete[counterStreamName] = []
                    else:
                        transientStreamCombo[streamName] = {
                            "out": streamName,
                            "in": counterStreamName,
                        }
        # delete empty streams
        for streamName in streamToDelete.keys():
            try:
                del streamLFNsMap[streamName]
            except Exception:
                pass
        # loop over all placeholders
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("placeholders"))
        for tmpMatch in re.finditer("\$\{([^\}]+)\}", parTemplate):
            placeHolder = tmpMatch.group(1)
            # remove decorators
            streamNames = placeHolder.split("/")[0]
            streamNameList = streamNames.split(",")
            listLFN = []
            for streamName in streamNameList:
                if streamName in streamLFNsMap:
                    listLFN += streamLFNsMap[streamName]
            if listLFN != []:
                decorators = re.sub("^" + streamNames, "", placeHolder)
                # long format
                if "/L" in decorators:
                    longLFNs = ""
                    for tmpLFN in listLFN:
                        if "/A" in decorators:
                            longLFNs += "'"
                        longLFNs += tmpLFN
                        if "/A" in decorators:
                            longLFNs += "'"
                        if "/S" in decorators:
                            # use white-space as separator
                            longLFNs += " "
                        else:
                            longLFNs += ","
                    if taskSpec.usingJumboJobs():
                        parTemplate = parTemplate.replace("${" + placeHolder + "}", "tmpin__cnt_" + streamDsMap[streamName])
                    else:
                        longLFNs = longLFNs[:-1]
                        parTemplate = parTemplate.replace("${" + placeHolder + "}", longLFNs)
                    continue
                # list to string
                if "/T" in decorators:
                    parTemplate = parTemplate.replace("${" + placeHolder + "}", str(listLFN))
                    continue
                # write to file
                if "/F" in decorators:
                    parTemplate = parTemplate.replace("${" + placeHolder + "}", "tmpin_" + streamDsMap[streamName])
                # single file
                if len(listLFN) == 1:
                    # just replace with the original file name
                    replaceStr = listLFN[0]
                    parTemplate = parTemplate.replace("${" + streamNames + "}", replaceStr)
                    # encoded
                    encStreamName = streamNames + "/E"
                    replaceStr = unquote(replaceStr)
                    parTemplate = parTemplate.replace("${" + encStreamName + "}", replaceStr)
                    # mathematics
                    if "/M" in decorators:
                        tmp_m = re.search(r"/M\[([^\]]+)\]", decorators)
                        if tmp_m:
                            tmp_formula = tmp_m.group(1).replace("#", listLFN[0])
                            replaceStr = str(eval(tmp_formula))
                            parTemplate = parTemplate.replace("${" + placeHolder + "}", replaceStr)
                else:
                    # compact format
                    compactLFNs = []
                    # remove attempt numbers
                    fullLFNList = ""
                    for tmpLFN in listLFN:
                        # keep full LFNs
                        fullLFNList += f"{tmpLFN},"
                        compactLFNs.append(re.sub("\.\d+$", "", tmpLFN))
                    fullLFNList = fullLFNList[:-1]
                    # find head and tail to convert file.1.pool,file.2.pool,file.4.pool to file.[1,2,4].pool
                    tmpHead = ""
                    tmpTail = ""
                    tmpLFN0 = compactLFNs[0]
                    tmpLFN1 = compactLFNs[1]
                    i = 0
                    for s1, s2 in zip(tmpLFN0, tmpLFN1):
                        if s1 != s2:
                            break
                        i += 1
                        tmpHead = tmpLFN0[:i]
                    i = 0
                    for s1, s2 in zip(tmpLFN0[::-1], tmpLFN1[::-1]):
                        if s1 != s2:
                            break
                        i += 1
                        tmpTail = tmpLFN0[-i:]
                    # remove numbers : ABC_00,00_XYZ -> ABC_,_XYZ
                    tmpHead = re.sub("\d*$", "", tmpHead)
                    tmpTail = re.sub("^\d*", "", tmpTail)
                    # create compact paramter
                    compactPar = f"{tmpHead}["
                    for tmpLFN in compactLFNs:
                        # extract number
                        tmpLFN = re.sub(f"^{tmpHead}", "", tmpLFN)
                        tmpLFN = re.sub(f"{tmpTail}$", "", tmpLFN)
                        compactPar += f"{tmpLFN},"
                    compactPar = compactPar[:-1]
                    compactPar += f"]{tmpTail}"
                    # check contents in []
                    conMatch = re.search("\[([^\]]+)\]", compactPar)
                    if conMatch is not None and re.search("^[\d,]+$", conMatch.group(1)) is not None:
                        # replace with compact format
                        replaceStr = compactPar
                    else:
                        # replace with full format since [] contains non digits
                        replaceStr = fullLFNList
                    parTemplate = parTemplate.replace("${" + streamNames + "}", replaceStr)
                    # encoded
                    encStreamName = streamNames + "/E"
                    replaceStr = unquote(fullLFNList)
                    parTemplate = parTemplate.replace("${" + encStreamName + "}", replaceStr)
        # replace params related to transient files
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("transient files"))
        replaceStrMap = {}
        emptyStreamMap = {}
        for streamName, transientStreamMap in transientStreamCombo.items():
            # remove serial number
            streamNameBase = re.sub("\d+$", "", streamName)
            # empty streams
            if streamNameBase not in emptyStreamMap:
                emptyStreamMap[streamNameBase] = []
            # make param
            replaceStr = ""
            if streamLFNsMap[transientStreamMap["in"]] == []:
                emptyStreamMap[streamNameBase].append(streamName)
            else:
                for tmpLFN in streamLFNsMap[transientStreamMap["in"]]:
                    replaceStr += f"{tmpLFN},"
                replaceStr = replaceStr[:-1]
                replaceStr += ":"
                for tmpLFN in streamLFNsMap[transientStreamMap["out"]]:
                    replaceStr += f"{tmpLFN},"
                replaceStr = replaceStr[:-1]
            # concatenate per base stream name
            if streamNameBase not in replaceStrMap:
                replaceStrMap[streamNameBase] = ""
            replaceStrMap[streamNameBase] += f"{replaceStr} "
        for streamNameBase, replaceStr in replaceStrMap.items():
            targetName = "${TRN_" + streamNameBase + ":" + streamNameBase + "}"
            if targetName in parTemplate:
                parTemplate = parTemplate.replace(targetName, replaceStr)
                # remove outputs with empty input files
                for emptyStream in emptyStreamMap[streamNameBase]:
                    tmpFileIdx = 0
                    for tmpJobFileSpec in jobFileList:
                        if tmpJobFileSpec.lfn in streamLFNsMap[emptyStream]:
                            jobFileList.pop(tmpFileIdx)
                            break
                        tmpFileIdx += 1
        # remove outputs and params for deleted streams
        for streamName, deletedLFNs in streamToDelete.items():
            # remove params
            parTemplate = re.sub("--[^=]+=\$\{" + streamName + "\}", "", parTemplate)
            # remove output files
            if deletedLFNs == []:
                continue
            tmpFileIdx = 0
            for tmpJobFileSpec in jobFileList:
                if tmpJobFileSpec.lfn in deletedLFNs:
                    jobFileList.pop(tmpFileIdx)
                    break
                tmpFileIdx += 1
        # replace placeholders for numbers
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("numbers"))
        if serialNr is None:
            serialNr = 0
        for streamName, parVal in [
            ("SN", serialNr),
            ("SN/P", f"{serialNr:06d}"),
            ("RNDMSEED", rndmSeed),
            ("MAXEVENTS", maxEvents),
            ("SKIPEVENTS", skipEvents),
            ("FIRSTEVENT", firstEvent),
            ("SURL", sourceURL),
            ("ATTEMPTNR", jobSpec.attemptNr),
        ] + paramList:
            # ignore undefined
            if parVal is None:
                continue
            # replace
            parTemplate = parTemplate.replace("${" + streamName + "}", str(parVal))
        # replace unmerge files
        for jobFileSpec in jobFileList:
            if jobFileSpec.isUnMergedOutput():
                mergedFileName = re.sub("^panda\.um\.", "", jobFileSpec.lfn)
                parTemplate = parTemplate.replace(mergedFileName, jobFileSpec.lfn)
        # remove duplicated panda.um
        parTemplate = parTemplate.replace("panda.um.panda.um.", "panda.um.")
        # remove ES parameters if necessary
        if useEventService:
            parTemplate = parTemplate.replace("<PANDA_ES_ONLY>", "")
            parTemplate = parTemplate.replace("</PANDA_ES_ONLY>", "")
        else:
            parTemplate = re.sub("<PANDA_ES_ONLY>[^<]*</PANDA_ES_ONLY>", "", parTemplate)
            parTemplate = re.sub("<PANDA_ESMERGE.*>[^<]*</PANDA_ESMERGE.*>", "", parTemplate)
        # multi-step execution
        if not taskSpec.is_multi_step_exec():
            multiExecSpec = None
        else:
            multiExecSpec = copy.deepcopy(taskParamMap["multiStepExec"])
            for k, v in multiExecSpec.items():
                for kk, vv in v.items():
                    # resolve placeholders
                    new_vv = vv.replace("${TRF_ARGS}", parTemplate)
                    new_vv = new_vv.replace("${TRF}", jobSpec.transformation)
                    v[kk] = new_vv
        # check unresolved placeholders
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time("unresolved placeholders"))
        matchI = re.search("\${IN.*}", parTemplate)
        if matchI is None:
            matchI = re.search("\${SEQNUMBER}", parTemplate)
        if matchI is not None:
            raise UnresolvedParam(f"unresolved {matchI.group(0)} when making job parameters")
        # return
        if self.time_profile_level >= TIME_PROFILE_DEEP:
            tmp_log.debug(stop_watch.get_elapsed_time(""))
        return parTemplate, multiExecSpec

    # make build/prepro job parameters
    def makeBuildJobParameters(self, jobParameters, paramMap):
        parTemplate = jobParameters
        # replace placeholders
        for streamName, parVal in paramMap.items():
            # ignore undefined
            if parVal is None:
                continue
            # replace
            parTemplate = parTemplate.replace("${" + streamName + "}", str(parVal))
        # return
        return parTemplate

    # increase event service consumers
    def increaseEventServiceConsumers(
        self, pandaJobs, nConsumers, nSitesPerJob, parallelOutMap, outDsMap, oldPandaIDs, taskSpec, inputChunk, masterEventsList, build_spec_map
    ):
        newPandaJobs = []
        newOldPandaIDs = []
        for pandaJob, oldPandaID, masterEvents in zip(pandaJobs, oldPandaIDs, masterEventsList):
            for iConsumers in range(nConsumers):
                newPandaJob = self.clonePandaJob(
                    pandaJob,
                    iConsumers,
                    parallelOutMap,
                    outDsMap,
                    taskSpec=taskSpec,
                    inputChunk=inputChunk,
                    totalMasterEvents=masterEvents,
                    build_spec_map=build_spec_map,
                )
                newPandaJobs.append(newPandaJob)
                newOldPandaIDs.append(oldPandaID)
        # return
        return newPandaJobs, newOldPandaIDs

    # close panda job with new specialHandling
    def clonePandaJob(
        self, pandaJob, index, parallelOutMap, outDsMap, sites=None, forJumbo=False, taskSpec=None, inputChunk=None, totalMasterEvents=None, build_spec_map=None
    ):
        newPandaJob = copy.copy(pandaJob)
        if sites is None:
            sites = newPandaJob.computingSite.split(",")
        nSites = len(sites)
        # set site for parallel jobs
        newPandaJob.computingSite = sites[index % nSites]
        siteSpec = self.siteMapper.getSite(newPandaJob.computingSite)
        scope_input, scope_output = select_scope(siteSpec, pandaJob.prodSourceLabel, pandaJob.job_label)
        siteCandidate = inputChunk.getSiteCandidate(newPandaJob.computingSite)
        newPandaJob.computingSite = siteSpec.get_unified_name()
        if taskSpec.coreCount == 1 or siteSpec.coreCount in [None, 0]:
            newPandaJob.coreCount = 1
        else:
            newPandaJob.coreCount = siteSpec.coreCount
        if taskSpec is not None and inputChunk is not None:
            newPandaJob.minRamCount, newPandaJob.minRamUnit = JobUtils.getJobMinRamCount(taskSpec, inputChunk, siteSpec, newPandaJob.coreCount)
            newPandaJob.hs06 = (newPandaJob.coreCount or 1) * siteSpec.corepower
            if totalMasterEvents is not None:
                CoreUtils.getJobMaxWalltime(taskSpec, inputChunk, totalMasterEvents, newPandaJob, siteSpec)
        try:
            newPandaJob.resource_type = JobUtils.get_resource_type_job(self.resource_types, newPandaJob)
        except Exception:
            newPandaJob.resource_type = "Undefined"
        datasetList = set()
        # reset SH for jumbo
        if forJumbo:
            EventServiceUtils.removeHeaderForES(newPandaJob)
        # clone files
        newPandaJob.Files = []
        for fileSpec in pandaJob.Files:
            # copy files
            if (
                forJumbo
                or nSites == 1
                or fileSpec.type not in ["log", "output"]
                or (fileSpec.fileID in parallelOutMap and len(parallelOutMap[fileSpec.fileID]) == 1)
            ):
                newFileSpec = copy.copy(fileSpec)
            else:
                newFileSpec = parallelOutMap[fileSpec.fileID][index % nSites]
                datasetSpec = outDsMap[newFileSpec.datasetID]
                newFileSpec = newFileSpec.convertToJobFileSpec(datasetSpec, useEventService=True)
            # set locality
            if inputChunk is not None and newFileSpec.type == "input":
                if siteCandidate is not None:
                    locality = siteCandidate.getFileLocality(newFileSpec)
                    if locality in ["localdisk", "remote"]:
                        newFileSpec.status = "ready"
                    elif locality == "cache":
                        newFileSpec.status = "cached"
                    else:
                        newFileSpec.status = None
            # use log/output files and only one input file per dataset for jumbo jobs
            if forJumbo:
                # reset fileID to avoid updating JEDI tables
                newFileSpec.fileID = None
                if newFileSpec.type in ["log", "output"]:
                    pass
                elif newFileSpec.type == "input" and newFileSpec.datasetID not in datasetList:
                    datasetList.add(newFileSpec.datasetID)
                    # reset LFN etc since jumbo job runs on any file
                    newFileSpec.lfn = "any"
                    newFileSpec.GUID = None
                    # reset status to trigger input data transfer
                    newFileSpec.status = None
                else:
                    continue
            # append PandaID as suffix for log files to avoid LFN duplication
            if newFileSpec.type == "log":
                newFileSpec.lfn += ".$PANDAID"
            if (nSites > 1 or forJumbo) and fileSpec.type in ["log", "output"]:
                # distributed dataset
                datasetSpec = outDsMap[newFileSpec.datasetID]
                tmpDistributedDestination = DataServiceUtils.getDistributedDestination(datasetSpec.storageToken)
                if tmpDistributedDestination is not None:
                    tmpDestination = siteSpec.ddm_endpoints_output[scope_output].getAssociatedEndpoint(tmpDistributedDestination)
                    # change destination
                    newFileSpec.destinationSE = newPandaJob.computingSite
                    if tmpDestination is not None:
                        newFileSpec.destinationDBlockToken = f"ddd:{tmpDestination['ddm_endpoint_name']}"
            newPandaJob.addFile(newFileSpec)
        if build_spec_map and newPandaJob.computingSite in build_spec_map:
            tmp_build_file = copy.copy(build_spec_map[newPandaJob.computingSite])
            newPandaJob.addFile(tmp_build_file)
            newPandaJob.jobParameters = newPandaJob.jobParameters.replace("${LIB}", tmp_build_file.lfn)
        return newPandaJob

    # make jumbo jobs
    def makeJumboJobs(self, pandaJobs, taskSpec, inputChunk, simul, outDsMap, tmpLog):
        jumboJobs = []
        # no original
        if len(pandaJobs) == 0:
            return jumboJobs
        # get active jumbo jobs
        if not simul:
            activeJumboJobs = self.taskBufferIF.getActiveJumboJobs_JEDI(taskSpec.jediTaskID)
        else:
            activeJumboJobs = {}
        # enough jobs
        numNewJumboJobs = taskSpec.getNumJumboJobs() - len(activeJumboJobs)
        if numNewJumboJobs <= 0:
            return jumboJobs
        # sites which already have jumbo jobs
        sitesWithJumbo = dict()
        for tmpPandaID, activeJumboJob in activeJumboJobs.items():
            sitesWithJumbo.setdefault(activeJumboJob["site"], [])
            if activeJumboJob["status"] not in ["transferring", "holding"]:
                sitesWithJumbo[activeJumboJob["site"]].append(tmpPandaID)
        # sites with enough jumbo
        maxJumboPerSite = taskSpec.getMaxJumboPerSite()
        ngSites = []
        for tmpSite, tmpPandaIDs in sitesWithJumbo.items():
            if len(tmpPandaIDs) >= maxJumboPerSite:
                ngSites.append(tmpSite)
        # get sites
        newSites = []
        for i in range(numNewJumboJobs):
            siteCandidate = inputChunk.getOneSiteCandidateForJumbo(ngSites)
            if siteCandidate is None:
                break
            newSites.append(siteCandidate.siteName)
            # check if enough
            sitesWithJumbo.setdefault(siteCandidate.siteName, [])
            sitesWithJumbo[siteCandidate.siteName].append(None)
            if len(sitesWithJumbo[siteCandidate.siteName]) >= maxJumboPerSite:
                ngSites.append(siteCandidate.siteName)
        nJumbo = len(newSites)
        newSites.sort()
        # get job parameter of the first job
        if nJumbo > 0:
            jobParams, outFileMap = self.taskBufferIF.getJobParamsOfFirstJob_JEDI(taskSpec.jediTaskID)
            if jobParams is None:
                tmpLog.error("cannot get first job for jumbo")
                return jumboJobs
        # make jumbo jobs
        for iJumbo in range(nJumbo):
            newJumboJob = self.clonePandaJob(pandaJobs[0], iJumbo, {}, outDsMap, newSites, True, taskSpec=taskSpec, inputChunk=inputChunk)
            newJumboJob.eventService = EventServiceUtils.jumboJobFlagNumber
            # job params inherit from the first job since first_event etc must be the first value
            if jobParams != "":
                newJumboJob.jobParameters = jobParams
                # change output file name
                for outDatasetID, outLFN in outFileMap.items():
                    for fileSpec in newJumboJob.Files:
                        if fileSpec.type == "output" and fileSpec.datasetID == outDatasetID:
                            newJumboJob.jobParameters = newJumboJob.jobParameters.replace(outLFN, fileSpec.lfn)
                            break
            jumboJobs.append(newJumboJob)
        # return
        return jumboJobs

    # sort parallel jobs by site
    def sortParallelJobsBySite(self, pandaJobs, oldPandaIDs):
        tmpMap = {}
        oldMap = {}
        for pandaJob, oldPandaID in zip(pandaJobs, oldPandaIDs):
            if pandaJob.computingSite not in tmpMap:
                tmpMap[pandaJob.computingSite] = []
            if pandaJob.computingSite not in oldMap:
                oldMap[pandaJob.computingSite] = []
            tmpMap[pandaJob.computingSite].append(pandaJob)
            oldMap[pandaJob.computingSite].append(oldPandaID)
        newPandaJobs = []
        newOldPandaIds = []
        for computingSite in tmpMap.keys():
            newPandaJobs += tmpMap[computingSite]
            newOldPandaIds += oldMap[computingSite]
        # return
        return newPandaJobs, newOldPandaIds

    # get the largest attempt number
    def getLargestAttemptNr(self, inSubChunk):
        largestAttemptNr = 0
        for tmpDatasetSpec, tmpFileSpecList in inSubChunk:
            if tmpDatasetSpec.isMaster():
                for tmpFileSpec in tmpFileSpecList:
                    if tmpFileSpec.attemptNr is not None and tmpFileSpec.attemptNr > largestAttemptNr:
                        largestAttemptNr = tmpFileSpec.attemptNr
        return largestAttemptNr + 1

    # touch sandbox files
    def touchSandoboxFiles(self, task_spec, task_param_map, tmp_log):
        # get task parameter map
        tmpStat, taskParamMap = self.readTaskParams(task_spec, task_param_map, tmp_log)
        if not tmpStat:
            return Interaction.SC_FAILED, "failed to get task parameter dict", taskParamMap
        # look for sandbox
        sandboxName = RefinerUtils.get_sandbox_name(taskParamMap)
        # tarball is downloaded by pilot
        tarball_via_pilot = "tarBallViaDDM" in taskParamMap or ("buildSpec" in taskParamMap and "tarBallViaDDM" in taskParamMap["buildSpec"])
        if sandboxName is not None and not tarball_via_pilot:
            tmpRes = self.taskBufferIF.extendSandboxLifetime_JEDI(task_spec.jediTaskID, sandboxName)
            tmp_log.debug(f"extend lifetime for {sandboxName} with {tmpRes}")
            if not tmpRes:
                errMsg = "user sandbox file unavailable. resubmit the task with --useNewCode"
                return Interaction.SC_FAILED, errMsg, taskParamMap
        return Interaction.SC_SUCCEEDED, None, taskParamMap


# launch


def launcher(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, cloudList, withThrottle=True, execJobs=True, loopCycle_cust=None):
    p = JobGenerator(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, cloudList, withThrottle, execJobs, loopCycle_cust)
    p.start()
