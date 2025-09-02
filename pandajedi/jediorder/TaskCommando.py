import os
import re
import socket
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

from .JediKnight import JediKnight

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class to kill/amend tasks
class TaskCommando(JediKnight):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)

    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        # go into main loop
        while True:
            startTime = naive_utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.debug("start")
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # get the list of tasks to exec command
                        tmpList = self.taskBufferIF.getTasksToExecCommand_JEDI(vo, prodSourceLabel)
                        if tmpList is None:
                            # failed
                            tmpLog.error(f"failed to get the task list for vo={vo} label={prodSourceLabel}")
                        else:
                            tmpLog.debug(f"got {len(tmpList)} tasks")
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # make workers
                            nWorker = jedi_config.taskrefine.nWorkers
                            for iWorker in range(nWorker):
                                thr = TaskCommandoThread(taskList, threadPool, self.taskBufferIF, self.ddmIF, self.pid)
                                thr.start()
                            # join
                            threadPool.join()
                tmpLog.debug("done")
            except Exception as e:
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {str(e)} {traceback.format_exc()}")
            # sleep if needed
            loopCycle = jedi_config.tcommando.loopCycle
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)


# thread for real worker
class TaskCommandoThread(WorkerThread):
    # constructor
    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, pid):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = "taskcommando"
        self.pid = pid

    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug(f"{self.__class__.__name__} terminating since no more items")
                    return
                # loop over all tasks
                for jediTaskID, commandMap in taskList:
                    # make logger
                    tmpLog = MsgWrapper(self.logger, f" < jediTaskID={jediTaskID} >")
                    commandStr = commandMap["command"]
                    commentStr = commandMap["comment"]
                    oldStatus = commandMap["oldStatus"]
                    tmpLog.info(f"start for {commandStr}")
                    tmpStat = Interaction.SC_SUCCEEDED
                    if commandStr in ["kill", "finish", "reassign"]:
                        tmpMsg = f"executing {commandStr}"
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg, self.msgType)
                        # loop twice to see immediate result
                        for iLoop in range(2):
                            # get active PandaIDs to be killed
                            if commandStr == "reassign" and commentStr is not None and "soft reassign" in commentStr:
                                pandaIDs = self.taskBufferIF.getQueuedPandaIDsWithTask_JEDI(jediTaskID)
                            elif commandStr == "reassign" and commentStr is not None and "nokill reassign" in commentStr:
                                pandaIDs = []
                            else:
                                pandaIDs = self.taskBufferIF.getPandaIDsWithTask_JEDI(jediTaskID, True)
                            if pandaIDs is None:
                                tmpLog.error(f"failed to get PandaIDs for jediTaskID={jediTaskID}")
                                tmpStat = Interaction.SC_FAILED
                            # kill jobs or update task
                            if tmpStat == Interaction.SC_SUCCEEDED:
                                if pandaIDs == []:
                                    # done since no active jobs
                                    tmpMsg = "completed cleaning jobs"
                                    tmpLog.sendMsg(tmpMsg, self.msgType)
                                    tmpLog.info(tmpMsg)
                                    tmpTaskSpec = JediTaskSpec()
                                    tmpTaskSpec.jediTaskID = jediTaskID
                                    updateTaskStatus = True
                                    if commandStr != "reassign":
                                        # reset oldStatus
                                        # keep oldStatus for task reassignment since it is reset when actually reassigned
                                        tmpTaskSpec.forceUpdate("oldStatus")
                                    else:
                                        # extract cloud or site
                                        if commentStr is not None:
                                            tmpItems = commentStr.split(":")
                                            if tmpItems[0] == "cloud":
                                                tmpTaskSpec.cloud = tmpItems[1]
                                            elif tmpItems[0] == "nucleus":
                                                tmpTaskSpec.nucleus = tmpItems[1]
                                            else:
                                                tmpTaskSpec.site = tmpItems[1]
                                            tmpMsg = f"set {tmpItems[0]}={tmpItems[1]}"
                                            tmpLog.sendMsg(tmpMsg, self.msgType)
                                            tmpLog.info(tmpMsg)
                                            # back to oldStatus if necessary
                                            if tmpItems[2] == "y":
                                                tmpTaskSpec.status = oldStatus
                                                tmpTaskSpec.forceUpdate("oldStatus")
                                                updateTaskStatus = False
                                    if commandStr == "reassign":
                                        tmpTaskSpec.forceUpdate("errorDialog")
                                    if commandStr == "finish":
                                        # update datasets
                                        tmpLog.info("updating datasets to finish")
                                        tmpStat = self.taskBufferIF.updateDatasetsToFinishTask_JEDI(jediTaskID, self.pid)
                                        if not tmpStat:
                                            tmpLog.info("wait until datasets are updated to finish")
                                        # ignore failGoalUnreached when manually finished
                                        tmpStat, taskSpec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID)
                                        tmpTaskSpec.splitRule = taskSpec.splitRule
                                        tmpTaskSpec.unsetFailGoalUnreached()
                                    if updateTaskStatus:
                                        tmpTaskSpec.status = JediTaskSpec.commandStatusMap()[commandStr]["done"]
                                    tmpMsg = f"set task_status={tmpTaskSpec.status}"
                                    tmpLog.sendMsg(tmpMsg, self.msgType)
                                    tmpLog.info(tmpMsg)
                                    tmpRet = self.taskBufferIF.updateTask_JEDI(tmpTaskSpec, {"jediTaskID": jediTaskID}, setOldModTime=True)
                                    tmpLog.info(f"done with {str(tmpRet)}")
                                    break
                                else:
                                    # kill only in the first loop
                                    if iLoop > 0:
                                        break
                                    # wait or kill jobs
                                    if commentStr and "soft finish" in commentStr:
                                        queuedPandaIDs = self.taskBufferIF.getQueuedPandaIDsWithTask_JEDI(jediTaskID)
                                        tmpMsg = f"trying to kill {len(queuedPandaIDs)} queued jobs for soft finish"
                                        tmpLog.info(tmpMsg)
                                        tmpRet = self.taskBufferIF.killJobs(queuedPandaIDs, commentStr, "52", True)
                                        tmpMsg = f"wating {len(pandaIDs)} jobs for soft finish"
                                        tmpLog.info(tmpMsg)
                                        tmpRet = True
                                        tmpLog.info(f"done with {str(tmpRet)}")
                                        break
                                    else:
                                        tmpMsg = f"trying to kill {len(pandaIDs)} jobs"
                                        tmpLog.info(tmpMsg)
                                        tmpLog.sendMsg(tmpMsg, self.msgType)
                                        if commandStr in ["finish"]:
                                            # force kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs, commentStr, "52", True)
                                        elif commandStr in ["reassign"]:
                                            # force kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs, commentStr, "51", True)
                                        else:
                                            # normal kill
                                            tmpRet = self.taskBufferIF.killJobs(pandaIDs, commentStr, "50", True)
                                        tmpLog.info(f"done with {str(tmpRet)}")
                    elif commandStr in ["retry", "incexec"]:
                        tmpMsg = f"executing {commandStr}"
                        tmpLog.info(tmpMsg)
                        tmpLog.sendMsg(tmpMsg, self.msgType)
                        # change task params for incexec
                        if commandStr == "incexec":
                            try:
                                # read task params
                                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(jediTaskID)
                                taskParamMap = RefinerUtils.decodeJSON(taskParam)
                                # remove old sandbox file specified in the previous reattempt
                                taskParamMap.pop("fixedSandbox", None)
                                # convert new params
                                decoded = RefinerUtils.decodeJSON(commentStr)
                                if isinstance(decoded, dict):
                                    # old style
                                    newParamMap = decoded
                                    command_qualifiers = []
                                else:
                                    # new style
                                    newParamMap, command_qualifiers = decoded
                                # change params
                                for newKey, newVal in newParamMap.items():
                                    if newVal is None:
                                        # delete
                                        if newKey in taskParamMap:
                                            del taskParamMap[newKey]
                                    else:
                                        # change
                                        taskParamMap[newKey] = newVal
                                # overwrite sandbox
                                if "fixedSandbox" in taskParamMap:
                                    # noBuild
                                    for tmpParam in taskParamMap["jobParameters"]:
                                        if tmpParam["type"] == "constant" and re.search("^-a [^ ]+$", tmpParam["value"]) is not None:
                                            tmpParam["value"] = f"-a {taskParamMap['fixedSandbox']}"
                                    # build
                                    if "buildSpec" in taskParamMap:
                                        taskParamMap["buildSpec"]["archiveName"] = taskParamMap["fixedSandbox"]
                                    # merge
                                    if "mergeSpec" in taskParamMap:
                                        taskParamMap["mergeSpec"]["jobParameters"] = re.sub(
                                            "-a [^ ]+", f"-a {taskParamMap['fixedSandbox']}", taskParamMap["mergeSpec"]["jobParameters"]
                                        )
                                # encode new param
                                strTaskParams = RefinerUtils.encodeJSON(taskParamMap)
                                tmpRet = self.taskBufferIF.updateTaskParams_JEDI(jediTaskID, strTaskParams)
                                if tmpRet is not True:
                                    tmpLog.error("failed to update task params")
                                    continue
                            except Exception as e:
                                tmpLog.error(f"failed to change task params with {str(e)} {traceback.format_exc()}")
                                continue
                        else:
                            # command qualifiers for retry
                            command_qualifiers = commentStr.split()
                        # retry child tasks
                        retryChildTasks = "sole" not in command_qualifiers
                        # discard events
                        discardEvents = "discard" in command_qualifiers
                        # release un-staged files
                        releaseUnstaged = "staged" in command_qualifiers
                        # keep gshare and priority
                        keep_share_priority = "keep" in command_qualifiers
                        # ignore limit for hard-exhausted
                        ignore_hard_exhausted = "transcend" in command_qualifiers
                        # retry the task
                        tmpRet, newTaskStatus, retried_tasks = self.taskBufferIF.retryTask_JEDI(
                            jediTaskID,
                            commandStr,
                            retryChildTasks=retryChildTasks,
                            discardEvents=discardEvents,
                            release_unstaged=releaseUnstaged,
                            keep_share_priority=keep_share_priority,
                            ignore_hard_exhausted=ignore_hard_exhausted,
                        )
                        if tmpRet is True:
                            tmpMsg = f"set task_status={newTaskStatus}"
                            tmpLog.sendMsg(tmpMsg, self.msgType)
                            tmpLog.info(tmpMsg)
                            if newTaskStatus in ["rerefine", "ready"]:
                                tmpStat, task_spec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID)
                                if tmpStat and task_spec.is_msg_driven():
                                    # msg driven
                                    if newTaskStatus == "rerefine":
                                        push_ret = self.taskBufferIF.push_task_trigger_message("jedi_contents_feeder", jediTaskID)
                                        if push_ret:
                                            tmpLog.debug("pushed trigger message to jedi_contents_feeder")
                                        else:
                                            tmpLog.warning("failed to push trigger message to jedi_contents_feeder")
                                    elif newTaskStatus == "ready":
                                        push_ret = self.taskBufferIF.push_task_trigger_message("jedi_job_generator", jediTaskID)
                                        if push_ret:
                                            tmpLog.debug("pushed trigger message to jedi_job_generator")
                                        else:
                                            tmpLog.warning("failed to push trigger message to jedi_job_generator")
                            # reset global share and priority
                            if not keep_share_priority:
                                for task_id in retried_tasks:
                                    try:
                                        global_share = RefinerUtils.get_initial_global_share(self.taskBufferIF, task_id)
                                        self.taskBufferIF.reassignShare([task_id], global_share, True)
                                        tmp_msg = f"reset gshare={global_share} to jediTaskID={task_id}"
                                        tmpLog.info(tmp_msg)
                                    except Exception as e:
                                        tmpLog.error(f"failed to reset gshare for {task_id} with {str(e)}")

                        tmpLog.info(f"done with {tmpRet}")
                    else:
                        tmpLog.error("unknown command")
            except Exception as e:
                errStr = f"{self.__class__.__name__} failed in runImpl() with {str(e)} {traceback.format_exc()} "
                logger.error(errStr)


# launch


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None):
    p = TaskCommando(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels)
    p.start()
