import os
import socket
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedibrokerage import AtlasBrokerUtils
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.dataservice import DataServiceUtils
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import JobUtils

from .JumboWatchDog import JumboWatchDog
from .TypicalWatchDogBase import TypicalWatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# watchdog for ATLAS production
class AtlasProdWatchDog(TypicalWatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TypicalWatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"

    # main
    def doAction(self):
        try:
            # get logger
            tmpLog = MsgWrapper(logger)
            tmpLog.debug("start")

            # action for priority boost
            self.doActionForPriorityBoost(tmpLog)

            # action for reassign
            self.doActionForReassign(tmpLog)

            # action for throttled
            self.doActionForThrottled(tmpLog)

            # action for high prio pending
            for minPriority, timeoutVal in [
                (950, 10),
                (900, 30),
            ]:
                self.doActionForHighPrioPending(tmpLog, minPriority, timeoutVal)

            # action to set scout job data w/o scouts
            self.doActionToSetScoutJobData(tmpLog)

            # action to throttle jobs in paused tasks
            self.doActionToThrottleJobInPausedTasks(tmpLog)

            # action for jumbo
            jumbo = JumboWatchDog(self.taskBufferIF, self.ddmIF, tmpLog, "atlas", "managed")
            jumbo.run()

            # action to provoke (mark files ready) data carousel tasks to start if DDM rules of input DS are done
            self.doActionToProvokeDCTasks(tmpLog)

        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype.__name__}:{errvalue} {traceback.format_exc()}")
        # return
        tmpLog.debug("done")
        return self.SC_SUCCEEDED

    # action for priority boost
    def doActionForPriorityBoost(self, gTmpLog):
        # get work queue mapper
        workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # get list of work queues
        workQueueList = workQueueMapper.getAlignedQueueList(self.vo, self.prodSourceLabel)
        resource_types = self.taskBufferIF.load_resource_types()
        # loop over all work queues
        for workQueue in workQueueList:
            break_loop = False  # for special workqueues we only need to iterate once
            for resource_type in resource_types:
                gTmpLog.debug(f"start workQueue={workQueue.queue_name}")
                # get tasks to be boosted
                if workQueue.is_global_share:
                    task_criteria = {"gshare": workQueue.queue_name, "resource_type": resource_type.resource_name}
                else:
                    break_loop = True
                    task_criteria = {"workQueue_ID": workQueue.queue_id}
                dataset_criteria = {"masterID": None, "type": ["input", "pseudo_input"]}
                task_param_list = ["jediTaskID", "taskPriority", "currentPriority", "parent_tid", "gshare"]
                dataset_param_list = ["nFiles", "nFilesUsed", "nFilesTobeUsed", "nFilesFinished", "nFilesFailed"]
                taskVarList = self.taskBufferIF.getTasksWithCriteria_JEDI(
                    self.vo,
                    self.prodSourceLabel,
                    ["running"],
                    taskCriteria=task_criteria,
                    datasetCriteria=dataset_criteria,
                    taskParamList=task_param_list,
                    datasetParamList=dataset_param_list,
                    taskLockColumn="throttledTime",
                    taskLockInterval=20,
                )
                boostedPrio = 900
                toBoostRatio = 0.95
                for taskParam, datasetParam in taskVarList:
                    jediTaskID = taskParam["jediTaskID"]
                    taskPriority = taskParam["taskPriority"]
                    currentPriority = taskParam["currentPriority"]
                    parent_tid = taskParam["parent_tid"]
                    gshare = taskParam["gshare"]
                    # check parent
                    parentState = None
                    if parent_tid not in [None, jediTaskID]:
                        parentState = self.taskBufferIF.checkParentTask_JEDI(parent_tid)
                        if parentState != "completed":
                            gTmpLog.info(
                                f"#ATM #KV label=managed jediTaskID={jediTaskID} skip prio boost since parent_id={parent_tid} has parent_status={parentState}"
                            )
                            continue
                    nFiles = datasetParam["nFiles"]
                    nFilesFinished = datasetParam["nFilesFinished"]
                    nFilesFailed = datasetParam["nFilesFailed"]
                    # get num jobs
                    nJobs = self.taskBufferIF.getNumJobsForTask_JEDI(jediTaskID)
                    nRemJobs = None
                    if nJobs is not None:
                        try:
                            nRemJobs = int(float(nFiles - nFilesFinished - nFilesFailed) * float(nJobs) / float(nFiles))
                        except Exception:
                            pass
                    tmpStr = f"jediTaskID={jediTaskID} nFiles={nFiles} nFilesFinishedFailed={nFilesFinished + nFilesFailed} "
                    tmpStr += f"nJobs={nJobs} nRemJobs={nRemJobs} parent_tid={parent_tid} parentStatus={parentState}"
                    gTmpLog.debug(tmpStr)

                    try:
                        if nRemJobs is not None and float(nFilesFinished + nFilesFailed) / float(nFiles) >= toBoostRatio and nRemJobs <= 100:
                            # skip high enough
                            if currentPriority < boostedPrio:
                                gTmpLog.info(f" >>> action=priority_boosting of jediTaskID={jediTaskID} to priority={boostedPrio} #ATM #KV label=managed ")
                                self.taskBufferIF.changeTaskPriorityPanda(jediTaskID, boostedPrio)

                            # skip express or non global share
                            newShare = "Express"
                            newShareType = "managed"
                            if gshare != newShare and workQueue.is_global_share and workQueue.queue_type == newShareType:
                                gTmpLog.info(
                                    " >>> action=gshare_reassignment jediTaskID={0} from gshare_old={2} to gshare_new={1} #ATM #KV label=managed".format(
                                        jediTaskID, newShare, gshare
                                    )
                                )
                                self.taskBufferIF.reassignShare([jediTaskID], newShare, True)
                            gTmpLog.info(f">>> done jediTaskID={jediTaskID}")
                    except Exception:
                        pass

                if break_loop:
                    break

    # action for reassignment
    def doActionForReassign(self, gTmpLog):
        # get DDM I/F
        ddmIF = self.ddmIF.getInterface(self.vo)
        # get site mapper
        siteMapper = self.taskBufferIF.get_site_mapper()
        # get tasks to get reassigned
        taskList = self.taskBufferIF.getTasksToReassign_JEDI(self.vo, self.prodSourceLabel)

        gTmpLog.debug(f"got {len(taskList)} tasks to reassign")
        for taskSpec in taskList:
            tmpLog = MsgWrapper(logger, f"< jediTaskID={taskSpec.jediTaskID} >")
            tmpLog.debug("start to reassign")
            # DDM backend
            ddmBackEnd = taskSpec.getDdmBackEnd()
            # get datasets
            tmpStat, datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID, ["output", "log"])
            if tmpStat is not True:
                tmpLog.error("failed to get datasets")
                continue

            # re-run task brokerage
            if taskSpec.nucleus in [None, ""]:
                taskSpec.status = "assigning"
                taskSpec.oldStatus = None
                taskSpec.setToRegisterDatasets()
                self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID}, setOldModTime=True)
                tmpLog.debug(f"#ATM #KV label=managed action=trigger_new_brokerage by setting task_status={taskSpec.status}")
                continue

            # get nucleus
            nucleusSpec = siteMapper.getNucleus(taskSpec.nucleus)
            if nucleusSpec is None:
                tmpLog.error(f"nucleus={taskSpec.nucleus} doesn't exist")
                continue

            # set nucleus
            retMap = {taskSpec.jediTaskID: AtlasBrokerUtils.getDictToSetNucleus(nucleusSpec, datasetSpecList)}
            self.taskBufferIF.setCloudToTasks_JEDI(retMap)

            # get nucleus
            t1SiteName = nucleusSpec.getOnePandaSite()
            t1Site = siteMapper.getSite(t1SiteName)

            # loop over all datasets
            isOK = True
            for datasetSpec in datasetSpecList:
                tmpLog.debug(f"dataset={datasetSpec.datasetName}")
                if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
                    tmpLog.debug(f"skip {datasetSpec.datasetName} is distributed")
                    continue
                # get location
                location = siteMapper.getDdmEndpoint(
                    t1Site.sitename, datasetSpec.storageToken, taskSpec.prodSourceLabel, JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType)
                )
                # make subscription
                try:
                    tmpLog.debug(f"registering subscription to {location} with backend={ddmBackEnd}")
                    tmpStat = ddmIF.registerDatasetSubscription(datasetSpec.datasetName, location, "Production Output", asynchronous=True)
                    if tmpStat is not True:
                        tmpLog.error("failed to make subscription")
                        isOK = False
                        break
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    tmpLog.warning(f"failed to make subscription with {errtype.__name__}:{errvalue}")
                    isOK = False
                    break
            # succeeded
            if isOK:
                # activate task
                if taskSpec.oldStatus in ["assigning", "exhausted", None]:
                    taskSpec.status = "ready"
                else:
                    taskSpec.status = taskSpec.oldStatus
                taskSpec.oldStatus = None
                self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID}, setOldModTime=True)
                tmpLog.debug("finished to reassign")

    # action for throttled tasks
    def doActionForThrottled(self, gTmpLog):
        # release tasks
        nTasks = self.taskBufferIF.releaseThrottledTasks_JEDI(self.vo, self.prodSourceLabel)
        gTmpLog.debug(f"released {nTasks} tasks")

        # throttle tasks
        nTasks = self.taskBufferIF.throttleTasks_JEDI(self.vo, self.prodSourceLabel, jedi_config.watchdog.waitForThrottled)
        gTmpLog.debug(f"throttled {nTasks} tasks")

    # action for high priority pending tasks
    def doActionForHighPrioPending(self, gTmpLog, minPriority, timeoutVal):
        timeoutForPending = None
        # try to get the timeout from the config files
        if hasattr(jedi_config.watchdog, "timeoutForPendingVoLabel"):
            timeoutForPending = CoreUtils.getConfigParam(jedi_config.watchdog.timeoutForPendingVoLabel, self.vo, self.prodSourceLabel)
        if timeoutForPending is None:
            timeoutForPending = jedi_config.watchdog.timeoutForPending
        timeoutForPending = int(timeoutForPending) * 24
        tmpRet, _ = self.taskBufferIF.reactivatePendingTasks_JEDI(self.vo, self.prodSourceLabel, timeoutVal, timeoutForPending, minPriority=minPriority)
        if tmpRet is None:
            # failed
            gTmpLog.error(f"failed to reactivate high priority (>{minPriority}) tasks")
        else:
            gTmpLog.info(f"reactivated high priority (>{minPriority}) {tmpRet} tasks")

    # action to throttle jobs in paused tasks
    def doActionToThrottleJobInPausedTasks(self, gTmpLog):
        tmpRet = self.taskBufferIF.throttleJobsInPausedTasks_JEDI(self.vo, self.prodSourceLabel)
        if tmpRet is None:
            # failed
            gTmpLog.error("failed to thottle jobs in paused tasks")
        else:
            for jediTaskID, pandaIDs in tmpRet.items():
                gTmpLog.info(f"throttled jobs in paused jediTaskID={jediTaskID} successfully")
                tmpRet = self.taskBufferIF.killJobs(pandaIDs, "reassign", "51", True)
                gTmpLog.info(f"reassigned {len(pandaIDs)} jobs in paused jediTaskID={jediTaskID} with {tmpRet}")

    # action to provoke (mark files ready) data carousel tasks to start if DDM rules of input DS are done
    def doActionToProvokeDCTasks(self, gTmpLog):
        # lock
        got_lock = self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=self.prodSourceLabel,
            cloud=None,
            workqueue_id=None,
            resource_name=None,
            component="AtlasProdWatchDog.doActionToProvokeDCT",
            pid=self.pid,
            timeLimit=30,
        )
        if not got_lock:
            gTmpLog.debug("doActionToProvokeDCTasks locked by another process. Skipped")
            return
        # run
        res_dict = self.taskBufferIF.get_pending_dc_tasks_JEDI(task_type="prod", time_limit_minutes=60)
        if res_dict is None:
            # failed
            gTmpLog.error("failed to get pending DC tasks")
        elif not res_dict:
            # empty
            gTmpLog.debug("no pending DC task; skipped")
        else:
            gTmpLog.debug(f"got {len(res_dict)} DC tasks to provoke")
            ddm_if = self.ddmIF.getInterface(self.vo)
            # loop over pending DC tasks
            for task_id, ds_name_list in res_dict.items():
                if not ds_name_list:
                    continue
                total_all_ok = True
                for ds_name in ds_name_list:
                    ret_data = ddm_if.get_rules_state(ds_name)
                    try:
                        all_ok, rule_dict = ret_data
                        total_all_ok = total_all_ok and all_ok
                    except ValueError:
                        gTmpLog.error(f"failed to get rule info for task={task_id}. data={ret_data}")
                        total_all_ok = False
                        break
                if total_all_ok:
                    # all rules ok; provoke the task
                    gTmpLog.info(f"provoking task {task_id}")
                    self.taskBufferIF.updateInputDatasetsStaged_JEDI(task_id, None, None, by="AtlasProdWatchDog")
                    gTmpLog.info(f"all staging rules of task {task_id} are OK; provoked")
                else:
                    gTmpLog.debug(f"not all staging rules of task {task_id} are OK; skipped ")
