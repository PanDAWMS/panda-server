import copy
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
from pandaserver.taskbuffer import JediTaskSpec, JobUtils

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

            # actions based on task progress
            self.do_task_progress_based_actions(tmpLog)

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

    # actions based on task progress
    def do_task_progress_based_actions(self, g_tmp_log: MsgWrapper) -> None:
        """
        Take actions based on task progress
        1) boost priority of tasks which have finished >95% of input files and have <=100 remaining jobs to prio 900
        2) reassign such tasks to Express global share if not already
        3) pause tasks with high failure rate

        :param g_tmp_log: logger
        :return: None
        """
        # get work queue mapper
        workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # get list of work queues
        workQueueList = workQueueMapper.getAlignedQueueList(self.vo, self.prodSourceLabel)
        resource_types = self.taskBufferIF.load_resource_types()

        # get config values
        failure_checker = "watchdog"
        max_job_failure_rate_key = f"MAX_JOB_FAILURE_RATE_TO_PAUSE_{self.prodSourceLabel}"
        max_job_failure_rate_base = self.taskBufferIF.getConfigValue(failure_checker, max_job_failure_rate_key, "jedi")
        max_hep_score_failure_rate_key = f"MAX_HEP_SCORE_FAILURE_RATE_TO_PAUSE_{self.prodSourceLabel}"
        max_hep_score_failure_rate_base = self.taskBufferIF.getConfigValue(failure_checker, max_hep_score_failure_rate_key, "jedi")
        min_jobs_to_pause_key = f"MIN_JOBS_TO_PAUSE_{self.prodSourceLabel}"
        min_jobs_to_pause = self.taskBufferIF.getConfigValue(failure_checker, min_jobs_to_pause_key, "jedi")
        if min_jobs_to_pause is None:
            min_jobs_to_pause = 1000
        min_remaining_jobs_to_pause_key = f"MIN_REMAINING_JOBS_TO_PAUSE_{self.prodSourceLabel}"
        min_remaining_jobs_to_pause = self.taskBufferIF.getConfigValue(failure_checker, min_remaining_jobs_to_pause_key, "jedi")
        if min_remaining_jobs_to_pause is None:
            min_remaining_jobs_to_pause = 100

        # loop over all work queues
        g_tmp_log.debug("start do_task_progress_based_actions")
        for workQueue in workQueueList:
            break_loop = False  # for special workqueues we only need to iterate once
            for resource_type in resource_types:
                g_tmp_log.debug(f"start workQueue={workQueue.queue_name} resource_type={resource_type.resource_name}")
                # get tasks to be boosted
                if workQueue.is_global_share:
                    task_criteria = {"gshare": workQueue.queue_name, "resource_type": resource_type.resource_name}
                else:
                    break_loop = True
                    task_criteria = {"workQueue_ID": workQueue.queue_id}

                # define max failure rate
                max_job_failure_rate = self.taskBufferIF.getConfigValue(failure_checker, f"{max_job_failure_rate_key}_{workQueue.queue_name}", "jedi")
                if max_job_failure_rate is None:
                    max_job_failure_rate = max_job_failure_rate_base
                max_hep_score_failure_rate = self.taskBufferIF.getConfigValue(
                    failure_checker, f"{max_hep_score_failure_rate_key}_{workQueue.queue_name}", "jedi"
                )
                if max_hep_score_failure_rate is None:
                    max_hep_score_failure_rate = max_hep_score_failure_rate_base

                dataset_criteria = {"masterID": None, "type": ["input", "pseudo_input"]}
                task_param_list = ["jediTaskID", "currentPriority", "parent_tid", "gshare", "requestType", "splitRule"]
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
                    currentPriority = taskParam["currentPriority"]
                    parent_tid = taskParam["parent_tid"]
                    gshare = taskParam["gshare"]
                    request_type = taskParam["requestType"]
                    split_rule = taskParam["splitRule"]
                    # check parent
                    parentState = None
                    if parent_tid not in [None, jediTaskID]:
                        parentState = self.taskBufferIF.checkParentTask_JEDI(parent_tid, jediTaskID)
                        if parentState != "completed":
                            g_tmp_log.info(
                                f"#ATM #KV label={self.prodSourceLabel} jediTaskID={jediTaskID} skip prio boost since parent_id={parent_tid} has parent_status={parentState}"
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
                    failure_metrics = self.taskBufferIF.get_task_failure_metrics(jediTaskID)

                    tmpStr = f"jediTaskID={jediTaskID} nFiles={nFiles} nFilesFinishedFailed={nFilesFinished + nFilesFailed} "
                    tmpStr += f"nJobs={nJobs} nRemJobs={nRemJobs} parent_tid={parent_tid} parentStatus={parentState} failure_metrics={failure_metrics}"
                    g_tmp_log.debug(tmpStr)

                    try:
                        if nRemJobs is not None and float(nFilesFinished + nFilesFailed) / float(nFiles) >= toBoostRatio and nRemJobs <= 100:
                            # skip high enough
                            if currentPriority < boostedPrio:
                                g_tmp_log.info(
                                    f" >>> action=priority_boosting of jediTaskID={jediTaskID} to priority={boostedPrio} #ATM #KV label={self.prodSourceLabel} "
                                )
                                self.taskBufferIF.changeTaskPriorityPanda(jediTaskID, boostedPrio)

                            # skip express or non global share
                            newShare = "Express"
                            newShareType = self.prodSourceLabel
                            if gshare != newShare and workQueue.is_global_share and workQueue.queue_type == newShareType:
                                g_tmp_log.info(
                                    f" >>> action=gshare_reassignment jediTaskID={jediTaskID} from gshare_old={gshare} to gshare_new={newShare} #ATM #KV label={self.prodSourceLabel}"
                                )
                                self.taskBufferIF.reassignShare([jediTaskID], newShare, True)
                            g_tmp_log.debug(f"reassigned jediTaskID={jediTaskID}")
                    except Exception:
                        pass

                    # check failure rate
                    failure_key = None
                    bad_value = None
                    if JediTaskSpec.is_auto_pause_disabled(split_rule):
                        # auto pause disabled
                        pass
                    elif not failure_metrics:
                        # failure metrics is unavailable
                        pass
                    elif nJobs is None or nJobs < min_jobs_to_pause:
                        # total num jobs is small
                        pass
                    elif nRemJobs is None or nRemJobs < min_remaining_jobs_to_pause:
                        # remaining num jobs is small
                        pass
                    elif (
                        max_job_failure_rate is not None
                        and failure_metrics["single_failure_rate"] is not None
                        and failure_metrics["single_failure_rate"] >= max_job_failure_rate > 0
                    ):
                        # job failure rate is high
                        failure_key = "single_failure_rate"
                        bad_value = failure_metrics[failure_key]
                        g_tmp_log.info(
                            f" >>> action=pausing_high_failure_rate jediTaskID={jediTaskID} job_failure_rate={bad_value:.2f} "
                            f"(max_allowed={max_job_failure_rate}) #ATM #KV label={self.prodSourceLabel}"
                        )
                        self.taskBufferIF.sendCommandTaskPanda(
                            jediTaskID, f"{failure_checker} due to high job failure rate {bad_value:.2f} > {max_job_failure_rate}", True, "pause"
                        )
                    elif (
                        max_hep_score_failure_rate is not None
                        and failure_metrics["hep_score_failure_rate"] is not None
                        and failure_metrics["hep_score_failure_rate"] >= max_hep_score_failure_rate > 0
                    ):
                        # failed HEP score rate is high
                        failure_key = "hep_score_failure_rate"
                        bad_value = failure_metrics[failure_key]
                        g_tmp_log.info(
                            f" >>> action=pausing_high_failure_rate jediTaskID={jediTaskID} hep_score_failure_rate={bad_value:.2f} "
                            f"(max_allowed={max_hep_score_failure_rate}) #ATM #KV label={self.prodSourceLabel}"
                        )
                        self.taskBufferIF.sendCommandTaskPanda(
                            jediTaskID, f"{failure_checker} due to high HEP score failure rate {bad_value:.2f} > {max_hep_score_failure_rate}", True, "pause"
                        )
                    if failure_key is not None:
                        # get tasks with the same requestType to be paused
                        if request_type:
                            task_criteria = copy.copy(task_criteria)
                            task_criteria["requestType"] = request_type
                            tasks_with_same_request_type = self.taskBufferIF.getTasksWithCriteria_JEDI(
                                self.vo,
                                self.prodSourceLabel,
                                ["running"],
                                taskCriteria=task_criteria,
                                taskParamList=["jediTaskID", "splitRule"],
                            )
                            for other_task_param, _ in tasks_with_same_request_type:
                                other_jedi_task_id = other_task_param["jediTaskID"]
                                # auto pause disabled
                                if JediTaskSpec.is_auto_pause_disabled(other_jedi_task_id):
                                    continue
                                if other_jedi_task_id != jediTaskID:
                                    g_tmp_log.info(
                                        f" >>> action=pausing_high_failure_rate jediTaskID={other_jedi_task_id} due to requestType={request_type} shared by {jediTaskID} "
                                        f"#ATM #KV label={self.prodSourceLabel}"
                                    )
                                    if failure_key == "single_failure_rate":
                                        msg = f"{failure_checker} due to high job failure rate in another task {jediTaskID} ({bad_value:.2f} > {max_job_failure_rate})"
                                    else:
                                        msg = f"{failure_checker} due to high HEP score failure rate in another task {jediTaskID} ({bad_value:.2f} > {max_hep_score_failure_rate})"
                                    self.taskBufferIF.sendCommandTaskPanda(
                                        other_jedi_task_id,
                                        msg,
                                        True,
                                        "pause",
                                    )
                        g_tmp_log.debug(f"paused jediTaskID={jediTaskID}")

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
