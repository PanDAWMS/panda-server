"""
provide web interface to users

"""

import datetime
import json
import re
import sys
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

import pandaserver.jobdispatcher.Protocol as Protocol
import pandaserver.taskbuffer.ProcessGroups
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id, resolve_bool
from pandaserver.taskbuffer import JobUtils, PrioUtil
from pandaserver.taskbuffer.WrappedPickle import WrappedPickle

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
    from idds.client.clientmanager import ClientManager as iDDS_ClientManager
except ImportError:
    pass

MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"

CODE_SSL = 100
CODE_LOGIC = 101
CODE_OTHER_PARAMS = 102


_logger = PandaLogger().getLogger("UserIF")


def resolve_true(variable):
    return variable == "True"


def resolve_false(variable):
    return variable != "False"


# main class
class UserIF:
    # constructor
    def __init__(self):
        self.taskBuffer = None

    # initialize
    def init(self, taskBuffer):
        self.taskBuffer = taskBuffer

    # submit jobs
    def submitJobs(self, jobsStr, user, host, userFQANs, prodRole=False, toPending=False):
        try:
            # deserialize jobspecs
            try:
                jobs = WrappedPickle.loads(jobsStr)
            except Exception:
                jobs = JobUtils.load_jobs_json(jobsStr)
            _logger.debug(f"submitJobs {user} len:{len(jobs)} prodRole={prodRole} FQAN:{str(userFQANs)}")
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error(f"submitJobs: too many jobs more than {maxJobs}")
                jobs = jobs[:maxJobs]
        except Exception as ex:
            _logger.error(f"submitJobs : {str(ex)} {traceback.format_exc()}")
            jobs = []
        # check prodSourceLabel
        try:
            good_labels = True
            for tmpJob in jobs:
                # check production role
                if tmpJob.prodSourceLabel in ["managed"] and not prodRole:
                    good_labels = False
                    good_labels_message = f"submitJobs {user} missing prod-role for prodSourceLabel={tmpJob.prodSourceLabel}"
                    break

                # check the job_label is valid
                if tmpJob.job_label not in [None, "", "NULL"] and tmpJob.job_label not in JobUtils.job_labels:
                    good_labels = False
                    good_labels_message = f"submitJobs {user} wrong job_label={tmpJob.job_label}"
                    break
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            _logger.error(f"submitJobs : checking good_labels {err_type} {err_value}")
            good_labels = False

        # reject injection for error with the labels
        if not good_labels:
            _logger.error(good_labels_message)
            return f"ERROR: {good_labels_message}"

        job0 = None

        # get user VO
        userVO = "atlas"
        try:
            job0 = jobs[0]
            if job0.VO not in [None, "", "NULL"]:
                userVO = job0.VO
        except (IndexError, AttributeError) as e:
            _logger.error(f"submitJobs : checking userVO. userVO not found, defaulting to {userVO}. (Exception {e})")

        # atlas jobs require FQANs
        if userVO == "atlas" and userFQANs == []:
            _logger.error(f"submitJobs : VOMS FQANs are missing in your proxy. They are required for {userVO}")
            # return "ERROR: VOMS FQANs are missing. They are required for {0}".format(userVO)

        # get LSST pipeline username
        if userVO.lower() == "lsst":
            try:
                if job0.prodUserName and job0.prodUserName.lower() != "none":
                    user = job0.prodUserName
            except AttributeError:
                _logger.error(f"submitJobs : checking username for userVO {userVO}: username not found, defaulting to {user}.")

        # store jobs
        ret = self.taskBuffer.storeJobs(
            jobs,
            user,
            fqans=userFQANs,
            hostname=host,
            toPending=toPending,
            userVO=userVO,
        )
        _logger.debug(f"submitJobs {user} ->:{len(ret)}")

        # serialize
        return WrappedPickle.dumps(ret)

    # set debug mode
    def setDebugMode(self, dn, pandaID, prodManager, modeOn, workingGroup):
        ret = self.taskBuffer.setDebugMode(dn, pandaID, prodManager, modeOn, workingGroup)
        # return
        return ret

    # insert sandbox file info
    def insertSandboxFileInfo(self, userName, hostName, fileName, fileSize, checkSum):
        ret = self.taskBuffer.insertSandboxFileInfo(userName, hostName, fileName, fileSize, checkSum)
        # return
        return ret

    # check duplicated sandbox file
    def checkSandboxFile(self, userName, fileSize, checkSum):
        ret = self.taskBuffer.checkSandboxFile(userName, fileSize, checkSum)
        # return
        return ret

    # get job status
    def getJobStatus(self, idsStr, use_json, no_pickle=False):
        try:
            # deserialize IDs
            if use_json or no_pickle:
                ids = json.loads(idsStr)
            else:
                ids = WrappedPickle.loads(idsStr)
            _logger.debug(f"getJobStatus len   : {len(ids)}")
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error(f"too long ID list more than {maxIDs}")
                ids = ids[:maxIDs]
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"getJobStatus : {type} {value}")
            ids = []
        _logger.debug(f"getJobStatus start : {str(ids)} json={use_json}")
        # peek jobs
        ret = self.taskBuffer.peekJobs(ids, use_json=use_json)
        _logger.debug("getJobStatus end")
        # serialize
        if use_json:
            return json.dumps(ret)
        if no_pickle:
            return JobUtils.dump_jobs_json(ret)
        return WrappedPickle.dumps(ret)

    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self, jediTaskID):
        # get PandaIDs
        ret = self.taskBuffer.getPandaIDsWithTaskID(jediTaskID)
        # serialize
        return WrappedPickle.dumps(ret)

    # get job statistics
    def getJobStatistics(self, sourcetype=None):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForExtIF(sourcetype)
        return WrappedPickle.dumps(ret)

    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self, useMorePG=False):
        ret = self.taskBuffer.getJobStatisticsForBamboo(useMorePG)
        return WrappedPickle.dumps(ret)

    # get job statistics per site
    def getJobStatisticsPerSite(
        self,
        predefined=False,
        workingGroup="",
        countryGroup="",
        jobType="",
        minPriority=None,
        readArchived=True,
    ):
        # get job statistics
        ret = self.taskBuffer.getJobStatistics(
            readArchived,
            predefined,
            workingGroup,
            countryGroup,
            jobType,
            minPriority=minPriority,
        )
        return WrappedPickle.dumps(ret, convert_to_safe=True)

    # get job statistics per site and resource
    def getJobStatisticsPerSiteResource(self, timeWindow):
        ret = self.taskBuffer.getJobStatisticsPerSiteResource(timeWindow)
        return json.dumps(ret)

    # get job statistics per site, source label, and resource type
    def get_job_statistics_per_site_label_resource(self, time_window):
        ret = self.taskBuffer.get_job_statistics_per_site_label_resource(time_window)
        return json.dumps(ret)

    # kill jobs
    def killJobs(self, idsStr, user, host, code, prodManager, useMailAsID, fqans, killOpts=[]):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        if not isinstance(ids, list):
            ids = [ids]
        _logger.info(f"killJob : {user} {code} {prodManager} {fqans} {ids}")
        try:
            if useMailAsID:
                _logger.debug(f"killJob : getting mail address for {user}")
                nTry = 3
                for iDDMTry in range(nTry):
                    status, userInfo = rucioAPI.finger(user)
                    if status:
                        _logger.debug(f"killJob : {user} is converted to {userInfo['email']}")
                        user = userInfo["email"]
                        break
                    time.sleep(1)
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            _logger.error(f"killJob : failed to convert email address {user} : {err_type} {err_value}")
        # get working groups with prod role
        wgProdRole = []
        for fqan in fqans:
            tmpMatch = re.search("/atlas/([^/]+)/Role=production", fqan)
            if tmpMatch is not None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1)
                if tmpWG not in ["", "usatlas"] + wgProdRole:
                    wgProdRole.append(tmpWG)
                    # group production
                    wgProdRole.append(f"gr_{tmpWG}")
        # kill jobs
        ret = self.taskBuffer.killJobs(ids, user, code, prodManager, wgProdRole, killOpts)
        return WrappedPickle.dumps(ret)

    # reassign jobs
    def reassignJobs(self, idsStr, user, host, forPending, firstSubmission):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # reassign jobs
        ret = self.taskBuffer.reassignJobs(
            ids,
            forPending=forPending,
            firstSubmission=firstSubmission,
        )
        return WrappedPickle.dumps(ret)

    # get list of site spec
    def getSiteSpecs(self, siteType="analysis"):
        # get analysis site list
        specList = {}
        siteMapper = SiteMapper(self.taskBuffer)
        for id in siteMapper.siteSpecList:
            spec = siteMapper.siteSpecList[id]
            if siteType == "all" or spec.type == siteType:
                # convert to map
                tmpSpec = {}
                for attr in spec._attributes:
                    if attr in [
                        "ddm_endpoints_input",
                        "ddm_endpoints_output",
                        "ddm_input",
                        "ddm_output",
                        "setokens_input",
                        "num_slots_map",
                    ]:
                        continue
                    tmpSpec[attr] = getattr(spec, attr)
                specList[id] = tmpSpec
        return WrappedPickle.dumps(specList)

    # get script for offline running
    def getScriptOfflineRunning(self, pandaID, days=None):
        ret = self.taskBuffer.getScriptOfflineRunning(pandaID, days)
        return ret

    # get ban users
    def get_ban_users(self):
        ret = self.taskBuffer.get_ban_users()
        return json.dumps(ret)

    # get client version
    def getPandaClientVer(self):
        ret = self.taskBuffer.getPandaClientVer()
        return ret

    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self, dn, timeRange, fullFlag, minTaskID, task_type):
        ret = self.taskBuffer.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID, task_type)
        return WrappedPickle.dumps(ret)

    # get details of JediTask
    def getJediTaskDetails(self, jediTaskID, fullFlag, withTaskInfo):
        ret = self.taskBuffer.getJediTaskDetails(jediTaskID, fullFlag, withTaskInfo)
        return WrappedPickle.dumps(ret)

    # get full job status
    def getFullJobStatus(self, idsStr, dn):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            # truncate
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error(f"getFullJobStatus: too long ID list more than {maxIDs}")
                ids = ids[:maxIDs]
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"getFullJobStatus : {type} {value}")
            ids = []
        _logger.debug(f"getFullJobStatus start : {dn} {str(ids)}")
        # peek jobs
        ret = self.taskBuffer.getFullJobStatus(ids)
        _logger.debug("getFullJobStatus end")
        return WrappedPickle.dumps(ret)

    # insert task params
    def insertTaskParams(self, taskParams, user, prodRole, fqans, properErrorCode, parent_tid):
        ret = self.taskBuffer.insertTaskParamsPanda(
            taskParams,
            user,
            prodRole,
            fqans,
            properErrorCode=properErrorCode,
            parent_tid=parent_tid,
        )
        return ret

    # kill task
    def killTask(self, jediTaskID, user, prodRole, properErrorCode, broadcast):
        ret = self.taskBuffer.sendCommandTaskPanda(
            jediTaskID,
            user,
            prodRole,
            "kill",
            properErrorCode=properErrorCode,
            broadcast=broadcast,
        )
        return ret

    # finish task
    def finishTask(self, jediTaskID, user, prodRole, properErrorCode, qualifier, broadcast):
        ret = self.taskBuffer.sendCommandTaskPanda(
            jediTaskID,
            user,
            prodRole,
            "finish",
            properErrorCode=properErrorCode,
            comQualifier=qualifier,
            broadcast=broadcast,
        )
        return ret

    # reload input
    def reloadInput(self, jediTaskID, user, prodRole):
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID, user, prodRole, "incexec", comComment="{}", properErrorCode=True)
        return ret

    # retry task
    def retryTask(
        self,
        jediTaskID,
        user,
        prodRole,
        properErrorCode,
        newParams,
        noChildRetry,
        discardEvents,
        disable_staging_mode,
        keep_gshare_priority,
    ):
        # retry with new params
        if newParams is not None:
            try:
                # convert to dict
                newParams = PrioUtil.decodeJSON(newParams)
                # get original params
                taskParams = self.taskBuffer.getTaskParamsPanda(jediTaskID)
                taskParamsJson = PrioUtil.decodeJSON(taskParams)
                # replace with new values
                for newKey in newParams:
                    newVal = newParams[newKey]
                    taskParamsJson[newKey] = newVal
                taskParams = json.dumps(taskParamsJson)
                # retry with new params
                ret = self.taskBuffer.insertTaskParamsPanda(
                    taskParams,
                    user,
                    prodRole,
                    [],
                    properErrorCode=properErrorCode,
                    allowActiveTask=True,
                )
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                ret = 1, f"server error with {err_type}:{err_value}"
        else:
            com_qualifier = ""
            for com_key, com_param in [
                ("sole", noChildRetry),
                ("discard", discardEvents),
                ("staged", disable_staging_mode),
                ("keep", keep_gshare_priority),
            ]:
                if com_param:
                    com_qualifier += f"{com_key} "
            com_qualifier = com_qualifier.strip()
            # normal retry
            ret = self.taskBuffer.sendCommandTaskPanda(
                jediTaskID,
                user,
                prodRole,
                "retry",
                properErrorCode=properErrorCode,
                comQualifier=com_qualifier,
            )
        if properErrorCode is True and ret[0] == 5:
            # retry failed analysis jobs
            jobdefList = self.taskBuffer.getJobdefIDsForFailedJob(jediTaskID)
            cUID = self.taskBuffer.cleanUserID(user)
            for jobID in jobdefList:
                self.taskBuffer.finalizePendingJobs(cUID, jobID)
            self.taskBuffer.increaseAttemptNrPanda(jediTaskID, 5)
            return_str = f"retry has been triggered for failed jobs while the task is still {ret[1]}"
            if newParams is None:
                ret = 0, return_str
            else:
                ret = 3, return_str
        return ret

    # reassign task
    def reassignTask(self, jediTaskID, user, prodRole, comComment):
        ret = self.taskBuffer.sendCommandTaskPanda(
            jediTaskID,
            user,
            prodRole,
            "reassign",
            comComment=comComment,
            properErrorCode=True,
        )
        return ret

    # pause task
    def pauseTask(self, jediTaskID, user, prodRole):
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID, user, prodRole, "pause", properErrorCode=True)
        return ret

    # resume task
    def resumeTask(self, jediTaskID, user, prodRole):
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID, user, prodRole, "resume", properErrorCode=True)
        return ret

    # force avalanche for task
    def avalancheTask(self, jediTaskID, user, prodRole):
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID, user, prodRole, "avalanche", properErrorCode=True)
        return ret

    # send command to task
    def send_command_to_task(self, jedi_task_id, user, prod_role, command_string):
        ret = self.taskBuffer.sendCommandTaskPanda(jedi_task_id, user, prod_role, command_string, properErrorCode=True)
        return ret

    # change task priority
    def changeTaskPriority(self, jediTaskID, newPriority):
        ret = self.taskBuffer.changeTaskPriorityPanda(jediTaskID, newPriority)
        return ret

    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self, jediTaskID, increasedNr):
        ret = self.taskBuffer.increaseAttemptNrPanda(jediTaskID, increasedNr)
        return ret

    # change task attribute
    def changeTaskAttributePanda(self, jediTaskID, attrName, attrValue):
        ret = self.taskBuffer.changeTaskAttributePanda(jediTaskID, attrName, attrValue)
        return ret

    # change split rule for task
    def changeTaskSplitRulePanda(self, jediTaskID, attrName, attrValue):
        ret = self.taskBuffer.changeTaskSplitRulePanda(jediTaskID, attrName, attrValue)
        return ret

    # reactivate task
    def reactivateTask(self, jediTaskID, keep_attempt_nr, trigger_job_generation):
        ret = self.taskBuffer.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)
        return ret

    # get task status
    def getTaskStatus(self, jediTaskID):
        ret = self.taskBuffer.getTaskStatus(jediTaskID)
        return ret[0]

    # reassign share
    def reassignShare(self, jedi_task_ids, share_dest, reassign_running):
        return self.taskBuffer.reassignShare(jedi_task_ids, share_dest, reassign_running)

    # get taskParamsMap
    def getTaskParamsMap(self, jediTaskID):
        # get taskParamsMap
        ret = self.taskBuffer.getTaskParamsMap(jediTaskID)
        return ret

    # update workers
    def updateWorkers(self, user, host, harvesterID, data):
        ret = self.taskBuffer.updateWorkers(harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # update workers
    def updateServiceMetrics(self, user, host, harvesterID, data):
        ret = self.taskBuffer.updateServiceMetrics(harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # add harvester dialog messages
    def addHarvesterDialogs(self, user, harvesterID, dialogs):
        ret = self.taskBuffer.addHarvesterDialogs(harvesterID, dialogs)
        if not ret:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, "")
        return json.dumps(return_value)

    # heartbeat for harvester
    def harvesterIsAlive(self, user, host, harvesterID, data):
        ret = self.taskBuffer.harvesterIsAlive(user, host, harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # get stats of workers
    def getWorkerStats(self):
        return self.taskBuffer.getWorkerStats()

    # report stat of workers
    def reportWorkerStats_jobtype(self, harvesterID, siteName, paramsList):
        return self.taskBuffer.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        return_value = self.taskBuffer.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        return json.dumps(return_value)

    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID, totalJumboJobs, nJumboPerSite):
        return_value = self.taskBuffer.enableJumboJobs(jediTaskID, totalJumboJobs, nJumboPerSite)
        if totalJumboJobs > 0 and return_value[0] == 0:
            self.avalancheTask(jediTaskID, "panda", True)
        return json.dumps(return_value)

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        return_value = self.taskBuffer.getUserJobMetadata(jediTaskID)
        return json.dumps(return_value)

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        return_value = self.taskBuffer.getJumboJobDatasets(n_days, grace_period)
        # serialize
        return json.dumps(return_value)

    # sweep panda queue
    def sweepPQ(self, panda_queue, status_list, ce_list, submission_host_list):
        try:
            panda_queue_des = json.loads(panda_queue)
            status_list_des = json.loads(status_list)
            ce_list_des = json.loads(ce_list)
            submission_host_list_des = json.loads(submission_host_list)
        except Exception:
            _logger.error("Problem deserializing variables")

        ret = self.taskBuffer.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        return WrappedPickle.dumps(ret)

    # send command to a job
    def send_command_to_job(self, panda_id, com):
        ret = self.taskBuffer.send_command_to_job(panda_id, com)
        return ret

    # set user secret
    def set_user_secret(self, owner, key, value):
        ret = self.taskBuffer.set_user_secret(owner, key, value)
        return ret

    # get user secrets
    def get_user_secrets(self, owner, keys, get_json):
        ret = self.taskBuffer.get_user_secrets(owner, keys, get_json)
        return ret

    # get files in datasets
    def get_files_in_datasets(self, task_id, dataset_types):
        ret = self.taskBuffer.get_files_in_datasets(task_id, dataset_types)
        return ret


# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmp_key in req.subprocess_env:
        tmp_value = req.subprocess_env[tmp_key]
        # Scan VOMS attributes
        # compact style
        if tmp_key.startswith("GRST_CRED_") and tmp_value.startswith("VOMS"):
            fqan = tmp_value.split()[-1]
            fqans.append(fqan)

        # old style
        elif tmp_key.startswith("GRST_CONN_"):
            tmp_items = tmp_value.split(":")
            if len(tmp_items) == 2 and tmp_items[0] == "fqan":
                fqans.append(tmp_items[-1])

    return fqans


# get DN
def _getDN(req):
    real_dn = ""
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        # remove redundant CN
        real_dn = CoreUtils.get_bare_dn(req.subprocess_env["SSL_CLIENT_S_DN"], keep_proxy=True)
    return real_dn


# check role
def _has_production_role(req):
    # check DN
    user = _getDN(req)
    for sdn in panda_config.production_dns:
        if sdn in user:
            return True
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in [
            "/atlas/usatlas/Role=production",
            "/atlas/Role=production",
            "^/[^/]+/Role=production",
        ]:
            if fqan.startswith(rolePat):
                return True
            if re.search(rolePat, fqan):
                return True
    return False


# get primary working group with prod role
def _getWGwithPR(req):
    try:
        fqans = _getFQAN(req)
        for fqan in fqans:
            tmpMatch = re.search("/[^/]+/([^/]+)/Role=production", fqan)
            if tmpMatch is not None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1)
                if tmpWG not in ["", "usatlas"]:
                    return tmpWG.split("-")[-1].lower()
    except Exception:
        pass
    return None


"""
web service interface

"""


# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        _logger.warning(f"access via limited proxy : {req.subprocess_env['SSL_CLIENT_S_DN']}")
        return False
    return True


# submit jobs
def submitJobs(req, jobs, toPending=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    # production Role
    is_production_role = _has_production_role(req)
    # to pending
    toPending = resolve_true(toPending)

    return userIF.submitJobs(jobs, user, host, fqans, is_production_role, toPending)


# get job status
def getJobStatus(req, ids, no_pickle=None):
    return userIF.getJobStatus(ids, req.acceptJson(), no_pickle)


# set debug mode
def setDebugMode(req, pandaID, modeOn):
    tmp_log = LogWrapper(_logger, f"setDebugMode {pandaID} {modeOn}")
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        error_str = MESSAGE_SSL
        tmp_log.error(error_str)
        return f"ERROR: {error_str}"
    user = _getDN(req)
    # check role
    is_production_manager = _has_production_role(req)
    fqans = _getFQAN(req)
    # mode
    modeOn = resolve_true(modeOn)

    # get the primary working group with prod role
    working_group = _getWGwithPR(req)
    tmp_log.debug(f"user={user} mgr={is_production_manager} wg={working_group} fqans={str(fqans)}")
    # exec
    return userIF.setDebugMode(user, pandaID, is_production_manager, modeOn, working_group)


# insert sandbox file info
def insertSandboxFileInfo(req, userName, fileName, fileSize, checkSum):
    tmp_log = LogWrapper(_logger, f"insertSandboxFileInfo {userName} {fileName}")
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        error_str = MESSAGE_SSL
        tmp_log.error(error_str)
        return f"ERROR: {error_str}"

    # check role
    is_production_manager = _has_production_role(req)
    if not is_production_manager:
        tmp_log.error(MESSAGE_PROD_ROLE)
        return f"ERROR: {MESSAGE_PROD_ROLE}"

    # hostname
    if hasattr(panda_config, "sandboxHostname") and panda_config.sandboxHostname:
        hostName = panda_config.sandboxHostname
    else:
        hostName = req.get_remote_host()
    # exec
    return userIF.insertSandboxFileInfo(userName, hostName, fileName, fileSize, checkSum)


# check duplicated sandbox file
def checkSandboxFile(req, fileSize, checkSum):
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        return f"ERROR: {MESSAGE_SSL}"
    user = _getDN(req)
    # exec
    return userIF.checkSandboxFile(user, fileSize, checkSum)


# get job statistics
def getJobStatistics(req, sourcetype=None):
    return userIF.getJobStatistics(sourcetype)


# get job statistics for Babmoo
def getJobStatisticsForBamboo(req, useMorePG=None):
    if useMorePG == "True":
        useMorePG = pandaserver.taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ["False", None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except Exception:
            useMorePG = False
    return userIF.getJobStatisticsForBamboo(useMorePG)


# get job statistics per site and resource
def getJobStatisticsPerSiteResource(req, timeWindow=None):
    return userIF.getJobStatisticsPerSiteResource(timeWindow)


# get job statistics per site and resource
def get_job_statistics_per_site_label_resource(req, time_window=None):
    return userIF.get_job_statistics_per_site_label_resource(time_window)


# get job statistics per site
def getJobStatisticsPerSite(
    req,
    predefined="False",
    workingGroup="",
    countryGroup="",
    jobType="",
    minPriority=None,
    readArchived=None,
):
    predefined = resolve_true(predefined)

    if minPriority is not None:
        try:
            minPriority = int(minPriority)
        except Exception:
            minPriority = None

    if readArchived == "True":
        readArchived = True
    elif readArchived == "False":
        readArchived = False
    else:
        host = req.get_remote_host()
        # read jobsArchived for panglia
        if re.search("panglia.*\.triumf\.ca$", host) is not None or host in ["gridweb.triumf.ca"]:
            readArchived = True
        else:
            readArchived = False
    return userIF.getJobStatisticsPerSite(predefined, workingGroup, countryGroup, jobType, minPriority, readArchived)


# kill jobs
def killJobs(req, ids, code=None, useMailAsID=None, killOpts=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_manager = _has_production_role(req)
    # get FQANs
    fqans = _getFQAN(req)
    # use email address as ID
    useMailAsID = resolve_true(useMailAsID)

    # hostname
    host = req.get_remote_host()
    # options
    if killOpts is None:
        killOpts = []
    else:
        killOpts = killOpts.split(",")
    return userIF.killJobs(ids, user, host, code, is_production_manager, useMailAsID, fqans, killOpts)


# reassign jobs
def reassignJobs(req, ids, forPending=None, firstSubmission=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    # for pending
    forPending = resolve_true(forPending)

    # first submission
    firstSubmission = resolve_false(firstSubmission)

    return userIF.reassignJobs(ids, user, host, forPending, firstSubmission)


# get list of site spec
def getSiteSpecs(req, siteType=None):
    if siteType is not None:
        return userIF.getSiteSpecs(siteType)
    else:
        return userIF.getSiteSpecs()


# get ban users
def get_ban_users(req):
    return userIF.get_ban_users()


# get client version
def getPandaClientVer(req):
    return userIF.getPandaClientVer()


# get script for offline running
def getScriptOfflineRunning(req, pandaID, days=None):
    try:
        if days is not None:
            days = int(days)
    except Exception:
        days = None
    return userIF.getScriptOfflineRunning(pandaID, days)


# get active JediTasks in a time range
def getJediTasksInTimeRange(req, timeRange, dn=None, fullFlag=None, minTaskID=None, task_type="user"):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        return False
    if dn is None:
        dn = _getDN(req)
    fullFlag = resolve_true(fullFlag)

    try:
        minTaskID = int(minTaskID)
    except Exception:
        minTaskID = None
    _logger.debug(f"getJediTasksInTimeRange {dn} {timeRange}")
    # execute
    return userIF.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID, task_type)


# get details of JediTask
def getJediTaskDetails(req, jediTaskID, fullFlag, withTaskInfo):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        return False
    # option
    fullFlag = resolve_true(fullFlag)
    withTaskInfo = resolve_true(withTaskInfo)

    _logger.debug(f"getJediTaskDetails {jediTaskID} {fullFlag} {withTaskInfo}")
    # execute
    return userIF.getJediTaskDetails(jediTaskID, fullFlag, withTaskInfo)


# get full job status
def getFullJobStatus(req, ids):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if "SSL_CLIENT_S_DN" not in req.subprocess_env:
        return False
    dn = _getDN(req)
    return userIF.getFullJobStatus(ids, dn)


# insert task params
def insertTaskParams(req, taskParams=None, properErrorCode=None, parent_tid=None):
    tmp_log = LogWrapper(_logger, f"insertTaskParams-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_log.debug("start")
    properErrorCode = resolve_true(properErrorCode)

    # check security
    if not isSecure(req):
        tmp_log.debug(MESSAGE_SSL)
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)

    # check format
    try:
        json.loads(taskParams)
    except Exception:
        tmp_log.debug(MESSAGE_JSON)
        return WrappedPickle.dumps((False, MESSAGE_JSON))
    # check role
    is_production_role = _has_production_role(req)
    # get FQANs
    fqans = _getFQAN(req)

    tmp_log.debug(f"user={user} prodRole={is_production_role} FQAN:{str(fqans)} parent_tid={parent_tid}")
    ret = userIF.insertTaskParams(taskParams, user, is_production_role, fqans, properErrorCode, parent_tid)
    try:
        tmp_log.debug(ret[1])
    except Exception:
        pass
    return WrappedPickle.dumps(ret)


# kill task
def killTask(req, jediTaskID=None, properErrorCode=None, broadcast=None):
    properErrorCode = resolve_true(properErrorCode)
    broadcast = resolve_true(broadcast)

    # check security
    if not isSecure(req):
        error_code = CODE_SSL if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_SSL))

    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        error_code = CODE_LOGIC if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_TASK_ID))

    ret = userIF.killTask(jediTaskID, user, is_production_role, properErrorCode, broadcast)
    return WrappedPickle.dumps(ret)


# retry task
def retryTask(
    req,
    jediTaskID,
    properErrorCode=None,
    newParams=None,
    noChildRetry=None,
    discardEvents=None,
    disable_staging_mode=None,
    keep_gshare_priority=None,
):
    properErrorCode = resolve_true(properErrorCode)
    noChildRetry = resolve_true(noChildRetry)
    discardEvents = resolve_true(discardEvents)
    disable_staging_mode = resolve_true(disable_staging_mode)
    keep_gshare_priority = resolve_true(keep_gshare_priority)

    # check security
    if not isSecure(req):
        error_code = CODE_SSL if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_SSL))

    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        error_code = CODE_LOGIC if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_TASK_ID))

    ret = userIF.retryTask(
        jediTaskID,
        user,
        is_production_role,
        properErrorCode,
        newParams,
        noChildRetry,
        discardEvents,
        disable_staging_mode,
        keep_gshare_priority,
    )
    return WrappedPickle.dumps(ret)


# reassign task to site/cloud
def reassignTask(req, jediTaskID, site=None, cloud=None, nucleus=None, soft=None, mode=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((CODE_SSL, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((CODE_LOGIC, MESSAGE_TASK_ID))
    # site or cloud
    if site is not None:
        # set 'y' to go back to oldStatus immediately
        comComment = f"site:{site}:y"
    elif nucleus is not None:
        comComment = f"nucleus:{nucleus}:n"
    else:
        comComment = f"cloud:{cloud}:n"
    if mode == "nokill":
        comComment += ":nokill reassign"
    elif mode == "soft" or soft == "True":
        comComment += ":soft reassign"
    ret = userIF.reassignTask(jediTaskID, user, is_production_role, comComment)
    return WrappedPickle.dumps(ret)


# finish task
def finishTask(req, jediTaskID=None, properErrorCode=None, soft=None, broadcast=None):
    properErrorCode = resolve_true(properErrorCode)
    broadcast = resolve_true(broadcast)
    qualifier = None
    if soft == "True":
        qualifier = "soft"

    # check security
    if not isSecure(req):
        error_code = CODE_SSL if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_SSL))

    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        error_code = CODE_LOGIC if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_TASK_ID))

    ret = userIF.finishTask(jediTaskID, user, is_production_role, properErrorCode, qualifier, broadcast)
    return WrappedPickle.dumps(ret)


# reload input
def reloadInput(req, jediTaskID, properErrorCode=None):
    properErrorCode = resolve_true(properErrorCode)
    # check security
    if not isSecure(req):
        error_code = CODE_SSL if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_SSL))

    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        error_code = CODE_LOGIC if properErrorCode else False
        return WrappedPickle.dumps((error_code, MESSAGE_TASK_ID))

    ret = userIF.reloadInput(jediTaskID, user, is_production_role)
    return WrappedPickle.dumps(ret)


# change task priority
def changeTaskPriority(req, jediTaskID=None, newPriority=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return "Failed : production or pilot role required"
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    # check priority
    try:
        newPriority = int(newPriority)
    except Exception:
        return WrappedPickle.dumps((False, "newPriority must be an integer"))
    ret = userIF.changeTaskPriority(jediTaskID, newPriority)
    return WrappedPickle.dumps(ret)


# increase attempt number for unprocessed files
def increaseAttemptNrPanda(req, jediTaskID, increasedNr):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((3, MESSAGE_PROD_ROLE))

    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((4, MESSAGE_TASK_ID))

    # check value for increase
    try:
        increasedNr = int(increasedNr)
    except Exception:
        increasedNr = -1
    if increasedNr < 0:
        return WrappedPickle.dumps((4, "increase must be a positive integer"))

    return WrappedPickle.dumps(userIF.increaseAttemptNrPanda(jediTaskID, increasedNr))


# change task attribute
def changeTaskAttributePanda(req, jediTaskID, attrName, attrValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    # check attribute
    if attrName not in ["ramCount", "wallTime", "cpuTime", "coreCount"]:
        return WrappedPickle.dumps((2, f"disallowed to update {attrName}"))
    ret = userIF.changeTaskAttributePanda(jediTaskID, attrName, attrValue)
    return WrappedPickle.dumps((ret, None))


# change split rule for task
def changeTaskSplitRulePanda(req, jediTaskID, attrName, attrValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    # check attribute
    if attrName not in [
        "AI",
        "TW",
        "EC",
        "ES",
        "MF",
        "NG",
        "NI",
        "NF",
        "NJ",
        "AV",
        "IL",
        "LI",
        "LC",
        "CC",
        "OT",
        "UZ",
    ]:
        return WrappedPickle.dumps((2, f"disallowed to update {attrName}"))
    ret = userIF.changeTaskSplitRulePanda(jediTaskID, attrName, attrValue)
    return WrappedPickle.dumps((ret, None))


# pause task
def pauseTask(req, jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.pauseTask(jediTaskID, user, is_production_role)
    return WrappedPickle.dumps(ret)


# resume task
def resumeTask(req, jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, "production role required"))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.resumeTask(jediTaskID, user, is_production_role)
    return WrappedPickle.dumps(ret)


# force avalanche for task
def avalancheTask(req, jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, "production role required"))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.avalancheTask(jediTaskID, user, is_production_role)
    return WrappedPickle.dumps(ret)


# release task
def release_task(req, jedi_task_id):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    prod_role = _has_production_role(req)
    # only prod managers can use this method
    if not prod_role:
        return json.dumps((False, "production role required"))
    # check jediTaskID
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return json.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.send_command_to_task(jedi_task_id, user, prod_role, "release")
    return json.dumps(ret)


# kill unfinished jobs
def killUnfinishedJobs(req, jediTaskID, code=None, useMailAsID=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_manager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ["/atlas/usatlas/Role=production", "/atlas/Role=production"]:
            if fqan.startswith(rolePat):
                is_production_manager = True
                break
        # escape
        if is_production_manager:
            break
    # use email address as ID
    useMailAsID = resolve_true(useMailAsID)
    # hostname
    host = req.get_remote_host()
    # get PandaIDs
    ids = userIF.getPandaIDsWithTaskID(jediTaskID)
    # kill
    return userIF.killJobs(ids, user, host, code, is_production_manager, useMailAsID, fqans)


# change modificationTime for task
def changeTaskModTimePanda(req, jediTaskID, diffValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    try:
        diffValue = int(diffValue)
        attrValue = datetime.datetime.now() + datetime.timedelta(hours=diffValue)
    except Exception:
        return WrappedPickle.dumps((False, f"failed to convert {diffValue} to time diff"))
    ret = userIF.changeTaskAttributePanda(jediTaskID, "modificationTime", attrValue)
    return WrappedPickle.dumps((ret, None))


# get PandaIDs with TaskID
def getPandaIDsWithTaskID(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    idsStr = userIF.getPandaIDsWithTaskID(jediTaskID)
    # deserialize
    ids = WrappedPickle.loads(idsStr)

    return WrappedPickle.dumps(ids)


# reactivate Task
def reactivateTask(req, jediTaskID, keep_attempt_nr=None, trigger_job_generation=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # check role
    is_production_manager = _has_production_role(req)
    if not is_production_manager:
        msg = "production role is required"
        _logger.error(f"reactivateTask: {msg}")
        return WrappedPickle.dumps((False, msg))
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    keep_attempt_nr = resolve_true(keep_attempt_nr)
    trigger_job_generation = resolve_true(trigger_job_generation)

    ret = userIF.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)

    return WrappedPickle.dumps(ret)


# get task status
def getTaskStatus(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.getTaskStatus(jediTaskID)
    return WrappedPickle.dumps(ret)


# reassign share
def reassignShare(req, jedi_task_ids_pickle, share, reassign_running):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    prod_role = _has_production_role(req)
    if not prod_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))

    jedi_task_ids = WrappedPickle.loads(jedi_task_ids_pickle)
    _logger.debug(f"reassignShare: jedi_task_ids: {jedi_task_ids}, share: {share}, reassign_running: {reassign_running}")

    if not ((isinstance(jedi_task_ids, list) or (isinstance(jedi_task_ids, tuple)) and isinstance(share, str))):
        return WrappedPickle.dumps((False, "jedi_task_ids must be tuple/list and share must be string"))

    ret = userIF.reassignShare(jedi_task_ids, share, reassign_running)
    return WrappedPickle.dumps(ret)


# get taskParamsMap with TaskID
def getTaskParamsMap(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.getTaskParamsMap(jediTaskID)
    return WrappedPickle.dumps(ret)


# update workers
def updateWorkers(req, harvesterID, workers):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    return_value = None
    tStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    # convert
    try:
        data = json.loads(workers)
    except Exception:
        return_value = json.dumps((False, MESSAGE_JSON))
    # update
    if return_value is None:
        return_value = userIF.updateWorkers(user, host, harvesterID, data)
    tDelta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - tStart
    _logger.debug(f"updateWorkers {harvesterID} took {tDelta.seconds}.{tDelta.microseconds // 1000:03d} sec")

    return return_value


# update workers
def updateServiceMetrics(req, harvesterID, metrics):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))

    user = _getDN(req)

    host = req.get_remote_host()
    return_value = None
    tStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    # convert
    try:
        data = json.loads(metrics)
    except Exception:
        return_value = json.dumps((False, MESSAGE_JSON))

    # update
    if return_value is None:
        return_value = userIF.updateServiceMetrics(user, host, harvesterID, data)

    tDelta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - tStart
    _logger.debug(f"updateServiceMetrics {harvesterID} took {tDelta.seconds}.{tDelta.microseconds // 1000:03d} sec")

    return return_value


# add harvester dialog messages
def addHarvesterDialogs(req, harvesterID, dialogs):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # convert
    try:
        data = json.loads(dialogs)
    except Exception:
        return json.dumps((False, MESSAGE_JSON))
    # update
    return userIF.addHarvesterDialogs(user, harvesterID, data)


# heartbeat for harvester
def harvesterIsAlive(req, harvesterID, data=None):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    # convert
    try:
        if data is not None:
            data = json.loads(data)
        else:
            data = dict()
    except Exception:
        return json.dumps((False, MESSAGE_JSON))
    # update
    return userIF.harvesterIsAlive(user, host, harvesterID, data)


# get stats of workers
def getWorkerStats(req):
    # get
    ret = userIF.getWorkerStats()
    return json.dumps(ret)


# report stat of workers
def reportWorkerStats_jobtype(req, harvesterID, siteName, paramsList):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # update
    ret = userIF.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)
    return json.dumps(ret)


# set num slots for workload provisioning
def setNumSlotsForWP(req, pandaQueueName, numSlots, gshare=None, resourceType=None, validPeriod=None):
    # check security
    if not isSecure(req):
        return json.dumps((CODE_SSL, MESSAGE_SSL))
    # check role
    if not _has_production_role(req):
        return json.dumps((CODE_LOGIC, "production role is required in the certificate"))
    # convert
    try:
        numSlots = int(numSlots)
    except Exception:
        return json.dumps((CODE_OTHER_PARAMS, "numSlots must be int"))
    # execute
    return userIF.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)


# enable jumbo jobs
def enableJumboJobs(req, jediTaskID, nJumboJobs, nJumboPerSite=None):
    # check security
    if not isSecure(req):
        return json.dumps((CODE_SSL, MESSAGE_SSL))
    # check role
    if not _has_production_role(req):
        return json.dumps((CODE_LOGIC, "production role is required in the certificate"))
    # convert
    try:
        nJumboJobs = int(nJumboJobs)
    except Exception:
        return json.dumps((CODE_OTHER_PARAMS, "nJumboJobs must be int"))
    try:
        nJumboPerSite = int(nJumboPerSite)
    except Exception:
        nJumboPerSite = nJumboJobs
    # execute
    return userIF.enableJumboJobs(jediTaskID, nJumboJobs, nJumboPerSite)


# get user job metadata
def getUserJobMetadata(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    return userIF.getUserJobMetadata(jediTaskID)


# get jumbo job datasets
def getJumboJobDatasets(req, n_days, grace_period=0):
    try:
        n_days = int(n_days)
    except Exception:
        return WrappedPickle.dumps((False, "wrong n_days"))
    try:
        grace_period = int(grace_period)
    except Exception:
        return WrappedPickle.dumps((False, "wrong grace_period"))
    return userIF.getJumboJobDatasets(n_days, grace_period)


# send Harvester the command to clean up the workers for a panda queue
def sweepPQ(req, panda_queue, status_list, ce_list, submission_host_list):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # check role
    prod_role = _has_production_role(req)
    if not prod_role:
        return json.dumps((False, MESSAGE_PROD_ROLE))

    return json.dumps((True, userIF.sweepPQ(panda_queue, status_list, ce_list, submission_host_list)))


# json decoder for idds constants
def decode_idds_enum(d):
    if "__idds_const__" in d:
        items = d["__idds_const__"].split(".")
        obj = idds.common.constants
        for item in items:
            obj = getattr(obj, item)
        return obj
    else:
        return d


# relay iDDS command
def relay_idds_command(req, command_name, args=None, kwargs=None, manager=None, json_outputs=None):
    tmp_log = LogWrapper(
        _logger,
        f"relay_idds_command-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}",
    )
    # check security
    if not isSecure(req):
        tmp_log.error(MESSAGE_SSL)
        return json.dumps((False, MESSAGE_SSL))
    try:
        manager = resolve_bool(manager)
        if not manager:
            manager = False
        if "+" in command_name:
            command_name, idds_host = command_name.split("+")
        else:
            idds_host = idds.common.utils.get_rest_host()
        if manager:
            c = iDDS_ClientManager(idds_host)
        else:
            c = iDDS_Client(idds_host)
        if not hasattr(c, command_name):
            tmp_str = f"{command_name} is not a command of iDDS {c.__class__.__name__}"
            tmp_log.error(tmp_str)
            return json.dumps((False, tmp_str))
        if args:
            try:
                args = idds.common.utils.json_loads(args)
            except Exception as e:
                tmp_log.warning(f"failed to load args json with {str(e)}")
                args = json.loads(args, object_hook=decode_idds_enum)
        else:
            args = []
        if kwargs:
            try:
                kwargs = idds.common.utils.json_loads(kwargs)
            except Exception as e:
                tmp_log.warning(f"failed to load kwargs json with {str(e)}")
                kwargs = json.loads(kwargs, object_hook=decode_idds_enum)
        else:
            kwargs = {}
        # json outputs
        if json_outputs and manager:
            c.setup_json_outputs()
        # set original username
        dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
        if dn:
            c.set_original_user(user_name=clean_user_id(dn))
        tmp_log.debug(f"execute: class={c.__class__.__name__} com={command_name} host={idds_host} args={str(args)[:200]} kwargs={str(kwargs)[:200]}")
        ret = getattr(c, command_name)(*args, **kwargs)
        tmp_log.debug(f"ret: {str(ret)[:200]}")
        try:
            return json.dumps((True, ret))
        except Exception:
            return idds.common.utils.json_dumps((True, ret))
    except Exception as e:
        tmp_str = f"failed to execute command with {str(e)}"
        tmp_log.error(f"{tmp_str} {traceback.format_exc()}")
        return json.dumps((False, tmp_str))


# relay iDDS workflow command with ownership check
def execute_idds_workflow_command(req, command_name, kwargs=None, json_outputs=None):
    try:
        tmp_log = LogWrapper(
            _logger,
            f"execute_idds_workflow_command-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}",
        )
        if kwargs:
            try:
                kwargs = idds.common.utils.json_loads(kwargs)
            except Exception:
                kwargs = json.loads(kwargs, object_hook=decode_idds_enum)
        else:
            kwargs = {}
        if "+" in command_name:
            command_name, idds_host = command_name.split("+")
        else:
            idds_host = idds.common.utils.get_rest_host()
        # check permission
        if command_name in ["get_status"]:
            check_owner = False
        elif command_name in ["abort", "suspend", "resume", "retry", "finish"]:
            check_owner = True
        else:
            tmp_message = f"{command_name} is unsupported"
            tmp_log.error(tmp_message)
            return json.dumps((False, tmp_message))
        # check owner
        c = iDDS_ClientManager(idds_host)
        if json_outputs:
            c.setup_json_outputs()
        dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
        if check_owner:
            # requester
            if not dn:
                tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
            requester = clean_user_id(dn)
            # get request_id
            request_id = kwargs.get("request_id")
            if request_id is None:
                tmp_message = "request_id is missing"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
            # get request
            req = c.get_requests(request_id=request_id)
            if not req:
                tmp_message = f"request {request_id} is not found"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
            user_name = req[0].get("username")
            if user_name and user_name != requester:
                tmp_message = f"request {request_id} is not owned by {requester}"
                tmp_log.error(tmp_message)
                return json.dumps((False, tmp_message))
        # set original username
        if dn:
            c.set_original_user(user_name=clean_user_id(dn))
        # execute command
        tmp_log.debug(f"com={command_name} host={idds_host} kwargs={str(kwargs)}")
        ret = getattr(c, command_name)(**kwargs)
        tmp_log.debug(str(ret))
        if isinstance(ret, dict) and "message" in ret:
            return json.dumps((True, [ret["status"], ret["message"]]))
        return json.dumps((True, ret))
    except Exception as e:
        tmp_log.error(f"failed with {str(e)} {traceback.format_exc()}")
        return json.dumps((False, f"server failed with {str(e)}"))


# send command to a job
def send_command_to_job(req, panda_id, com):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # check role
    prod_role = _has_production_role(req)
    if not prod_role:
        return json.dumps((False, MESSAGE_PROD_ROLE))
    return json.dumps(userIF.send_command_to_job(panda_id, com))


# set user secret
def set_user_secret(req, key=None, value=None):
    tmp_log = LogWrapper(_logger, f"set_user_secret-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    # get owner
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    if not dn:
        tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
        tmp_log.error(tmp_message)
        return json.dumps((False, tmp_message))
    owner = clean_user_id(dn)
    return json.dumps(userIF.set_user_secret(owner, key, value))


# get user secrets
def get_user_secrets(req, keys=None, get_json=None):
    tmp_log = LogWrapper(_logger, f"get_user_secrets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    # get owner
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    get_json = resolve_true(get_json)

    if not dn:
        tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
        tmp_log.error(tmp_message)
        return json.dumps((False, tmp_message))
    owner = clean_user_id(dn)
    return json.dumps(userIF.get_user_secrets(owner, keys, get_json))


# get files in datasets
def get_files_in_datasets(req, task_id, dataset_types="input,pseudo_input"):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    return json.dumps(userIF.get_files_in_datasets(task_id, dataset_types))
