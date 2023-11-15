#!/usr/bin/python

"""
entry point

"""

import datetime
import gzip
import io
import json
import signal
import sys
import tempfile
import traceback

import pandaserver.taskbuffer.ErrorCode
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config

# pylint: disable=W0611
# These imports are used by the web I/F, although pylint doesn't see it
from pandaserver.dataservice.DataService import (
    dataService,
    datasetCompleted,
    updateFileStatusInDisp,
)

# pylint: disable=W0611
from pandaserver.jobdispatcher.JobDispatcher import (
    ackCommands,
    checkEventsAvailability,
    checkJobStatus,
    genPilotToken,
    get_events_status,
    get_max_worker_id,
    getCommands,
    getDNsForS3,
    getEventRanges,
    getJob,
    getKeyPair,
    getProxy,
    getResourceTypes,
    getStatus,
    jobDispatcher,
    updateEventRange,
    updateEventRanges,
    updateJob,
    updateJobsInBulk,
    updateWorkerPilotStatus,
)
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer.Initializer import initializer
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# pylint: disable=W0611
from pandaserver.taskbuffer.Utils import (
    delete_checkpoint,
    deleteFile,
    fetchLog,
    getAttr,
    getServer,
    getVomsAttr,
    isAlive,
    put_checkpoint,
    put_file_recovery_request,
    put_workflow_request,
    putEventPickingRequest,
    putFile,
    touchFile,
    updateLog,
    uploadLog,
)
from pandaserver.userinterface import Client

# pylint: disable=W0611
from pandaserver.userinterface.UserIF import (
    addHarvesterDialogs,
    addSiteAccess,
    avalancheTask,
    changeJobPriorities,
    changeTaskAttributePanda,
    changeTaskModTimePanda,
    changeTaskPriority,
    changeTaskSplitRulePanda,
    checkMergeGenerationStatus,
    checkSandboxFile,
    enableJumboJobs,
    execute_idds_workflow_command,
    finishTask,
    get_ban_users,
    get_files_in_datasets,
    get_job_statistics_per_site_label_resource,
    get_user_secrets,
    getActiveDatasets,
    getAssigningTask,
    getCloudSpecs,
    getDisInUseForAnal,
    getFilesInUseForAnal,
    getFullJobStatus,
    getGShareStatus,
    getHighestPrioJobStat,
    getJediTaskDetails,
    getJediTasksInTimeRange,
    getJobIDsInTimeRange,
    getJobStatistics,
    getJobStatisticsForBamboo,
    getJobStatisticsPerSite,
    getJobStatisticsPerSiteResource,
    getJobStatisticsPerUserSite,
    getJobStatisticsWithLabel,
    getJobStatus,
    getJobsToBeUpdated,
    getJumboJobDatasets,
    getLFNsInUseForAnal,
    getNumPilots,
    getNUserJobs,
    getPandaClientVer,
    getPandaIDsSite,
    getPandaIDsWithTaskID,
    getPandaIDwithJobExeID,
    getPandIDsWithJobID,
    getProxyKey,
    getQueuedAnalJobs,
    getRetryHistory,
    getScriptOfflineRunning,
    getSerialNumberForGroupJob,
    getSiteSpecs,
    getSlimmedFileInfoPandaIDs,
    getTaskParamsMap,
    getTaskStatus,
    getUserJobMetadata,
    harvesterIsAlive,
    increaseAttemptNrPanda,
    insertSandboxFileInfo,
    insertTaskParams,
    killJobs,
    killTask,
    killUnfinishedJobs,
    listSiteAccess,
    listTasksInShare,
    pauseTask,
    queryJobInfoPerCloud,
    queryLastFilesInDataset,
    queryPandaIDs,
    reactivateTask,
    reassignJobs,
    reassignShare,
    reassignTask,
    registerProxyKey,
    relay_idds_command,
    release_task,
    reloadInput,
    reportWorkerStats,
    reportWorkerStats_jobtype,
    resubmitJobs,
    resumeTask,
    retryFailedJobsInActive,
    retryTask,
    runTaskAssignment,
    seeCloudTask,
    send_command_to_job,
    set_user_secret,
    setCloudTaskByUser,
    setDebugMode,
    setNumSlotsForWP,
    submitJobs,
    sweepPQ,
    updateProdDBUpdateTimes,
    updateServiceMetrics,
    updateSiteAccess,
    updateWorkers,
    userIF,
)

# initialize cx_Oracle using dummy connection
initializer.init()

# initialize TaskBuffer
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(
    panda_config.dbhost,
    panda_config.dbpasswd,
    nDBConnection=panda_config.nDBConnection,
    useTimeout=True,
    requester=requester_id,
)

# initialize JobDispatcher
if panda_config.nDBConnection != 0:
    jobDispatcher.init(taskBuffer)

# initialize DataService
if panda_config.nDBConnection != 0:
    dataService.init(taskBuffer)

# initialize UserIF
if panda_config.nDBConnection != 0:
    userIF.init(taskBuffer)

# import web I/F
allowedMethods = []

allowedMethods += [
    "isAlive",
    "putFile",
    "deleteFile",
    "getServer",
    "updateLog",
    "fetchLog",
    "touchFile",
    "getVomsAttr",
    "putEventPickingRequest",
    "getAttr",
    "uploadLog",
    "put_checkpoint",
    "delete_checkpoint",
    "put_file_recovery_request",
    "put_workflow_request",
]

allowedMethods += ["datasetCompleted", "updateFileStatusInDisp"]

allowedMethods += [
    "getJob",
    "updateJob",
    "getStatus",
    "genPilotToken",
    "getEventRanges",
    "updateEventRange",
    "getKeyPair",
    "updateEventRanges",
    "getDNsForS3",
    "getProxy",
    "getCommands",
    "ackCommands",
    "checkJobStatus",
    "checkEventsAvailability",
    "updateJobsInBulk",
    "getResourceTypes",
    "updateWorkerPilotStatus",
    "get_max_worker_id",
    "get_events_status",
]

allowedMethods += [
    "submitJobs",
    "getJobStatus",
    "queryPandaIDs",
    "killJobs",
    "reassignJobs",
    "getJobStatistics",
    "getJobStatisticsPerSite",
    "resubmitJobs",
    "queryLastFilesInDataset",
    "getPandaIDsSite",
    "getJobsToBeUpdated",
    "updateProdDBUpdateTimes",
    "runTaskAssignment",
    "getAssigningTask",
    "getSiteSpecs",
    "getCloudSpecs",
    "seeCloudTask",
    "queryJobInfoPerCloud",
    "registerProxyKey",
    "getProxyKey",
    "getJobIDsInTimeRange",
    "getPandIDsWithJobID",
    "getFullJobStatus",
    "getJobStatisticsForBamboo",
    "getNUserJobs",
    "addSiteAccess",
    "listSiteAccess",
    "getFilesInUseForAnal",
    "updateSiteAccess",
    "getPandaClientVer",
    "getSlimmedFileInfoPandaIDs",
    "getQueuedAnalJobs",
    "getHighestPrioJobStat",
    "getActiveDatasets",
    "setCloudTaskByUser",
    "getSerialNumberForGroupJob",
    "checkMergeGenerationStatus",
    "getNumPilots",
    "retryFailedJobsInActive",
    "getJobStatisticsWithLabel",
    "getPandaIDwithJobExeID",
    "getJobStatisticsPerUserSite",
    "getDisInUseForAnal",
    "getLFNsInUseForAnal",
    "getScriptOfflineRunning",
    "setDebugMode",
    "insertSandboxFileInfo",
    "checkSandboxFile",
    "changeJobPriorities",
    "insertTaskParams",
    "killTask",
    "finishTask",
    "getJediTasksInTimeRange",
    "getJediTaskDetails",
    "retryTask",
    "getRetryHistory",
    "changeTaskPriority",
    "reassignTask",
    "changeTaskAttributePanda",
    "pauseTask",
    "resumeTask",
    "increaseAttemptNrPanda",
    "killUnfinishedJobs",
    "changeTaskSplitRulePanda",
    "changeTaskModTimePanda",
    "avalancheTask",
    "getPandaIDsWithTaskID",
    "reactivateTask",
    "getTaskStatus",
    "reassignShare",
    "listTasksInShare",
    "getTaskParamsMap",
    "updateWorkers",
    "harvesterIsAlive",
    "reportWorkerStats",
    "reportWorkerStats_jobtype",
    "addHarvesterDialogs",
    "getJobStatisticsPerSiteResource",
    "setNumSlotsForWP",
    "reloadInput",
    "enableJumboJobs",
    "updateServiceMetrics",
    "getUserJobMetadata",
    "getJumboJobDatasets",
    "getGShareStatus",
    "sweepPQ",
    "get_job_statistics_per_site_label_resource",
    "relay_idds_command",
    "send_command_to_job",
    "execute_idds_workflow_command",
    "set_user_secret",
    "get_user_secrets",
    "get_ban_users",
    "get_files_in_datasets",
    "release_task",
]


# FastCGI/WSGI entry
if panda_config.useFastCGI or panda_config.useWSGI:
    import os

    from pandacommon.pandalogger.LogWrapper import LogWrapper
    from pandacommon.pandalogger.PandaLogger import PandaLogger
    from werkzeug.datastructures import CombinedMultiDict, EnvironHeaders
    from werkzeug.formparser import parse_form_data

    if panda_config.token_authType is None:
        pass
    elif panda_config.token_authType == "scitokens":
        import scitokens
    else:
        from pandaserver.srvcore.oidc_utils import TokenDecoder

        token_decoder = TokenDecoder()

    # logger
    _logger = PandaLogger().getLogger("Entry")

    # ban list
    if panda_config.nDBConnection != 0:
        # get ban list directly from the database
        ban_user_list = CoreUtils.CachedObject("ban_list", 600, taskBuffer.get_ban_users, _logger)
    else:
        # get ban list from remote
        ban_user_list = CoreUtils.CachedObject("ban_list", 600, Client.get_ban_users, _logger)

    # dummy request object
    class DummyReq:
        def __init__(self, env, tmp_log):
            # environ
            self.subprocess_env = env
            # header
            self.headers_in = {}
            # authentication
            self.authenticated = True
            # message
            self.message = None
            # content-length
            if "CONTENT_LENGTH" in self.subprocess_env:
                self.headers_in["content-length"] = self.subprocess_env["CONTENT_LENGTH"]
            # tokens
            try:
                if panda_config.token_authType in ["scitokens", "oidc"] and "HTTP_AUTHORIZATION" in env:
                    serialized_token = env["HTTP_AUTHORIZATION"].split()[1]
                    role = None
                    if panda_config.token_authType == "oidc":
                        self.authenticated = False
                        if "HTTP_ORIGIN" in env:
                            vo = env["HTTP_ORIGIN"]
                            vo_group = vo.replace(":", ".")
                        else:
                            vo = None
                            vo_group = ""
                        token = token_decoder.deserialize_token(serialized_token, panda_config.auth_config, vo, tmp_log)
                        # extract role
                        if vo:
                            vo = token["vo"].split(":")[0]
                            vo = vo.split(".")[0]
                            if vo != token["vo"]:
                                role = token["vo"].split(":")[-1]
                                role = role.split(".")[-1]
                        # check vo
                        if vo not in panda_config.auth_policies:
                            self.message = f"Unknown vo : {vo}"
                            tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                        else:
                            # robot
                            if vo_group in panda_config.auth_vo_dict and "robot_ids" in panda_config.auth_vo_dict[vo_group]:
                                robot_ids = [i for i in panda_config.auth_vo_dict[vo_group].get("robot_ids").split(",") if i]
                                if token["sub"] in robot_ids:
                                    if "groups" not in token:
                                        if role:
                                            token["groups"] = [f"{vo}/{role}"]
                                        else:
                                            token["groups"] = [f"{vo}"]
                                    if "name" not in token:
                                        token["name"] = f"robot {role}"
                            # check role
                            if role:
                                if f"{vo}/{role}" not in token["groups"]:
                                    self.message = f"Not a member of the {vo}/{role} group"
                                    tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                                else:
                                    self.subprocess_env["PANDA_OIDC_VO"] = vo
                                    self.subprocess_env["PANDA_OIDC_GROUP"] = role
                                    self.subprocess_env["PANDA_OIDC_ROLE"] = role
                                    self.authenticated = True
                            else:
                                for member_string, member_info in panda_config.auth_policies[vo]:
                                    if member_string in token["groups"]:
                                        self.subprocess_env["PANDA_OIDC_VO"] = vo
                                        self.subprocess_env["PANDA_OIDC_GROUP"] = member_info["group"]
                                        self.subprocess_env["PANDA_OIDC_ROLE"] = member_info["role"]
                                        self.authenticated = True
                                        break
                                if not self.authenticated:
                                    self.message = f"Not a member of the {vo} group"
                                    tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                    else:
                        token = scitokens.SciToken.deserialize(serialized_token, audience=panda_config.token_audience)
                    # check issuer
                    if "iss" not in token:
                        self.message = "Issuer is undefined in the token"
                        tmp_log.error(self.message)
                    else:
                        if panda_config.token_authType == "scitokens":
                            items = token.claims()
                        else:
                            items = token.items()
                        for c, v in items:
                            self.subprocess_env[f"PANDA_OIDC_CLAIM_{str(c)}"] = str(v)
                        # use sub and scope as DN and FQAN
                        if "SSL_CLIENT_S_DN" not in self.subprocess_env:
                            if "name" in token:
                                self.subprocess_env["SSL_CLIENT_S_DN"] = " ".join([t[:1].upper() + t[1:].lower() for t in str(token["name"]).split()])
                                if "preferred_username" in token:
                                    self.subprocess_env["SSL_CLIENT_S_DN"] += f"/CN=nickname:{token['preferred_username']}"
                            else:
                                self.subprocess_env["SSL_CLIENT_S_DN"] = str(token["sub"])
                            i = 0
                            for scope in token.get("scope", "").split():
                                if scope.startswith("role:"):
                                    self.subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = "VOMS " + str(scope.split(":")[-1])
                                    i += 1
                            if role:
                                self.subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = f"VOMS /{vo}/Role={role}"
                                i += 1
            except Exception as e:
                self.message = f"Corrupted token. {str(e)}"
                tmp_log.debug(f"Origin: {env.get('HTTP_ORIGIN', None)}, Token: {env.get('HTTP_AUTHORIZATION', None)}\n{traceback.format_exc()}")

        # get remote host
        def get_remote_host(self):
            if "REMOTE_HOST" in self.subprocess_env:
                return self.subprocess_env["REMOTE_HOST"]
            return ""

        # accept json
        def acceptJson(self):
            try:
                if "HTTP_ACCEPT" in self.subprocess_env:
                    return "application/json" in self.subprocess_env["HTTP_ACCEPT"]
            except Exception:
                pass
            return False

    # application
    def application(environ, start_response):
        # get method name from environment
        method_name = ""
        if "SCRIPT_NAME" in environ:
            method_name = environ["SCRIPT_NAME"].split("/")[-1]

        tmp_log = LogWrapper(_logger, f"PID={os.getpid()} {method_name}", seeMem=True)
        cont_length = int(environ.get("CONTENT_LENGTH", 0))
        json_body = environ.get("CONTENT_TYPE", None) == "application/json"
        tmp_log.debug(f"start content-length={cont_length} json={json_body}")

        start_time = datetime.datetime.utcnow()
        return_type = None

        # check method name is allowed, otherwise return 403
        if method_name not in allowedMethods:
            error_message = f"{method_name} is forbidden"
            tmp_log.error(error_message)
            start_response("403 Forbidden", [("Content-Type", "text/plain")])
            return [f"ERROR : {error_message}".encode()]

        else:
            # get method object
            tmp_method = None
            try:
                tmp_method = globals()[method_name]
            except Exception:
                pass
            # object not found
            if tmp_method is None:
                tmp_log.error("is undefined")
                exec_result = "False"
            else:
                body = b""
                try:
                    # dummy request object
                    dummy_request = DummyReq(environ, tmp_log)
                    if not dummy_request.authenticated:
                        error_message = f"Token authentication failed. {dummy_request.message}"
                        tmp_log.error(error_message)
                        start_response("403 Forbidden", [("Content-Type", "text/plain")])
                        return [f"ERROR : {error_message}".encode()]

                    username = dummy_request.subprocess_env.get("SSL_CLIENT_S_DN", None)
                    if username:
                        username = CoreUtils.clean_user_id(username)
                        if username in ban_user_list:
                            error_message = f"{username} is banned"
                            tmp_log.error(error_message)
                            start_response("403 Forbidden", [("Content-Type", "text/plain")])
                            return [f"ERROR : {error_message}".encode()]

                    # read contents
                    while cont_length > 0:
                        chunk = environ["wsgi.input"].read(min(cont_length, 1024 * 1024))
                        if not chunk:
                            break
                        cont_length -= len(chunk)
                        body += chunk
                    if cont_length > 0:
                        raise OSError(f"partial read from client. {cont_length} bytes remaining")
                    if not json_body:
                        environ["wsgi.input"] = io.BytesIO(body)
                        environ["CONTENT_LENGTH"] = str(len(body))
                        environ["wsgi.headers"] = EnvironHeaders(environ)

                        # Parse form data. Combine the form (string fields) and the files (file uploads) into a single object
                        stream, form, files = parse_form_data(environ)
                        tmp_log.debug(f"form: {form} files: {files}")
                        tmp_params = CombinedMultiDict([form, files])
                        tmp_log.debug(f"params: {params}")

                        # convert to map
                        params = {}
                        for tmp_key in combined:
                            key = tmp_key.decode()
                            params[key] = tmp_params[tmp_key][0].decode()
                        tmp_log.debug(f"params: {params}")
                    else:
                        # json
                        body = gzip.decompress(body)
                        params = json.loads(body)
                        # patch for True/False
                        for k in list(params):
                            if params[k] is True:
                                params[k] = "True"
                            elif params[k] is False:
                                params[k] = "False"
                    if panda_config.entryVerbose:
                        tmp_log.debug(f"with {str(list(params))}")
                    param_list = [dummy_request]

                    # exec
                    exec_result = tmp_method(*param_list, **params)

                    # extract return type
                    if isinstance(exec_result, dict):
                        return_type = exec_result["type"]
                        exec_result = exec_result["content"]
                    # convert bool to string
                    if exec_result in [True, False]:
                        exec_result = str(exec_result)

                except Exception as e:
                    tmp_log.error(f"execution failure : {str(e)}\n {traceback.format_exc()}")
                    if hasattr(panda_config, "dumpBadRequest") and panda_config.dumpBadRequest:
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, prefix="req_dump_") as f:
                                environ["WSGI_INPUT_DUMP"] = f.name
                                f.write(body)
                                os.chmod(f.name, 0o775)
                        except Exception:
                            tmp_log.error(traceback.format_exc())
                            pass
                    error_string = ""
                    for tmp_key in environ:
                        tmp_value = environ[tmp_key]
                        error_string += f"{tmp_key} : {str(tmp_value)}\n"
                    # tmp_log.error(error_string)

                    # return internal server error
                    start_response("500 INTERNAL SERVER ERROR", [("Content-Type", "text/plain")])
                    # force kill to release memory
                    if isinstance(e, OSError):
                        tmp_log.warning("force restart due")
                        os.kill(os.getpid(), signal.SIGINT)
                    return [str(e).encode()]

        if panda_config.entryVerbose:
            tmp_log.debug("done")

        duration = datetime.datetime.utcnow() - start_time
        tmp_log.info("exec_time=%s.%03d sec, return len=%s B" % (duration.seconds, duration.microseconds / 1000, len(str(exec_result))))

        if exec_result == pandaserver.taskbuffer.ErrorCode.EC_NotFound:
            start_response("404 Not Found", [("Content-Type", "text/plain")])
            return ["not found".encode()]

        if exec_result == pandaserver.taskbuffer.ErrorCode.EC_Forbidden:
            start_response("403 Forbidden", [("Content-Type", "text/plain")])
            return ["forbidden".encode()]

        if return_type == "json":
            start_response("200 OK", [("Content-Type", "application/json")])
        else:
            start_response("200 OK", [("Content-Type", "text/plain")])

        if isinstance(exec_result, str):
            exec_result = exec_result.encode()

        return [exec_result]

    # start server
    if panda_config.useFastCGI:
        from flup.server.fcgi import WSGIServer

        WSGIServer(application, multithreaded=False).run()
