#!/usr/bin/python

"""
entry point

"""

import datetime
import gzip
import io
import json
import os
import signal
import sys
import tempfile
import traceback
from urllib.parse import parse_qsl

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from werkzeug.datastructures import CombinedMultiDict, EnvironHeaders
from werkzeug.formparser import parse_form_data

import pandaserver.taskbuffer.ErrorCode
from pandaserver.config import panda_config

# pylint: disable=W0611
from pandaserver.jobdispatcher.JobDispatcher import (
    ackCommands,
    checkEventsAvailability,
    checkJobStatus,
    get_access_token,
    get_events_status,
    get_max_worker_id,
    get_token_key,
    getCommands,
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

# IMPORTANT: Add any new methods here to allow them to be called from the web I/F
from pandaserver.srvcore.allowed_methods import allowed_methods
from pandaserver.srvcore.panda_request import PandaRequest
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
    avalancheTask,
    changeTaskAttributePanda,
    changeTaskModTimePanda,
    changeTaskPriority,
    changeTaskSplitRulePanda,
    checkSandboxFile,
    enableJumboJobs,
    execute_idds_workflow_command,
    finishTask,
    get_ban_users,
    get_files_in_datasets,
    get_job_statistics_per_site_label_resource,
    get_user_secrets,
    getFullJobStatus,
    getJediTaskDetails,
    getJediTasksInTimeRange,
    getJobStatistics,
    getJobStatisticsForBamboo,
    getJobStatisticsPerSite,
    getJobStatisticsPerSiteResource,
    getJobStatus,
    getJumboJobDatasets,
    getPandaClientVer,
    getPandaIDsWithTaskID,
    getScriptOfflineRunning,
    getSiteSpecs,
    getTaskParamsMap,
    getTaskStatus,
    getUserJobMetadata,
    getWorkerStats,
    harvesterIsAlive,
    increaseAttemptNrPanda,
    insertSandboxFileInfo,
    insertTaskParams,
    killJobs,
    killTask,
    killUnfinishedJobs,
    pauseTask,
    reactivateTask,
    reassignJobs,
    reassignShare,
    reassignTask,
    relay_idds_command,
    release_task,
    reloadInput,
    reportWorkerStats_jobtype,
    resumeTask,
    retryTask,
    send_command_to_job,
    set_user_secret,
    setDebugMode,
    setNumSlotsForWP,
    submitJobs,
    sweepPQ,
    updateServiceMetrics,
    updateWorkers,
    userIF,
)

# initialize oracledb using dummy connection
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

# initialize UserIF
if panda_config.nDBConnection != 0:
    userIF.init(taskBuffer)

# logger
_logger = PandaLogger().getLogger("Entry")

# ban list
if panda_config.nDBConnection != 0:
    # get ban list directly from the database
    ban_user_list = CoreUtils.CachedObject("ban_list", 600, taskBuffer.get_ban_users, _logger)
else:
    # get ban list from remote
    ban_user_list = CoreUtils.CachedObject("ban_list", 600, Client.get_ban_users, _logger)


# This is the starting point for all WSGI requests
def application(environ, start_response):
    # get method name from environment
    method_name = ""
    if "SCRIPT_NAME" in environ:
        method_name = environ["SCRIPT_NAME"].split("/")[-1]

    tmp_log = LogWrapper(_logger, f"PID={os.getpid()} {method_name}", seeMem=True)
    cont_length = int(environ.get("CONTENT_LENGTH", 0))
    json_body = environ.get("CONTENT_TYPE", None) == "application/json"
    tmp_log.debug(f"""start content-length={cont_length} json={json_body} origin={environ.get("HTTP_ORIGIN", None)}""")

    start_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return_type = None

    # check method name is allowed, otherwise return 403
    if method_name not in allowed_methods:
        error_message = f"{method_name} is forbidden"
        tmp_log.error(error_message)
        start_response("403 Forbidden", [("Content-Type", "text/plain")])
        return [f"ERROR : {error_message}".encode()]

    # get the method object to be executed
    try:
        tmp_method = globals()[method_name]
    except Exception:
        error_message = f"{method_name} is undefined"
        tmp_log.error(error_message)
        start_response("500 INTERNAL SERVER ERROR", [("Content-Type", "text/plain")])
        return ["ERROR : {error_message}".encode()]

    body = b""
    try:
        # generate a request object with the environment and the logger
        panda_request = PandaRequest(environ, tmp_log)

        # check authentication
        if not panda_request.authenticated:
            error_message = f"Token authentication failed. {panda_request.message}"
            tmp_log.error(error_message)
            start_response("403 Forbidden", [("Content-Type", "text/plain")])
            return [f"ERROR : {error_message}".encode()]

        # check ban list
        username = panda_request.subprocess_env.get("SSL_CLIENT_S_DN", None)
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

        # parse parameters for non-json requests
        if not json_body:
            environ["wsgi.input"] = io.BytesIO(body)
            environ["CONTENT_LENGTH"] = str(len(body))
            environ["wsgi.headers"] = EnvironHeaders(environ)

            # get request method
            request_method = environ.get("REQUEST_METHOD", None)

            # In the case of GET, HEAD methods we need to parse the query string list in the URL looking for parameters
            if request_method in ["GET", "HEAD"]:
                params = dict(parse_qsl(environ.get("QUERY_STRING", ""), keep_blank_values=True))

            # In the case of POST, PUT methods we need to parse the form data
            else:
                # Parse form data. Combine the form (string fields) and the files (file uploads) into a single object
                _, form, files = parse_form_data(environ)

                # Combine the form and files into a single dictionary
                params = dict(CombinedMultiDict([form, files]))

        # parse parameters for json requests
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
        param_list = [panda_request]

        # execute the method, passing along the request and the decoded parameters
        exec_result = tmp_method(*param_list, **params)

        # extract return type
        if isinstance(exec_result, dict):
            return_type = exec_result["type"]
            exec_result = exec_result["content"]

        # convert bool to string
        if exec_result in [True, False]:
            exec_result = str(exec_result)

    except Exception as exc:
        tmp_log.error(f"execution failure : {str(exc)}\n {traceback.format_exc()}")
        if hasattr(panda_config, "dumpBadRequest") and panda_config.dumpBadRequest:
            try:
                with tempfile.NamedTemporaryFile(delete=False, prefix="req_dump_") as file_object:
                    environ["WSGI_INPUT_DUMP"] = file_object.name
                    file_object.write(body)
                    os.chmod(file_object.name, 0o775)
            except Exception:
                tmp_log.error(traceback.format_exc())
                pass
        error_string = ""
        for tmp_key in environ:
            tmp_value = environ[tmp_key]
            error_string += f"{tmp_key} : {str(tmp_value)}\n"
        tmp_log.error(error_string)

        # return internal server error
        start_response("500 INTERNAL SERVER ERROR", [("Content-Type", "text/plain")])
        # force kill to release memory
        if isinstance(exc, OSError):
            tmp_log.warning("force restart due")
            os.kill(os.getpid(), signal.SIGINT)

        return [str(exc).encode()]

    if panda_config.entryVerbose:
        tmp_log.debug("done")

    # log execution time and return length
    duration = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - start_time
    tmp_log.info("exec_time=%s.%03d sec, return len=%s B" % (duration.seconds, duration.microseconds / 1000, len(str(exec_result))))

    # start the response and return result
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
