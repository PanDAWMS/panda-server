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
from collections import defaultdict
from urllib.parse import parse_qsl

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow
from pandacommon.pandautils.thread_utils import GenericThread
from werkzeug.datastructures import CombinedMultiDict, EnvironHeaders
from werkzeug.formparser import parse_form_data

import pandaserver.taskbuffer.ErrorCode
from pandaserver.api.v1 import credential_management_api as cred_api_v1
from pandaserver.api.v1 import data_carousel_api as data_carousel_api_v1
from pandaserver.api.v1 import event_api as event_api_v1
from pandaserver.api.v1 import file_server_api as file_server_api_v1
from pandaserver.api.v1 import harvester_api as harvester_api_v1
from pandaserver.api.v1 import idds_api as idds_api_v1
from pandaserver.api.v1 import job_api as job_api_v1
from pandaserver.api.v1 import metaconfig_api as metaconfig_api_v1
from pandaserver.api.v1 import pilot_api as pilot_api_v1
from pandaserver.api.v1 import statistics_api as statistics_api_v1
from pandaserver.api.v1 import system_api as system_api_v1
from pandaserver.api.v1 import task_api as task_api_v1
from pandaserver.api.v1.common import extract_allowed_methods
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
    getJobStatisticsPerSite,
    getJobStatus,
    getJumboJobDatasets,
    getPandaIDsWithTaskID,
    getScriptOfflineRunning,
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

_logger = PandaLogger().getLogger("Entry")

LATEST = "1"

# generate the allowed methods dynamically with all function names present in the API modules,
# excluding functions imported from other modules or the init_task_buffer function
cred_api_v1_methods = extract_allowed_methods(cred_api_v1)
data_carousel_api_v1_methods = extract_allowed_methods(data_carousel_api_v1)
event_api_v1_methods = extract_allowed_methods(event_api_v1)
file_server_api_v1_methods = extract_allowed_methods(file_server_api_v1)
harvester_api_v1_methods = extract_allowed_methods(harvester_api_v1)
idds_api_v1_methods = extract_allowed_methods(idds_api_v1)
job_api_v1_methods = extract_allowed_methods(job_api_v1)
metaconfig_api_v1_methods = extract_allowed_methods(metaconfig_api_v1)
pilot_api_v1_methods = extract_allowed_methods(pilot_api_v1)
statistics_api_v1_methods = extract_allowed_methods(statistics_api_v1)
system_api_v1_methods = extract_allowed_methods(system_api_v1)
task_api_v1_methods = extract_allowed_methods(task_api_v1)

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

if panda_config.nDBConnection != 0:
    # initialize all the API modules
    cred_api_v1.init_task_buffer(taskBuffer)
    data_carousel_api_v1.init_task_buffer(taskBuffer)
    event_api_v1.init_task_buffer(taskBuffer)
    file_server_api_v1.init_task_buffer(taskBuffer)
    harvester_api_v1.init_task_buffer(taskBuffer)
    # IDDS API does not need to be initialized. idds_server_api_v1.init_task_buffer(taskBuffer)
    job_api_v1.init_task_buffer(taskBuffer)
    metaconfig_api_v1.init_task_buffer(taskBuffer)
    pilot_api_v1.init_task_buffer(taskBuffer)
    statistics_api_v1.init_task_buffer(taskBuffer)
    # System API does not need to be initialized. system_api_v1.init_task_buffer(taskBuffer)
    task_api_v1.init_task_buffer(taskBuffer)

    # initialize JobDispatcher
    jobDispatcher.init(taskBuffer)

    # initialize UserIF
    userIF.init(taskBuffer)

# ban list
if panda_config.nDBConnection != 0:
    # get ban list directly from the database
    ban_user_list = CoreUtils.CachedObject("ban_list", 600, taskBuffer.get_ban_users, _logger)
else:
    # get ban list from remote
    ban_user_list = CoreUtils.CachedObject("ban_list", 600, Client.get_banned_users, _logger)


def pre_validate_request(panda_request):
    # check authentication
    if not panda_request.authenticated:
        error_message = f"Token authentication failed. {panda_request.message}"
        return error_message

    # check list of banned users
    username = panda_request.subprocess_env.get("SSL_CLIENT_S_DN", None)
    if username:
        username = CoreUtils.clean_user_id(username)
        if username in ban_user_list:
            error_message = f"{username} is banned"
            return error_message

    return None


def read_body(environ, content_length):
    # read body contents
    body = b""
    while content_length > 0:
        chunk = environ["wsgi.input"].read(min(content_length, 1024 * 1024))
        if not chunk:
            break
        content_length -= len(chunk)
        body += chunk
    if content_length > 0:
        # OSError is caught in the main function and forces killing the process
        raise OSError(f"partial read from client. {content_length} bytes remaining")

    return body


def parse_qsl_parameters(environ, body, request_method):
    # parse parameters for non-json requests
    environ["wsgi.input"] = io.BytesIO(body)
    environ["CONTENT_LENGTH"] = str(len(body))
    environ["wsgi.headers"] = EnvironHeaders(environ)

    # In the case of GET, HEAD methods we need to parse the query string list in the URL looking for parameters
    if request_method in ["GET", "HEAD"]:
        # Parse the query string list in the URL looking for parameters. Repeated query parameters submitted multiple times will be appended to a list
        results_tmp = defaultdict(list)
        parameter_list = parse_qsl(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        for key, value in parameter_list:
            results_tmp[key].append(value)

        params = {key: values[0] if len(values) == 1 else values for key, values in results_tmp.items()}

    # In the case of POST, PUT methods we need to parse the form data
    else:
        # Parse form data. Combine the form (string fields) and the files (file uploads) into a single object
        _, form, files = parse_form_data(environ)

        # Combine the form and files into a single dictionary
        params = dict(CombinedMultiDict([form, files]))
    return params


def parse_json_parameters_legacy(body):
    # parse parameters for json requests
    # decompress the body, this was done without checking the content encoding
    body = gzip.decompress(body)

    # de-serialize the body and patch for True/False
    params = json.loads(body)
    for key in list(params):
        if params[key] is True:
            params[key] = "True"
        elif params[key] is False:
            params[key] = "False"
    return params


def parse_json_parameters(body, content_encoding):
    # parse parameters for json requests
    # decompress the body if necessary
    if content_encoding == "gzip":
        body = gzip.decompress(body)

    # de-serialize the body
    params = json.loads(body)

    return params


def parse_parameters(api_module, json_app, json_body, content_encoding, environ, body, request_method):
    # parse parameters with the new refactored API
    if is_new_api(api_module):
        # the request specifies json and it's a PUT/POST request with the data in the body
        if json_body:
            return parse_json_parameters(body, content_encoding)
        else:
            return parse_qsl_parameters(environ, body, request_method)

    # parse parameters conserving the legacy API logic
    else:
        # parse parameters for json requests with the legacy API, even for GET requests
        if json_app:
            return parse_json_parameters_legacy(body)
        # parse parameters for non-json requests
        else:
            return parse_qsl_parameters(environ, body, request_method)


def is_new_api(api_module):
    return api_module != "panda"


def parse_script_name(environ):
    method_name = ""
    api_module = ""
    version = "v0"

    if "SCRIPT_NAME" in environ:
        script_name = environ["SCRIPT_NAME"]
        fields = script_name.split("/")

        # Legacy API: /server/panda/<method>
        if script_name.startswith("/server/panda/") and len(fields) == 4:
            api_module = "panda"
            method_name = fields[-1]

        # Refactored API: /api/<version>/<module>/<method>
        elif script_name.startswith("/api/") and len(fields) == 5:
            method_name = fields[-1]
            api_module = fields[-2]
            version = fields[-3]
            if version == "latest":
                version = LATEST

        else:
            _logger.error(f"Could not parse script name: {script_name}")

    return method_name, api_module, version


def module_mapping(version, api_module):
    mapping = {
        "v0": {"panda": {"module": None, "allowed_methods": allowed_methods}},  # legacy API uses globals instead of a particular module
        "v1": {
            "creds": {"module": cred_api_v1, "allowed_methods": cred_api_v1_methods},
            "data_carousel": {"module": data_carousel_api_v1, "allowed_methods": data_carousel_api_v1_methods},
            "event": {"module": event_api_v1, "allowed_methods": event_api_v1_methods},
            "file_server": {"module": file_server_api_v1, "allowed_methods": file_server_api_v1_methods},
            "harvester": {"module": harvester_api_v1, "allowed_methods": harvester_api_v1_methods},
            "idds": {"module": idds_api_v1, "allowed_methods": idds_api_v1_methods},
            "job": {"module": job_api_v1, "allowed_methods": job_api_v1_methods},
            "metaconfig": {"module": metaconfig_api_v1, "allowed_methods": metaconfig_api_v1_methods},
            "pilot": {"module": pilot_api_v1, "allowed_methods": pilot_api_v1_methods},
            "statistics": {"module": statistics_api_v1, "allowed_methods": statistics_api_v1_methods},
            "system": {"module": system_api_v1, "allowed_methods": system_api_v1_methods},
            "task": {"module": task_api_v1, "allowed_methods": task_api_v1_methods},
        },
    }
    try:
        return mapping[version][api_module]
    except KeyError:
        _logger.error(f"Could not find module {api_module} in API version {version}")
        return None


def validate_method(method_name, api_module, version):
    # We are in the refactored API and the method is not in the specific allowed list
    mapping = module_mapping(version, api_module)
    if mapping and method_name in mapping["allowed_methods"]:
        return True

    return False


# Encoder: convert datetime → ISO string with a marker
def encode_special_cases(obj):
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": obj.isoformat()}
    raise TypeError(f"Type not serializable for {obj} ({type(obj)})")


# This is the starting point for all WSGI requests
def application(environ, start_response):
    # Parse the script name to retrieve method, module and version
    method_name, api_module, version = parse_script_name(environ)

    tmp_log = LogWrapper(_logger, f"PID={os.getpid()} module={api_module} method={method_name} version={version}", seeMem=True)
    cont_length = int(environ.get("CONTENT_LENGTH", 0))
    request_method = environ.get("REQUEST_METHOD", None)  # GET, POST, PUT, DELETE

    # see if we are on the new or old APIs
    new_api = is_new_api(api_module)

    # json app means the content type is application/json,
    # while json body requires additionally to be a PUT or POST request, where the body is json encoded
    if new_api:
        json_app = environ.get("CONTENT_TYPE", None) == "application/json" or environ.get("HTTP_ACCEPT", None) == "application/json"
    else:
        json_app = environ.get("CONTENT_TYPE", None) == "application/json"
    json_body = environ.get("CONTENT_TYPE", None) == "application/json" and request_method in ["PUT", "POST"]

    # Content encoding specifies whether the body is compressed through gzip or others.
    # No encoding usually means the body is not compressed
    content_encoding = environ.get("HTTP_CONTENT_ENCODING")

    tmp_log.debug(f"""start content-length={cont_length} json={json_app} origin={environ.get("HTTP_ORIGIN", None)}""")

    start_time = naive_utcnow()
    return_type = None

    # check method name is allowed, otherwise return 403
    if not validate_method(method_name, api_module, version):
        error_message = f"method {method_name} is forbidden"
        tmp_log.error(error_message)
        start_response("403 Forbidden", [("Content-Type", "text/plain")])
        return [f"ERROR : {error_message}".encode()]

    # get the method object to be executed
    try:
        if new_api:
            module = module_mapping(version, api_module)["module"]
            tmp_method = getattr(module, method_name)
        else:
            tmp_method = globals()[method_name]
    except Exception:
        error_message = f"method {method_name} is undefined in {api_module} {version}"
        tmp_log.error(error_message)
        start_response("500 INTERNAL SERVER ERROR", [("Content-Type", "text/plain")])
        return ["ERROR : {error_message}".encode()]

    try:
        # generate a request object with the environment and the logger
        panda_request = PandaRequest(environ, tmp_log)

        # pre-validate the request
        error_message = pre_validate_request(panda_request)
        if error_message:
            tmp_log.error(error_message)
            start_response("403 Forbidden", [("Content-Type", "text/plain")])
            return [f"ERROR : {error_message}".encode()]

        # read the body of the request
        body = read_body(environ, cont_length)

        # parse the parameters
        params = parse_parameters(api_module, json_app, json_body, content_encoding, environ, body, request_method)

        if panda_config.entryVerbose:
            tmp_log.debug(f"with {str(list(params))}")

        # execute the method, passing along the request and the decoded parameters
        param_list = [panda_request]
        exec_result = tmp_method(*param_list, **params)

        # extract return type
        if isinstance(exec_result, dict) and "type" in exec_result and "content" in exec_result:
            return_type = exec_result["type"]
            exec_result = exec_result["content"]

        # convert bool to string
        if exec_result in [True, False]:
            exec_result = str(exec_result)

        # convert the response to JSON or str depending on HTTP_ACCEPT and CONTENT_TYPE
        if new_api:
            if json_app:
                exec_result = json.dumps(exec_result, default=encode_special_cases)
            elif not isinstance(exec_result, str):
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
        error_string = "\n".join(f"{tmp_key} : {str(tmp_value)}" for tmp_key, tmp_value in environ.items())
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
    duration = naive_utcnow() - start_time
    tmp_log.info(
        f"exec_time={duration.seconds}.{duration.microseconds // 1000:03d} sec, return_type={return_type} real_type={type(exec_result).__name__} len={len(str(exec_result))} B"
    )

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
