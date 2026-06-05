"""
API endpoints for submitting and polling async processing requests.
Currently supports grep (rg / zgrep) on log files; extensible to other request types.
"""

import json
import os
import uuid
from threading import Lock
from typing import Any, Dict

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    generate_response,
    get_dn,
    has_production_role,
    request_validation,
)
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_async_process")

global_task_buffer = None
global_dispatch_parameter_cache = None

global_lock = Lock()


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """Initialize the task buffer. Must be called before any other method."""
    with global_lock:
        global global_task_buffer
        global_task_buffer = task_buffer

        global global_dispatch_parameter_cache
        global_dispatch_parameter_cache = CoreUtils.CachedObject("dispatcher_params", 60 * 10, task_buffer.get_special_dispatch_params, _logger)


def _is_authorized_with_allowlist(req):
    """Check whether the caller's DN is in the allowAsyncRequest list."""
    compact_dn = clean_user_id(get_dn(req))
    global global_dispatch_parameter_cache
    with global_lock:
        global_dispatch_parameter_cache.update()
    if global_dispatch_parameter_cache is None:
        return False, "authorization cache not ready"
    allowed = global_dispatch_parameter_cache.get("allowAsyncRequest", [])
    if compact_dn not in allowed:
        return False, f"'{compact_dn}' is not authorized"
    return True, f"'{compact_dn}' is authorized"


# valid access levels for reading back request results
ACCESS_LEVELS = ("owner", "production", "anyone")

# bounds for the sleep+echo request
MAX_SLEEP_SECONDS = 60  # cap below the processor's subprocess timeout (240s)
MAX_MESSAGE_LENGTH = 1000  # cap echoed message size


def _set_owner_info(parameters: dict, req, access: str = "owner") -> dict:
    """
    Embed the requester and access level into a request's parameters dict.
    Used by submit_* endpoints when building parameters_json.

    Args:
        parameters(dict): the request's parameters dict to be augmented in place
        req(PandaRequest): request object, used to derive the requester's compact DN
        access(str): access level controlling who may read results; one of
            "owner", "production", "anyone" (default "owner")

    Returns:
        dict: the same parameters dict, with "requester" and "access" set
    """
    parameters["requester"] = clean_user_id(get_dn(req))
    parameters["access"] = access
    return parameters


def _is_authorized_to_read(req, req_row):
    """
    Authorize the caller to read a request's results based on its access level.

    Args:
        req(PandaRequest): request object, used to derive the caller's compact DN
            and (for the "production" level) the production role
        req_row(dict): the row dict from global_task_buffer.get_async_request();
            only req_row["parameters"] (the JSON holding requester/access) is used

    Returns:
        tuple[bool, str]: (authorized, message)
    """
    caller = clean_user_id(get_dn(req))
    try:
        params = json.loads(req_row["parameters"] or "{}")
    except json.JSONDecodeError:
        params = {}
    requester = params.get("requester")
    access = params.get("access", "owner")
    if access == "owner":
        authorized = caller == requester
    elif access == "production":
        authorized = caller == requester or has_production_role(req)
    else:  # "anyone"; any unknown value falls through to not authorized
        authorized = access == "anyone"
    if not authorized:
        return False, f"'{caller}' is not authorized to read results (access='{access}', requester='{requester}')"
    return True, f"'{caller}' is authorized (access='{access}')"


@request_validation(_logger, secure=True, request_method="POST")
def submit_grep_request(
    req: PandaRequest,
    pattern: str,
    log_filename: str,
    service_name: str = None,
    machine_name: str = None,
) -> Dict[str, Any]:
    """
    Submit a grep request to be processed asynchronously on the target service or machine.

    API details:
        HTTP Method: POST
        Path: /v1/async_process/submit_grep_request

    Args:
        req(PandaRequest): request object
        pattern(str): grep pattern to search for
        log_filename(str): filename (not full path) of the log file under panda_config.logdir
        service_name(str): target service (e.g. "server", "jedi"); mutually exclusive with machine_name
        machine_name(str): target specific machine hostname; mutually exclusive with service_name

    Returns:
        dict: {"success": bool, "message": str, "data": {"request_id": str}}
    """
    tmp_logger = LogWrapper(_logger, "submit_grep_request")
    tmp_logger.debug("Start")

    ok, msg = _is_authorized_with_allowlist(req)
    if not ok:
        tmp_logger.warning(msg)
        return generate_response(False, msg)
    tmp_logger.debug(msg)

    if bool(service_name) == bool(machine_name):
        msg = "exactly one of service_name or machine_name must be provided"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # prevent directory traversal — only bare filenames are accepted
    if os.sep in log_filename or ".." in log_filename:
        msg = "invalid log_filename: must not contain path separators"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # log_filename is expected to be something like "panda-*.log" or "panda-*.log.*.gz"
    if not (log_filename.startswith("panda-") and (log_filename.endswith(".log") or log_filename.endswith(".gz"))):
        msg = "invalid log_filename: must start with 'panda-' and end with '.log' or '.gz'"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # determine expected machines from liveness snapshot
    if service_name:
        expected = global_task_buffer.get_alive_machines(service_name)
        if not expected:
            msg = f"no alive machines found for service '{service_name}'"
            tmp_logger.warning(msg)
            return generate_response(False, msg)
    else:
        alive = global_task_buffer.get_alive_machines(machine_name)
        # get_alive_machines matches on service_name; for a specific machine check heartbeat directly
        expected = [machine_name]
        # warn but don't block — machine may have started after last heartbeat window
        if not alive:
            msg = f"machine '{machine_name}' has no recent heartbeat; request submitted anyway"
            tmp_logger.warning(msg)

    request_id = str(uuid.uuid4())
    parameters = _set_owner_info({"pattern": pattern, "log_filename": log_filename}, req)  # grep results stay owner-only
    parameters_json = json.dumps(parameters)
    expected_machines_json = json.dumps(expected)

    ok = global_task_buffer.insert_async_request(
        request_id,
        "grep",
        parameters_json,
        service_name,
        machine_name,
        expected_machines_json,
    )
    if not ok:
        msg = "failed to insert request into DB"
        tmp_logger.error(msg)
        return generate_response(False, msg)

    tmp_logger.debug(f"Done request_id={request_id}")
    return generate_response(True, "", {"request_id": request_id})


@request_validation(_logger, secure=True, production=True, request_method="POST")
def submit_sleep_echo_request(
    req: PandaRequest,
    service_name: str,
    message: str,
    seconds: int = 1,
) -> Dict[str, Any]:
    """
    Submit a sleep+echo request, run on any one machine in the target service.
    Results are readable by the requester or any production-role caller (access="production").

    API details:
        HTTP Method: POST
        Path: /v1/async_process/submit_sleep_echo_request

    Args:
        req(PandaRequest): request object
        service_name(str): target service; the job runs on exactly one of its alive machines
        message(str): text echoed back as the result
        seconds(int): seconds to sleep before echoing (0..MAX_SLEEP_SECONDS)

    Returns:
        dict: {"success": bool, "message": str, "data": {"request_id": str}}
    """
    tmp_logger = LogWrapper(_logger, "submit_sleep_echo_request")
    tmp_logger.debug("Start")

    if not 0 <= seconds <= MAX_SLEEP_SECONDS:
        msg = f"invalid seconds: must be between 0 and {MAX_SLEEP_SECONDS}"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    if len(message) > MAX_MESSAGE_LENGTH:
        msg = f"invalid message: must be at most {MAX_MESSAGE_LENGTH} characters"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    if not service_name:
        msg = "service_name must be provided"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # the "any" job needs at least one alive machine in the service to run on
    expected = global_task_buffer.get_alive_machines(service_name)
    if not expected:
        msg = f"no alive machines found for service '{service_name}'"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    request_id = str(uuid.uuid4())
    parameters = _set_owner_info({"seconds": seconds, "message": message}, req, access="production")

    ok = global_task_buffer.insert_async_request(
        request_id,
        "sleep_echo",
        json.dumps(parameters),
        service_name,
        ANY_MACHINE,
        None,  # expected_machines auto-derived to ["any"] for the sentinel
    )
    if not ok:
        msg = "failed to insert request into DB"
        tmp_logger.error(msg)
        return generate_response(False, msg)

    tmp_logger.debug(f"Done request_id={request_id}")
    return generate_response(True, "", {"request_id": request_id})


@request_validation(_logger, secure=True, request_method="GET")
def get_result(req: PandaRequest, request_id: str) -> Dict[str, Any]:
    """
    Poll for the results of an async request.

    API details:
        HTTP Method: GET
        Path: /v1/async_process/get_result

    Args:
        req(PandaRequest): request object
        request_id(str): UUID returned by a submit_* endpoint

    Returns:
        dict: {
            "success": bool,
            "message": str,
            "data": {
                "overall_status": "complete" | "pending",
                "expected_machines": [str, ...],
                "results": [{"machine_name": str, "status": str, "result": str,
                              "truncated": int, "error_msg": str, "attempts": int,
                              "started_at": str, "finished_at": str,
                              "stderr": str, "return_code": int}, ...]
            }
        }
        overall_status is "complete" when all expected machines have a terminal result (done/failed).
    """
    tmp_logger = LogWrapper(_logger, f"get_result < request_id={request_id} >")
    tmp_logger.debug("Start")

    req_row = global_task_buffer.get_async_request(request_id)
    if req_row is None:
        msg = f"request_id '{request_id}' not found"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # authorize the caller to read the results based on the request's access level
    ok, msg = _is_authorized_to_read(req, req_row)
    if not ok:
        tmp_logger.warning(msg)
        return generate_response(False, msg)
    tmp_logger.debug(msg)

    results = global_task_buffer.get_async_results(request_id)

    expected = json.loads(req_row["expected_machines"] or "[]")
    responded = {r["machine_name"] for r in results if r["status"] in ("done", "failed")}
    overall_status = "complete" if expected and set(expected) <= responded else "pending"

    # serialize datetime objects to strings for JSON
    for r in results:
        for key in ("started_at", "finished_at"):
            if r[key] is not None:
                r[key] = str(r[key])

    tmp_logger.debug(f"Done overall_status={overall_status}")
    return generate_response(
        True,
        "",
        {
            "overall_status": overall_status,
            "expected_machines": expected,
            "results": results,
        },
    )
