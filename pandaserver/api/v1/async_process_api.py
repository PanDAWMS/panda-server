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

from pandaserver.api.v1.common import generate_response, get_dn, request_validation
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.srvcore.panda_request import PandaRequest
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


def _is_authorized(req):
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

    ok, msg = _is_authorized(req)
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
    parameters_json = json.dumps({"pattern": pattern, "log_filename": log_filename, "requester": clean_user_id(get_dn(req))})
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


@request_validation(_logger, secure=True, request_method="GET")
def get_result(req: PandaRequest, request_id: str) -> Dict[str, Any]:
    """
    Poll for the results of an async request.

    API details:
        HTTP Method: GET
        Path: /v1/async_process/get_result

    Args:
        req(PandaRequest): request object
        request_id(str): UUID returned by submit_grep_request

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

    ok, msg = _is_authorized(req)
    if not ok:
        tmp_logger.warning(msg)
        return generate_response(False, msg)
    tmp_logger.debug(msg)

    req_row = global_task_buffer.get_async_request(request_id)
    if req_row is None:
        msg = f"request_id '{request_id}' not found"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

    # only the original requester may read back the results
    caller = clean_user_id(get_dn(req))
    try:
        requester = json.loads(req_row["parameters"] or "{}").get("requester")
    except json.JSONDecodeError:
        requester = None
    if caller != requester:
        msg = f"'{caller}' is not the requester '{requester}'"
        tmp_logger.warning(msg)
        return generate_response(False, msg)

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
