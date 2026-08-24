import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.api.v1.common import (
    MESSAGE_DATABASE,
    TIME_OUT,
    TimedMethod,
    generate_response,
    get_dn,
    get_fqan,
    has_production_role,
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
from pandaserver.workflow.workflow_core import WorkflowInterface
from pandaserver.workflow.workflow_native_utils import (
    RAW_TASK_PARAMS_STEP_TYPES,
    validate_workflow_description,
)
from pandaserver.workflow.workflow_parser import INLINE_DESCRIPTION_KEY

_logger = PandaLogger().getLogger("api_workflow")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None
global_wfif = None

# These global variables don't depend on DB access and can be initialized here
# global_proxy_cache = panda_proxy_cache.MyProxyInterface()
# global_token_cache = token_cache.TokenCache()


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer and other interfaces. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_wfif
    global_wfif = WorkflowInterface(global_task_buffer)


@request_validation(_logger, secure=True, production=False, request_method="POST")
def submit_workflow(req: PandaRequest, params: dict | str) -> dict:
    """
    Submit a PanDA native workflow as a raw request, with the description in a sandbox.

    This is the endpoint the pandaclient uses (see Client.submit_workflow). The description itself
    lives in an uploaded sandbox and is downloaded and parsed asynchronously, so nothing about it can
    be validated here. To submit a description inline instead, with no sandbox, use
    submit_workflow_description.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        params (dict|str): dictionary or JSON of parameters of the raw request

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """

    user_dn = get_dn(req)
    prodsourcelabel = "user"

    # FIXME: only for analysis temporarily
    # if has_production_role(req):
    #     prodsourcelabel = "managed"

    tmp_logger = LogWrapper(_logger, f'submit_workflow prodsourcelabel={prodsourcelabel} user_dn="{user_dn}" ')
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception as exc:
            message = f"Failed to parse params: {params} {str(exc)}"
            tmp_logger.error(message)
            return generate_response(success, message, data)

    workflow_id = global_wfif.register_workflow(prodsourcelabel, user_dn, raw_request_params=params)

    if workflow_id is not None:
        success = True
        data = {"workflow_id": workflow_id}
    else:
        message = "Failed to submit raw workflow request"

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


# Deprecated alias of submit_workflow, kept so that pandaclient versions which still target
# /v1/workflow/submit_workflow_raw_request keep working. It is intentionally undecorated: it is
# routed because extract_allowed_methods picks up every public module-level function, and the
# validation happens in submit_workflow itself, so the request is not validated twice. Remove once
# deployed clients have moved to /v1/workflow/submit_workflow.
def submit_workflow_raw_request(req: PandaRequest, params: dict | str) -> dict:
    """
    Deprecated alias of submit_workflow.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow_raw_request

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        params (dict|str): dictionary or JSON of parameters of the raw request

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    LogWrapper(_logger, "submit_workflow_raw_request").warning("deprecated path; use /v1/workflow/submit_workflow instead")
    return submit_workflow(req, params)


@request_validation(_logger, secure=True, production=False, request_method="POST")
def submit_workflow_definition(req: PandaRequest, workflow_definition: dict) -> dict:
    """
    Submit a PanDA native workflow from an already-resolved workflow definition.

    NOTE: for testing only. A workflow definition is the engine's internal, fully-resolved form,
    normally produced by the server itself when it parses a submitted description. Nothing outside
    tests should call this; use submit_workflow to submit a raw request with a sandbox, or
    submit_workflow_description to submit a workflow description inline.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow_definition

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        workflow_definition (dict): dictionary of workflow definition

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """

    user_dn = get_dn(req)
    prodsourcelabel = "user"
    if has_production_role(req):
        prodsourcelabel = "managed"
    workflow_name = workflow_definition.get("workflow_name", None)

    tmp_logger = LogWrapper(_logger, f'submit_workflow_definition prodsourcelabel={prodsourcelabel} user_dn="{user_dn}" workflow_name={workflow_name}')
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    workflow_id = global_wfif.register_workflow(prodsourcelabel, user_dn, workflow_name, workflow_definition)

    if workflow_id is not None:
        success = True
        data = {"workflow_id": workflow_id}
    else:
        message = "Failed to submit workflow"

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


def _collect_task_names(workflow_description: dict) -> Dict[str, List[str]]:
    """
    Collect the taskName of every raw-task-params step, grouped by (vo, prodSourceLabel)

    Args:
        workflow_description (dict): The workflow description

    Returns:
        dict: Map of "{vo}/{prodSourceLabel}" to the list of taskNames declared for it
    """
    grouped = {}
    workflow_data = workflow_description.get("workflow", workflow_description)
    for step_spec in (workflow_data.get("steps") or {}).values():
        if not isinstance(step_spec, dict) or step_spec.get("type") not in RAW_TASK_PARAMS_STEP_TYPES:
            continue
        task_params = step_spec.get("task_params")
        if not isinstance(task_params, dict):
            continue
        task_name = task_params.get("taskName")
        vo = task_params.get("vo")
        prod_source_label = task_params.get("prodSourceLabel")
        if task_name and vo and prod_source_label:
            grouped.setdefault(f"{vo}/{prod_source_label}", []).append(task_name)
    return grouped


def _warn_about_duplicated_task_names(tmp_logger: LogWrapper, workflow_description: dict) -> str:
    """
    Look for taskNames which already exist and describe them, without blocking the submission

    Production tasks are not duplicate-checked on insert, so a colliding taskName would otherwise go
    unnoticed until it caused confusion downstream. This is advisory only: the caller still registers
    the workflow, matching how a production task submitted outside a workflow behaves.

    Args:
        tmp_logger (LogWrapper): Logger
        workflow_description (dict): The workflow description

    Returns:
        str: A warning message describing the collisions, or an empty string when there are none
    """
    collisions = []
    for group, task_names in _collect_task_names(workflow_description).items():
        vo, prod_source_label = group.split("/", 1)
        existing = global_task_buffer.get_existing_task_names(vo, prod_source_label, task_names)
        if existing is None:
            tmp_logger.warning(f"failed to check taskName duplication for {group}; skipped")
            continue
        for task_name, info in existing.items():
            where = f"status={info['status']}" if info["status"] else "queued in DEFT"
            collisions.append(f"{task_name} (jediTaskID={info['jediTaskID']}, {where})")
    if not collisions:
        return ""
    message = f"warning: {len(collisions)} taskName(s) already exist: " + "; ".join(collisions)
    tmp_logger.warning(message)
    return message


@request_validation(_logger, secure=True, production=False, request_method="POST")
def submit_workflow_description(req: PandaRequest, workflow_description: dict | str) -> dict:
    """
    Submit a PanDA native workflow described inline, without a sandbox.

    The description uses the native workflow schema (name, inputs, outputs, steps, options) and is
    passed in the request body rather than in a sandbox, so a step carrying raw task parameters
    needs nothing uploaded. The description is validated synchronously, so that an authoring mistake
    is reported here, while parsing into a workflow definition stays asynchronous as for the other
    submission paths.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow_description

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        workflow_description (dict|str): dictionary or JSON of the workflow description

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """

    user_dn = get_dn(req)
    # Captured here, from VOMS, because a step's task is submitted long after this request and
    # cannot re-derive them; never taken from the submitted description.
    prod_role = has_production_role(req)
    fqans = get_fqan(req)
    prodsourcelabel = "managed" if prod_role else "user"

    tmp_logger = LogWrapper(_logger, f'submit_workflow_description prodsourcelabel={prodsourcelabel} user_dn="{user_dn}" ')
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    if isinstance(workflow_description, str):
        try:
            workflow_description = json.loads(workflow_description)
        except Exception as exc:
            message = f"Failed to parse workflow_description: {str(exc)}"
            tmp_logger.error(message)
            return generate_response(success, message, data)

    # validate the description up front so authoring mistakes come back on this request
    is_valid, errors = validate_workflow_description(workflow_description)
    if not is_valid:
        message = "Invalid workflow description: " + "; ".join(errors)
        tmp_logger.error(message)
        return generate_response(success, message, data)

    workflow_name = workflow_description.get("workflow", workflow_description).get("name")

    # advisory only; a collision does not prevent the workflow from being registered
    warning_message = _warn_about_duplicated_task_names(tmp_logger, workflow_description)

    # the description is carried in the raw request so that parsing happens asynchronously
    raw_request_params = {INLINE_DESCRIPTION_KEY: workflow_description}
    workflow_id = global_wfif.register_workflow(
        prodsourcelabel, user_dn, workflow_name, raw_request_params=raw_request_params, prod_role=prod_role, fqans=fqans
    )

    if workflow_id is not None:
        success = True
        data = {"workflow_id": workflow_id}
        message = warning_message
    else:
        message = "Failed to submit the workflow description"

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)
