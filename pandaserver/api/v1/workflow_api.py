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
    has_production_role,
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer
from pandaserver.workflow.workflow_core import WorkflowInterface

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
def submit_workflow_raw_request(req: PandaRequest, params: dict | str) -> dict:
    """
    Submit raw request of PanDA native workflow.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow_raw_request

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

    tmp_logger = LogWrapper(_logger, f'submit_workflow_raw_request prodsourcelabel={prodsourcelabel} user_dn="{user_dn}" ')
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


@request_validation(_logger, secure=True, production=False, request_method="POST")
def submit_workflow(req: PandaRequest, workflow_definition: dict) -> dict:
    """
    Submit a PanDA native workflow.

    API details:
        HTTP Method: POST
        Path: /v1/workflow/submit_workflow

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

    tmp_logger = LogWrapper(_logger, f'submit_workflow prodsourcelabel={prodsourcelabel} user_dn="{user_dn}" workflow_name={workflow_name}')
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
