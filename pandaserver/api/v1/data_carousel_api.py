"""
API endpoints for Data Carousel operations.

Each operation comes in two flavours sharing one implementation
(pandaserver.taskbuffer.data_carousel_ops):

* the synchronous endpoints (`change_staging_destination`, ...) run the operation inline and
  return its outcome, at the risk of hitting the HTTP timeout on slow DDM/iDDS calls;
* the asynchronous endpoints (`submit_change_staging_destination`, ...) register a request in
  the async_requests table and return immediately with an async_id; the async request watchdog
  on JEDI runs the very same operation and the outcome is polled with the shared reader
  `/v1/async_process/get_result`.

Both flavours are kept so callers can switch between them without a server-side change.
"""

import json
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    generate_response,
    request_validation,
    set_owner_info,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer import data_carousel_ops
from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import (
    ANY_MACHINE,
    SERVICE_JEDI,
)
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_data_carousel")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None
global_dcif = None

# service whose async request daemon runs the Data Carousel operations, which must match the
# service_name that daemon runs with (pandajedi.jedidog.AsyncRequestWatchDog); JEDI is where the
# rest of the Data Carousel machinery lives, so its operations run next to the Data Carousel
# watchdog rather than on the server. The requests are submitted with machine_name=ANY_MACHINE so
# exactly one of the service's machines executes each one
DC_SERVICE_NAME = SERVICE_JEDI

# prefix of the async_requests.request_type values owned by this module, which is what the
# handlers in pandaserver.asyncprocess.data_carousel_handlers are registered under
REQUEST_TYPE_PREFIX = "dc_"


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer and other interfaces. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_dcif
    global_dcif = DataCarouselInterface(global_task_buffer)


def _submit_request(req: PandaRequest, operation: str, parameters: dict, tmp_logger: LogWrapper) -> dict:
    """
    Register an async request running the given Data Carousel operation.

    Args:
        req(PandaRequest): internally generated request object
        operation(str): name of the operation in pandaserver.taskbuffer.data_carousel_ops.OPERATIONS
        parameters(dict): arguments to pass to the operation
        tmp_logger(LogWrapper): logger of the calling endpoint

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': {'async_id': <uuid>}}`
    """
    is_valid, err_msg = data_carousel_ops.validate_target(parameters.get("request_id"), parameters.get("dataset"))
    if not is_valid:
        tmp_logger.warning(err_msg)
        return generate_response(False, err_msg)

    async_id = str(uuid.uuid4())
    # results are readable by the requester or any production-role caller, matching the production
    # role already required to submit; structured_result makes get_result report the operation's
    # own success/message/data at the top level of its response
    parameters = set_owner_info(dict(parameters), req, access="production", structured_result=True)

    is_ok = global_task_buffer.insert_async_request(
        async_id,
        f"{REQUEST_TYPE_PREFIX}{operation}",
        json.dumps(parameters),
        DC_SERVICE_NAME,
        ANY_MACHINE,
        None,  # expected_machines auto-derived to ["any"] for the sentinel
    )
    if not is_ok:
        err_msg = "failed to insert request into DB"
        tmp_logger.error(err_msg)
        return generate_response(False, err_msg)

    tmp_logger.debug(f"submitted async_id={async_id}")
    return generate_response(True, "request submitted", {"async_id": async_id})


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_staging_destination(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Change destination of staging

    The current active staging request will be cancelled, and a new request will be created with the newly selected destination RSE, excluding the original destination.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.
    Runs synchronously; see `submit_change_staging_destination` for the asynchronous flavour.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/change_staging_destination

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    success, message, data = data_carousel_ops.change_staging_destination(global_dcif, request_id, dataset)
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_staging_source(
    req: PandaRequest,
    request_id: int | None = None,
    dataset: str | None = None,
    cancel_fts: bool = False,
    change_src_expr: bool = False,
    source_rse: str | None = None,
) -> dict:
    """
    Change source of staging

    If the request is queued, its source_rse will be rechosen, excluding the original source.
    If the request is staging, the source_replica_expression of its DDM rule is unset so new source can be tried.
    Only effective on queued or staging requests.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.
    Runs synchronously; see `submit_change_staging_source` for the asynchronous flavour.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/change_staging_source

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`
        cancel_fts (bool): whether to cancel current FTS requests on DDM, False by default
        change_src_expr (bool): whether to change source_replica_expression of the DDM rule by replacing old source with new one, instead of just dropping old source
        source_rse (str|None): if set, use this source RSE instead of choosing one randomly, also force change_src_expr to be True; default is None

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    success, message, data = data_carousel_ops.change_staging_source(global_dcif, request_id, dataset, cancel_fts, change_src_expr, source_rse)
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def force_to_staging(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Force to staging

    The request will skip the queue and go to staging immediately (will submit DDM rules).
    Only effective on queued requests.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.
    Runs synchronously; see `submit_force_to_staging` for the asynchronous flavour.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/force_to_staging

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    success, message, data = data_carousel_ops.force_to_staging(global_dcif, request_id, dataset)
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def retire_unused(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Retire unused staging request

    If the request is done and has no related tasks, it can be retired to clean up the DDM rules and replicas.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.
    Runs synchronously; see `submit_retire_unused` for the asynchronous flavour.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/retire_unused

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    success, message, data = data_carousel_ops.retire_unused(global_dcif, request_id, dataset)
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def submit_change_staging_destination(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Submit a request to change destination of staging, to be processed asynchronously

    Asynchronous flavour of `change_staging_destination`: the request is registered in DB and
    processed by the async request daemon, so the call returns without waiting for DDM or iDDS.
    Poll `/v1/async_process/get_result` with the returned async_id as its request_id to get the
    outcome; it reports the operation's own success, message and data at the top level.
    Requires a secure connection production role.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/submit_change_staging_destination

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': {'async_id': <uuid to poll with /v1/async_process/get_result>}}`
    """
    tmp_logger = LogWrapper(_logger, f"submit_change_staging_destination request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    return _submit_request(req, "change_staging_destination", {"request_id": request_id, "dataset": dataset}, tmp_logger)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def submit_change_staging_source(
    req: PandaRequest,
    request_id: int | None = None,
    dataset: str | None = None,
    cancel_fts: bool = False,
    change_src_expr: bool = False,
    source_rse: str | None = None,
) -> dict:
    """
    Submit a request to change source of staging, to be processed asynchronously

    Asynchronous flavour of `change_staging_source`: the request is registered in DB and
    processed by the async request daemon, so the call returns without waiting for DDM or iDDS.
    Poll `/v1/async_process/get_result` with the returned async_id as its request_id to get the
    outcome; it reports the operation's own success, message and data at the top level.
    Requires a secure connection production role.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/submit_change_staging_source

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`
        cancel_fts (bool): whether to cancel current FTS requests on DDM, False by default
        change_src_expr (bool): whether to change source_replica_expression of the DDM rule by replacing old source with new one, instead of just dropping old source
        source_rse (str|None): if set, use this source RSE instead of choosing one randomly, also force change_src_expr to be True; default is None

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': {'async_id': <uuid to poll with /v1/async_process/get_result>}}`
    """
    tmp_logger = LogWrapper(
        _logger,
        f"submit_change_staging_source request_id={request_id} dataset={dataset} cancel_fts={cancel_fts} change_src_expr={change_src_expr} source_rse={source_rse}",
    )
    tmp_logger.debug("Start")
    parameters = {
        "request_id": request_id,
        "dataset": dataset,
        "cancel_fts": cancel_fts,
        "change_src_expr": change_src_expr,
        "source_rse": source_rse,
    }
    return _submit_request(req, "change_staging_source", parameters, tmp_logger)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def submit_force_to_staging(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Submit a request to force to staging, to be processed asynchronously

    Asynchronous flavour of `force_to_staging`: the request is registered in DB and processed
    by the async request daemon, so the call returns without waiting for DDM or iDDS.
    Poll `/v1/async_process/get_result` with the returned async_id as its request_id to get the
    outcome; it reports the operation's own success, message and data at the top level.
    Requires a secure connection production role.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/submit_force_to_staging

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': {'async_id': <uuid to poll with /v1/async_process/get_result>}}`
    """
    tmp_logger = LogWrapper(_logger, f"submit_force_to_staging request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    return _submit_request(req, "force_to_staging", {"request_id": request_id, "dataset": dataset}, tmp_logger)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def submit_retire_unused(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Submit a request to retire an unused staging request, to be processed asynchronously

    Asynchronous flavour of `retire_unused`: the request is registered in DB and processed by
    the async request daemon, so the call returns without waiting for DDM or iDDS.
    Poll `/v1/async_process/get_result` with the returned async_id as its request_id to get the
    outcome; it reports the operation's own success, message and data at the top level.
    Requires a secure connection production role.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/submit_retire_unused

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': {'async_id': <uuid to poll with /v1/async_process/get_result>}}`
    """
    tmp_logger = LogWrapper(_logger, f"submit_retire_unused request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    return _submit_request(req, "retire_unused", {"request_id": request_id, "dataset": dataset}, tmp_logger)
