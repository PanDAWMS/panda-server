import datetime
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
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselInterface,
    DataCarouselRequestStatus,
)
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_data_carousel")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None
global_dcif = None

# These global variables don't depend on DB access and can be initialized here
# global_proxy_cache = panda_proxy_cache.MyProxyInterface()
# global_token_cache = token_cache.TokenCache()


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer and other interfaces. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_dcif
    global_dcif = DataCarouselInterface(global_task_buffer)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_staging_destination(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Change destination of staging

    The current active staging request will be cancelled, and a new request will be created with the newly selected destination RSE, excluding the original destination.
    The reqeusts can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.

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
    tmp_logger = LogWrapper(_logger, f"change_staging_destination request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    dc_req_spec_resubmitted = None
    to_submit_idds = False
    time_start = naive_utcnow()

    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = global_dcif.get_request_by_id(request_id)
    elif request_id is None and dataset is not None:
        # specified by dataset
        dc_req_spec = global_dcif.get_request_by_dataset(dataset)

    if dc_req_spec is not None:
        dc_req_spec_resubmitted, err_msg = global_dcif.resubmit_request(dc_req_spec, submit_idds_request=False, exclude_prev_dst=True)
        if not dc_req_spec_resubmitted or err_msg:
            err_msg = f"failed to resubmit request_id={dc_req_spec.request_id} : {err_msg}"
            tmp_logger.error(err_msg)
            success, message = False, err_msg
        else:
            to_submit_idds = True
    else:
        err_msg = f"failed to get corresponding request"
        tmp_logger.error(err_msg)
        success, message = False, err_msg

    if dc_req_spec_resubmitted and dc_req_spec_resubmitted.status == DataCarouselRequestStatus.staging:
        success = True
        data = {"request_id": dc_req_spec.request_id, "new_request_id": dc_req_spec_resubmitted.request_id, "dataset": dc_req_spec_resubmitted.dataset}
        message = "new request resubmitted, destination changed"
        if to_submit_idds:
            new_request_id = dc_req_spec_resubmitted.request_id
            task_id_list = global_dcif._get_related_tasks(new_request_id)
            if task_id_list:
                tmp_logger.debug(f"related tasks: {task_id_list}")
                with ThreadPoolExecutor() as thread_pool:
                    thread_pool.map((lambda task_id: global_dcif._submit_idds_stagein_request(task_id, dc_req_spec_resubmitted)), task_id_list)
                tmp_logger.debug(f"submitted corresponding iDDS requests for related tasks")
                message += "; submitted iDDS requests"

            else:
                err_msg = f"failed to get related tasks; skipped to submit iDDS requests"
                tmp_logger.warning(err_msg)
                message += f"; {err_msg}"

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

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
    tmp_logger = LogWrapper(
        _logger,
        f"change_staging_source request_id={request_id} dataset={dataset} cancel_fts={cancel_fts} change_src_expr={change_src_expr} source_rse={source_rse}",
    )
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = global_dcif.get_request_by_id(request_id)
    elif request_id is None and dataset is not None:
        # specified by dataset
        dc_req_spec = global_dcif.get_request_by_dataset(dataset)

    if dc_req_spec is not None:
        status = dc_req_spec.status
        orig_source_rse = dc_req_spec.source_rse
        if status not in [DataCarouselRequestStatus.queued, DataCarouselRequestStatus.staging]:
            err_msg = f"request_id={dc_req_spec.request_id} status={status} not queued or staging; skipped"
            tmp_logger.warning(err_msg)
            success, message = False, err_msg
        else:
            ret, dc_req_spec, err_msg = global_dcif.change_request_source_rse(dc_req_spec, cancel_fts, change_src_expr, source_rse)
            if not ret:
                err_msg = f"failed to change source request_id={dc_req_spec.request_id} : {err_msg}"
                tmp_logger.error(err_msg)
                success, message = False, err_msg
            else:
                success = True
                if dc_req_spec.status == DataCarouselRequestStatus.queued or change_src_expr:
                    message = f"status={status} changed source_rse from {orig_source_rse} to {dc_req_spec.source_rse}"
                else:
                    message = f"status={status} source replica expression is dropped"
                data = {
                    "request_id": dc_req_spec.request_id,
                    "dataset": dc_req_spec.dataset,
                    "source_rse": dc_req_spec.source_rse,
                    "ddm_rule_id": dc_req_spec.ddm_rule_id,
                }
    else:
        err_msg = f"failed to get corresponding request"
        tmp_logger.error(err_msg)
        success, message = False, err_msg

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def force_to_staging(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Force to staging

    The request will skip the queue and go to staging immediately (will submit DDM rules).
    Only effective on queued requests.
    The reqeusts can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.

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
    tmp_logger = LogWrapper(_logger, f"force_to_staging request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = global_dcif.get_request_by_id(request_id)
    elif request_id is None and dataset is not None:
        # specified by dataset
        dc_req_spec = global_dcif.get_request_by_dataset(dataset)

    if dc_req_spec is not None:
        is_ok, err_msg, dc_req_spec = global_dcif.stage_request(dc_req_spec)
        if not is_ok:
            err_msg = f"failed to stage request_id={dc_req_spec.request_id} : {err_msg}"
            tmp_logger.error(err_msg)
            success, message = False, err_msg
        else:
            success = True
            message = f"status has become {dc_req_spec.status}"
            data = {
                "request_id": dc_req_spec.request_id,
                "dataset": dc_req_spec.dataset,
                "status": dc_req_spec.status,
                "ddm_rule_id": dc_req_spec.ddm_rule_id,
            }
    else:
        err_msg = f"failed to get corresponding request"
        tmp_logger.error(err_msg)
        success, message = False, err_msg

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)
