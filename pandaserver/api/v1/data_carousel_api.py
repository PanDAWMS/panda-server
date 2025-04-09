import datetime
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
from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_data_carousel")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None
global_data_carousel_interface = None

# These global variables don't depend on DB access and can be initialized here
# global_proxy_cache = panda_proxy_cache.MyProxyInterface()
# global_token_cache = token_cache.TokenCache()

# global multithreading lock
global_lock = Lock()


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer and other interfaces. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_data_carousel_interface
    global_data_carousel_interface = DataCarouselInterface(global_task_buffer)


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
    time_start = naive_utcnow()

    # acquire lock
    full_pid = global_data_carousel_interface.acquire_global_dc_lock(timeout_sec=10, lock_expiration_sec=300)
    if full_pid is None:
        # timeout
        err_msg = f"timed out without getting lock"
        tmp_logger.error(err_msg)
        success, message = False, err_msg
        return generate_response(success, message, data)

    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = global_data_carousel_interface.get_request_by_id(request_id)
    elif request_id is None and dataset is not None:
        # specified by dataset
        dc_req_spec = global_data_carousel_interface.get_request_by_dataset(dataset)

    if dc_req_spec is not None:
        dc_req_spec_resubmitted = global_data_carousel_interface.resubmit_request(dc_req_spec.request_id, submit_idds_request=True, exclude_prev_dst=True)
        if not dc_req_spec_resubmitted:
            err_msg = f"failed to resubmit request_id={dc_req_spec.request_id}"
            tmp_logger.error(err_msg)
            success, message = False, err_msg
        else:
            success = True
            data = {"request_id": dc_req_spec.request_id, "new_request_id": dc_req_spec_resubmitted.request_id}
    else:
        err_msg = f"failed to get corresponding request"
        tmp_logger.error(err_msg)
        success, message = False, err_msg

    # release lock
    global_data_carousel_interface.release_global_dc_lock(full_pid)

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_staging_source(req: PandaRequest, request_id: int | None = None, dataset: str | None = None) -> dict:
    """
    Change source of staging

    The current active staging request stays, and source_replica_expression of its DDM rule is unset so new source can be tried
    The reqeusts can be specified by request_id or dataset (if both exist, request_id is taken).
    Requires a secure connection production role.

    API details:
        HTTP Method: POST
        Path: /v1/data_carousel/change_staging_source

    Args:
        req(PandaRequest): internally generated request object
        request_id (int|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"change_staging_source request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    # acquire lock
    full_pid = global_data_carousel_interface.acquire_global_dc_lock(timeout_sec=10, lock_expiration_sec=300)
    if full_pid is None:
        # timeout
        err_msg = f"timed out without getting lock"
        tmp_logger.error(err_msg)
        success, message = False, err_msg
        return generate_response(success, message, data)

    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = global_data_carousel_interface.get_request_by_id(request_id)
    elif request_id is None and dataset is not None:
        # specified by dataset
        dc_req_spec = global_data_carousel_interface.get_request_by_dataset(dataset)

    if dc_req_spec is not None:
        ret = global_data_carousel_interface.change_request_source_rse(dc_req_spec)
        if not ret:
            err_msg = f"failed to change source request_id={dc_req_spec.request_id}"
            tmp_logger.error(err_msg)
            success, message = False, err_msg
        else:
            success = True
            data = {"request_id": dc_req_spec.request_id, "ddm_rule_id": dc_req_spec.ddm_rule_id}
    else:
        err_msg = f"failed to get corresponding request"
        tmp_logger.error(err_msg)
        success, message = False, err_msg

    # release lock
    global_data_carousel_interface.release_global_dc_lock(full_pid)

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


# @request_validation(_logger, secure=True, production=True, request_method="POST")
# def force_to_staging(req: PandaRequest, request_id: int|None = None, dataset: str|None = None) -> dict:
#     """
#     Change source of staging

#     The current active staging request stays, and source_replica_expression of its DDM rule is unset so new source can be tried
#     The reqeusts can be specified by request_id or dataset (if both exist, request_id is taken).
#     Requires a secure connection production role.

#     API details:
#         HTTP Method: POST
#         Path: /v1/data_carousel/change_staging_source

#     Args:
#         req(PandaRequest): internally generated request object
#         request_id (int|None): request_id of the staging request, e.g. `123`
#         dataset (str|None): dataset name of the staging request in the format of Rucio DID, e.g. `"mc20_13TeV:mc20_13TeV.700449.Sh_2211_Wtaunu_mW_120_ECMS_BFilter.merge.AOD.e8351_s3681_r13144_r13146_tid36179107_00"`

#     Returns:
#         dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
#     """
#     tmp_logger = LogWrapper(_logger, f"change_staging_source request_id={request_id} dataset={dataset}")
#     tmp_logger.debug("Start")
#     success, message, data = False, "", None
#     time_start = naive_utcnow()

#     dc_req_spec = None
#     if request_id is not None:
#         # specified by request_id
#         dc_req_spec = global_data_carousel_interface.get_request_by_id(request_id)
#     elif request_id is None and dataset is not None:
#         # specified by dataset
#         dc_req_spec = global_data_carousel_interface.get_request_by_dataset(dataset)

#     if dc_req_spec is not None:
#         ret = global_data_carousel_interface.change_request_source_rse(dc_req_spec)
#         if not ret:
#             err_msg = f"failed to resubmit request_id={request_id}"
#             tmp_logger.error(err_msg)
#             success, message = False, err_msg
#         else:
#             success = True
#             data = {"request_id": dc_req_spec.request_id, "ddm_rule_id": dc_req_spec.ddm_rule_id}
#     else:
#         err_msg = f"failed to get corresponding request"
#         tmp_logger.error(err_msg)
#         success, message = False, err_msg

#     time_delta = naive_utcnow() - time_start
#     tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

#     return generate_response(success, message, data)
