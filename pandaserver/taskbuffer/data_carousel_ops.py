"""
Core Data Carousel operations shared by the synchronous and the asynchronous APIs.

Each operation takes an already initialized DataCarouselInterface and returns the
(success, message, data) triple that pandaserver.api.v1.data_carousel_api packs into its
HTTP response. The synchronous endpoints call these functions directly, while the
asynchronous endpoints queue a request in async_requests and
pandaserver.asyncprocess.data_carousel_handlers calls the very same functions from the
async request daemon. Keeping the bodies here is what makes the two paths interchangeable.
"""

from concurrent.futures import ThreadPoolExecutor

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselInterface,
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
)

# deliberately the same logger as pandaserver.api.v1.data_carousel_api, so an operation logs
# under one name whether it ran inline in the API or in the async request daemon
_logger = PandaLogger().getLogger("api_data_carousel")

# (success, message, data) returned by every operation
OperationResult = tuple[bool, str, dict | None]

# cap on the threads submitting iDDS requests in parallel, so a request with many related tasks
# can't spawn an unbounded number of threads in the API or daemon process
IDDS_SUBMISSION_MAX_WORKERS = 8


def validate_target(request_id: int | str | None = None, dataset: str | None = None) -> tuple[bool, str]:
    """
    Check that the arguments identify a request, without touching the DB.

    Used by the asynchronous endpoints to reject bad input before queueing anything; the
    synchronous endpoints don't need it since they report the failure to resolve directly.

    Args:
        request_id (int|str|None): request_id of the staging request
        dataset (str|None): dataset name of the staging request

    Returns:
        tuple[bool, str]: (valid, error message)
    """
    if request_id is None and dataset is None:
        return False, "either request_id or dataset must be provided"
    if request_id is not None:
        try:
            int(request_id)
        except (TypeError, ValueError):
            return False, f"invalid request_id: {request_id}"
    return True, ""


def _resolve_request(dcif: DataCarouselInterface, request_id: int | str | None, dataset: str | None) -> DataCarouselRequestSpec | None:
    """
    Get the spec of the request specified by request_id or dataset (request_id is taken if both exist).

    Args:
        dcif (DataCarouselInterface): Data Carousel interface
        request_id (int|str|None): request_id of the staging request; may come in as a string
            since parameters round-trip through JSON on the asynchronous path
        dataset (str|None): dataset name of the staging request

    Returns:
        DataCarouselRequestSpec|None: spec of the request, or None if not found
    """
    if request_id is not None:
        # specified by request_id
        return dcif.get_request_by_id(int(request_id))
    elif dataset is not None:
        # specified by dataset
        return dcif.get_request_by_dataset(dataset)
    return None


def change_staging_destination(dcif: DataCarouselInterface, request_id: int | str | None = None, dataset: str | None = None) -> OperationResult:
    """
    Change destination of staging

    The current active staging request will be cancelled, and a new request will be created with the newly selected destination RSE, excluding the original destination.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).

    Args:
        dcif (DataCarouselInterface): Data Carousel interface
        request_id (int|str|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID

    Returns:
        tuple[bool, str, dict|None]: (success, message, data)
    """
    tmp_logger = LogWrapper(_logger, f"change_staging_destination request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    dc_req_spec_resubmitted = None
    to_submit_idds = False
    time_start = naive_utcnow()

    dc_req_spec = _resolve_request(dcif, request_id, dataset)

    if dc_req_spec is not None:
        dc_req_spec_resubmitted, err_msg = dcif.resubmit_request(dc_req_spec, submit_idds_request=False, exclude_prev_dst=True)
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
            task_id_list = dcif._get_related_tasks(new_request_id)
            if task_id_list:
                tmp_logger.debug(f"related tasks: {task_id_list}")
                with ThreadPoolExecutor(max_workers=min(IDDS_SUBMISSION_MAX_WORKERS, len(task_id_list))) as thread_pool:
                    future_map = {task_id: thread_pool.submit(dcif._submit_idds_stagein_request, task_id, dc_req_spec_resubmitted) for task_id in task_id_list}
                # the results must be consumed, otherwise a submission raising in its thread is silently lost
                failed_task_id_list = []
                for task_id, future in future_map.items():
                    try:
                        future.result()
                    except Exception as e:
                        failed_task_id_list.append(task_id)
                        tmp_logger.error(f"failed to submit iDDS request for task_id={task_id} : {e}")
                if failed_task_id_list:
                    err_msg = f"submitted iDDS requests for {len(task_id_list) - len(failed_task_id_list)}/{len(task_id_list)} related tasks; failed for {failed_task_id_list}"
                    tmp_logger.warning(err_msg)
                    message += f"; {err_msg}"
                else:
                    tmp_logger.debug(f"submitted corresponding iDDS requests for related tasks")
                    message += "; submitted iDDS requests"

            else:
                err_msg = f"failed to get related tasks; skipped to submit iDDS requests"
                tmp_logger.warning(err_msg)
                message += f"; {err_msg}"

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return success, message, data


def change_staging_source(
    dcif: DataCarouselInterface,
    request_id: int | str | None = None,
    dataset: str | None = None,
    cancel_fts: bool = False,
    change_src_expr: bool = False,
    source_rse: str | None = None,
) -> OperationResult:
    """
    Change source of staging

    If the request is queued, its source_rse will be rechosen, excluding the original source.
    If the request is staging, the source_replica_expression of its DDM rule is unset so new source can be tried.
    Only effective on queued or staging requests.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).

    Args:
        dcif (DataCarouselInterface): Data Carousel interface
        request_id (int|str|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID
        cancel_fts (bool): whether to cancel current FTS requests on DDM, False by default
        change_src_expr (bool): whether to change source_replica_expression of the DDM rule by replacing old source with new one, instead of just dropping old source
        source_rse (str|None): if set, use this source RSE instead of choosing one randomly, also force change_src_expr to be True; default is None

    Returns:
        tuple[bool, str, dict|None]: (success, message, data)
    """
    tmp_logger = LogWrapper(
        _logger,
        f"change_staging_source request_id={request_id} dataset={dataset} cancel_fts={cancel_fts} change_src_expr={change_src_expr} source_rse={source_rse}",
    )
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    dc_req_spec = _resolve_request(dcif, request_id, dataset)

    if dc_req_spec is not None:
        status = dc_req_spec.status
        orig_source_rse = dc_req_spec.source_rse
        if status not in [DataCarouselRequestStatus.queued, DataCarouselRequestStatus.staging]:
            err_msg = f"request_id={dc_req_spec.request_id} status={status} not queued or staging; skipped"
            tmp_logger.warning(err_msg)
            success, message = False, err_msg
        else:
            ret, dc_req_spec, err_msg = dcif.change_request_source_rse(dc_req_spec, cancel_fts, change_src_expr, source_rse)
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

    return success, message, data


def force_to_staging(dcif: DataCarouselInterface, request_id: int | str | None = None, dataset: str | None = None) -> OperationResult:
    """
    Force to staging

    The request will skip the queue and go to staging immediately (will submit DDM rules).
    Only effective on queued requests.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).

    Args:
        dcif (DataCarouselInterface): Data Carousel interface
        request_id (int|str|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID

    Returns:
        tuple[bool, str, dict|None]: (success, message, data)
    """
    tmp_logger = LogWrapper(_logger, f"force_to_staging request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    dc_req_spec = _resolve_request(dcif, request_id, dataset)

    if dc_req_spec is not None:
        is_ok, err_msg, dc_req_spec = dcif.stage_request(dc_req_spec)
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

    return success, message, data


def retire_unused(dcif: DataCarouselInterface, request_id: int | str | None = None, dataset: str | None = None) -> OperationResult:
    """
    Retire unused staging request

    If the request is done and has no related tasks, it can be retired to clean up the DDM rules and replicas.
    The requests can be specified by request_id or dataset (if both exist, request_id is taken).

    Args:
        dcif (DataCarouselInterface): Data Carousel interface
        request_id (int|str|None): request_id of the staging request, e.g. `123`
        dataset (str|None): dataset name of the staging request in the format of Rucio DID

    Returns:
        tuple[bool, str, dict|None]: (success, message, data)
    """
    tmp_logger = LogWrapper(_logger, f"retire_unused request_id={request_id} dataset={dataset}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    dc_req_spec = _resolve_request(dcif, request_id, dataset)

    if dc_req_spec is not None:
        is_ok, dc_req_spec, err_msg = dcif.retire_unused_request(dc_req_spec)
        if not is_ok:
            err_msg = f"failed to retire request_id={dc_req_spec.request_id} : {err_msg}"
            tmp_logger.error(err_msg)
            success, message = False, err_msg
        else:
            success = True
            message = f"retired successfully"
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

    return success, message, data


# operations addressable by name, used by the asynchronous handlers to dispatch on request_type
OPERATIONS = {
    "change_staging_destination": change_staging_destination,
    "change_staging_source": change_staging_source,
    "force_to_staging": force_to_staging,
    "retire_unused": retire_unused,
}
