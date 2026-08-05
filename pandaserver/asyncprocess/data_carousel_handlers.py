"""
Async request handlers for Data Carousel operations, registered into processor.HANDLERS.

The operations themselves live in pandaserver.taskbuffer.data_carousel_ops and are shared
with the synchronous endpoints of pandaserver.api.v1.data_carousel_api. These handlers only
translate between the async request framework and those operations.

Data Carousel operations mutate DDM rules and Data Carousel requests, so they must never be
run twice for a single request: a failed operation is reported as a terminal "done" result
holding success=False, and an unexpected exception is written as a non-retriable "failed"
result. While an operation is in flight a heartbeat keeps its result row fresh so
recover_stale_results doesn't hand it to another machine.
"""

import functools
import json
import threading
import traceback

# keys added to the stored parameters by the API layer, not accepted by the operations
_META_PARAMETER_KEYS = {"requester", "access"}

# how often the heartbeat refreshes started_at of a running result row
_HEARTBEAT_INTERVAL_SECONDS = 60

_dcif = None
_dcif_lock = threading.Lock()


def _get_dcif(tb):
    """
    Get the DataCarouselInterface, creating it on first use.

    The import and the constructor are expensive (Rucio, iDDS and polars imports, RSE listing
    and config loading), so a daemon that never gets a Data Carousel request never pays for it.

    Args:
        tb(TaskBuffer): task buffer to build the interface on

    Returns:
        DataCarouselInterface: shared interface instance
    """
    global _dcif
    with _dcif_lock:
        if _dcif is None:
            from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface

            _dcif = DataCarouselInterface(tb)
        return _dcif


class _ResultHeartbeat:
    """
    Context manager refreshing started_at of a running result row until the operation finishes.

    processor.run resets rows that have been running for longer than its stale threshold back to
    pending, which for a mutating operation would mean executing it a second time on another
    machine. Refreshing started_at keeps a legitimately slow operation from looking stale.
    """

    def __init__(self, tb, request_id, machine_name, tmp_logger):
        self._tb = tb
        self._request_id = request_id
        self._machine_name = machine_name
        self._tmp_logger = tmp_logger
        self._stop_event = threading.Event()
        self._thread = None

    def _beat(self):
        while not self._stop_event.wait(_HEARTBEAT_INTERVAL_SECONDS):
            try:
                if not self._tb.touch_async_result(self._request_id, self._machine_name):
                    # the row is no longer running, so this operation may already have been handed
                    # to another machine by recover_stale_results; keep going but make it visible
                    self._tmp_logger.warning(f"heartbeat did not refresh the result row of machine={self._machine_name}; the claim may have been lost")
            except Exception as e:
                self._tmp_logger.warning(f"heartbeat failed with {e}")

    def __enter__(self):
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._stop_event.set()
        self._thread.join(timeout=5)
        return False


def _handle(operation_name, row, tb, tmp_logger, result_machine):
    """
    Run one Data Carousel operation and store its outcome as the request's result.

    Args:
        operation_name(str): key in data_carousel_ops.OPERATIONS
        row(dict): the async_requests row to process
        tb(TaskBuffer): task buffer
        tmp_logger(LogWrapper): logger of the processing cycle
        result_machine(str): machine_name the result row is keyed by
    """
    from pandaserver.taskbuffer import data_carousel_ops

    request_id = row["request_id"]
    try:
        operation = data_carousel_ops.OPERATIONS[operation_name]
        parameters = json.loads(row["parameters"] or "{}")
        kwargs = {key: value for key, value in parameters.items() if key not in _META_PARAMETER_KEYS}
        tmp_logger.debug(f"running {operation_name} with {kwargs}")
        with _ResultHeartbeat(tb, request_id, result_machine, tmp_logger):
            success, message, data = operation(_get_dcif(tb), **kwargs)
    except Exception:
        # the operation crashed; never retry since it may have partially applied
        err_msg = traceback.format_exc()
        tmp_logger.error(f"failed to run {operation_name} with {err_msg}")
        tb.finish_async_result(request_id, result_machine, "failed", error_msg=err_msg, retriable=False)
        return

    # an operation that ran and reported failure is a terminal result, not a retriable error
    tmp_logger.debug(f"{operation_name} returned success={success} message={message}")
    tb.finish_async_result(
        request_id,
        result_machine,
        "done",
        result=json.dumps({"success": success, "message": message, "data": data}),
    )


# names listed here rather than taken from data_carousel_ops.OPERATIONS to keep this module's
# import cheap; an unknown name would be reported as a failed result by _handle
_OPERATION_NAMES = (
    "change_staging_destination",
    "change_staging_source",
    "force_to_staging",
    "retire_unused",
)

# request_type -> handler; the type is the operation name prefixed with data_carousel_api.REQUEST_TYPE_PREFIX
HANDLERS = {f"dc_{operation_name}": functools.partial(_handle, operation_name) for operation_name in _OPERATION_NAMES}
