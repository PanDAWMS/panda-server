from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    MESSAGE_DATABASE,
    TIME_OUT,
    TimedMethod,
    generate_response,
    request_validation,
)
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("secret_management_api")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """

    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, production=True, request_method="GET")
def get_available_event_range_count(req: PandaRequest, panda_id: int, jobset_id: int, task_id: int, timeout=60) -> dict:
    """
    Get available event range count

    This function returns the count of available event ranges for a given panda_id, jobset_id, and task_id.

    API details:
        HTTP Method: GET
        Path: /event/v1/get_available_event_range_count

    Args:
        req(PandaRequest): internally generated request object
        panda_id(int): PanDA job ID
        jobset_id(int): Jobset ID
        task_id(int): JEDI task ID
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the number of available event ranges.
    """

    tmp_logger = LogWrapper(_logger, f"get_available_event_range_count < panda_id={panda_id} jobset_id={jobset_id} task_id={task_id} >")

    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.checkEventsAvailability, timeout)
    timed_method.run(panda_id, jobset_id, task_id)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.debug("Timed out")
        return generate_response(False, TIME_OUT)

    # Case of failure
    if timed_method.result is None:
        tmp_logger.debug(MESSAGE_DATABASE)
        return generate_response(False, MESSAGE_DATABASE)

    n_event_ranges = timed_method.result

    tmp_logger.debug(f"Done: {n_event_ranges}")
    return generate_response(True, data=n_event_ranges)


@request_validation(_logger, secure=True, request_method="GET")
def get_event_range_statuses(req: PandaRequest, job_task_ids: str) -> dict:
    """
    Get event range statuses

    Gets a dictionary with the status of the event ranges for the given pairs of PanDA job IDs and JEDI task IDs.

    API details:
        HTTP Method: GET
        Path: /event/v1/get_event_range_statuses

    Args:
        req(PandaRequest): internally generated request object
        job_task_ids(int): json encoded string with JEDI task ID + PanDA job ID pairs, in the format `[{"task_id": <task>, "panda_id": <job>}, ...]`

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the status of the event ranges in the format `{<panda_id>: {<event_range_id>: {"status": <status>, "error": <error_code>, "dialog": <dialog>}, ...}, ...}`
    """

    tmp_logger = LogWrapper(_logger, f"get_event_range_statuses")
    tmp_logger.debug("Start")

    status_dictionary = global_task_buffer.get_events_status(job_task_ids)

    # In the case of an exception it will return None. (Case of no event ranges found is {})
    if status_dictionary is None:
        tmp_logger.debug(MESSAGE_DATABASE)
        return generate_response(False, MESSAGE_DATABASE)

    return generate_response(True, data=status_dictionary)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def acquire_event_ranges(
    req: PandaRequest,
    panda_id: int,
    jobset_id: int,
    task_id: int = None,
    n_ranges: int = 10,
    timeout: int = 60,
    scattered: bool = False,
    segment_id: int = None,
) -> dict:
    """
    Acquire event ranges

    Acquires a list of event ranges for a given PandaID.

    API details:
        HTTP Method: POST
        Path: /event/v1/acquire_event_ranges

    Args:
        req: Internally generated request object containing the environment.
        panda_id(str): PanDa job ID.
        jobset_id(str): Jobset ID.
        task_id(str, optional): JEDI task ID. Defaults to None.
        n_ranges(int, optional): The number of event ranges to retrieve. Defaults to 10.
        timeout(int, optional): The timeout value. Defaults to 60.
        scattered(str, optional): Whether the event ranges are scattered. Defaults to None.
        segment_id(int, optional): The segment ID. Defaults to None.

    Returns:
        dict: The response from the job dispatcher.
    """

    tmp_logger = LogWrapper(
        _logger, f"acquire_event_ranges < PandaID={panda_id} jobset_id={jobset_id} task_id={task_id},n_ranges={n_ranges},segment={segment_id} >"
    )
    tmp_logger.debug("Start")

    accept_json = True  # Dummy variable required in the timed method

    timed_method = TimedMethod(global_task_buffer.getEventRanges, timeout)
    timed_method.run(panda_id, jobset_id, task_id, n_ranges, accept_json, scattered, segment_id)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.debug("Timed out")
        return generate_response(False, TIME_OUT)

    # Case of failure
    if timed_method.result is None:
        tmp_logger.debug(MESSAGE_DATABASE)
        return generate_response(False, MESSAGE_DATABASE)

    event_ranges = timed_method.result

    tmp_logger.debug(f"Done: {event_ranges}")
    return generate_response(True, data=event_ranges)
