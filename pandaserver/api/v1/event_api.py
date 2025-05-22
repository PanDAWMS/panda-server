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

_logger = PandaLogger().getLogger("api_event")

# These global variables are initialized in the init_task_buffer method
global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """

    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, production=True, request_method="GET")
def get_available_event_range_count(req: PandaRequest, job_id: int, jobset_id: int, task_id: int, timeout=60) -> dict:
    """
    Get available event range count

    This function returns the count of available event ranges for a given job_id, jobset_id, and task_id. Requires a secure connection and production role.

    API details:
        HTTP Method: GET
        Path: /v1/event/get_available_event_range_count

    Args:
        req(PandaRequest): internally generated request object
        job_id(int): PanDA job ID
        jobset_id(int): Jobset ID
        task_id(int): JEDI task ID
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the number of available event ranges.
    """

    tmp_logger = LogWrapper(_logger, f"get_available_event_range_count < job_id={job_id} jobset_id={jobset_id} task_id={task_id} >")

    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.checkEventsAvailability, timeout)
    timed_method.run(job_id, jobset_id, task_id)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.error("Timed out")
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

    Gets a dictionary with the status of the event ranges for the given pairs of PanDA job IDs and JEDI task IDs. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/event/get_event_range_statuses

    Args:
        req(PandaRequest): internally generated request object
        job_task_ids(int): json encoded string with JEDI task ID + PanDA job ID pairs, in the format `[{"task_id": <task>, "job_id": <job>}, ...]`

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the status of the event ranges in the format `{<job_id>: {<event_range_id>: {"status": <status>, "error": <error_code>, "dialog": <dialog>}, ...}, ...}`
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
    job_id: int,
    jobset_id: int,
    task_id: int = None,
    n_ranges: int = 10,
    timeout: int = 60,
    scattered: bool = False,
    segment_id: int = None,
) -> dict:
    """
    Acquire event ranges

    Acquires a list of event ranges with a given PandaID for execution. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/event/acquire_event_ranges

    Args:
        req(PandaRequest): Internally generated request object containing the environment.
        job_id(str): PanDa job ID.
        jobset_id(str): Jobset ID.
        task_id(int, optional): JEDI task ID. Defaults to None.
        n_ranges(int, optional): The number of event ranges to retrieve. Defaults to 10.
        timeout(int, optional): The timeout value. Defaults to 60.
        scattered(bool, optional): Whether the event ranges are scattered. Defaults to None.
        segment_id(int, optional): The segment ID. Defaults to None.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the event ranges. When unsuccessful, the message field contains the error message.

    """

    tmp_logger = LogWrapper(
        _logger, f"acquire_event_ranges < job_id={job_id} jobset_id={jobset_id} task_id={task_id} n_ranges={n_ranges} segment_id={segment_id} >"
    )
    tmp_logger.debug("Start")

    accept_json = True  # Dummy variable required in the timed method

    timed_method = TimedMethod(global_task_buffer.getEventRanges, timeout)
    timed_method.run(job_id, jobset_id, task_id, n_ranges, accept_json, scattered, segment_id)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.error("Timed out")
        return generate_response(False, TIME_OUT)

    # Case of failure
    if timed_method.result is None:
        tmp_logger.debug(MESSAGE_DATABASE)
        return generate_response(False, MESSAGE_DATABASE)

    event_ranges = timed_method.result

    tmp_logger.debug(f"Done: {event_ranges}")
    return generate_response(True, data=event_ranges)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_single_event_range(
    req: PandaRequest,
    event_range_id: str,
    event_range_status: str,
    core_count: int = None,
    cpu_consumption_time: float = None,
    object_store_id: id = None,
    timeout: int = 60,
):
    """
    Update single event range

    Updates the status of a specific event range. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/event/update_single_event_range

    Args:
        req(PandaRequest): The request object containing the environment variables.
        event_range_id(str): The ID of the event range to update.
        event_range_status(str): The new status of the event range.
        core_count(int, optional): The number of cores used. Defaults to None.
        cpu_consumption_time(float, optional): The CPU consumption time. Defaults to None.
        object_store_id(int, optional): The object store ID. Defaults to None.
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field can contain a command for the pilot. When unsuccessful, the message field contains the error message.
    """
    tmp_logger = LogWrapper(
        _logger,
        f"update_single_event_range < {event_range_id} status={event_range_status} core_count={core_count} cpu_consumption_time={cpu_consumption_time} object_store_id={object_store_id} >",
    )
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.updateEventRange, timeout)
    timed_method.run(event_range_id, event_range_status, core_count, cpu_consumption_time, object_store_id)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.error("Timed out")
        return generate_response(False, TIME_OUT)

    if not timed_method.result or timed_method.result[0] is False:
        tmp_logger.debug(MESSAGE_DATABASE)
        return generate_response(False, MESSAGE_DATABASE)

    success = timed_method.result[0]
    command = timed_method.result[1]

    _logger.debug(f"Done with command: {command}")
    return generate_response(success, data={"Command": command})


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_event_ranges(req: PandaRequest, event_ranges: str, timeout: int = 120, version: int = 0):
    """
    Update event ranges

    Updates the event ranges in bulk. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/event/update_event_ranges

    Args:
        req (PandaRequest): Internally generated request object containing the environment.
        event_ranges (str): JSON-encoded string containing the list of event ranges to update.
        timeout (int, optional): The timeout value. Defaults to 120.
        version (int, optional): The version of the event ranges.  Defaults to 0.
                                 Version 0: normal event service
                                 Version 1: jumbo jobs with zip file support
                                 Version 2: fine-grained processing where events can be updated before being dispatched

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field will contain a dictionary `{"Returns": [], "Commands":{<PanDA ID>: <Command>, ...}}`. `Returns` is list with a status for each event range
              and `Commands` is a dictionary with a possible command per PanDA job ID.
              When unsuccessful, the message field contains the error message.
    """

    tmp_logger = LogWrapper(_logger, f"update_event_ranges({event_ranges})")
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.updateEventRanges, timeout)
    timed_method.run(event_ranges, version)

    # Case of time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.error("Timed out")
        return generate_response(False, TIME_OUT)

    data = {"Returns", timed_method.result[0], "Commands", timed_method.result[1]}

    _logger.debug(f"Done with: {data}")
    return generate_response(True, data=data)
