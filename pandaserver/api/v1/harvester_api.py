import datetime
from typing import List, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import MESSAGE_DATABASE, get_dn, request_validation
from pandaserver.api.v1.timed_method import TIME_OUT, TimedMethod
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("harvester_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True)
def update_workers(req: PandaRequest, harvester_id: str, workers: List) -> Tuple:
    """
    Update workers

    **Requires POST method and a secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        workers: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    tmp_logger = LogWrapper(_logger, f"update_workers harvester_id={harvester_id}")
    tmp_logger.debug(f"Start")

    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    ret = global_task_buffer.updateWorkers(harvester_id, workers)
    if not ret:
        tmp_logger.error(f"Error updating database for workers: {workers}")
        return_tuple = False, MESSAGE_DATABASE
    else:
        return_tuple = True, ret

    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return return_tuple


@request_validation(_logger, secure=True)
def update_harvester_service_metrics(req: PandaRequest, harvester_id: str, metrics: str) -> Tuple:
    """
    Update harvester service metrics

    **Requires POST method and a secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        metrics: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    tmp_logger = LogWrapper(_logger, f"update_harvester_service_metrics harvester_id={harvester_id}")
    tmp_logger.debug(f"Start")

    # update the metrics in the database
    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    ret = global_task_buffer.updateServiceMetrics(harvester_id, metrics)
    if not ret:
        tmp_logger.error(f"Error updating database for metrics: {metrics}")
        return_tuple = False, MESSAGE_DATABASE
    else:
        return_tuple = True, ret

    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return return_tuple


@request_validation(_logger, secure=True)
def add_harvester_dialogs(req: PandaRequest, harvester_id: str, dialogs: str) -> Tuple:
    """
    Add harvester dialog messages

    **Requires POST method and a secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        dialogs: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    tmp_logger = LogWrapper(_logger, f"add_harvester_dialogs harvester_id={harvester_id}")
    tmp_logger.debug(f"Start")

    ret = global_task_buffer.addHarvesterDialogs(harvester_id, dialogs)
    if not ret:
        tmp_logger.error(f"Error updating database: {dialogs}")
        return False, MESSAGE_DATABASE

    tmp_logger.debug(f"Done")
    return True, ret


@request_validation(_logger, secure=True)
def harvester_heartbeat(req: PandaRequest, harvester_id: str, data: str = None) -> Tuple:
    """
    Heartbeat for harvester. User and host are retrieved from the request object and updated in the database.

    **Requires POST method and a secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        data: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    tmp_logger = LogWrapper(_logger, f"harvester_heartbeat harvester_id={harvester_id}")
    tmp_logger.debug(f"Start")

    # get user and hostname to record in harvester metadata
    user = get_dn(req)
    host = req.get_remote_host()

    ret = global_task_buffer.harvesterIsAlive(user, host, harvester_id, data)
    if not ret:
        tmp_logger.error(f"Error updating database: {data}")
        return False, MESSAGE_DATABASE

    tmp_logger.debug(f"Done")
    return True, ret


def get_current_worker_id(req: PandaRequest, harvester_id: str) -> Tuple:
    """
    TODO: Validate.
    Get the current worker ID.
    :param req:
    :param harvester_id:
    :return:
    """
    tmp_logger = LogWrapper(_logger, f"get_current_worker_id")
    tmp_logger.debug(f"Start")
    current_worker_id = global_task_buffer.get_max_worker_id(harvester_id)
    tmp_logger.debug(f"Done")

    if current_worker_id is None:
        return False, MESSAGE_DATABASE

    return True, current_worker_id


def get_worker_statistics(req: PandaRequest) -> Tuple:
    """
    Get statistics for all the workers managed across the Grid.

    Requires GET method.

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and the statistics or an error message, e.g. (False, 'Error message') or (True, {...}})
    """
    tmp_logger = LogWrapper(_logger, f"get_worker_statistics")
    tmp_logger.debug(f"Start")
    return_tuple = True, global_task_buffer.getWorkerStats()
    tmp_logger.debug(f"Done")
    return return_tuple


@request_validation(_logger, secure=True)
def report_worker_statistics(req: PandaRequest, harvester_id: str, panda_queue: str, statistics: str) -> Tuple:
    """
    Report statistics for the workers managed by a harvester instance at a PanDA queue.

    **Requires POST method and a secure connection.**

    Args:
        req (PandaRequest): Internally generated request object.
        harvester_id (str): Harvester ID.
        panda_queue (str): Name of the PanDA queue.
        statistics (str): JSON string containing a dictionary with the statistics to be reported.
            The format should follow this structure:

            ::

                {
                    "prodsourcelabel_1": {
                        "RESOURCE_TYPE_1": {"running": 1, "submitted": 2, ...},
                        "RESOURCE_TYPE_2": {"running": 1, "submitted": 2, ...}
                    },
                    "prodsourcelabel_2": {
                        ...
                    }
                }

    Returns:
        str: JSON string with the result of the operation, typically a tuple with a boolean and a message,
        e.g., `(False, 'Error message')` or `(True, 'OK')`.
    """
    tmp_logger = LogWrapper(_logger, f"report_worker_statistics harvester_id={harvester_id}")
    tmp_logger.debug(f"Start")
    return_tuple = global_task_buffer.reportWorkerStats_jobtype(harvester_id, panda_queue, statistics)
    tmp_logger.debug(f"Done")
    return return_tuple


@request_validation(_logger, secure=True, production=True)
def get_harvester_commands(req: PandaRequest, harvester_id: str, n_commands: int, timeout: int = 30) -> Tuple:
    """
    This function checks the permissions and retrieves the commands for a specified harvester instance.

    TODO: Validate.
    Args:
        req: The request object containing the environment variables.
        harvester_id (str): The ID of the harvester instance.
        n_commands (int): The number of commands to retrieve.
        timeout (int, optional): The timeout value. Defaults to 30.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_logger = LogWrapper(_logger, f"get_harvester_commands")
    tmp_logger.debug(f"Start")

    timed_method = TimedMethod(global_task_buffer.getCommands, timeout)
    timed_method.run(harvester_id, n_commands)

    tmp_logger.debug(f"Done")

    # Getting the commands timed out
    if timed_method.result == TIME_OUT:
        return False, TIME_OUT

    # Unpack the return code and the commands
    return_code, commands = timed_method.result

    # There was an error retrieving the commands from the database
    if return_code == -1:
        return False, MESSAGE_DATABASE

    return True, commands


@request_validation(_logger, secure=True, production=True)
def acknowledge_harvester_commands(req: PandaRequest, command_ids: List, timeout: int = 30) -> Tuple:
    """
    This function checks the permissions, parses the command IDs from JSON, and acknowledges the list of commands.

    TODO: Validate.
    Args:
        req: The request object containing the environment variables.
        command_ids (str): A JSON string containing the list of command IDs to acknowledge.
        timeout (int, optional): The timeout value. Defaults to 30.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_logger = LogWrapper(_logger, f"acknowledge_harvester_commands")
    tmp_logger.debug(f"Start")

    timed_method = TimedMethod(global_task_buffer.ackCommands, timeout)
    timed_method.run(command_ids)

    tmp_logger.debug(f"Done")

    # Make response
    if timed_method.result == TIME_OUT:
        return False, TIME_OUT

    # Unpack the return code and the commands
    return_code = timed_method.result

    # There was an error acknowledging the commands in the database
    if return_code == -1:
        return False, MESSAGE_DATABASE

    return True, ""


@request_validation(_logger, secure=True, production=True)
def add_sweep_harvester_command(req: PandaRequest, panda_queue: str, status_list: List[str], ce_list: List[str], submission_host_list: List[str]) -> Tuple:
    """
    Send a command to harvester to kill the workers in a PanDA queue, with the possibility of specifying filters by status, CE or submission host.

    **Requires POST method, secure connection and production role.**

    Args:
        req(PandaRequest): internally generated request object
        panda_queue(str): name of the PanDA queue
        status_list: list of worker statuses to be considered, e.g. ['submitted', 'running']
        ce_list: list of the Computing Elements to be considered
        submission_host_list: list of the harvester submission hosts to be considered

    Returns
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    tmp_logger = LogWrapper(_logger, f"add_sweep_harvester_command panda_queue={panda_queue}")
    tmp_logger.debug(f"Start")
    return_tuple = True, global_task_buffer.sweepPQ(panda_queue, status_list, ce_list, submission_host_list)
    tmp_logger.debug(f"Done")
    return return_tuple


@request_validation(_logger, secure=True, production=True)
def add_target_slots(req, panda_queue: str, slots: int, global_share: str = None, resource_type: str = None, expiration_date: str = None):
    """
    Set the number of slots for a PanDA queue.

    TODO: Validate.
    **Requires POST method, secure connection and production role.**

    Args:
        req(PandaRequest): internally generated request object
        panda_queue(str): name of the PanDA queue
        slots(int): number of slots to set
        global_share(str): global share the slots apply to. Optional - by default it applies to the whole queue.
        resource_type: resource type (SCORE, MCORE,...) the slots apply to. Optional - by default it applies to the whole queue.
        expiration_date: the expiration date of the slots. Optional - by default it applies indefinitely.

    Returns
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, '...')
    """
    tmp_logger = LogWrapper(_logger, f"add_target_slots panda_queue={panda_queue}")
    tmp_logger.debug(f"Start with slots={slots}, global_share={global_share}, resource_type={resource_type}, expiration_date={expiration_date}")
    return_code, return_message = global_task_buffer.setNumSlotsForWP(panda_queue, slots, global_share, resource_type, expiration_date)

    if return_code == 0:
        return_tuple = True, return_message
    else:
        return_tuple = False, return_message

    tmp_logger.debug(f"Done")
    return return_tuple
