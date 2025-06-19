import datetime
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
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_harvester")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, request_method="POST")
def update_workers(req: PandaRequest, harvester_id: str, workers: List) -> dict:
    """
    Update workers.

    Update the details for a list of workers. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/update_workers

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        workers(list): list of worker dictionaries that describe the fields of a pandaserver/taskbuffer/WorkerSpec object.
                ```
                [{"workerID": 1, "batchID": 1, "queueName": "queue1", "status": "running",
                "computingSite": "site1", "nCore": 1, "nodeID": None,
                "submitTime": "02-NOV-24 00:02:18", "startTime": "02-NOV-24 00:02:18", "endTime": None,
                "jobType": "managed", "resourceType": "SCORE", "nativeExitCode": None, "nativeStatus": None,
                "diagMessage": None, "nJobs": 1, "computingElement": "ce1", "syncLevel": 0,
                "submissionHost": "submissionhost1", "harvesterHost": "harvesterhost1",
                "errorCode": None, "minRamCount": 2000},...]
                ```

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`

    """
    tmp_logger = LogWrapper(_logger, f"update_workers harvester_id={harvester_id}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None
    time_start = naive_utcnow()

    ret = global_task_buffer.updateWorkers(harvester_id, workers)
    if not ret:
        tmp_logger.error(f"Error updating database for workers: {workers}")
        success, message = False, MESSAGE_DATABASE
    else:
        success, data = True, ret

    time_delta = naive_utcnow() - time_start
    tmp_logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def update_service_metrics(req: PandaRequest, harvester_id: str, metrics: list) -> Dict[str, Any]:
    """
    Update harvester service metrics.

    Update the service metrics for a harvester instance. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/update_service_metrics

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        metrics(list): list of triplets `[[host, timestamp, metric_dict],[host, timestamp, metric_dict]...]`. The metric dictionary is json encoded, as it is stored in the database like that.
            ```
            harvester_host = "harvester_host.cern.ch"
            creation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            metric = {
                "rss_mib": 2737.36,
                "memory_pc": 39.19,
                "cpu_pc": 15.23,
                "volume_data_pc": 20.0,
                "cert_lifetime": {
                    "/data/atlpan/proxy/x509up_u25606_prod": 81,
                    "/data/atlpan/proxy/x509up_u25606_pilot": 81,
                    "/cephfs/atlpan/harvester/proxy/x509up_u25606_prod": 96,
                    "/cephfs/atlpan/harvester/proxy/x509up_u25606_pilot": 96,
                },
            }

            # DBProxy expects the metrics in json format and stores them directly in the database
            metrics = [[creation_time, harvester_host, json.dumps(metric)]]
            ```

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"update_service_metrics harvester_id={harvester_id}")
    tmp_logger.debug("Start")
    success, message, data = False, "", None

    # update the metrics in the database
    time_start = naive_utcnow()

    ret = global_task_buffer.updateServiceMetrics(harvester_id, metrics)
    if not ret:
        tmp_logger.error(f"Error updating database for metrics: {metrics}")
        success, message = False, MESSAGE_DATABASE
    else:
        success, data = True, ret

    time_delta = naive_utcnow() - time_start
    _logger.debug(f"Done. Took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def add_dialogs(req: PandaRequest, harvester_id: str, dialogs: list) -> Dict[str, Any]:
    """
    Add harvester dialog messages.

    Add messages for a harvester instance. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/add_dialogs

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        dialogs(list): list of dialog dictionaries, e.g.
            ```
            dialogs = [{
                "diagID": 1,
                "moduleName": "test_module",
                "identifier": "test identifier",
                "creationTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "messageLevel": "INFO",
                "diagMessage": "test message",
                },...]
            ```

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"add_dialogs harvester_id={harvester_id}")
    tmp_logger.debug("Start")

    ret = global_task_buffer.addHarvesterDialogs(harvester_id, dialogs)
    if not ret:
        tmp_logger.error(f"Error updating database: {dialogs}")
        return generate_response(False, message=MESSAGE_DATABASE)

    tmp_logger.debug("Done")
    return generate_response(True)


@request_validation(_logger, secure=True, request_method="POST")
def heartbeat(req: PandaRequest, harvester_id: str, data: dict = None) -> Dict[str, Any]:
    """
    Heartbeat for harvester.

    Send a heartbeat for harvester and optionally update the instance data. User and host are retrieved from the request object and updated in the database. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/heartbeat

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        data(dict): metadata dictionary to be updated in the PanDA database, e.g. `data = {"startTime": <start time>, "sw_version": <release version>, "commit_stamp": <commit timestamp>}`


    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"heartbeat harvester_id={harvester_id}")
    tmp_logger.debug("Start")

    # get user and hostname to record in harvester metadata
    user = get_dn(req)
    host = req.get_remote_host()

    ret_message = global_task_buffer.harvesterIsAlive(user, host, harvester_id, data)
    if not ret_message or ret_message != "succeeded":
        tmp_logger.error(f"Error updating database: {data}")
        return generate_response(False, message=MESSAGE_DATABASE)

    tmp_logger.debug("Done")
    return generate_response(True)


@request_validation(_logger, secure=True, request_method="GET")
def get_current_worker_id(req: PandaRequest, harvester_id: str) -> Dict[str, Any]:
    """
    Get the current worker ID.

    Retrieve the current worker ID. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/harvester/get_current_worker_id

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): harvester id, e.g. `harvester_central_A`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, "get_current_worker_id")
    tmp_logger.debug("Start")
    current_worker_id = global_task_buffer.get_max_worker_id(harvester_id)
    tmp_logger.debug("Done")

    if current_worker_id is None:
        return generate_response(False, message=MESSAGE_DATABASE)

    return generate_response(True, data=current_worker_id)


@request_validation(_logger, secure=True, request_method="GET")
def get_worker_statistics(req: PandaRequest) -> Dict[str, Any]:
    """
    Get worker statistics.

    Get statistics for all the workers managed across the Grid. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/harvester/get_worker_statistics

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, "get_worker_statistics")
    tmp_logger.debug("Start")
    worker_stats = global_task_buffer.getWorkerStats()
    tmp_logger.debug("Done")
    return generate_response(True, data=worker_stats)


@request_validation(_logger, secure=True, request_method="POST")
def report_worker_statistics(req: PandaRequest, harvester_id: str, panda_queue: str, statistics: str) -> Dict[str, Any]:
    """
    Report worker statistics.

    Report statistics for the workers managed by a harvester instance at a PanDA queue. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/report_worker_statistics

    Args:
        req (PandaRequest): Internally generated request object.
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        panda_queue(str): Name of the PanDA queue, e.g. `CERN`.
        statistics(str): JSON string containing a dictionary with the statistics to be reported. It will be stored as a json in the database. E.g.
            ```
            json.dumps({"user": {"SCORE": {"running": 1, "submitted": 1}}, "managed": {"MCORE": {"running": 1, "submitted": 1}}})
            ```

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"report_worker_statistics harvester_id={harvester_id}")
    tmp_logger.debug("Start")
    success, message = global_task_buffer.reportWorkerStats_jobtype(harvester_id, panda_queue, statistics)
    tmp_logger.debug("Done")
    return generate_response(success, message=message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def acquire_commands(req: PandaRequest, harvester_id: str, n_commands: int, timeout: int = 30) -> Dict[str, Any]:
    """
    Get harvester commands.

    Retrieves the commands for a specified harvester instance. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/acquire_commands

    Args:
        req(PandaRequest): The request object containing the environment variables.
        harvester_id(str): harvester id, e.g. `harvester_central_A`
        n_commands(int): The number of commands to retrieve, e.g. `10`.
        timeout(int, optional): The timeout value. Defaults to `30`.

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, "acquire_commands")
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.getCommands, timeout)
    timed_method.run(harvester_id, n_commands)

    tmp_logger.debug("Done")

    # Getting the commands timed out
    if timed_method.result == TIME_OUT:
        return generate_response(False, message=TIME_OUT)

    # Unpack the return code and the commands
    return_code, commands = timed_method.result

    # There was an error retrieving the commands from the database
    if return_code == -1:
        return generate_response(False, message=MESSAGE_DATABASE)

    return generate_response(True, data=commands)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def acknowledge_commands(req: PandaRequest, command_ids: List, timeout: int = 30) -> Dict[str, Any]:
    """
    Acknowledge harvester commands.

    Acknowledges the list of command IDs in the PanDA database. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/acknowledge_commands

    Args:
        req(PandaRequest): The request object containing the environment variables.
        command_ids(list): A list of command IDs to acknowledge, e.g. `[1, 2, 3, 4,...]`.
        timeout(int, optional): The timeout value. Defaults to `30`.

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, "acknowledge_commands")
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.ackCommands, timeout)
    timed_method.run(command_ids)

    tmp_logger.debug("Done")

    # Make response
    if timed_method.result == TIME_OUT:
        return generate_response(False, message=TIME_OUT)

    # Unpack the return code and the commands
    return_code = timed_method.result

    # There was an error acknowledging the commands in the database
    if return_code == -1:
        return generate_response(False, message=MESSAGE_DATABASE)

    return generate_response(True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def add_sweep_command(req: PandaRequest, panda_queue: str, status_list: List[str], ce_list: List[str], submission_host_list: List[str]) -> Dict[str, Any]:
    """
    Add sweep command for harvester.

    Send a command to harvester to kill the workers in a PanDA queue, with the possibility of specifying filters by status, CE or submission host. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/add_sweep_command

    Args:
        req(PandaRequest): internally generated request object
        panda_queue(str): Name of the PanDA queue, e.g. `CERN`.
        status_list (list): list of worker statuses to be considered, e.g. `['submitted', 'running']`
        ce_list (list): list of the Computing Elements to be considered, e.g. `['ce1.cern.ch', 'ce2.cern.ch']`
        submission_host_list(list): list of the harvester submission hosts to be considered, e.g. `['submission_host1.cern.ch', 'submission_host2.cern.ch']`

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """

    tmp_logger = LogWrapper(_logger, f"add_sweep_command panda_queue={panda_queue}")
    tmp_logger.debug("Start")
    return_message = global_task_buffer.sweepPQ(panda_queue, status_list, ce_list, submission_host_list)
    if return_message == "OK":
        success, message = True, ""
    else:
        success, message = False, return_message
    tmp_logger.debug("Done")
    return generate_response(success, message=message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def add_target_slots(req, panda_queue: str, slots: int, global_share: str = None, resource_type: str = None, expiration_date: str = None):
    """
    Set target slots.

    Set the target number of slots for a PanDA queue, when you want to build up job pressure. Requires secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/harvester/add_target_slots

    Args:
        req (PandaRequest): Internally generated request object.
        panda_queue(str): Name of the PanDA queue, e.g. `CERN`.
        slots (int): Number of slots to set, e.g. `10000`.
        global_share (str, optional): Global share the slots apply to. Optional - by default it applies to the whole queue. E.g. `User Analysis`
        resource_type (str, optional): Resource type the slots apply to. Optional - by default it applies to the whole queue. E.g. `SCORE` or `MCORE`.
        expiration_date (str, optional): The expiration date of the slots. Optional - by default it applies indefinitely.

    Returns:
        dict: dictionary `{'success': True/False, 'message': 'Description of error', 'data': <requested data>}`
    """
    tmp_logger = LogWrapper(_logger, f"add_target_slots panda_queue={panda_queue}")
    tmp_logger.debug(f"Start with slots={slots}, global_share={global_share}, resource_type={resource_type}, expiration_date={expiration_date}")
    return_code, return_message = global_task_buffer.setNumSlotsForWP(panda_queue, slots, global_share, resource_type, expiration_date)

    if return_code == 0:
        success, message = True, return_message
    else:
        success, message = False, return_message

    tmp_logger.debug("Done")
    return generate_response(success, message=message)
