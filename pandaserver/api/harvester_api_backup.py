import datetime
import json
from typing import List

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.common import (
    MESSAGE_DATABASE,
    MESSAGE_JSON,
    get_dn,
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.userinterface.UserIF import MESSAGE_JSON

_logger = PandaLogger().getLogger("harvester_api")


class HarvesterAPI:
    def __init__(self):
        self.task_buffer = None

    # initialize
    def init(self, task_buffer):
        self.task_buffer = task_buffer

    # update workers
    def update_workers(self, harvester_id, workers):
        ret = self.task_buffer.updateWorkers(harvester_id, workers)
        if not ret:
            return False, MESSAGE_DATABASE

        return True, ret

    # update workers
    def update_harvester_service_metrics(self, harvester_id, data):
        ret = self.task_buffer.updateServiceMetrics(harvester_id, data)
        if not ret:
            return False, MESSAGE_DATABASE

        return True, ret

    # add harvester dialog messages
    def add_harvester_dialogs(self, harvester_id, dialogs):
        ret = self.task_buffer.addHarvesterDialogs(harvester_id, dialogs)
        if not ret:
            return False, MESSAGE_DATABASE

        return True, ""

    # heartbeat for harvester
    def harvester_heartbeat(self, user, host, harvester_id, data):
        ret = self.task_buffer.harvesterIsAlive(user, host, harvester_id, data)
        if not ret:
            return False, MESSAGE_DATABASE
        return True, ret

    # get stats of workers
    def get_worker_statistics(self):
        return True, self.task_buffer.getWorkerStats()

    # report stat of workers
    def report_worker_statistics(self, harvester_id, panda_queue, statistics: str):
        return self.task_buffer.reportWorkerStats_jobtype(harvester_id, panda_queue, statistics)

    # sweep panda queue
    def add_sweep_harvester_command(self, panda_queue, status_list, ce_list, submission_host_list):
        try:
            panda_queue_des = json.loads(panda_queue)
            status_list_des = json.loads(status_list)
            ce_list_des = json.loads(ce_list)
            submission_host_list_des = json.loads(submission_host_list)
        except Exception:
            _logger.error("Problem deserializing variables")
            return False, MESSAGE_JSON

        return True, self.task_buffer.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)


# Singleton
harvester_api = HarvesterAPI()
del harvester_api


@request_validation(_logger, secure=True)
def update_workers(req: PandaRequest, harvester_id: str, workers: str) -> str:
    """
    Update workers

    **Requires secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        workers: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """

    # convert and validate the workers field
    try:
        workers = json.loads(workers)
    except Exception:
        return json.dumps((False, MESSAGE_JSON))

    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    ret = harvester_api.update_workers(harvester_id, workers)
    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"update_workers {harvester_id} took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return json.dumps(ret)


@request_validation(_logger, secure=True)
def update_harvester_service_metrics(req: PandaRequest, harvester_id: str, metrics: str) -> str:
    """
    Update harvester service metrics

    **Requires secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        metrics: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    # convert and validate the metrics field
    try:
        metrics = json.loads(metrics)
    except ValueError:
        return json.dumps((False, MESSAGE_JSON))

    # update the metrics in the database
    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    ret = harvester_api.update_harvester_service_metrics(harvester_id, metrics)
    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"update_harvester_service_metrics {harvester_id} took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return json.dumps(ret)


@request_validation(_logger, secure=True)
def add_harvester_dialogs(req: PandaRequest, harvester_id: str, dialogs: str) -> str:
    """
    Add harvester dialog messages

    **Requires secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        dialogs: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """

    # convert and validate the dialogs field
    try:
        dialogs = json.loads(dialogs)
    except ValueError:
        return json.dumps((False, MESSAGE_JSON))

    ret = harvester_api.add_harvester_dialogs(harvester_id, dialogs)
    return json.dumps(ret)


@request_validation(_logger, secure=True)
def harvester_heartbeat(req: PandaRequest, harvester_id: str, data: str = None) -> str:
    """
    Heartbeat for harvester. User and host are retrieved from the request object and updated in the database.

    **Requires secure connection.**

    Args:
        req(PandaRequest): internally generated request object
        harvester_id(str): string containing the harvester id
        data: TODO

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    # get user and hostname to record in harvester metadata
    user = get_dn(req)
    host = req.get_remote_host()

    # convert and validate the data field
    try:
        if data:
            data = json.loads(data)
        else:
            data = dict()
    except ValueError:
        return json.dumps((False, MESSAGE_JSON))

    ret = harvester_api.harvester_heartbeat(user, host, harvester_id, data)
    return json.dumps(ret)


def get_worker_statistics(req: PandaRequest) -> str:
    """
    Get statistics for all the workers managed across the Grid.

    Args:
        req(PandaRequest): internally generated request object

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and the statistics or an error message, e.g. (False, 'Error message') or (True, {...}})
    """
    ret = harvester_api.get_worker_statistics()
    return json.dumps(ret)


@request_validation(_logger, secure=True)
def report_worker_statistics(req: PandaRequest, harvester_id: str, panda_queue: str, statistics: str) -> str:
    """
    Report statistics for the workers managed by a harvester instance at a PanDA queue.

    **Requires a secure connection.**

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

    ret = harvester_api.report_worker_statistics(harvester_id, panda_queue, statistics)
    return json.dumps(ret)


@request_validation(_logger, secure=True, production=True)
def add_sweep_harvester_command(req: PandaRequest, panda_queue: str, status_list: List[str], ce_list: List[str], submission_host_list: List[str]) -> str:
    """
    Send a command to harvester to kill the workers in a PanDA queue, with the possibility of specifying filters by status, CE or submission host.

    **Requires secure connection and production role.**

    Args:
        req(PandaRequest): internally generated request object
        panda_queue(str): name of the PanDA queue
        status_list: list of worker statuses to be considered, e.g. ['submitted', 'running']
        ce_list: list of the Computing Elements to be considered
        submission_host_list: list of the harvester submission hosts to be considered

    Returns
        str: json string with the result of the operation, typically a tuple with a boolean and a message, e.g. (False, 'Error message') or (True, 'OK')
    """
    ret = harvester_api.add_sweep_harvester_command(panda_queue, status_list, ce_list, submission_host_list)
    return json.dumps(ret)
