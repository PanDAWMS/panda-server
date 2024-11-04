import datetime
import json

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

global_task_buffer = None


def init_task_buffer(task_buffer):
    global global_task_buffer
    global_task_buffer = task_buffer


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

    ret = global_task_buffer.updateWorkers(harvester_id, workers)
    if not ret:
        return_tuple = False, MESSAGE_DATABASE
    else:
        return_tuple = True, ret

    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"update_workers {harvester_id} took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return json.dumps(return_tuple)
