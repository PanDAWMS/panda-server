import json

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    MESSAGE_DATABASE,
    get_dn,
    has_production_role,
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer import PrioUtil
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("task_api")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True)
def retry(
    req: PandaRequest,
    jedi_task_id: int,
    new_parameters: str = None,
    no_child_retry: bool = None,
    disable_staging_mode: bool = None,
    keep_gshare_priority: bool = None,
) -> tuple[int, str]:
    """
    Retry a given task.

    **Requires POST method, and secure connection. Production role for others' task.**

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        new_parameters(str): a json string of new parameters the task uses when rerunning
        no_child_retry(bool): if True, the child tasks are not retried
        disable_staging_mode(bool): if True, the task skips staging state and directly goes to subsequent state
        keep_gshare_priority(bool): if True, the task keeps current gshare and priority

    Returns:
        str: json string with the result of the operation, typically a tuple with an integer and the statistics or an error message, e.g. (non-zero int, 'Error message') or (0, 'Successful messages')
    """
    user = get_dn(req)
    production_role = has_production_role(req)

    # retry with new params
    if new_parameters:
        try:
            # convert to dict
            new_parameters_dict = PrioUtil.decodeJSON(new_parameters)
            # get original params
            task_params = global_task_buffer.getTaskParamsPanda(jedi_task_id)
            task_params_dict = PrioUtil.decodeJSON(task_params)
            # update with new values
            task_params_dict.update(new_parameters_dict)
            task_params = json.dumps(task_params_dict)
            # retry with new params
            ret = global_task_buffer.insertTaskParamsPanda(
                task_params,
                user,
                production_role,
                [],
                properErrorCode=True,
                allowActiveTask=True,
            )
        except Exception as e:
            ret = 1, f"new parameter conversion failed with {str(e)}"
    else:
        # get command qualifier
        qualifier = ""
        for com_key, com_param in [
            ("sole", no_child_retry),
            ("discard", no_child_retry),
            ("staged", disable_staging_mode),
            ("keep", keep_gshare_priority),
        ]:
            if com_param:
                qualifier += f"{com_key} "
        qualifier = qualifier.strip()
        # normal retry
        ret = global_task_buffer.sendCommandTaskPanda(
            jedi_task_id,
            user,
            production_role,
            "retry",
            properErrorCode=True,
            comQualifier=qualifier,
        )
    return ret


@request_validation(_logger, secure=True, production=True)
def enable_job_cloning(
    req: PandaRequest,
    jedi_task_id: int,
    mode: str = None,
    multiplicity: int = None,
    num_sites: int = None,
) -> tuple[bool, str]:
    """
    Enable job cloning for a given task.

    **Requires POST method, secure connection and production role.**

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        mode(str): mode of operation, runonce or storeonce
        multiplicity(int): number of clones to be created for each target
        num_sites(int): number of sites to be used for each target

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and the statistics or an error message, e.g. (False, 'Error message') or (True, None)
    """
    tmp_logger = LogWrapper(_logger, f"enable_job_cloning jediTaskID=={jedi_task_id}")
    tmp_logger.debug(f"Start")
    return_tuple = global_task_buffer.enable_job_cloning(jedi_task_id, mode, multiplicity, num_sites)
    tmp_logger.debug(f"Done")
    return return_tuple


@request_validation(_logger, secure=True, production=True)
def disable_job_cloning(
    req: PandaRequest,
    jedi_task_id: int,
) -> tuple[bool, str]:
    """
    Disable job cloning for a given task.

    **Requires POST method, secure connection and production role.**

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        str: json string with the result of the operation, typically a tuple with a boolean and the statistics or an error message, e.g. (False, 'Error message') or (True, None)
    """
    tmp_logger = LogWrapper(_logger, f"disable_job_cloning jediTaskID=={jedi_task_id}")
    tmp_logger.debug(f"Start")
    return_tuple = global_task_buffer.disable_job_cloning(jedi_task_id)
    tmp_logger.debug(f"Done")
    return return_tuple
