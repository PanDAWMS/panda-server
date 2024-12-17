import json
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    MESSAGE_TASK_ID,
    generate_response,
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


@request_validation(_logger, secure=True, request_method="POST")
def retry(
    req: PandaRequest,
    jedi_task_id: int,
    new_parameters: str = None,
    no_child_retry: bool = None,
    disable_staging_mode: bool = None,
    keep_gshare_priority: bool = None,
) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: POST
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        new_parameters(str): a json string of new parameters the task uses when rerunning
        no_child_retry(bool): if True, the child tasks are not retried
        disable_staging_mode(bool): if True, the task skips staging state and directly goes to subsequent state
        keep_gshare_priority(bool): if True, the task keeps current gshare and priority

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
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
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def resume(req: PandaRequest, jedi_task_id: int) -> Dict[str, Any]:
    """
    Task resume

    Resume a given task. This transitions a paused or throttled task back to its previous active state. Resume can also be used to kick a task in staging state to the next state.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/resume

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check jedi_task_id
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(jedi_task_id, user, is_production_role, "resume", properErrorCode=True)
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def release(req: PandaRequest, jedi_task_id: int) -> Dict[str, Any]:
    """
    Task release

    Release a given task. This triggers the avalanche for tasks in scouting state or dynamically reconfigures the task to skip over the scouting state.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/release

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check jedi_task_id
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(jedi_task_id, user, is_production_role, "release", properErrorCode=True)
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


# reassign task to site/cloud
@request_validation(_logger, secure=True, request_method="POST")
def reassign(req: PandaRequest, jedi_task_id: int, site: str = None, cloud: str = None, nucleus: str = None, soft: bool = None, mode: str = None):
    """
    Task reassign

    Reassign a given task to a site, nucleus or cloud - depending on the parameters. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/reassign

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        site(str, optional): site name
        cloud(str, optional): cloud name
        nucleus(str, optional): nucleus name
        soft(bool, optional): soft reassign
        mode(str, optional): soft/nokill reassign

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    # check jedi_task_id
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # reassign to site, nucleus or cloud
    if site:
        comment = f"site:{site}:y"  # set 'y' to go back to oldStatus immediately
    elif nucleus:
        comment = f"nucleus:{nucleus}:n"
    else:
        comment = f"cloud:{cloud}:n"

    # set additional modes
    if mode == "nokill":
        comment += ":nokill reassign"
    elif mode == "soft" or soft == True:
        comment += ":soft reassign"

    ret = global_task_buffer.sendCommandTaskPanda(
        jedi_task_id,
        user,
        is_production_role,
        "reassign",
        comComment=comment,
        properErrorCode=True,
    )
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def pause(req: PandaRequest, jedi_task_id: int) -> Dict[str, Any]:
    """
    Task pause

    Pause a given task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/pause

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    user = get_dn(req)
    is_production_role = has_production_role(req)

    ret = global_task_buffer.sendCommandTaskPanda(jedi_task_id, user, is_production_role, "pause", properErrorCode=True)
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def kill(req, jedi_task_id=None, broadcast=False):
    """
    Task kill

    Kill a given task. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/kill

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        broadcast(bool, optional): broadcast kill command to pilots to kill the jobs

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    # check jedi_task_id
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    user = get_dn(req)
    is_production_role = has_production_role(req)

    ret = global_task_buffer.sendCommandTaskPanda(
        jedi_task_id,
        user,
        is_production_role,
        "kill",
        properErrorCode=True,
        broadcast=broadcast,
    )
    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def reactivate(req: PandaRequest, jedi_task_id: int, keep_attempt_nr: bool = False, trigger_job_generation: bool = False):
    """
    Reactivate task

    Reactivate a given task, i.e. recycle a finished/done task. A reactivated task will generate new jobs and then go to done/finished.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reactivate

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        keep_attempt_nr(bool, optional): keep the original attempt number
        trigger_job_generation(bool, optional): trigger the job generation

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.reactivateTask(jedi_task_id, keep_attempt_nr, trigger_job_generation)
    code, message = ret
    success = code == 0
    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def reassign_global_share(req: PandaRequest, jedi_task_id_list: List, share: str, reassign_running_jobs: bool) -> Dict[str, Any]:
    """
    Reassign the global share of a task

    Reassign the global share of a task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reassign_global_share

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id_list(List): List of JEDI task IDs to reassign
        share(str): destination share
        reassign_running_jobs(bool): whether you want to reassign existing running jobs

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    _logger.debug(f"reassignShare: jedi_task_ids: {jedi_task_id_list}, share: {share}, reassign_running: {reassign_running_jobs}")

    if not isinstance(jedi_task_id_list, list) or not isinstance(share, str):
        return generate_response(False, message="wrong parameters: jedi_task_ids must be list and share must be string")

    code, message = global_task_buffer.reassignShare(jedi_task_id_list, share, reassign_running_jobs)
    success = code == 0
    return generate_response(success, message, code)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def enable_job_cloning(
    req: PandaRequest,
    jedi_task_id: int,
    mode: str = None,
    multiplicity: int = None,
    num_sites: int = None,
) -> Dict[str, Any]:
    """
    Enable job cloning

    Enable job cloning for a given task. Requires secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/enable_job_cloning

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID
        mode(str): mode of operation, runonce or storeonce
        multiplicity(int): number of clones to be created for each target
        num_sites(int): number of sites to be used for each target

    Returns:
        dict: The system response. True for success, False for failure, and an error message.
    """
    tmp_logger = LogWrapper(_logger, f"enable_job_cloning jediTaskID=={jedi_task_id}")
    tmp_logger.debug(f"Start")
    success, message = global_task_buffer.enable_job_cloning(jedi_task_id, mode, multiplicity, num_sites)
    tmp_logger.debug(f"Done")
    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def disable_job_cloning(
    req: PandaRequest,
    jedi_task_id: int,
) -> Dict[str, Any]:
    """
    Disable job cloning

    Disable job cloning for a given task. Requires secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/disable_job_cloning

    Args:
        req(PandaRequest): internally generated request object
        jedi_task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message.
    """
    tmp_logger = LogWrapper(_logger, f"disable_job_cloning jediTaskID=={jedi_task_id}")
    tmp_logger.debug(f"Start")
    success, message = global_task_buffer.disable_job_cloning(jedi_task_id)
    tmp_logger.debug(f"Done")
    return generate_response(success, message)
