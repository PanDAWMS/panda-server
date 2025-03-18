# See the task state diagram for a better understanding of some of the actions https://panda-wms.readthedocs.io/en/latest/terminology/terminology.html#task

import datetime
import json
import re
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    MESSAGE_TASK_ID,
    extract_production_working_groups,
    generate_response,
    get_dn,
    get_email_address,
    get_fqan,
    has_production_role,
    request_validation,
)
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_task")

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
    task_id: int,
    new_parameters: str = None,
    no_child_retry: bool = None,
    disable_staging_mode: bool = None,
    keep_gshare_priority: bool = None,
) -> Dict[str, Any]:
    """
    Task retry

    Retry a given task e.g. in exhausted state. Requires a secure connection without a production role to retry own tasks and with a production role to retry others' tasks.

    API details:
        HTTP Method: POST
        Path: /task/v1/retry

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        new_parameters(Dict, optional): a json dictionary with the new parameters for rerunning the task. The new parameters are merged with the existing ones.
                                        The parameters are the attributes in the JediTaskSpec object (https://github.com/PanDAWMS/panda-jedi/blob/master/pandajedi/jedicore/JediTaskSpec.py).
        no_child_retry(bool, optional): if True, the child tasks are not retried
        disable_staging_mode(bool, optional): if True, the task skips staging state and directly goes to subsequent state
        keep_gshare_priority(bool, optional): if True, the task keeps current gshare and priority

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"retry < task_id={task_id} >")
    tmp_logger.debug("Start")

    user = get_dn(req)
    production_role = has_production_role(req)

    # retry with new parameters
    if new_parameters:
        try:
            # get original parameters
            old_parameters_json = global_task_buffer.getTaskParamsPanda(task_id)
            old_parameters = json.loads(old_parameters_json)

            # update with new values
            old_parameters.update(new_parameters)
            final_task_parameters_json = json.dumps(old_parameters)

            # retry with new parameters
            ret = global_task_buffer.insertTaskParamsPanda(
                final_task_parameters_json,
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
            task_id,
            user,
            production_role,
            "retry",
            properErrorCode=True,
            comQualifier=qualifier,
        )
    tmp_logger.debug("Done")

    data, message = ret
    success = data == 0
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def resume(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Task resume

    Resume a given task. This transitions a paused or throttled task back to its previous active state. Resume can also be used to kick a task in staging state to the next state.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/resume

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"resume < jediTaskID={task_id} >")
    tmp_logger.debug("Start")

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(task_id, user, is_production_role, "resume", properErrorCode=True)
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def release(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Task release

    Release a given task. This triggers the avalanche for tasks in scouting state or dynamically reconfigures the task to skip over the scouting state.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/release

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    tmp_logger = LogWrapper(_logger, f"release < task_id={task_id} >")
    tmp_logger.debug("Start")

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(task_id, user, is_production_role, "release", properErrorCode=True)
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


# reassign task to site/cloud
@request_validation(_logger, secure=True, request_method="POST")
def reassign(req: PandaRequest, task_id: int, site: str = None, cloud: str = None, nucleus: str = None, soft: bool = None, mode: str = None):
    """
    Task reassign

    Reassign a given task to a site, nucleus or cloud - depending on the parameters. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/reassign

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        site(str, optional): site name
        cloud(str, optional): cloud name
        nucleus(str, optional): nucleus name
        soft(bool, optional): soft reassign
        mode(str, optional): soft/nokill reassign

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    tmp_logger = LogWrapper(_logger, f"reassign < task_id={task_id} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
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
    elif mode == "soft" or soft is True:
        comment += ":soft reassign"

    ret = global_task_buffer.sendCommandTaskPanda(
        task_id,
        user,
        is_production_role,
        "reassign",
        comComment=comment,
        properErrorCode=True,
    )
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def pause(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Task pause

    Pause a given task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/pause

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    tmp_logger = LogWrapper(_logger, f"pause < task_id={task_id} >")
    tmp_logger.debug("Start")

    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    user = get_dn(req)
    is_production_role = has_production_role(req)

    ret = global_task_buffer.sendCommandTaskPanda(task_id, user, is_production_role, "pause", properErrorCode=True)
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def kill(req: PandaRequest, task_id: int = None, broadcast: bool = False) -> Dict[str, Any]:
    """
    Task kill

    Kill a given task. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/kill

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        broadcast(bool, optional): broadcast kill command to pilots to kill the jobs

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"kill < task_id={task_id} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    user = get_dn(req)
    is_production_role = has_production_role(req)

    ret = global_task_buffer.sendCommandTaskPanda(
        task_id,
        user,
        is_production_role,
        "kill",
        properErrorCode=True,
        broadcast=broadcast,
    )
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def kill_unfinished_jobs(req: PandaRequest, task_id: int, code: int = None, use_email_as_id: bool = False):
    """
    Kill all unfinished jobs in a task

    Kills all unfinished jobs in a task. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/kill_unfinished_jobs

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        task_id (int): JEDI task ID
        code (int, optional): The kill code. Defaults to None.
        ```
        code
        2: expire
        3: aborted
        4: expire in waiting
        7: retry by server
        8: rebrokerage
        9: force kill
        10: fast rebrokerage in overloaded PQ
        50: kill by JEDI
        51: reassigned by JEDI
        52: force kill by JEDI
        55: killed since task is (almost) done
        60: workload was terminated by the pilot without actual work
        91: kill user jobs with prod role
        99: force kill user jobs with prod role
        ```
        use_email_as_id (bool, optional): Use the email as ID. Defaults to False.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. The data field contains a list of bools indicating the success of the kill operations.
    """

    tmp_logger = LogWrapper(_logger, f"kill_unfinished_jobs")

    # retrieve the user information
    user = get_dn(req)
    fqans = get_fqan(req)
    is_production_manager = has_production_role(req)

    if use_email_as_id:
        email = get_email_address(user, tmp_logger)
        if email:
            user = email

    tmp_logger.debug(f"Start user: {user} code: {code} is_production_manager: {is_production_manager} fqans: {fqans} task_id: {task_id}")

    # Extract working groups with production role from FQANs
    wg_prod_roles = extract_production_working_groups(fqans)

    # get PandaIDs
    job_ids = global_task_buffer.getPandaIDsWithTaskID(task_id)

    # kill jobs
    ret = global_task_buffer.killJobs(job_ids, user, code, is_production_manager, wg_prod_roles, [])
    tmp_logger.debug(f"Done with ret: {ret}")
    return generate_response(True, data=ret)


@request_validation(_logger, secure=True, request_method="POST")
def finish(req: PandaRequest, task_id: int, soft: bool = False, broadcast: bool = False) -> Dict[str, Any]:
    """
    Task finish

    Finish a given task. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/finish

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        soft(bool, optional): soft finish
        broadcast(bool, optional): broadcast finish command to pilots

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"finish < task_id={task_id} soft={soft} broadcast={broadcast} >")
    tmp_logger.debug("Start")

    qualifier = None
    if soft:
        qualifier = "soft"

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(
        task_id,
        user,
        is_production_role,
        "finish",
        properErrorCode=True,
        comQualifier=qualifier,
        broadcast=broadcast,
    )
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def reactivate(req: PandaRequest, task_id: int, keep_attempt_nr: bool = False, trigger_job_generation: bool = False) -> Dict[str, Any]:
    """
    Reactivate task

    Reactivate a given task, i.e. recycle a finished/done task. A reactivated task will generate new jobs and then go to done/finished.
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reactivate

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        keep_attempt_nr(bool, optional): keep the original attempt number
        trigger_job_generation(bool, optional): trigger the job generation

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"reactivate < task_id={task_id} >")
    tmp_logger.debug("Start")

    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.reactivateTask(task_id, keep_attempt_nr, trigger_job_generation)
    code, message = ret
    success = code == 0

    tmp_logger.debug("Done")

    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def avalanche(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Task avalanche

    Avalanche a given task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/avalanche

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"avalanche < task_id={task_id} >")
    tmp_logger.debug("Start")

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(task_id, user, is_production_role, "avalanche", properErrorCode=True)
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, request_method="POST")
def reload_input(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Reload input

    Request to reload the input for a given task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reload_input

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"reload_input < task_id={task_id} >")
    tmp_logger.debug("Start")

    user = get_dn(req)
    is_production_role = has_production_role(req)

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.sendCommandTaskPanda(task_id, user, is_production_role, "incexec", comComment="{}", propeErrorCode=True)
    data, message = ret
    success = data == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def reassign_global_share(req: PandaRequest, task_id_list: List, share: str, reassign_running_jobs: bool) -> Dict[str, Any]:
    """
    Reassign the global share of a task

    Reassign the global share of a task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reassign_global_share

    Args:
        req(PandaRequest): internally generated request object
        task_id_list(List): List of JEDI task IDs to reassign
        share(str): destination share
        reassign_running_jobs(bool): whether you want to reassign existing running jobs

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    tmp_logger = LogWrapper(_logger, f"reassign_global_share < task_id_list={task_id_list} share={share} reassign_running_jobs={reassign_running_jobs} >")
    tmp_logger.debug("Start")

    if not isinstance(task_id_list, list) or not isinstance(share, str):
        tmp_logger.error("Failed due to invalid task list")
        return generate_response(False, message="wrong parameters: task_ids must be list and share must be string")

    code, message = global_task_buffer.reassignShare(task_id_list, share, reassign_running_jobs)
    success = code == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, code)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def enable_jumbo_jobs(req: PandaRequest, task_id: int, jumbo_jobs_total: int, jumbo_jobs_per_site: int = None):
    """
    Enable Jumbo jobs

    Enables the Jumbo jobs for a given task ID. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/reassign_global_share

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        jumbo_jobs_total(int): Total number of jumbo jobs
        jumbo_jobs_per_site(int): Number of jumbo jobs per site. Defaults to `jumbo_jobs_total`.

    Returns:
        dict: The system response. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """

    tmp_logger = LogWrapper(_logger, f"enable_jumbo_jobs < task_id={task_id} jumbo_jobs_total={jumbo_jobs_total} n_jumbo_jobs_per_site={jumbo_jobs_per_site} >")
    tmp_logger.debug("Start")

    if not jumbo_jobs_per_site:
        jumbo_jobs_per_site = jumbo_jobs_total

    code, message = global_task_buffer.enableJumboJobs(task_id, jumbo_jobs_total, jumbo_jobs_per_site)
    if jumbo_jobs_total > 0 and code == 0:
        tmp_logger.debug("Calling task avalanche")
        avalanche(task_id)

    success = code == 0

    tmp_logger.debug("Done")
    return generate_response(success, message, code)


@request_validation(_logger, secure=True, request_method="GET")
def get_jumbo_job_datasets(req: PandaRequest, from_offset: int, to_offset: int = 0) -> Dict:
    """
    Get jumbo job datasets

    Gets a map of the jumbo-job-enabled tasks to their datasets, filtering by the last modification time (now - from_offset to now - to_offset).
    Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_jumbo_job_datasets

    Args:
        req(PandaRequest): internally generated request object
        from_offset(int): `now - from_offset` in days will serve as the floor for modification time (Previously called n_days)
        to_offset(int, optional): `now - to_offset` in days will serve as the ceiling for modification time. Defaults to 0. (Previously called grace_period)

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the dictionary of JEDI task IDs to datasets.
    """
    tmp_logger = LogWrapper(_logger, f"get_jumbo_job_datasets")

    tmp_logger.debug("Start")
    jumbo_datasets = global_task_buffer.getJumboJobDatasets(from_offset, to_offset)
    tmp_logger.debug("Done")

    return generate_response(True, data=jumbo_datasets)


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
        mode(str, optional): mode of operation, runonce or storeonce
        multiplicity(int, optional): number of clones to be created for each target
        num_sites(int, optional): number of sites to be used for each target

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When there was an error, the message field contains the description.
    """
    tmp_logger = LogWrapper(_logger, f"enable_job_cloning < jedi_task_id={jedi_task_id} >")
    tmp_logger.debug("Start")
    success, message = global_task_buffer.enable_job_cloning(jedi_task_id, mode, multiplicity, num_sites)
    tmp_logger.debug("Done")
    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def disable_job_cloning(req: PandaRequest, jedi_task_id: int) -> Dict[str, Any]:
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
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When there was an error, the message field contains the description.
    """
    tmp_logger = LogWrapper(_logger, f"disable_job_cloning < jedi_task_id={jedi_task_id} >")
    tmp_logger.debug("Start")
    success, message = global_task_buffer.disable_job_cloning(jedi_task_id)
    tmp_logger.debug("Done")
    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def increase_attempts(req: PandaRequest, task_id: int, increase: int) -> Dict[str, Any]:
    """
    Increase possible task attempts

    Increase possible task attempts. Requires secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/increase_attempts

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        increase(int): number of attempts to increase

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When there was an error, the message field contains the description and the data field contains the code.
    """
    tmp_logger = LogWrapper(_logger, f"increase_attempt_number task_id={task_id}")
    tmp_logger.debug("Start")

    try:
        task_id = int(task_id)
    except Exception:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # check value for increase
    try:
        increase = int(increase)
        if increase < 0:
            raise ValueError
    except Exception:
        message = f"increase must be a positive integer, got {increase}"
        tmp_logger.error(message)
        return generate_response(False, message=message)

    code, message = global_task_buffer.increaseAttemptNrPanda(task_id, increase)
    success = code == 0

    tmp_logger.debug("Done")

    return generate_response(success, message, code)


@request_validation(_logger, secure=True, request_method="GET")
def get_status(req, task_id):
    """
    Get task status

    Get the status of a given task. Requires secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_status

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When successful, the data field contains the status of the task.
              When there was an error, the message field contains the description.
    """
    tmp_logger = LogWrapper(_logger, f"get_status < task_id={task_id} >")
    tmp_logger.debug("Start")

    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    ret = global_task_buffer.getTaskStatus(task_id)
    if not ret:
        return generate_response(False, message="Task not found")
    status = ret[0]

    tmp_logger.debug("Done")

    return generate_response(True, data=status)


@request_validation(_logger, request_method="GET", secure=True)
def get_details(req: PandaRequest, task_id: int, include_parameters: bool = False, include_status: bool = False):
    """
    Get task details

    Get the details of a given task. Requires secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_details

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI Task ID
        include_parameters(bool, optional): flag to include task parameter information (Previously fullFlag)
        include_status(bool, optional): flag to include status information (Previously withTaskInfo)

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              When succesful, there data field contains the task details.
              When there was an error, the message field contains the description.
    """
    tmp_logger = LogWrapper(_logger, f"get_details  < task_id={task_id} include_parameters={include_parameters} include_status={include_status} >")
    tmp_logger.debug("Start")

    details = global_task_buffer.getJediTaskDetails(task_id, include_parameters, include_status)
    if not details:
        tmp_logger.error("Task not found or error retrieving the details")
        return generate_response(False, message="Task not found or error retrieving the details")

    tmp_logger.debug("Done")

    return generate_response(True, data=details)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_attribute(req: PandaRequest, task_id: int, attribute_name: str, value: int) -> Dict[str, Any]:
    """
    Change a task attribute

    Change a task attribute within the list of valid attributes ("ramCount", "wallTime", "cpuTime", "coreCount").
    Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/change_attribute

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        attribute_name(str): attribute to change
        value(int): value to set to the attribute

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
              Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"change_attribute < task_id={task_id} attribute_name={attribute_name} value={value} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # check if attribute_name is valid
    valid_attributes = ["ramCount", "wallTime", "cpuTime", "coreCount"]
    if attribute_name not in valid_attributes:
        tmp_logger.error("Failed due to invalid attribute_name")
        return generate_response(False, message=f"{attribute_name} is not a valid attribute. Valid attributes are {valid_attributes}")

    n_tasks_changed = global_task_buffer.changeTaskAttributePanda(task_id, attribute_name, value)
    if n_tasks_changed is None:  # method excepted
        tmp_logger.error("Failed due to exception while changing the attribute")
        return generate_response(False, message="Exception while changing the attribute")

    if n_tasks_changed == 0:  # no tasks were changed should mean that it doesn't exist
        tmp_logger.error("Failed due to task not found")
        return generate_response(False, message="Task not found")

    tmp_logger.debug("Done")
    return generate_response(True, message=f"{n_tasks_changed} tasks changed")


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_modification_time(req: PandaRequest, task_id: int, positive_hour_offset: int) -> Dict[str, Any]:
    """
    Change task modification time

    Change the modification time for a task to `now() + positive_hour_offset`. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/change_modification_time

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        positive_hour_offset(int): number of hours to add to the current time

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"change_modification_time < task_id={task_id} positive_hour_offset={positive_hour_offset} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # check offset
    try:
        positive_hour_offset = int(positive_hour_offset)
        new_modification_time = datetime.datetime.now() + datetime.timedelta(hours=positive_hour_offset)
    except ValueError:
        tmp_logger.error("Failed due to invalid positive_hour_offset")
        return generate_response(False, message=f"failed to convert {positive_hour_offset} to time")

    n_tasks_changed = global_task_buffer.changeTaskAttributePanda(task_id, "modificationTime", new_modification_time)
    if n_tasks_changed is None:  # method excepted
        tmp_logger.error("Failed due to exception while changing the attribute")
        return generate_response(False, message="Exception while changing the attribute")

    if n_tasks_changed == 0:  # no tasks were changed should mean that it doesn't exist
        tmp_logger.error("Failed due to task not found")
        return generate_response(False, message="Task not found")

    tmp_logger.debug("Done")
    return generate_response(True, message=f"{n_tasks_changed} tasks changed")


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_priority(req: PandaRequest, task_id: int, priority: int):
    """
    Change priority

    Change the priority of a given task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/change_priority

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        priority(int): new priority for the task

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
              True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"change_priority < task_id={task_id} priority={priority} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # check priority
    try:
        priority = int(priority)
    except ValueError:
        tmp_logger.error("Failed due to invalid priority")
        return generate_response(False, message="priority must be an integer")

    n_tasks_changed = global_task_buffer.changeTaskPriorityPanda(task_id, priority)

    if n_tasks_changed is None:  # method excepted
        tmp_logger.error("Failed due to exception while changing the priority")
        return generate_response(False, message="Exception while changing the priority")

    if n_tasks_changed == 0:  # no tasks were changed should mean that it doesn't exist
        tmp_logger.error("Failed due to task not found")
        return generate_response(False, message="Task not found")

    tmp_logger.debug("Done")

    return generate_response(True, message=f"{n_tasks_changed} tasks changed")


@request_validation(_logger, secure=True, production=True, request_method="POST")
def change_split_rule(req: PandaRequest, task_id: int, attribute_name: str, value: int) -> Dict[str, Any]:
    """
    Change the split rule

    Change the split rule for a task. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /task/v1/change_split_rule

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        attribute_name(str): split rule attribute to change
        value(int): value to set to the attribute

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
              Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"change_split_rule < task_id={task_id} attribute_name={attribute_name} value={value} >")
    tmp_logger.debug("Start")

    # check task_id
    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # see what the attributes mean in pandaserver/taskbuffer/task_split_rules.py
    valid_attributes = ["AI", "TW", "EC", "ES", "MF", "NG", "NI", "NF", "NJ", "AV", "IL", "LI", "LC", "CC", "OT", "UZ"]
    # check attribute
    if attribute_name not in valid_attributes:
        tmp_logger.error("Failed due to invalid attribute_name")
        return generate_response(False, message=f"{attribute_name} is not a valid attribute. Valid attributes are {valid_attributes}", data=2)

    n_tasks_changed = global_task_buffer.changeTaskSplitRulePanda(task_id, attribute_name, value)
    if n_tasks_changed is None:  # method excepted
        tmp_logger.error("Failed due to exception while changing the split rule")
        return generate_response(False, message="Exception while changing the split rule")

    if n_tasks_changed == 0:  # no tasks were changed should mean that it doesn't exist
        tmp_logger.error("Failed due to task not found")
        return generate_response(False, message="Task not found")

    tmp_logger.debug("Done")

    return generate_response(True, message=f"{n_tasks_changed} tasks changed")


@request_validation(_logger, secure=True, request_method="GET")
def get_tasks_modified_since(req, since: str, dn: str = "", full: bool = False, min_task_id: int = None, prod_source_label: str = "user") -> Dict[str, Any]:
    """
    Get tasks modified since

    Get the tasks with `modificationtime > since`. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_tasks_modified_since

    Args:
        req(PandaRequest): internally generated request object
        since(str): time in the format `%Y-%m-%d %H:%M:%S`, e.g. `2024-12-18 14:30:45`. The tasks with `modificationtime > since` will be returned
        dn(str, optional): user DN
        full(bool, optional): flag to include full task information. If `full=False` the basic fields are `jediTaskID, modificationTime, status, processingType, transUses, transHome, architecture, reqID, creationDate, site, cloud, taskName`
        min_task_id(int, optional): minimum task ID
        prod_source_label(str, optional): task type (e.g. `user`, `managed`, `test`, etc.)

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, "get_tasks_modified_since")
    tmp_logger.debug("Start")

    if not dn:
        dn = get_dn(req)

    try:
        min_task_id = int(min_task_id)
    except ValueError:
        min_task_id = None

    tmp_logger.debug(f"parameters dn:{dn} since:{since} full:{full} min_task_id:{min_task_id} prod_source_label:{prod_source_label}")

    tasks = global_task_buffer.getJediTasksInTimeRange(dn, since, full, min_task_id, prod_source_label)

    tmp_logger.debug("Done")

    return generate_response(True, data=tasks)


@request_validation(_logger, secure=True, request_method="GET")
def get_datasets_and_files(req, task_id, dataset_types: List = ("input", "pseudo_input")) -> Dict[str, Any]:
    """
    Get datasets and files

    Get the files in the datasets associated to a given task. You can filter passing a list of dataset types. The return format is:
    ```
    [
        {
            "dataset": {
                "name": dataset_name,
                "id": dataset_id
            },
            "files": [
                {
                    "lfn": lfn,
                    "scope": file_scope,
                    "id": file_id,
                    "status": status
                },
                ...
            ]
        },
        ...
    ]
    ```
    Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_datasets_and_files

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID
        dataset_types(List, optional): list of dataset types, defaults to `["input", "pseudo_input"]`

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"get_datasets_and_files < task_id={task_id} dataset_types={dataset_types} >")
    tmp_logger.debug("Start")

    data = global_task_buffer.get_files_in_datasets(task_id, dataset_types)
    if data is None:
        tmp_logger.error("Failed due to exception while gathering files")
        return generate_response(False, message="Database exception while gathering files")

    if data == []:
        tmp_logger.error("Failed due to no data found for the task")
        return generate_response(False, message="No data found for the task")

    tmp_logger.debug("Done")

    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_job_ids(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Get job IDs

    Get a list with the job IDs `[job_id, ...]` (in any status) associated to a given task. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_job_ids

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.
    """
    tmp_logger = LogWrapper(_logger, f"get_job_ids < task_id={task_id} >")
    tmp_logger.debug("Start")

    try:
        task_id = int(task_id)
    except ValueError:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    job_id_list = global_task_buffer.getPandaIDsWithTaskID(task_id)

    tmp_logger.debug("Done")

    return generate_response(True, data=job_id_list)


@request_validation(_logger, secure=True, request_method="POST")
def submit(req: PandaRequest, task_parameters: Dict, parent_tid: int = None) -> Dict[str, Any]:
    """
    Register task

    Insert the task parameters to register a task. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /task/v1/submit

    Args:
        req(PandaRequest): internally generated request object
        task_parameters(dict): Dictionary with all the required task parameters. The parameters are the attributes in the JediTaskSpec object (https://github.com/PanDAWMS/panda-jedi/blob/master/pandajedi/jedicore/JediTaskSpec.py).
        parent_tid(int, optional): Parent task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
              Return code in the data field, 0 for success, others for failure.
    """
    tmp_log = LogWrapper(_logger, f"submit {datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_log.debug("Start")

    user = get_dn(req)
    is_production_role = has_production_role(req)
    fqans = get_fqan(req)

    tmp_log.debug(f"user={user} is_production_role={is_production_role} FQAN:{str(fqans)} parent_tid={parent_tid}")
    ret = global_task_buffer.insertTaskParamsPanda(task_parameters, user, is_production_role, fqans, properErrorCode=True, parent_tid=parent_tid, decode=False)

    code, message = ret
    success = code == 0
    if not success:
        return generate_response(False, message=message, data=code)

    # Extract the task ID from the message
    task_id = None
    match = re.search(r"jediTaskID=(\d+)", message)
    if match:
        try:
            task_id = int(match.group(1))  # Convert to an integer
            tmp_log.debug(f"Created task with task_id: {task_id}")
        except ValueError:
            tmp_log.error("Failed to extract the task ID from the message")
            task_id = None

    tmp_log.debug("Done")

    return generate_response(True, message=message, data=task_id)


@request_validation(_logger, request_method="GET")
def get_task_parameters(req: PandaRequest, task_id: int) -> Dict[str, Any]:
    """
    Get task parameters

    Get a dictionary with the task parameters used to create a task.

    API details:
        HTTP Method: GET
        Path: /task/v1/get_task_parameters

    Args:
        req(PandaRequest): internally generated request object
        task_id(int): JEDI task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message. Return code in the data field, 0 for success, others for failure.

    """
    tmp_logger = LogWrapper(_logger, f"get_task_parameters < task_id={task_id} >")
    tmp_logger.debug("Start")

    # validate the task id
    try:
        task_id = int(task_id)
    except Exception:
        tmp_logger.error("Failed due to invalid task_id")
        return generate_response(False, message=MESSAGE_TASK_ID)

    # get the parameters
    task_parameters_str = global_task_buffer.getTaskParamsMap(task_id)
    if not task_parameters_str:
        tmp_logger.error("Failed due to task not found")
        return generate_response(False, message="Task not found")

    # decode the parameters
    try:
        task_parameters = json.loads(task_parameters_str)
    except json.JSONDecodeError as e:
        tmp_logger.error("Failed due to error decoding the task parameters")
        return generate_response(False, message=f"Error decoding the task parameters: {str(e)}")

    tmp_logger.debug("Done")

    return generate_response(True, data=task_parameters)
