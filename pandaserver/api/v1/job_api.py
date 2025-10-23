import sys
import traceback
from typing import Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    TimedMethod,
    extract_primary_production_working_group,
    extract_production_working_groups,
    generate_response,
    get_dn,
    get_email_address,
    get_fqan,
    has_production_role,
    request_validation,
)
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_job")

global_task_buffer = None


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, request_method="GET")
def get_status(req: PandaRequest, job_ids: List[int], timeout: int = 60) -> Dict:
    """
    Get status of a job.

    Gets the status for a job and command to the pilot if any. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/job/get_status

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_ids(list of int): list of PanDA job IDs.
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains a list of tupes with (status, command). When unsuccessful, the message field contains the error message and data an error code.
    """
    tmp_logger = LogWrapper(_logger, f"get_status job_ids={job_ids} timeout={timeout}")
    tmp_logger.debug("Start")

    # The task buffer method expect a comma separated list of job_ids
    job_ids_str = ",".join([str(job_id) for job_id in job_ids])
    timed_method = TimedMethod(global_task_buffer.checkJobStatus, timeout)
    timed_method.run(job_ids_str)

    # Time out
    if timed_method.result == Protocol.TimeOutToken:
        tmp_logger.debug("Timed out")
        return generate_response(False, message="time out", data={"code": Protocol.SC_TimeOut})

    # No result
    if not isinstance(timed_method.result, list):
        tmp_logger.debug(f"Failed")
        return generate_response(False, message="failed", data={"code": Protocol.SC_Failed})

    # Success
    data = timed_method.result
    tmp_logger.debug(f"Done with data={data}")
    return generate_response(True, data=data)


@request_validation(_logger, secure=True, request_method="GET")
def get_description(self, job_ids: List[int]) -> Dict:
    """
    Get description of a job.

    Gets the description of a job from the main/active schema. The description includes job attributes, job parameters and related file attributes. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/job/get_description

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_ids (list of int): List of PanDA job IDs.
        timeout (int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains a list with job descriptions. When unsuccessful, the message field contains the error message and data an error code.
    """
    tmp_logger = LogWrapper(_logger, f"get_description")
    tmp_logger.debug("Start")

    try:
        tmp_logger.debug(f"Number of requested PanDA IDs: {len(job_ids)}")
        max_ids = 5500
        if len(job_ids) > max_ids:
            tmp_logger.error(f"List of PanDA IDs is longer than {max_ids}. Truncating the list.")
            job_ids = job_ids[:max_ids]
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        tmp_logger.error(f"Failed deserializing the list of PanDA IDs: {error_type} {error_value}")
        job_ids = []

    tmp_logger.debug(f"Retrieving data for {job_ids}")
    ret = global_task_buffer.peekJobs(job_ids, use_json=True)
    _logger.debug("Done")

    return generate_response(True, data=ret)


@request_validation(_logger, secure=True, request_method="GET")
def get_description_incl_archive(req: PandaRequest, job_ids: List[int]) -> Dict:
    """
    Get description of a job.

    Gets the description of a job, also looking into the secondary/archive schema. The description includes job attributes, job parameters and related file attributes. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/job/get_description_incl_archive

    Args:
        req(PandaRequest): internally generated request object containing the env variables.
        job_ids (list of int): List of PanDA job IDs.
        timeout (int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains a list with job descriptions. When unsuccessful, the message field contains the error message and data an error code.
    """
    tmp_logger = LogWrapper(_logger, f"get_description_including_archive")
    tmp_logger.debug("Start")

    try:
        tmp_logger.debug(f"Number of requested PanDA IDs: {len(job_ids)}")
        max_ids = 5500
        if len(job_ids) > max_ids:
            tmp_logger.error(f"List of PanDA IDs is longer than {max_ids}. Truncating the list.")
            job_ids = job_ids[:max_ids]
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        tmp_logger.error(f"Failed deserializing the list of PanDA IDs: {error_type} {error_value}")
        job_ids = []

    tmp_logger.debug(f"Retrieving data for {str(job_ids)}")

    ret = global_task_buffer.getFullJobStatus(job_ids, use_json=True)
    tmp_logger.debug("getFullJobStatus end")
    return generate_response(True, data=ret)


@request_validation(_logger, secure=False, request_method="GET")
def generate_offline_execution_script(req: PandaRequest, job_id: int, days: int = None) -> Dict:
    """
    Get execution script for a job.

    Gets the execution script for a job, including Rucio download of input, ALRB setup, downloading transformation script and running the script. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/job/generate_offline_execution_script

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_id (int): PanDA job ID
        timeout (int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field will contain `{"script": script}`. When unsuccessful, the message field contains the error message and data an error code.
    """
    tmp_logger = LogWrapper(_logger, f"generate_offline_execution_script job_id={job_id} days={days}")
    tmp_logger.debug("Start")
    script = global_task_buffer.getScriptOfflineRunning(job_id, days)

    if script.startswith("ERROR"):
        tmp_logger.debug(f"Failed to generate script: {script}")
        return script

    tmp_logger.debug("Done")
    return script


@request_validation(_logger, secure=True, request_method="GET")
def get_metadata_for_analysis_jobs(req: PandaRequest, task_id: int) -> Dict:
    """
    Get metadata for analysis jobs

    Gets the metadata from the metatable for analysis jobs in `finished` status. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/job/get_metadata_for_analysis_jobs

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        task_id (int): JEDI task ID

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. When successful, the data field contains the medata. When unsuccessful, the message field contains the error message and data an error code.
    """

    tmp_logger = LogWrapper(_logger, f"get_metadata_for_analysis_jobs task_id={task_id}")
    tmp_logger.debug("Start")

    metadata = global_task_buffer.getUserJobMetadata(task_id)
    if not metadata:
        tmp_logger.debug("No metadata found")
        return generate_response(True, message="No metadata found", data={})

    tmp_logger.debug("Done")
    return generate_response(True, data=metadata)


@request_validation(_logger, secure=True, request_method="POST")
def kill(req, job_ids: List[int], code: int = None, use_email_as_id: bool = False, kill_options: List[str] = []):
    """
    Kill the jobs

    Kills the jobs with the given PanDA IDs. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/job/kill

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_ids (list): List of PanDA job IDs
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
        kill_options (List, optional): Defaults to []. Possible options are: `keepUnmerged`, `jobSubStatus=xyz`

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. The data field contains a list of bools indicating the success of the kill operations.
    """

    # retrieve the user information
    user = get_dn(req)
    is_production_manager = has_production_role(req)
    fqans = get_fqan(req)

    tmp_logger = LogWrapper(_logger, f"kill")
    tmp_logger.debug(f"Start user: {user} code: {code} is_production_manager: {is_production_manager} fqans: {fqans} job_ids: {job_ids}")

    # Get the user's email address if use_email_as_id is set
    if use_email_as_id:
        email = get_email_address(user)
        if email:
            user = email

    # Extract working groups with production role from FQANs
    wg_prod_roles = extract_production_working_groups(fqans)

    # kill jobs
    ret = global_task_buffer.killJobs(job_ids, user, code, is_production_manager, wg_prod_roles, kill_options)
    tmp_logger.debug(f"Done with ret: {ret}")
    return generate_response(True, data=ret)


@request_validation(_logger, secure=True, request_method="POST")
def reassign(req: PandaRequest, job_ids: List[int]):
    """
    Reassign a list of jobs

    Reassigns a list of jobs. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/job/reassign

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_ids (list): List of PanDA job IDs

    Returns:
        dict: The system response `{"success": True}`.
    """
    # TODO: think about the default values for for_pending and first_submission. The resolve_true and resolve_false functions are confusing me.

    tmp_logger = LogWrapper(_logger, f"reassign job_ids={job_ids}")
    tmp_logger.debug("Start")
    # taskBuffer.reassignJobs always returns True
    global_task_buffer.reassignJobs(job_ids)
    tmp_logger.debug(f"Done")
    return generate_response(True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def set_command(req: PandaRequest, job_id: int, command: str):
    """
    Set a pilot command

    Sets a command to the pilot for a job. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/job/set_command

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_id (int): PanDA job ID
        command (str): The command for the pilot, e.g. `tobekilled`

    Returns:
        dict: The system response `{"success": success, "message": message}`.
    """
    tmp_logger = LogWrapper(_logger, f"set_command job_id={job_id} command={command}")
    tmp_logger.debug("Start")
    success, message = global_task_buffer.send_command_to_job(job_id, command)
    tmp_logger.debug("Done with success={success} message={message}")
    return generate_response(success, message=message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def set_debug_mode(req: PandaRequest, job_id: int, mode: bool):
    """
    Set the debug mode

    Sets the debug mode for a job. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/job/set_debug_mode

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_id (int): PanDA job ID
        mode (bool): True to set debug mode, False to unset debug mode

    Returns:
        dict: The system response `{"success": success, "message": message}`.
    """

    tmp_logger = LogWrapper(_logger, f"set_debug_mode job_id={job_id}")

    user = get_dn(req)
    is_production_manager = has_production_role(req)
    fqans = get_fqan(req)

    # get the primary working group with prod role
    working_group = extract_primary_production_working_group(fqans)

    tmp_logger.debug(f"Start user={user} mgr={is_production_manager} wg={working_group} fqans={str(fqans)}")

    message = global_task_buffer.setDebugMode(user, job_id, is_production_manager, mode, working_group)

    success = False
    if message != "Succeeded":
        success = True

    return generate_response(success, message=message)


@request_validation(_logger, secure=True, request_method="POST")
def submit(req: PandaRequest, jobs: str):
    """
    Submit jobs

    Sets the debug mode for a job. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/job/submit

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        jobs (str): JSON string with a list of job specs

    Returns:
        dict: The system response `{"success": success, "message": message}`.
    """
    tmp_logger = LogWrapper(_logger, f"submit")
    user = get_dn(req)
    fqans = get_fqan(req)
    is_production_role = has_production_role(req)
    host = req.get_remote_host()

    # deserialize job specs
    try:
        jobs = JobUtils.load_jobs_json(jobs)
        tmp_logger.debug(f"{user} len:{len(jobs)} is_production_role={is_production_role} FQAN:{fqans}")
        max_jobs = 5000
        if len(jobs) > max_jobs:
            _logger.error(f"Number of jobs exceeded maximum {max_jobs}. Truncating the list.")
            jobs = jobs[:max_jobs]
    except Exception as ex:
        error_message = f"Failed to deserialize jobs: {str(ex)} {traceback.format_exc()}"
        tmp_logger.error(error_message)
        return generate_response(False, message=error_message)

    if not jobs:
        return generate_response(False, message="No jobs were submitted")

    # check prodSourceLabel and job_label are correct
    for tmp_job in jobs:
        # check production jobs are submitted with production role
        if tmp_job.prodSourceLabel in ["managed"] and not is_production_role:
            return generate_response(False, message=f"{user} is missing production for prodSourceLabel={tmp_job.prodSourceLabel} submission")

        # validate the job_label
        if tmp_job.job_label not in [None, "", "NULL"] and tmp_job.job_label not in JobUtils.job_labels:
            return generate_response(False, message=f"job_label={tmp_job.job_label} is not valid")

    sample_job = jobs[0]

    # get user VO
    try:
        user_vo = "atlas"
        if sample_job.VO not in [None, "", "NULL"]:
            user_vo = sample_job.VO
    except (IndexError, AttributeError) as e:
        tmp_logger.error(f"User VO was not found and defaulted to {user_vo}. (Exception {e})")

    # atlas jobs require FQANs
    if user_vo == "atlas" and not fqans:
        tmp_logger.error(f"Proxy was missing FQANs for {user_vo}")

    # LSST: try to overwrite with pipeline username
    if user_vo.lower() == "lsst":
        try:
            if sample_job.prodUserName and sample_job.prodUserName.lower() != "none":
                user = sample_job.prodUserName
        except AttributeError:
            tmp_logger.error(f"VO {user_vo} check: username not found in job parameters and defaulted to submitter ({user})")

    # store jobs
    ret = global_task_buffer.storeJobs(jobs, user, fqans=fqans, hostname=host, userVO=user_vo)
    tmp_logger.debug(f"{user} -> {len(ret)}")

    # There was no response
    if not ret:
        return generate_response(False, message="Failed to submit jobs")

    # There was a string for response, i.e. an error message
    if isinstance(ret, str):
        return generate_response(False, message=ret)

    return generate_response(True, data=ret)
