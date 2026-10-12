import datetime
import sys
import traceback
from typing import Any, Dict, List

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
from pandaserver.config import panda_config
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_job")

# Installed by init_task_buffer() before any handler runs, so these are declared
# non-Optional for the same reason as BaseModule.conn/cur: an Optional type would
# only push a None check onto every handler without making any of them safer.
global_task_buffer: TaskBuffer = None  # type: ignore[assignment]

# most jobs one get_status_history call may name: what Oracle takes in one IN list. The
# db_read_probe_budget normally bounds it far lower, since each job costs one index lookup per
# partition of jobs_StatusLog
MAX_STATUS_HISTORY_JOBS = 1000

# slack put on both ends of a get_status_history window before it is widened to whole days. A
# creationTime is written with the clock of a server and jobs_StatusLog with the database clock, so
# a job created at 00:00:02 may have its first row at 23:59:57 of the day before; widening to the
# day alone would leave that row out
STATUS_HISTORY_CLOCK_SLACK = datetime.timedelta(minutes=10)


def _parse_utc_time(value: str) -> datetime.datetime:
    """
    Read a time given in ISO 8601, e.g. `2026-10-12 14:30:45`, as a naive datetime in UTC.

    Args:
        value(str): the time; without a UTC offset it is taken to be in UTC already

    Returns:
        datetime: the time in UTC without tzinfo, as the database columns hold it
    """
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer


@request_validation(_logger, secure=True, request_method="GET")
def get_status(req: PandaRequest, job_ids: List[int], timeout: int = 60) -> Dict[str, Any]:
    """
    Get status of a job.

    Gets the status of a job and command to the pilot if any. Requires a secure connection.

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
        tmp_logger.debug("Failed")
        return generate_response(False, message="failed", data={"code": Protocol.SC_Failed})

    # Success
    data = timed_method.result
    tmp_logger.debug(f"Done with data={data}")
    return generate_response(True, data=data)


@request_validation(_logger, secure=True)
def get_description(req: PandaRequest, job_ids: List[int]) -> Dict[str, Any]:
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
    tmp_logger = LogWrapper(_logger, "get_description")
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
    ret = [job.to_dict_advanced() if job is not None else None for job in global_task_buffer.peekJobs(job_ids)]
    _logger.debug("Done")

    return generate_response(True, data=ret)


@request_validation(_logger, secure=True)
def get_description_incl_archive(req: PandaRequest, job_ids: List[int]) -> Dict[str, Any]:
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
    tmp_logger = LogWrapper(_logger, "get_description_including_archive")
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

    ret = [job.to_dict_advanced() if job is not None else None for job in global_task_buffer.getFullJobStatus(job_ids)]

    tmp_logger.debug("getFullJobStatus end")
    return generate_response(True, data=ret)


@request_validation(_logger, secure=True, request_method="GET")
def get_status_history(req: PandaRequest, job_ids: List[int], since: str | None = None, until: str | None = None) -> Dict[str, Any]:
    """
    Get status history of jobs

    Gets the status changes recorded for jobs in jobs_StatusLog, oldest first for each job, optionally within a time window. Requires a secure connection.

    Each job is looked up once in every daily partition of jobs_StatusLog the window touches, and in every partition without a
    window, so the most jobs one call may name is the db_read_probe_budget of the server divided by the number of partitions
    touched. A call naming more is refused, not cut short. Both numbers come back with every answer, so a caller can split its
    jobs into calls that fit. A window around the lifetimes of the jobs, e.g. from their creationTime, lets one call take many
    more jobs than the ~10 a call without a window takes.

    The window is widened, so that a change is not missed when the clock of the server that wrote a time such as creationTime
    and the clock of the database differ by a little: both ends are first moved out by 10 minutes, then to whole UTC days,
    since back to the start of its day and until forward to the start of the next day. Whole days touch no more daily
    partitions; the 10 minutes add one only when an end lies within 10 minutes of midnight.

    API details:
        HTTP Method: GET
        Path: /v1/job/get_status_history

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_ids(list of int): list of PanDA job IDs
        since(str, optional): read only the changes at or after this time, in UTC, e.g. `2026-10-12 14:30:45` (ISO 8601 is accepted)
        until(str, optional): read only the changes before this time, in UTC, in the same format

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`.
            When successful, the data field is `{"rows": [...], "as_of": datetime, "retained_from": datetime or None,
            "since": datetime or None, "until": datetime or None, "partitions": int, "budget": int, "record_statuschange": bool}`:
            - rows: one dict per status change, with PandaID, modificationTime, jobStatus, prodSourceLabel, cloud, computingSite,
              modificationHost and modiftime_extended. A job without rows has none
            - as_of: database time in UTC taken before the rows were read. Every change committed by then is in rows
            - retained_from: time from which the table holds every row, or None when it cannot be told. Old partitions are dropped,
              so a job without rows before this time may still have changed its status then
            - since, until: the window actually read, after the widening, or None for an open end. A job without rows outside
              the window may still have changed its status there
            - partitions, budget: the number of partitions the window touches, counted as 1 when there are none, and the
              db_read_probe_budget; a call with the same window may name at most budget // partitions jobs
            - record_statuschange: whether this server writes jobs_StatusLog. Other PanDA nodes have their own setting
            When unsuccessful, the message field says why, and the data field says it as values:
            - `{"rejected": "no_keys"}`: no job_ids
            - `{"rejected": "too_many_keys", "keys": int, "max_keys": int}`: more job_ids than one IN list takes
            - `{"rejected": "invalid_window"}`: since or until is not a time, or since is not before until
            - `{"rejected": "probe_budget", "keys": int, "partitions": int, "budget": int}`: keys x partitions exceed the budget
            - `{"error": str}`: the database failed, with the error code (ORA-xxxxx, or the SQLSTATE on Postgres)
    """
    tmp_logger = LogWrapper(_logger, f"get_status_history n_jobs={len(job_ids)} since={since} until={until}")
    tmp_logger.debug("Start")

    # a job named twice is looked up once
    job_ids = list(dict.fromkeys(job_ids))
    if not job_ids:
        message = "job_ids must not be empty"
        tmp_logger.debug(message)
        return generate_response(False, message=message, data={"rejected": "no_keys"})
    if len(job_ids) > MAX_STATUS_HISTORY_JOBS:
        message = f"at most {MAX_STATUS_HISTORY_JOBS} job_ids are accepted, got {len(job_ids)}"
        tmp_logger.debug(message)
        return generate_response(False, message=message, data={"rejected": "too_many_keys", "keys": len(job_ids), "max_keys": MAX_STATUS_HISTORY_JOBS})

    # the window, moved out by the clock slack and then to whole UTC days
    since_time: datetime.datetime | None = None
    until_time: datetime.datetime | None = None
    try:
        if since:
            since_time = (_parse_utc_time(since) - STATUS_HISTORY_CLOCK_SLACK).replace(hour=0, minute=0, second=0, microsecond=0)
        if until:
            until_slack = _parse_utc_time(until) + STATUS_HISTORY_CLOCK_SLACK
            until_time = until_slack.replace(hour=0, minute=0, second=0, microsecond=0)
            if until_time != until_slack:
                until_time += datetime.timedelta(days=1)
    except ValueError as e:
        message = f"invalid since or until: {e}"
        tmp_logger.debug(message)
        return generate_response(False, message=message, data={"rejected": "invalid_window"})
    if since and until and _parse_utc_time(since) >= _parse_utc_time(until):
        message = f"since={since} must be before until={until}"
        tmp_logger.debug(message)
        return generate_response(False, message=message, data={"rejected": "invalid_window"})

    success, message, data = global_task_buffer.get_job_status_history(job_ids, panda_config.db_read_probe_budget, since_time, until_time)
    if not success:
        tmp_logger.debug(f"Failed with {message}")
        return generate_response(False, message=message, data=data)
    data["record_statuschange"] = getattr(panda_config, "record_statuschange", False) is True

    tmp_logger.debug(f"""Done with {len(data["rows"])} rows""")
    return generate_response(True, data=data)


@request_validation(_logger, secure=False, request_method="GET")
def generate_offline_execution_script(req: PandaRequest, job_id: int, days: int | None = None) -> Dict[str, Any]:
    """
    Get execution script for a job.

    Gets the execution script for a job, including Rucio download of input, ALRB setup, downloading transformation script and running the script.
    The generated script takes options to use only a subset of the input files (`--nfiles=<N>`) and to read them directly from storage
    through a generated PoolFileCatalog.xml instead of downloading them (`--direct`, `--rse_expression=<RSE_EXP>`, `--schemes=<SCHEMES>`).

    API details:
        HTTP Method: GET
        Path: /v1/job/generate_offline_execution_script

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_id (int): PanDA job ID
        days (int, optional): Number of days to look back in the archive tables. Defaults to 30.

    Returns:
        str: The generated script, or a message starting with `ERROR:` when the script could not be generated.
    """
    tmp_logger = LogWrapper(_logger, f"generate_offline_execution_script job_id={job_id} days={days}")
    tmp_logger.debug("Start")
    script = global_task_buffer.getScriptOfflineRunning(job_id, days)

    if script.startswith("ERROR"):
        tmp_logger.debug(f"Failed to generate script: {script}")
        return script  # type: ignore[return-value]

    tmp_logger.debug("Done")
    return script  # type: ignore[return-value]


@request_validation(_logger, secure=True, request_method="GET")
def get_metadata_for_analysis_jobs(req: PandaRequest, task_id: int) -> Dict[str, Any]:
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
def kill(req: PandaRequest, job_ids: List[int], code: int | None = None, use_email_as_id: bool = False, kill_options: List[str] = []) -> dict[str, Any]:
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

    tmp_logger = LogWrapper(_logger, "kill")
    tmp_logger.debug(f"Start user: {user} code: {code} is_production_manager: {is_production_manager} fqans: {fqans} job_ids: {job_ids}")

    # Get the user's email address if use_email_as_id is set
    if use_email_as_id:
        email = get_email_address(user, tmp_logger)
        if email:
            user = email

    # Extract working groups with production role from FQANs
    wg_prod_roles = extract_production_working_groups(fqans)

    # kill jobs
    ret = global_task_buffer.killJobs(job_ids, user, code, is_production_manager, wg_prod_roles, kill_options)
    tmp_logger.debug(f"Done with ret: {ret}")
    return generate_response(True, data=ret)


@request_validation(_logger, secure=True, request_method="POST")
def reassign(req: PandaRequest, job_ids: List[int]) -> dict[str, Any]:
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

    tmp_logger = LogWrapper(_logger, f"reassign job_ids={job_ids}")
    tmp_logger.debug("Start")
    # taskBuffer.reassignJobs always returns True
    global_task_buffer.reassignJobs(job_ids)
    tmp_logger.debug("Done")
    return generate_response(True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def set_command(req: PandaRequest, job_id: int, command: str) -> dict[str, Any]:
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
def set_debug_mode(req: PandaRequest, job_id: int, mode: bool) -> dict[str, Any]:
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

    # setDebugMode reports the outcome in the message and says "Succeeded" for the one case
    # that worked; every other string is a refusal or a failure, and None is a database error
    success = message == "Succeeded"

    return generate_response(success, message=message)


@request_validation(_logger, secure=True, request_method="POST")
def submit(req: PandaRequest, jobs: str) -> dict[str, Any]:
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
    tmp_logger = LogWrapper(_logger, "submit")
    user = get_dn(req)
    fqans = get_fqan(req)
    is_production_role = has_production_role(req)
    host = req.get_remote_host()

    # deserialize job specs
    try:
        job_specs = JobUtils.load_jobs_json(jobs)
        tmp_logger.debug(f"{user} len:{len(job_specs)} is_production_role={is_production_role} FQAN:{fqans}")
        max_jobs = 5000
        if len(job_specs) > max_jobs:
            _logger.error(f"Number of jobs exceeded maximum {max_jobs}. Truncating the list.")
            job_specs = job_specs[:max_jobs]
    except Exception as ex:
        error_message = f"Failed to deserialize jobs: {str(ex)} {traceback.format_exc()}"
        tmp_logger.error(error_message)
        return generate_response(False, message=error_message)

    if not job_specs:
        return generate_response(False, message="No jobs were submitted")

    # check prodSourceLabel and job_label are correct
    for tmp_job in job_specs:
        # check production jobs are submitted with production role
        if tmp_job.prodSourceLabel in ["managed"] and not is_production_role:
            return generate_response(False, message=f"{user} is missing production for prodSourceLabel={tmp_job.prodSourceLabel} submission")

        # validate the job_label
        if tmp_job.job_label not in [None, "", "NULL"] and tmp_job.job_label not in JobUtils.job_labels:
            return generate_response(False, message=f"job_label={tmp_job.job_label} is not valid")

    sample_job = job_specs[0]

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
    ret = global_task_buffer.storeJobs(job_specs, user, fqans=fqans, hostname=host, userVO=user_vo)
    tmp_logger.debug(f"{user} -> {len(ret)}")

    # There was no response
    if not ret:
        return generate_response(False, message="Failed to submit jobs")

    # There was a string for response, i.e. an error message
    if isinstance(ret, str):
        return generate_response(False, message=ret)

    return generate_response(True, data=ret)
